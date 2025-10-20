/**
 * @fileoverview MongoDB applier v2 - Refactored with concise syntax
 *
 * This module provides a MongoDB applier that executes migration operations
 * against real MongoDB databases. Refactored to use a more concise and
 * maintainable syntax similar to the memory applier.
 *
 * @module
 */

import type { Db } from "../../mongodb.ts";
import type { MigrationDefinition, MigrationRule, SchemasDefinition } from "../types.ts";
import { ulid } from "@std/ulid";
import * as v from "valibot";
import { toMongoValidator } from "../../validator.ts";
import { extractIndexes, keyEqual, normalizeIndexOptions } from "../../indexes.ts";
import { sanitizePathName } from "../../schema-navigator.ts";
import {
  createMultiCollectionInfo,
  createMetadataSchemas,
  discoverMultiCollectionInstances,
  MULTI_COLLECTION_INFO_TYPE,
  MULTI_COLLECTION_MIGRATIONS_TYPE,
  multiCollectionInstanceExists,
  recordMultiCollectionMigration,
  shouldInstanceReceiveMigration,
} from "../multicollection-registry.ts";

/**
 * Generates a new unique ID using ULID
 */
function newId() {
  return ulid().toLowerCase();
}

/**
 * Configuration options for the MongoDB applier
 */
export interface MongodbApplierOptions {
  /** Whether to validate operations strictly before applying */
  strictValidation?: boolean;
  /** Maximum number of documents to process in a single batch */
  batchSize?: number;
  /** Current migration ID being applied (for version tracking) */
  currentMigrationId?: string;
}

const DEFAULT_OPTIONS: Required<MongodbApplierOptions> = {
  strictValidation: true,
  batchSize: 1000,
  currentMigrationId: "unknown",
};

export function createMongodbApplier(
  db: Db,
  migration: MigrationDefinition,
  options: MongodbApplierOptions = {},
): {
  applyOperation: (operation: MigrationRule) => Promise<void>;
  reverseOperation: (operation: MigrationRule) => Promise<void>;
  applyMigration: (operations: MigrationRule[], direction: 'up' | 'down') => Promise<void>;
  setCurrentMigrationId: (migrationId: string) => void;
} {
  const opts = { ...DEFAULT_OPTIONS, ...options };

  // Track which multi-model instances have been recorded for this migration
  // to avoid duplicate recordings when multiple operations target the same instance
  const recordedInstances = new Set<string>();

  /**
   * Helper to check if a collection exists
   */
  async function collectionExists(collectionName: string): Promise<boolean> {
    try {
      const collections = await db.listCollections({ name: collectionName }).toArray();
      return collections.length > 0;
    } catch {
      return false;
    }
  }

  /**
   * Helper to disable validator for a collection temporarily
   */
  async function disableValidator(collectionName: string): Promise<void> {
    if (await collectionExists(collectionName)) {
      try {
        await db.command({
          collMod: collectionName,
          validator: {},
          validationLevel: "off",
        });
      } catch (error) {
        // Tolerate errors (collection might not have validators)
        console.warn(`Could not disable validator for ${collectionName}:`, error);
      }
    }
  }

  /**
   * Helper to create a single index with conflict resolution
   */
  async function createIndexSafely(
    collectionName: string,
    indexName: string,
    keySpec: Record<string, number>,
    options: { unique: boolean; sparse: boolean }
  ): Promise<void> {
    const collection = db.collection(collectionName);

    // Check if the index already exists with the same specifications
    // Uses the same logic as collection.ts and multi-collection.ts
    const existingIndexes = await collection.indexes();
    const existingIndex = existingIndexes.find(idx => idx.name === indexName) ||
      existingIndexes.find(idx => keyEqual(idx.key || {}, keySpec));

    const desiredOptions = {
      unique: options.unique,
      sparse: options.sparse,
      name: indexName,
    };

    // Use the shared normalization logic for comparison
    let needsRecreate = true;
    if (existingIndex) {
      const existingNorm = normalizeIndexOptions(existingIndex);
      const desiredNorm = normalizeIndexOptions(desiredOptions);

      // Compare normalized options
      if (
        existingNorm.unique === desiredNorm.unique &&
        existingNorm.collation === desiredNorm.collation &&
        existingNorm.partialFilterExpression === desiredNorm.partialFilterExpression &&
        keyEqual(existingIndex.key || {}, keySpec)
      ) {
        needsRecreate = false;
      }
    }

    if (!needsRecreate) {
      // Index already exists with the same specifications, no need to recreate
      return;
    }

    // Drop existing index if it exists but with different specifications
    if (existingIndex) {
      try {
        await collection.dropIndex(existingIndex.name!);
      } catch (dropError) {
        // Tolerate IndexNotFound errors (race condition)
        // @ts-ignore - MongoDB driver error has code property
        const isNotFound = dropError instanceof Error && (
          // @ts-ignore
          dropError.code === 27 ||
          dropError.message.includes("IndexNotFound")
        );
        if (!isNotFound) {
          console.warn(`Could not drop index ${indexName} on ${collectionName}:`, dropError);
          return;
        }
      }
    }

    // Create the index
    try {
      await collection.createIndex(keySpec, desiredOptions);
    } catch (error) {
      // Tolerate race conditions where the index was already created
      const isIndexError = error instanceof Error && (
        error.message.includes("already exists") ||
        // @ts-ignore - MongoDB driver error has code property
        error.code === 86 ||
        error.message.includes("IndexKeySpecsConflict")
      );

      if (!isIndexError) {
        throw error;
      }

      // If there's still a conflict after dropping, log a warning
      console.warn(`Index ${indexName} on ${collectionName} already exists or has conflicts, skipping`);
    }
  }

  /**
   * Helper to apply indexes to a collection
   */
  async function applyIndexes(
    collectionName: string,
    schema: Record<string, unknown>,
  ): Promise<void> {
    const wrappedSchema = v.object(schema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
    const indexes = extractIndexes(wrappedSchema);

    for (const index of indexes) {
      const indexName = sanitizePathName(index.path);
      const keySpec: Record<string, number> = { [index.path]: 1 };

      await createIndexSafely(collectionName, indexName, keySpec, {
        unique: index.metadata.unique || false,
        sparse: false,
      });
    }
  }

  /**
   * Helper to apply indexes for a specific type in a multi-collection
   */
  async function applyIndexesForType(
    collectionName: string,
    typeName: string,
    typeSchema: Record<string, unknown>,
  ): Promise<void> {
    const typeSchemaWithType = {
      _type: v.literal(typeName),
      ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
    };
    const wrappedTypeSchema = v.object(typeSchemaWithType);
    const indexes = extractIndexes(wrappedTypeSchema);

    for (const index of indexes) {
      const indexName = sanitizePathName(index.path);
      const keySpec: Record<string, number> = { [index.path]: 1 };

      await createIndexSafely(collectionName, indexName, keySpec, {
        unique: index.metadata.unique || false,
        sparse: false,
      });
    }
  }

  /**
   * Synchronizes validators and indexes for all collections, multi-collections, and multi-models
   * based on the target schemas
   */
  async function synchronizeValidatorsAndIndexes(schemas: SchemasDefinition): Promise<void> {
    // Synchronize simple collections
    if (schemas.collections) {
      for (const [collectionName, schema] of Object.entries(schemas.collections)) {
        if (await collectionExists(collectionName)) {
          // Update validator
          const wrappedSchema = v.object(schema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
          const validator = toMongoValidator(wrappedSchema);
          await db.command({
            collMod: collectionName,
            validator,
            validationLevel: "strict",
          });

          // Synchronize indexes
          await applyIndexes(collectionName, schema);
        }
      }
    }

    // Synchronize multi-collections (WITH metadata)
    if (schemas.multiCollections) {
      for (const [collectionName, multiSchema] of Object.entries(schemas.multiCollections)) {
        if (await collectionExists(collectionName)) {
          // Build union validator with metadata schemas
          const typeSchemas = Object.entries(multiSchema).map(
            ([typeName, typeSchema]) => v.object({
              _type: v.literal(typeName),
              ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
            })
          );

          // Include metadata schemas so _information and _migrations documents can be inserted
          const allSchemas = [...typeSchemas, ...createMetadataSchemas()];

          const unionSchema = allSchemas.length > 0
            // deno-lint-ignore no-explicit-any
            ? v.union(allSchemas as any)
            : v.object({ _type: v.string() });

          const validator = toMongoValidator(unionSchema);
          await db.command({
            collMod: collectionName,
            validator,
            validationLevel: "strict",
          });

          // Synchronize indexes for each type
          for (const [typeName, typeSchema] of Object.entries(multiSchema)) {
            await applyIndexesForType(collectionName, typeName, typeSchema as Record<string, unknown>);
          }
        }
      }
    }

    // Synchronize multi-models (WITH metadata)
    if (schemas.multiModels) {
      for (const [modelType, multiSchema] of Object.entries(schemas.multiModels)) {
        // Discover all instances of this model type
        const instances = await discoverMultiCollectionInstances(db, modelType);

        for (const instanceName of instances) {
          if (await collectionExists(instanceName)) {
            // Build union validator with metadata schemas
            const typeSchemas = Object.entries(multiSchema).map(
              ([typeName, typeSchema]) => v.object({
                _type: v.literal(typeName),
                ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
              })
            );

            const allSchemas = [...typeSchemas, ...createMetadataSchemas()];
            const unionSchema = allSchemas.length > 0
              // deno-lint-ignore no-explicit-any
              ? v.union(allSchemas as any)
              : v.object({ _type: v.string() });

            const validator = toMongoValidator(unionSchema);
            await db.command({
              collMod: instanceName,
              validator,
              validationLevel: "strict",
            });

            // Synchronize indexes for each type
            for (const [typeName, typeSchema] of Object.entries(multiSchema)) {
              await applyIndexesForType(instanceName, typeName, typeSchema as Record<string, unknown>);
            }
          }
        }
      }
    }
  }

  /**
   * Disables all validators for collections in the target schemas
   * Used before rollback to prevent validation errors
   */
  async function disableAllValidators(schemas: SchemasDefinition): Promise<void> {
    // Disable validators for simple collections
    if (schemas.collections) {
      for (const collectionName of Object.keys(schemas.collections)) {
        await disableValidator(collectionName);
      }
    }

    // Disable validators for multi-collections
    if (schemas.multiCollections) {
      for (const collectionName of Object.keys(schemas.multiCollections)) {
        await disableValidator(collectionName);
      }
    }

    // Disable validators for multi-models
    if (schemas.multiModels) {
      for (const modelType of Object.keys(schemas.multiModels)) {
        const instances = await discoverMultiCollectionInstances(db, modelType);
        for (const instanceName of instances) {
          await disableValidator(instanceName);
        }
      }
    }
  }

  /**
   * Helper to transform documents in batches
   */
  async function transformDocuments(
    collectionName: string,
    filter: Record<string, unknown>,
    transformer: (doc: Record<string, unknown>) => Record<string, unknown>,
  ): Promise<void> {
    const collection = db.collection(collectionName);
    let processedCount = 0;

    while (true) {
      const documents = await collection.find(filter)
        .limit(opts.batchSize)
        .skip(processedCount)
        .toArray();

      if (documents.length === 0) break;

      const bulkOps = documents.map(doc => {
        try {
          const transformed = transformer(doc);
          return {
            replaceOne: {
              filter: { _id: doc._id },
              replacement: transformed,
            },
          };
        } catch (error) {
          if (opts.strictValidation) {
            throw new Error(
              `Transform failed for document ${doc._id}: ${
                error instanceof Error ? error.message : String(error)
              }`,
            );
          }
          console.warn(`Skipping document ${doc._id} due to transform error:`, error);
          return null;
        }
      }).filter(op => op !== null);

      if (bulkOps.length > 0) {
        await collection.bulkWrite(bulkOps);
      }

      processedCount += documents.length;
    }
  }

  const migrations: {
    [K in MigrationRule['type']]: {
      apply: (operation: Extract<MigrationRule, { type: K }>) => Promise<void>,
      reverse: (operation: Extract<MigrationRule, { type: K }>) => Promise<void>,
    }
  } = {
    create_collection: {
      apply: async (operation) => {
        const collExist = await collectionExists(operation.collectionName);

        const collOptions: Record<string, unknown> = {};
        if (operation.schema) {
          const wrappedSchema = v.object(operation.schema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
          collOptions.validator = toMongoValidator(wrappedSchema);
        }

        if(collExist) {
          if (opts.strictValidation) {
            // throw new Error(`Collection ${operation.collectionName} already exists`);
            console.warn(`Collection ${operation.collectionName} already exists, skipping creation.`);
          }

          // If collection exists, still apply indexes && update validator if schema provided
          if (operation.schema) {
            await applyIndexes(operation.collectionName, operation.schema);

            await db.command({
              collMod: operation.collectionName,
              validator: collOptions.validator || {},
              validationLevel: "strict",
            });
          }
        } else {
          await db.createCollection(operation.collectionName, collOptions);
        }

        if (operation.schema) {
          await applyIndexes(operation.collectionName, operation.schema);
        }
      },
      reverse: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        await db.collection(operation.collectionName).drop();
      }
    },

    create_multicollection: {
      apply: async (operation) => {
        const collExist = await collectionExists(operation.collectionName);

        // Create union validator for all types (including metadata schemas)
        const typeSchemas = Object.entries(operation.schema).map(
          ([typeName, typeSchema]) => v.object({
            _type: v.literal(typeName),
            ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
          })
        );

        // Include metadata schemas so _information and _migrations documents can be inserted
        const allSchemas = [...typeSchemas, ...createMetadataSchemas()];

        const unionSchema = allSchemas.length > 0
          // deno-lint-ignore no-explicit-any
          ? v.union(allSchemas as any)
          : v.object({ _type: v.string() });

        const validator = toMongoValidator(unionSchema);

        if (collExist) {
          if (opts.strictValidation) {
            console.warn(`Multi-collection ${operation.collectionName} already exists, skipping creation.`);
          }

          // If collection exists, still apply indexes && update validator
          await db.command({
            collMod: operation.collectionName,
            validator,
            validationLevel: "strict",
          });
        } else {
          const collOptions = { validator };
          await db.createCollection(operation.collectionName, collOptions);
        }

        // Apply indexes for each type
        for (const [typeName, typeSchema] of Object.entries(operation.schema)) {
          await applyIndexesForType(operation.collectionName, typeName, typeSchema as Record<string, unknown>);
        }
      },
      reverse: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        await db.collection(operation.collectionName).drop();
      }
    },

    create_multimodel_instance: {
      apply: async (operation) => {
        const collExist = await collectionExists(operation.collectionName);

        // Create union validator for all types + metadata schemas
        const typeSchemas = Object.entries(operation.schema).map(
          ([typeName, typeSchema]) => v.object({
            _type: v.literal(typeName),
            ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
          })
        );

        const allSchemas = [...typeSchemas, ...createMetadataSchemas()];
        const unionSchema = allSchemas.length > 0
          // deno-lint-ignore no-explicit-any
          ? v.union(allSchemas as any)
          : v.object({ _type: v.string() });

        const validator = toMongoValidator(unionSchema);

        if (collExist) {
          if (opts.strictValidation) {
            console.warn(`Multi-model instance ${operation.collectionName} already exists, skipping creation.`);
          }

          // If collection exists, still apply indexes && update validator
          await db.command({
            collMod: operation.collectionName,
            validator,
            validationLevel: "strict",
          });
        } else {
          const collOptions = { validator };
          await db.createCollection(operation.collectionName, collOptions);

          // Create metadata info document only for new collections
          await createMultiCollectionInfo(
            db,
            operation.collectionName,
            operation.modelType,
            opts.currentMigrationId,
          );

          // Mark this migration as already recorded for this instance
          // createMultiCollectionInfo already added it to the appliedMigrations array
          if (opts.currentMigrationId) {
            const recordKey = `${operation.collectionName}:${opts.currentMigrationId}:applied`;
            recordedInstances.add(recordKey);
          }
        }

        // Apply indexes for each type
        for (const [typeName, typeSchema] of Object.entries(operation.schema)) {
          await applyIndexesForType(operation.collectionName, typeName, typeSchema as Record<string, unknown>);
        }
      },
      reverse: async (operation) => {
        if (opts.strictValidation && !await multiCollectionInstanceExists(db, operation.collectionName)) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }
        await db.collection(operation.collectionName).drop();
      }
    },

    mark_as_multimodel: {
      apply: async (operation) => {
        const collection = db.collection(operation.collectionName);

        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          // throw new Error(`Collection ${operation.collectionName} does not exist`);
          console.warn(`Collection ${operation.collectionName} does not exist, creating it first.`);
        }

        const existing = await collection.findOne({ _type: MULTI_COLLECTION_INFO_TYPE });
        if (existing) {
          throw new Error(`Collection ${operation.collectionName} is already marked as multi-model`);
        }

        // Get the schema for this model type from the migration schemas
        const modelSchema = migration.schemas.multiModels?.[operation.modelType];
        if (!modelSchema) {
          throw new Error(`Model type ${operation.modelType} not found in migration schemas`);
        }

        // Update validator to include metadata schemas BEFORE inserting _information document
        const typeSchemas = Object.entries(modelSchema).map(
          ([typeName, typeSchema]) => v.object({
            _type: v.literal(typeName),
            ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
          })
        );

        const allSchemas = [...typeSchemas, ...createMetadataSchemas()];
        const unionSchema = allSchemas.length > 0
          // deno-lint-ignore no-explicit-any
          ? v.union(allSchemas as any)
          : v.object({ _type: v.string() });

        const validator = toMongoValidator(unionSchema);
        await db.command({
          collMod: operation.collectionName,
          validator,
          validationLevel: "strict",
        });

        await createMultiCollectionInfo(
          db,
          operation.collectionName,
          operation.modelType,
          opts.currentMigrationId,
        );

        // Mark this migration as already recorded for this instance
        // createMultiCollectionInfo already added it to the appliedMigrations array
        if (opts.currentMigrationId) {
          const recordKey = `${operation.collectionName}:${opts.currentMigrationId}:applied`;
          recordedInstances.add(recordKey);
        }
      },
      reverse: async (operation) => {
        const collection = db.collection(operation.collectionName);
        await collection.deleteMany({
          _type: { $in: [MULTI_COLLECTION_INFO_TYPE, MULTI_COLLECTION_MIGRATIONS_TYPE] },
        });

        // Restore validator WITHOUT metadata schemas (back to plain multi-collection)
        // Get schema from the model type definition in current migration
        const modelSchema = migration.schemas.multiModels?.[operation.modelType];

        if (modelSchema) {
          const typeSchemas = Object.entries(modelSchema).map(
            ([typeName, typeSchema]) => v.object({
              _type: v.literal(typeName),
              ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
            })
          );

          // NO metadata schemas - just type schemas
          const unionSchema = typeSchemas.length > 0
            // deno-lint-ignore no-explicit-any
            ? v.union(typeSchemas as any)
            : v.object({ _type: v.string() });

          const validator = toMongoValidator(unionSchema);
          await db.command({
            collMod: operation.collectionName,
            validator,
            validationLevel: "strict",
          });
        }
      }
    },

    seed_collection: {
      apply: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }

        const collection = db.collection(operation.collectionName);
        const documents = operation.documents.map((doc: unknown) => {
          const typedDoc = doc as Record<string, unknown>;
          const value = v.safeParse(v.object(operation.schema), typedDoc);
          if (!value.success) {
            throw new Error(`Document validation failed: ${JSON.stringify(value.issues)}`);
          }
          return value.output;
        });

        for (let i = 0; i < documents.length; i += opts.batchSize) {
          const batch = documents.slice(i, i + opts.batchSize);
          // deno-lint-ignore no-explicit-any
          await collection.insertMany(batch as any);
        }
      },
      reverse: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }

        const collection = db.collection(operation.collectionName);
        const documentIds = operation.documents
          .map((doc: unknown) => {
            const typedDoc = doc as Record<string, unknown>;
            return typedDoc._id;
          })
          .filter(id => id !== undefined && id !== null);

        if (documentIds.length > 0) {
          await collection.deleteMany({ _id: { $in: documentIds } } as Record<string, unknown>);
        } else {
          console.warn(
            `Warning: Cannot reverse seed for collection ${operation.collectionName} - no _id fields found`,
          );
        }
      }
    },

    seed_multicollection_type: {
      apply: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }

        const collection = db.collection(operation.collectionName);
        const documents = operation.documents.map((doc: unknown) => {
          const typedDoc = doc as Record<string, unknown>;
          const value = v.safeParse(v.object({
            _type: v.literal(operation.documentType),
            ...operation.schema,
          }), {
            _type: operation.documentType,
            ...typedDoc,
          });
          if (!value.success) {
            throw new Error(`Document validation failed: ${JSON.stringify(value.issues)}`);
          }

          return value.output;
        });

        for (let i = 0; i < documents.length; i += opts.batchSize) {
          const batch = documents.slice(i, i + opts.batchSize);
          // deno-lint-ignore no-explicit-any
          await collection.insertMany(batch as any);
        }
      },
      reverse: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }

        const collection = db.collection(operation.collectionName);
        const documentIds = operation.documents
          .map((doc: unknown) => {
            const typedDoc = doc as Record<string, unknown>;
            return typedDoc._id;
          })
          .filter(id => id !== undefined && id !== null);

        if (documentIds.length > 0) {
          await collection.deleteMany({ _id: { $in: documentIds } } as Record<string, unknown>);
        } else {
          console.warn(
            `Warning: Cannot reverse seed for multi-collection ${operation.collectionName} - no _id fields found`,
          );
        }
      }
    },

    seed_multimodel_instance_type: {
      apply: async (operation) => {
        if (opts.strictValidation && !await multiCollectionInstanceExists(db, operation.collectionName)) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }

        const collection = db.collection(operation.collectionName);
        const documents = operation.documents.map((doc: unknown) => {
          const typedDoc = doc as Record<string, unknown>;
          const value = v.safeParse(v.object({
            _type: v.literal(operation.documentType),
            ...operation.schema,
          }), {
            _type: operation.documentType,
            ...typedDoc,
          });
          if (!value.success) {
            throw new Error(`Document validation failed: ${JSON.stringify(value.issues)}`);
          }
          return value.output;
        });

        for (let i = 0; i < documents.length; i += opts.batchSize) {
          const batch = documents.slice(i, i + opts.batchSize);
          // deno-lint-ignore no-explicit-any
          await collection.insertMany(batch as any);
        }
      },
      reverse: async (operation) => {
        if (opts.strictValidation && !await multiCollectionInstanceExists(db, operation.collectionName)) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }

        const collection = db.collection(operation.collectionName);
        const documentIds = operation.documents
          .map((doc: unknown) => {
            const typedDoc = doc as Record<string, unknown>;
            return typedDoc._id;
          })
          .filter(id => id !== undefined && id !== null);

        if (documentIds.length > 0) {
          await collection.deleteMany({ _id: { $in: documentIds } } as Record<string, unknown>);
        } else {
          console.warn(
            `Warning: Cannot reverse seed for multi-model instance ${operation.collectionName} - no _id fields found`,
          );
        }
      }
    },

    seed_multimodel_instances_type: {
      apply: async (operation) => {
        const instances = await discoverMultiCollectionInstances(db, operation.modelType);
        
        if (instances.length === 0) {
          console.warn(`No instances found for model type ${operation.modelType}`);
          return;
        }

        for (const collectionName of instances) {
          // Check if this instance should receive this migration
          if (opts.currentMigrationId) {
            const shouldReceive = await shouldInstanceReceiveMigration(
              db,
              collectionName,
              opts.currentMigrationId,
            );
            if (!shouldReceive) {
              console.log(`Skipping instance ${collectionName} - already has this migration`);
              continue;
            }
          }

          const collection = db.collection(collectionName);
          const documents = operation.documents.map((doc: unknown) => {
            const typedDoc = doc as Record<string, unknown>;
            return {
              _id: typedDoc._id || (operation.schema._id ? v.getDefault(operation.schema._id) : undefined) || `${operation.documentType}:${newId()}`,
              ...typedDoc,
              _type: operation.documentType,
            };
          });

          for (let i = 0; i < documents.length; i += opts.batchSize) {
            const batch = documents.slice(i, i + opts.batchSize);
            // deno-lint-ignore no-explicit-any
            await collection.insertMany(batch as any);
          }

          // Record migration for this instance (only once per migration, even if multiple seed operations)
          if (opts.currentMigrationId) {
            const recordKey = `${collectionName}:${opts.currentMigrationId}`;
            if (!recordedInstances.has(recordKey)) {
              await recordMultiCollectionMigration(db, collectionName, opts.currentMigrationId);
              recordedInstances.add(recordKey);
            }
          }
        }
      },
      reverse: async (operation) => {
        const instances = await discoverMultiCollectionInstances(db, operation.modelType);

        for (const collectionName of instances) {
          const collection = db.collection(collectionName);
          const documentIds = operation.documents
            .map((doc: unknown) => {
              const typedDoc = doc as Record<string, unknown>;
              return typedDoc._id;
            })
            .filter(id => id !== undefined && id !== null);

          if (documentIds.length > 0) {
            await collection.deleteMany({ _id: { $in: documentIds } } as Record<string, unknown>);
          }

          // Record rollback for this instance (only once per migration, even if multiple seed operations)
          if (opts.currentMigrationId) {
            const recordKey = `${collectionName}:${opts.currentMigrationId}:reverted`;
            if (!recordedInstances.has(recordKey)) {
              await recordMultiCollectionMigration(
                db,
                collectionName,
                opts.currentMigrationId,
                "reverted"
              );
              recordedInstances.add(recordKey);
            }
          }
        }
      }
    },

    transform_collection: {
      apply: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        await transformDocuments(operation.collectionName, {}, operation.up as (doc: Record<string, unknown>) => Record<string, unknown>);
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        await transformDocuments(operation.collectionName, {}, operation.down as (doc: Record<string, unknown>) => Record<string, unknown>);
      }
    },

    transform_multicollection_type: {
      apply: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        await transformDocuments(
          operation.collectionName,
          { _type: operation.documentType } as Record<string, unknown>,
          operation.up as (doc: Record<string, unknown>) => Record<string, unknown>
        );
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        await transformDocuments(
          operation.collectionName,
          { _type: operation.documentType } as Record<string, unknown>,
          operation.down as (doc: Record<string, unknown>) => Record<string, unknown>
        );
      }
    },

    transform_multimodel_instance_type: {
      apply: async (operation) => {
        if (opts.strictValidation && !await multiCollectionInstanceExists(db, operation.collectionName)) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }
        await transformDocuments(
          operation.collectionName,
          { _type: operation.documentType } as Record<string, unknown>,
          operation.up as (doc: Record<string, unknown>) => Record<string, unknown>
        );
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        if (opts.strictValidation && !await multiCollectionInstanceExists(db, operation.collectionName)) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }
        await transformDocuments(
          operation.collectionName,
          { _type: operation.documentType } as Record<string, unknown>,
          operation.down as (doc: Record<string, unknown>) => Record<string, unknown>
        );
      }
    },

    transform_multimodel_instances_type: {
      apply: async (operation) => {
        const instances = await discoverMultiCollectionInstances(db, operation.modelType);
        
        if (instances.length === 0) {
          console.warn(`No instances found for model type ${operation.modelType}`);
          return;
        }

        for (const collectionName of instances) {
          // Check if this instance should receive this migration
          if (opts.currentMigrationId) {
            const shouldReceive = await shouldInstanceReceiveMigration(
              db,
              collectionName,
              opts.currentMigrationId,
            );
            if (!shouldReceive) {
              console.log(`Skipping instance ${collectionName} - already has this migration`);
              continue;
            }
          }

          await transformDocuments(
            collectionName,
            { _type: operation.documentType } as Record<string, unknown>,
            operation.up as (doc: Record<string, unknown>) => Record<string, unknown>
          );

          // Record migration for this instance (only once per migration, even if multiple operations)
          if (opts.currentMigrationId) {
            const recordKey = `${collectionName}:${opts.currentMigrationId}`;
            if (!recordedInstances.has(recordKey)) {
              await recordMultiCollectionMigration(db, collectionName, opts.currentMigrationId);
              recordedInstances.add(recordKey);
            }
          }
        }
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }

        const instances = await discoverMultiCollectionInstances(db, operation.modelType);

        for (const collectionName of instances) {
          await transformDocuments(
            collectionName,
            { _type: operation.documentType } as Record<string, unknown>,
            operation.down as (doc: Record<string, unknown>) => Record<string, unknown>
          );

          // Record rollback for this instance (only once per migration, even if multiple operations)
          if (opts.currentMigrationId) {
            const recordKey = `${collectionName}:${opts.currentMigrationId}:reverted`;
            if (!recordedInstances.has(recordKey)) {
              await recordMultiCollectionMigration(
                db,
                collectionName,
                opts.currentMigrationId,
                "reverted"
              );
              recordedInstances.add(recordKey);
            }
          }
        }
      }
    },

    update_indexes: {
      apply: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        await applyIndexes(operation.collectionName, operation.schema as Record<string, unknown>);
      },
      reverse: (_operation) => {
        // Index updates are idempotent, no reversal needed
        return Promise.resolve();
      }
    },
  };

  async function applyOperation(operation: MigrationRule): Promise<void> {
    const handler = migrations[operation.type]?.apply;
    if (!handler) {
      throw new Error(`No handler for operation type: ${operation.type}`);
    }
    // Type assertion is safe here because we're dispatching to the correct handler
    // deno-lint-ignore no-explicit-any
    return await handler(operation as any);
  }

  async function reverseOperation(operation: MigrationRule): Promise<void> {
    const handler = migrations[operation.type]?.reverse;
    if (!handler) {
      throw new Error(`No reverse handler for operation type: ${operation.type}`);
    }
    // Type assertion is safe here because we're dispatching to the correct handler
    // deno-lint-ignore no-explicit-any
    return await handler(operation as any);
  }

  /**
   * Applies a complete migration (all operations + schema synchronization)
   * 
   * This is the recommended way to apply migrations as it ensures validators
   * and indexes are synchronized after all operations are executed.
   * 
   * Strategy:
   * 1. Disable ALL validators before starting (prevents validation errors during transforms)
   * 2. Apply all operations without validation interference
   * 3. Re-enable and synchronize validators with target schemas
   * 
   * @param operations - Array of migration operations to apply
   * @param direction - 'up' for forward migration, 'down' for rollback
   */
  async function applyMigration(
    operations: MigrationRule[],
    direction: 'up' | 'down',
  ): Promise<void> {
    // Determine target schemas based on direction
    const targetSchemas = direction === 'up'
      ? migration.schemas
      : (migration.parent?.schemas || migration.schemas);

    // STEP 1: Disable ALL validators (prevents validation errors during transforms)
    // This is critical for both up and down migrations because:
    // - Up: old validators would reject documents transformed to new schema
    // - Down: new validators would reject documents transformed back to old schema
    await disableAllValidators(targetSchemas);

    // STEP 2: Apply all operations without validation interference
    for (const operation of operations) {
      if (direction === 'up') {
        await applyOperation(operation);
      } else {
        await reverseOperation(operation);
      }
    }

    // STEP 3: Synchronize validators and indexes with target schemas
    // Re-enables validators with the correct schema for the migration direction
    await synchronizeValidatorsAndIndexes(targetSchemas);

    // STEP 4: Record migration on ALL multi-model instances (even if not affected)
    // This ensures complete tracking of which migrations each instance has seen
    if (opts.currentMigrationId && targetSchemas.multiModels) {
      await recordMigrationOnAllMultiModelInstances(
        direction === 'up' ? 'applied' : 'reverted'
      );
    }
  }

  /**
   * Records the current migration on all instances of all multi-model types
   * This is called after migration to ensure all instances track the migration,
   * even if they weren't directly affected by it
   */
  async function recordMigrationOnAllMultiModelInstances(
    operation: 'applied' | 'reverted'
  ): Promise<void> {
    if (!opts.currentMigrationId) return;
    if (!migration.schemas.multiModels) return;

    const modelTypes = Object.keys(migration.schemas.multiModels);
    const recordedKey = `${opts.currentMigrationId}:${operation}:all`;

    // Prevent duplicate recording
    if (recordedInstances.has(recordedKey)) {
      return;
    }

    for (const modelType of modelTypes) {
      const instances = await discoverMultiCollectionInstances(db, modelType);
      
      for (const collectionName of instances) {
        const instanceKey = `${collectionName}:${opts.currentMigrationId}:${operation}`;
        
        // Skip if already recorded by operation handlers
        if (recordedInstances.has(instanceKey)) {
          continue;
        }

        await recordMultiCollectionMigration(
          db,
          collectionName,
          opts.currentMigrationId,
          operation
        );
        
        recordedInstances.add(instanceKey);
      }
    }

    recordedInstances.add(recordedKey);
  }

  return {
    applyOperation,
    reverseOperation,
    applyMigration,
    setCurrentMigrationId: (migrationId: string) => {
      opts.currentMigrationId = migrationId;
    },
  };
}

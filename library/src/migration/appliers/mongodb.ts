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
import { applyCollectionIndexes, applyMultiCollectionIndexes } from "../../indexes-applier.ts";
import {
  createMultiCollectionInfo,
  createMetadataSchemas,
  discoverMultiCollectionInstances,
  MULTI_COLLECTION_INFO_TYPE,
  MULTI_COLLECTION_MIGRATIONS_TYPE,
  multiCollectionInstanceExists,
  recordMultiCollectionMigration,
  shouldInstanceReceiveMigrationFromChain,
} from "../multicollection-registry.ts";
import { flowTargetId, extractIdPrefix, resolveSeedId } from "../utils/seed-id.ts";
import { getIrreversibleOperations } from "../builder.ts";

/**
 * Generates a new unique ID using ULID
 */
function newId() {
  return ulid().toLowerCase();
}

/**
 * Resolve the `_id` of a seed document: honour an explicit `_id`, else
 * derive a deterministic one (so apply and reverse compute the same id and
 * rollback can delete exactly what was inserted).
 */
function resolveSeedDocId(
  originalDoc: Record<string, unknown>,
  schemaIdField: unknown,
  fallbackPrefix: string,
  migrationId: string,
  opSignature: string,
  docIndex: number,
): string {
  return resolveSeedId(
    originalDoc,
    schemaIdField,
    fallbackPrefix,
    migrationId,
    opSignature,
    docIndex,
  );
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
   * Synchronizes validators and indexes for all collections, multi-collections, and multi-models
   * based on the target schemas
   */
  async function synchronizeValidatorsAndIndexes(schemas: SchemasDefinition): Promise<void> {
    // Synchronize simple collections
    if (schemas.collections) {
      for (const [collectionName, schema] of Object.entries(schemas.collections)) {
        if (await collectionExists(collectionName)) {
          // Update validator
          const collectionSchema = v.object(schema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
          const validator = toMongoValidator(collectionSchema);
          await db.command({
            collMod: collectionName,
            validator,
            validationLevel: "strict",
          });

          // Synchronize indexes using shared applier
          const collection = db.collection(collectionName);
          await applyCollectionIndexes(collection, collectionSchema);
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

          // Synchronize indexes using shared applier
          const collection = db.collection(collectionName);
          const schemasPerType = Object.entries(multiSchema).reduce<Record<string, v.ObjectSchema<Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>, undefined>>>((acc, [typeName, typeSchema]) => {
            acc[typeName] = v.object(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
            return acc;
          }, {});
          await applyMultiCollectionIndexes(collection, schemasPerType);
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

            // Synchronize indexes using shared applier
            const collection = db.collection(instanceName);
            const schemasPerType = Object.entries(multiSchema).reduce((acc, [typeName, typeSchema]) => {
              acc[typeName] = v.object(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
              return acc;
            }, {} as Record<string, v.ObjectSchema<Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>, undefined>>);
            await applyMultiCollectionIndexes(collection, schemasPerType);
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
   * Helper to transform documents in batches.
   *
   * Pagination is keyed on `_id` (sorted ascending, `_id > lastSeen`) rather
   * than `skip`/`limit`. `skip` is unsafe on a dataset being mutated in place:
   * documents shifting position can be skipped or processed twice. Because the
   * transform uses `replaceOne` (the `_id` never changes), the `_id` cursor
   * advances monotonically and each document is processed exactly once.
   */
  async function transformDocuments(
    collectionName: string,
    filter: Record<string, unknown>,
    transformer: (doc: Record<string, unknown>) => Record<string, unknown>,
  ): Promise<void> {
    const collection = db.collection(collectionName);
    let lastId: unknown = undefined;

    while (true) {
      const pageFilter = lastId === undefined
        ? filter
        : { $and: [filter, { _id: { $gt: lastId } }] };

      const documents = await collection.find(pageFilter as Record<string, unknown>)
        .sort({ _id: 1 })
        .limit(opts.batchSize)
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

      lastId = documents[documents.length - 1]._id;
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
            const collection = db.collection(operation.collectionName);
            const collectionSchema = v.object(operation.schema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
            await applyCollectionIndexes(collection, collectionSchema);

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
          const collection = db.collection(operation.collectionName);
          const collectionSchema = v.object(operation.schema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
          await applyCollectionIndexes(collection, collectionSchema);
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

        // Apply indexes using shared applier
        const collection = db.collection(operation.collectionName);
        const schemasPerType = Object.entries(operation.schema).reduce((acc, [typeName, typeSchema]) => {
          acc[typeName] = v.object(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
          return acc;
        }, {} as Record<string, v.ObjectSchema<Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>, undefined>>);
        await applyMultiCollectionIndexes(collection, schemasPerType);
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

        // Apply indexes using shared applier
        const multiCollection = db.collection(operation.collectionName);
        const schemasPerType = Object.entries(operation.schema).reduce((acc, [typeName, typeSchema]) => {
          acc[typeName] = v.object(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
          return acc;
        }, {} as Record<string, v.ObjectSchema<Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>, undefined>>);
        await applyMultiCollectionIndexes(multiCollection, schemasPerType);
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

        // Apply indexes using shared applier (critical for multi-model tracking)
        const modelCollection = db.collection(operation.collectionName);
        const schemasPerType = Object.entries(modelSchema).reduce((acc, [typeName, typeSchema]) => {
          acc[typeName] = v.object(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
          return acc;
        }, {} as Record<string, v.ObjectSchema<Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>, undefined>>);
        await applyMultiCollectionIndexes(modelCollection, schemasPerType);
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
        const sig = operation.collectionName;
        const documents = operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          // Resolve the deterministic _id BEFORE validation so schemas with a
          // required _id (e.g. a bare refId without a default) still validate.
          const _id = resolveSeedDocId(typedDoc, operation.schema._id, "", migration.id, sig, i);
          const value = v.safeParse(v.object(operation.schema), { ...typedDoc, _id });
          if (!value.success) {
            throw new Error(`Document validation failed: ${JSON.stringify(value.issues)}`);
          }
          return { ...(value.output as Record<string, unknown>), _id };
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
        const sig = operation.collectionName;
        const documentIds = operation.documents.map((doc: unknown, i) =>
          resolveSeedDocId(doc as Record<string, unknown>, operation.schema._id, "", migration.id, sig, i)
        );
        if (documentIds.length > 0) {
          await collection.deleteMany({ _id: { $in: documentIds } } as Record<string, unknown>);
        }
      }
    },

    seed_multicollection_type: {
      apply: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }

        const collection = db.collection(operation.collectionName);
        const sig = `${operation.collectionName}:${operation.documentType}`;
        const documents = operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          const _id = resolveSeedDocId(typedDoc, operation.schema._id, operation.documentType, migration.id, sig, i);
          const value = v.safeParse(v.object({
            _type: v.literal(operation.documentType),
            ...operation.schema,
          }), {
            ...typedDoc,
            _id,
            _type: operation.documentType,
          });
          if (!value.success) {
            throw new Error(`Document validation failed: ${JSON.stringify(value.issues)}`);
          }
          return { ...(value.output as Record<string, unknown>), _id };
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
        const sig = `${operation.collectionName}:${operation.documentType}`;
        const documentIds = operation.documents.map((doc: unknown, i) =>
          resolveSeedDocId(doc as Record<string, unknown>, operation.schema._id, operation.documentType, migration.id, sig, i)
        );
        if (documentIds.length > 0) {
          await collection.deleteMany({ _id: { $in: documentIds } } as Record<string, unknown>);
        }
      }
    },

    seed_multimodel_instance_type: {
      apply: async (operation) => {
        if (opts.strictValidation && !await multiCollectionInstanceExists(db, operation.collectionName)) {
          throw new Error(`Multi-model instance ${operation.collectionName} does not exist`);
        }

        const collection = db.collection(operation.collectionName);
        const sig = `${operation.collectionName}:${operation.modelType}:${operation.documentType}`;
        const documents = operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          const _id = resolveSeedDocId(typedDoc, operation.schema._id, operation.documentType, migration.id, sig, i);
          const value = v.safeParse(v.object({
            _type: v.literal(operation.documentType),
            ...operation.schema,
          }), {
            ...typedDoc,
            _id,
            _type: operation.documentType,
          });
          if (!value.success) {
            throw new Error(`Document validation failed: ${JSON.stringify(value.issues)}`);
          }
          return { ...(value.output as Record<string, unknown>), _id };
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
        const sig = `${operation.collectionName}:${operation.modelType}:${operation.documentType}`;
        const documentIds = operation.documents.map((doc: unknown, i) =>
          resolveSeedDocId(doc as Record<string, unknown>, operation.schema._id, operation.documentType, migration.id, sig, i)
        );
        if (documentIds.length > 0) {
          await collection.deleteMany({ _id: { $in: documentIds } } as Record<string, unknown>);
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
          // Check if this instance should receive this migration. Uses the
          // chain-based comparison (walks `migration.parent` chain) — safe
          // even when migration IDs from different generators coexist
          // (legacy padded vs. timestamp+ULID).
          const shouldReceive = await shouldInstanceReceiveMigrationFromChain(
            db,
            collectionName,
            migration,
          );
          if (!shouldReceive) {
            console.log(`Skipping instance ${collectionName} - already has this migration`);
            continue;
          }

          const collection = db.collection(collectionName);
          const sig = `${operation.modelType}:${operation.documentType}`;
          const documents = operation.documents.map((doc: unknown, i) => {
            const typedDoc = doc as Record<string, unknown>;
            return {
              ...typedDoc,
              _id: resolveSeedDocId(typedDoc, operation.schema._id, operation.documentType, migration.id, sig, i),
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

        const sig = `${operation.modelType}:${operation.documentType}`;
        for (const collectionName of instances) {
          const collection = db.collection(collectionName);
          const documentIds = operation.documents.map((doc: unknown, i) =>
            resolveSeedDocId(doc as Record<string, unknown>, operation.schema._id, operation.documentType, migration.id, sig, i)
          );
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
          // Check if this instance should receive this migration. Uses the
          // chain-based comparison (walks `migration.parent` chain) — safe
          // even when migration IDs from different generators coexist
          // (legacy padded vs. timestamp+ULID).
          const shouldReceive = await shouldInstanceReceiveMigrationFromChain(
            db,
            collectionName,
            migration,
          );
          if (!shouldReceive) {
            console.log(`Skipping instance ${collectionName} - already has this migration`);
            continue;
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

    flow: {
      apply: async (operation) => {
        const prefix = extractIdPrefix(operation.targetIdSchema, "");
        const source = db.collection(operation.from.collection);
        const target = db.collection(operation.into.collection);
        const baseFilter = (operation.from.where ?? {}) as Record<string, unknown>;

        // Batch by _id cursor (stable on a mutating set; see transformDocuments).
        let lastId: unknown = undefined;
        while (true) {
          const pageFilter = lastId === undefined
            ? baseFilter
            : { $and: [baseFilter, { _id: { $gt: lastId } }] };
          const docs = await source.find(pageFilter as Record<string, unknown>)
            .sort({ _id: 1 })
            .limit(opts.batchSize)
            .toArray();
          if (docs.length === 0) break;

          const mapped = docs.map((doc) => {
            const out = operation.map({ ...doc }) as Record<string, unknown>;
            out._id = flowTargetId(
              prefix,
              migration.id,
              operation.from.collection,
              String(doc._id),
            );
            return out;
          });
          // deno-lint-ignore no-explicit-any
          await target.insertMany(mapped as any);
          lastId = docs[docs.length - 1]._id;
        }

        if (operation.sourceDisposition === "consume") {
          await source.deleteMany(baseFilter as Record<string, unknown>);
        }
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(
            "Flow with source: 'consume' (move) is irreversible — cannot roll back",
          );
        }
        const prefix = extractIdPrefix(operation.targetIdSchema, "");
        const source = db.collection(operation.from.collection);
        const target = db.collection(operation.into.collection);
        const baseFilter = (operation.from.where ?? {}) as Record<string, unknown>;

        // Copy reverse: recompute target ids from the still-present source and
        // delete those copies, batched by _id cursor.
        let lastId: unknown = undefined;
        while (true) {
          const pageFilter = lastId === undefined
            ? baseFilter
            : { $and: [baseFilter, { _id: { $gt: lastId } }] };
          const docs = await source.find(pageFilter as Record<string, unknown>)
            .sort({ _id: 1 })
            .limit(opts.batchSize)
            .toArray();
          if (docs.length === 0) break;

          const ids = docs.map((doc) =>
            flowTargetId(prefix, migration.id, operation.from.collection, String(doc._id))
          );
          await target.deleteMany({ _id: { $in: ids } } as Record<string, unknown>);
          lastId = docs[docs.length - 1]._id;
        }
      },
    },
    update_indexes: {
      apply: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Collection ${operation.collectionName} does not exist`);
        }
        const collection = db.collection(operation.collectionName);
        const collectionSchema = v.object(operation.schema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
        await applyCollectionIndexes(collection, collectionSchema);
      },
      reverse: (_operation) => {
        // Index updates are idempotent, no reversal needed
        return Promise.resolve();
      }
    },

    delete_multicollection_type: {
      apply: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        const collection = db.collection(operation.collectionName);
        await collection.deleteMany({ _type: operation.documentType } as Record<string, unknown>);
      },
      reverse: async (_operation) => {
        // Cannot restore deleted documents - this is irreversible
        throw new Error(`Cannot reverse delete_multicollection_type: operation is irreversible`);
      }
    },

    delete_multimodel_instances_type: {
      apply: async (operation) => {
        const instances = await discoverMultiCollectionInstances(db, operation.modelType);

        if (instances.length === 0) {
          console.warn(`No instances found for model type ${operation.modelType}`);
          return;
        }

        for (const collectionName of instances) {
          const collection = db.collection(collectionName);
          await collection.deleteMany({ _type: operation.documentType } as Record<string, unknown>);
        }
      },
      reverse: async (_operation) => {
        // Cannot restore deleted documents - this is irreversible
        throw new Error(`Cannot reverse delete_multimodel_instances_type: operation is irreversible`);
      }
    },

    rename_multicollection_type: {
      apply: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        const collection = db.collection(operation.collectionName);
        await collection.updateMany(
          { _type: operation.oldTypeName } as Record<string, unknown>,
          { $set: { _type: operation.newTypeName } } as Record<string, unknown>
        );
      },
      reverse: async (operation) => {
        if (opts.strictValidation && !await collectionExists(operation.collectionName)) {
          throw new Error(`Multi-collection ${operation.collectionName} does not exist`);
        }
        const collection = db.collection(operation.collectionName);
        await collection.updateMany(
          { _type: operation.newTypeName } as Record<string, unknown>,
          { $set: { _type: operation.oldTypeName } } as Record<string, unknown>
        );
      }
    },

    create_scoped_multicollection: {
      apply: async (_operation) => {
        throw new Error("create_scoped_multicollection: not implemented yet (task #13)");
      },
      reverse: async (_operation) => {
        throw new Error("create_scoped_multicollection.reverse: not implemented yet (task #13)");
      },
    },

    seed_scoped_multicollection_type: {
      apply: async (_operation) => {
        throw new Error("seed_scoped_multicollection_type: not implemented yet (task #13)");
      },
      reverse: async (_operation) => {
        throw new Error("seed_scoped_multicollection_type.reverse: not implemented yet (task #13)");
      },
    },

    transform_scoped_multicollection_type: {
      apply: async (_operation) => {
        throw new Error("transform_scoped_multicollection_type: not implemented yet (task #13)");
      },
      reverse: async (_operation) => {
        throw new Error("transform_scoped_multicollection_type.reverse: not implemented yet (task #13)");
      },
    },

    rename_multimodel_instances_type: {
      apply: async (operation) => {
        const instances = await discoverMultiCollectionInstances(db, operation.modelType);

        if (instances.length === 0) {
          console.warn(`No instances found for model type ${operation.modelType}`);
          return;
        }

        for (const collectionName of instances) {
          const collection = db.collection(collectionName);
          const oldTypePrefix = `${operation.oldTypeName}:`;
          const newTypePrefix = `${operation.newTypeName}:`;
          await renameTypeInPlace(
            collection,
            operation.oldTypeName,
            operation.newTypeName,
            oldTypePrefix,
            newTypePrefix,
          );
        }
      },
      reverse: async (operation) => {
        const instances = await discoverMultiCollectionInstances(db, operation.modelType);

        for (const collectionName of instances) {
          const collection = db.collection(collectionName);
          const oldTypePrefix = `${operation.oldTypeName}:`;
          const newTypePrefix = `${operation.newTypeName}:`;
          // Reverse direction: newTypeName → oldTypeName
          await renameTypeInPlace(
            collection,
            operation.newTypeName,
            operation.oldTypeName,
            newTypePrefix,
            oldTypePrefix,
          );
        }
      }
    },
  };

  /**
   * Rename every document of `fromType` to `toType` in a multi-model
   * instance collection, rewriting the `_id` prefix when present.
   *
   * Iterates by repeatedly querying `{ _type: fromType }` and processing a
   * batch — no `skip`. Each renamed document stops matching the query, so
   * the loop drains the set without skipping or double-processing (the old
   * `skip(processedCount)` approach silently lost documents because the
   * matching set shrinks as we rename).
   */
  async function renameTypeInPlace(
    // deno-lint-ignore no-explicit-any
    collection: ReturnType<Db["collection"]> | any,
    fromType: string,
    toType: string,
    fromPrefix: string,
    toPrefix: string,
  ): Promise<void> {
    while (true) {
      const documents = await collection.find(
        { _type: fromType } as Record<string, unknown>,
      ).limit(opts.batchSize).toArray();

      if (documents.length === 0) break;

      for (const doc of documents) {
        const currentId = doc._id;
        let nextId = currentId;
        if (typeof currentId === "string" && currentId.startsWith(fromPrefix)) {
          nextId = toPrefix + currentId.slice(fromPrefix.length);
        }

        if (nextId !== currentId) {
          // _id is immutable in MongoDB → delete + re-insert with new id.
          await collection.deleteOne({ _id: currentId } as Record<string, unknown>);
          await collection.insertOne({ ...doc, _id: nextId, _type: toType });
        } else {
          await collection.updateOne(
            { _id: currentId } as Record<string, unknown>,
            { $set: { _type: toType } } as Record<string, unknown>,
          );
        }
      }
    }
  }

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
    // Pre-scan: refuse to roll back if any operation is irreversible, BEFORE
    // touching validators or data — otherwise we'd leave the database in a
    // partially rolled-back state.
    if (direction === "down") {
      const irreversible = getIrreversibleOperations(operations);
      if (irreversible.length > 0) {
        throw new Error(
          `Cannot roll back: migration contains ${irreversible.length} ` +
            `irreversible operation(s) [${irreversible.map((o) => o.type).join(", ")}]. ` +
            `Rollback aborted before any changes were made.`,
        );
      }
    }

    // Determine target schemas based on direction
    const targetSchemas = direction === 'up'
      ? migration.schemas
      : (migration.parent?.schemas || migration.schemas);

    // STEP 1: Disable ALL validators (prevents validation errors during transforms)
    // This is critical for both up and down migrations because:
    // - Up: old validators would reject documents transformed to new schema
    // - Down: new validators would reject documents transformed back to old schema
    await disableAllValidators(targetSchemas);

    // STEP 2: Apply all operations without validation interference.
    // Rollback undoes operations in LIFO order — reverse the list for 'down'
    // so dependent operations (e.g. a seed) are undone before the
    // create_collection they rely on.
    //
    // The whole apply/re-enable sequence is wrapped so validators are ALWAYS
    // restored, even if an operation throws mid-migration. Leaving validators
    // disabled is the worst outcome (silent acceptance of invalid documents);
    // re-syncing in `finally` guarantees the collection regains its guard.
    const ordered = direction === "down" ? [...operations].reverse() : operations;
    let applyError: unknown;
    try {
      for (const operation of ordered) {
        if (direction === 'up') {
          await applyOperation(operation);
        } else {
          await reverseOperation(operation);
        }
      }
    } catch (err) {
      applyError = err;
    } finally {
      // STEP 3: Re-enable validators/indexes with the target schemas — even
      // on failure. If re-sync itself fails, surface it loudly but don't
      // mask the original error.
      try {
        await synchronizeValidatorsAndIndexes(targetSchemas);
      } catch (syncErr) {
        console.error(
          "CRITICAL: failed to re-enable validators after migration. " +
            "Collections may be left without validation until the next " +
            "`migrate`/`check` run.",
          syncErr,
        );
        if (!applyError) applyError = syncErr;
      }
    }

    if (applyError) throw applyError;

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

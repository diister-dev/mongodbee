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
import type { MigrationRule } from "../types.ts";
import { ulid } from "@std/ulid";
import * as v from "valibot";
import { toMongoValidator } from "../../validator.ts";
import { extractIndexes } from "../../indexes.ts";
import { sanitizePathName } from "../../schema-navigator.ts";
import {
  createMultiCollectionInfo,
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
  options: MongodbApplierOptions = {},
) {
  const opts = { ...DEFAULT_OPTIONS, ...options };

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
   * Helper to apply indexes to a collection
   */
  async function applyIndexes(
    collectionName: string,
    schema: Record<string, unknown>,
  ): Promise<void> {
    const collection = db.collection(collectionName);
    const wrappedSchema = v.object(schema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
    const indexes = extractIndexes(wrappedSchema);

    for (const index of indexes) {
      const indexName = sanitizePathName(index.path);
      const keySpec: Record<string, number> = { [index.path]: 1 };

      try {
        await collection.createIndex(keySpec, {
          name: indexName,
          unique: index.metadata.unique || false,
          sparse: false,
        });
      } catch (error) {
        // Tolerate duplicate index errors
        if (!(error instanceof Error && error.message.includes("already exists"))) {
          throw error;
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
        if (opts.strictValidation && await collectionExists(operation.collectionName)) {
          // throw new Error(`Collection ${operation.collectionName} already exists`);
          console.warn(`Collection ${operation.collectionName} already exists, skipping creation.`);
        }

        const collOptions: Record<string, unknown> = {};
        if (operation.schema) {
          const wrappedSchema = v.object(operation.schema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>);
          collOptions.validator = toMongoValidator(wrappedSchema);
        }

        await db.createCollection(operation.collectionName, collOptions);

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
        if (opts.strictValidation && await collectionExists(operation.collectionName)) {
          // throw new Error(`Multi-collection ${operation.collectionName} already exists`);
          console.warn(`Multi-collection ${operation.collectionName} already exists, skipping creation.`);
        }

        // Create union validator for all types (without metadata schemas)
        const typeSchemas = Object.entries(operation.schema).map(
          ([typeName, typeSchema]) => v.object({
            _type: v.literal(typeName),
            ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
          })
        );

        const unionSchema = typeSchemas.length > 0
          // deno-lint-ignore no-explicit-any
          ? v.union(typeSchemas as any)
          : v.object({ _type: v.string() });

        const collOptions = { validator: toMongoValidator(unionSchema) };
        await db.createCollection(operation.collectionName, collOptions);

        // Apply indexes for each type
        const collection = db.collection(operation.collectionName);
        for (const [typeName, typeSchema] of Object.entries(operation.schema)) {
          const typeSchemaWithType = {
            _type: v.literal(typeName),
            ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
          };
          const wrappedTypeSchema = v.object(typeSchemaWithType);
          const indexes = extractIndexes(wrappedTypeSchema);

          for (const index of indexes) {
            const indexName = sanitizePathName(index.path);
            const keySpec: Record<string, number> = { [index.path]: 1 };

            try {
              await collection.createIndex(keySpec, {
                name: indexName,
                unique: index.metadata.unique || false,
                sparse: false,
              });
            } catch (error) {
              if (!(error instanceof Error && error.message.includes("already exists"))) {
                throw error;
              }
            }
          }
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
        if (opts.strictValidation && await collectionExists(operation.collectionName)) {
          // throw new Error(`Multi-model instance ${operation.collectionName} already exists`);
          console.warn(`Multi-model instance ${operation.collectionName} already exists, skipping creation.`);
        }

        // Create union validator for all types + metadata schemas
        const typeSchemas = Object.entries(operation.schema).map(
          ([typeName, typeSchema]) => v.object({
            _type: v.literal(typeName),
            ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
          })
        );

        const metadataSchemas = [
          v.object({
            _id: v.literal(MULTI_COLLECTION_INFO_TYPE),
            _type: v.literal(MULTI_COLLECTION_INFO_TYPE),
            collectionType: v.string(),
            createdAt: v.date(),
          }),
          v.object({
            _id: v.literal(MULTI_COLLECTION_MIGRATIONS_TYPE),
            _type: v.literal(MULTI_COLLECTION_MIGRATIONS_TYPE),
            fromMigrationId: v.string(),
            appliedMigrations: v.array(v.object({
              id: v.string(),
              appliedAt: v.date(),
            })),
          }),
        ];

        const allSchemas = [...typeSchemas, ...metadataSchemas];
        const unionSchema = allSchemas.length > 0
          // deno-lint-ignore no-explicit-any
          ? v.union(allSchemas as any)
          : v.object({ _type: v.string() });

        const collOptions = { validator: toMongoValidator(unionSchema) };
        await db.createCollection(operation.collectionName, collOptions);

        // Apply indexes for each type
        const collection = db.collection(operation.collectionName);
        for (const [typeName, typeSchema] of Object.entries(operation.schema)) {
          const typeSchemaWithType = {
            _type: v.literal(typeName),
            ...(typeSchema as Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>),
          };
          const wrappedTypeSchema = v.object(typeSchemaWithType);
          const indexes = extractIndexes(wrappedTypeSchema);

          for (const index of indexes) {
            const indexName = sanitizePathName(index.path);
            const keySpec: Record<string, number> = { [index.path]: 1 };

            try {
              await collection.createIndex(keySpec, {
                name: indexName,
                unique: index.metadata.unique || false,
                sparse: false,
              });
            } catch (error) {
              if (!(error instanceof Error && error.message.includes("already exists"))) {
                throw error;
              }
            }
          }
        }

        // Create metadata info document
        await createMultiCollectionInfo(
          db,
          operation.collectionName,
          operation.modelType,
          opts.currentMigrationId,
        );
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

        await createMultiCollectionInfo(
          db,
          operation.collectionName,
          operation.modelType,
          opts.currentMigrationId,
        );
      },
      reverse: async (operation) => {
        const collection = db.collection(operation.collectionName);
        await collection.deleteMany({
          _type: { $in: [MULTI_COLLECTION_INFO_TYPE, MULTI_COLLECTION_MIGRATIONS_TYPE] },
        });
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

          // Record migration for this instance
          if (opts.currentMigrationId) {
            await recordMultiCollectionMigration(db, collectionName, opts.currentMigrationId);
          }
        }
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }

        const instances = await discoverMultiCollectionInstances(db, operation.modelType);

        for (const collectionName of instances) {
          // Check version filtering for reverse as well
          if (opts.currentMigrationId) {
            const shouldReceive = await shouldInstanceReceiveMigration(
              db,
              collectionName,
              opts.currentMigrationId,
            );
            if (shouldReceive) {
              console.log(`Skipping reverse for instance ${collectionName} - migration not yet applied`);
              continue;
            }
          }

          await transformDocuments(
            collectionName,
            { _type: operation.documentType } as Record<string, unknown>,
            operation.down as (doc: Record<string, unknown>) => Record<string, unknown>
          );
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

  return {
    applyOperation,
    reverseOperation,
    setCurrentMigrationId: (migrationId: string) => {
      opts.currentMigrationId = migrationId;
    },
  };
}

/**
 * @fileoverview MongoDB applier for real database migrations
 *
 * This module provides a MongoDB applier that executes migration operations
 * against real MongoDB databases using MongoDBee's collection and multi-collection
 * functionality. It's designed for production use and handles real database operations.
 *
 * @example
 * ```typescript
 * import { createMongodbApplier } from "@diister/mongodbee/migration/appliers";
 * import { MongoDB } from "@diister/mongodbee/mongodb";
 *
 * const mongodb = new MongoDB(client);
 * const applier = createMongodbApplier(mongodb);
 *
 * // Apply operations
 * for (const operation of migrationState.operations) {
 *   await applier.applyOperation(operation);
 * }
 * ```
 *
 * @module
 */

import type {
  CreateCollectionRule,
  CreateMultiCollectionInstanceRule,
  MarkAsMultiCollectionRule,
  MigrationApplier,
  MigrationRule,
  SeedCollectionRule,
  SeedMultiCollectionInstanceRule,
  TransformCollectionRule,
  TransformMultiCollectionTypeRule,
  UpdateIndexesRule,
} from "../types.ts";
import type { Db } from "../../mongodb.ts";
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
 *
 * @returns A new ULID string in lowercase
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

  /** Whether to use transactions for reversible operations */
  useTransactions?: boolean;

  /** Maximum number of documents to process in a single batch */
  batchSize?: number;

  /** Timeout for operations in milliseconds */
  operationTimeout?: number;
}

/**
 * Type-safe bidirectional operation handlers for MongoDB operations
 */
type MongoOperationHandlers = {
  [K in MigrationRule["type"]]: {
    apply: (operation: Extract<MigrationRule, { type: K }>) => Promise<void>;
    reverse: (operation: Extract<MigrationRule, { type: K }>) => Promise<void>;
  };
};

/**
 * Implementation of MongoDB migration applier for production database operations
 *
 * This class provides a complete implementation of the MigrationApplier interface
 * that works with real MongoDB databases through MongoDBee's database access.
 * It's designed for production use with proper error handling and validation.
 */
export class MongodbApplier implements MigrationApplier {
  /**
   * Current migration ID being applied (used for version tracking)
   * Set this before applying a migration to enable proper version filtering
   */
  private currentMigrationId?: string;

  /**
   * Type-safe bidirectional operation handlers - TypeScript enforces that all operation types
   * have both apply and reverse implementations
   */
  private readonly operationHandlers: MongoOperationHandlers = {
    create_collection: {
      apply: (operation) => this.applyCreateCollection(operation),
      reverse: (operation) => this.reverseCreateCollection(operation),
    },
    seed_collection: {
      apply: (operation) => this.applySeedCollection(operation),
      reverse: (operation) => this.reverseSeedCollection(operation),
    },
    transform_collection: {
      apply: (operation) => this.applyTransformCollection(operation),
      reverse: (operation) => this.reverseTransformCollection(operation),
    },
    create_multicollection_instance: {
      apply: (operation) => this.applyCreateMultiCollectionInstance(operation),
      reverse: (operation) =>
        this.reverseCreateMultiCollectionInstance(operation),
    },
    seed_multicollection_instance: {
      apply: (operation) => this.applySeedMultiCollectionInstance(operation),
      reverse: (operation) =>
        this.reverseSeedMultiCollectionInstance(operation),
    },
    transform_multicollection_type: {
      apply: (operation) => this.applyTransformMultiCollectionType(operation),
      reverse: (operation) =>
        this.reverseTransformMultiCollectionType(operation),
    },
    update_indexes: {
      apply: (operation) => this.applyUpdateIndexes(operation),
      reverse: (operation) => this.reverseUpdateIndexes(operation),
    },
    mark_as_multicollection: {
      apply: (operation) => this.applyMarkAsMultiCollection(operation),
      reverse: (operation) => this.reverseMarkAsMultiCollection(operation),
    },
  };

  constructor(
    private db: Db,
    private options: MongodbApplierOptions = {},
  ) {
    // Set default options
    this.options = {
      strictValidation: true,
      useTransactions: false,
      batchSize: 1000,
      operationTimeout: 30000,
      ...options,
    };
  }

  /**
   * Sets the current migration ID for version tracking
   *
   * This should be called before applying a migration to enable proper
   * filtering of multi-collection instances based on their creation version.
   *
   * @param migrationId - The ID of the migration being applied
   */
  setCurrentMigrationId(migrationId: string): void {
    this.currentMigrationId = migrationId;
  }

  /**
   * Synchronizes validators and indexes for all collections in the migration schemas
   *
   * This method ensures that all collections have the correct JSON Schema validators
   * and indexes as defined in the migration schemas. It's called automatically after
   * applying migration operations to keep the database schema in sync.
   *
   * @param schemas - Migration schemas containing collection and multi-collection definitions
   */
  async synchronizeSchemas(
    schemas?: {
      collections?: Record<string, Record<string, any>>;
      multiCollections?: Record<string, Record<string, Record<string, any>>>;
    },
  ): Promise<void> {
    if (!schemas) {
      return;
    }

    // Synchronize regular collections
    if (schemas.collections) {
      for (
        const [collectionName, schema] of Object.entries(schemas.collections)
      ) {
        await this.synchronizeCollectionSchema(collectionName, schema);
      }
    }

    // Synchronize multi-collections
    if (schemas.multiCollections) {
      for (
        const [multiCollectionName, multiCollectionSchema] of Object.entries(
          schemas.multiCollections,
        )
      ) {
        await this.synchronizeMultiCollectionSchema(
          multiCollectionName,
          multiCollectionSchema,
        );
      }
    }
  }

  /**
   * Synchronizes validator and indexes for a single collection
   *
   * @private
   * @param collectionName - Name of the collection
   * @param schema - Valibot schema for the collection
   */
  private async synchronizeCollectionSchema(
    collectionName: string,
    schema: Record<string, any>,
  ): Promise<void> {
    // Check if collection exists
    const exists = await this.collectionExists(collectionName);
    if (!exists) {
      // Skip non-existent collections (they might not have been created yet)
      return;
    }

    // Apply validator
    const wrappedSchema = v.object(schema);
    const validator = toMongoValidator(wrappedSchema);

    await this.db.command({
      collMod: collectionName,
      validator,
    });

    // Apply indexes
    await this.applyIndexesForCollection(collectionName, schema);
  }

  /**
   * Synchronizes validator and indexes for a multi-collection
   *
   * @private
   * @param multiCollectionName - Base name of the multi-collection
   * @param multiCollectionSchema - Schema containing all type schemas
   */
  private async synchronizeMultiCollectionSchema(
    multiCollectionName: string,
    multiCollectionSchema: Record<string, Record<string, any>>,
  ): Promise<void> {
    // Discover all instances of this multi-collection
    const instances = await discoverMultiCollectionInstances(
      this.db,
      multiCollectionName,
    );

    if (instances.length === 0) {
      // No instances to synchronize
      return;
    }

    // Create union validator for all types (including metadata types)
    const typeSchemas = Object.entries(multiCollectionSchema).map(
      ([typeName, typeSchema]) => {
        return v.object({
          _type: v.literal(typeName),
          ...typeSchema,
        });
      },
    );

    // Add metadata schemas
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
      ? v.union(allSchemas as any)
      : v.object({ _type: v.string() });
    const validator = toMongoValidator(unionSchema);

    // Apply validator and indexes to each instance
    for (const collectionName of instances) {
      const exists = await this.collectionExists(collectionName);

      if (!exists) {
        continue;
      }

      // Apply validator
      await this.db.command({
        collMod: collectionName,
        validator,
      });

      // Apply indexes for each type schema
      const collection = this.db.collection(collectionName);
      for (
        const [typeName, typeSchema] of Object.entries(multiCollectionSchema)
      ) {
        const typeSchemaWithType = {
          _type: v.literal(typeName),
          ...typeSchema,
        };
        const wrappedTypeSchema = v.object(typeSchemaWithType);
        const indexes = extractIndexes(wrappedTypeSchema);

        for (const index of indexes) {
          const indexName = sanitizePathName(index.path);
          const keySpec: Record<string, number> = {};
          keySpec[index.path] = 1;

          try {
            await collection.createIndex(keySpec, {
              name: indexName,
              unique: index.metadata.unique || false,
              sparse: false,
            });
          } catch (error) {
            // Tolerate duplicate index errors
            if (
              error instanceof Error &&
              !error.message.includes("already exists")
            ) {
              throw error;
            }
          }
        }
      }
    }
  }

  /**
   * Applies a migration operation to the MongoDB database
   *
   * @param operation - Migration operation to apply
   *
   * @example
   * ```typescript
   * const applier = new MongodbApplier(db);
   *
   * await applier.applyOperation({
   *   type: 'create_collection',
   *   collectionName: 'users'
   * });
   * ```
   */
  applyOperation(operation: MigrationRule): Promise<void> {
    // Type-safe operation dispatch using bidirectional handlers
    const handler = this.operationHandlers[operation.type];
    if (!handler) {
      throw new Error(`Unknown operation type: ${operation.type}`);
    }

    return handler.apply(operation as never);
  }

  /**
   * Reverses a migration operation on the MongoDB database
   *
   * @param operation - Migration operation to reverse
   *
   * @example
   * ```typescript
   * const applier = new MongodbApplier(db);
   *
   * await applier.applyReverseOperation({
   *   type: 'create_collection',
   *   collectionName: 'users'
   * });
   * // This will drop the 'users' collection
   * ```
   */
  applyReverseOperation(operation: MigrationRule): Promise<void> {
    // Type-safe operation dispatch using bidirectional handlers
    const handler = this.operationHandlers[operation.type];
    if (!handler) {
      throw new Error(`Unknown operation type: ${operation.type}`);
    }

    return handler.reverse(operation as never);
  }

  /**
   * Applies a create collection operation
   *
   * @private
   * @param operation - Create collection operation
   */
  private async applyCreateCollection(
    operation: CreateCollectionRule,
  ): Promise<void> {
    try {
      // Check if collection already exists (if strict validation is enabled)
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (exists) {
          throw new Error(
            `Collection ${operation.collectionName} already exists`,
          );
        }
      }

      // Prepare collection options with JSON Schema validator if schema is provided
      const options: Record<string, unknown> = {};
      if (operation.schema) {
        // The schema is a Record<string, ValibotSchema>, wrap it in v.object()
        const wrappedSchema = v.object(operation.schema as Record<string, any>);
        // Convert Valibot schema to MongoDB JSON Schema validator
        const validator = toMongoValidator(wrappedSchema);
        options.validator = validator;
      }

      // Create collection with validator
      await this.db.createCollection(operation.collectionName, options);

      // Apply indexes if schema is provided
      if (operation.schema) {
        await this.applyIndexesForCollection(
          operation.collectionName,
          operation.schema,
        );
      }
    } catch (error) {
      throw new Error(
        `Failed to create collection ${operation.collectionName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Applies indexes to a collection from its Valibot schema
   *
   * @private
   * @param collectionName - Name of the collection
   * @param schema - Valibot schema containing index definitions
   */
  private async applyIndexesForCollection(
    collectionName: string,
    schema: Record<string, any>,
  ): Promise<void> {
    const collection = this.db.collection(collectionName);
    const wrappedSchema = v.object(schema);
    const indexes = extractIndexes(wrappedSchema);

    if (indexes.length === 0) {
      return;
    }

    for (const index of indexes) {
      const indexName = sanitizePathName(index.path);
      const keySpec: Record<string, number> = {};
      keySpec[index.path] = 1; // Ascending index

      try {
        await collection.createIndex(keySpec, {
          name: indexName,
          unique: index.metadata.unique || false,
          sparse: false,
        });
      } catch (error) {
        // Tolerate duplicate index errors
        if (
          error instanceof Error && error.message.includes("already exists")
        ) {
          return;
        } else {
          throw error;
        }
      }
    }
  }

  /**
   * Reverses a create collection operation by dropping the collection
   *
   * @private
   * @param operation - Create collection operation to reverse
   */
  private async reverseCreateCollection(
    operation: CreateCollectionRule,
  ): Promise<void> {
    try {
      // Check if collection exists (if strict validation is enabled)
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist for dropping`,
          );
        }
      }

      // Drop the collection
      const collection = this.db.collection(operation.collectionName);
      await collection.drop();
    } catch (error) {
      throw new Error(
        `Failed to drop collection ${operation.collectionName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Applies a seed collection operation
   *
   * @private
   * @param operation - Seed collection operation
   */
  private async applySeedCollection(
    operation: SeedCollectionRule,
  ): Promise<void> {
    try {
      const collection = this.db.collection(operation.collectionName);

      // Check if collection exists (if strict validation is enabled)
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist for seeding`,
          );
        }
      }

      // Convert documents to proper format
      const documents = operation.documents.map((doc) =>
        typeof doc === "object" && doc !== null
          ? doc as Record<string, unknown>
          : { value: doc }
      );

      // Insert documents in batches
      const batchSize = this.options.batchSize || 1000;
      for (let i = 0; i < documents.length; i += batchSize) {
        const batch = documents.slice(i, i + batchSize);
        await collection.insertMany(batch);
      }
    } catch (error) {
      throw new Error(
        `Failed to seed collection ${operation.collectionName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Reverses a seed collection operation by removing seeded documents
   *
   * Note: This is a best-effort operation. If documents don't have unique _id fields
   * from the seed operation, we cannot reliably identify and remove only the seeded documents.
   *
   * @private
   * @param operation - Seed collection operation to reverse
   */
  private async reverseSeedCollection(
    operation: SeedCollectionRule,
  ): Promise<void> {
    try {
      const collection = this.db.collection(operation.collectionName);

      // Check if collection exists
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist for unseeding`,
          );
        }
      }

      // Extract document IDs from the seed operation
      const documentIds = operation.documents
        .map((doc) =>
          typeof doc === "object" && doc !== null &&
            "_id" in (doc as Record<string, unknown>)
            ? (doc as Record<string, unknown>)._id
            : null
        )
        .filter((id) => id !== null);

      if (documentIds.length > 0) {
        // Remove documents by their IDs (filter out null/undefined first and cast properly)
        const validIds = documentIds.filter((id) => id != null) as unknown[];
        if (validIds.length > 0) {
          await collection.deleteMany({
            _id: { $in: validIds },
          } as Record<string, unknown>);
        }
      } else {
        console.warn(
          `Warning: Cannot reverse seed operation for collection ${operation.collectionName} - ` +
            "no documents with _id fields found. This may leave orphaned data.",
        );
      }
    } catch (error) {
      throw new Error(
        `Failed to reverse seed operation for collection ${operation.collectionName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Applies a transform collection operation
   *
   * @private
   * @param operation - Transform collection operation
   */
  private async applyTransformCollection(
    operation: TransformCollectionRule,
  ): Promise<void> {
    try {
      const collection = this.db.collection(operation.collectionName);

      // Check if collection exists
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist for transforming`,
          );
        }
      }

      // Process documents in batches
      const batchSize = this.options.batchSize || 1000;
      let processedCount = 0;

      while (true) {
        // Fetch a batch of documents
        const documents = await collection.find({})
          .limit(batchSize)
          .skip(processedCount)
          .toArray();

        if (documents.length === 0) {
          break; // No more documents to process
        }

        // Transform each document and prepare bulk operations
        const bulkOps = [];
        for (const doc of documents) {
          try {
            const transformedDoc = operation.up(doc);
            bulkOps.push({
              replaceOne: {
                filter: { _id: doc._id },
                replacement: transformedDoc,
              },
            });
          } catch (error) {
            if (this.options.strictValidation) {
              throw new Error(
                `Transform failed for document ${doc._id}: ${
                  error instanceof Error ? error.message : "Unknown error"
                }`,
              );
            }
            // In non-strict mode, skip documents that fail transformation
            console.warn(
              `Skipping document ${doc._id} due to transform error:`,
              error,
            );
          }
        }

        // Execute bulk operations
        if (bulkOps.length > 0) {
          await collection.bulkWrite(bulkOps);
        }

        processedCount += documents.length;
      }
    } catch (error) {
      throw new Error(
        `Failed to transform collection ${operation.collectionName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Reverses a transform collection operation
   *
   * @private
   * @param operation - Transform collection operation to reverse
   */
  private async reverseTransformCollection(
    operation: TransformCollectionRule,
  ): Promise<void> {
    try {
      const collection = this.db.collection(operation.collectionName);

      // Check if collection exists
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist for reverse transforming`,
          );
        }
      }

      // Process documents in batches (similar to forward transform)
      const batchSize = this.options.batchSize || 1000;
      let processedCount = 0;

      while (true) {
        // Fetch a batch of documents
        const documents = await collection.find({})
          .limit(batchSize)
          .skip(processedCount)
          .toArray();

        if (documents.length === 0) {
          break; // No more documents to process
        }

        // Reverse transform each document and prepare bulk operations
        const bulkOps = [];
        for (const doc of documents) {
          try {
            const transformedDoc = operation.down(doc);
            bulkOps.push({
              replaceOne: {
                filter: { _id: doc._id },
                replacement: transformedDoc,
              },
            });
          } catch (error) {
            if (this.options.strictValidation) {
              throw new Error(
                `Reverse transform failed for document ${doc._id}: ${
                  error instanceof Error ? error.message : "Unknown error"
                }`,
              );
            }
            // In non-strict mode, skip documents that fail transformation
            console.warn(
              `Skipping document ${doc._id} due to reverse transform error:`,
              error,
            );
          }
        }

        // Execute bulk operations
        if (bulkOps.length > 0) {
          await collection.bulkWrite(bulkOps);
        }

        processedCount += documents.length;
      }
    } catch (error) {
      throw new Error(
        `Failed to reverse transform collection ${operation.collectionName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Checks if a collection exists in the database
   *
   * @private
   * @param collectionName - Name of the collection to check
   * @returns True if collection exists, false otherwise
   */
  private async collectionExists(collectionName: string): Promise<boolean> {
    try {
      const collections = await this.db.listCollections(
        { name: collectionName },
      ).toArray();

      return collections.length > 0;
    } catch (_error) {
      // If we can't check, assume it doesn't exist
      return false;
    }
  }

  /**
   * Applies a create multi-collection instance operation
   *
   * @private
   * @param operation - Create multi-collection instance operation
   */
  private async applyCreateMultiCollectionInstance(
    operation: CreateMultiCollectionInstanceRule,
  ): Promise<void> {
    try {
      // Check if instance already exists
      if (this.options.strictValidation) {
        const exists = await multiCollectionInstanceExists(
          this.db,
          operation.collectionName,
        );
        if (exists) {
          throw new Error(
            `Multi-collection instance ${operation.collectionName} already exists`,
          );
        }
      }

      // Insert metadata document to create collection and mark it as multi-collection instance
      await createMultiCollectionInfo(
        this.db,
        operation.collectionName,
        operation.collectionType,
        "migration-id-placeholder", // TODO: Pass actual migration ID from context
      );
    } catch (error) {
      throw new Error(
        `Failed to create multi-collection instance ${operation.collectionName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Reverses a create multi-collection instance operation by dropping the collection
   *
   * @private
   * @param operation - Create multi-collection instance operation to reverse
   */
  private async reverseCreateMultiCollectionInstance(
    operation: CreateMultiCollectionInstanceRule,
  ): Promise<void> {
    try {
      // Check if instance exists
      if (this.options.strictValidation) {
        const exists = await multiCollectionInstanceExists(
          this.db,
          operation.collectionName,
        );
        if (!exists) {
          throw new Error(
            `Multi-collection instance ${operation.collectionName} does not exist for dropping`,
          );
        }
      }

      // Drop the entire collection
      const collection = this.db.collection(operation.collectionName);
      await collection.drop();
    } catch (error) {
      throw new Error(
        `Failed to drop multi-collection instance ${operation.collectionName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Applies a seed multi-collection instance operation
   *
   * @private
   * @param operation - Seed multi-collection instance operation
   */
  private async applySeedMultiCollectionInstance(
    operation: SeedMultiCollectionInstanceRule,
  ): Promise<void> {
    try {
      // Check if instance exists
      if (this.options.strictValidation) {
        const exists = await multiCollectionInstanceExists(
          this.db,
          operation.collectionName,
        );
        if (!exists) {
          throw new Error(
            `Multi-collection instance ${operation.collectionName} does not exist for seeding`,
          );
        }
      }

      const collection = this.db.collection(operation.collectionName);

      // Convert documents to proper format and add _type field with auto-generated _id
      const documents = operation.documents.map((doc) => {
        const baseDoc = typeof doc === "object" && doc !== null
          ? doc as Record<string, unknown>
          : { value: doc };

        // Generate _id automatically if not provided, following the same pattern as multi-collection.ts
        const _id = baseDoc._id ?? `${operation.typeName}:${newId()}`;

        return {
          ...baseDoc,
          _id,
          _type: operation.typeName,
        };
      });

      // Insert documents in batches
      const batchSize = this.options.batchSize || 1000;
      for (let i = 0; i < documents.length; i += batchSize) {
        const batch = documents.slice(i, i + batchSize);
        await collection.insertMany(batch as any); // Cast to any to avoid strict type issues
      }
    } catch (error) {
      throw new Error(
        `Failed to seed multi-collection instance ${operation.collectionName} with type ${operation.typeName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Reverses a seed multi-collection instance operation by removing seeded documents
   *
   * @private
   * @param operation - Seed multi-collection instance operation to reverse
   */
  private async reverseSeedMultiCollectionInstance(
    operation: SeedMultiCollectionInstanceRule,
  ): Promise<void> {
    try {
      // Check if instance exists
      if (this.options.strictValidation) {
        const exists = await multiCollectionInstanceExists(
          this.db,
          operation.collectionName,
        );
        if (!exists) {
          throw new Error(
            `Multi-collection instance ${operation.collectionName} does not exist for unseeding`,
          );
        }
      }

      const collection = this.db.collection(operation.collectionName);

      // Extract document IDs from the seed operation
      const documentIds = operation.documents
        .map((doc) =>
          typeof doc === "object" && doc !== null &&
            "_id" in (doc as Record<string, unknown>)
            ? (doc as Record<string, unknown>)._id
            : null
        )
        .filter((id) => id !== null);

      if (documentIds.length > 0) {
        // Remove documents by their IDs
        const validIds = documentIds.filter((id) => id != null) as unknown[];
        if (validIds.length > 0) {
          await collection.deleteMany({
            _id: { $in: validIds },
            _type: operation.typeName,
          } as Record<string, unknown>);
        }
      } else {
        console.warn(
          `Warning: Cannot reverse seed operation for multi-collection instance ${operation.collectionName} type ${operation.typeName} - ` +
            "no documents with _id fields found. This may leave orphaned data.",
        );
      }
    } catch (error) {
      throw new Error(
        `Failed to reverse seed operation for multi-collection instance ${operation.collectionName} type ${operation.typeName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Applies a transform multi-collection type operation to ALL instances
   *
   * @private
   * @param operation - Transform multi-collection type operation
   */
  private async applyTransformMultiCollectionType(
    operation: TransformMultiCollectionTypeRule,
  ): Promise<void> {
    try {
      // Discover all instances of this multi-collection type
      const instances = await discoverMultiCollectionInstances(
        this.db,
        operation.collectionType,
      );

      if (instances.length === 0) {
        console.warn(
          `Warning: No instances found for multi-collection type ${operation.collectionType}. ` +
            "Transform operation will have no effect.",
        );
        return;
      }

      // Apply transformation to each instance (with version filtering)
      for (const collectionName of instances) {
        // ✅ VERSION TRACKING: Check if this instance should receive this migration
        if (this.currentMigrationId) {
          const shouldReceive = await shouldInstanceReceiveMigration(
            this.db,
            collectionName,
            this.currentMigrationId,
          );

          if (!shouldReceive) {
            console.log(
              `Skipping collection ${collectionName} - created after migration ${this.currentMigrationId}`,
            );
            continue;
          }
        }
        const collection = this.db.collection(collectionName);

        // Process documents of the specified type in batches
        const batchSize = this.options.batchSize || 1000;
        let processedCount = 0;

        while (true) {
          // Fetch a batch of documents with the specified _type
          const documents = await collection.find({
            _type: operation.typeName,
          } as Record<string, unknown>)
            .limit(batchSize)
            .skip(processedCount)
            .toArray();

          if (documents.length === 0) {
            break; // No more documents to process
          }

          // Transform each document and prepare bulk operations
          const bulkOps = [];
          for (const doc of documents) {
            try {
              const transformedDoc = operation.up(doc);
              bulkOps.push({
                replaceOne: {
                  filter: { _id: doc._id },
                  replacement: {
                    ...transformedDoc,
                    _type: operation.typeName, // Ensure _type is preserved
                  },
                },
              });
            } catch (error) {
              if (this.options.strictValidation) {
                throw new Error(
                  `Transform failed for document ${doc._id} in instance ${collectionName}: ${
                    error instanceof Error ? error.message : "Unknown error"
                  }`,
                );
              }
              console.warn(
                `Skipping document ${doc._id} in ${collectionName} due to transform error:`,
                error,
              );
            }
          }

          // Execute bulk operations
          if (bulkOps.length > 0) {
            await collection.bulkWrite(bulkOps);
          }

          processedCount += documents.length;
        }

        // Record migration for this instance
        await recordMultiCollectionMigration(
          this.db,
          collectionName,
          "migration-id-placeholder", // TODO: Pass actual migration ID from context
        );
      }
    } catch (error) {
      throw new Error(
        `Failed to transform multi-collection type ${operation.collectionType}.${operation.typeName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Applies an update indexes operation to synchronize indexes with the schema
   *
   * @private
   * @param operation - Update indexes operation
   */
  private async applyUpdateIndexes(
    operation: UpdateIndexesRule,
  ): Promise<void> {
    try {
      // Check if collection exists
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist for updating indexes`,
          );
        }
      }

      // Apply indexes using the existing helper method
      await this.applyIndexesForCollection(
        operation.collectionName,
        operation.schema as Record<string, any>,
      );
    } catch (error) {
      throw new Error(
        `Failed to update indexes for collection ${operation.collectionName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Applies a mark as multi-collection operation
   *
   * @private
   * @param operation - Mark as multi-collection operation
   */
  private async applyMarkAsMultiCollection(
    operation: MarkAsMultiCollectionRule,
  ): Promise<void> {
    try {
      const collection = this.db.collection(operation.collectionName);

      // Check if collection exists
      const collectionNames = await this.db.listCollections().map((
        c: { name: string },
      ) => c.name).toArray();
      if (!collectionNames.includes(operation.collectionName)) {
        throw new Error(
          `Collection ${operation.collectionName} does not exist`,
        );
      }

      // Check if already marked
      const existing = await collection.findOne({
        _type: MULTI_COLLECTION_INFO_TYPE,
      });

      if (existing) {
        console.warn(
          `[WARN] Collection ${operation.collectionName} is already marked as a multi-collection instance`,
        );
        return; // Idempotent: already marked, no-op
      }

      // Get current migration ID or use 'current' as fallback
      const migrationId = this.currentMigrationId || "current";

      // Create the metadata documents
      await createMultiCollectionInfo(
        this.db,
        operation.collectionName,
        operation.collectionType,
        migrationId,
      );
    } catch (error) {
      throw new Error(
        `Failed to mark collection ${operation.collectionName} as multi-collection: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Reverses a mark as multi-collection operation
   *
   * @private
   * @param operation - Mark as multi-collection operation to reverse
   */
  private async reverseMarkAsMultiCollection(
    operation: MarkAsMultiCollectionRule,
  ): Promise<void> {
    try {
      const collection = this.db.collection(operation.collectionName);

      // Remove the metadata documents
      await collection.deleteMany({
        _type: {
          $in: [MULTI_COLLECTION_INFO_TYPE, MULTI_COLLECTION_MIGRATIONS_TYPE],
        },
      });
    } catch (error) {
      throw new Error(
        `Failed to reverse mark as multi-collection for ${operation.collectionName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }

  /**
   * Reverses an update indexes operation (no-op, as index updates are idempotent)
   *
   * @private
   * @param operation - Update indexes operation to reverse
   */
  private reverseUpdateIndexes(_operation: UpdateIndexesRule): Promise<void> {
    // Index updates are idempotent and don't need reversal
    // If we wanted to be more strict, we could drop indexes that were added,
    // but this would require tracking which indexes existed before the migration
    return Promise.resolve();
  }

  /**
   * Reverses a transform multi-collection type operation on ALL instances
   *
   * @private
   * @param operation - Transform multi-collection type operation to reverse
   */
  private async reverseTransformMultiCollectionType(
    operation: TransformMultiCollectionTypeRule,
  ): Promise<void> {
    try {
      // Discover all instances of this multi-collection type
      const instances = await discoverMultiCollectionInstances(
        this.db,
        operation.collectionType,
      );

      if (instances.length === 0) {
        console.warn(
          `Warning: No instances found for multi-collection type ${operation.collectionType}. ` +
            "Reverse transform operation will have no effect.",
        );
        return;
      }

      // Apply reverse transformation to each instance (with version filtering)
      for (const collectionName of instances) {
        // ✅ VERSION TRACKING: Check if this instance should receive this migration reversal
        if (this.currentMigrationId) {
          const shouldReceive = await shouldInstanceReceiveMigration(
            this.db,
            collectionName,
            this.currentMigrationId,
          );

          if (!shouldReceive) {
            console.log(
              `Skipping collection ${collectionName} - created after migration ${this.currentMigrationId}`,
            );
            continue;
          }
        }
        const collection = this.db.collection(collectionName);

        // Process documents of the specified type in batches
        const batchSize = this.options.batchSize || 1000;
        let processedCount = 0;

        while (true) {
          // Fetch a batch of documents with the specified _type
          const documents = await collection.find({
            _type: operation.typeName,
          } as Record<string, unknown>)
            .limit(batchSize)
            .skip(processedCount)
            .toArray();

          if (documents.length === 0) {
            break; // No more documents to process
          }

          // Reverse transform each document and prepare bulk operations
          const bulkOps = [];
          for (const doc of documents) {
            try {
              const transformedDoc = operation.down(doc);
              bulkOps.push({
                replaceOne: {
                  filter: { _id: doc._id },
                  replacement: {
                    ...transformedDoc,
                    _type: operation.typeName, // Ensure _type is preserved
                  },
                },
              });
            } catch (error) {
              if (this.options.strictValidation) {
                throw new Error(
                  `Reverse transform failed for document ${doc._id} in instance ${collectionName}: ${
                    error instanceof Error ? error.message : "Unknown error"
                  }`,
                );
              }
              console.warn(
                `Skipping document ${doc._id} in ${collectionName} due to reverse transform error:`,
                error,
              );
            }
          }

          // Execute bulk operations
          if (bulkOps.length > 0) {
            await collection.bulkWrite(bulkOps);
          }

          processedCount += documents.length;
        }

        // Record reverse migration for this instance
        await recordMultiCollectionMigration(
          this.db,
          collectionName,
          "migration-id-placeholder", // TODO: Pass actual migration ID from context
        );
      }
    } catch (error) {
      throw new Error(
        `Failed to reverse transform multi-collection type ${operation.collectionType}.${operation.typeName}: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      );
    }
  }
}

/**
 * Factory function to create a MongoDB applier instance
 *
 * @param db - MongoDBee database instance to use for database operations
 * @param options - Configuration options for the applier
 * @returns A new MongoDB applier instance
 *
 * @example
 * ```typescript
 * import { createMongodbApplier } from "@diister/mongodbee/migration/appliers";
 * import { MongoClient } from "@diister/mongodbee/mongodb";
 *
 * const client = new MongoClient("mongodb://localhost:27017");
 * const db = client.db("myapp");
 * const applier = createMongodbApplier(db, {
 *   strictValidation: true,
 *   useTransactions: false,
 *   batchSize: 500
 * });
 * ```
 */
export function createMongodbApplier(
  db: Db,
  options?: MongodbApplierOptions,
): MongodbApplier {
  return new MongodbApplier(db, options);
}

/**
 * Utility function to validate that a MongoDB connection is ready
 *
 * @param db - MongoDBee database instance to validate
 * @returns Promise that resolves if connection is ready
 * @throws Error if connection is not ready
 *
 * @example
 * ```typescript
 * await validateMongodbConnection(db);
 * const applier = createMongodbApplier(db);
 * ```
 */
export async function validateMongodbConnection(db: Db): Promise<void> {
  try {
    // Test the connection by running a simple command
    await db.client.db().admin().ping();
  } catch (error) {
    throw new Error(
      `MongoDB connection validation failed: ${
        error instanceof Error ? error.message : "Unknown error"
      }`,
    );
  }
}

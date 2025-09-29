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
  MigrationApplier,
  MigrationRule,
  CreateCollectionRule,
  SeedCollectionRule,
  TransformCollectionRule,
  CreateMultiCollectionInstanceRule,
  SeedMultiCollectionInstanceRule,
  TransformMultiCollectionTypeRule,
} from '../types.ts';
import type { Db } from '../../mongodb.ts';
import { ulid } from "@std/ulid";
import {
  discoverMultiCollectionInstances,
  createMultiCollectionInfo,
  recordMultiCollectionMigration,
  multiCollectionInstanceExists,
} from '../multicollection-registry.ts';

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
  [K in MigrationRule['type']]: {
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
      reverse: (operation) => this.reverseCreateMultiCollectionInstance(operation),
    },
    seed_multicollection_instance: {
      apply: (operation) => this.applySeedMultiCollectionInstance(operation),
      reverse: (operation) => this.reverseSeedMultiCollectionInstance(operation),
    },
    transform_multicollection_type: {
      apply: (operation) => this.applyTransformMultiCollectionType(operation),
      reverse: (operation) => this.reverseTransformMultiCollectionType(operation),
    },
  };

  constructor(
    private db: Db,
    private options: MongodbApplierOptions = {}
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
  private async applyCreateCollection(operation: CreateCollectionRule): Promise<void> {
    try {
      // Check if collection already exists (if strict validation is enabled)
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (exists) {
          throw new Error(`Collection ${operation.collectionName} already exists`);
        }
      }

      // Create collection by accessing it (MongoDB creates collections on first write)
      const collection = this.db.collection(operation.collectionName);
      
      // Insert a dummy document and then remove it to ensure collection creation
      // This is needed because MongoDB creates collections lazily
      const result = await collection.insertOne({ _temp: true });
      if (result.insertedId) {
        await collection.deleteOne({ _id: result.insertedId });
      }
    } catch (error) {
      throw new Error(
        `Failed to create collection ${operation.collectionName}: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Reverses a create collection operation by dropping the collection
   * 
   * @private
   * @param operation - Create collection operation to reverse
   */
  private async reverseCreateCollection(operation: CreateCollectionRule): Promise<void> {
    try {
      // Check if collection exists (if strict validation is enabled)
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(`Collection ${operation.collectionName} does not exist for dropping`);
        }
      }

      // Drop the collection
      const collection = this.db.collection(operation.collectionName);
      await collection.drop();
    } catch (error) {
      throw new Error(
        `Failed to drop collection ${operation.collectionName}: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Applies a seed collection operation
   * 
   * @private
   * @param operation - Seed collection operation
   */
  private async applySeedCollection(operation: SeedCollectionRule): Promise<void> {
    try {
      const collection = this.db.collection(operation.collectionName);
      
      // Check if collection exists (if strict validation is enabled)
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(`Collection ${operation.collectionName} does not exist for seeding`);
        }
      }

      // Convert documents to proper format
      const documents = operation.documents.map(doc => 
        typeof doc === 'object' && doc !== null 
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
          error instanceof Error ? error.message : 'Unknown error'
        }`
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
  private async reverseSeedCollection(operation: SeedCollectionRule): Promise<void> {
    try {
      const collection = this.db.collection(operation.collectionName);
      
      // Check if collection exists
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(`Collection ${operation.collectionName} does not exist for unseeding`);
        }
      }

      // Extract document IDs from the seed operation
      const documentIds = operation.documents
        .map(doc => 
          typeof doc === 'object' && doc !== null && '_id' in (doc as Record<string, unknown>) 
            ? (doc as Record<string, unknown>)._id 
            : null
        )
        .filter(id => id !== null);

      if (documentIds.length > 0) {
        // Remove documents by their IDs (filter out null/undefined first and cast properly)
        const validIds = documentIds.filter(id => id != null) as unknown[];
        if (validIds.length > 0) {
          await collection.deleteMany({
            _id: { $in: validIds }
          } as Record<string, unknown>);
        }
      } else {
        console.warn(
          `Warning: Cannot reverse seed operation for collection ${operation.collectionName} - ` +
          'no documents with _id fields found. This may leave orphaned data.'
        );
      }
    } catch (error) {
      throw new Error(
        `Failed to reverse seed operation for collection ${operation.collectionName}: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Applies a transform collection operation
   * 
   * @private
   * @param operation - Transform collection operation
   */
  private async applyTransformCollection(operation: TransformCollectionRule): Promise<void> {
    try {
      const collection = this.db.collection(operation.collectionName);
      
      // Check if collection exists
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(`Collection ${operation.collectionName} does not exist for transforming`);
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
                replacement: transformedDoc
              }
            });
          } catch (error) {
            if (this.options.strictValidation) {
              throw new Error(
                `Transform failed for document ${doc._id}: ${
                  error instanceof Error ? error.message : 'Unknown error'
                }`
              );
            }
            // In non-strict mode, skip documents that fail transformation
            console.warn(`Skipping document ${doc._id} due to transform error:`, error);
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
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Reverses a transform collection operation
   * 
   * @private
   * @param operation - Transform collection operation to reverse
   */
  private async reverseTransformCollection(operation: TransformCollectionRule): Promise<void> {
    try {
      const collection = this.db.collection(operation.collectionName);
      
      // Check if collection exists
      if (this.options.strictValidation) {
        const exists = await this.collectionExists(operation.collectionName);
        if (!exists) {
          throw new Error(`Collection ${operation.collectionName} does not exist for reverse transforming`);
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
                replacement: transformedDoc
              }
            });
          } catch (error) {
            if (this.options.strictValidation) {
              throw new Error(
                `Reverse transform failed for document ${doc._id}: ${
                  error instanceof Error ? error.message : 'Unknown error'
                }`
              );
            }
            // In non-strict mode, skip documents that fail transformation
            console.warn(`Skipping document ${doc._id} due to reverse transform error:`, error);
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
          error instanceof Error ? error.message : 'Unknown error'
        }`
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
        { name: collectionName }
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
  private async applyCreateMultiCollectionInstance(operation: CreateMultiCollectionInstanceRule): Promise<void> {
    try {
      const collectionName = `${operation.multiCollectionName}_${operation.instanceName}`;

      // Check if instance already exists
      if (this.options.strictValidation) {
        const exists = await multiCollectionInstanceExists(this.db, operation.multiCollectionName, operation.instanceName);
        if (exists) {
          throw new Error(`Multi-collection instance ${collectionName} already exists`);
        }
      }

      // Insert metadata document to create collection and mark it as multi-collection instance
      await createMultiCollectionInfo(
        this.db,
        operation.multiCollectionName,
        operation.instanceName,
        'migration-id-placeholder', // TODO: Pass actual migration ID from context
        {} // Empty schemas for now, will be populated by subsequent operations
      );

    } catch (error) {
      throw new Error(
        `Failed to create multi-collection instance ${operation.multiCollectionName}_${operation.instanceName}: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Reverses a create multi-collection instance operation by dropping the collection
   *
   * @private
   * @param operation - Create multi-collection instance operation to reverse
   */
  private async reverseCreateMultiCollectionInstance(operation: CreateMultiCollectionInstanceRule): Promise<void> {
    try {
      const collectionName = `${operation.multiCollectionName}_${operation.instanceName}`;

      // Check if instance exists
      if (this.options.strictValidation) {
        const exists = await multiCollectionInstanceExists(this.db, operation.multiCollectionName, operation.instanceName);
        if (!exists) {
          throw new Error(`Multi-collection instance ${collectionName} does not exist for dropping`);
        }
      }

      // Drop the entire collection
      const collection = this.db.collection(collectionName);
      await collection.drop();

    } catch (error) {
      throw new Error(
        `Failed to drop multi-collection instance ${operation.multiCollectionName}_${operation.instanceName}: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Applies a seed multi-collection instance operation
   *
   * @private
   * @param operation - Seed multi-collection instance operation
   */
  private async applySeedMultiCollectionInstance(operation: SeedMultiCollectionInstanceRule): Promise<void> {
    try {
      const collectionName = `${operation.multiCollectionName}_${operation.instanceName}`;

      // Check if instance exists
      if (this.options.strictValidation) {
        const exists = await multiCollectionInstanceExists(this.db, operation.multiCollectionName, operation.instanceName);
        if (!exists) {
          throw new Error(`Multi-collection instance ${collectionName} does not exist for seeding`);
        }
      }

      const collection = this.db.collection(collectionName);

      // Convert documents to proper format and add _type field with auto-generated _id
      const documents = operation.documents.map(doc => {
        const baseDoc = typeof doc === 'object' && doc !== null
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
        `Failed to seed multi-collection instance ${operation.multiCollectionName}_${operation.instanceName} with type ${operation.typeName}: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Reverses a seed multi-collection instance operation by removing seeded documents
   *
   * @private
   * @param operation - Seed multi-collection instance operation to reverse
   */
  private async reverseSeedMultiCollectionInstance(operation: SeedMultiCollectionInstanceRule): Promise<void> {
    try {
      const collectionName = `${operation.multiCollectionName}_${operation.instanceName}`;

      // Check if instance exists
      if (this.options.strictValidation) {
        const exists = await multiCollectionInstanceExists(this.db, operation.multiCollectionName, operation.instanceName);
        if (!exists) {
          throw new Error(`Multi-collection instance ${collectionName} does not exist for unseeding`);
        }
      }

      const collection = this.db.collection(collectionName);

      // Extract document IDs from the seed operation
      const documentIds = operation.documents
        .map(doc =>
          typeof doc === 'object' && doc !== null && '_id' in (doc as Record<string, unknown>)
            ? (doc as Record<string, unknown>)._id
            : null
        )
        .filter(id => id !== null);

      if (documentIds.length > 0) {
        // Remove documents by their IDs
        const validIds = documentIds.filter(id => id != null) as unknown[];
        if (validIds.length > 0) {
          await collection.deleteMany({
            _id: { $in: validIds },
            _type: operation.typeName
          } as Record<string, unknown>);
        }
      } else {
        console.warn(
          `Warning: Cannot reverse seed operation for multi-collection instance ${collectionName} type ${operation.typeName} - ` +
          'no documents with _id fields found. This may leave orphaned data.'
        );
      }

    } catch (error) {
      throw new Error(
        `Failed to reverse seed operation for multi-collection instance ${operation.multiCollectionName}_${operation.instanceName} type ${operation.typeName}: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Applies a transform multi-collection type operation to ALL instances
   *
   * @private
   * @param operation - Transform multi-collection type operation
   */
  private async applyTransformMultiCollectionType(operation: TransformMultiCollectionTypeRule): Promise<void> {
    try {
      // Discover all instances of this multi-collection type
      const instances = await discoverMultiCollectionInstances(this.db, operation.multiCollectionName);

      if (instances.length === 0) {
        console.warn(
          `Warning: No instances found for multi-collection ${operation.multiCollectionName}. ` +
          'Transform operation will have no effect.'
        );
        return;
      }

      // Apply transformation to each instance
      for (const instanceName of instances) {
        const collectionName = `${operation.multiCollectionName}_${instanceName}`;
        const collection = this.db.collection(collectionName);

        // Process documents of the specified type in batches
        const batchSize = this.options.batchSize || 1000;
        let processedCount = 0;

        while (true) {
          // Fetch a batch of documents with the specified _type
          const documents = await collection.find({
            _type: operation.typeName
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
                  }
                }
              });
            } catch (error) {
              if (this.options.strictValidation) {
                throw new Error(
                  `Transform failed for document ${doc._id} in instance ${collectionName}: ${
                    error instanceof Error ? error.message : 'Unknown error'
                  }`
                );
              }
              console.warn(`Skipping document ${doc._id} in ${collectionName} due to transform error:`, error);
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
          operation.multiCollectionName,
          instanceName,
          'migration-id-placeholder' // TODO: Pass actual migration ID from context
        );
      }

    } catch (error) {
      throw new Error(
        `Failed to transform multi-collection type ${operation.multiCollectionName}.${operation.typeName}: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Reverses a transform multi-collection type operation on ALL instances
   *
   * @private
   * @param operation - Transform multi-collection type operation to reverse
   */
  private async reverseTransformMultiCollectionType(operation: TransformMultiCollectionTypeRule): Promise<void> {
    try {
      // Discover all instances of this multi-collection type
      const instances = await discoverMultiCollectionInstances(this.db, operation.multiCollectionName);

      if (instances.length === 0) {
        console.warn(
          `Warning: No instances found for multi-collection ${operation.multiCollectionName}. ` +
          'Reverse transform operation will have no effect.'
        );
        return;
      }

      // Apply reverse transformation to each instance
      for (const instanceName of instances) {
        const collectionName = `${operation.multiCollectionName}_${instanceName}`;
        const collection = this.db.collection(collectionName);

        // Process documents of the specified type in batches
        const batchSize = this.options.batchSize || 1000;
        let processedCount = 0;

        while (true) {
          // Fetch a batch of documents with the specified _type
          const documents = await collection.find({
            _type: operation.typeName
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
                  }
                }
              });
            } catch (error) {
              if (this.options.strictValidation) {
                throw new Error(
                  `Reverse transform failed for document ${doc._id} in instance ${collectionName}: ${
                    error instanceof Error ? error.message : 'Unknown error'
                  }`
                );
              }
              console.warn(`Skipping document ${doc._id} in ${collectionName} due to reverse transform error:`, error);
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
          operation.multiCollectionName,
          instanceName,
          'migration-id-placeholder' // TODO: Pass actual migration ID from context
        );
      }

    } catch (error) {
      throw new Error(
        `Failed to reverse transform multi-collection type ${operation.multiCollectionName}.${operation.typeName}: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
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
  options?: MongodbApplierOptions
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
        error instanceof Error ? error.message : 'Unknown error'
      }`
    );
  }
}
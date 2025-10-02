/**
 * @fileoverview In-memory simulation applier for migration testing and validation
 * 
 * This module provides an in-memory applier that simulates migration operations
 * without actually touching a real database. It's used for validation, testing,
 * and ensuring migrations can be reversed properly before applying them to production.
 * 
 * @example
 * ```typescript
 * import { createSimulationApplier } from "@diister/mongodbee/migration/appliers";
 * 
 * const applier = createSimulationApplier();
 * const initialState = { collections: {}, multiCollections: {} };
 * 
 * // Apply operations
 * let state = initialState;
 * for (const operation of migrationState.operations) {
 *   state = await applier.applyOperation(state, operation);
 * }
 * 
 * // Reverse operations
 * for (let i = migrationState.operations.length - 1; i >= 0; i--) {
 *   state = await applier.applyReverseOperation(state, migrationState.operations[i]);
 * }
 * ```
 * 
 * @module
 */

import type {
  MigrationRule,
  DatabaseState,
  CreateCollectionRule,
  SeedCollectionRule,
  TransformCollectionRule,
  CreateMultiCollectionInstanceRule,
  SeedMultiCollectionInstanceRule,
  TransformMultiCollectionTypeRule,
  UpdateIndexesRule,
} from '../types.ts';
import { createMockGenerator } from '@diister/valibot-mock';

/**
 * Configuration options for the simulation applier
 */
export interface SimulationApplierOptions {
  /** Whether to validate operations strictly */
  strictValidation?: boolean;
  
  /** Whether to track operation history */
  trackHistory?: boolean;
}

/**
 * Extended database state with operation history tracking
 */
export interface SimulationDatabaseState extends DatabaseState {
  /** History of applied operations (if tracking enabled) */
  operationHistory?: Array<{
    operation: MigrationRule;
    timestamp: Date;
    type: 'apply' | 'reverse';
  }>;
}

/**
 * Interface for simulation-based migration appliers that work with database state
 * 
 * Unlike regular MigrationApplier which works with real databases, this interface
 * operates on in-memory database state objects for simulation and testing.
 */
export interface SimulationMigrationApplier {
  /**
   * Applies a migration operation to a database state
   * @param state - Current database state
   * @param operation - Migration operation to apply
   * @returns Updated database state
   */
  applyOperation(state: SimulationDatabaseState, operation: MigrationRule): SimulationDatabaseState;
  
  /**
   * Reverses a migration operation on a database state
   * @param state - Current database state
   * @param operation - Migration operation to reverse
   * @returns Updated database state
   */
  applyReverseOperation(state: SimulationDatabaseState, operation: MigrationRule): SimulationDatabaseState;
}

/**
 * Type-safe bidirectional operation handlers for simulation
 */
type SimulationOperationHandlers = {
  [K in MigrationRule['type']]: {
    apply: (
      state: SimulationDatabaseState,
      operation: Extract<MigrationRule, { type: K }>
    ) => SimulationDatabaseState;
    reverse: (
      state: SimulationDatabaseState,
      operation: Extract<MigrationRule, { type: K }>
    ) => SimulationDatabaseState;
  };
};

/**
 * Generates a test document from a Valibot schema using mock data
 * Used when creating test documents for transform validation
 */
function generateTestDocumentFromSchema(schema: unknown): Record<string, unknown> {
  if (!schema) {
    // No schema provided - return minimal test document
    return {
      _id: 'test_simulation_id'
    };
  }

  try {
    // Use valibot-mock to generate realistic test data from schema
    // deno-lint-ignore no-explicit-any
    const generator = createMockGenerator(schema as any);
    const mockData = generator.generate();
    return {
      _id: 'test_simulation_id',
      ...(typeof mockData === 'object' && mockData !== null ? mockData : {})
    };
  } catch (_error) {
    // If mock generation fails, return minimal document
    return {
      _id: 'test_simulation_id'
    };
  }
}

/**
 * Implementation of in-memory migration applier for simulation and testing
 *
 * This class provides a complete implementation for working with in-memory database states,
 * making it perfect for validation and testing migration operations without affecting real databases.
 */
export class SimulationApplier implements SimulationMigrationApplier {
  /**
   * Type-safe bidirectional operation handlers - TypeScript enforces that all operation types 
   * have both apply and reverse implementations
   */
  private readonly operationHandlers: SimulationOperationHandlers = {
    create_collection: {
      apply: (state, operation) => this.applyCreateCollection(state, operation),
      reverse: (state, operation) => this.reverseCreateCollection(state, operation),
    },
    seed_collection: {
      apply: (state, operation) => this.applySeedCollection(state, operation),
      reverse: (state, operation) => this.reverseSeedCollection(state, operation),
    },
    transform_collection: {
      apply: (state, operation) => this.applyTransformCollection(state, operation),
      reverse: (state, operation) => this.reverseTransformCollection(state, operation),
    },
    create_multicollection_instance: {
      apply: (state, operation) => this.applyCreateMultiCollectionInstance(state, operation),
      reverse: (state, operation) => this.reverseCreateMultiCollectionInstance(state, operation),
    },
    seed_multicollection_instance: {
      apply: (state, operation) => this.applySeedMultiCollectionInstance(state, operation),
      reverse: (state, operation) => this.reverseSeedMultiCollectionInstance(state, operation),
    },
    transform_multicollection_type: {
      apply: (state, operation) => this.applyTransformMultiCollectionType(state, operation),
      reverse: (state, operation) => this.reverseTransformMultiCollectionType(state, operation),
    },
    update_indexes: {
      apply: (state, operation) => this.applyUpdateIndexes(state, operation),
      reverse: (state, operation) => this.reverseUpdateIndexes(state, operation),
    },
  };

  constructor(private options: SimulationApplierOptions = {}) {}

  /**
   * Applies a migration operation to the in-memory database state
   * 
   * @param state - Current database state
   * @param operation - Migration operation to apply
   * @returns Updated database state
   * 
   * @example
   * ```typescript
   * const applier = new SimulationApplier();
   * let state = { collections: {}, multiCollections: {} };
   * 
   * state = applier.applyOperation(state, {
   *   type: 'create_collection',
   *   collectionName: 'users'
   * });
   * ```
   */
  applyOperation(state: SimulationDatabaseState, operation: MigrationRule): SimulationDatabaseState {
    // Create a deep copy to avoid mutations
    const newState = this.deepClone(state);

    // Type-safe operation dispatch using bidirectional handlers
    const handler = this.operationHandlers[operation.type];
    if (!handler) {
      throw new Error(`Unknown operation type: ${operation.type}`);
    }

    return handler.apply(newState, operation as never);
  }

  /**
   * Reverses a migration operation on the in-memory database state
   * 
   * @param state - Current database state
   * @param operation - Migration operation to reverse
   * @returns Updated database state
   * 
   * @example
   * ```typescript
   * const applier = new SimulationApplier();
   * let state = { collections: { users: { content: [] } } };
   * 
   * state = applier.applyReverseOperation(state, {
   *   type: 'create_collection',
   *   collectionName: 'users'
   * });
   * // Now state.collections.users is undefined
   * ```
   */
  applyReverseOperation(state: SimulationDatabaseState, operation: MigrationRule): SimulationDatabaseState {
    // Create a deep copy to avoid mutations
    const newState = this.deepClone(state);

    // Type-safe operation dispatch using bidirectional handlers
    const handler = this.operationHandlers[operation.type];
    if (!handler) {
      throw new Error(`Unknown operation type: ${operation.type}`);
    }

    return handler.reverse(newState, operation as never);
  }

  /**
   * Applies a create collection operation
   * 
   * @private
   * @param state - Current database state
   * @param operation - Create collection operation
   * @returns Updated database state
   */
  private applyCreateCollection(state: SimulationDatabaseState, operation: CreateCollectionRule): SimulationDatabaseState {
    if (this.options.strictValidation && state.collections[operation.collectionName]) {
      throw new Error(`Collection ${operation.collectionName} already exists`);
    }

    state.collections[operation.collectionName] = { content: [] };
    this.trackOperation(state, operation, 'apply');
    
    return state;
  }

  /**
   * Reverses a create collection operation by dropping the collection
   * 
   * @private
   * @param state - Current database state
   * @param operation - Create collection operation to reverse
   * @returns Updated database state
   */
  private reverseCreateCollection(state: SimulationDatabaseState, operation: CreateCollectionRule): SimulationDatabaseState {
    if (this.options.strictValidation && !state.collections[operation.collectionName]) {
      throw new Error(`Collection ${operation.collectionName} does not exist for dropping`);
    }

    delete state.collections[operation.collectionName];
    this.trackOperation(state, operation, 'reverse');
    
    return state;
  }

  /**
   * Applies a seed collection operation
   * 
   * @private
   * @param state - Current database state
   * @param operation - Seed collection operation
   * @returns Updated database state
   */
  private applySeedCollection(state: SimulationDatabaseState, operation: SeedCollectionRule): SimulationDatabaseState {
    if (this.options.strictValidation && !state.collections[operation.collectionName]) {
      throw new Error(`Collection ${operation.collectionName} does not exist for seeding`);
    }

    // Ensure collection exists (create if not in strict mode)
    if (!state.collections[operation.collectionName]) {
      state.collections[operation.collectionName] = { content: [] };
    }

    // Add documents to the collection
    const documents = operation.documents.map(doc => 
      typeof doc === 'object' && doc !== null 
        ? { ...doc as Record<string, unknown> } 
        : doc
    );
    
    state.collections[operation.collectionName].content.push(
      ...(documents as Record<string, unknown>[])
    );
    this.trackOperation(state, operation, 'apply');
    
    return state;
  }

  /**
   * Reverses a seed collection operation by removing seeded documents
   * 
   * @private
   * @param state - Current database state
   * @param operation - Seed collection operation to reverse
   * @returns Updated database state
   */
  private reverseSeedCollection(state: SimulationDatabaseState, operation: SeedCollectionRule): SimulationDatabaseState {
    if (this.options.strictValidation && !state.collections[operation.collectionName]) {
      throw new Error(`Collection ${operation.collectionName} does not exist for unseeding`);
    }

    const collection = state.collections[operation.collectionName];
    if (!collection) return state;

    // Create a set of seeded document IDs for efficient lookup
    const seededIds = new Set(
      operation.documents
        .map(doc => 
          typeof doc === 'object' && doc !== null && '_id' in (doc as Record<string, unknown>) 
            ? (doc as Record<string, unknown>)._id 
            : null
        )
        .filter(id => id !== null)
    );

    // Filter out documents with matching IDs
    collection.content = collection.content.filter(doc => {
      const docId = typeof doc === 'object' && doc !== null && '_id' in doc ? doc._id : null;
      return !seededIds.has(docId as string | number);
    });

    this.trackOperation(state, operation, 'reverse');
    return state;
  }

  /**
   * Applies a transform collection operation
   * 
   * @private
   * @param state - Current database state
   * @param operation - Transform collection operation
   * @returns Updated database state
   */
  private applyTransformCollection(state: SimulationDatabaseState, operation: TransformCollectionRule): SimulationDatabaseState {
    if (this.options.strictValidation && !state.collections[operation.collectionName]) {
      throw new Error(`Collection ${operation.collectionName} does not exist for transforming`);
    }

    const collection = state.collections[operation.collectionName];
    if (!collection) return state;

    // Apply transformation to each document
    collection.content = collection.content.map(doc => {
      try {
        return operation.up(doc);
      } catch (error) {
        if (this.options.strictValidation) {
          throw new Error(`Transform failed for document: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
        // In non-strict mode, return original document if transform fails
        return doc;
      }
    });

    this.trackOperation(state, operation, 'apply');
    return state;
  }

  /**
   * Reverses a transform collection operation
   * 
   * @private
   * @param state - Current database state
   * @param operation - Transform collection operation to reverse
   * @returns Updated database state
   */
  private reverseTransformCollection(state: SimulationDatabaseState, operation: TransformCollectionRule): SimulationDatabaseState {
    if (this.options.strictValidation && !state.collections[operation.collectionName]) {
      throw new Error(`Collection ${operation.collectionName} does not exist for reverse transforming`);
    }

    const collection = state.collections[operation.collectionName];
    if (!collection) return state;

    // Apply reverse transformation to each document
    collection.content = collection.content.map(doc => {
      try {
        return operation.down(doc);
      } catch (error) {
        if (this.options.strictValidation) {
          throw new Error(`Reverse transform failed for document: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
        // In non-strict mode, return original document if transform fails
        return doc;
      }
    });

    this.trackOperation(state, operation, 'reverse');
    return state;
  }

  /**
   * Applies a create multi-collection instance operation
   * 
   * @private
   * @param state - Current database state
   * @param operation - Create multi-collection instance operation
   * @returns Updated database state
   */
  private applyCreateMultiCollectionInstance(state: SimulationDatabaseState, operation: CreateMultiCollectionInstanceRule): SimulationDatabaseState {
    // Initialize multiCollections if not present
    if (!state.multiCollections) {
      state.multiCollections = {};
    }

    if (this.options.strictValidation && state.multiCollections[operation.collectionName]) {
      throw new Error(`Multi-collection instance ${operation.collectionName} already exists`);
    }

    // Create the multi-collection instance with metadata documents
    state.multiCollections[operation.collectionName] = {
      content: [
        {
          _id: '_information',
          _type: '_information',
          collectionType: operation.collectionType,
          createdAt: new Date(),
        },
        {
          _id: '_migrations',
          _type: '_migrations',
          fromMigrationId: 'simulation',
          appliedMigrations: [
            {
              id: 'simulation',
              appliedAt: new Date(),
            }
          ]
        }
      ]
    };

    this.trackOperation(state, operation, 'apply');
    return state;
  }

  /**
   * Reverses a create multi-collection instance operation by removing the instance
   * 
   * @private
   * @param state - Current database state
   * @param operation - Create multi-collection instance operation to reverse
   * @returns Updated database state
   */
  private reverseCreateMultiCollectionInstance(state: SimulationDatabaseState, operation: CreateMultiCollectionInstanceRule): SimulationDatabaseState {
    if (!state.multiCollections) {
      state.multiCollections = {};
    }

    if (this.options.strictValidation && !state.multiCollections[operation.collectionName]) {
      throw new Error(`Multi-collection instance ${operation.collectionName} does not exist for dropping`);
    }

    delete state.multiCollections[operation.collectionName];
    this.trackOperation(state, operation, 'reverse');
    return state;
  }

  /**
   * Applies a seed multi-collection instance operation
   * 
   * @private
   * @param state - Current database state
   * @param operation - Seed multi-collection instance operation
   * @returns Updated database state
   */
  private applySeedMultiCollectionInstance(state: SimulationDatabaseState, operation: SeedMultiCollectionInstanceRule): SimulationDatabaseState {
    if (!state.multiCollections) {
      state.multiCollections = {};
    }

    if (this.options.strictValidation && !state.multiCollections[operation.collectionName]) {
      throw new Error(`Multi-collection instance ${operation.collectionName} does not exist for seeding`);
    }

    // Ensure collection exists (create if not in strict mode)
    if (!state.multiCollections[operation.collectionName]) {
      state.multiCollections[operation.collectionName] = { content: [] };
    }

    // Add documents to the multi-collection with _type field
    const documents = operation.documents.map(doc => {
      const docObj: Record<string, unknown> = typeof doc === 'object' && doc !== null
        ? { ...doc as Record<string, unknown> }
        : { value: doc };

      // Add _type field
      docObj._type = operation.typeName;

      // Generate _id if missing (simple simulation)
      if (!docObj._id) {
        docObj._id = `${operation.typeName}:sim_${Math.random().toString(36).substring(2)}`;
      }

      return docObj;
    });

    state.multiCollections[operation.collectionName].content.push(...documents);
    this.trackOperation(state, operation, 'apply');
    return state;
  }

  /**
   * Reverses a seed multi-collection instance operation by removing seeded documents
   * 
   * @private
   * @param state - Current database state
   * @param operation - Seed multi-collection instance operation to reverse
   * @returns Updated database state
   */
  private reverseSeedMultiCollectionInstance(state: SimulationDatabaseState, operation: SeedMultiCollectionInstanceRule): SimulationDatabaseState {
    if (!state.multiCollections) {
      return state;
    }

    const collection = state.multiCollections[operation.collectionName];
    
    if (!collection) return state;

    // Create a set of seeded document IDs for efficient lookup
    const seededIds = new Set(
      operation.documents
        .map(doc => 
          typeof doc === 'object' && doc !== null && '_id' in (doc as Record<string, unknown>) 
            ? (doc as Record<string, unknown>)._id 
            : null
        )
        .filter(id => id !== null)
    );

    // Filter out documents with matching IDs and type
    collection.content = collection.content.filter(doc => {
      const docId = typeof doc === 'object' && doc !== null && '_id' in doc ? doc._id : null;
      const docType = typeof doc === 'object' && doc !== null && '_type' in doc ? doc._type : null;
      
      // Keep documents that don't match the operation's type or aren't in the seeded IDs
      return docType !== operation.typeName || !seededIds.has(docId as string | number);
    });

    this.trackOperation(state, operation, 'reverse');
    return state;
  }

  /**
   * Applies a transform multi-collection type operation to ALL instances
   * 
   * @private
   * @param state - Current database state
   * @param operation - Transform multi-collection type operation
   * @returns Updated database state
   */
  private applyTransformMultiCollectionType(state: SimulationDatabaseState, operation: TransformMultiCollectionTypeRule): SimulationDatabaseState {
    if (!state.multiCollections) {
      return state;
    }

    // Find all instances of this multi-collection type by checking metadata
    const matchingCollections = Object.keys(state.multiCollections).filter(name => {
      const collection = state.multiCollections![name];
      const infoDoc = collection.content.find((doc: Record<string, unknown>) => doc._type === '_information');
      return infoDoc && infoDoc.collectionType === operation.collectionType;
    });

    if (matchingCollections.length === 0) {
      // No instances found - create a test instance with mock data to validate the transform
      const testInstanceName = `${operation.collectionType}_test_simulation`;
      const testDocument = {
        ...generateTestDocumentFromSchema(operation.schema),
        _type: operation.typeName,
      };

      state.multiCollections[testInstanceName] = {
        content: [{
          _id: '_information',
          _type: '_information',
          collectionType: operation.collectionType,
          createdAt: new Date(),
        }, testDocument]
      };

      // Test the transformation with the mock document
      try {
        const transformed = operation.up(testDocument);

        // If transform succeeded, update state with transformed document
        state.multiCollections[testInstanceName].content = [transformed];

        this.trackOperation(state, operation, 'apply');
        return state;
      } catch (error) {
        if (this.options.strictValidation) {
          throw new Error(`Transform validation failed on test document: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
        return state;
      }
    }

    // Apply transformation to each instance
    for (const collectionName of matchingCollections) {
      const collection = state.multiCollections[collectionName];
      if (!collection) continue;

      // Transform documents of the specified type
      collection.content = collection.content.map(doc => {
        // Only transform documents of the specified type
        if (typeof doc === 'object' && doc !== null && '_type' in doc && doc._type === operation.typeName) {
          try {
            return operation.up(doc);
          } catch (error) {
            if (this.options.strictValidation) {
              throw new Error(`Transform failed for document in ${collectionName}: ${error instanceof Error ? error.message : 'Unknown error'}`);
            }
            // In non-strict mode, return original document if transform fails
            return doc;
          }
        }
        // Return other documents unchanged
        return doc;
      });
    }

    this.trackOperation(state, operation, 'apply');
    return state;
  }

  /**
   * Reverses a transform multi-collection type operation on ALL instances
   * 
   * @private
   * @param state - Current database state
   * @param operation - Transform multi-collection type operation to reverse
   * @returns Updated database state
   */
  private reverseTransformMultiCollectionType(state: SimulationDatabaseState, operation: TransformMultiCollectionTypeRule): SimulationDatabaseState {
    if (!state.multiCollections) {
      return state;
    }

    // Find all instances of this multi-collection type by checking metadata
    const matchingCollections = Object.keys(state.multiCollections).filter(name => {
      const collection = state.multiCollections![name];
      const infoDoc = collection.content.find((doc: Record<string, unknown>) => doc._type === '_information');
      return infoDoc && infoDoc.collectionType === operation.collectionType;
    });

    if (matchingCollections.length === 0) {
      // No instances found - create a test instance with mock data to validate the reverse transform
      const testInstanceName = `${operation.collectionType}_test_simulation`;

      // First apply the forward transform to create a "new" document, then reverse it
      const originalTestDocument = {
        ...generateTestDocumentFromSchema(operation.schema),
        _type: operation.typeName,
      };

      try {
        // Apply forward transform to get a transformed document
        const transformedDocument = operation.up(originalTestDocument);

        // Now test the reverse transform on the transformed document
        const reversedDocument = operation.down(transformedDocument);

        // Store the reversed document in a test instance
        state.multiCollections[testInstanceName] = {
          content: [reversedDocument]
        };

        this.trackOperation(state, operation, 'reverse');
        return state;
      } catch (error) {
        if (this.options.strictValidation) {
          throw new Error(`Reverse transform validation failed on test document: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
        return state;
      }
    }

    // Apply reverse transformation to each instance
    for (const collectionName of matchingCollections) {
      const collection = state.multiCollections[collectionName];
      if (!collection) continue;

      // Reverse transform documents of the specified type
      collection.content = collection.content.map(doc => {
        // Only transform documents of the specified type
        if (typeof doc === 'object' && doc !== null && '_type' in doc && doc._type === operation.typeName) {
          try {
            return operation.down(doc);
          } catch (error) {
            if (this.options.strictValidation) {
              throw new Error(`Reverse transform failed for document in ${collectionName}: ${error instanceof Error ? error.message : 'Unknown error'}`);
            }
            // In non-strict mode, return original document if transform fails
            return doc;
          }
        }
        // Return other documents unchanged
        return doc;
      });
    }

    this.trackOperation(state, operation, 'reverse');
    return state;
  }

  /**
   * Applies an update indexes operation
   * Note: In simulation mode, this is a no-op since indexes don't affect in-memory data
   * 
   * @private
   * @param state - Current database state
   * @param operation - Update indexes operation
   * @returns Database state (unchanged)
   */
  private applyUpdateIndexes(state: SimulationDatabaseState, operation: UpdateIndexesRule): SimulationDatabaseState {
    // In simulation mode, indexes don't affect the in-memory state
    // We just validate that the collection exists
    if (this.options.strictValidation && !state.collections[operation.collectionName]) {
      throw new Error(`Collection ${operation.collectionName} does not exist for updating indexes`);
    }

    this.trackOperation(state, operation, 'apply');
    return state;
  }

  /**
   * Reverses an update indexes operation
   * Note: In simulation mode, this is a no-op since indexes don't affect in-memory data
   * 
   * @private
   * @param state - Current database state
   * @param operation - Update indexes operation
   * @returns Database state (unchanged)
   */
  private reverseUpdateIndexes(state: SimulationDatabaseState, operation: UpdateIndexesRule): SimulationDatabaseState {
    // In simulation mode, reversing index operations is a no-op
    // We just validate that the collection exists
    if (this.options.strictValidation && !state.collections[operation.collectionName]) {
      throw new Error(`Collection ${operation.collectionName} does not exist for reversing index update`);
    }

    this.trackOperation(state, operation, 'reverse');
    return state;
  }

  /**
   * Creates a deep clone of the database state to avoid mutations
   * 
   * @private
   * @param state - State to clone
   * @returns Deep cloned state
   */
  private deepClone(state: SimulationDatabaseState): SimulationDatabaseState {
    return JSON.parse(JSON.stringify(state));
  }

  /**
   * Tracks an operation in the history if tracking is enabled
   * 
   * @private
   * @param state - Current database state
   * @param operation - Operation being tracked
   * @param type - Whether operation is being applied or reversed
   */
  private trackOperation(
    state: SimulationDatabaseState,
    operation: MigrationRule,
    type: 'apply' | 'reverse'
  ): void {
    if (this.options.trackHistory) {
      if (!state.operationHistory) {
        state.operationHistory = [];
      }
      
      state.operationHistory.push({
        operation,
        timestamp: new Date(),
        type,
      });
    }
  }
}

/**
 * Factory function to create a simulation applier instance
 * 
 * @param options - Configuration options for the applier
 * @returns A new simulation applier instance
 * 
 * @example
 * ```typescript
 * import { createSimulationApplier } from "@diister/mongodbee/migration/appliers";
 * 
 * const applier = createSimulationApplier({
 *   strictValidation: true,
 *   trackHistory: true
 * });
 * ```
 */
export function createSimulationApplier(options?: SimulationApplierOptions): SimulationApplier {
  return new SimulationApplier(options);
}

/**
 * Utility function to create an empty database state
 * 
 * @returns Empty database state
 * 
 * @example
 * ```typescript
 * const initialState = createEmptyDatabaseState();
 * console.log(initialState); // { collections: {}, multiCollections: {} }
 * ```
 */
export function createEmptyDatabaseState(): SimulationDatabaseState {
  return {
    collections: {},
    multiCollections: {},
  };
}

/**
 * Utility function to compare two database states for equality
 * 
 * @param state1 - First database state
 * @param state2 - Second database state
 * @returns True if states are deeply equal
 * 
 * @example
 * ```typescript
 * const areEqual = compareDatabaseStates(stateBefore, stateAfter);
 * if (areEqual) {
 *   console.log("Migration is reversible!");
 * }
 * ```
 */
export function compareDatabaseStates(
  state1: SimulationDatabaseState,
  state2: SimulationDatabaseState
): boolean {
  // Helper to filter out test simulation instances
  const filterTestInstances = (multiCollections?: Record<string, { content: Record<string, unknown>[] }>) => {
    if (!multiCollections) return undefined;
    const filtered: Record<string, { content: Record<string, unknown>[] }> = {};
    for (const [name, collection] of Object.entries(multiCollections)) {
      // Skip test simulation instances
      if (!name.endsWith('_test_simulation')) {
        filtered[name] = collection;
      }
    }
    return Object.keys(filtered).length > 0 ? filtered : undefined;
  };

  // Compare excluding operation history and test simulation instances
  const clean1 = {
    collections: state1.collections,
    multiCollections: filterTestInstances(state1.multiCollections)
  };
  const clean2 = {
    collections: state2.collections,
    multiCollections: filterTestInstances(state2.multiCollections)
  };

  return JSON.stringify(clean1) === JSON.stringify(clean2);
}
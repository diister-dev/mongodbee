/**
 * @fileoverview Core types and interfaces for the MongoDBee migration system
 * 
 * This module defines all the fundamental types, interfaces, and schemas used
 * throughout the migration system. It provides type safety and clear contracts
 * for migration operations, builders, and configurations.
 * 
 * @module
 */

import type * as v from '../schema.ts';
import type { MongoClient } from '../mongodb.ts';

/**
 * Represents the different properties that can be applied to a migration
 */
export type MigrationProperty = {
  /** Indicates that this migration cannot be reversed */
  type: 'irreversible';
};

/**
 * Rule for creating a new collection
 */
export type CreateCollectionRule = {
  type: 'create_collection';
  collectionName: string;
};

/**
 * Rule for seeding a collection with initial data
 */
export type SeedCollectionRule = {
  type: 'seed_collection';
  collectionName: string;
  documents: readonly unknown[];
};

/**
 * Rule for transforming documents in a collection
 * 
 * @template T - Input document type
 * @template U - Output document type
 */
export type TransformCollectionRule<T = Record<string, unknown>, U = Record<string, unknown>> = {
  type: 'transform_collection';
  collectionName: string;
  up: (doc: T) => U;
  down: (doc: U) => T;
};

/**
 * Union type representing all possible migration operations
 */
export type MigrationRule =
  | CreateCollectionRule
  | SeedCollectionRule
  | TransformCollectionRule;

/**
 * Transformation rule for bidirectional document changes
 * 
 * @template T - Input document type
 * @template U - Output document type
 */
export type TransformRule<T = Record<string, unknown>, U = Record<string, unknown>> = {
  /** Function to transform from old to new format */
  readonly up: (doc: T) => U;
  /** Function to transform from new to old format */
  readonly down: (doc: U) => T;
};

/**
 * Represents the current state of a migration during execution
 * 
 * This interface tracks all operations, properties, and provides
 * utilities for querying migration state.
 */
export interface MigrationState {
  /** Array of properties applied to this migration */
  properties: MigrationProperty[];
  
  /** Array of operations to be executed */
  operations: MigrationRule[];
  
  /**
   * Marks the migration with a specific property
   * @param props - The property to add to the migration
   */
  mark(props: MigrationProperty): void;
  
  /**
   * Checks if the migration has a specific property type
   * @param type - The property type to check for
   * @returns True if the migration has this property
   */
  hasProperty(type: MigrationProperty['type']): boolean;
}

/**
 * Builder interface for configuring operations on a specific collection
 * 
 * This interface provides a fluent API for chaining operations on a single
 * collection during migration definition.
 */
export interface MigrationCollectionBuilder {
  /**
   * Seeds the collection with initial documents
   * @param documents - Array of documents to insert
   * @returns The collection builder for method chaining
   */
  seed(documents: readonly unknown[]): MigrationCollectionBuilder;
  
  /**
   * Applies a transformation to all documents in the collection
   * @param rule - The transformation rule with up/down functions
   * @returns The collection builder for method chaining
   */
  transform(rule: TransformRule): MigrationCollectionBuilder;
  
  /**
   * Finishes configuring this collection and returns to the main builder
   * @returns The main migration builder
   */
  done(): MigrationBuilder;
}

/**
 * Main builder interface for defining migrations
 * 
 * This interface provides the primary API for defining migration operations
 * including creating collections, configuring existing collections, and
 * compiling the final migration state.
 */
export interface MigrationBuilder {
  /**
   * Creates a new collection and returns a builder to configure it
   * @param name - The name of the collection to create
   * @returns A collection builder for the new collection
   */
  createCollection(name: string): MigrationCollectionBuilder;
  
  /**
   * Configures an existing collection
   * @param name - The name of the collection to configure
   * @returns A collection builder for the existing collection
   */
  collection(name: string): MigrationCollectionBuilder;
  
  /**
   * Compiles the migration into its final executable state
   * @returns The compiled migration state
   */
  compile(): MigrationState;
}

/**
 * Schema definition for collections and multi-collections
 * 
 * This type defines the structure expected for schema definitions
 * in migrations, supporting both regular collections and multi-collections.
 */
export type SchemasDefinition = {
  /** Schema definitions for regular collections */
  collections: Record<string, Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>>;
  
  /** Schema definitions for multi-collections (optional) */
  multiCollections?: Record<string, Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>>;
};

/**
 * Core migration definition type
 * 
 * This type represents a complete migration with its metadata, schema,
 * parent relationship, and migration function.
 * 
 * @template Schema - The schema type for this migration
 */
export type MigrationDefinition<Schema extends SchemasDefinition = SchemasDefinition> = {
  /** Unique identifier for this migration */
  id: string;
  
  /** Human-readable name describing what this migration does */
  name: string;
  
  /** Reference to the parent migration (null for initial migration) */
  parent: MigrationDefinition | null;
  
  /** Schema definitions for this migration */
  schemas: Schema;
  
  /**
   * Function that defines the migration operations
   * @param migration - The migration builder instance
   * @returns The compiled migration state
   */
  migrate: (migration: MigrationBuilder) => MigrationState;
};

/**
 * Interface for applying migration operations
 * 
 * This interface defines the contract for migration appliers,
 * which can be implemented for different targets (simulation, MongoDB, etc.).
 */
export interface MigrationApplier {
  /**
   * Applies a single migration operation
   * @param operation - The migration operation to apply
   */
  applyOperation(operation: MigrationRule): Promise<void> | void;
  
  /**
   * Reverses a single migration operation
   * @param operation - The migration operation to reverse
   */
  applyReverseOperation(operation: MigrationRule): Promise<void> | void;
}

/**
 * Represents the state of a database during simulation
 * 
 * This type is used for in-memory simulation of database operations
 * during migration validation and testing.
 */
export type DatabaseState = {
  /** Collections and their document contents */
  collections: Record<string, { content: Record<string, unknown>[] }>;
  
  /** Multi-collections and their contents (future feature) */
  multiCollections?: Record<string, { content: Record<string, unknown>[] }>;
};

/**
 * Configuration options for migration mock data generation
 */
export type MockDataConfig = {
  /** Whether to enable mock data generation */
  enabled: boolean;
  
  /** Locales to use for fake data generation */
  locale: string[];
  
  /** Maximum length for generated strings */
  defaultStringMaxLength: number;
  
  /** Number of documents to generate per collection */
  documentsPerCollection: number;
  
  /** Optional seed for reproducible random data */
  seed?: number;
};

/**
 * Configuration for database connections and operations
 */
export type DatabaseConfig = {
  /** MongoDB connection URL */
  url: string;
  
  /** Database name */
  name: string;
  
  /** Whether to drop database on reset (useful for testing) */
  dropOnReset?: boolean;
};

/**
 * Configuration for path management
 */
export type PathsConfig = {
  /** Directory containing migration files */
  migrations: string;
  
  /** Directory containing schema files */
  schemas: string;
  
  /** Optional path for generated final schemas */
  finalSchemas?: string;
};

/**
 * Configuration for validation settings
 */
export type ValidationConfig = {
  /** Whether to validate migration chain integrity */
  chainValidation: boolean;
  
  /** Whether to perform integrity checks */
  integrityCheck: boolean;
  
  /** Whether to check schema consistency */
  schemaConsistency: boolean;
  
  /** Whether to test migration reversibility */
  reversibilityCheck: boolean;
};

/**
 * CLI-specific configuration options
 */
export type CLIConfig = {
  /** Path to custom migration templates */
  templatesPath?: string;
  
  /** Whether to automatically import new migrations */
  autoImport: boolean;
};

/**
 * Complete configuration for the migration system
 * 
 * This interface defines all configuration options available
 * for customizing the behavior of the migration system.
 */
export interface MigrationConfig {
  /** Path configuration */
  paths: PathsConfig;
  
  /** Database configuration */
  database: DatabaseConfig;
  
  /** Mock data generation configuration */
  mockData: MockDataConfig;
  
  /** Validation configuration */
  validation: ValidationConfig;
  
  /** CLI configuration */
  cli: CLIConfig;
}

/**
 * Options for creating a migration applier
 */
export type MigrationApplierOptions = {
  /** MongoDB client instance */
  client: MongoClient;
  
  /** Database name */
  database: string;
  
  /** Additional configuration options */
  options?: {
    /** Whether to use transactions */
    useTransactions?: boolean;
    
    /** Batch size for bulk operations */
    batchSize?: number;
  };
};

/**
 * Result of a migration operation
 */
export type MigrationResult = {
  /** Whether the operation was successful */
  success: boolean;
  
  /** Optional error message if operation failed */
  error?: string;
  
  /** Number of operations executed */
  operationsExecuted: number;
  
  /** Time taken to execute the migration */
  executionTime: number;
  
  /** Additional metadata about the migration */
  metadata?: Record<string, unknown>;
};

/**
 * Status of a migration in the system
 */
export type MigrationStatus = {
  /** Migration ID */
  id: string;
  
  /** Migration name */
  name: string;
  
  /** Current status */
  status: 'pending' | 'applied' | 'failed' | 'rolled-back';
  
  /** Timestamp when migration was last executed */
  appliedAt?: Date;
  
  /** Error message if migration failed */
  error?: string;
};

/**
 * Context for migration execution
 * 
 * This interface provides context and utilities during migration execution,
 * including access to the database, configuration, and logging utilities.
 */
export interface MigrationContext {
  /** Migration configuration */
  config: MigrationConfig;
  
  /** Database connection */
  database: Record<string, unknown>;
  
  /** Logging utility */
  logger: {
    info(message: string): void;
    warn(message: string): void;
    error(message: string): void;
    debug(message: string): void;
  };
}

// =============================================================================
// Type-Safe Operation Handler Utilities
// =============================================================================

/**
 * Union of all possible migration rule types for type-safe operations
 */
export type MigrationRuleType = MigrationRule['type'];

/**
 * Helper type to extract a specific migration rule by its type
 */
export type ExtractMigrationRule<T extends MigrationRuleType> = Extract<MigrationRule, { type: T }>;

/**
 * Type-safe operation handler map - forces implementation of all operation types
 * 
 * @template TResult - The return type of the operation handlers
 * @template TArgs - Additional arguments passed to operation handlers
 * 
 * @example
 * ```typescript
 * // For appliers that return void
 * const handlers: OperationHandlerMap<Promise<void>, []> = {
 *   create_collection: (operation) => handleCreateCollection(operation),
 *   seed_collection: (operation) => handleSeedCollection(operation),
 *   transform_collection: (operation) => handleTransformCollection(operation),
 * };
 * 
 * // For appliers that take state and return new state  
 * const stateHandlers: OperationHandlerMap<State, [State]> = {
 *   create_collection: (operation, state) => applyToState(operation, state),
 *   seed_collection: (operation, state) => applyToState(operation, state),
 *   transform_collection: (operation, state) => applyToState(operation, state),
 * };
 * ```
 */
export type OperationHandlerMap<TResult, TArgs extends readonly unknown[]> = {
  [K in MigrationRuleType]: (
    operation: ExtractMigrationRule<K>,
    ...args: TArgs
  ) => TResult;
};

/**
 * Utility type for creating type-safe operation dispatchers
 * 
 * @example
 * ```typescript
 * function createDispatcher<TResult, TArgs extends readonly unknown[]>(
 *   handlers: OperationHandlerMap<TResult, TArgs>
 * ): OperationDispatcher<TResult, TArgs> {
 *   return (operation: MigrationRule, ...args: TArgs): TResult => {
 *     const handler = handlers[operation.type];
 *     if (!handler) {
 *       throw new Error(`Unknown operation type: ${operation.type}`);
 *     }
 *     return handler(operation as never, ...args);
 *   };
 * }
 * ```
 */
export type OperationDispatcher<TResult, TArgs extends readonly unknown[]> = (
  operation: MigrationRule,
  ...args: TArgs
) => TResult;

/**
 * Factory function to create a type-safe operation dispatcher
 * 
 * @param handlers - Map of operation handlers
 * @returns Type-safe dispatcher function
 * 
 * @example
 * ```typescript
 * import { createOperationDispatcher } from "@diister/mongodbee/migration/types";
 * 
 * const dispatcher = createOperationDispatcher({
 *   create_collection: (operation) => console.log(`Creating ${operation.collectionName}`),
 *   seed_collection: (operation) => console.log(`Seeding ${operation.collectionName}`),
 *   transform_collection: (operation) => console.log(`Transforming ${operation.collectionName}`)
 * });
 * 
 * dispatcher({ type: 'create_collection', collectionName: 'users' });
 * ```
 */
export function createOperationDispatcher<TResult, TArgs extends readonly unknown[]>(
  handlers: OperationHandlerMap<TResult, TArgs>
): OperationDispatcher<TResult, TArgs> {
  return (operation: MigrationRule, ...args: TArgs): TResult => {
    const handler = handlers[operation.type];
    if (!handler) {
      throw new Error(`Unknown operation type: ${operation.type}`);
    }
    return handler(operation as never, ...args);
  };
}
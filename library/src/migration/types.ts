/**
 * @fileoverview Core types and interfaces for the MongoDBee migration system
 *
 * This module defines all the fundamental types, interfaces, and schemas used
 * throughout the migration system. It provides type safety and clear contracts
 * for migration operations, builders, and configurations.
 *
 * @module
 */

import type * as v from "../schema.ts";

/**
 * Represents the different properties that can be applied to a migration
 */
export type MigrationProperty =
  | {
      /** Indicates that this migration cannot be reversed */
      type: "irreversible";
    }
  | {
      /** Indicates that this migration has lossy transformations */
      type: "lossy";
    };

/**
 * Rule for creating a new collection
 */
export type CreateCollectionRule = {
  type: "create_collection";
  collectionName: string;
  /** Valibot schema for the collection - used to create MongoDB JSON Schema validator */
  schema?: unknown;
};

/**
 * Rule for seeding a collection with initial data
 */
export type SeedCollectionRule = {
  type: "seed_collection";
  collectionName: string;
  documents: readonly unknown[];
};

/**
 * Rule for transforming documents in a collection
 *
 * @template T - Input document type
 * @template U - Output document type
 */
export type TransformCollectionRule<
  T = Record<string, unknown>,
  U = Record<string, unknown>,
> = {
  type: "transform_collection";
  collectionName: string;
  up: (doc: T) => U;
  down: (doc: U) => T;
  /** Marks this transformation as irreversible (cannot be rolled back) */
  irreversible?: boolean;
  /** Marks this transformation as lossy (rollback loses data) */
  lossy?: boolean;
};

/**
 * Rule for creating a multi-collection instance
 */
export type CreateMultiCollectionInstanceRule = {
  type: "create_multicollection_instance";
  collectionName: string;
  collectionType: string;
};

/**
 * Rule for seeding a specific type in a multi-collection instance
 */
export type SeedMultiCollectionInstanceRule = {
  type: "seed_multicollection_instance";
  collectionName: string;
  typeName: string;
  documents: readonly unknown[];
};

/**
 * Rule for transforming a specific type across ALL instances of a multi-collection
 */
export type TransformMultiCollectionTypeRule<
  T = Record<string, unknown>,
  U = Record<string, unknown>,
> = {
  type: "transform_multicollection_type";
  collectionType: string;
  typeName: string;
  up: (doc: T) => U;
  down: (doc: U) => T;
  /** Optional Valibot schema for the type (new schema) - used to validate transformed documents */
  schema?: unknown;
  /** Optional Valibot schema from parent migration (old schema) - used to generate mock data for simulation */
  parentSchema?: unknown;
  /** Marks this transformation as irreversible (cannot be rolled back) */
  irreversible?: boolean;
  /** Marks this transformation as lossy (rollback loses data) */
  lossy?: boolean;
};

/**
 * Rule for updating indexes on an existing collection
 */
export type UpdateIndexesRule = {
  type: "update_indexes";
  collectionName: string;
  /** Valibot schema containing index definitions */
  schema: unknown;
};

/**
 * Rule for marking an existing collection as a multi-collection
 *
 * This is useful when migrating from a regular collection to a multi-collection structure,
 * or when adopting an existing collection that already has the multi-collection format
 * (documents with _type field) but lacks the metadata documents.
 */
export type MarkAsMultiCollectionRule = {
  type: "mark_as_multicollection";
  collectionName: string;
  collectionType: string;
};

/**
 * Union type representing all possible migration operations
 */
export type MigrationRule =
  | CreateCollectionRule
  | SeedCollectionRule
  | TransformCollectionRule
  | CreateMultiCollectionInstanceRule
  | SeedMultiCollectionInstanceRule
  | TransformMultiCollectionTypeRule
  | UpdateIndexesRule
  | MarkAsMultiCollectionRule;

/**
 * Transformation rule for bidirectional document changes
 *
 * @template T - Input document type
 * @template U - Output document type
 */
export type TransformRule<
  T = Record<string, unknown>,
  U = Record<string, unknown>,
> = {
  /** Function to transform from old to new format */
  readonly up: (doc: T) => U;
  /** Function to transform from new to old format */
  readonly down: (doc: U) => T;
  /**
   * Marks this transformation as irreversible
   * Use when the migration cannot be rolled back (no valid down() function)
   * Will show a warning during migrate and prevent rollback
   * @default false
   */
  readonly irreversible?: boolean;
  /**
   * Marks this transformation as lossy
   * Use when the down() function cannot restore the exact original data
   * Will show a warning during rollback and require confirmation
   * @default false
   */
  readonly lossy?: boolean;
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
  hasProperty(type: MigrationProperty["type"]): boolean;
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
 * Builder interface for configuring a specific type within a multi-collection
 */
export interface MultiCollectionTypeBuilder {
  /**
   * Applies a transformation to all documents of this type across ALL instances
   * @param rule - The transformation rule with up/down functions
   * @returns The type builder for method chaining
   */
  transform(rule: TransformRule): MultiCollectionTypeBuilder;

  /**
   * Finishes configuring this type and returns to the multi-collection builder
   * @returns The multi-collection builder
   */
  end(): MultiCollectionBuilder;
}

/**
 * Builder interface for configuring a multi-collection template
 */
export interface MultiCollectionBuilder {
  /**
   * Configures a specific type within the multi-collection
   * @param typeName - The name of the type to configure
   * @returns A type builder for the specified type
   */
  type(typeName: string): MultiCollectionTypeBuilder;

  /**
   * Finishes configuring this multi-collection and returns to the main builder
   * @returns The main migration builder
   */
  end(): MigrationBuilder;
}

/**
 * Builder interface for configuring a specific multi-collection instance
 */
export interface MultiCollectionInstanceBuilder {
  /**
   * Seeds a specific type in this instance
   * @param typeName - The name of the type to seed
   * @param documents - Array of documents to insert
   * @returns The instance builder for method chaining
   */
  seedType(
    typeName: string,
    documents: readonly unknown[],
  ): MultiCollectionInstanceBuilder;

  /**
   * Finishes configuring this instance and returns to the main builder
   * @returns The main migration builder
   */
  end(): MigrationBuilder;
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
   * Configures a multi-collection template (affects ALL instances)
   * @param name - The name of the multi-collection template
   * @returns A multi-collection builder for the template
   */
  multiCollection(name: string): MultiCollectionBuilder;

  /**
   * Creates a new instance of a multi-collection
   * @param collectionName - The full name of the collection
   * @param collectionType - The type/model of the multi-collection
   * @returns An instance builder for the new instance
   */
  newMultiCollection(
    collectionName: string,
    collectionType: string,
  ): MultiCollectionInstanceBuilder;

  /**
   * Configures an existing multi-collection instance
   * @param collectionName - The full name of the collection
   * @returns An instance builder for the existing instance
   */
  multiCollectionInstance(
    collectionName: string,
  ): MultiCollectionInstanceBuilder;

  /**
   * Updates indexes on an existing collection to match the schema
   * @param collectionName - The name of the collection
   * @returns The main migration builder for method chaining
   */
  updateIndexes(collectionName: string): MigrationBuilder;

  /**
   * Marks an existing collection as a multi-collection instance
   *
   * This is useful when migrating from a regular collection to a multi-collection,
   * or when adopting an existing collection that already has the multi-collection format.
   *
   * @param collectionName - The full name of the collection to mark
   * @param collectionType - The type/model of the multi-collection
   * @returns The main migration builder for method chaining
   *
   * @example
   * ```typescript
   * migrate(migration) {
   *   return migration
   *     .markAsMultiCollection("comments_legacy", "comments")
   *     .compile();
   * }
   * ```
   */
  markAsMultiCollection(
    collectionName: string,
    collectionType: string,
  ): MigrationBuilder;

  /**
   * Compiles the migration into its final executable state
   * @returns The compiled migration state
   */
  compile(): MigrationState;
}

export type SchemaContent = Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>;

/**
 * Schema definition for collections and multi-collections
 *
 * This type defines the structure expected for schema definitions
 * in migrations, supporting both regular collections and multi-collections.
 */
export type SchemasDefinition = {
  /** Schema definitions for regular collections */
  collections?: Record<
    string,
    SchemaContent
  >;

  /** Schema definitions for regular multi-collections */
  multiCollections?: Record<
    string, // multi-collection name
    Record<
      string, // type name within the multi-collection
      SchemaContent
    >
  >;

  /** Schema definitions for multi-collections models */
  multiModels?: Record<
    string, // multi-collection model schema name
    Record<
      string, // type name within the multi-collection
      SchemaContent
    >
  >;
};

/**
 * Core migration definition type
 *
 * This type represents a complete migration with its metadata, schema,
 * parent relationship, and migration function.
 *
 * @template Schema - The schema type for this migration
 */
export type MigrationDefinition<
  Schema extends SchemasDefinition = SchemasDefinition,
> = {
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

type StateCollectionContent = { content: Record<string, unknown>[] };

/**
 * Represents the state of a database during simulation
 *
 * This type is used for in-memory simulation of database operations
 * during migration validation and testing.
 */
export type DatabaseState = {
  /** Collections and their document contents */
  collections: Record<string, StateCollectionContent>;

  /** Multi-collections and their instances */
  multiCollections: Record<string, StateCollectionContent>;

  /** Multi-collections models and their contents (future feature) */
  multiModels: Record<string,
    Record<string, StateCollectionContent>
  >;
};
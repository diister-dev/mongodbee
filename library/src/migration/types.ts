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
  schema: SchemaContent;
};

export type CreateMultiCollectionRule = {
  type: "create_multicollection";
  collectionName: string;
  schema: MultiSchema;
}

export type CreateMultiModelInstanceRule = {
  type: "create_multimodel_instance";
  collectionName: string;
  modelType: string;
  schema: MultiSchema;
};

/**
 * Rule for creating a new scoped multi-collection.
 *
 * Differs from `create_multicollection` by carrying an additional `scope`
 * schema — the value validated against {@link ScopedMultiSchema.scope} on
 * every `.scope()` call.
 */
export type CreateScopedMultiCollectionRule = {
  type: "create_scoped_multicollection";
  collectionName: string;
  schema: ScopedMultiSchema;
};

/**
 * Rule for seeding a collection with initial data
 */
export type SeedCollectionRule = {
  type: "seed_collection";
  collectionName: string;
  documents: readonly unknown[];
  schema: SchemaContent;
};

export type SeedMultiCollectionTypeRule = {
  type: "seed_multicollection_type";
  collectionName: string;
  documentType: string;
  documents: readonly unknown[];
  schema: SchemaContent;
}

/**
 * Rule for seeding a specific (scope, type) bucket of a scoped
 * multi-collection.
 *
 * Each seed is bound to one explicit `scope` — there is no "default" or
 * "global" scope here. To seed several scopes with the same data, emit
 * one rule per scope.
 */
export type SeedScopedMultiCollectionTypeRule = {
  type: "seed_scoped_multicollection_type";
  collectionName: string;
  scope: string;
  documentType: string;
  documents: readonly unknown[];
  schema: SchemaContent;
}

export type SeedMultiModelInstanceTypeRule = {
  type: "seed_multimodel_instance_type";
  collectionName: string;
  modelType: string;
  documentType: string;
  documents: readonly unknown[];
  schema: SchemaContent;
};

export type SeedMultiModelInstancesTypeRule = {
  type: "seed_multimodel_instances_type";
  modelType: string;
  documentType: string;
  documents: readonly unknown[];
  schema: SchemaContent;
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
  schema: SchemaContent;
  parentSchema?: SchemaContent;
  /** Marks this transformation as irreversible (cannot be rolled back) */
  irreversible?: boolean;
  /** Marks this transformation as lossy (rollback loses data) */
  lossy?: boolean;
};

export type TransformMultiCollectionTypeRule<
  T = Record<string, unknown>,
  U = Record<string, unknown>,
> = {
  type: "transform_multicollection_type";
  collectionName: string;
  documentType: string;
  up: (doc: T) => U;
  down: (doc: U) => T;
  schema: SchemaContent,
  parentSchema?: SchemaContent,
  /** Marks this transformation as irreversible (cannot be rolled back) */
  irreversible?: boolean;
  /** Marks this transformation as lossy (rollback loses data) */
  lossy?: boolean;
};

/**
 * Rule for transforming every document of a given type inside a scoped
 * multi-collection.
 *
 * By default the transform touches **every scope**. Restrict the rule to
 * a subset with `scopeFilter` — useful when a migration only concerns a
 * specific tenant cohort.
 */
export type TransformScopedMultiCollectionTypeRule<
  T = Record<string, unknown>,
  U = Record<string, unknown>,
> = {
  type: "transform_scoped_multicollection_type";
  collectionName: string;
  documentType: string;
  up: (doc: T) => U;
  down: (doc: U) => T;
  schema: SchemaContent;
  parentSchema?: SchemaContent;
  /** Restrict the transform to a subset of scope values. Empty/absent = all scopes. */
  scopeFilter?: readonly string[];
  /** Marks this transformation as irreversible (cannot be rolled back) */
  irreversible?: boolean;
  /** Marks this transformation as lossy (rollback loses data) */
  lossy?: boolean;
};

export type TransformMultiModelInstanceTypeRule<
  T = Record<string, unknown>,
  U = Record<string, unknown>,
> = {
  type: "transform_multimodel_instance_type";
  collectionName: string;
  modelType: string;
  documentType: string;
  up: (doc: T) => U;
  down: (doc: U) => T;
  schema: SchemaContent,
  parentSchema?: SchemaContent,
  /** Marks this transformation as irreversible (cannot be rolled back) */
  irreversible?: boolean;
  /** Marks this transformation as lossy (rollback loses data) */
  lossy?: boolean;
};

export type TransformMultiModelInstancesTypeRule<
  T = Record<string, unknown>,
  U = Record<string, unknown>,
> = {
  type: "transform_multimodel_instances_type";
  modelType: string;
  documentType: string;
  up: (doc: T) => U;
  down: (doc: U) => T;
  schema: SchemaContent,
  parentSchema?: SchemaContent,
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
export type MarkAsMultiModelTypeRule = {
  type: "mark_as_multimodel";
  collectionName: string;
  modelType: string;
};

/**
 * Rule for deleting a type from a multi-collection or multi-model
 *
 * This removes all documents of a specific type. The down() migration
 * should handle restoring these documents if needed (usually irreversible).
 */
export type DeleteMultiCollectionTypeRule = {
  type: "delete_multicollection_type";
  collectionName: string;
  documentType: string;
  /** Parent schema for the deleted type (needed for down migration) */
  parentSchema?: SchemaContent;
};

export type DeleteMultiModelInstancesTypeRule = {
  type: "delete_multimodel_instances_type";
  modelType: string;
  documentType: string;
  /** Parent schema for the deleted type (needed for down migration) */
  parentSchema?: SchemaContent;
};

/**
 * Rule for renaming a type in a multi-collection or multi-model
 *
 * This changes the _type field of all documents from oldTypeName to newTypeName.
 */
export type RenameMultiCollectionTypeRule = {
  type: "rename_multicollection_type";
  collectionName: string;
  oldTypeName: string;
  newTypeName: string;
  schema: SchemaContent;
  parentSchema?: SchemaContent;
};

export type RenameMultiModelInstancesTypeRule = {
  type: "rename_multimodel_instances_type";
  modelType: string;
  oldTypeName: string;
  newTypeName: string;
  schema: SchemaContent;
  parentSchema?: SchemaContent;
};

/**
 * Cross-collection document flow (régime A).
 *
 * Reads documents from a source collection (optionally filtered), maps them,
 * and writes them into a target collection. The target `_id` is derived
 * deterministically from the source `_id` so the operation is reversible
 * without a provenance log:
 *
 * - `sourceDisposition: "keep"` (copy) — source is left intact ; rollback
 *   recomputes the target ids from the still-present source and deletes the
 *   copies. Fully reversible.
 * - `sourceDisposition: "consume"` (move) — source documents are deleted ;
 *   reversing would require provenance, so a move is marked `irreversible`.
 */
export type FlowRule = {
  type: "flow";
  from: { collection: string; where?: Record<string, unknown> };
  into: { collection: string };
  map: (doc: Record<string, unknown>) => Record<string, unknown>;
  sourceDisposition: "keep" | "consume";
  /** Target `_id` schema — used to derive the id prefix for generated ids. */
  targetIdSchema?: unknown;
  irreversible?: boolean;
  lossy?: boolean;
};

/** Context passed to the `flowToScope` derivers for each source document. */
export type FlowToScopeContext = {
  /** Set when the source is a plain collection. */
  sourceCollection?: string;
  /** Set when the source is a multi-model instance — the instance's name. */
  instanceName?: string;
  /** Set when the source is a multi-collection type. */
  documentType?: string;
};

/** Where a {@link FlowToScopeRule} reads its documents from. */
export type FlowToScopeSource =
  | { kind: "collection"; name: string; where?: Record<string, unknown> }
  | { kind: "multiModelInstances"; model: string }
  | { kind: "multiCollectionType"; collectionName: string; documentType: string };

/**
 * Route documents from a source (a plain collection, every instance of a
 * multi-model, or a multi-collection type) INTO a scoped multi-collection,
 * deriving the `_scope` per document. The general primitive behind
 * "consolidate N collections into one scoped collection" — composes rather
 * than hard-coding any particular migration.
 *
 * All knobs are general: `scope` (where each doc lands), `toType` (its
 * `_type`), `map` (per-doc transform, incl. re-keying via `_id`),
 * `onConflict` + `merge` (when a `(scope,type,_id)` target already exists),
 * and `sourceDisposition` (copy vs move). Forward-only by default
 * (`irreversible`).
 */
export type FlowToScopeRule = {
  type: "flow_to_scope";
  from: FlowToScopeSource;
  into: { collection: string };
  /** Target scope value for a source document. */
  scope: (doc: Record<string, unknown>, ctx: FlowToScopeContext) => string;
  /** Target `_type`. Defaults to the document's existing `_type`. */
  toType?: (doc: Record<string, unknown>, ctx: FlowToScopeContext) => string;
  /** Per-document transform (e.g. re-key by setting/removing `_id`). */
  map?: (
    doc: Record<string, unknown>,
    ctx: FlowToScopeContext,
  ) => Record<string, unknown>;
  /** What to do when a `(scope, type, _id)` document already exists in the target. */
  onConflict?: "error" | "skip" | "merge";
  /** Required when `onConflict === "merge"` — combine existing + incoming. */
  merge?: (
    existing: Record<string, unknown>,
    incoming: Record<string, unknown>,
  ) => Record<string, unknown>;
  /** `keep` = copy ; `consume` = move (delete/drop the source). */
  sourceDisposition: "keep" | "consume";
  irreversible?: boolean;
  lossy?: boolean;
};

/**
 * Rule for renaming a physical collection. The clean primitive behind a
 * temp→final swap during a scoped-collection consolidation, where the new
 * scoped collection shares its name with a legacy source: build into a temp
 * name, consume the sources, then rename the temp over the (now-free) name.
 *
 * Reversible — `down` renames `to` back to `from` — unless `dropTarget` drops
 * an existing target, which cannot be restored (then it is lossy).
 */
export type RenameCollectionRule = {
  type: "rename_collection";
  from: string;
  to: string;
  /** Drop `to` first if it already exists (a plain rename fails otherwise). */
  dropTarget?: boolean;
  lossy?: boolean;
};

/**
 * Union type representing all possible migration operations
 */
export type MigrationRule =
  // Create
  | CreateCollectionRule
  | CreateMultiCollectionRule
  | CreateMultiModelInstanceRule
  | CreateScopedMultiCollectionRule
  // Seed
  | SeedCollectionRule
  | SeedMultiCollectionTypeRule
  | SeedMultiModelInstanceTypeRule
  | SeedMultiModelInstancesTypeRule
  | SeedScopedMultiCollectionTypeRule
  // Transform
  | TransformCollectionRule
  | TransformMultiCollectionTypeRule
  | TransformMultiModelInstanceTypeRule
  | TransformMultiModelInstancesTypeRule
  | TransformScopedMultiCollectionTypeRule
  // Delete type
  | DeleteMultiCollectionTypeRule
  | DeleteMultiModelInstancesTypeRule
  // Rename type
  | RenameMultiCollectionTypeRule
  | RenameMultiModelInstancesTypeRule
  // Cross-collection
  | FlowRule
  | FlowToScopeRule
  // Other
  | UpdateIndexesRule
  | MarkAsMultiModelTypeRule
  | RenameCollectionRule;

/**
 * Transformation rule for bidirectional document changes
 *
 * @template T - Input document type
 * @template U - Output document type
 */
export type TransformRule<
  T = Record<string, any>,
  U = Record<string, any>,
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
export interface CollectionBuilder {
  /**
   * Seeds the collection with initial documents
   * @param documents - Array of documents to insert
   * @returns The collection builder for method chaining
   */
  seed(documents: readonly unknown[]): CollectionBuilder;

  /**
   * Applies a transformation to all documents in the collection
   * @param rule - The transformation rule with up/down functions
   * @returns The collection builder for method chaining
   */
  transform(rule: TransformRule): CollectionBuilder;

  /**
   * Finishes configuring this collection and returns to the main builder
   * @returns The main migration builder
   */
  end(): MigrationBuilder;
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
 * Builder interface for configuring a specific type within a multi-collection
 */
export interface MultiCollectionTypeBuilder {
  /**
   * Seeds this type with initial documents
   * @param documents - Array of documents to insert
   * @returns The type builder for method chaining
   */
  seed(documents: readonly unknown[]): MultiCollectionTypeBuilder;

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

export interface MultiModelInstanceTypeBuilder {
  /**
   * Seeds this type with initial documents
   * @param documents - Array of documents to insert
   * @returns The type builder for method chaining
   */
  seed(documents: readonly unknown[]): MultiModelInstanceTypeBuilder;
  
  /**
   * Applies a transformation to all documents of this type in this instance
   * @param rule - The transformation rule with up/down functions
   * @returns The type builder for method chaining
   */
  transform(rule: TransformRule): MultiModelInstanceTypeBuilder;

  /**
   * Finishes configuring this type and returns to the instance builder
   * @returns The instance builder
   */
  end(): MultiModelInstanceBuilder;
}

export interface MultiModelInstancesTypeBuilder {
  /**
   * Seeds this type with initial documents across ALL instances of this model type
   * @param documents - Array of documents to insert
   * @returns The type builder for method chaining
   */
  seed(documents: readonly unknown[]): MultiModelInstancesTypeBuilder;

  /**
   * Applies a transformation to all documents of this type across ALL instances of this model type
   * @param rule - The transformation rule with up/down functions
   * @returns The type builder for method chaining
   */
  transform(rule: TransformRule): MultiModelInstancesTypeBuilder;

  /**
   * Finishes configuring this type and returns to the main builder
   * @returns The main migration builder
   */
  end(): MultiModelInstancesBuilder;
}

export interface MultiModelInstancesBuilder {
  /**
   * Configures a specific type within ALL instances of this multi-collection model
   * @param typeName - The name of the type to configure
   * @returns A type builder for the specified type
   */
  type(typeName: string): MultiModelInstancesTypeBuilder;

  /**
   * Deletes a type from ALL instances of this multi-collection model
   * This removes all documents with the specified _type
   * @param typeName - The name of the type to delete
   * @returns The instances builder for method chaining
   */
  deleteType(typeName: string): MultiModelInstancesBuilder;

  /**
   * Renames a type in ALL instances of this multi-collection model
   * This changes the _type field from oldTypeName to newTypeName
   * @param oldTypeName - The current name of the type
   * @param newTypeName - The new name for the type
   * @returns The instances builder for method chaining
   */
  renameType(oldTypeName: string, newTypeName: string): MultiModelInstancesBuilder;

  /**
   * Finishes configuring this model type and returns to the main builder
   * @returns The main migration builder
   */
  end(): MigrationBuilder;
}

/**
 * Builder interface for configuring a specific multi-collection instance
 */
export interface MultiModelInstanceBuilder {
  /**
   * Configures a specific type within this multi-collection instance
   * @param typeName - The name of the type to configure
   * @returns A type builder for the specified type
   */
  type(typeName: string): MultiModelInstanceTypeBuilder;

  /**
   * Finishes configuring this instance and returns to the main builder
   * @returns The main migration builder
   */
  end(): MigrationBuilder;
}

/**
 * Builder interface for configuring a scoped multi-collection.
 */
export interface ScopedMultiCollectionBuilder {
  /**
   * Configures a specific document-type within the scoped multi-collection.
   */
  type(typeName: string): ScopedMultiCollectionTypeBuilder;

  /**
   * Finishes configuring this scoped multi-collection and returns to the
   * main builder.
   */
  end(): MigrationBuilder;
}

/**
 * Builder interface for configuring a specific type within a scoped
 * multi-collection.
 */
export interface ScopedMultiCollectionTypeBuilder {
  /**
   * Seeds this type with documents bound to one explicit scope.
   *
   * @param scope - The scope value (e.g. `"exposition:abc123"`)
   * @param documents - Documents to insert
   */
  seed(
    scope: string,
    documents: readonly unknown[],
  ): ScopedMultiCollectionTypeBuilder;

  /**
   * Applies a transformation to every document of this type. By default
   * the transform spans every scope ; pass `scopeFilter` to restrict the
   * effect to a subset.
   */
  transform(
    rule: TransformRule & { readonly scopeFilter?: readonly string[] },
  ): ScopedMultiCollectionTypeBuilder;

  /** Finishes configuring this type and returns to the scoped builder. */
  end(): ScopedMultiCollectionBuilder;
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
  createCollection(name: string): CollectionBuilder;

  /**
   * Configures an existing collection
   * @param name - The name of the collection to configure
   * @returns A collection builder for the existing collection
   */
  collection(name: string): CollectionBuilder;

  /**
   * Creates a new multi-collection template and returns a builder to configure it
   * @param name - The name of the multi-collection template to create
   * @returns A multi-collection builder for the new template
   */
  createMultiCollection(name: string): MultiCollectionBuilder;

  /**
   * Configures a multi-collection template (affects ALL instances)
   * @param name - The name of the multi-collection template
   * @returns A multi-collection builder for the template
   */
  multiCollection(name: string): MultiCollectionBuilder;

  /**
   * Creates a new instance of a multi-collection
   * @param collectionName - The full name of the collection
   * @param modelType - The type/model of the multi-collection
   * @returns An instance builder for the new instance
   */
  createMultiModelInstance(
    collectionName: string,
    modelType: string,
  ): MultiModelInstanceBuilder;

  /**
   * Configures an existing multi-collection instance
   * @param collectionName - The full name of the collection
   * @returns An instance builder for the existing instance
   */
  multiModelInstance(
    collectionName: string,
    modelType: string,
  ): MultiModelInstanceBuilder;

  /**
   * Configures all instances of a specific multi-collection model type
   * @param modelType - The type/model of the multi-collection
   * @returns An instance builder that applies to all instances of this model type
   */
  multiModelInstances(
    modelType: string,
  ): MultiModelInstancesBuilder;

  /**
   * Creates a new scoped multi-collection.
   *
   * @param name - Name of the scoped multi-collection
   * @returns A builder to configure its types and seed scopes
   */
  createScopedMultiCollection(name: string): ScopedMultiCollectionBuilder;

  /**
   * Configures an existing scoped multi-collection (for transforms / seeds
   * of subsequent migrations).
   */
  scopedMultiCollection(name: string): ScopedMultiCollectionBuilder;

  /**
   * Moves or copies documents from one collection to another.
   *
   * @example
   * ```typescript
   * migration.flow({
   *   from: { collection: "users", where: { active: false } },
   *   into: { collection: "archived_users" },
   *   map: (doc) => ({ ...doc, archivedReason: "inactivity" }),
   *   source: "keep", // copy (reversible) ; "consume" = move (irreversible)
   * });
   * ```
   */
  flow(config: {
    from: { collection: string; where?: Record<string, unknown> };
    into: { collection: string };
    map: (doc: Record<string, unknown>) => Record<string, unknown>;
    /** "keep" = copy (default, reversible) ; "consume" = move (irreversible). */
    source?: "keep" | "consume";
  }): MigrationBuilder;

  /**
   * Route documents from a source (collection / all instances of a
   * multi-model / a multi-collection type) into a scoped multi-collection,
   * deriving `_scope` per document. The composable primitive for
   * consolidating N collections into one scoped collection. Forward-only
   * (irreversible) by default.
   */
  flowToScope(config: {
    from: FlowToScopeSource;
    into: { collection: string };
    scope: (doc: Record<string, unknown>, ctx: FlowToScopeContext) => string;
    toType?: (doc: Record<string, unknown>, ctx: FlowToScopeContext) => string;
    map?: (
      doc: Record<string, unknown>,
      ctx: FlowToScopeContext,
    ) => Record<string, unknown>;
    onConflict?: "error" | "skip" | "merge";
    merge?: (
      existing: Record<string, unknown>,
      incoming: Record<string, unknown>,
    ) => Record<string, unknown>;
    /** "keep" = copy ; "consume" = move (delete/drop source). */
    source?: "keep" | "consume";
  }): MigrationBuilder;

  /**
   * Updates indexes on an existing collection to match the schema
   * @param collectionName - The name of the collection
   * @returns The main migration builder for method chaining
   */
  updateIndexes(collectionName: string): MigrationBuilder;

  /**
   * Renames a physical collection (`from` → `to`). The clean way to land a new
   * scoped collection whose name collides with a legacy source: build into a
   * temp name, consume the sources, then `renameCollection(temp, finalName)`.
   * Reversible unless `dropTarget` is used (drops an existing target).
   *
   * @param from - Current collection name
   * @param to - New collection name
   * @returns The main migration builder for method chaining
   */
  renameCollection(
    from: string,
    to: string,
    options?: { dropTarget?: boolean },
  ): MigrationBuilder;

  /**
   * Marks an existing collection as a multi-collection instance
   *
   * This is useful when migrating from a regular collection to a multi-collection,
   * or when adopting an existing collection that already has the multi-collection format.
   *
   * @param collectionName - The full name of the collection to mark
   * @param modelType - The type/model of the multi-collection
   * @returns The main migration builder for method chaining
   */
  markMultiModelType(
    collectionName: string,
    modelType: string,
  ): MultiModelInstanceBuilder;

  /**
   * Compiles the migration into its final executable state
   * @returns The compiled migration state
   */
  compile(): MigrationState;
}

export type SchemaContent = Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>;
export type MultiSchema = Record<string, SchemaContent>;

/**
 * Schema shape for a scoped multi-collection inside migrations: a single
 * `scope` Valibot schema validating scope values, plus the standard
 * `types` map identical in shape to a {@link MultiSchema}.
 */
export type ScopedMultiSchema = {
  scope: v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;
  types: MultiSchema;
};

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
    MultiSchema
  >;

  /** Schema definitions for multi-collections models */
  multiModels?: Record<
    string, // multi-collection model schema name
    MultiSchema
  >;

  /** Schema definitions for scoped multi-collections (one physical collection partitioned by a discriminator value). */
  scopedMultiCollections?: Record<
    string, // scoped multi-collection name
    ScopedMultiSchema
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
  multiModels: Record<string, StateCollectionContent & { modelType: string }>;

  /**
   * Scoped multi-collections — keyed by collection name. The flat `content`
   * stores every document ; each doc carries `_scope` + `_type` so the
   * applier and tests can filter naturally without an extra index layer.
   */
  scopedMultiCollections: Record<string, StateCollectionContent>;
};


/**
 * Extended database state with operation history tracking
 */
export interface SimulationDatabaseState extends DatabaseState {
  /** History of applied operations (if tracking enabled) */
  operationHistory?: Array<{
    operation: MigrationRule;
    timestamp: Date;
    type: "apply" | "reverse";
  }>;
}

/**
 * Utility function to create an empty database state
 *
 * @returns Empty database state
 *
 * @example
 * ```typescript
 * const initialState = createEmptyDatabaseState();
 * console.log(initialState); // { collections: {}, multiModels: {} }
 * ```
 */
export function createEmptyDatabaseState(): SimulationDatabaseState {
  return {
    collections: {},
    multiCollections: {},
    multiModels: {},
    scopedMultiCollections: {},
  };
}
/**
 * @fileoverview Migration builder implementation providing a fluent API for defining migrations
 *
 * This module implements the core migration builder functionality using a functional approach,
 * allowing developers to define migrations using a clean, fluent API. The builder handles validation,
 * state management, and compilation of migration operations.
 *
 * @example
 * ```typescript
 * import { migrationBuilder } from "@diister/mongodbee/migration";
 *
 * const migration = migrationBuilder({ schemas: mySchemas })
 *   .createCollection("users")
 *     .seed([{ name: "John", email: "john@example.com" }])
 *     .end()
 *   .collection("posts")
 *     .transform({
 *       up: (doc) => ({ ...doc, slug: doc.title.toLowerCase() }),
 *       down: (doc) => { const { slug, ...rest } = doc; return rest; }
 *     })
 *     .end()
 *   .collection("users")
 *     .transform({
 *       up: (doc) => ({ ...doc, fullName: `${doc.firstName} ${doc.lastName}` }),
 *       down: (doc) => ({ ...doc, firstName: doc.fullName.split(' ')[0], lastName: doc.fullName.split(' ')[1] || '' }),
 *       lossy: true  // Rollback may not preserve exact original names
 *     })
 *     .end()
 *   .collection("passwords")
 *     .transform({
 *       up: (doc) => ({ ...doc, password: hash(doc.password) }),
 *       down: (doc) => doc,  // Cannot unhash passwords
 *       irreversible: true  // No valid rollback possible
 *     })
 *     .end()
 *   .compile();
 * ```
 *
 * @module
 */

import type {
  CreateCollectionRule,
  MigrationBuilder,
  CollectionBuilder,
  MigrationProperty,
  MigrationRule,
  MigrationState,
  MultiCollectionBuilder,
  MultiModelInstanceBuilder,
  MultiCollectionTypeBuilder,
  SchemasDefinition,
  SeedCollectionRule,
  TransformCollectionRule,
  MultiModelInstanceTypeBuilder,
  MultiModelInstancesBuilder,
  MultiModelInstancesTypeBuilder,
} from "./types.ts";

/**
 * Options for creating a migration builder
 */
export type MigrationBuilderOptions = {
  schemas: SchemasDefinition,
  parentSchemas?: SchemasDefinition,
}

/**
 * Creates a new migration state instance with required methods
 */
function createMigrationState(
  operations: MigrationRule[] = [],
  properties: MigrationProperty[] = [],
): MigrationState {
  return {
    operations,
    properties,

    mark(props: MigrationProperty): void {
      // Avoid duplicate properties
      if (!this.hasProperty(props.type)) {
        this.properties.push(props);
      }
    },

    hasProperty(type: MigrationProperty["type"]): boolean {
      return this.properties.some((prop) => prop.type === type);
    },
  };
}

/**
 * Creates a collection builder with functional operations
 */
function createCollectionBuilder(
  state: MigrationState,
  collectionName: string,
  options: MigrationBuilderOptions,
): CollectionBuilder {
  const builder: CollectionBuilder = {
    seed(documents) {
      const collectionSchema = options.schemas?.collections?.[collectionName];
      if (!collectionSchema) {
        throw new Error(`Cannot seed collection ${collectionName}: schema not found in migration.schemas.collections`);
      }

      state.operations.push({
        type: "seed_collection",
        collectionName,
        documents,
        schema: collectionSchema,
      });

      return builder;
    },

    transform(rule) {
      const collectionSchema = options.schemas?.collections?.[collectionName];
      const parentCollectionSchema = options.parentSchemas?.collections?.[collectionName];

      if (!collectionSchema) {
        throw new Error(`Cannot transform collection ${collectionName}: schema not found in migration.schemas.collections`);
      }

      state.operations.push({
        type: "transform_collection",
        collectionName,
        up: rule.up,
        down: rule.down,
        schema: collectionSchema,
        parentSchema: parentCollectionSchema,
        irreversible: rule.irreversible,
        lossy: rule.lossy,
      });

      // Mark migration as irreversible if the transform is marked as such
      if (rule.irreversible) {
        state.mark({ type: "irreversible" });
      }

      // Mark migration as lossy if the transform is marked as such
      if (rule.lossy) {
        state.mark({ type: "lossy" });
      }

      return builder;
    },

    end() {
      return createMigrationBuilder(state, options);
    },
  }

  return builder;
}

/**
 * Creates a multi-collection type builder
 */
function createMultiCollectionTypeBuilder(
  state: MigrationState,
  collectionName: string,
  documentType: string,
  parentBuilder: MultiCollectionBuilder,
  options: MigrationBuilderOptions,
): MultiCollectionTypeBuilder {
  const builder: MultiCollectionTypeBuilder = {
    seed(documents) {
      const documentSchema = options.schemas?.multiCollections?.[collectionName]?.[documentType];
      if(!documentSchema) {
        throw new Error(`Cannot seed document type "${documentType}" in multi-collection "${collectionName}": schema not found in migration.schemas.multiCollections`);
      }

      state.operations.push({
        type: "seed_multicollection_type",
        collectionName,
        documentType,
        documents,
        schema: documentSchema,
      });
      return builder;
    },

    transform(rule) {
      // Extract schema for this specific type from options
      const typeSchema = options.schemas?.multiCollections
        ?.[collectionName]
        ?.[documentType];

      // Extract parent schema if available
      const parentTypeSchema = options.parentSchemas?.multiCollections
        ?.[collectionName]
        ?.[documentType];

      if (!typeSchema) {
        throw new Error(`Cannot transform type ${documentType} in multi-collection ${collectionName}: schema not found in migration.schemas.multiCollections`);
      }

      state.operations.push({
        type: "transform_multicollection_type",
        collectionName,
        documentType,
        up: rule.up,
        down: rule.down,
        schema: typeSchema,
        parentSchema: parentTypeSchema,
        irreversible: rule.irreversible,
        lossy: rule.lossy,
      });

      // Mark migration as irreversible if the transform is marked as such
      if (rule.irreversible) {
        state.mark({ type: "irreversible" });
      }

      // Mark migration as lossy if the transform is marked as such
      if (rule.lossy) {
        state.mark({ type: "lossy" });
      }

      return builder;
    },

    end() {
      return parentBuilder;
    },
  };

  return builder;
}

/**
 * Creates a multi-collection builder
 */
function createMultiCollectionBuilder(
  state: MigrationState,
  collectionName: string,
  mainBuilder: MigrationBuilder,
  options: MigrationBuilderOptions,
): MultiCollectionBuilder {
  const builder: MultiCollectionBuilder = {
    type(typeName): MultiCollectionTypeBuilder {
      return createMultiCollectionTypeBuilder(
        state,
        collectionName,
        typeName,
        builder,
        options,
      );
    },

    end(): MigrationBuilder {
      return mainBuilder;
    },
  };

  return builder;
}

/**
 * Creates a multi-collection instance builder
 */
function createMultiModelInstanceBuilder(
  state: MigrationState,
  collectionName: string,
  modelType: string,
  mainBuilder: MigrationBuilder,
  options: MigrationBuilderOptions,
): MultiModelInstanceBuilder {
  const builder: MultiModelInstanceBuilder = {
    type(typeName) {
      return createMultiModelInstanceTypeBuilder(
        state,
        collectionName,
        modelType,
        typeName,
        builder,
        options,
      );
    },
    end() {
      return mainBuilder;
    },
  };

  return builder;
}

/**
 * Creates a builder for all instances of a multi-collection model
 * This allows applying operations to all instances of a model type
 */
function createMultiModelInstancesBuilder(
  state: MigrationState,
  modelType: string,
  mainBuilder: MigrationBuilder,
  options: MigrationBuilderOptions,
) : MultiModelInstancesBuilder {
  const builder: MultiModelInstancesBuilder = {
    type(typeName) {
      return createMultiModelInstancesTypeBuilder(
        state,
        modelType,
        typeName,
        builder,
        options,
      );
    },
    end() {
      return mainBuilder;
    },
  };

  return builder;
}

function createMultiModelInstanceTypeBuilder(
  state: MigrationState,
  collectionName: string,
  modelType: string,
  documentType: string,
  parentBuilder: MultiModelInstanceBuilder,
  options: MigrationBuilderOptions,
): MultiModelInstanceTypeBuilder {
  const builder: MultiModelInstanceTypeBuilder = {
    seed(documents) {
      const documentSchema = options.schemas?.multiModels?.[modelType]?.[documentType];
      if(!documentSchema) {
        throw new Error(`Cannot seed document type "${documentType}" in multi-model instance "${collectionName}" (model: ${modelType}): schema not found in migration.schemas.multiModels`);
      }

      state.operations.push({
        type: "seed_multimodel_instance_type",
        collectionName,
        modelType,
        documentType,
        documents,
        schema: documentSchema,
      });
      return builder;
    },

    transform(rule) {
      // Extract schema for this specific type from options
      const typeSchema = options.schemas?.multiModels
        ?.[modelType]
        ?.[documentType];

      // Extract parent schema if available
      const parentTypeSchema = options.parentSchemas?.multiModels
        ?.[modelType]
        ?.[documentType];

      if (!typeSchema) {
        throw new Error(`Cannot transform type ${documentType} in multi-model instance ${collectionName} of model ${modelType}: schema not found in migration.schemas.multiModels`);
      }

      state.operations.push({
        type: "transform_multimodel_instance_type",
        collectionName,
        modelType,
        documentType,
        up: rule.up,
        down: rule.down,
        schema: typeSchema,
        parentSchema: parentTypeSchema,
        irreversible: rule.irreversible,
        lossy: rule.lossy,
      });

      // Mark migration as irreversible if the transform is marked as such
      if (rule.irreversible) {
        state.mark({ type: "irreversible" });
      }

      // Mark migration as lossy if the transform is marked as such
      if (rule.lossy) {
        state.mark({ type: "lossy" });
      }

      return builder;
    },

    end() {
      return parentBuilder;
    },
  };

  return builder;
}

function createMultiModelInstancesTypeBuilder(
  state: MigrationState,
  modelType: string,
  documentType: string,
  parentBuilder: MultiModelInstancesBuilder,
  options: MigrationBuilderOptions,
): MultiModelInstancesTypeBuilder {
  const builder: MultiModelInstancesTypeBuilder = {
    seed(documents) {
      const documentSchema = options.schemas?.multiModels?.[modelType]?.[documentType];
      if(!documentSchema) {
        throw new Error(`Cannot seed document type "${documentType}" in multi-model instances (model: ${modelType}): schema not found in migration.schemas.multiModels`);
      }

      state.operations.push({
        type: "seed_multimodel_instances_type",
        modelType,
        documentType,
        documents,
        schema: documentSchema,
      });
      return builder;
    },

    transform(rule) {
      // Extract schema for this specific type from options
      const typeSchema = options.schemas?.multiModels
        ?.[modelType]
        ?.[documentType];

      // Extract parent schema if available
      const parentTypeSchema = options.parentSchemas?.multiModels
        ?.[modelType]
        ?.[documentType];

      if (!typeSchema) {
        throw new Error(`Cannot transform type ${documentType} in multi-model instances of model ${modelType}: schema not found in migration.schemas.multiModels`);
      }

      state.operations.push({
        type: "transform_multimodel_instances_type",
        modelType,
        documentType,
        up: rule.up,
        down: rule.down,
        schema: typeSchema,
        parentSchema: parentTypeSchema,
        irreversible: rule.irreversible,
        lossy: rule.lossy,
      });

      // Mark migration as irreversible if the transform is marked as such
      if (rule.irreversible) {
        state.mark({ type: "irreversible" });
      }

      // Mark migration as lossy if the transform is marked as such
      if (rule.lossy) {
        state.mark({ type: "lossy" });
      }

      return builder;
    },
    end() {
      return parentBuilder;
    }
  };

  return builder;
}
  

/**
 * Creates the main migration builder with functional operations
 */
function createMigrationBuilder(
  state: MigrationState,
  options: MigrationBuilderOptions,
): MigrationBuilder {
  const builder: MigrationBuilder = {
    createCollection(name) {
      // Extract schema for this collection from options
      const collectionSchema = options.schemas?.collections?.[name];
      
      if (!collectionSchema) {
        throw new Error(`Cannot create collection ${name}: schema not found in migration.schemas.collections`);
      }

      state.operations.push({
        type: "create_collection",
        collectionName: name,
        schema: collectionSchema,
      });

      // Creating a collection makes the migration lossy (rollback drops the collection)
      state.mark({ type: "lossy" });

      return createCollectionBuilder(state, name, options);
    },

    collection(name) {
      return createCollectionBuilder(state, name, options);
    },

    createMultiCollection(name) {
      // Extract schema for this multi-collection from options
      const multiCollectionSchema = options.schemas?.multiCollections?.[name];
      if(!multiCollectionSchema) {
        throw new Error(`Cannot create multi-collection ${name}: schema not found in migration.schemas.multiCollections`);
      }

      state.operations.push({
        type: "create_multicollection",
        collectionName: name,
        schema: multiCollectionSchema,
      });

      // Creating a multi-collection makes the migration lossy (rollback drops the collection)
      state.mark({ type: "lossy" });

      return createMultiCollectionBuilder(state, name, builder, options);
    },

    multiCollection(name) {
      return createMultiCollectionBuilder(state, name, builder, options);
    },

    createMultiModelInstance(collectionName, modelType) {
      const multiSchema = options.schemas?.multiModels?.[modelType];
      if (!multiSchema) {
        throw new Error(
          `Cannot create multi-collection instance ${collectionName} of type ${modelType}: schema not found in migration.schemas.multiModels`,
        );
      }

      state.operations.push({
        type: "create_multimodel_instance",
        collectionName,
        modelType,
        schema: multiSchema,
      });

      // Creating a multi-collection instance makes the migration lossy (rollback drops the collection)
      state.mark({ type: "lossy" });

      return createMultiModelInstanceBuilder(
        state,
        collectionName,
        modelType,
        builder,
        options,
      );
    },

    multiModelInstance(collectionName, modelType) {
      return createMultiModelInstanceBuilder(
        state,
        collectionName,
        modelType,
        builder,
        options,
      );
    },

    multiModelInstances(modelType) {
      return createMultiModelInstancesBuilder(
        state,
        modelType,
        builder,
        options,
      );
    },

    updateIndexes(collectionName) {
      // Extract schema for this collection from options
      const collectionSchema = options.schemas?.collections?.[collectionName];

      if (!collectionSchema) {
        throw new Error(
          `Cannot update indexes for ${collectionName}: schema not found in migration.schemas.collections`,
        );
      }

      state.operations.push({
        type: "update_indexes",
        collectionName,
        schema: collectionSchema,
      });

      // Updating indexes is lossy (rollback doesn't restore old indexes)
      state.mark({ type: "lossy" });

      return builder;
    },

    markMultiModelType(collectionName, modelType) {
      state.operations.push({
        type: "mark_as_multimodel",
        collectionName,
        modelType,
      });
      
      return createMultiModelInstanceBuilder(
        state,
        collectionName,
        modelType,
        builder,
        options,
      )
    },

    compile() {
      return state;
    },
  };

  return builder;
}

/**
 * Creates a new migration builder instance
 *
 * This factory function creates a new migration builder with the provided
 * options and an initial empty state. The builder uses a fluent API pattern
 * for defining migration operations.
 *
 * @param options - Configuration options including schema definitions
 * @param initState - Optional initial state to start from
 * @returns A new migration builder instance
 *
 * @example
 * ```typescript
 * import { migrationBuilder } from "@diister/mongodbee/migration";
 *
 * const schemas = {
 *   collections: {
 *     users: {
 *       _id: v.string(),
 *       name: v.string(),
 *       email: v.pipe(v.string(), v.email())
 *     }
 *   }
 * };
 *
 * const migration = migrationBuilder({ schemas })
 *   .createCollection("users")
 *     .seed([
 *       { name: "Alice", email: "alice@example.com" },
 *       { name: "Bob", email: "bob@example.com" }
 *     ])
 *     .end()
 *   .compile();
 * ```
 */
export function migrationBuilder(
  options: MigrationBuilderOptions,
  initState?: MigrationState,
): MigrationBuilder {
  const state = initState ?? createMigrationState();
  return createMigrationBuilder(state, options);
}

/**
 * Type guard to check if an operation is a create collection rule
 *
 * @param operation - The migration operation to check
 * @returns True if the operation is a create collection rule
 */
export function isCreateCollectionRule(
  operation: MigrationRule,
): operation is CreateCollectionRule {
  return operation.type === "create_collection";
}

/**
 * Type guard to check if an operation is a seed collection rule
 *
 * @param operation - The migration operation to check
 * @returns True if the operation is a seed collection rule
 */
export function isSeedCollectionRule(
  operation: MigrationRule,
): operation is SeedCollectionRule {
  return operation.type === "seed_collection";
}

/**
 * Type guard to check if an operation is a transform collection rule
 *
 * @param operation - The migration operation to check
 * @returns True if the operation is a transform collection rule
 */
export function isTransformCollectionRule(
  operation: MigrationRule,
): operation is TransformCollectionRule {
  return operation.type === "transform_collection";
}

/**
 * Utility function to get a summary of migration operations
 *
 * @param state - The migration state to summarize
 * @returns A summary object with operation counts and properties
 *
 * @example
 * ```typescript
 * const summary = getMigrationSummary(migrationState);
 * console.log(`Creates ${summary.creates} collections`);
 * console.log(`Seeds ${summary.seeds} collections`);
 * console.log(`Transforms ${summary.transforms} collections`);
 * console.log(`Is reversible: ${!summary.isIrreversible}`);
 * ```
 */
export function getMigrationSummary(state: MigrationState): {
  creates: number;
  seeds: number;
  transforms: number;
  totalOperations: number;
  isIrreversible: boolean;
  properties: Array<MigrationProperty["type"]>;
} {
  const summary = {
    creates: 0,
    seeds: 0,
    transforms: 0,
    totalOperations: state.operations.length,
    isIrreversible: state.hasProperty("irreversible"),
    properties: state.properties.map((p) => p.type),
  };

  for (const operation of state.operations) {
    switch (operation.type) {
      case "create_collection":
        summary.creates++;
        break;
      case "seed_collection":
        summary.seeds++;
        break;
      case "transform_collection":
        summary.transforms++;
        break;
    }
  }

  return summary;
}

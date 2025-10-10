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
 *     .done()
 *   .collection("posts")
 *     .transform({
 *       up: (doc) => ({ ...doc, slug: doc.title.toLowerCase() }),
 *       down: (doc) => { const { slug, ...rest } = doc; return rest; }
 *     })
 *     .done()
 *   .collection("users")
 *     .transform({
 *       up: (doc) => ({ ...doc, fullName: `${doc.firstName} ${doc.lastName}` }),
 *       down: (doc) => ({ ...doc, firstName: doc.fullName.split(' ')[0], lastName: doc.fullName.split(' ')[1] || '' }),
 *       lossy: true  // Rollback may not preserve exact original names
 *     })
 *     .done()
 *   .collection("passwords")
 *     .transform({
 *       up: (doc) => ({ ...doc, password: hash(doc.password) }),
 *       down: (doc) => doc,  // Cannot unhash passwords
 *       irreversible: true  // No valid rollback possible
 *     })
 *     .done()
 *   .compile();
 * ```
 *
 * @module
 */

import * as v from "../schema.ts";
import type {
  CreateCollectionRule,
  CreateMultiCollectionInstanceRule,
  CreateMultiCollectionRule,
  MarkAsMultiCollectionRule,
  MigrationBuilder,
  MigrationCollectionBuilder,
  MigrationProperty,
  MigrationRule,
  MigrationState,
  MultiCollectionBuilder,
  MultiCollectionInstanceBuilder,
  MultiCollectionTypeBuilder,
  SchemasDefinition,
  SeedCollectionRule,
  SeedMultiCollectionTypeRule,
  SeedMultiCollectionInstanceRule,
  TransformCollectionRule,
  TransformMultiCollectionTypeRule,
  TransformRule,
  UpdateIndexesRule,
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
): MigrationCollectionBuilder {
  return {
    seed(documents: readonly unknown[]): MigrationCollectionBuilder {
      // Validate documents against schema if available
      const schema = options.schemas.collections?.[collectionName];
      let validatedDocs: readonly unknown[] = documents;

      if (schema) {
        validatedDocs = documents.map((doc) => {
          const parseResult = v.safeParse(v.object(schema), doc);
          if (!parseResult.success) {
            throw new Error(
              `Document in collection ${collectionName} failed schema validation: ${
                parseResult.issues.map((i) => i.message).join(", ")
              }`,
            );
          }
          return parseResult.output;
        });
      }

      const seedRule: SeedCollectionRule = {
        type: "seed_collection",
        collectionName,
        documents: validatedDocs,
      };

      state.operations.push(seedRule);
      return this;
    },

    transform(rule: TransformRule): MigrationCollectionBuilder {
      const transformRule: TransformCollectionRule = {
        type: "transform_collection",
        collectionName,
        up: rule.up,
        down: rule.down,
        irreversible: rule.irreversible,
        lossy: rule.lossy,
      };

      state.operations.push(transformRule);

      // Mark migration as irreversible if the transform is marked as such
      if (rule.irreversible) {
        state.mark({ type: "irreversible" });
      }

      // Mark migration as lossy if the transform is marked as such
      if (rule.lossy) {
        state.mark({ type: "lossy" });
      }

      return this;
    },

    done(): MigrationBuilder {
      return createMigrationBuilder(state, options);
    },
  };
}

/**
 * Creates a multi-collection type builder
 */
function createMultiCollectionTypeBuilder(
  state: MigrationState,
  collectionType: string,
  typeName: string,
  parentBuilder: MultiCollectionBuilder,
  options: MigrationBuilderOptions,
): MultiCollectionTypeBuilder {
  return {
    seed(documents: readonly unknown[]): MultiCollectionTypeBuilder {
      // Validate documents against schema if available
      const schema = options.schemas.multiModels?.[collectionType]
        ?.[typeName];
      let validatedDocs: readonly unknown[] = documents;

      if (schema) {
        validatedDocs = documents.map((doc) => {
          const parseResult = v.safeParse(v.object(schema), doc);
          if (!parseResult.success) {
            throw new Error(
              `Document in multi-collection ${collectionType}.${typeName} failed schema validation: ${
                parseResult.issues.map((i) => i.message).join(", ")
              }`,
            );
          }
          // Return original document instead of parseResult.output to preserve Date objects
          return doc;
        });
      }

      const seedRule: SeedMultiCollectionInstanceRule = {
        type: "seed_multicollection_instance",
        collectionName: collectionType, // Use collectionType as the base collection name
        typeName,
        documents: validatedDocs,
      };

      state.operations.push(seedRule);
      return this;
    },

    transform(rule: TransformRule): MultiCollectionTypeBuilder {
      // Extract schema for this specific type from options
      const typeSchema = options.schemas?.multiModels?.[collectionType]
        ?.[typeName];

      // Extract parent schema if available
      const parentTypeSchema = options.parentSchemas?.multiModels
        ?.[collectionType]
        ?.[typeName];

      const transformRule: TransformMultiCollectionTypeRule = {
        type: "transform_multicollection_type",
        collectionType,
        typeName,
        up: rule.up,
        down: rule.down,
        schema: typeSchema,
        parentSchema: parentTypeSchema,
        irreversible: rule.irreversible,
        lossy: rule.lossy,
      };

      state.operations.push(transformRule);

      // Mark migration as irreversible if the transform is marked as such
      if (rule.irreversible) {
        state.mark({ type: "irreversible" });
      }

      // Mark migration as lossy if the transform is marked as such
      if (rule.lossy) {
        state.mark({ type: "lossy" });
      }

      return this;
    },

    end(): MultiCollectionBuilder {
      return parentBuilder;
    },
  };
}

/**
 * Creates a multi-collection builder
 */
function createMultiCollectionBuilder(
  state: MigrationState,
  collectionType: string,
  mainBuilder: MigrationBuilder,
  options: MigrationBuilderOptions,
): MultiCollectionBuilder {
  const builder: MultiCollectionBuilder = {
    type(typeName: string): MultiCollectionTypeBuilder {
      return createMultiCollectionTypeBuilder(
        state,
        collectionType,
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
function createMultiCollectionInstanceBuilder(
  state: MigrationState,
  collectionName: string,
  collectionType: string,
  mainBuilder: MigrationBuilder,
  options: MigrationBuilderOptions,
): MultiCollectionInstanceBuilder {
  return {
    seedType(
      typeName: string,
      documents: readonly unknown[],
    ): MultiCollectionInstanceBuilder {
      // Validate documents against schema if available, similar to collection seed
      const schema = options.schemas.multiModels?.[collectionType]
        ?.[typeName];
      let validatedDocs: readonly unknown[] = documents;

      if (schema) {
        validatedDocs = documents.map((doc) => {
          const parseResult = v.safeParse(v.object(schema), doc);
          if (!parseResult.success) {
            throw new Error(
              `Document in multi-collection ${collectionName}.${typeName} failed schema validation: ${
                parseResult.issues.map((i) => i.message).join(", ")
              }`,
            );
          }
          // Return original document instead of parseResult.output to preserve Date objects
          return doc;
        });
      }

      const seedRule: SeedMultiCollectionInstanceRule = {
        type: "seed_multicollection_instance",
        collectionName,
        typeName,
        documents: validatedDocs,
      };

      state.operations.push(seedRule);
      return this;
    },

    end(): MigrationBuilder {
      return mainBuilder;
    },
  };
}

/**
 * Creates the main migration builder with functional operations
 */
function createMigrationBuilder(
  state: MigrationState,
  options: MigrationBuilderOptions,
): MigrationBuilder {
  const builder: MigrationBuilder = {
    createCollection(name: string): MigrationCollectionBuilder {
      // Extract schema for this collection from options
      const collectionSchema = options.schemas?.collections?.[name];

      const createRule: CreateCollectionRule = {
        type: "create_collection",
        collectionName: name,
        // Store the raw schema object (will be wrapped in v.object() by the applier)
        schema: collectionSchema,
      };

      state.operations.push(createRule);

      // Creating a collection makes the migration lossy (rollback drops the collection)
      state.mark({ type: "lossy" });

      return createCollectionBuilder(state, name, options);
    },

    collection(name: string): MigrationCollectionBuilder {
      return createCollectionBuilder(state, name, options);
    },

    createMultiCollection(name: string) : MultiCollectionBuilder {
      // Extract schema for this multi-collection from options
      const multiCollectionSchema = options.schemas?.multiCollections?.[name];

      state.operations.push({
        type: "create_multicollection",
        collectionName: name,
        schema: multiCollectionSchema,
      });

      return createMultiCollectionBuilder(state, name, builder, options);
    },

    multiCollection(name: string): MultiCollectionBuilder {
      return createMultiCollectionBuilder(state, name, builder, options);
    },

    newMultiCollection(
      collectionName: string,
      collectionType: string,
    ): MultiCollectionInstanceBuilder {
      const createRule: CreateMultiCollectionInstanceRule = {
        type: "create_multicollection_instance",
        collectionName,
        collectionType,
      };

      state.operations.push(createRule);

      // Creating a multi-collection instance makes the migration lossy (rollback drops the collection)
      state.mark({ type: "lossy" });

      return createMultiCollectionInstanceBuilder(
        state,
        collectionName,
        collectionType,
        builder,
        options,
      );
    },

    multiCollectionInstance(
      collectionName: string,
    ): MultiCollectionInstanceBuilder {
      // Need to infer collection type from schemas - find which multi-collection model this belongs to
      let collectionType: string | undefined;

      if (options.schemas?.multiModels) {
        // Try to match collection name pattern with model names
        for (const modelName of Object.keys(options.schemas.multiModels)) {
          // Simple heuristic: if collection name starts with model name, it's probably that type
          if (
            collectionName.startsWith(modelName + "_") ||
            collectionName === modelName
          ) {
            collectionType = modelName;
            break;
          }
        }
      }

      if (!collectionType) {
        // Fallback: use collection name itself as type
        collectionType = collectionName;
      }

      return createMultiCollectionInstanceBuilder(
        state,
        collectionName,
        collectionType,
        builder,
        options,
      );
    },

    updateIndexes(collectionName: string): MigrationBuilder {
      // Extract schema for this collection from options
      const collectionSchema = options.schemas?.collections?.[collectionName];

      if (!collectionSchema) {
        throw new Error(
          `Cannot update indexes for ${collectionName}: schema not found in migration.schemas.collections`,
        );
      }

      const updateRule: UpdateIndexesRule = {
        type: "update_indexes",
        collectionName,
        schema: collectionSchema,
      };

      state.operations.push(updateRule);

      // Updating indexes is lossy (rollback doesn't restore old indexes)
      state.mark({ type: "lossy" });

      return builder;
    },

    markAsMultiCollection(
      collectionName: string,
      collectionType: string,
    ): MigrationBuilder {
      const markRule: MarkAsMultiCollectionRule = {
        type: "mark_as_multicollection",
        collectionName,
        collectionType,
      };

      state.operations.push(markRule);

      // Marking is reversible (can remove metadata documents)
      return builder;
    },

    compile(): MigrationState {
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
 *     .done()
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
export function getMigrationSummary(state: MigrationState) {
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

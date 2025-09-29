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
 *   .compile();
 * ```
 * 
 * @module
 */

import * as v from '../schema.ts';
import type {
  MigrationBuilder,
  MigrationCollectionBuilder,
  MultiCollectionBuilder,
  MultiCollectionTypeBuilder,
  MultiCollectionInstanceBuilder,
  MigrationState,
  MigrationProperty,
  MigrationRule,
  TransformRule,
  CreateCollectionRule,
  SeedCollectionRule,
  TransformCollectionRule,
  CreateMultiCollectionInstanceRule,
  SeedMultiCollectionInstanceRule,
  TransformMultiCollectionTypeRule,
} from './types.ts';

/**
 * Schema for migration builder options
 */
export const MigrationBuilderOptionsSchema = v.object({
  schemas: v.record(v.string(), v.record(v.string(), v.any())),
});

/**
 * Options for creating a migration builder
 */
export type MigrationBuilderOptions = v.InferInput<typeof MigrationBuilderOptionsSchema>;

/**
 * Creates a new migration state instance with required methods
 */
function createMigrationState(operations: MigrationRule[] = [], properties: MigrationProperty[] = []): MigrationState {
  return {
    operations,
    properties,
    
    mark(props: MigrationProperty): void {
      // Avoid duplicate properties
      if (!this.hasProperty(props.type)) {
        this.properties.push(props);
      }
    },
    
    hasProperty(type: MigrationProperty['type']): boolean {
      return this.properties.some(prop => prop.type === type);
    }
  };
}

/**
 * Creates a collection builder with functional operations
 */
function createCollectionBuilder(
  state: MigrationState,
  collectionName: string,
  options: MigrationBuilderOptions
): MigrationCollectionBuilder {
  return {
    seed(documents: readonly unknown[]): MigrationCollectionBuilder {
      // Validate documents against schema if available
      const schema = options.schemas.collections?.[collectionName];
      let validatedDocs: readonly unknown[] = documents;
      
      if (schema) {
        validatedDocs = documents.map(doc => {
          const parseResult = v.safeParse(v.object(schema), doc);
          if (!parseResult.success) {
            throw new Error(
              `Document in collection ${collectionName} failed schema validation: ${parseResult.issues.map(i => i.message).join(', ')}`
            );
          }
          return parseResult.output;
        });
      }

      const seedRule: SeedCollectionRule = {
        type: 'seed_collection',
        collectionName,
        documents: validatedDocs,
      };

      state.operations.push(seedRule);
      return this;
    },

    transform(rule: TransformRule): MigrationCollectionBuilder {
      const transformRule: TransformCollectionRule = {
        type: 'transform_collection',
        collectionName,
        up: rule.up,
        down: rule.down,
      };

      state.operations.push(transformRule);
      return this;
    },

    done(): MigrationBuilder {
      return createMigrationBuilder(state, options);
    }
  };
}

/**
 * Creates a multi-collection type builder
 */
function createMultiCollectionTypeBuilder(
  state: MigrationState,
  multiCollectionName: string,
  typeName: string,
  parentBuilder: MultiCollectionBuilder
): MultiCollectionTypeBuilder {
  return {
    transform(rule: TransformRule): MultiCollectionTypeBuilder {
      const transformRule: TransformMultiCollectionTypeRule = {
        type: 'transform_multicollection_type',
        multiCollectionName,
        typeName,
        up: rule.up,
        down: rule.down,
      };

      state.operations.push(transformRule);
      return this;
    },

    end(): MultiCollectionBuilder {
      return parentBuilder;
    }
  };
}

/**
 * Creates a multi-collection builder
 */
function createMultiCollectionBuilder(
  state: MigrationState,
  multiCollectionName: string,
  mainBuilder: MigrationBuilder
): MultiCollectionBuilder {
  const builder: MultiCollectionBuilder = {
    type(typeName: string): MultiCollectionTypeBuilder {
      return createMultiCollectionTypeBuilder(state, multiCollectionName, typeName, builder);
    },

    end(): MigrationBuilder {
      return mainBuilder;
    }
  };

  return builder;
}

/**
 * Creates a multi-collection instance builder
 */
function createMultiCollectionInstanceBuilder(
  state: MigrationState,
  multiCollectionName: string,
  instanceName: string,
  mainBuilder: MigrationBuilder,
  options: MigrationBuilderOptions
): MultiCollectionInstanceBuilder {
  return {
    seedType(typeName: string, documents: readonly unknown[]): MultiCollectionInstanceBuilder {
      // Validate documents against schema if available, similar to collection seed
      const schema = options.schemas.multiCollections?.[multiCollectionName]?.[typeName];
      let validatedDocs: readonly unknown[] = documents;
      
      if (schema) {
        validatedDocs = documents.map(doc => {
          const parseResult = v.safeParse(v.object(schema), doc);
          if (!parseResult.success) {
            throw new Error(
              `Document in multi-collection ${multiCollectionName}.${typeName} failed schema validation: ${parseResult.issues.map(i => i.message).join(', ')}`
            );
          }
          return parseResult.output;
        });
      }

      const seedRule: SeedMultiCollectionInstanceRule = {
        type: 'seed_multicollection_instance',
        multiCollectionName,
        instanceName,
        typeName,
        documents: validatedDocs,
      };

      state.operations.push(seedRule);
      return this;
    },

    end(): MigrationBuilder {
      return mainBuilder;
    }
  };
}

/**
 * Creates the main migration builder with functional operations
 */
function createMigrationBuilder(
  state: MigrationState,
  options: MigrationBuilderOptions
): MigrationBuilder {
  const builder: MigrationBuilder = {
    createCollection(name: string): MigrationCollectionBuilder {
      const createRule: CreateCollectionRule = {
        type: 'create_collection',
        collectionName: name,
      };

      state.operations.push(createRule);

      // Creating a collection makes the migration irreversible
      state.mark({ type: 'irreversible' });

      return createCollectionBuilder(state, name, options);
    },

    collection(name: string): MigrationCollectionBuilder {
      return createCollectionBuilder(state, name, options);
    },

    multiCollection(name: string): MultiCollectionBuilder {
      return createMultiCollectionBuilder(state, name, builder);
    },

    createMultiCollectionInstance(multiCollectionName: string, instanceName: string): MultiCollectionInstanceBuilder {
      const createRule: CreateMultiCollectionInstanceRule = {
        type: 'create_multicollection_instance',
        multiCollectionName,
        instanceName,
      };

      state.operations.push(createRule);

      // Creating a multi-collection instance makes the migration irreversible
      state.mark({ type: 'irreversible' });

      return createMultiCollectionInstanceBuilder(state, multiCollectionName, instanceName, builder, options);
    },

    multiCollectionInstance(multiCollectionName: string, instanceName: string): MultiCollectionInstanceBuilder {
      return createMultiCollectionInstanceBuilder(state, multiCollectionName, instanceName, builder, options);
    },

    compile(): MigrationState {
      return state;
    }
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
  initState?: MigrationState
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
export function isCreateCollectionRule(operation: MigrationRule): operation is CreateCollectionRule {
  return operation.type === 'create_collection';
}

/**
 * Type guard to check if an operation is a seed collection rule
 * 
 * @param operation - The migration operation to check
 * @returns True if the operation is a seed collection rule
 */
export function isSeedCollectionRule(operation: MigrationRule): operation is SeedCollectionRule {
  return operation.type === 'seed_collection';
}

/**
 * Type guard to check if an operation is a transform collection rule
 * 
 * @param operation - The migration operation to check
 * @returns True if the operation is a transform collection rule
 */
export function isTransformCollectionRule(operation: MigrationRule): operation is TransformCollectionRule {
  return operation.type === 'transform_collection';
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
    isIrreversible: state.hasProperty('irreversible'),
    properties: state.properties.map(p => p.type),
  };

  for (const operation of state.operations) {
    switch (operation.type) {
      case 'create_collection':
        summary.creates++;
        break;
      case 'seed_collection':
        summary.seeds++;
        break;
      case 'transform_collection':
        summary.transforms++;
        break;
    }
  }

  return summary;
}
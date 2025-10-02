/**
 * @fileoverview Multi-Collection Model System
 *
 * This module provides a model/template system for multi-collections, enabling:
 * - Reusable schema definitions across multiple collection instances
 * - Type-safe model creation and instance management
 * - Version tracking for migration consistency
 *
 * @module
 */

import type * as v from './schema.ts';

/**
 * Base schema type constraint
 */
type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

/**
 * Schema definition for a multi-collection model
 */
export type MultiCollectionSchema = Record<string, Record<string, AnySchema>>;

/**
 * Multi-collection model metadata
 */
export type MultiCollectionModel<T extends MultiCollectionSchema = MultiCollectionSchema> = {
  /** Unique name of the model (used as template identifier) */
  readonly name: string;

  /** Schema definition for each document type in this model */
  readonly schema: T;

  /** Model version for tracking schema evolution */
  readonly version: string;

  /** Metadata for additional model information */
  readonly metadata?: Record<string, unknown>;

  /**
   * Expose the schema in the format expected by schemas.ts
   * Returns an object with the model name as key and schema as value
   */
  expose(): { [K in typeof name]: T };

  /**
   * Get a summary of the model structure
   */
  getSummary(): {
    name: string;
    version: string;
    types: string[];
    fieldCount: number;
  };
};

/**
 * Options for creating a multi-collection model
 */
export type CreateMultiCollectionModelOptions<T extends MultiCollectionSchema> = {
  /** Schema definition */
  schema: T;

  /** Optional version identifier (defaults to '1.0.0') */
  version?: string;

  /** Optional metadata */
  metadata?: Record<string, unknown>;
};

/**
 * Creates a multi-collection model (template) that can be reused across multiple instances
 *
 * A model defines the structure and types that can be stored in collections using this template.
 * Multiple physical MongoDB collections can use the same model.
 *
 * @param name - Unique identifier for this model
 * @param options - Model configuration including schema and optional version
 * @returns A reusable multi-collection model
 *
 * @example
 * ```typescript
 * import { createMultiCollectionModel } from "@diister/mongodbee";
 * import * as v from "valibot";
 *
 * // Define a reusable catalog model
 * const catalogModel = createMultiCollectionModel("catalog", {
 *   schema: {
 *     product: {
 *       name: v.string(),
 *       price: v.number(),
 *       category: v.string()
 *     },
 *     category: {
 *       name: v.string(),
 *       parentId: v.optional(v.string())
 *     }
 *   },
 *   version: "1.0.0"
 * });
 *
 * // Use in schemas.ts
 * export const schemas = {
 *   collections: {},
 *   multiCollections: {
 *     ...catalogModel.expose() // { "catalog": { product: {...}, category: {...} } }
 *   }
 * };
 *
 * // Create instances using this model
 * const louvre = await newMultiCollection(db, "catalog_louvre", catalogModel);
 * const orsay = await multiCollection(db, "catalog_orsay", catalogModel);
 * ```
 */
export function createMultiCollectionModel<const T extends MultiCollectionSchema>(
  name: string,
  options: CreateMultiCollectionModelOptions<T>
): MultiCollectionModel<T> {
  const { schema, version = '1.0.0', metadata } = options;

  // Validate model name
  if (!name || typeof name !== 'string') {
    throw new Error('Model name must be a non-empty string');
  }

  // Validate schema has at least one type
  if (Object.keys(schema).length === 0) {
    throw new Error(`Model "${name}" must define at least one document type`);
  }

  const model: MultiCollectionModel<T> = {
    name,
    schema,
    version,
    metadata,

    expose() {
      return { [this.name]: this.schema } as { [K in typeof this.name]: T };
    },

    getSummary() {
      const types = Object.keys(this.schema);
      const fieldCount = types.reduce((acc, type) => {
        return acc + Object.keys(this.schema[type]).length;
      }, 0);

      return {
        name: this.name,
        version: this.version,
        types,
        fieldCount,
      };
    },
  };

  return Object.freeze(model);
}

/**
 * Type guard to check if a value is a MultiCollectionModel
 *
 * @param value - Value to check
 * @returns True if value is a MultiCollectionModel
 */
export function isMultiCollectionModel(value: unknown): value is MultiCollectionModel {
  return (
    typeof value === 'object' &&
    value !== null &&
    'name' in value &&
    'schema' in value &&
    'version' in value &&
    'expose' in value &&
    typeof (value as MultiCollectionModel).expose === 'function'
  );
}

/**
 * Validates that a schema matches a model definition
 *
 * @param schema - Schema to validate
 * @param model - Model to validate against
 * @returns True if schema matches model structure
 */
export function validateSchemaMatchesModel<T extends MultiCollectionSchema>(
  schema: MultiCollectionSchema,
  model: MultiCollectionModel<T>
): boolean {
  const modelTypes = Object.keys(model.schema);
  const schemaTypes = Object.keys(schema);

  // Check all model types are present
  for (const type of modelTypes) {
    if (!schemaTypes.includes(type)) {
      return false;
    }
  }

  return true;
}

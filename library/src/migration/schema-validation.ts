/**
 * @fileoverview Schema validation utilities
 *
 * This module provides functions to validate that the last migration's schema
 * matches the current project schema defined in schemas.ts
 *
 * @module
 */

import * as path from "@std/path";
import type { MigrationDefinition, SchemasDefinition } from "./types.ts";

/**
 * Loads the current project schema from schemas.ts
 *
 * @param schemaPath - Path to the schemas.ts file
 * @returns The loaded schema definition
 */
export async function loadProjectSchema(
  schemaPath: string,
): Promise<SchemasDefinition> {
  const fullPath = path.resolve(schemaPath);

  // Convert to file:// URL for dynamic import
  const importPath = Deno.build.os === "windows"
    ? `file:///${fullPath.replace(/\\/g, "/")}`
    : `file://${fullPath}`;

  try {
    const module = await import(importPath);

    if (!module.schemas) {
      throw new Error(`Schema file ${schemaPath} does not export 'schemas'`);
    }

    return module.schemas;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Failed to load project schema from ${schemaPath}: ${message}`,
    );
  }
}

/**
 * Properties to remove from Valibot schemas during simplification
 */
const CLEANUP_PROPERTIES = [
  "~standard",
  "async",
  "expects",
  "message",
  "default",
];

/**
 * Schema kinds to ignore during simplification
 */
const CLEANUP_KINDS = [
  "transformation",
  "metadata",
];

/**
 * Type-specific handlers for simplifying Valibot schemas
 */
const simplifyHandlers: Record<string, (schema: any) => any> = {
  "map": (schema: any) => ({
    ...schema,
    key: simplifySchema(schema.key),
    value: simplifySchema(schema.value),
  }),
  "record": (schema: any) => ({
    ...schema,
    key: simplifySchema(schema.key),
    value: simplifySchema(schema.value),
  }),
  "set": (schema: any) => ({
    ...schema,
    value: simplifySchema(schema.value),
  }),
  "object": (schema: any) => ({
    ...schema,
    entries: Object.fromEntries(
      Object.entries(schema.entries).map(
        ([key, value]) => [key, simplifySchema(value)],
      ),
    ),
  }),
  "loose_object": (schema: any) => simplifyHandlers["object"](schema),
  "object_with_rest": (schema: any) => ({
    ...simplifyHandlers["object"](schema),
    rest: simplifySchema(schema.rest),
  }),
  "strict_object": (schema: any) => simplifyHandlers["object"](schema),
  "array": (schema: any) => ({
    ...schema,
    item: simplifySchema(schema.item),
  }),
  "tuple": (schema: any) => ({
    ...schema,
    items: schema.items.map((s: any) => simplifySchema(s)),
  }),
  "loose_tuple": (schema: any) => simplifyHandlers["tuple"](schema),
  "strict_tuple": (schema: any) => simplifyHandlers["tuple"](schema),
  "tuple_with_rest": (schema: any) => ({
    ...simplifyHandlers["tuple"](schema),
    rest: simplifySchema(schema.rest),
  }),
  "union": (schema: any) => schema.options.map((s: any) => simplifySchema(s)),
  "intersect": (schema: any) => ({
    ...schema,
    options: schema.options.map((s: any) => simplifySchema(s)),
  }),
  "variant": (schema: any) => ({
    ...schema,
    options: schema.options.map((s: any) => simplifySchema(s)),
  }),
  "#wrapped": (schema: any) => ({
    ...schema,
    wrapped: simplifySchema(schema.wrapped),
  }),
  "optional": (schema: any) => simplifyHandlers["#wrapped"](schema),
  "non_optional": (schema: any) => simplifyHandlers["#wrapped"](schema),
  "undefinedable": (schema: any) => simplifyHandlers["#wrapped"](schema),
  "nullable": (schema: any) => simplifyHandlers["#wrapped"](schema),
  "non_nullable": (schema: any) => simplifyHandlers["#wrapped"](schema),
  "nullish": (schema: any) => simplifyHandlers["#wrapped"](schema),
  "non_nullish": (schema: any) => simplifyHandlers["#wrapped"](schema),
  "exact_optional": (schema: any) => simplifyHandlers["#wrapped"](schema),
};

/**
 * Simplifies a Valibot schema by removing internal metadata and recursively
 * processing nested schemas
 *
 * @param schema - Valibot schema object
 * @returns Simplified schema with metadata removed
 */
export function simplifySchema(schema: any): any {
  if (!schema || typeof schema !== "object") {
    return schema;
  }

  // Skip schemas marked for cleanup
  if (CLEANUP_KINDS.includes(schema.kind)) {
    return undefined;
  }

  // Create a shallow copy to avoid mutating the original
  const simplified = { ...schema };

  // Remove cleanup properties
  for (const prop of CLEANUP_PROPERTIES) {
    delete simplified[prop];
  }

  // Process pipe array if present
  if ("pipe" in simplified && Array.isArray(simplified.pipe)) {
    simplified.pipe = simplified.pipe
      .map((s: any) => simplifySchema(s))
      .filter((s: any) => s !== undefined);
  }

  // Use type-specific handler if available
  const handler = simplifyHandlers[schema.type];
  if (handler) {
    return handler(simplified);
  }

  return simplified;
}

/**
 * Flattens a simplified schema into dot-notation keys
 *
 * @param schema - Simplified schema object
 * @returns Flattened object with dot-notation paths as keys
 */
function flattenSchema(schema: any): Record<string, any> {
  const result: Record<string, any> = {};

  if (!schema || typeof schema !== "object") {
    return result;
  }

  const keys = Object.keys(schema);
  for (const key of keys) {
    // Skip ~standard properties as they add noise
    if (key === "~standard" || key.endsWith(".~standard")) {
      continue;
    }

    const value = schema[key];

    if (Array.isArray(value)) {
      for (const [index, subValue] of value.entries()) {
        if (typeof subValue === "object" && subValue !== null) {
          for (const [subKey, subSubValue] of Object.entries(flattenSchema(subValue))) {
            // Skip ~standard in nested paths
            if (subKey.includes(".~standard") || subKey === "~standard") {
              continue;
            }
            result[`${key}[${index}].${subKey}`] = subSubValue;
          }
        } else {
          result[`${key}[${index}]`] = subValue;
        }
      }
    } else if (typeof value === "object" && value !== null) {
      for (const [subKey, subValue] of Object.entries(flattenSchema(value))) {
        // Skip ~standard in nested paths
        if (subKey.includes(".~standard") || subKey === "~standard") {
          continue;
        }
        result[`${key}.${subKey}`] = subValue;
      }
    } else {
      result[key] = value;
    }
  }

  return result;
}

/**
 * Compares two flattened schemas and returns differences
 *
 * @param schema1 - First flattened schema
 * @param schema2 - Second flattened schema
 * @returns Array of differences with key and before/after values
 */
function diffSchemas(
  schema1: Record<string, any>,
  schema2: Record<string, any>,
): Array<{ key: string; before?: any; after?: any }> {
  const diffs: Array<{ key: string; before?: any; after?: any }> = [];
  const keys = new Set([...Object.keys(schema1), ...Object.keys(schema2)]);

  // Properties to skip in diff output (too verbose/technical for users)
  const skipProperties = ["kind", ...CLEANUP_PROPERTIES];

  for (const key of keys) {
    // Skip if key matches or ends with any skip property
    const shouldSkip = skipProperties.some((prop) =>
      key === prop || key.endsWith(`.${prop}`)
    );

    if (shouldSkip) {
      continue;
    }

    const valA = schema1[key];
    const valB = schema2[key];

    // Use JSON.stringify for deep comparison
    if (JSON.stringify(valA) !== JSON.stringify(valB)) {
      diffs.push({
        key,
        ...(key in schema1 ? { before: valA } : {}),
        ...(key in schema2 ? { after: valB } : {}),
      });
    }
  }

  return diffs;
}

/**
 * Compares two schema objects for equality using simplified representation
 *
 * @param schema1 - First schema
 * @param schema2 - Second schema
 * @returns True if schemas are equal (same fields and types)
 */
function schemasEqual(schema1: any, schema2: any): boolean {
  const simple1 = flattenSchema(simplifySchema(schema1));
  const simple2 = flattenSchema(simplifySchema(schema2));

  const diffs = diffSchemas(simple1, simple2);
  return diffs.length === 0;
}

/**
 * Formats schema differences as a human-readable string with color codes
 *
 * @param diffs - Array of differences from diffSchemas
 * @returns Formatted string with ANSI color codes
 */
function formatSchemaDifferences(
  diffs: Array<{ key: string; before?: any; after?: any }>,
): string {
  if (diffs.length === 0) {
    return "\x1b[32m    ✓ No differences\x1b[0m";
  }

  const lines: string[] = [];

  for (const diff of diffs) {
    const hasBefore = "before" in diff;
    const hasAfter = "after" in diff;

    if (!hasBefore && hasAfter) {
      // Added field
      lines.push(
        `\x1b[32m    + ${diff.key} = ${JSON.stringify(diff.after)}\x1b[0m`,
      );
    } else if (hasBefore && !hasAfter) {
      // Removed field
      lines.push(
        `\x1b[31m    - ${diff.key} = ${JSON.stringify(diff.before)}\x1b[0m`,
      );
    } else {
      // Modified field
      lines.push(`\x1b[33m    ~ ${diff.key}\x1b[0m`);
      lines.push(`\x1b[31m      - ${JSON.stringify(diff.before)}\x1b[0m`);
      lines.push(`\x1b[32m      + ${JSON.stringify(diff.after)}\x1b[0m`);
    }
  }

  return lines.join("\n");
}

/**
 * Validates that the last migration's schema matches the project schema
 *
 * @param lastMigration - The last migration in the chain
 * @param projectSchema - The project schema from schemas.ts
 * @returns Validation result with errors if any
 */
export function validateLastMigrationMatchesProjectSchema(
  lastMigration: MigrationDefinition,
  projectSchema: SchemasDefinition,
): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];

  // Check collections
  {
    const migrationCollections = lastMigration.schemas.collections || {};
    const projectCollections = projectSchema.collections || {};
    if (!schemasEqual(migrationCollections, projectCollections)) {
      const migrationNameSet = new Set(Object.keys(migrationCollections));
      const projectNameSet = new Set(Object.keys(projectCollections));

      const missingInMigration = projectNameSet.difference(migrationNameSet);
      const extraInMigration = migrationNameSet.difference(projectNameSet);

      if (missingInMigration.size > 0) {
        errors.push(
          `Collections missing in last migration: ${[...missingInMigration].sort().join(", ")}`,
        );
      }
      if (extraInMigration.size > 0) {
        errors.push(
          `Extra collections detected in last migration: ${[...extraInMigration].sort().join(", ")}`,
        );
      }

      if (missingInMigration.size === 0 && extraInMigration.size === 0) {
        // Same collection names but different schemas - find which ones differ
        for (const name of migrationNameSet) {
          const migrationSchema = migrationCollections[name];
          const projectSchema = projectCollections[name];

          if (!schemasEqual(migrationSchema, projectSchema)) {
            const simple1 = flattenSchema(simplifySchema(migrationSchema));
            const simple2 = flattenSchema(simplifySchema(projectSchema));
            const diffs = diffSchemas(simple1, simple2);

            errors.push(
              `Collection "${name}" schema differs:\n${formatSchemaDifferences(diffs)}`,
            );
          }
        }
      }
    }
  }

  // Check multiCollections if they exist
  {
    const migrationMultiCollections = lastMigration.schemas.multiCollections || {};
    const projectMultiCollections = projectSchema.multiCollections || {};

    if (!schemasEqual(migrationMultiCollections, projectMultiCollections)) {
      const migrationNameSet = new Set(Object.keys(migrationMultiCollections));
      const projectNameSet = new Set(Object.keys(projectMultiCollections));

      const missingInMigration = projectNameSet.difference(migrationNameSet);
      const extraInMigration = migrationNameSet.difference(projectNameSet);

      if (missingInMigration.size > 0) {
        errors.push(
          `Multi-collections missing in last migration: ${[...missingInMigration].sort().join(", ")}`,
        );
      }
      if (extraInMigration.size > 0) {
        errors.push(
          `Extra multi-collections in last migration not in project schema: ${[...extraInMigration].sort().join(", ")}`,
        );
      }

      if (missingInMigration.size === 0 && extraInMigration.size === 0) {
        // Same multi-collection names but different schemas
        for (const name of migrationNameSet) {
          const migrationSchema = migrationMultiCollections[name];
          const projectSchema = projectMultiCollections[name];

          const migrationTypesSet = new Set(Object.keys(migrationSchema));
          const projectTypesSet = new Set(Object.keys(projectSchema));

          const missingTypesInMigration = projectTypesSet.difference(migrationTypesSet);
          const extraTypesInMigration = migrationTypesSet.difference(projectTypesSet);

          if (missingTypesInMigration.size > 0) {
            errors.push(
              `Multi-collection "${name}" is missing types in last migration: ${[...missingTypesInMigration].sort().join(", ")}`,
            );
          }

          if (extraTypesInMigration.size > 0) {
            errors.push(
              `Multi-collection "${name}" has extra types in last migration not in project schema: ${[...extraTypesInMigration].sort().join(", ")}`,
            );
          }

          if (missingTypesInMigration.size > 0 || extraTypesInMigration.size > 0) {
            // Skip further type comparison if types differ
            continue;
          }

          const commonTypes = [...projectTypesSet.intersection(migrationTypesSet)];
          for(const typeName of commonTypes) {
            const migrationTypeSchema = migrationSchema[typeName];
            const projectTypeSchema = projectSchema[typeName];
            if (!schemasEqual(migrationTypeSchema, projectTypeSchema)) {
              const simple1 = flattenSchema(simplifySchema(migrationTypeSchema));
              const simple2 = flattenSchema(simplifySchema(projectTypeSchema));
              const diffs = diffSchemas(simple1, simple2);
              errors.push(
                `Multi-collection "${name}" type "${typeName}" schema differs:\n${formatSchemaDifferences(diffs)}`,
              );
            }
          }
        }
      }
    }
  }

  // Check multiModels if they exist
  {
    const migrationMultiModels = lastMigration.schemas.multiModels || {};
    const projectMultiModels = projectSchema.multiModels || {};
    if (!schemasEqual(migrationMultiModels, projectMultiModels)) {
      const migrationNameSet = new Set(Object.keys(migrationMultiModels));
      const projectNameSet = new Set(Object.keys(projectMultiModels));

      const missingInMigration = projectNameSet.difference(migrationNameSet);
      const extraInMigration = migrationNameSet.difference(projectNameSet);

      if (missingInMigration.size > 0) {
        errors.push(
          `Multi-models missing in last migration: ${[...missingInMigration].sort().join(", ")}`,
        );
      }
      if (extraInMigration.size > 0) {
        errors.push(
          `Extra multi-models in last migration not in project schema: ${[...extraInMigration].sort().join(", ")}`,
        );
      }

      if (missingInMigration.size === 0 && extraInMigration.size === 0) {
        // Same multi-model names but different schemas
        for (const name of migrationNameSet) {
          const migrationSchema = migrationMultiModels[name];
          const projectSchema = projectMultiModels[name];

          const migrationTypesSet = new Set(Object.keys(migrationSchema));
          const projectTypesSet = new Set(Object.keys(projectSchema));

          const missingTypesInMigration = projectTypesSet.difference(migrationTypesSet);
          const extraTypesInMigration = migrationTypesSet.difference(projectTypesSet);

          if (missingTypesInMigration.size > 0) {
            errors.push(
              `Multi-model "${name}" is missing types in last migration: ${[...missingTypesInMigration].sort().join(", ")}`,
            );
          }
          if (extraTypesInMigration.size > 0) {
            errors.push(
              `Multi-model "${name}" has extra types in last migration not in project schema: ${[...extraTypesInMigration].sort().join(", ")}`,
            );
          }

          if (missingTypesInMigration.size > 0 || extraTypesInMigration.size > 0) {
            // Skip further type comparison if types differ
            continue;
          }

          const commonTypes = [...projectTypesSet.intersection(migrationTypesSet)];
          for(const typeName of commonTypes) {
            const migrationTypeSchema = migrationSchema[typeName];
            const projectTypeSchema = projectSchema[typeName];
            if (!schemasEqual(migrationTypeSchema, projectTypeSchema)) {
              const simple1 = flattenSchema(simplifySchema(migrationTypeSchema));
              const simple2 = flattenSchema(simplifySchema(projectTypeSchema));
              const diffs = diffSchemas(simple1, simple2);
              errors.push(
                `Multi-model "${name}" type "${typeName}" schema differs:\n${formatSchemaDifferences(diffs)}`,
              );
            }
          }
        }
      }
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

/**
 * Validates the entire migration chain including schema consistency
 *
 * @param migrations - Array of migrations
 * @param projectSchemaPath - Path to schemas.ts
 * @returns Validation result
 */
export async function validateMigrationChainWithProjectSchema(
  migrations: MigrationDefinition[],
  projectSchemaPath: string,
): Promise<{
  valid: boolean;
  errors: string[];
  warnings: string[];
}> {
  const errors: string[] = [];
  const warnings: string[] = [];

  if (migrations.length === 0) {
    warnings.push(
      'No migrations found. Run "mongodbee generate" to create your first migration.',
    );
    return { valid: true, errors, warnings };
  }

  // Load project schema
  let projectSchema: SchemasDefinition;
  try {
    projectSchema = await loadProjectSchema(projectSchemaPath);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    errors.push(`Failed to load project schema: ${message}`);
    return { valid: false, errors, warnings };
  }

  // Check if project schema is empty
  const hasCollections = Object.keys(projectSchema.collections ?? {}).length > 0;
  const hasMultiCollections = Object.keys(projectSchema.multiCollections || {}).length > 0;
  const hasMultiModels = Object.keys(projectSchema.multiModels || {}).length > 0;

  if (!hasCollections && !hasMultiModels && !hasMultiCollections) {
    warnings.push("Project schema is empty. Define your schemas in schemas.ts");
  }

  // Validate last migration matches project schema
  const lastMigration = migrations[migrations.length - 1];
  const validation = validateLastMigrationMatchesProjectSchema(
    lastMigration,
    projectSchema,
  );

  if (!validation.valid) {
    errors.push(...validation.errors);
    errors.push(
      `\nThe last migration schema must match your project schema (schemas.ts).\n` +
        `Update your schemas.ts to match the last migration, or create a new migration to reflect your current schema.`,
    );
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

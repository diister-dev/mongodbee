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
 * Gets a simplified type string from a Valibot schema
 *
 * @param schema - Valibot schema object
 * @returns Simplified type string like "string", "number?", "string[]"
 */
function getSchemaTypeString(schema: any): string {
  if (!schema || typeof schema !== "object") {
    return "unknown";
  }

  const type = schema.type;

  switch (type) {
    case "optional":
      return `${getSchemaTypeString(schema.wrapped)}?`;
    case "nullable":
      return `${getSchemaTypeString(schema.wrapped)} | null`;
    case "nullish":
      return `${getSchemaTypeString(schema.wrapped)}?`;
    case "array":
      return `${getSchemaTypeString(schema.item)}[]`;
    case "union":
      if (Array.isArray(schema.options)) {
        return schema.options.map(getSchemaTypeString).join(" | ");
      }
      return "union";
    case "intersect":
      if (Array.isArray(schema.options)) {
        return schema.options.map(getSchemaTypeString).join(" & ");
      }
      return "intersect";
    case "picklist":
      if (Array.isArray(schema.options)) {
        return schema.options.map((o: string) => `"${o}"`).join(" | ");
      }
      return "picklist";
    case "literal":
      return JSON.stringify(schema.literal);
    case "object":
      return "object";
    case "record":
      return `Record<string, ${getSchemaTypeString(schema.value)}>`;
    case "tuple":
      if (Array.isArray(schema.items)) {
        return `[${schema.items.map(getSchemaTypeString).join(", ")}]`;
      }
      return "tuple";
    default:
      return type || "unknown";
  }
}

/**
 * Simplifies a Valibot schema to a flat representation with field paths and types
 * This avoids exposing internal Valibot metadata like ~run, ~standard, etc.
 *
 * @param schema - Schema object (collection or model schema)
 * @param prefix - Current path prefix
 * @returns Flattened object with paths as keys and type strings as values
 */
export function simplifySchema(schema: any, prefix = ""): Record<string, string> {
  const result: Record<string, string> = {};

  if (!schema || typeof schema !== "object") {
    return result;
  }

  for (const key in schema) {
    // Skip internal Valibot properties
    if (key.startsWith("~") || key === "async" || key === "kind" || key === "type" ||
        key === "message" || key === "pipe" || key === "reference" || key === "expects" ||
        key === "requirement" || key === "wrapped" || key === "item" || key === "options" ||
        key === "entries" || key === "rest" || key === "default" || key === "literal" ||
        key === "value" || key === "items") {
      continue;
    }

    const value = schema[key];
    const newKey = prefix ? `${prefix}.${key}` : key;

    // Check if this is a Valibot schema (has 'kind' and 'type' properties)
    if (value && typeof value === "object" && value.kind === "schema") {
      result[newKey] = getSchemaTypeString(value);

      // If it's an object schema, recurse into its entries
      if (value.type === "object" && value.entries) {
        Object.assign(result, simplifySchema(value.entries, newKey));
      }
    } else if (value && typeof value === "object" && !Array.isArray(value)) {
      // This might be a nested structure (like multiModels with models inside)
      Object.assign(result, simplifySchema(value, newKey));
    }
  }

  return result;
}

/**
 * Flattens an object into dot-notation keys
 * @deprecated Use simplifySchema for Valibot schema comparison
 *
 * @param obj - Object to flatten
 * @param prefix - Prefix for keys
 * @returns Flattened object
 */
function flattenObject(obj: any, prefix = ""): Record<string, any> {
  const result: Record<string, any> = {};

  for (const key in obj) {
    const value = obj[key];
    const newKey = prefix ? `${prefix}.${key}` : key;

    if (
      value && typeof value === "object" && !Array.isArray(value) &&
      value.constructor === Object
    ) {
      Object.assign(result, flattenObject(value, newKey));
    } else {
      result[newKey] = value;
    }
  }

  return result;
}

/**
 * Compares two schema objects for equality using simplified representation
 *
 * @param schema1 - First schema
 * @param schema2 - Second schema
 * @returns True if schemas are equal (same fields and types)
 */
function schemasEqual(schema1: any, schema2: any): boolean {
  const simple1 = simplifySchema(schema1);
  const simple2 = simplifySchema(schema2);

  const keys1 = Object.keys(simple1).sort();
  const keys2 = Object.keys(simple2).sort();

  if (keys1.length !== keys2.length) {
    return false;
  }

  // Check both keys and values (types)
  return keys1.every((key, i) => key === keys2[i] && simple1[key] === simple2[key]);
}

/**
 * Finds detailed differences between two schemas
 *
 * @param schema1 - First schema (migration)
 * @param schema2 - Second schema (project)
 * @returns Object with added, removed, and modified fields
 */
function findSchemaDifferences(
  schema1: any,
  schema2: any,
): { added: string[]; removed: string[]; modified: string[] } {
  const simple1 = simplifySchema(schema1);
  const simple2 = simplifySchema(schema2);

  const keys1 = new Set(Object.keys(simple1));
  const keys2 = new Set(Object.keys(simple2));

  const added = [...keys2].filter((k) => !keys1.has(k)).sort();
  const removed = [...keys1].filter((k) => !keys2.has(k)).sort();

  // Find fields that exist in both but have different types
  const modified: string[] = [];
  for (const key of keys1) {
    if (keys2.has(key) && simple1[key] !== simple2[key]) {
      modified.push(`${key}: ${simple1[key]} → ${simple2[key]}`);
    }
  }

  return { added, removed, modified };
}

/**
 * Finds which items in a collection have different schemas
 *
 * @param migrationItems - Items from migration schema
 * @param projectItems - Items from project schema
 * @returns Array of item names with differences and their details
 */
function findDifferingItems(
  migrationItems: Record<string, any>,
  projectItems: Record<string, any>,
): { name: string; added: string[]; removed: string[]; modified: string[] }[] {
  const differences: { name: string; added: string[]; removed: string[]; modified: string[] }[] = [];

  // Check items that exist in both
  const migrationNames = new Set(Object.keys(migrationItems));
  const projectNames = new Set(Object.keys(projectItems));

  for (const name of migrationNames) {
    if (projectNames.has(name)) {
      const migrationSchema = migrationItems[name];
      const projectSchema = projectItems[name];

      if (!schemasEqual(migrationSchema, projectSchema)) {
        const diff = findSchemaDifferences(migrationSchema, projectSchema);
        differences.push({
          name,
          added: diff.added,
          removed: diff.removed,
          modified: diff.modified,
        });
      }
    }
  }

  return differences;
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
        const differingCollections = findDifferingItems(
          migrationCollections,
          projectCollections,
        );

        for (const diff of differingCollections) {
          const details: string[] = [];
          if (diff.added.length > 0) {
            details.push(`added: ${diff.added.join(", ")}`);
          }
          if (diff.removed.length > 0) {
            details.push(`removed: ${diff.removed.join(", ")}`);
          }
          if (diff.modified.length > 0) {
            details.push(`changed: ${diff.modified.join(", ")}`);
          }
          errors.push(
            `Collection "${diff.name}" schema differs: ${details.join("; ")}`,
          );
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
        const differingMultiCollections = findDifferingItems(
          migrationMultiCollections,
          projectMultiCollections,
        );

        for (const diff of differingMultiCollections) {
          const details: string[] = [];
          if (diff.added.length > 0) {
            details.push(`added: ${diff.added.join(", ")}`);
          }
          if (diff.removed.length > 0) {
            details.push(`removed: ${diff.removed.join(", ")}`);
          }
          if (diff.modified.length > 0) {
            details.push(`changed: ${diff.modified.join(", ")}`);
          }
          errors.push(
            `Multi-collection "${diff.name}" schema differs: ${details.join("; ")}`,
          );
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
        const differingMultiModels = findDifferingItems(
          migrationMultiModels,
          projectMultiModels,
        );

        for (const diff of differingMultiModels) {
          const details: string[] = [];
          if (diff.added.length > 0) {
            details.push(`added: ${diff.added.join(", ")}`);
          }
          if (diff.removed.length > 0) {
            details.push(`removed: ${diff.removed.join(", ")}`);
          }
          if (diff.modified.length > 0) {
            details.push(`changed: ${diff.modified.join(", ")}`);
          }
          errors.push(
            `Multi-model "${diff.name}" schema differs: ${details.join("; ")}`,
          );
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

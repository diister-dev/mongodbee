/**
 * @fileoverview Schema validation utilities
 *
 * This module provides functions to validate that the last migration's schema
 * matches the current project schema defined in schemas.ts
 *
 * @module
 */

import * as path from "@std/path";
import type { MigrationDefinition, SchemasDefinition } from './types.ts';

/**
 * Loads the current project schema from schemas.ts
 *
 * @param schemaPath - Path to the schemas.ts file
 * @returns The loaded schema definition
 */
export async function loadProjectSchema(schemaPath: string): Promise<SchemasDefinition> {
  const fullPath = path.resolve(schemaPath);

  // Convert to file:// URL for dynamic import
  const importPath = Deno.build.os === 'windows'
    ? `file:///${fullPath.replace(/\\/g, '/')}`
    : `file://${fullPath}`;

  try {
    const module = await import(`${importPath}?t=${Date.now()}`);

    if (!module.schemas) {
      throw new Error(`Schema file ${schemaPath} does not export 'schemas'`);
    }

    return module.schemas;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to load project schema from ${schemaPath}: ${message}`);
  }
}

/**
 * Flattens an object into dot-notation keys
 *
 * @param obj - Object to flatten
 * @param prefix - Prefix for keys
 * @returns Flattened object
 */
function flattenObject(obj: any, prefix = ''): Record<string, any> {
  const result: Record<string, any> = {};

  for (const key in obj) {
    const value = obj[key];
    const newKey = prefix ? `${prefix}.${key}` : key;

    if (value && typeof value === 'object' && !Array.isArray(value) && value.constructor === Object) {
      Object.assign(result, flattenObject(value, newKey));
    } else {
      result[newKey] = value;
    }
  }

  return result;
}

/**
 * Compares two schema objects for equality
 *
 * @param schema1 - First schema
 * @param schema2 - Second schema
 * @returns True if schemas are equal (same keys)
 */
function schemasEqual(schema1: any, schema2: any): boolean {
  const flat1 = flattenObject(schema1);
  const flat2 = flattenObject(schema2);

  const keys1 = Object.keys(flat1).sort();
  const keys2 = Object.keys(flat2).sort();

  if (keys1.length !== keys2.length) {
    return false;
  }

  return keys1.every((key, i) => key === keys2[i]);
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
  projectSchema: SchemasDefinition
): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];

  // Check collections
  if (!schemasEqual(lastMigration.schemas.collections, projectSchema.collections)) {
    errors.push(
      `Last migration collections schema does not match project schema.\n` +
      `  Migration: ${Object.keys(lastMigration.schemas.collections).sort().join(', ')}\n` +
      `  Project:   ${Object.keys(projectSchema.collections).sort().join(', ')}`
    );
  }

  // Check multiCollections if they exist
  const migrationMultiCollections = lastMigration.schemas.multiCollections || {};
  const projectMultiCollections = projectSchema.multiCollections || {};

  if (!schemasEqual(migrationMultiCollections, projectMultiCollections)) {
    errors.push(
      `Last migration multiCollections schema does not match project schema.\n` +
      `  Migration: ${Object.keys(migrationMultiCollections).sort().join(', ')}\n` +
      `  Project:   ${Object.keys(projectMultiCollections).sort().join(', ')}`
    );
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
  projectSchemaPath: string
): Promise<{
  valid: boolean;
  errors: string[];
  warnings: string[];
}> {
  const errors: string[] = [];
  const warnings: string[] = [];

  if (migrations.length === 0) {
    warnings.push('No migrations found. Run "mongodbee generate" to create your first migration.');
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
  const hasCollections = Object.keys(projectSchema.collections).length > 0;
  const hasMultiCollections = Object.keys(projectSchema.multiCollections || {}).length > 0;

  if (!hasCollections && !hasMultiCollections) {
    warnings.push('Project schema is empty. Define your schemas in schemas.ts');
  }

  // Validate last migration matches project schema
  const lastMigration = migrations[migrations.length - 1];
  const validation = validateLastMigrationMatchesProjectSchema(lastMigration, projectSchema);

  if (!validation.valid) {
    errors.push(...validation.errors);
    errors.push(
      `\nThe last migration schema must match your project schema (schemas.ts).\n` +
      `Update your schemas.ts to match the last migration, or create a new migration to reflect your current schema.`
    );
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}
/**
 * @fileoverview Migration file discovery and loading utilities
 *
 * This module provides functions to discover migration files from the filesystem,
 * load them dynamically, and validate their structure.
 *
 * @module
 */

import * as path from "@std/path";
import type { MigrationDefinition } from "./types.ts";

/**
 * Discovers all migration files in a directory
 *
 * @param migrationsDir - Directory containing migration files
 * @returns Array of migration file names sorted alphabetically
 */
export async function discoverMigrationFiles(
  migrationsDir: string,
): Promise<string[]> {
  try {
    const entries = [];
    for await (const entry of Deno.readDir(migrationsDir)) {
      if (entry.isFile && entry.name.endsWith(".ts")) {
        entries.push(entry.name);
      }
    }
    return entries.sort();
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) {
      throw new Error(`Migrations directory not found: ${migrationsDir}`);
    }
    throw error;
  }
}

/**
 * Loads a migration definition from a file
 *
 * @param migrationsDir - Directory containing migration files
 * @param fileName - Name of the migration file
 * @returns The loaded migration definition
 */
export async function loadMigrationFile(
  migrationsDir: string,
  fileName: string,
): Promise<MigrationDefinition> {
  const fullPath = path.resolve(migrationsDir, fileName);

  // Convert to file:// URL for dynamic import
  const importPath = Deno.build.os === "windows"
    ? `file:///${fullPath.replace(/\\/g, "/")}`
    : `file://${fullPath}`;

  try {
    const module = await import(importPath);

    if (!module.default) {
      throw new Error(
        `Migration file ${fileName} does not have a default export`,
      );
    }

    const migration = module.default as MigrationDefinition;

    // Validate migration structure
    if (
      !migration.id || !migration.name ||
      typeof migration.migrate !== "function"
    ) {
      throw new Error(
        `Migration file ${fileName} is missing required properties (id, name, migrate)`,
      );
    }

    return migration;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to load migration ${fileName}: ${message}`);
  }
}

/**
 * Loads all migrations from a directory
 *
 * @param migrationsDir - Directory containing migration files
 * @returns Array of loaded migration definitions with their filenames
 */
export async function loadAllMigrations(
  migrationsDir: string,
): Promise<Array<{ fileName: string; migration: MigrationDefinition }>> {
  const fileNames = await discoverMigrationFiles(migrationsDir);

  const migrations = await Promise.all(
    fileNames.map(async (fileName) => {
      const migration = await loadMigrationFile(migrationsDir, fileName);
      return { fileName, migration };
    }),
  );

  return migrations;
}

/**
 * Validates the parent-child chain of migrations
 *
 * @param migrations - Array of migrations to validate
 * @returns Validation errors (empty array if valid)
 */
export function validateMigrationChain(
  migrations: MigrationDefinition[],
): string[] {
  const errors: string[] = [];

  if (migrations.length === 0) {
    return errors;
  }

  // Check first migration has no parent
  if (migrations[0].parent !== null) {
    errors.push(
      `First migration "${migrations[0].name}" (${
        migrations[0].id
      }) should not have a parent`,
    );
  }

  // Check subsequent migrations have correct parent
  for (let i = 1; i < migrations.length; i++) {
    const migration = migrations[i];
    const expectedParent = migrations[i - 1];

    if (migration.parent?.id !== expectedParent.id) {
      errors.push(
        `Migration "${migration.name}" (${migration.id}) has incorrect parent. Expected ${expectedParent.id}, got ${
          migration.parent?.id ?? "null"
        }`,
      );
    }
  }

  // Check for duplicate IDs
  const seenIds = new Set<string>();
  for (const migration of migrations) {
    if (seenIds.has(migration.id)) {
      errors.push(`Duplicate migration ID found: ${migration.id}`);
    }
    seenIds.add(migration.id);
  }

  return errors;
}

/**
 * Builds a migration chain from individual migrations
 *
 * This reconstructs the parent-child relationships by sorting migrations
 * and validating the chain integrity.
 *
 * @param migrations - Array of migrations to chain
 * @returns Ordered array of migrations forming a valid chain
 */
export function buildMigrationChain(
  migrations: Array<{ fileName: string; migration: MigrationDefinition }>,
): MigrationDefinition[] {
  // Sort by filename (which should be timestamp-based)
  const sorted = migrations
    .slice()
    .sort((a, b) => a.fileName.localeCompare(b.fileName));

  const chain = sorted.map((m) => m.migration);

  // Validate the chain
  const errors = validateMigrationChain(chain);
  if (errors.length > 0) {
    throw new Error(`Migration chain validation failed:\n${errors.join("\n")}`);
  }

  return chain;
}

/**
 * Gets pending migrations that haven't been applied yet
 *
 * @param allMigrations - All available migrations
 * @param appliedMigrationIds - IDs of migrations that have been applied
 * @returns Array of migrations that need to be applied
 */
export function getPendingMigrations(
  allMigrations: MigrationDefinition[],
  appliedMigrationIds: string[],
): MigrationDefinition[] {
  const appliedSet = new Set(appliedMigrationIds);

  return allMigrations.filter((m) => !appliedSet.has(m.id));
}

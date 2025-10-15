/**
 * @fileoverview Migration definition utilities for creating and managing migrations
 *
 * This module provides utilities for defining migrations with proper parent-child
 * relationships, schema evolution tracking, and type safety. It ensures migrations
 * are properly linked and can be executed in the correct order.
 *
 * @example
 * ```typescript
 * import { migrationDefinition } from "@diister/mongodbee/migration";
 * import { migrationBuilder } from "@diister/mongodbee/migration";
 * import * as v from "valibot";
 *
 * // Initial migration
 * const migration0 = migrationDefinition("000001", "Create users table", {
 *   parent: null,
 *   schemas: {
 *     collections: {
 *       users: {
 *         _id: v.string(),
 *         name: v.string(),
 *         email: v.string()
 *       }
 *     }
 *   },
 *   migrate: (builder) => builder
 *     .createCollection("users")
 *     .seed([{ name: "Admin", email: "admin@example.com" }])
 *     .done()
 *     .compile()
 * });
 *
 * // Child migration
 * const migration1 = migrationDefinition("000002", "Add user age", {
 *   parent: migration0,
 *   schemas: {
 *     collections: {
 *       ...migration0.schemas.collections,
 *       users: {
 *         ...migration0.schemas.collections.users,
 *         age: v.optional(v.number())
 *       }
 *     }
 *   },
 *   migrate: (builder) => builder
 *     .collection("users")
 *     .transform({
 *       up: (doc) => ({ ...doc, age: 25 }),
 *       down: (doc) => { const { age, ...rest } = doc; return rest; }
 *     })
 *     .done()
 *     .compile()
 * });
 * ```
 *
 * @module
 */

import { ulid } from "@std/ulid/ulid";
import type {
  MigrationBuilder,
  MigrationDefinition,
  MigrationState,
  SchemasDefinition,
} from "./types.ts";

/**
 * Options for creating a migration definition
 *
 * @template Schema - The schema type for this migration
 */
export interface MigrationDefinitionOptions<Schema extends SchemasDefinition> {
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
}

/**
 * Creates a new migration definition with proper parent-child relationships
 *
 * This function creates a migration definition that can be used in a migration
 * chain. It ensures proper typing and parent-child relationships between
 * migrations, allowing for schema evolution tracking.
 *
 * @template Schema - The schema type for this migration
 * @param id - Unique identifier for this migration (should be sequential)
 * @param name - Human-readable name describing what this migration does
 * @param options - Migration options including parent, schemas, and migration function
 * @returns A complete migration definition
 *
 * @example
 * ```typescript
 * const initialMigration = migrationDefinition("000001", "Initial setup", {
 *   parent: null,
 *   schemas: {
 *     collections: {
 *       users: {
 *         _id: v.string(),
 *         name: v.string()
 *       }
 *     }
 *   },
 *   migrate: (builder) => builder
 *     .createCollection("users")
 *     .compile()
 * });
 *
 * const childMigration = migrationDefinition("000002", "Add email field", {
 *   parent: initialMigration,
 *   schemas: {
 *     collections: {
 *       users: {
 *         _id: v.string(),
 *         name: v.string(),
 *         email: v.string()
 *       }
 *     }
 *   },
 *   migrate: (builder) => builder
 *     .collection("users")
 *     .transform({
 *       up: (doc) => ({ ...doc, email: "" }),
 *       down: (doc) => { const { email, ...rest } = doc; return rest; }
 *     })
 *     .done()
 *     .compile()
 * });
 * ```
 */
export function migrationDefinition<Schema extends SchemasDefinition>(
  id: string,
  name: string,
  options: MigrationDefinitionOptions<Schema>,
): MigrationDefinition<Schema> {
  // Validate migration ID format
  if (!id || typeof id !== "string") {
    throw new Error("Migration ID must be a non-empty string");
  }

  // Validate migration name
  if (!name || typeof name !== "string") {
    throw new Error("Migration name must be a non-empty string");
  }

  // Validate schemas
  if (!options.schemas || !options.schemas.collections) {
    throw new Error("Migration must define at least collections schema");
  }

  // Validate migrate function
  if (typeof options.migrate !== "function") {
    throw new Error("Migration must provide a migrate function");
  }

  return {
    id,
    name,
    parent: options.parent,
    schemas: options.schemas,
    migrate: options.migrate,
  };
}

/**
 * Validates a chain of migrations for consistency and proper linking
 *
 * This function checks that migrations are properly linked in a parent-child
 * relationship and that IDs are unique and in proper sequence.
 *
 * @param migrations - Array of migrations in order
 * @returns Validation result with any errors found
 *
 * @example
 * ```typescript
 * const migrations = [migration0, migration1, migration2];
 * const validation = validateMigrationChain(migrations);
 *
 * if (!validation.valid) {
 *   console.error("Migration chain validation failed:", validation.errors);
 * }
 * ```
 */
export function validateMigrationChain(migrations: MigrationDefinition[]): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];
  const seenIds = new Set<string>();

  for (let i = 0; i < migrations.length; i++) {
    const migration = migrations[i];
    const expectedParent = i > 0 ? migrations[i - 1] : null;

    // Check for duplicate IDs
    if (seenIds.has(migration.id)) {
      errors.push(`Duplicate migration ID found: ${migration.id}`);
    }
    seenIds.add(migration.id);

    // Check parent-child relationship
    if (i === 0) {
      // First migration should have no parent
      if (migration.parent !== null) {
        errors.push(
          `First migration ${migration.name} (${migration.id}) should have no parent, but has parent ${migration.parent?.id}`,
        );
      }
    } else {
      // Subsequent migrations should have the previous migration as parent
      if (migration.parent?.id !== expectedParent?.id) {
        errors.push(
          `Migration ${migration.name} (${migration.id}) parent is not the previous migration ${expectedParent?.name} (${expectedParent?.id})`,
        );
      }
    }

    // Validate ID format (should be sequential)
    if (!/^\d+$/.test(migration.id)) {
      errors.push(
        `Migration ID ${migration.id} should be numeric for proper ordering`,
      );
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

/**
 * Generates a unique migration ID based on the current timestamp and an optional name.
 * The format is `YYYY_MM_DD_HHMM_<MINI_ULID>[@<name>]`, where `<ULID>` is a unique identifier.
 *
 * @param name - Optional name to include in the migration ID
 * @returns A unique migration ID string
 *
 * @example
 * ```typescript
 * const migrationId = generateMigrationId("add-users-collection");
 * console.log(migrationId); // e.g. "2023_10_05_01F8Z8X1Y2Z3_add-users-collection"
 * ```
 */
export function generateMigrationId(name?: string): string {
  const date = new Date();
  // Use UTC for both date and time to ensure consistency
  const dateISO = date.toISOString(); // e.g. "2025-10-15T23:58:30.123Z"
  const datePart = dateISO.split("T")[0].replace(/-/g, "_"); // "2025_10_15"
  const timePart = dateISO.split("T")[1].slice(0, 5).replace(/:/g, ""); // "2358"
  const uniquePart = ulid().slice(4, 14); // Shorten ULID for brevity
  const namePart = name ? `@${name.replace(/\s+/g, "-").toLowerCase()}` : "";
  return `${datePart}_${timePart}_${uniquePart}${namePart}`;
}

/**
 * Gets all migrations that are ancestors of a given migration
 *
 * @param migration - The migration to get ancestors for
 * @returns Array of ancestor migrations in order from root to parent
 *
 * @example
 * ```typescript
 * const ancestors = getMigrationAncestors(migration2);
 * console.log(ancestors.map(m => m.id)); // ["000001", "000002"]
 * ```
 */
export function getMigrationAncestors(
  migration: MigrationDefinition,
): MigrationDefinition[] {
  const ancestors: MigrationDefinition[] = [];
  let current = migration.parent;

  while (current !== null) {
    ancestors.unshift(current); // Add to beginning to maintain order
    current = current.parent;
  }

  return ancestors;
}

/**
 * Gets the full migration path from root to the given migration
 *
 * @param migration - The migration to get the path for
 * @returns Array of migrations from root to the given migration (inclusive)
 *
 * @example
 * ```typescript
 * const path = getMigrationPath(migration2);
 * console.log(path.map(m => m.id)); // ["000001", "000002", "000003"]
 * ```
 */
export function getMigrationPath(
  migration: MigrationDefinition,
): MigrationDefinition[] {
  const ancestors = getMigrationAncestors(migration);
  return [...ancestors, migration];
}

/**
 * Finds the common ancestor of two migrations
 *
 * @param migration1 - First migration
 * @param migration2 - Second migration
 * @returns The common ancestor migration, or null if no common ancestor
 *
 * @example
 * ```typescript
 * const commonAncestor = findCommonAncestor(branchA, branchB);
 * if (commonAncestor) {
 *   console.log(`Common ancestor: ${commonAncestor.name}`);
 * }
 * ```
 */
export function findCommonAncestor(
  migration1: MigrationDefinition,
  migration2: MigrationDefinition,
): MigrationDefinition | null {
  const path1 = getMigrationPath(migration1);
  const path2 = getMigrationPath(migration2);

  let commonAncestor: MigrationDefinition | null = null;

  const minLength = Math.min(path1.length, path2.length);
  for (let i = 0; i < minLength; i++) {
    if (path1[i].id === path2[i].id) {
      commonAncestor = path1[i];
    } else {
      break;
    }
  }

  return commonAncestor;
}

/**
 * Checks if one migration is an ancestor of another
 *
 * @param ancestor - The potential ancestor migration
 * @param descendant - The potential descendant migration
 * @returns True if ancestor is an ancestor of descendant
 *
 * @example
 * ```typescript
 * const isAncestor = isMigrationAncestor(migration1, migration3);
 * console.log(isAncestor); // true if migration1 is ancestor of migration3
 * ```
 */
export function isMigrationAncestor(
  ancestor: MigrationDefinition,
  descendant: MigrationDefinition,
): boolean {
  const ancestors = getMigrationAncestors(descendant);
  return ancestors.some((a) => a.id === ancestor.id);
}

/**
 * Creates a migration summary with metadata about operations and relationships
 *
 * @param migration - The migration to create a summary for
 * @returns Summary object with migration metadata
 *
 * @example
 * ```typescript
 * const summary = createMigrationSummary(migration);
 * console.log(`Migration: ${summary.name}`);
 * console.log(`Operations: ${summary.operationCount}`);
 * console.log(`Depth: ${summary.depth}`);
 * ```
 */
export function createMigrationSummary(migration: MigrationDefinition): {
  id: string;
  name: string;
  depth: number;
  hasParent: boolean;
  parentId: string | null;
  ancestorCount: number;
  collectionCount: number;
  multiCollectionCount: number;
  multiModelCount: number;
} {
  const ancestors = getMigrationAncestors(migration);

  return {
    id: migration.id,
    name: migration.name,
    depth: ancestors.length,
    hasParent: migration.parent !== null,
    parentId: migration.parent?.id ?? null,
    ancestorCount: ancestors.length,
    collectionCount: Object.keys(migration.schemas.collections ?? {}).length,
    multiCollectionCount: Object.keys(migration.schemas.multiModels ?? {}).length,
    multiModelCount: Object.keys(migration.schemas.multiModels ?? {}).length
  };
}

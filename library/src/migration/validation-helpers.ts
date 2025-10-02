/**
 * @fileoverview Application startup validation helpers
 *
 * This module provides functions to validate migration state at application startup.
 * These helpers ensure that:
 * 1. All pending migrations have been applied before running the application
 * 2. The application schemas match the current migration state
 *
 * Use these in your application startup to catch migration issues early.
 *
 * @module
 */

import type { Db } from '../mongodb.ts';
import { loadAllMigrations, buildMigrationChain, getPendingMigrations } from './discovery.ts';
import { getAppliedMigrationIds } from './history.ts';
import { loadSchemaFromFile, compareSchemas, type DatabaseSchema } from './schema.ts';
import { loadConfig } from './config/loader.ts';
import * as path from "@std/path";

/**
 * Result of migration validation check
 */
export interface MigrationValidationResult {
  /** Whether all migrations are applied */
  isUpToDate: boolean;

  /** List of pending migration IDs that need to be applied */
  pendingMigrations: string[];

  /** Total number of migrations found */
  totalMigrations: number;

  /** Number of applied migrations */
  appliedCount: number;

  /** Last applied migration ID (if any) */
  lastAppliedMigration?: string;

  /** Human-readable message describing the status */
  message: string;
}

/**
 * Checks if all migrations have been applied to the database
 *
 * This function is designed to be called at application startup to ensure
 * the database is up-to-date with the latest migrations.
 *
 * @param db - MongoDB database connection
 * @param migrationsDir - Path to migrations directory (default: "./migrations")
 * @returns Validation result with migration status
 *
 * @example
 * ```typescript
 * import { checkMigrationStatus } from "@diister/mongodbee/migration";
 *
 * const db = client.db("myapp");
 * const status = await checkMigrationStatus(db);
 *
 * if (!status.isUpToDate) {
 *   if (Deno.env.get("ENV") === "production") {
 *     throw new Error(status.message);
 *   } else {
 *     console.warn("⚠️ Migration Warning:", status.message);
 *   }
 * }
 * ```
 */
export async function checkMigrationStatus(
  db: Db,
  migrationsDir = "./migrations"
): Promise<MigrationValidationResult> {
  try {
    // Load all migrations from filesystem
    const resolvedDir = path.resolve(migrationsDir);
    const migrationsWithFiles = await loadAllMigrations(resolvedDir);
    const allMigrations = buildMigrationChain(migrationsWithFiles);

    // Get applied migrations from database
    const appliedIds = await getAppliedMigrationIds(db);

    // Find pending migrations
    const pending = getPendingMigrations(allMigrations, appliedIds);

    // Get last applied migration
    const lastAppliedMigration = appliedIds.length > 0 
      ? appliedIds[appliedIds.length - 1]
      : undefined;

    // Build result message
    let message: string;
    if (pending.length === 0) {
      message = "✓ Database is up-to-date. All migrations have been applied.";
    } else if (appliedIds.length === 0) {
      message = `⚠️ No migrations applied yet. ${pending.length} migration(s) need to be run.`;
    } else {
      message = `⚠️ Database is outdated. ${pending.length} pending migration(s) need to be applied.`;
    }

    return {
      isUpToDate: pending.length === 0,
      pendingMigrations: pending.map(m => m.id),
      totalMigrations: allMigrations.length,
      appliedCount: appliedIds.length,
      lastAppliedMigration,
      message,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to check migration status: ${message}`);
  }
}

/**
 * Checks if the last migration in the filesystem has been applied to the database
 *
 * This is a simpler check than `checkMigrationStatus` that only verifies
 * whether the most recent migration file has been executed.
 *
 * @param db - MongoDB database connection
 * @param migrationsDir - Path to migrations directory (default: "./migrations")
 * @returns True if the last migration has been applied, false otherwise
 *
 * @example
 * ```typescript
 * import { isLastMigrationApplied } from "@diister/mongodbee/migration";
 *
 * const db = client.db("myapp");
 * const upToDate = await isLastMigrationApplied(db);
 *
 * if (!upToDate) {
 *   if (Deno.env.get("ENV") === "production") {
 *     throw new Error("Database schema is outdated. Please run migrations.");
 *   } else {
 *     console.warn("⚠️ Database schema is outdated. Run: deno task migrate:apply");
 *   }
 * }
 * ```
 */
export async function isLastMigrationApplied(
  db: Db,
  migrationsDir = "./migrations"
): Promise<boolean> {
  try {
    // Load all migrations from filesystem
    const resolvedDir = path.resolve(migrationsDir);
    const migrationsWithFiles = await loadAllMigrations(resolvedDir);
    
    if (migrationsWithFiles.length === 0) {
      // No migrations exist, consider it "up-to-date"
      return true;
    }

    const allMigrations = buildMigrationChain(migrationsWithFiles);
    const lastMigration = allMigrations[allMigrations.length - 1];

    // Get applied migrations from database
    const appliedIds = await getAppliedMigrationIds(db);

    // Check if last migration ID is in applied list
    return appliedIds.includes(lastMigration.id);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to check last migration status: ${message}`);
  }
}

/**
 * Asserts that all migrations are applied, throwing an error if not
 *
 * This is a convenience function that combines `checkMigrationStatus` with
 * automatic error throwing. Useful for production startup checks.
 *
 * @param db - MongoDB database connection
 * @param migrationsDir - Path to migrations directory (default: "./migrations")
 * @throws Error if any pending migrations exist
 *
 * @example
 * ```typescript
 * import { assertMigrationsApplied } from "@diister/mongodbee/migration";
 *
 * const db = client.db("myapp");
 *
 * // Will throw if migrations are pending
 * await assertMigrationsApplied(db);
 *
 * // Application continues only if all migrations are applied
 * console.log("✓ Database is up-to-date");
 * ```
 */
export async function assertMigrationsApplied(
  db: Db,
  migrationsDir = "./migrations"
): Promise<void> {
  const status = await checkMigrationStatus(db, migrationsDir);

  if (!status.isUpToDate) {
    throw new Error(
      `Migration validation failed: ${status.pendingMigrations.length} pending migration(s). ` +
      `Run migrations before starting the application. Pending: [${status.pendingMigrations.join(', ')}]`
    );
  }
}

/**
 * Helper function for environment-aware migration validation
 *
 * Warns in development, throws in production
 *
 * @param db - MongoDB database connection
 * @param env - Environment name ("development", "production", etc.)
 * @param migrationsDir - Path to migrations directory (default: "./migrations")
 *
 * @example
 * ```typescript
 * import { validateMigrationsForEnv } from "@diister/mongodbee/migration";
 *
 * const db = client.db("myapp");
 * const env = Deno.env.get("ENV") || "development";
 *
 * await validateMigrationsForEnv(db, env);
 * ```
 */
export async function validateMigrationsForEnv(
  db: Db,
  env: string,
  migrationsDir = "./migrations"
): Promise<void> {
  const status = await checkMigrationStatus(db, migrationsDir);

  if (!status.isUpToDate) {
    if (env === "production" || env === "prod") {
      throw new Error(
        `🚨 Production startup blocked: ${status.pendingMigrations.length} pending migration(s). ` +
        `Apply migrations before deploying. Pending: [${status.pendingMigrations.join(', ')}]`
      );
    } else {
      console.warn(`\n⚠️  Warning: ${status.message}`);
      console.warn(`   Pending migrations: [${status.pendingMigrations.join(', ')}]`);
      console.warn(`   Run: deno task migrate:apply\n`);
    }
  }
}

/**
 * Schema alignment validation result
 */
export interface SchemaAlignmentResult {
  /** Whether application schemas match the last applied migration */
  isAligned: boolean;

  /** Schema validation errors (critical mismatches) */
  errors: Array<{
    collection: string;
    field: string;
    expected: string;
    actual: string;
  }>;

  /** Schema validation warnings (non-critical differences) */
  warnings: Array<{
    collection: string;
    field: string;
    expected: string;
    actual: string;
  }>;

  /** Human-readable message describing the status */
  message: string;
}

/**
 * Complete migration state validation result
 * Combines migration status and schema alignment checks
 */
export interface DatabaseStateValidationResult {
  /** Overall validation status - true if both migrations and schemas are valid */
  isValid: boolean;

  /** Migration status check result */
  migrations: MigrationValidationResult;

  /** Schema alignment check result */
  schemas: SchemaAlignmentResult;

  /** Combined human-readable summary message */
  message: string;

  /** Array of all issues found (migrations + schemas) */
  issues: string[];
}

/**
 * Checks if application schemas match the actual database structure
 *
 * This validates that your application's schema definitions (from schemas.ts)
 * match the actual database structure. This catches cases where:
 * - Developers changed schemas.ts without creating a migration
 * - Migrations were applied but schemas.ts wasn't updated
 * - Database was modified directly outside of migrations
 * - Schema files are out of sync with database state
 *
 * **How it works**:
 * 1. Loads your application schema from the schemas directory
 * 2. Generates a schema from the actual MongoDB collections
 * 3. Compares them to find mismatches
 *
 * @param db - MongoDB database connection
 * @param configPath - Path to mongodbee.config.ts (default: "./mongodbee.config.ts")
 * @returns Schema alignment result with errors and warnings
 *
 * @example
 * ```typescript
 * import { checkSchemaAlignment } from "@diister/mongodbee/migration";
 *
 * const db = client.db("myapp");
 * const result = await checkSchemaAlignment(db);
 *
 * if (!result.isAligned) {
 *   console.error("❌ Schema mismatch detected!");
 *   console.error(result.message);
 *   
 *   for (const error of result.errors) {
 *     console.error(`  - ${error.collection}.${error.field}: expected ${error.expected}, got ${error.actual}`);
 *   }
 *   
 *   if (Deno.env.get("ENV") === "production") {
 *     throw new Error("Schema alignment check failed");
 *   }
 * }
 * ```
 */
export async function checkSchemaAlignment(
  db: Db,
  configPath = "./mongodbee.config.ts"
): Promise<SchemaAlignmentResult> {
  try {
    // Load configuration to get paths
    let config;
    try {
      config = await loadConfig({ configPath });
    } catch (error) {
      // If config file doesn't exist, return a skipped result
      return {
        isAligned: true,
        errors: [],
        warnings: [],
        message: "⚠️ Configuration file not found. Schema validation skipped.",
      };
    }

    const schemasPath = path.resolve(config.paths?.schemas || "./schemas");

    // Load application schemas from file
    const schemaFilePath = path.join(schemasPath, "database.json");
    let applicationSchema: DatabaseSchema;
    
    try {
      applicationSchema = await loadSchemaFromFile(schemaFilePath);
    } catch (error) {
      return {
        isAligned: false,
        errors: [{
          collection: "*",
          field: "*",
          expected: "Schema file to exist",
          actual: `Schema file not found at ${schemaFilePath}`,
        }],
        warnings: [],
        message: `⚠️ Application schema file not found. Expected at: ${schemaFilePath}`,
      };
    }

    // Generate current database schema by inferring from actual collections
    const mongoUri = config.database?.connection?.uri || Deno.env.get("MONGODB_URI") || "";
    const dbName = config.database?.name || db.databaseName;
    
    if (!mongoUri) {
      throw new Error("MongoDB URI not found in config or MONGODB_URI environment variable");
    }

    const { generateDatabaseSchema } = await import('./schema.ts');
    const actualSchema = await generateDatabaseSchema(mongoUri, dbName);

    // Compare application schema (expected) with actual database schema
    const comparisonResult = compareSchemas(actualSchema, applicationSchema);

    const errors = comparisonResult.errors.map(e => ({
      collection: e.collection,
      field: e.field,
      expected: e.expected,
      actual: e.actual,
    }));

    const warnings = comparisonResult.warnings.map(w => ({
      collection: w.collection,
      field: w.field,
      expected: w.expected,
      actual: w.actual,
    }));

    let message: string;
    if (errors.length === 0 && warnings.length === 0) {
      message = "✓ Application schemas match the database structure.";
    } else if (errors.length > 0) {
      message = `❌ Schema misalignment detected: ${errors.length} error(s), ${warnings.length} warning(s). ` +
                `Your schemas.ts file doesn't match the actual database structure.`;
    } else {
      message = `⚠️ Schema differences detected: ${warnings.length} warning(s).`;
    }

    return {
      isAligned: errors.length === 0,
      errors,
      warnings,
      message,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to check schema alignment: ${message}`);
  }
}

/**
 * Validates complete database state: checks both migration status and schema alignment
 *
 * This is the recommended function for application startup validation. It performs
 * two critical checks:
 * 1. Verifies all migrations are applied
 * 2. Validates application schemas match the database structure
 *
 * @param db - MongoDB database connection
 * @param options - Optional configuration
 * @param options.configPath - Path to mongodbee.config.ts
 * @param options.migrationsDir - Path to migrations directory
 * @param options.env - Environment name for context-aware validation
 * @returns Complete validation result with migrations and schema status
 *
 * @example
 * ```typescript
 * import { validateDatabaseState } from "@diister/mongodbee/migration";
 *
 * // At application startup
 * const db = client.db("myapp");
 * const env = Deno.env.get("ENV") || "development";
 * 
 * const result = await validateDatabaseState(db, { env });
 *
 * if (!result.isValid) {
 *   console.error("❌ Database validation failed!");
 *   console.error(result.message);
 *   
 *   for (const issue of result.issues) {
 *     console.error(`  - ${issue}`);
 *   }
 *   
 *   if (env === "production") {
 *     throw new Error("Database validation failed - cannot start application");
 *   }
 * } else {
 *   console.log("✓ Database validation passed");
 * }
 * ```
 */
export async function validateDatabaseState(
  db: Db,
  options?: {
    configPath?: string;
    migrationsDir?: string;
    env?: string;
  }
): Promise<DatabaseStateValidationResult> {
  const configPath = options?.configPath || "./mongodbee.config.ts";
  const migrationsDir = options?.migrationsDir || "./migrations";
  const env = options?.env;

  // Run both checks in parallel for better performance
  const [migrations, schemas] = await Promise.all([
    checkMigrationStatus(db, migrationsDir),
    checkSchemaAlignment(db, configPath),
  ]);

  const issues: string[] = [];

  // Collect migration issues
  if (!migrations.isUpToDate) {
    issues.push(`${migrations.pendingMigrations.length} pending migration(s): [${migrations.pendingMigrations.join(', ')}]`);
  }

  // Collect schema issues
  if (!schemas.isAligned) {
    if (schemas.errors.length > 0) {
      issues.push(`${schemas.errors.length} schema error(s) detected`);
      for (const error of schemas.errors.slice(0, 3)) { // Show first 3 errors
        issues.push(`  └─ ${error.collection}.${error.field}: expected ${error.expected}, got ${error.actual}`);
      }
      if (schemas.errors.length > 3) {
        issues.push(`  └─ ... and ${schemas.errors.length - 3} more error(s)`);
      }
    }
    if (schemas.warnings.length > 0) {
      issues.push(`${schemas.warnings.length} schema warning(s) detected`);
    }
  }

  const isValid = migrations.isUpToDate && schemas.isAligned;

  let message: string;
  if (isValid) {
    message = "✓ Database validation passed - migrations applied and schemas aligned";
  } else {
    const envContext = env ? ` [${env}]` : "";
    message = `❌ Database validation failed${envContext}: ${issues.length} issue(s) found`;
  }

  return {
    isValid,
    migrations,
    schemas,
    message,
    issues,
  };
}

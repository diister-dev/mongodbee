/**
 * @fileoverview Migration status checking utilities
 *
 * This module provides a programmatic API for checking the status of migrations
 * without applying them. This is useful for application startup checks to ensure
 * migrations are in a healthy state.
 *
 * @example
 * ```typescript
 * import { checkMigrationStatus } from "@diister/mongodbee/migration";
 * import { MongoClient } from "mongodb";
 *
 * // Check without database connection (schema validation only)
 * const status = await checkMigrationStatus({
 *   migrationsDir: "./migrations",
 *   schemaPath: "./schemas.ts"
 * });
 *
 * if (!status.isHealthy) {
 *   console.error("Migration system has issues:", status.errors);
 *   process.exit(1);
 * }
 *
 * // Check with database connection (includes pending migrations check)
 * const client = new MongoClient("mongodb://localhost:27017");
 * await client.connect();
 *
 * const statusWithDb = await checkMigrationStatus({
 *   migrationsDir: "./migrations",
 *   schemaPath: "./schemas.ts",
 *   db: client.db("myapp")
 * });
 *
 * if (statusWithDb.hasPendingMigrations) {
 *   console.warn(`Warning: ${statusWithDb.pendingMigrationsCount} pending migration(s)`);
 * }
 * ```
 *
 * @module
 */

import type { Db } from "../mongodb.ts";
import type { MigrationDefinition } from "./types.ts";
import { createEmptyDatabaseState, type SimulationDatabaseState } from "./types.ts";
import {
  buildMigrationChain,
  getPendingMigrations,
  loadAllMigrations,
} from "./discovery.ts";
import { validateMigrationChainWithProjectSchema } from "./schema-validation.ts";
import { createSimulationValidator, type SimulationPowerLevel } from "./validators/simulation.ts";
import { getAppliedMigrationIds, getLastAppliedMigration } from "./state.ts";
import { loadConfig } from "./config/loader.ts";
import * as path from "@std/path";
import { extractIndexes, keyEqual, normalizeIndexOptions } from "../indexes.ts";
import { sanitizePathName } from "../schema-navigator.ts";
import * as v from "../schema.ts";

/**
 * Options for checking migration status
 */
export interface CheckMigrationStatusOptions {
  /**
   * Path to the migrations directory
   * If not provided, will be loaded from mongodbee.config.ts
   */
  migrationsDir?: string;

  /**
   * Path to the schemas file (typically schemas.ts)
   * If not provided, will be loaded from mongodbee.config.ts
   */
  schemaPath?: string;

  /**
   * Path to the config file
   * @default "./mongodbee.config.ts" (auto-discovered)
   */
  configPath?: string;

  /**
   * Current working directory for resolving paths
   * @default Deno.cwd()
   */
  cwd?: string;

  /**
   * Optional database connection to check applied migrations
   * If not provided, only schema validation will be performed
   */
  db?: Db;

  /**
   * Whether to perform strict validation (including simulation)
   * @default true
   */
  strictValidation?: boolean;

  /**
   * Whether to include detailed information in the result
   * @default false
   */
  verbose?: boolean;

  /**
   * Simulation power level controlling mock data generation complexity
   * - `quick`: Fast validation with minimal mock data (10-20 docs)
   * - `normal`: Balanced validation (100 docs)
   * - `hard`: Comprehensive validation (500+ docs)
   *
   * @default "quick"
   */
  simulationMode?: SimulationPowerLevel;

  /**
   * Only validate the last N migrations
   * If not provided, all migrations are validated
   *
   * @example
   * ```typescript
   * // Only validate the last 3 migrations
   * await checkMigrationStatus({ lastN: 3 });
   * ```
   */
  lastN?: number;
}

/**
 * Detailed information about a migration
 */
export interface MigrationInfo {
  /** Migration ID */
  id: string;

  /** Migration name */
  name: string;

  /** Whether this migration has been applied (only available when db is provided) */
  isApplied?: boolean;

  /** Whether this migration is valid */
  isValid: boolean;

  /** Validation errors for this migration */
  errors: string[];

  /** Validation warnings for this migration */
  warnings: string[];

  /** Number of operations in this migration */
  operationCount?: number;

  /** Whether this migration is reversible */
  isReversible?: boolean;
}

/**
 * Information about a specific index issue
 */
export interface IndexIssue {
  /** Collection name */
  collection: string;

  /** Field path for the index */
  path: string;

  /** Type of issue */
  type: "missing" | "outdated" | "orphaned";

  /** Expected index configuration (for missing/outdated) */
  expected?: {
    unique?: boolean;
    collation?: unknown;
    partialFilterExpression?: unknown;
  };

  /** Current index configuration (for outdated/orphaned) */
  current?: {
    unique?: boolean;
    collation?: unknown;
    partialFilterExpression?: unknown;
  };

  /** Human-readable description of the issue */
  description: string;
}

/**
 * Index validation status for all collections
 */
export interface IndexValidationDetails {
  /** Whether all indexes match the schema definitions */
  areIndexesValid: boolean;

  /** Number of collections checked */
  collectionsChecked: number;

  /** Number of indexes that are correct */
  validIndexCount: number;

  /** Number of indexes with issues */
  invalidIndexCount: number;

  /** Detailed list of all index issues found */
  issues: IndexIssue[];
}

/**
 * Validation details for the migration system
 */
export interface MigrationValidationDetails {
  /** Whether schema is consistent between migrations and project */
  isSchemaConsistent: boolean;

  /** Whether all migrations are valid (can be simulated) */
  areMigrationsValid: boolean;

  /** All validation errors encountered */
  errors: string[];

  /** All validation warnings encountered */
  warnings: string[];
}

/**
 * Database status details (only available when db is provided)
 */
export interface DatabaseStatusDetails {
  /** Whether all migrations are applied to the database */
  isUpToDate: boolean;

  /** Number of migrations applied */
  appliedCount: number;

  /** Number of migrations pending */
  pendingCount: number;

  /** Last applied migration ID */
  lastAppliedId?: string;

  /** Last applied migration name */
  lastAppliedName?: string;

  /** List of pending migration IDs */
  pendingIds: string[];
}

/**
 * Migration counts and statistics
 */
export interface MigrationCounts {
  /** Total number of migrations found */
  total: number;

  /** Number of valid migrations */
  valid: number;

  /** Number of invalid migrations */
  invalid: number;
}

/**
 * Result of checking migration status
 */
export interface MigrationStatusResult {
  /**
   * Overall health status - true if everything is OK
   * This is the main property to check
   */
  ok: boolean;

  /**
   * Human-readable summary message
   */
  message: string;

  /**
   * Migration counts and statistics
   */
  counts: MigrationCounts;

  /**
   * Validation details (schema consistency, simulation results)
   */
  validation: MigrationValidationDetails;

  /**
   * Database status (only available when db is provided)
   */
  database?: DatabaseStatusDetails;

  /**
   * Index validation status (only available when db is provided)
   */
  indexes?: IndexValidationDetails;

  /**
   * Detailed information about each migration (only when verbose = true)
   */
  migrations?: MigrationInfo[];
}

/**
 * Validates indexes for a collection against its schema definition
 * @internal
 */
async function validateCollectionIndexes(
  db: Db,
  collectionName: string,
  schema: v.ObjectSchema<any, any> | Record<string, any>,
): Promise<IndexIssue[]> {
  const issues: IndexIssue[] = [];

  try {
    const collection = db.collection(collectionName);
    const currentIndexes = await collection.indexes();

    // If schema is not an ObjectSchema, wrap it
    const objectSchema = (schema as any).type === "object"
      ? schema as v.ObjectSchema<any, any>
      : v.object(schema as any);

    const expectedIndexes = extractIndexes(objectSchema);

    // Build a set of expected index names
    const expectedIndexNames = new Set<string>();
    for (const index of expectedIndexes) {
      expectedIndexNames.add(sanitizePathName(index.path));
    }

    // Get all possible field paths from schema to detect mongodbee indexes
    const allSchemaPaths = new Set<string>();
    function collectPaths(obj: Record<string, unknown> | undefined | null, prefix = "") {
      if (!obj || typeof obj !== "object") return;

      for (const [key, value] of Object.entries(obj)) {
        const fullPath = prefix ? `${prefix}.${key}` : key;
        const sanitizedPath = sanitizePathName(fullPath);
        allSchemaPaths.add(sanitizedPath);

        if (value && typeof value === "object" && "entries" in value) {
          collectPaths((value as any).entries, fullPath);
        }
      }
    }
    const schemaEntries = (schema as any)?.entries;
    if (schemaEntries) {
      collectPaths(schemaEntries);
    }

    // Check for orphaned indexes
    for (const existingIndex of currentIndexes) {
      const indexName = existingIndex.name;
      if (!indexName || indexName === "_id_") continue;

      const isSchemaField = allSchemaPaths.has(indexName);

      if (isSchemaField && !expectedIndexNames.has(indexName)) {
        issues.push({
          collection: collectionName,
          path: indexName,
          type: "orphaned",
          current: {
            unique: existingIndex.unique,
            collation: existingIndex.collation,
            partialFilterExpression: existingIndex.partialFilterExpression,
          },
          description: `Index "${indexName}" exists in database but is not defined in current schema`,
        });
      }
    }

    // Check each expected index
    for (const expectedIndex of expectedIndexes) {
      const keySpec = { [expectedIndex.path]: 1 };
      const indexPath = sanitizePathName(expectedIndex.path);

      // Find existing index by name or key
      const existingIndex = currentIndexes.find((i) => i.name === indexPath) ||
        currentIndexes.find((i) => keyEqual(i.key || {}, keySpec));

      const desiredOptions = {
        unique: expectedIndex.metadata.unique,
        collation: expectedIndex.metadata.collation,
      };

      if (!existingIndex) {
        // Missing index
        issues.push({
          collection: collectionName,
          path: expectedIndex.path,
          type: "missing",
          expected: desiredOptions,
          description: `Index "${expectedIndex.path}" is defined in schema but missing in database`,
        });
      } else {
        // Check if index configuration matches
        const existingNorm = normalizeIndexOptions(existingIndex);
        const desiredNorm = normalizeIndexOptions(desiredOptions);

        if (
          existingNorm.unique !== desiredNorm.unique ||
          existingNorm.collation !== desiredNorm.collation ||
          existingNorm.partialFilterExpression !== desiredNorm.partialFilterExpression
        ) {
          issues.push({
            collection: collectionName,
            path: expectedIndex.path,
            type: "outdated",
            expected: desiredOptions,
            current: {
              unique: existingIndex.unique,
              collation: existingIndex.collation,
              partialFilterExpression: existingIndex.partialFilterExpression,
            },
            description: `Index "${expectedIndex.path}" exists but has different configuration than schema`,
          });
        }
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    issues.push({
      collection: collectionName,
      path: "_error",
      type: "missing",
      description: `Failed to validate indexes for collection "${collectionName}": ${message}`,
    });
  }

  return issues;
}

/**
 * Validates all indexes across all collections in the final schema
 * @internal
 */
async function validateAllIndexes(
  db: Db,
  schemas: Record<string, v.ObjectSchema<any, any> | Record<string, any>>,
): Promise<IndexValidationDetails> {
  const allIssues: IndexIssue[] = [];
  let totalValidIndexes = 0;

  const collectionNames = Object.keys(schemas);

  for (const collectionName of collectionNames) {
    const schema = schemas[collectionName];
    const issues = await validateCollectionIndexes(db, collectionName, schema);
    allIssues.push(...issues);

    // Count valid indexes (expected indexes minus issues for this collection)
    const objectSchema = (schema as any).type === "object"
      ? schema as v.ObjectSchema<any, any>
      : v.object(schema as any);
    const expectedIndexes = extractIndexes(objectSchema);
    const collectionIssues = issues.filter(
      (i) => i.collection === collectionName && (i.type === "missing" || i.type === "outdated")
    );
    totalValidIndexes += expectedIndexes.length - collectionIssues.length;
  }

  return {
    areIndexesValid: allIssues.length === 0,
    collectionsChecked: collectionNames.length,
    validIndexCount: totalValidIndexes,
    invalidIndexCount: allIssues.length,
    issues: allIssues,
  };
}

/**
 * Checks the status of the migration system
 *
 * This function validates:
 * 1. Migration discovery and loading
 * 2. Schema consistency between migrations and project
 * 3. Migration validity through simulation
 * 4. Database state (if db connection provided)
 * 5. Index consistency (if db connection provided)
 *
 * @param options - Options for checking migration status
 * @returns Promise resolving to migration status result
 *
 * @example
 * ```typescript
 * // Basic check (schema validation only)
 * const status = await checkMigrationStatus({
 *   migrationsDir: "./migrations",
 *   schemaPath: "./schemas.ts"
 * });
 *
 * console.log(status.isHealthy); // true or false
 * console.log(status.summary);   // Human-readable summary
 *
 * // Check with database
 * const statusWithDb = await checkMigrationStatus({
 *   migrationsDir: "./migrations",
 *   schemaPath: "./schemas.ts",
 *   db: mongoDb
 * });
 *
 * if (statusWithDb.hasPendingMigrations) {
 *   console.log(`${statusWithDb.pendingMigrationsCount} migration(s) pending`);
 * }
 * ```
 */
export async function checkMigrationStatus(
  options: CheckMigrationStatusOptions,
): Promise<MigrationStatusResult> {
  const {
    db,
    strictValidation = true,
    verbose = false,
    configPath,
    cwd = Deno.cwd(),
    simulationMode = "quick",
    lastN,
  } = options;

  let { migrationsDir, schemaPath } = options;

  // Load config if paths not provided
  if (!migrationsDir || !schemaPath) {
    try {
      const config = await loadConfig({ configPath, cwd });
      
      if (!migrationsDir && config.paths?.migrations) {
        migrationsDir = path.resolve(cwd, config.paths.migrations);
      }
      
      if (!schemaPath && config.paths?.schemas) {
        schemaPath = path.resolve(cwd, config.paths.schemas);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      // If config loading fails but paths were provided, continue
      if (!migrationsDir || !schemaPath) {
        return {
          ok: false,
          message: "Failed to load configuration",
          counts: { total: 0, valid: 0, invalid: 0 },
          validation: {
            isSchemaConsistent: false,
            areMigrationsValid: false,
            errors: [`Failed to load config: ${message}`],
            warnings: [],
          },
        };
      }
    }
  }

  // Ensure paths are resolved
  if (!migrationsDir || !schemaPath) {
    return {
      ok: false,
      message: "Missing required paths",
      counts: { total: 0, valid: 0, invalid: 0 },
      validation: {
        isSchemaConsistent: false,
        areMigrationsValid: false,
        errors: [
          "migrationsDir and schemaPath must be provided or available in mongodbee.config.ts",
        ],
        warnings: [],
      },
    };
  }

  const errors: string[] = [];
  const warnings: string[] = [];
  let allMigrations: MigrationDefinition[] = [];
  let isSchemaConsistent = false;
  let areMigrationsValid = false;

  // Step 1: Discover and load migrations
  try {
    const migrationsWithFiles = await loadAllMigrations(migrationsDir);

    if (migrationsWithFiles.length === 0) {
      warnings.push("No migrations found in the migrations directory");
    } else {
      allMigrations = buildMigrationChain(migrationsWithFiles);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    errors.push(`Failed to load migrations: ${message}`);
  }

  // Step 2: Validate schema consistency
  if (allMigrations.length > 0) {
    try {
      const schemaValidation = await validateMigrationChainWithProjectSchema(
        allMigrations,
        schemaPath,
      );

      isSchemaConsistent = schemaValidation.valid;
      errors.push(...schemaValidation.errors);
      warnings.push(...schemaValidation.warnings);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      errors.push(`Schema validation failed: ${message}`);
    }
  }

  // Step 3: Validate migrations with simulation (if strict validation enabled)
  const migrationsInfo: MigrationInfo[] = [];

  if (strictValidation && allMigrations.length > 0) {
    const simulationValidator = createSimulationValidator({
      strictValidation: true,
      maxOperations: 1000,
      stateRetentionRatio: 0.5,
      powerLevel: simulationMode,
    });

    let allValid = true;
    // Track current state to propagate between migrations (O(n) instead of O(n²))
    let currentState: SimulationDatabaseState = createEmptyDatabaseState();

    // Determine which migrations to validate based on lastN option
    const migrationsToValidate = lastN && lastN > 0 && lastN < allMigrations.length
      ? allMigrations.slice(-lastN)
      : allMigrations;

    // If using lastN, we need to fast-forward state to the start of validated migrations
    const skippedMigrations = lastN && lastN > 0 && lastN < allMigrations.length
      ? allMigrations.slice(0, -lastN)
      : [];

    // Fast-forward through skipped migrations (minimal validation, just state propagation)
    for (const migration of skippedMigrations) {
      try {
        const validationResult = await simulationValidator.validateMigration(
          migration,
          currentState,
        );
        if (validationResult.success && validationResult.data?.stateAfterMigration) {
          currentState = simulationValidator.prepareStateForNextMigration(
            validationResult.data.stateAfterMigration as SimulationDatabaseState,
            migration.schemas,
          );
        }
        // Add skipped migrations as valid (not fully validated but passed fast-forward)
        migrationsInfo.push({
          id: migration.id,
          name: migration.name,
          isValid: true,
          errors: [],
          warnings: ["Skipped (--last N mode)"],
        });
      } catch {
        // If fast-forward fails, still continue but mark as not validated
        migrationsInfo.push({
          id: migration.id,
          name: migration.name,
          isValid: true,
          errors: [],
          warnings: ["Skipped (--last N mode, fast-forward failed)"],
        });
      }
    }

    for (const migration of migrationsToValidate) {
      try {
        // Pass the current state to avoid re-simulating all parent migrations
        const validationResult = await simulationValidator.validateMigration(
          migration,
          currentState,
        );

        const migrationInfo: MigrationInfo = {
          id: migration.id,
          name: migration.name,
          isValid: validationResult.success,
          errors: validationResult.errors,
          warnings: validationResult.warnings,
          operationCount: typeof validationResult.data?.operationCount === 'number' 
            ? validationResult.data.operationCount 
            : undefined,
          isReversible: !validationResult.data?.hasIrreversibleProperty,
        };

        migrationsInfo.push(migrationInfo);

        if (!validationResult.success) {
          allValid = false;
          errors.push(
            `Migration "${migration.name}" (${migration.id}) validation failed: ${
              validationResult.errors.join(", ")
            }`,
          );
        } else {
          // Update state for next migration: apply retention ratio (keep 50%, generate fresh 50%)
          if (validationResult.data?.stateAfterMigration) {
            currentState = simulationValidator.prepareStateForNextMigration(
              validationResult.data.stateAfterMigration as SimulationDatabaseState,
              migration.schemas,
            );
          }
        }

        if (validationResult.warnings.length > 0) {
          warnings.push(
            `Migration "${migration.name}" (${migration.id}): ${
              validationResult.warnings.join(", ")
            }`,
          );
        }
      } catch (error) {
        allValid = false;
        const message = error instanceof Error ? error.message : String(error);
        errors.push(
          `Migration "${migration.name}" (${migration.id}) simulation error: ${message}`,
        );

        migrationsInfo.push({
          id: migration.id,
          name: migration.name,
          isValid: false,
          errors: [message],
          warnings: [],
        });
      }
    }

    areMigrationsValid = allValid;
  } else {
    // If not strict, assume valid
    areMigrationsValid = allMigrations.length > 0;
  }

  // Step 4: Check database state (if db provided)
  let appliedMigrationIds: string[] = [];
  let pendingMigrations: MigrationDefinition[] = [];
  let lastAppliedMigrationId: string | undefined;
  let lastAppliedMigrationName: string | undefined;

  if (db && allMigrations.length > 0) {
    try {
      appliedMigrationIds = await getAppliedMigrationIds(db);
      pendingMigrations = getPendingMigrations(allMigrations, appliedMigrationIds);

      const lastApplied = await getLastAppliedMigration(db);
      if (lastApplied) {
        lastAppliedMigrationId = lastApplied.id;
        lastAppliedMigrationName = lastApplied.name;
      }

      // Update migration info with applied status
      if (verbose) {
        const appliedSet = new Set(appliedMigrationIds);
        for (const info of migrationsInfo) {
          info.isApplied = appliedSet.has(info.id);
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      errors.push(`Failed to check database state: ${message}`);
    }
  }

  // Step 5: Validate indexes (if db provided and we have schema)
  let indexValidation: IndexValidationDetails | undefined;

  if (db && allMigrations.length > 0) {
    try {
      // Get the final schema from the last migration
      const lastMigration = allMigrations[allMigrations.length - 1];
      const finalSchemas = lastMigration.schemas.collections;

      if (finalSchemas && Object.keys(finalSchemas).length > 0) {
        indexValidation = await validateAllIndexes(db, finalSchemas);

        // Add errors and warnings based on index validation
        if (!indexValidation.areIndexesValid) {
          const missingCount = indexValidation.issues.filter((i) => i.type === "missing").length;
          const outdatedCount = indexValidation.issues.filter((i) => i.type === "outdated").length;
          const orphanedCount = indexValidation.issues.filter((i) => i.type === "orphaned").length;

          const indexIssues = [];
          if (missingCount > 0) indexIssues.push(`${missingCount} missing`);
          if (outdatedCount > 0) indexIssues.push(`${outdatedCount} outdated`);
          if (orphanedCount > 0) indexIssues.push(`${orphanedCount} orphaned`);

          errors.push(`Index validation failed: ${indexIssues.join(", ")} index(es)`);

          // Add detailed warnings for each issue
          if (verbose) {
            for (const issue of indexValidation.issues) {
              warnings.push(`[${issue.collection}] ${issue.description}`);
            }
          }
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      errors.push(`Failed to validate indexes: ${message}`);
    }
  }

  // Compute final status
  const totalMigrations = allMigrations.length;
  const validMigrations = migrationsInfo.filter((m) => m.isValid).length;
  const invalidMigrations = migrationsInfo.filter((m) => !m.isValid).length;

  const isDatabaseUpToDate = db ? pendingMigrations.length === 0 : undefined;
  const areIndexesValid = indexValidation ? indexValidation.areIndexesValid : true;

  const ok = errors.length === 0 &&
    isSchemaConsistent &&
    areMigrationsValid &&
    areIndexesValid &&
    (db ? (isDatabaseUpToDate === true) : true);

  // Generate summary message
  let message = "";

  if (ok) {
    if (db) {
      if (totalMigrations === 0) {
        message = "No migrations found. Migration system is ready.";
      } else if (isDatabaseUpToDate) {
        message =
          `All ${totalMigrations} migration(s) are valid and applied. Database is up to date.`;
      } else {
        message =
          `Migration system is healthy, but ${pendingMigrations.length} migration(s) are pending.`;
      }
    } else {
      if (totalMigrations === 0) {
        message = "No migrations found. Schema validation not applicable.";
      } else {
        message =
          `All ${totalMigrations} migration(s) are valid and consistent with project schema.`;
      }
    }
  } else {
    const issues = [];
    if (!isSchemaConsistent) issues.push("schema inconsistencies");
    if (!areMigrationsValid) issues.push("invalid migrations");
    if (!areIndexesValid && indexValidation) {
      issues.push(`${indexValidation.invalidIndexCount} invalid index(es)`);
    }
    if (db && !isDatabaseUpToDate) {
      issues.push(`${pendingMigrations.length} pending migration(s)`);
    }
    if (errors.length > 0) issues.push(`${errors.length} error(s)`);

    message = `Migration system has issues: ${issues.join(", ")}`;
  }

  // Build result object
  const result: MigrationStatusResult = {
    ok,
    message,
    counts: {
      total: totalMigrations,
      valid: validMigrations,
      invalid: invalidMigrations,
    },
    validation: {
      isSchemaConsistent,
      areMigrationsValid,
      errors,
      warnings,
    },
  };

  // Add database details if db was provided
  if (db) {
    result.database = {
      isUpToDate: isDatabaseUpToDate!,
      appliedCount: appliedMigrationIds.length,
      pendingCount: pendingMigrations.length,
      lastAppliedId: lastAppliedMigrationId,
      lastAppliedName: lastAppliedMigrationName,
      pendingIds: pendingMigrations.map((m) => m.id),
    };
  }

  // Add index validation details if available
  if (indexValidation) {
    result.indexes = indexValidation;
  }

  // Add verbose migration info if requested
  if (verbose && migrationsInfo.length > 0) {
    result.migrations = migrationsInfo;
  }

  return result;
}

/**
 * Throws an error if the migration system is not healthy
 *
 * Useful for application startup checks where you want to fail fast
 *
 * @param options - Options for checking migration status
 * @throws Error if migration system is not healthy
 *
 * @example
 * ```typescript
 * // Fail fast on application startup
 * try {
 *   await assertMigrationSystemHealthy({
 *     migrationsDir: "./migrations",
 *     schemaPath: "./schemas.ts",
 *     db: mongoDb
 *   });
 * } catch (error) {
 *   console.error("Cannot start application:", error.message);
 *   process.exit(1);
 * }
 * ```
 */
export async function assertMigrationSystemHealthy(
  options: CheckMigrationStatusOptions,
): Promise<void> {
  const status = await checkMigrationStatus(options);

  if (!status.ok) {
    const errorDetails = status.validation.errors.length > 0
      ? `\n\nErrors:\n${status.validation.errors.map((e) => `  - ${e}`).join("\n")}`
      : "";

    const warningDetails = status.validation.warnings.length > 0
      ? `\n\nWarnings:\n${status.validation.warnings.map((w) => `  - ${w}`).join("\n")}`
      : "";

    throw new Error(
      `Migration system is not healthy: ${status.message}${errorDetails}${warningDetails}`,
    );
  }
}

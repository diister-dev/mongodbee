/**
 * Migrate command for MongoDBee Migration CLI
 *
 * Applies pending migrations to the database
 *
 * @module
 */

import process from "node:process";
import { blue, bold, dim, green, red, yellow } from "@std/fmt/colors";
import { MongoClient } from "../../../mongodb.ts";
import * as path from "@std/path";

import { loadConfig } from "../../config/loader.ts";
import {
  buildMigrationChain,
  getPendingMigrations,
  loadAllMigrations,
} from "../../discovery.ts";
import {
  getAppliedMigrationIds,
  markMigrationAsApplied,
  markMigrationAsFailed,
} from "../../state.ts";
import { createMongodbApplier } from "../../appliers/mongodb.ts";
import { validateMigrationChainWithProjectSchema } from "../../schema-validation.ts";
import { validateMigrationsWithSimulation } from "../utils/validate-migrations.ts";
import type { SimulationPowerLevel } from "../../validators/simulation.ts";
import { migrationBuilder } from "../../builder.ts";
import { confirm } from "../utils/confirm.ts";
import {
  detectInstancesNeedingCatchUp,
  filterOperationsForModelType,
  getMigrationsForCatchUp,
} from "../../catch-up.ts";
import { recordMultiCollectionMigration } from "../../multicollection-registry.ts";

interface CliArgs {
  config?: string;
  "dry-run"?: boolean;
  force?: boolean;
  verbose?: boolean;
  "auto-sync"?: boolean;
  mode?: string;
  last?: number;
  [key: string]: unknown;
}

export interface MigrateCommandOptions {
  configPath?: string;
  dryRun?: boolean;
  cwd?: string;
  force?: boolean;
  verbose?: boolean;
  /** Automatically catch up orphaned multi-model instances without confirmation */
  autoSync?: boolean;
  /**
   * Simulation mode controlling validation complexity
   * - `quick`: Fast validation with minimal mock data
   * - `normal`: Balanced validation (default)
   * - `hard`: Comprehensive validation with extensive mock data
   */
  mode?: string;
  /**
   * Only validate the last N migrations
   */
  last?: number;
}

/**
 * Parse and validate the simulation mode option
 */
function parseSimulationMode(mode?: string): SimulationPowerLevel {
  if (!mode) return "normal";
  const normalized = mode.toLowerCase();
  if (normalized === "quick" || normalized === "normal" || normalized === "hard") {
    return normalized;
  }
  console.log(yellow(`⚠ Unknown mode "${mode}", using "normal" instead`));
  return "normal";
}

/**
 * Apply pending migrations
 */
export async function migrateCommand(
  options: MigrateCommandOptions = {},
): Promise<void> {
  console.log(bold(blue("🐝 Applying migrations...")));
  console.log();

  let client: MongoClient | undefined;

  try {
    // Map CLI args to options (handle kebab-case to camelCase)
    const cliArgs = options as unknown as CliArgs;
    const opts: MigrateCommandOptions = {
      configPath: options.configPath || cliArgs.config,
      dryRun: options.dryRun || cliArgs["dry-run"],
      cwd: options.cwd,
      force: options.force,
      verbose: options.verbose,
      autoSync: options.autoSync || cliArgs["auto-sync"],
      mode: options.mode || cliArgs.mode,
      last: options.last || cliArgs.last,
    };

    // Load configuration
    const cwd = opts.cwd || process.cwd();
    const config = await loadConfig({ configPath: opts.configPath, cwd });

    const migrationsDir = path.resolve(
      cwd,
      config.paths?.migrations || "./migrations",
    );
    const connectionUri = config.database?.connection?.uri ||
      "mongodb://localhost:27017";
    const dbName = config.database?.name || "myapp";

    console.log(dim(`Migrations directory: ${migrationsDir}`));
    console.log(dim(`Database: ${dbName}`));
    console.log();

    client = new MongoClient(connectionUri);
    await client.connect();

    const db = client.db(dbName);

    // Discover and load migrations
    const migrationsWithFiles = await loadAllMigrations(migrationsDir);
    
    if (migrationsWithFiles.length === 0) {
      console.log(yellow("⚠ No migrations found"));
      console.log();
      return;
    }

    const allMigrations = buildMigrationChain(migrationsWithFiles);

    console.log(dim(`Found ${allMigrations.length} migration(s)`));
    console.log();

    // Validate that last migration matches project schema
    const schemaPath = path.resolve(
      cwd,
      config.paths?.schemas || "./schemas.ts",
    );
    
    console.log(bold("📋 Validating schema consistency..."));
    const schemaValidation = await validateMigrationChainWithProjectSchema(
      allMigrations,
      schemaPath,
    );

    if (schemaValidation.warnings.length > 0) {
      console.log(yellow("\n  Warnings:"));
      for (const warning of schemaValidation.warnings) {
        console.log(yellow(`    ⚠ ${warning}`));
      }
    }

    if (!schemaValidation.valid) {
      console.log(red("\n  ✗ Schema validation failed"));
      for (const error of schemaValidation.errors) {
        console.log(red(`    ${error}`));
      }
      console.log();
      throw new Error("Schema validation failed");
    }

    console.log(green("  ✓ Schema consistency validated"));
    console.log();

    // Get applied migrations
    const appliedIds = await getAppliedMigrationIds(db);
    console.log(dim(`Applied migrations: ${appliedIds.length}`));

    // Calculate pending migrations
    const pendingMigrations = getPendingMigrations(allMigrations, appliedIds);

    if (pendingMigrations.length === 0) {
      console.log(green("✓ No pending migrations. Database is up to date."));
      
      // Even if no pending migrations, check for instances needing catch-up
      console.log();
      console.log(bold(blue("🔍 Checking for multi-model instances needing catch-up...")));
      const catchUpSummary = await detectInstancesNeedingCatchUp(db, allMigrations);
      
      if (catchUpSummary.totalInstances > 0) {
        console.log(yellow(`⚠  Found ${catchUpSummary.totalInstances} instance(s) needing catch-up`));
        console.log();
        
        // Display details
        for (const [modelType, instances] of catchUpSummary.instancesByModel) {
          console.log(yellow(`  Model type: ${modelType}`));
          for (const instance of instances) {
            console.log(yellow(`    • ${instance.collectionName}`));
            console.log(dim(`      Missing ${instance.missingMigrationIds.length} migration(s)`));
            if (instance.isOrphaned) {
              console.log(dim(`      Status: Orphaned (no migration tracking)`));
            }
          }
        }
        console.log();
        
        // Execute catch-up if auto-sync or force
        if (opts.autoSync || opts.force) {
          console.log(bold(blue("\n📦 Catching up multi-model instances...")));
          console.log();

          for (const [modelType, instances] of catchUpSummary.instancesByModel) {
            for (const instance of instances) {
              console.log(
                bold(
                  `Catching up: ${blue(instance.collectionName)} ${dim(`(${modelType})`)}`,
                ),
              );

              const migrationsToApply = getMigrationsForCatchUp(
                allMigrations,
                instance.missingMigrationIds,
              );

              for (const migration of migrationsToApply) {
                console.log(
                  dim(`  Applying: ${migration.name} (${migration.id})`),
                );

                const startTime = Date.now();

                try {
                  const builder = migrationBuilder({ schemas: migration.schemas });
                  const migrator = migration.migrate(builder);

                  // Filter operations for this specific model type
                  const filteredOps = filterOperationsForModelType(
                    migrator.operations,
                    modelType,
                  );

                  if (filteredOps.length === 0) {
                    console.log(dim(`    No relevant operations, skipping`));
                    
                    const duration = Date.now() - startTime;
                    
                    // Still record as applied to maintain consistency
                    await recordMultiCollectionMigration(
                      db,
                      instance.collectionName,
                      migration.id,
                      "applied",
                      duration,
                    );
                    continue;
                  }

                  console.log(
                    dim(`    Applying ${filteredOps.length} operation(s)...`),
                  );

                  // Create applier for this migration
                  const applier = createMongodbApplier(db, migration, {
                    currentMigrationId: migration.id,
                  });

                  // Apply filtered operations
                  for (const op of filteredOps) {
                    await applier.applyOperation(op);
                  }

                  const duration = Date.now() - startTime;

                  // Record migration for this instance
                  await recordMultiCollectionMigration(
                    db,
                    instance.collectionName,
                    migration.id,
                    "applied",
                    duration,
                  );

                  console.log(green(`    ✓ Applied successfully (${duration}ms)`));
                } catch (error) {
                  const duration = Date.now() - startTime;
                  
                  // Record failure
                  await recordMultiCollectionMigration(
                    db,
                    instance.collectionName,
                    migration.id,
                    "failed",
                    duration,
                    error instanceof Error ? error.message : String(error),
                  );

                  console.error(red(`    ✗ Failed: ${error instanceof Error ? error.message : String(error)}`));
                  throw error;
                }
              }

              console.log(green(`  ✓ Catch-up complete for ${instance.collectionName}`));
              console.log();
            }
          }

          console.log(green(`✓ All instances caught up successfully`));
        } else {
          console.log(yellow("  Run 'mongodbee migrate --auto-sync' to catch up these instances"));
        }
      } else {
        console.log(green("  ✓ All multi-model instances are up to date"));
      }
      
      return;
    }

    console.log(yellow(`⚡ Pending migrations: ${pendingMigrations.length}`));
    console.log();

    // STEP 0: Check for multi-model instances needing catch-up
    console.log(bold(blue("🔍 Checking for multi-model instances needing catch-up...")));
    const catchUpSummary = await detectInstancesNeedingCatchUp(db, allMigrations);

    if (catchUpSummary.totalInstances > 0) {
      console.log(yellow(`\n⚠  Found ${catchUpSummary.totalInstances} instance(s) needing catch-up:`));
      console.log();

      // Display details
      for (const [modelType, instances] of catchUpSummary.instancesByModel) {
        console.log(yellow(`  Model type: ${bold(modelType)}`));
        for (const instance of instances) {
          console.log(yellow(`    • ${instance.collectionName}`));
          console.log(dim(`      Missing: ${instance.missingMigrationIds.length} migration(s)`));
          if (instance.isOrphaned) {
            console.log(red(`      ⚠ Orphaned (no migration tracking - will receive ALL migrations)`));
          }
        }
      }

      console.log();
      console.log(dim(`  Total catch-up operations: ${catchUpSummary.totalMissingMigrations}`));
      console.log();

      // Ask for confirmation unless --auto-sync or --force
      if (!opts.autoSync && !opts.force) {
        const confirmed = await confirm(
          "Do you want to catch up these instances before applying new migrations?",
        );

        if (!confirmed) {
          console.log(yellow("Catch-up cancelled. Continuing with pending migrations only..."));
          console.log(
            dim("  Warning: Skipped instances may have schema inconsistencies!"),
          );
          console.log();
        } else {
          // Apply catch-up
          console.log(bold(blue("\n📦 Catching up multi-model instances...")));
          console.log();

          for (const [modelType, instances] of catchUpSummary.instancesByModel) {
            for (const instance of instances) {
              console.log(
                bold(
                  `Catching up: ${blue(instance.collectionName)} ${dim(`(${modelType})`)}`,
                ),
              );

              const migrationsToApply = getMigrationsForCatchUp(
                allMigrations,
                instance.missingMigrationIds,
              );

              for (const migration of migrationsToApply) {
                console.log(
                  dim(`  Applying: ${migration.name} (${migration.id})`),
                );

                const startTime = Date.now();

                try {
                  const builder = migrationBuilder({ schemas: migration.schemas });
                  const migrator = migration.migrate(builder);

                  // Filter operations for this specific model type
                  const filteredOps = filterOperationsForModelType(
                    migrator.operations,
                    modelType,
                  );

                  if (filteredOps.length === 0) {
                    console.log(dim(`    No relevant operations, skipping`));
                    
                    const duration = Date.now() - startTime;
                    
                    // Still record as applied to maintain consistency
                    await recordMultiCollectionMigration(
                      db,
                      instance.collectionName,
                      migration.id,
                      "applied",
                      duration,
                    );
                    continue;
                  }

                  console.log(
                    dim(`    Applying ${filteredOps.length} operation(s)...`),
                  );

                  // Create applier and apply only filtered operations
                  const applier = createMongodbApplier(db, migration, {
                    currentMigrationId: migration.id,
                  });

                  // Apply filtered operations
                  for (const op of filteredOps) {
                    await applier.applyOperation(op);
                  }

                  const duration = Date.now() - startTime;

                  // Record migration as applied for this instance
                  await recordMultiCollectionMigration(
                    db,
                    instance.collectionName,
                    migration.id,
                    "applied",
                    duration,
                  );

                  console.log(green(`    ✓ Applied successfully (${duration}ms)`));
                } catch (error) {
                  const duration = Date.now() - startTime;
                  const errorMessage = error instanceof Error ? error.message : String(error);
                  
                  console.log(red(`    ✗ Failed: ${errorMessage}`));
                  
                  // Record migration as failed for this instance
                  await recordMultiCollectionMigration(
                    db,
                    instance.collectionName,
                    migration.id,
                    "failed",
                    duration,
                    errorMessage,
                  );
                  
                  throw new Error(
                    `Catch-up failed for ${instance.collectionName}: ${errorMessage}`,
                  );
                }
              }

              console.log(
                green(`  ✓ Caught up ${instance.collectionName}`),
              );
              console.log();
            }
          }

          console.log(
            green(
              `✓ Caught up ${catchUpSummary.totalInstances} instance(s) successfully`,
            ),
          );
          console.log();
        }
      } else {
        // Auto-sync enabled or force flag - apply catch-up automatically
        if (opts.autoSync) {
          console.log(dim("  --auto-sync flag detected, catching up automatically..."));
        } else {
          console.log(dim("  --force flag detected, catching up automatically..."));
        }
        console.log();

        // Apply catch-up (same code as in the confirmed block)
        console.log(bold(blue("📦 Catching up multi-model instances...")));
        console.log();

        for (const [modelType, instances] of catchUpSummary.instancesByModel) {
          for (const instance of instances) {
            console.log(
              bold(
                `Catching up: ${blue(instance.collectionName)} ${dim(`(${modelType})`)}`,
              ),
            );

            const migrationsToApply = getMigrationsForCatchUp(
              allMigrations,
              instance.missingMigrationIds,
            );

            for (const migration of migrationsToApply) {
              console.log(
                dim(`  Applying: ${migration.name} (${migration.id})`),
              );

              const startTime = Date.now();

              try {
                const builder = migrationBuilder({ schemas: migration.schemas });
                const migrator = migration.migrate(builder);

                // Filter operations for this specific model type
                const filteredOps = filterOperationsForModelType(
                  migrator.operations,
                  modelType,
                );

                if (filteredOps.length === 0) {
                  console.log(dim(`    No relevant operations, skipping`));
                  
                  const duration = Date.now() - startTime;
                  
                  // Still record as applied to maintain consistency
                  await recordMultiCollectionMigration(
                    db,
                    instance.collectionName,
                    migration.id,
                    "applied",
                    duration,
                  );
                  continue;
                }

                console.log(
                  dim(`    Applying ${filteredOps.length} operation(s)...`),
                );

                // Create applier and apply only filtered operations
                const applier = createMongodbApplier(db, migration, {
                  currentMigrationId: migration.id,
                });

                // Apply filtered operations
                for (const op of filteredOps) {
                  await applier.applyOperation(op);
                }

                const duration = Date.now() - startTime;

                // Record migration as applied for this instance
                await recordMultiCollectionMigration(
                  db,
                  instance.collectionName,
                  migration.id,
                  "applied",
                  duration,
                );

                console.log(green(`    ✓ Applied successfully (${duration}ms)`));
              } catch (error) {
                const duration = Date.now() - startTime;
                const errorMessage = error instanceof Error ? error.message : String(error);
                
                console.log(red(`    ✗ Failed: ${errorMessage}`));
                
                // Record migration as failed for this instance
                await recordMultiCollectionMigration(
                  db,
                  instance.collectionName,
                  migration.id,
                  "failed",
                  duration,
                  errorMessage,
                );
                
                throw new Error(
                  `Catch-up failed for ${instance.collectionName}: ${errorMessage}`,
                );
              }
            }

            console.log(
              green(`  ✓ Caught up ${instance.collectionName}`),
            );
            console.log();
          }
        }

        console.log(
          green(
            `✓ Caught up ${catchUpSummary.totalInstances} instance(s) successfully`,
          ),
        );
        console.log();
      }
    } else {
      console.log(green("  ✓ All multi-model instances are up to date"));
      console.log();
    }

    // STEP 1: Validate ALL pending migrations BEFORE applying any
    const powerLevel = parseSimulationMode(opts.mode);
    const lastN = opts.last && opts.last > 0 ? opts.last : undefined;

    await validateMigrationsWithSimulation(allMigrations, {
      verbose: opts.verbose,
      powerLevel,
      lastN,
    });

    // STEP 2: Check for irreversible or lossy migrations
    const migrationsWithIssues: Array<{
      migration: typeof pendingMigrations[0];
      irreversible: boolean;
      lossyTransforms: string[];
    }> = [];

    for (const migration of pendingMigrations) {
      const builder = migrationBuilder({
        schemas: migration.schemas,
        parentSchemas: migration.parent?.schemas,
      });
      const state = migration.migrate(builder);

      const isIrreversible = state.hasProperty("irreversible");
      const isLossy = state.hasProperty("lossy");
      const lossyTransforms = isLossy
        ? state.operations
            .filter((op) => {
              if (op.type === "create_collection") return true;
              if (op.type === "create_multicollection") return true;
              if (op.type === "create_multimodel_instance") return true;
              if (op.type === "update_indexes") return true;
              if (
                (op.type === "transform_collection" ||
                  op.type === "transform_multicollection_type" ||
                  op.type === "transform_multimodel_instance_type" ||
                  op.type === "transform_multimodel_instances_type") &&
                op.lossy
              ) {
                return true;
              }
              return false;
            })
            .map((op) => {
              if (op.type === "create_collection") {
                return `Create collection: ${op.collectionName}`;
              } else if (op.type === "create_multicollection") {
                return `Create multi-collection: ${op.collectionName}`;
              } else if (op.type === "create_multimodel_instance") {
                return `Create multi-model instance: ${op.collectionName}`;
              } else if (op.type === "update_indexes") {
                return `Update indexes: ${op.collectionName}`;
              } else if (op.type === "transform_collection") {
                return `Transform collection: ${op.collectionName}`;
              } else if (op.type === "transform_multicollection_type") {
                return `Transform multi-collection type: ${op.collectionName}.${op.documentType}`;
              } else if (op.type === "transform_multimodel_instance_type") {
                return `Transform multi-model instance type: ${op.collectionName}.${op.documentType}`;
              } else if (op.type === "transform_multimodel_instances_type") {
                return `Transform multi-model instances type: ${op.modelType}.${op.documentType}`;
              }
              return "";
            })
        : [];

      if (isIrreversible || isLossy) {
        migrationsWithIssues.push({
          migration,
          irreversible: isIrreversible,
          lossyTransforms,
        });
      }
    }

    // Separate irreversible from lossy
    const irreversibleMigrations = migrationsWithIssues.filter((m) =>
      m.irreversible
    );
    const lossyOnlyMigrations = migrationsWithIssues.filter((m) =>
      !m.irreversible && m.lossyTransforms.length > 0
    );

    // Show irreversible warnings and require confirmation
    if (irreversibleMigrations.length > 0 && !options.force) {
      console.log(
        red("⚠  WARNING: Some migrations are IRREVERSIBLE (cannot be rolled back):"),
      );
      console.log();

      for (const issue of irreversibleMigrations) {
        console.log(red(`  • ${issue.migration.name}`));
      }

      console.log();

      const confirmed = await confirm(
        "Do you want to proceed with these irreversible migrations?",
      );

      if (!confirmed) {
        console.log(yellow("Migration cancelled."));
        console.log(dim("  Tip: Use --force to skip this confirmation"));
        return;
      }

      console.log();
    }

    // Show lossy warnings (informational only, no confirmation needed for migrate)
    if (lossyOnlyMigrations.length > 0) {
      console.log(
        yellow(
          "ℹ  Note: Some migrations are LOSSY (rollback will result in data loss):",
        ),
      );
      console.log();

      for (const issue of lossyOnlyMigrations) {
        console.log(yellow(`  ${issue.migration.name}:`));
        for (const op of issue.lossyTransforms) {
          console.log(yellow(`    - ${op}`));
        }
      }

      console.log();
    }

    // STEP 3: Apply each pending migration
    console.log(bold(blue("📝 Applying migrations...")));
    console.log();

    for (const migration of pendingMigrations) {
      console.log(
        bold(`Applying: ${blue(migration.name)} ${dim(`(${migration.id})`)}`),
      );

      if (options.dryRun) {
        console.log(dim("  [DRY RUN] Skipping actual execution"));
        continue;
      }

      try {
        const startTime = Date.now();

        // Execute migration on real database
        const builder = migrationBuilder({ schemas: migration.schemas });
        const migrator = migration.migrate(builder);

        if (options.verbose) {
          console.log(dim(`  📦 Operations: ${migrator.operations.length}`));
        }

        console.log(dim("  📝 Executing operations..."));

        // Create applier with migration context
        const migrationApplier = createMongodbApplier(db, migration, {
          currentMigrationId: migration.id,
        });

        // Apply all operations and synchronize schemas
        await migrationApplier.applyMigration(migrator.operations, 'up');

        const duration = Date.now() - startTime;

        // Mark as applied
        await markMigrationAsApplied(
          db,
          migration.id,
          migration.name,
          duration,
        );

        console.log(
          green(`  ✓ Applied successfully ${dim(`(${duration}ms)`)}`),
        );
      } catch (error) {
        console.error(error);
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        console.error(red(`  ✗ Failed: ${errorMessage}`));

        // Mark as failed
        await markMigrationAsFailed(
          db,
          migration.id,
          migration.name,
          errorMessage,
        );

        throw new Error(`Migration ${migration.name} failed: ${errorMessage}`);
      }

      console.log();
    }

    console.log(green(bold("✓ All migrations applied successfully!")));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red(bold("Error:")), message);
    throw error;
  } finally {
    if (client) {
      await client.close(true);
    }
  }
}

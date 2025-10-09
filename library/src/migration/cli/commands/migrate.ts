/**
 * Migrate command for MongoDBee Migration CLI
 *
 * Applies pending migrations to the database
 *
 * @module
 */

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
import { MongodbApplier } from "../../appliers/mongodb.ts";
import { validateMigrationChainWithProjectSchema } from "../../schema-validation.ts";
import { validateMigrationsWithSimulation } from "../utils/validate-migrations.ts";
import { migrationBuilder } from "../../builder.ts";
import { confirm } from "../utils/confirm.ts";

export interface MigrateCommandOptions {
  configPath?: string;
  dryRun?: boolean;
  cwd?: string;
  force?: boolean;
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
    // Load configuration
    const cwd = options.cwd || Deno.cwd();
    const config = await loadConfig({ configPath: options.configPath, cwd });

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
    console.log(dim("Discovering migrations..."));
    const migrationsWithFiles = await loadAllMigrations(migrationsDir);
    if (migrationsWithFiles.length === 0) {
      console.log(yellow("⚠ No migrations found"));
      return;
    }

    const allMigrations = buildMigrationChain(migrationsWithFiles);

    console.log(dim(`Found ${allMigrations.length} migration(s)`));

    // Validate that last migration matches project schema
    if (allMigrations.length > 0) {
      const schemaPath = path.resolve(
        cwd,
        config.paths?.schemas || "./schemas.ts",
      );
      const schemaValidation = await validateMigrationChainWithProjectSchema(
        allMigrations,
        schemaPath,
      );

      if (schemaValidation.warnings.length > 0) {
        for (const warning of schemaValidation.warnings) {
          console.log(yellow(`  ⚠ ${warning}`));
        }
      }

      if (!schemaValidation.valid) {
        console.log();
        throw new Error("Schema validation failed. See errors above.", {
          cause: schemaValidation,
        });
      }

      console.log(green(`✓ Schema validation passed`));
      console.log();
    }

    // Get applied migrations
    const appliedIds = await getAppliedMigrationIds(db);
    console.log(dim(`Applied migrations: ${appliedIds.length}`));

    // Calculate pending migrations
    const pendingMigrations = getPendingMigrations(allMigrations, appliedIds);

    if (pendingMigrations.length === 0) {
      console.log(green("✓ No pending migrations. Database is up to date."));
      return;
    }

    console.log(yellow(`⚡ Pending migrations: ${pendingMigrations.length}`));
    console.log();

    // STEP 1: Validate ALL pending migrations BEFORE applying any
    await validateMigrationsWithSimulation(pendingMigrations);

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
              if (op.type === "create_multicollection_instance") return true;
              if (op.type === "update_indexes") return true;
              if (
                (op.type === "transform_collection" ||
                  op.type === "transform_multicollection_type") &&
                op.lossy
              ) {
                return true;
              }
              return false;
            })
            .map((op) => {
              if (op.type === "create_collection") {
                return `Create collection: ${op.collectionName}`;
              } else if (op.type === "create_multicollection_instance") {
                return `Create multi-collection: ${op.collectionName}`;
              } else if (op.type === "update_indexes") {
                return `Update indexes: ${op.collectionName}`;
              } else if (op.type === "transform_collection") {
                return `Transform: ${op.collectionName}`;
              } else if (op.type === "transform_multicollection_type") {
                return `Transform: ${op.collectionType}.${op.typeName}`;
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
    const applier = new MongodbApplier(db);
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
        console.log(dim("  📝 Executing operations..."));
        const builder = migrationBuilder({ schemas: migration.schemas });
        const state = migration.migrate(builder);

        // ✅ Set migration ID for version tracking
        applier.setCurrentMigrationId(migration.id);

        // Apply operations
        for (const operation of state.operations) {
          await applier.applyOperation(operation);
        }

        // Synchronize validators and indexes with migration schemas
        console.log(dim("  🔧 Synchronizing validators and indexes..."));
        await applier.synchronizeSchemas(migration.schemas);

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
      await client.close();
    }
  }
}

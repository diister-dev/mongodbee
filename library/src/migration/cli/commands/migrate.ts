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
import { createSimulationValidator } from "../../validators/simulation.ts";
import { migrationBuilder } from "../../builder.ts";
import { validateMigrationChainWithProjectSchema } from "../../schema-validation.ts";

export interface MigrateCommandOptions {
  configPath?: string;
  dryRun?: boolean;
  cwd?: string;
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
    console.log(dim("Loading configuration..."));
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
    console.log(dim(`Connection URI: ${connectionUri}`));
    console.log(dim(`Database: ${dbName}`));
    console.log();

    // Connect to database
    console.log(dim("Connecting to database..."));
    client = new MongoClient(connectionUri);
    await client.connect();

    const db = client.db(dbName);

    console.log(green(`✓ Connected to database: ${dbName}`));
    console.log();

    // Discover and load migrations
    console.log(dim("Discovering migrations..."));
    const migrationsWithFiles = await loadAllMigrations(migrationsDir);
    const allMigrations = buildMigrationChain(migrationsWithFiles);

    console.log(green(`✓ Found ${allMigrations.length} migration(s)`));
    console.log();

    // Validate that last migration matches project schema
    if (allMigrations.length > 0) {
      console.log(dim("Validating schema consistency..."));
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

    // Apply each pending migration
    const applier = new MongodbApplier(db);
    const simulationValidator = createSimulationValidator({
      validateReversibility: true,
      strictValidation: true,
      maxOperations: 1000,
    });

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

        // Step 1: Validate migration with simulation
        console.log(dim("  🧪 Validating with simulation..."));
        const validationResult = await simulationValidator.validateMigration(
          migration,
        );

        if (!validationResult.success) {
          console.error(red(`  ✗ Simulation validation failed:`));
          for (const error of validationResult.errors) {
            console.error(red(`    ${error}`));
          }
          throw new Error(`Migration simulation validation failed`);
        }

        if (validationResult.warnings.length > 0) {
          for (const warning of validationResult.warnings) {
            console.log(yellow(`  ⚠ ${warning}`));
          }
        }

        console.log(
          green(
            `  ✓ Simulation validation passed (${
              validationResult.data?.operationCount || 0
            } operations)`,
          ),
        );

        // Step 2: Execute migration on real database
        console.log(dim("  📝 Executing migration..."));
        const builder = migrationBuilder({ schemas: migration.schemas });
        const state = migration.migrate(builder);

        // ✅ Set migration ID for version tracking
        applier.setCurrentMigrationId(migration.id);

        // Apply operations
        for (const operation of state.operations) {
          await applier.applyOperation(operation);
        }

        // Step 3: Synchronize validators and indexes with migration schemas
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

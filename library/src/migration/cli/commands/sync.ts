/**
 * Sync command for MongoDBee Migration CLI
 *
 * Synchronizes database schemas and indexes with the latest migration state.
 * Ensures all migrations are applied before syncing (unless --force is used).
 *
 * @module
 */

import { blue, bold, dim, green, red, yellow } from "@std/fmt/colors";
import { MongoClient } from "../../../mongodb.ts";
import * as path from "@std/path";

import { loadConfig } from "../../config/loader.ts";
import {
  buildMigrationChain,
  loadAllMigrations,
} from "../../discovery.ts";
import { getAppliedMigrationIds } from "../../state.ts";
import { createMongodbApplier } from "../../appliers/mongodb.ts";

export interface SyncCommandOptions {
  configPath?: string;
  cwd?: string;
  force?: boolean;
  verbose?: boolean;
}

/**
 * Synchronize schemas and indexes with the latest migration state
 */
export async function syncCommand(
  options: SyncCommandOptions = {},
): Promise<void> {
  console.log(bold(blue("🐝 Synchronizing schemas and indexes...")));
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

    // Connect to database
    client = new MongoClient(connectionUri);
    await client.connect();

    const db = client.db(dbName);

    // Load migrations from filesystem
    const migrationsWithFiles = await loadAllMigrations(migrationsDir);

    if (migrationsWithFiles.length === 0) {
      console.log(yellow("⚠ No migrations found"));
      console.log();
      return;
    }

    const allMigrations = buildMigrationChain(migrationsWithFiles);
    console.log(dim(`Found ${allMigrations.length} migration(s)`));

    // Get applied migrations
    const appliedIds = await getAppliedMigrationIds(db);
    console.log(dim(`Applied migrations: ${appliedIds.length}`));
    console.log();

    // Check if all migrations are applied
    const pendingMigrations = allMigrations.filter(
      (m) => !appliedIds.includes(m.id),
    );

    if (pendingMigrations.length > 0 && !options.force) {
      console.error(
        red(
          bold(
            `✗ Cannot sync: ${pendingMigrations.length} pending migration(s) not applied`,
          ),
        ),
      );
      console.log();
      console.log(red("Pending migrations:"));
      for (const migration of pendingMigrations) {
        console.log(red(`  • ${migration.name} (${migration.id})`));
      }
      console.log();
      console.log(
        yellow("Run 'mongodbee migrate' to apply pending migrations first"),
      );
      console.log(
        dim("Or use --force to sync schemas anyway (not recommended)"),
      );
      throw new Error("Cannot sync: pending migrations detected");
    }

    if (pendingMigrations.length > 0 && options.force) {
      console.log(
        yellow(
          `⚠ Warning: ${pendingMigrations.length} pending migration(s) detected, but --force flag is set`,
        ),
      );
      console.log(dim("  Proceeding with sync anyway..."));
      console.log();
    }

    // Get the latest migration (current schema state)
    const latestMigration = allMigrations[allMigrations.length - 1];

    if (!latestMigration) {
      console.log(yellow("⚠ No migrations to sync"));
      return;
    }

    console.log(
      bold(
        `Syncing to latest migration: ${blue(latestMigration.name)} ${
          dim(`(${latestMigration.id})`)
        }`,
      ),
    );
    console.log();

    if (options.verbose) {
      console.log(dim("Latest schema definition:"));
      if (latestMigration.schemas.collections) {
        console.log(
          dim(
            `  Collections: ${
              Object.keys(latestMigration.schemas.collections).join(", ")
            }`,
          ),
        );
      }
      if (latestMigration.schemas.multiCollections) {
        console.log(
          dim(
            `  Multi-collections: ${
              Object.keys(latestMigration.schemas.multiCollections).join(", ")
            }`,
          ),
        );
      }
      if (latestMigration.schemas.multiModels) {
        console.log(
          dim(
            `  Multi-models: ${
              Object.keys(latestMigration.schemas.multiModels).join(", ")
            }`,
          ),
        );
      }
      console.log();
    }

    // Create applier for synchronization
    const applier = createMongodbApplier(db, latestMigration, {
      currentMigrationId: latestMigration.id,
    });

    console.log(bold(blue("📋 Synchronizing validators and indexes...")));
    console.log();

    // The applier has a synchronizeValidatorsAndIndexes method that we can't access directly
    // So we'll use applyMigration with an empty operations array, which will trigger synchronization
    // Actually, let's directly synchronize by applying the migration with empty operations
    // We need to access the internal synchronization - let's use a workaround

    // Apply an empty migration to trigger synchronization
    await applier.applyMigration([], "up");

    console.log(green("  ✓ Validators synchronized"));
    console.log(green("  ✓ Indexes synchronized"));
    console.log();

    console.log(green(bold("✓ Synchronization complete!")));
    console.log();
    console.log(
      dim("All schemas and indexes are now up to date with the latest migration."),
    );
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

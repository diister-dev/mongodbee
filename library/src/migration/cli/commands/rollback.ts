/**
 * Revert command for MongoDBee Migration CLI
 *
 * Reverts the last applied migration
 *
 * @module
 */

import { green, yellow, red, dim, blue, bold } from "@std/fmt/colors";
import { MongoClient } from "../../../mongodb.ts";
import * as path from "@std/path";

import { loadConfig } from "../../config/loader.ts";
import { loadAllMigrations, buildMigrationChain } from "../../discovery.ts";
import { getLastAppliedMigration, markMigrationAsReverted } from "../../state.ts";
import { MongodbApplier } from "../../appliers/mongodb.ts";
import { migrationBuilder } from "../../builder.ts";

export interface RevertCommandOptions {
  configPath?: string;
  force?: boolean;
}

/**
 * Prompts user for confirmation
 */
async function confirm(message: string): Promise<boolean> {
  console.log(yellow(message));
  console.log(dim("Type 'yes' to confirm: "));

  const buf = new Uint8Array(1024);
  const n = await Deno.stdin.read(buf);

  if (n === null) {
    return false;
  }

  const answer = new TextDecoder().decode(buf.subarray(0, n)).trim().toLowerCase();
  return answer === 'yes';
}

/**
 * Reverts the last applied migration
 */
export async function rollbackCommand(options: RevertCommandOptions = {}): Promise<void> {
  console.log(bold(blue("🐝 Reverting migration...")));
  console.log();

  let client: MongoClient | undefined;

  try {
    // Load configuration
    console.log(dim("Loading configuration..."));
    const config = await loadConfig({ configPath: options.configPath });

    const migrationsDir = path.resolve(config.paths?.migrationsDir || "./migrations");
    const connectionUri = config.db?.uri || "mongodb://localhost:27017";
    const dbName = config.db?.name || "myapp";

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

    // Get last applied migration
    const lastApplied = await getLastAppliedMigration(db);

    if (!lastApplied) {
      console.log(yellow("No migrations to revert."));
      return;
    }

    // Load migrations to find the one to revert
    const migrationsWithFiles = await loadAllMigrations(migrationsDir);
    const allMigrations = buildMigrationChain(migrationsWithFiles);

    const migrationToRevert = allMigrations.find(m => m.id === lastApplied.id);

    if (!migrationToRevert) {
      throw new Error(
        `Migration ${lastApplied.id} is marked as applied but not found in filesystem`
      );
    }

    console.log(bold(`Last applied migration: ${blue(migrationToRevert.name)}`));
    console.log(dim(`  ID: ${migrationToRevert.id}`));
    console.log(dim(`  Applied at: ${lastApplied.appliedAt?.toISOString()}`));
    console.log();

    // Check if migration is reversible
    const builder = migrationBuilder({ schemas: migrationToRevert.schemas });
    const state = migrationToRevert.migrate(builder);

    if (state.hasProperty('irreversible')) {
      console.log(red("⚠  This migration is marked as IRREVERSIBLE."));
      console.log(red("   Reverting it may lead to data loss."));
      console.log();

      if (!options.force) {
        const confirmed = await confirm("Are you sure you want to revert this migration?");

        if (!confirmed) {
          console.log(yellow("Revert cancelled."));
          return;
        }
      }
    }

    // Apply reverse operations
    console.log(bold("Reverting operations..."));

    const applier = new MongodbApplier(db);

    try {
      // Apply operations in reverse order
      for (let i = state.operations.length - 1; i >= 0; i--) {
        const operation = state.operations[i];
        console.log(dim(`  Reverting ${operation.type} on ${operation.collectionName}...`));

        try {
          await applier.applyReverseOperation(operation);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          console.log(yellow(`  ⚠ Could not reverse ${operation.type}: ${message}`));

          if (operation.type === 'create_collection') {
            console.log(dim("    Note: Collection creation cannot be automatically reversed."));
            console.log(dim("    You may need to manually drop the collection if needed."));
          }
        }
      }

      // Mark as reverted
      await markMigrationAsReverted(db, migrationToRevert.id);

      console.log();
      console.log(green(bold("✓ Migration reverted successfully!")));
      console.log();
      console.log(dim("  Note: The migration file still exists in the filesystem."));
      console.log(dim("  To re-apply it, run `mongodbee apply`."));

    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Failed to revert migration: ${message}`);
    }

  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red(bold("Error:")), message);
    Deno.exit(1);
  } finally {
    if (client) {
      await client.close();
    }
  }
}
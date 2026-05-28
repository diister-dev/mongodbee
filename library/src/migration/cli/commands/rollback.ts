/**
 * Rollback command for MongoDBee Migration CLI
 *
 * Rolls back the last applied migration
 *
 * @module
 */

import process from "node:process";
import { blue, bold, dim, green, red, yellow } from "@std/fmt/colors";
import { MongoClient } from "../../../mongodb.ts";
import * as path from "@std/path";

import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import {
  getLastAppliedMigration,
  markMigrationAsReverted,
} from "../../state.ts";
import { createMongodbApplier } from "../../appliers/mongodb.ts";
import {
  getIrreversibleOperations,
  getLossyOperations,
  migrationBuilder,
} from "../../builder.ts";
import { confirm } from "../utils/confirm.ts";

export interface RollbackCommandOptions {
  configPath?: string;
  force?: boolean;
  cwd?: string;
}

/**
 * Rolls back the last applied migration
 */
export async function rollbackCommand(
  options: RollbackCommandOptions = {},
): Promise<void> {
  console.log(bold(blue("🐝 Rolling back migration...")));
  console.log();

  let client: MongoClient | undefined;

  try {
    // Load configuration
    const cwd = options.cwd || process.cwd();
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
    console.log();

    // Get last applied migration
    const lastApplied = await getLastAppliedMigration(db);

    if (!lastApplied) {
      console.log(yellow("No migrations to rollback."));
      return;
    }

    // Load migrations to find the one to rollback
    const migrationsWithFiles = await loadAllMigrations(migrationsDir);
    const allMigrations = buildMigrationChain(migrationsWithFiles);

    const migrationToRollback = allMigrations.find((m) =>
      m.id === lastApplied.id
    );

    if (!migrationToRollback) {
      throw new Error(
        `Migration ${lastApplied.id} is marked as applied but not found in filesystem`,
      );
    }

    console.log(
      bold(`Last applied migration: ${blue(migrationToRollback.name)}`),
    );
    console.log(dim(`  ID: ${migrationToRollback.id}`));
    console.log(dim(`  Applied at: ${lastApplied.appliedAt?.toISOString()}`));
    console.log();

    // Check if migration is reversible
    const builder = migrationBuilder({ schemas: migrationToRollback.schemas });
    const state = migrationToRollback.migrate(builder);

    // Compact "where" label for an operation, for human-readable listings.
    const opLabel = (op: typeof state.operations[number]): string => {
      const o = op as Record<string, unknown>;
      const target = (o.collectionName ?? o.modelType ?? "") as string;
      const docType = o.documentType ? `.${o.documentType as string}` : "";
      return target ? `${op.type}: ${target}${docType}` : op.type;
    };

    // Irreversible operations cannot be rolled back at all — refuse up-front
    // (the applier would throw anyway, before mutating anything). `force`
    // does not override this: there is no valid down() to run.
    const irreversibleOps = getIrreversibleOperations(state.operations);
    if (irreversibleOps.length > 0) {
      console.log(red("✗  This migration is IRREVERSIBLE and cannot be rolled back."));
      console.log(red("   Irreversible operation(s):"));
      for (const op of irreversibleOps) {
        console.log(red(`   - ${opLabel(op)}`));
      }
      console.log();
      console.log(
        dim(
          "To move forward instead, write a new migration that performs the " +
            "inverse change explicitly.",
        ),
      );
      return;
    }

    // Lossy transforms CAN be rolled back, but won't restore the exact
    // original data — confirm before proceeding.
    const lossyOps = getLossyOperations(state.operations);
    if (lossyOps.length > 0) {
      console.log(yellow("⚠  This migration contains LOSSY transformations:"));
      for (const op of lossyOps) {
        console.log(yellow(`   - ${opLabel(op)}`));
      }
      console.log(yellow("   Rolling back will result in DATA LOSS."));
      console.log();

      if (!options.force) {
        const confirmed = await confirm(
          "Are you sure you want to rollback this migration?",
        );

        if (!confirmed) {
          console.log(yellow("Rollback cancelled."));
          return;
        }
      }
    }

    // Apply reverse operations
    console.log(bold("Rolling back operations..."));

    try {
      // Create applier with migration context
      const applier = createMongodbApplier(db, migrationToRollback, {
        currentMigrationId: migrationToRollback.id,
      });

      // Reverse operations and synchronize with parent schemas.
      // applyMigration('down') undoes operations in LIFO order internally,
      // so we pass them in natural (forward) order. This handles:
      // - Collections
      // - Multi-collections
      // - Multi-model instances (with automatic history recording)
      await applier.applyMigration(state.operations, 'down');

      // Mark as reverted in global history
      await markMigrationAsReverted(db, migrationToRollback.id);

      console.log();
      console.log(green(bold("✓ Migration rolled back successfully!")));
      console.log();
      console.log(
        dim("  Note: The migration file still exists in the filesystem."),
      );
      console.log(dim("  To re-apply it, run `mongodbee migrate`."));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Failed to rollback migration: ${message}`);
    }
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

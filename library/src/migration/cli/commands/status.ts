/**
 * Status command for MongoDBee Migration CLI
 *
 * Shows migration status and information
 *
 * @module
 */

import { blue, bold, dim, gray, green, red, yellow } from "@std/fmt/colors";
import { MongoClient } from "../../../mongodb.ts";
import * as path from "@std/path";

import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import {
  getAllMigrationStates,
  type MigrationStateRecord,
} from "../../state.ts";
import { getAllOperations } from "../../history.ts";

export interface StatusCommandOptions {
  configPath?: string;
  history?: boolean;
  cwd?: string;
}

/**
 * Show migration status
 */
export async function statusCommand(
  options: StatusCommandOptions = {},
): Promise<void> {
  console.log(bold(blue("🐝 Migration Status")));
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

    // Load migrations from filesystem
    const migrationsWithFiles = await loadAllMigrations(migrationsDir);
    const allMigrations = buildMigrationChain(migrationsWithFiles);

    // Get migration states from database
    const migrationStates = await getAllMigrationStates(db);
    const statesMap = new Map<string, MigrationStateRecord>(
      migrationStates.map((s) => [s.id, s]),
    );

    // Display status table
    console.log(bold("Migrations:"));
    console.log();

    if (allMigrations.length === 0) {
      console.log(dim("  No migrations found."));
      console.log();
      return;
    }

    // Calculate column widths for table
    const maxIdLength = Math.max(...allMigrations.map((m) => m.id.length), 10);
    const maxNameLength = Math.max(
      ...allMigrations.map((m) => m.name.length),
      15,
    );

    // Header
    console.log(
      gray(
        `  ${"ID".padEnd(maxIdLength)}  ${
          "Name".padEnd(maxNameLength)
        }  Status      Applied`,
      ),
    );
    console.log(
      gray(
        `  ${"─".repeat(maxIdLength)}  ${"─".repeat(maxNameLength)}  ${
          "─".repeat(10)
        }  ${"─".repeat(20)}`,
      ),
    );

    // Rows
    for (const migration of allMigrations) {
      const state = statesMap.get(migration.id);

      let statusDisplay: string;
      let appliedDisplay: string;

      if (!state) {
        statusDisplay = yellow("pending");
        appliedDisplay = dim("-");
      } else if (state.status === "applied") {
        statusDisplay = green("applied");
        appliedDisplay = state.appliedAt
          ? dim(state.appliedAt.toISOString().split("T")[0])
          : dim("-");
      } else if (state.status === "failed") {
        statusDisplay = red("failed");
        appliedDisplay = dim(state.error?.slice(0, 20) || "error");
      } else if (state.status === "reverted") {
        statusDisplay = blue("reverted");
        appliedDisplay = state.revertedAt
          ? dim(state.revertedAt.toISOString().split("T")[0])
          : dim("-");
      } else {
        statusDisplay = yellow(state.status);
        appliedDisplay = dim("-");
      }

      console.log(
        `  ${dim(migration.id.padEnd(maxIdLength))}  ${
          migration.name.padEnd(maxNameLength)
        }  ${statusDisplay.padEnd(10)}  ${appliedDisplay}`,
      );
    }

    console.log();

    // Summary
    const appliedCount = migrationStates.filter((s) =>
      s.status === "applied"
    ).length;
    const pendingCount = allMigrations.length - appliedCount;

    console.log(bold("Summary:"));
    console.log(dim(`  Total migrations: ${allMigrations.length}`));
    console.log(green(`  Applied: ${appliedCount}`));
    if (pendingCount > 0) {
      console.log(yellow(`  Pending: ${pendingCount}`));
    } else {
      console.log(dim(`  Pending: ${pendingCount}`));
    }

    console.log();

    if (pendingCount > 0) {
      console.log(dim("  Run `mongodbee apply` to apply pending migrations."));
    } else {
      console.log(green("  ✓ Database is up to date!"));
    }

    // Show detailed history if requested
    if (options.history) {
      console.log();
      console.log(bold("Operation History:"));
      console.log();

      const allOps = await getAllOperations(db);

      if (allOps.length === 0) {
        console.log(dim("  No operations recorded yet."));
      } else {
        // Group operations by migration
        const byMigration = new Map<string, typeof allOps>();
        for (const op of allOps) {
          const existing = byMigration.get(op.migrationId) || [];
          existing.push(op);
          byMigration.set(op.migrationId, existing);
        }

        // Display operations for each migration
        for (const migration of allMigrations) {
          const ops = byMigration.get(migration.id);
          if (!ops || ops.length === 0) continue;

          console.log(dim(`  ${migration.name} (${migration.id}):`));

          for (const op of ops) {
            const dateStr =
              op.executedAt.toISOString().replace("T", " ").split(".")[0];
            const durationStr = op.duration ? dim(`(${op.duration}ms)`) : "";

            let icon = "  ";
            let opColor = dim;

            switch (op.operation) {
              case "applied":
                icon = op.status === "success" ? "✅" : "❌";
                opColor = op.status === "success" ? green : red;
                break;
              case "reverted":
                icon = op.status === "success" ? "🔄" : "❌";
                opColor = op.status === "success" ? blue : red;
                break;
              case "failed":
                icon = "❌";
                opColor = red;
                break;
            }

            console.log(
              `    ${icon} ${dim(dateStr)}  ${
                opColor(op.operation.padEnd(10))
              } ${durationStr}`,
            );
            if (op.error) {
              console.log(`       ${red(dim(op.error.slice(0, 60)))}`);
            }
          }

          console.log();
        }
      }
    }
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

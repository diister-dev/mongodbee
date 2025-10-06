/**
 * History command for MongoDBee Migration CLI
 *
 * Shows detailed operation history for migrations
 *
 * @module
 */

import { blue, bold, dim, gray, green, red } from "@std/fmt/colors";
import { MongoClient } from "../../../mongodb.ts";
import * as path from "@std/path";

import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import { getAllOperations, getMigrationHistory } from "../../history.ts";

export interface HistoryCommandOptions {
  configPath?: string;
  migrationId?: string;
  cwd?: string;
}

/**
 * Show migration operation history
 */
export async function historyCommand(
  options: HistoryCommandOptions = {},
): Promise<void> {
  console.log(bold(blue("🐝 Migration History")));
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

    // Load migrations
    const migrationsWithFiles = await loadAllMigrations(migrationsDir);
    const allMigrations = buildMigrationChain(migrationsWithFiles);

    // Get operations
    let operations;
    if (options.migrationId) {
      // Show history for specific migration
      operations = await getMigrationHistory(db, options.migrationId);
      const migration = allMigrations.find((m) => m.id === options.migrationId);

      if (!migration) {
        throw new Error(`Migration ${options.migrationId} not found`);
      }

      console.log(bold(`History for: ${blue(migration.name)}`));
      console.log(dim(`  ID: ${migration.id}`));
      console.log();

      if (operations.length === 0) {
        console.log(dim("  No operations recorded for this migration."));
        return;
      }
    } else {
      // Show all operations
      operations = await getAllOperations(db);

      if (operations.length === 0) {
        console.log(dim("No operations recorded yet."));
        console.log();
        console.log(dim("Run `mongodbee migrate` to apply migrations."));
        return;
      }

      console.log(bold("All Operations:"));
      console.log();
    }

    // Display operations timeline
    for (const op of operations) {
      const dateStr =
        op.executedAt.toISOString().replace("T", " ").split(".")[0];
      const durationStr = op.duration ? dim(`(${op.duration}ms)`) : "";

      let icon = "  ";
      let opColor = dim;
      let statusIcon = "";

      switch (op.operation) {
        case "applied":
          icon = "✅";
          opColor = green;
          statusIcon = op.status === "failure" ? red("✗") : "";
          break;
        case "reverted":
          icon = "🔄";
          opColor = blue;
          statusIcon = op.status === "failure" ? red("✗") : "";
          break;
        case "failed":
          icon = "❌";
          opColor = red;
          break;
      }

      // Format operation line
      const migrationName = !options.migrationId
        ? gray(` → ${op.migrationName}`)
        : "";
      console.log(
        `${icon} ${dim(dateStr)}  ${
          opColor(op.operation.padEnd(10))
        } ${durationStr} ${statusIcon}${migrationName}`,
      );

      // Show error if present
      if (op.error) {
        const errorLines = op.error.split("\n");
        console.log(`   ${red("Error:")} ${red(errorLines[0])}`);
        if (errorLines.length > 1) {
          for (let i = 1; i < Math.min(3, errorLines.length); i++) {
            console.log(`          ${dim(errorLines[i])}`);
          }
        }
      }
    }

    console.log();

    // Summary
    const appliedCount = operations.filter((op) =>
      op.operation === "applied" && op.status === "success"
    ).length;
    const revertedCount = operations.filter((op) =>
      op.operation === "reverted" && op.status === "success"
    ).length;
    const failedCount = operations.filter((op) =>
      op.status === "failure"
    ).length;

    console.log(bold("Summary:"));
    console.log(dim(`  Total operations: ${operations.length}`));
    if (appliedCount > 0) {
      console.log(green(`  Applied: ${appliedCount}`));
    }
    if (revertedCount > 0) {
      console.log(blue(`  Reverted: ${revertedCount}`));
    }
    if (failedCount > 0) {
      console.log(red(`  Failed: ${failedCount}`));
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

/**
 * Baseline command for MongoDBee Migration CLI
 *
 * Adopts an existing database into the migration chain by recording migrations
 * as applied without executing them.
 *
 * @module
 */

import process from "node:process";
import { blue, bold, dim, green, red, yellow } from "@std/fmt/colors";
import { MongoClient } from "../../../mongodb.ts";
import * as path from "@std/path";

import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import { getAppliedMigrationIds, markMigrationAsAdopted } from "../../state.ts";
import { confirm } from "../utils/confirm.ts";
import { resolveMigrationRef } from "../utils/resolve-ref.ts";

export interface BaselineCommandOptions {
  configPath?: string;
  cwd?: string;
  force?: boolean;
  /** Migration the database is already at, inclusive. */
  target?: string;
}

/**
 * Declares every migration up to `target` already applied, without running one.
 *
 * A database built before the chain existed, restored from a dump, or seeded by
 * hand has the schema of some migration but an empty ledger. `migrate` would
 * then replay the chain from its very first migration against a populated
 * database, which fails on the first operation that assumes it is creating
 * something. Recording the prefix is what makes the ledger describe the
 * database, so that `migrate` afterwards runs only what is genuinely left.
 *
 * Nothing is read from or written to the data itself: the operator asserts
 * where the database stands, and a wrong assertion is only caught later, by the
 * first migration that finds the schema it did not expect.
 */
export async function baselineCommand(
  options: BaselineCommandOptions = {},
): Promise<void> {
  console.log(bold(blue("🐝 Baselining database...")));
  console.log();

  let client: MongoClient | undefined;

  try {
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

    const migrationsWithFiles = await loadAllMigrations(migrationsDir);
    if (migrationsWithFiles.length === 0) {
      console.log(yellow("No migrations found."));
      return;
    }
    const allMigrations = buildMigrationChain(migrationsWithFiles);

    // Defaults to the whole chain: the common case is a database already at the
    // head, brought under migration control for the first time.
    const target = options.target
      ? resolveMigrationRef(allMigrations, options.target)
      : allMigrations[allMigrations.length - 1];

    const targetIndex = allMigrations.findIndex((m) => m.id === target.id);
    const prefix = allMigrations.slice(0, targetIndex + 1);

    client = new MongoClient(connectionUri);
    await client.connect();
    const db = client.db(dbName);

    const applied = new Set(await getAppliedMigrationIds(db));

    // A migration recorded AFTER the target contradicts the assertion being
    // made, so there is no safe way to guess which of the two is right.
    const beyond = allMigrations
      .slice(targetIndex + 1)
      .filter((m) => applied.has(m.id));
    if (beyond.length > 0) {
      throw new Error(
        `The ledger already records ${beyond.length} migration(s) AFTER ${target.id}:\n` +
          `${beyond.map((m) => `  ${m.id} (${m.name})`).join("\n")}\n` +
          `Baselining at ${target.id} would claim the database is behind where its history says it is.`,
      );
    }

    const toRecord = prefix.filter((m) => !applied.has(m.id));

    if (toRecord.length === 0) {
      console.log(
        green(`✓ Nothing to do, the ledger already covers ${target.id}`),
      );
      return;
    }

    console.log(bold(`Baseline at: ${blue(target.name)}`));
    console.log(dim(`  ID: ${target.id}`));
    console.log();
    console.log(
      bold(`${toRecord.length} migration(s) will be recorded as applied:`),
    );
    for (const m of toRecord) {
      console.log(`  ${dim(m.id)}  ${m.name}`);
    }
    console.log();
    console.log(
      yellow(
        "⚠ No operation is executed and no data is transformed. This only asserts",
      ),
    );
    console.log(
      yellow(
        "  that the database ALREADY has the schema these migrations produce.",
      ),
    );
    console.log();

    if (!options.force) {
      const confirmed = await confirm(
        `Record these ${toRecord.length} migration(s) as applied on "${dbName}"?`,
      );
      if (!confirmed) {
        console.log(yellow("Baseline cancelled."));
        return;
      }
      console.log();
    }

    for (const m of toRecord) {
      await markMigrationAsAdopted(db, m.id, m.name);
      console.log(green(`  ✓ ${m.id}`));
    }

    console.log();
    console.log(bold(green(`✓ Database baselined at ${target.id}`)));
    console.log(dim("  Run `mongodbee status` to see what is left to apply."));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red(bold("✗ Baseline failed:")), message);
    throw error;
  } finally {
    if (client) {
      await client.close();
    }
  }
}

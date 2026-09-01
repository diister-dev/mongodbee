import process from "node:process";
import { blue, bold, dim, green, red, yellow } from "@std/fmt/colors";
import * as path from "@std/path";

import { MongoClient } from "../../../mongodb.ts";
import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import { markMigrationAsAdopted } from "../../state.ts";
import { pathToFileUrl } from "../../utils/platform.ts";
import { resolveMigrationRef } from "../utils/resolve-ref.ts";
import {
  countDocuments,
  renderScenarioReport,
  runScenario,
  type SeedScenario,
  writeStateToDatabase,
} from "../../../scenario/mod.ts";

export interface SeedCommandOptions {
  configPath?: string;
  cwd?: string;
  scenario?: string;
  at?: string;
  uri?: string;
  db?: string;
  force?: boolean;
  dryRun?: boolean;
  "dry-run"?: boolean;
  json?: boolean;
}

export async function loadScenarioModule(
  cwd: string,
  file: string,
): Promise<SeedScenario> {
  const module = await import(pathToFileUrl(path.resolve(cwd, file)));
  const scenario = (module.scenario ?? module.default) as
    | SeedScenario
    | undefined;
  if (
    !scenario || typeof scenario.name !== "string" ||
    typeof scenario.birth !== "string"
  ) {
    throw new Error(
      `Scenario file ${file} must export a "scenario" (or default) object with "name" and "birth"`,
    );
  }
  return scenario;
}

export async function seedCommand(
  options: SeedCommandOptions = {},
): Promise<void> {
  const dryRun = options.dryRun || options["dry-run"] || false;
  if (!options.scenario) {
    throw new Error("seed requires --scenario <file>");
  }
  const cwd = options.cwd || process.cwd();
  const config = await loadConfig({ configPath: options.configPath, cwd });
  const migrationsDir = path.resolve(
    cwd,
    config.paths?.migrations || "./migrations",
  );
  const chain = buildMigrationChain(await loadAllMigrations(migrationsDir));
  if (chain.length === 0) {
    throw new Error("No migrations found; a scenario needs a birth migration");
  }
  const scenario = await loadScenarioModule(cwd, options.scenario);
  const at = options.at
    ? resolveMigrationRef(chain, options.at).id
    : chain[chain.length - 1].id;

  if (!options.json) {
    console.log(bold(blue("🐝 Seeding scenario...")));
    console.log(dim(`Scenario: ${scenario.name} (born at ${scenario.birth})`));
    console.log(dim(`Target step: ${at}`));
    console.log();
  }

  const run = await runScenario({ migrations: chain, scenario, at });
  if (options.json) {
    console.log(JSON.stringify(run.report, null, 2));
  } else {
    console.log(renderScenarioReport(run.report));
    console.log();
  }
  if (!run.report.ok && !options.force) {
    throw new Error(
      "The generated world fails its oracle; fix the scenario or pass --force",
    );
  }
  if (dryRun) {
    if (!options.json) console.log(yellow("Dry run: nothing written"));
    return;
  }

  const uri = options.uri || config.database?.connection?.uri ||
    "mongodb://localhost:27017";
  const dbName = options.db || config.database?.name || "myapp";
  const client = new MongoClient(uri);
  await client.connect();
  try {
    const db = client.db(dbName);
    const existing = await countDocuments(db);
    if (existing > 0 && !options.force) {
      throw new Error(
        `Database "${dbName}" already holds ${existing} document(s); seed only writes into an empty database (or pass --force)`,
      );
    }
    const written = await writeStateToDatabase(db, run.state);
    const atIndex = chain.findIndex((m) => m.id === at);
    for (const migration of chain.slice(0, atIndex + 1)) {
      await markMigrationAsAdopted(db, migration.id, migration.name);
    }
    if (!options.json) {
      const total = Object.values(written).reduce((a, b) => a + b, 0);
      console.log(
        green(
          `✓ ${total} document(s) written to "${dbName}", ledger baselined at ${at}`,
        ),
      );
    }
  } catch (error) {
    if (!options.json) console.log(red("✗ Seed failed"));
    throw error;
  } finally {
    await client.close();
  }
}

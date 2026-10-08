import process from "node:process";
import { blue, bold, dim, green, red, yellow } from "../../../utils/colors.ts";
import * as path from "node:path";

import { type Db, MongoClient } from "../../../mongodb.ts";
import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import { markMigrationAsAdopted } from "../../state.ts";
import { MIGRATION_OPERATIONS_COLLECTION } from "../../history.ts";
import { VALIDATOR_GUARD_COLLECTION } from "../../validator-guard.ts";
import type { DatabaseState, MigrationDefinition } from "../../types.ts";
import { migrateCommand } from "./migrate.ts";
import { pathToFileUrl } from "../../utils/platform.ts";
import { resolveMigrationRef } from "../utils/resolve-ref.ts";
import {
  checkScenarioWorld,
  generateScenarioAtBirth,
  populateDatabase,
  readStateFromDatabase,
  recomputeComputedFields,
  renderScenarioReport,
  runScenario,
  type ScenarioBirth,
  type ScenarioReport,
  type SeedScenario,
} from "../../../scenario/mod.ts";

export type SeedReplay = "memory" | "mongo";

export interface SeedCommandOptions {
  configPath?: string;
  cwd?: string;
  scenario?: string;
  at?: string;
  uri?: string;
  db?: string;
  /** Replaced by `allowViolations` and `allowNonEmpty`; passing it is refused. */
  force?: boolean;
  allowViolations?: boolean;
  "allow-violations"?: boolean;
  allowNonEmpty?: boolean;
  "allow-non-empty"?: boolean;
  replay?: string;
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
    !scenario ||
    typeof scenario.name !== "string" ||
    typeof scenario.birth !== "string"
  ) {
    throw new Error(
      `Scenario file ${file} must export a "scenario" (or default) object with "name" and "birth"`,
    );
  }
  return scenario;
}

function parseReplay(value: string | undefined): SeedReplay {
  if (value === undefined || value === "memory") return "memory";
  if (value === "mongo") return "mongo";
  throw new Error(`--replay must be "memory" or "mongo", got "${value}"`);
}

const LEDGER_COLLECTIONS: readonly string[] = [
  MIGRATION_OPERATIONS_COLLECTION,
  VALIDATOR_GUARD_COLLECTION,
];

function managedNames(
  migrations: readonly MigrationDefinition[],
  state: DatabaseState,
): { names: Set<string>; modelPrefixes: Set<string> } {
  const names = new Set<string>();
  const modelPrefixes = new Set<string>();
  for (const { schemas } of migrations) {
    for (const name of Object.keys(schemas.collections ?? {})) names.add(name);
    for (const name of Object.keys(schemas.multiCollections ?? {})) {
      names.add(name);
    }
    for (const name of Object.keys(schemas.scopedMultiCollections ?? {})) {
      names.add(name);
    }
    for (const model of Object.keys(schemas.multiModels ?? {})) {
      modelPrefixes.add(`${model}:`);
    }
  }
  for (const bucket of [
    "collections",
    "multiCollections",
    "scopedMultiCollections",
    "multiModels",
  ] as const) {
    for (const name of Object.keys(state[bucket])) names.add(name);
  }
  return { names, modelPrefixes };
}

async function existingCollections(db: Db): Promise<Set<string>> {
  const infos = await db.listCollections({}, { nameOnly: true }).toArray();
  return new Set(
    infos.map((info) => info.name).filter((n) => !n.startsWith("system.")),
  );
}

async function assertSeedable(
  db: Db,
  dbName: string,
  existing: ReadonlySet<string>,
  touched: readonly MigrationDefinition[],
  state: DatabaseState,
  allowNonEmpty: boolean,
): Promise<void> {
  if (existing.size === 0) return;
  if (!allowNonEmpty) {
    throw new Error(
      `Database "${dbName}" already holds ${existing.size} collection(s); seed writes into an empty database, or next to unrelated collections with --allow-non-empty`,
    );
  }
  const ledger = LEDGER_COLLECTIONS.filter((name) => existing.has(name));
  for (const name of ledger) {
    if ((await db.collection(name).countDocuments({}, { limit: 1 })) > 0) {
      throw new Error(
        `Database "${dbName}" already has a migration ledger (${name}); seed never appends to an existing ledger`,
      );
    }
  }
  const { names, modelPrefixes } = managedNames(touched, state);
  const clashing = [...existing].filter(
    (name) =>
      names.has(name) ||
      [...modelPrefixes].some((prefix) => name.startsWith(prefix)),
  );
  if (clashing.length > 0) {
    throw new Error(
      `Database "${dbName}" already holds the collection(s) ${clashing
        .sort()
        .join(
          ", ",
        )} that the schemas manage; seed never writes into nor drops them`,
    );
  }
}

async function dropCreatedCollections(
  db: Db,
  before: ReadonlySet<string>,
): Promise<string[]> {
  const leftovers: string[] = [];
  for (const name of await existingCollections(db)) {
    if (before.has(name)) continue;
    try {
      await db.collection(name).drop();
    } catch {
      leftovers.push(name);
    }
  }
  return leftovers;
}

async function withStdoutOnStderr<T>(
  enabled: boolean,
  fn: () => Promise<T>,
): Promise<T> {
  if (!enabled) return await fn();
  const log = console.log;
  console.log = console.error;
  try {
    return await fn();
  } finally {
    console.log = log;
  }
}

export async function seedCommand(
  options: SeedCommandOptions = {},
): Promise<void> {
  if (options.force === true) {
    throw new Error(
      "seed no longer takes --force: pass --allow-violations to write a world its oracle rejects, --allow-non-empty to write next to other collections",
    );
  }
  const dryRun = options.dryRun || options["dry-run"] || false;
  const allowViolations =
    options.allowViolations || options["allow-violations"] || false;
  const allowNonEmpty =
    options.allowNonEmpty || options["allow-non-empty"] || false;
  const replay = parseReplay(options.replay);
  if (!options.scenario) {
    throw new Error("seed requires --scenario <file>");
  }
  if (replay === "mongo" && dryRun) {
    throw new Error(
      "--replay mongo runs the real migrations on the database; it has no dry run (use --replay memory --dry-run)",
    );
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
    console.log(dim(`Target step: ${at} (replay: ${replay})`));
    console.log();
  }

  const printReport = (report: ScenarioReport) => {
    if (options.json) {
      console.log(JSON.stringify(report, null, 2));
    } else {
      console.log(renderScenarioReport(report));
      console.log();
    }
  };
  const refuseViolations = (report: ScenarioReport) => {
    if (!report.ok && !allowViolations) {
      throw new Error(
        "The generated world fails its oracle; fix the scenario or pass --allow-violations",
      );
    }
  };

  let write: { state: DatabaseState; at: MigrationDefinition };
  let birth: ScenarioBirth | undefined;
  if (replay === "memory") {
    const run = await runScenario({ migrations: chain, scenario, at });
    printReport(run.report);
    refuseViolations(run.report);
    if (dryRun) {
      if (!options.json) console.log(yellow("Dry run: nothing written"));
      return;
    }
    write = {
      state: run.state,
      at: chain[chain.findIndex((m) => m.id === at)],
    };
  } else {
    birth = await generateScenarioAtBirth({ migrations: chain, scenario, at });
    recomputeComputedFields(birth.state, birth.birth.schemas);
    write = { state: birth.state, at: birth.birth };
  }

  const uri =
    options.uri ||
    config.database?.connection?.uri ||
    "mongodb://localhost:27017";
  const dbName = options.db || config.database?.name || "myapp";
  const writeIndex = chain.findIndex((m) => m.id === write.at.id);
  const touched = chain.slice(
    writeIndex,
    chain.findIndex((m) => m.id === at) + 1,
  );
  const client = new MongoClient(uri);
  await client.connect();
  try {
    const db = client.db(dbName);
    const before = await existingCollections(db);
    await assertSeedable(
      db,
      dbName,
      before,
      touched,
      write.state,
      allowNonEmpty,
    );
    try {
      const written = await populateDatabase(db, write.state, {
        migration: write.at,
      });
      for (const migration of chain.slice(0, writeIndex + 1)) {
        await markMigrationAsAdopted(db, migration.id, migration.name);
      }
      let total = Object.values(written).reduce((a, b) => a + b, 0);
      if (birth) {
        if (birth.pending.length > 0) {
          await withStdoutOnStderr(options.json === true, () =>
            migrateCommand({
              cwd,
              configPath: options.configPath,
              force: true,
              target: at,
              connectionUri: uri,
              databaseName: dbName,
            }),
          );
        }
        const state = await readStateFromDatabase(db, birth.at.schemas);
        const report = checkScenarioWorld({
          scenario,
          birth: birth.birth,
          at: birth.at,
          applied: birth.pending.map((m) => m.id),
          state,
          generationViolations: birth.violations,
        });
        printReport(report);
        refuseViolations(report);
        total = Object.values(report.generated).reduce((a, b) => a + b, 0);
      }
      if (!options.json) {
        console.log(
          green(
            `✓ ${total} document(s) written to "${dbName}", ledger at ${at}`,
          ),
        );
      }
    } catch (error) {
      const leftovers = await dropCreatedCollections(db, before);
      if (leftovers.length > 0) {
        throw new Error(
          `Seed failed and the collection(s) ${leftovers.join(", ")} it created could not be removed`,
          { cause: error },
        );
      }
      throw error;
    }
  } catch (error) {
    if (!options.json) console.log(red("✗ Seed failed"));
    throw error;
  } finally {
    await client.close();
  }
}

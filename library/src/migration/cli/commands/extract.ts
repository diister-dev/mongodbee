import process from "node:process";
import { blue, bold, dim, green, red, yellow } from "@std/fmt/colors";
import * as path from "@std/path";

import { MongoClient } from "../../../mongodb.ts";
import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import { loadProjectSchema } from "../../schema-validation.ts";
import { markMigrationAsAdopted } from "../../state.ts";
import { resolveMigrationRef } from "../utils/resolve-ref.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  type PrivacyConsistency,
  type PrivacyPlan,
  type TransformNoteKind,
} from "../../../privacy/mod.ts";
import {
  applyMigrationsInMemory,
  countDocuments,
  docsOf,
  readStateFromDatabase,
  writeStateToDatabase,
} from "../../../scenario/mod.ts";
import {
  createEmptyDatabaseState,
  type DatabaseState,
  type MigrationDefinition,
  type SchemasDefinition,
} from "../../types.ts";

export interface ExtractCommandOptions {
  configPath?: string;
  cwd?: string;
  from?: string;
  "from-db"?: string;
  fromDb?: string;
  to?: string;
  "to-db"?: string;
  toDb?: string;
  secret?: string;
  consistency?: string;
  "shift-days"?: string | number;
  shiftDays?: number;
  scope?: string;
  "from-migration"?: string;
  fromMigration?: string;
  "allow-unknown"?: boolean;
  allowUnknown?: boolean;
  force?: boolean;
  dryRun?: boolean;
  "dry-run"?: boolean;
  json?: boolean;
}

export interface ExtractSummary {
  readonly targets: Record<
    string,
    {
      readonly documents: number;
      readonly notes: Partial<Record<TransformNoteKind, number>>;
    }
  >;
  readonly secretDiscarded: boolean;
  readonly applied: readonly string[];
}

function resolveSecret(
  raw: string | undefined,
): { secret: string; discarded: boolean } {
  if (raw === undefined || raw === "") {
    const bytes = crypto.getRandomValues(new Uint8Array(32));
    return {
      secret: Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join(
        "",
      ),
      discarded: true,
    };
  }
  if (raw.startsWith("env:")) {
    const value = process.env[raw.slice(4)];
    if (!value) {
      throw new Error(`Environment variable ${raw.slice(4)} is empty`);
    }
    return { secret: value, discarded: false };
  }
  return { secret: raw, discarded: false };
}

function parseConsistency(raw: string | undefined): PrivacyConsistency {
  if (raw === undefined) return "relationship";
  if (raw === "person" || raw === "relationship" || raw === "transaction") {
    return raw;
  }
  throw new Error(
    `Unknown consistency "${raw}" (expected person, relationship or transaction)`,
  );
}

export function transformState(
  state: DatabaseState,
  plan: PrivacyPlan,
  transformer: ReturnType<typeof createPrivacyTransformer>,
): { state: DatabaseState; summary: ExtractSummary["targets"] } {
  const out = createEmptyDatabaseState();
  const summary: ExtractSummary["targets"] = {};
  for (const target of plan.targets.values()) {
    const docs = docsOf(state, target);
    if (docs.length === 0) continue;
    const notes: Partial<Record<TransformNoteKind, number>> = {};
    const transformed: Record<string, unknown>[] = [];
    for (const doc of docs) {
      const result = transformer.transform(target.key, doc);
      for (const note of result.notes) {
        notes[note.kind] = (notes[note.kind] ?? 0) + 1;
      }
      transformed.push(result.doc);
    }
    summary[target.key] = { documents: transformed.length, notes };
    switch (target.bucket) {
      case "collections":
        out.collections[target.collection] = { content: transformed };
        break;
      case "multiCollections":
        (out.multiCollections[target.collection] ??= { content: [] }).content
          .push(...transformed);
        break;
      case "scopedMultiCollections":
        (out.scopedMultiCollections[target.collection] ??= { content: [] })
          .content.push(...transformed);
        break;
      case "multiModels":
        break;
    }
  }
  for (const [name, instance] of Object.entries(state.multiModels)) {
    out.multiModels[name] = { modelType: instance.modelType, content: [] };
  }
  return { state: out, summary };
}

export async function extractCommand(
  options: ExtractCommandOptions = {},
): Promise<void> {
  const dryRun = options.dryRun || options["dry-run"] || false;
  const allowUnknown = options.allowUnknown || options["allow-unknown"] ||
    false;
  const fromDb = options.fromDb || options["from-db"];
  const toDb = options.toDb || options["to-db"];
  const fromMigration = options.fromMigration || options["from-migration"];
  const shiftDaysRaw = options.shiftDays ?? options["shift-days"];
  const shiftDays = shiftDaysRaw === undefined ? 0 : Number(shiftDaysRaw);
  if (Number.isNaN(shiftDays)) throw new Error("--shift-days must be a number");

  const cwd = options.cwd || process.cwd();
  const config = await loadConfig({ configPath: options.configPath, cwd });
  const fromUri = options.from || config.database?.connection?.uri ||
    "mongodb://localhost:27017";
  const sourceDb = fromDb || config.database?.name;
  if (!sourceDb) {
    throw new Error(
      "extract requires --from-db (or a configured database name)",
    );
  }
  if (!dryRun && !toDb) {
    throw new Error("extract requires --to-db (or --dry-run)");
  }
  const toUri = options.to || fromUri;
  if (!dryRun && toUri === fromUri && toDb === sourceDb) {
    throw new Error(
      "extract refuses to write into its own source; pick another --to-db or --to",
    );
  }

  const migrationsDir = path.resolve(
    cwd,
    config.paths?.migrations || "./migrations",
  );
  const chain = buildMigrationChain(await loadAllMigrations(migrationsDir));
  let schemas: SchemasDefinition;
  let replay: readonly MigrationDefinition[] = [];
  let head: MigrationDefinition | undefined;
  if (chain.length > 0) {
    head = chain[chain.length - 1];
    schemas = head.schemas;
    if (fromMigration) {
      const from = resolveMigrationRef(chain, fromMigration);
      const fromIndex = chain.findIndex((m) => m.id === from.id);
      replay = chain.slice(fromIndex + 1);
    }
  } else {
    schemas = await loadProjectSchema(
      path.resolve(cwd, config.paths?.schemas || "./schemas.ts"),
    );
  }

  const plan = buildPrivacyPlan({ schemas });
  const errors = plan.findings.filter((f) => f.level === "error");
  if (errors.length > 0) {
    throw new Error(
      `Privacy classification has ${errors.length} error(s); run classify first`,
    );
  }
  if (plan.summary.unknown > 0 && !allowUnknown) {
    throw new Error(
      `${plan.summary.unknown} path(s) are UNKNOWN; declare them or pass --allow-unknown (they will be dropped)`,
    );
  }

  const { secret, discarded } = resolveSecret(options.secret);
  const consistency = parseConsistency(options.consistency);
  if (!options.json) {
    console.log(bold(blue("🐝 Extracting pseudonymised data...")));
    console.log(
      dim(`Source: ${sourceDb}${fromMigration ? ` at ${fromMigration}` : ""}`),
    );
    console.log(
      dim(
        `Consistency: ${consistency}${
          shiftDays ? `, time shift ${shiftDays} day(s)` : ""
        }`,
      ),
    );
    if (discarded) {
      console.log(dim("Secret: generated for this run and discarded"));
    }
    console.log();
  }

  const sourceClient = new MongoClient(fromUri);
  await sourceClient.connect();
  let state: DatabaseState;
  try {
    const sourceSchemas = replay.length > 0
      ? resolveMigrationRef(chain, fromMigration!).schemas
      : schemas;
    state = await readStateFromDatabase(
      sourceClient.db(sourceDb),
      sourceSchemas,
      {
        ...(options.scope !== undefined && { scope: options.scope }),
      },
    );
  } finally {
    await sourceClient.close();
  }
  const replayed = await applyMigrationsInMemory(state, replay);

  const transformer = createPrivacyTransformer({
    plan,
    schemas,
    secret,
    consistency,
    timeShiftMs: shiftDays * 86_400_000,
  });
  const result = transformState(replayed.state, plan, transformer);
  const summary: ExtractSummary = {
    targets: result.summary,
    secretDiscarded: discarded,
    applied: replayed.applied,
  };

  if (options.json) {
    console.log(JSON.stringify(summary, null, 2));
  } else {
    for (const [target, entry] of Object.entries(result.summary)) {
      const notes = Object.entries(entry.notes).map(([k, n]) => `${k} ${n}`)
        .join(", ");
      console.log(
        `  ${target.padEnd(60)} ${String(entry.documents).padStart(7)}${
          notes ? dim(`   ${notes}`) : ""
        }`,
      );
    }
    console.log();
  }
  if (dryRun) {
    if (!options.json) console.log(yellow("Dry run: nothing written"));
    return;
  }

  const targetClient = new MongoClient(toUri);
  await targetClient.connect();
  try {
    const db = targetClient.db(toDb!);
    const existing = await countDocuments(db);
    if (existing > 0 && !options.force) {
      throw new Error(
        `Database "${toDb}" already holds ${existing} document(s); extract only writes into an empty database (or pass --force)`,
      );
    }
    const written = await writeStateToDatabase(db, result.state);
    if (head) {
      for (const migration of chain) {
        await markMigrationAsAdopted(db, migration.id, migration.name);
      }
    }
    if (!options.json) {
      const total = Object.values(written).reduce((a, b) => a + b, 0);
      console.log(
        green(
          `✓ ${total} document(s) written to "${toDb}"${
            head ? `, ledger baselined at ${head.id}` : ""
          }`,
        ),
      );
    }
  } catch (error) {
    if (!options.json) console.log(red("✗ Extract failed"));
    throw error;
  } finally {
    await targetClient.close();
  }
}

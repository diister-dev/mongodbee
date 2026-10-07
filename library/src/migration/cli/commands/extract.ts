import process from "node:process";
import { blue, bold, dim, green, red, yellow } from "../../../utils/colors.ts";
import * as path from "node:path";

import { MongoClient } from "../../../mongodb.ts";
import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import { loadProjectSchema } from "../../schema-validation.ts";
import { resolveMigrationRef } from "../utils/resolve-ref.ts";
import { migrationDefinition } from "../../definition.ts";
import { getAppliedMigrationIds, markMigrationAsAdopted } from "../../state.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  type PrivacyConsistency,
  type PrivacyPlan,
  remapId,
  type TransformNoteKind,
} from "../../../privacy/mod.ts";
import {
  applyMigrationsInMemory,
  checkScenarioState,
  countDocuments,
  docsOf,
  populateDatabase,
  readStateFromDatabase,
  type ScenarioViolation,
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
  readonly skipped: Record<string, Record<string, number>>;
  readonly violations: readonly ScenarioViolation[];
  readonly secretDiscarded: boolean;
  readonly applied: readonly string[];
}

export interface TransformStateResult {
  readonly state: DatabaseState;
  readonly summary: ExtractSummary["targets"];
  readonly skipped: ExtractSummary["skipped"];
}

const REPORTED_VIOLATIONS: ReadonlySet<ScenarioViolation["kind"]> = new Set([
  "dangling_reference",
  "owner_unresolved",
  "duplicate_id",
]);

function resolveSecret(raw: string | undefined): {
  secret: string;
  discarded: boolean;
} {
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

function isMetadataDocument(doc: Record<string, unknown>): boolean {
  return typeof doc._type === "string" && doc._type.startsWith("_");
}

async function serverIdentity(client: MongoClient): Promise<string> {
  const hello = await client.db("admin").command({ hello: 1 });
  return String(hello.primary ?? hello.me);
}

async function assertTargetIsNotSource(
  sourceClient: MongoClient,
  targetClient: MongoClient,
  sourceDb: string,
  toDb: string,
): Promise<void> {
  if (toDb !== sourceDb) return;
  if (
    (await serverIdentity(sourceClient)) !==
    (await serverIdentity(targetClient))
  ) {
    return;
  }
  throw new Error(
    "extract refuses to write into its own source; pick another --to-db or --to",
  );
}

export function transformState(
  state: DatabaseState,
  plan: PrivacyPlan,
  transformer: ReturnType<typeof createPrivacyTransformer>,
  remapInstanceName: (name: string) => string = (name) => name,
): TransformStateResult {
  const out = createEmptyDatabaseState();
  const summary: ExtractSummary["targets"] = {};
  const skipped: Record<string, Record<string, number>> = {};
  const declaredTypes = new Map<string, Set<string>>();
  for (const target of plan.targets.values()) {
    const key = `${target.bucket}/${target.collection}`;
    const types = declaredTypes.get(key) ?? new Set<string>();
    types.add(target.type ?? "");
    declaredTypes.set(key, types);
  }

  const countSkipped = (
    bucket: string,
    collection: string,
    docs: readonly Record<string, unknown>[],
  ) => {
    const key = `${bucket}/${collection}`;
    const declared = declaredTypes.get(key);
    for (const doc of docs) {
      if (isMetadataDocument(doc)) continue;
      const type = String(doc._type);
      if (declared?.has(type)) continue;
      const counts = (skipped[key] ??= {});
      counts[type] = (counts[type] ?? 0) + 1;
    }
  };

  const record = (
    key: string,
    notes: Partial<Record<TransformNoteKind, number>>,
    documents: number,
  ) => {
    const entry = (summary[key] ??= { documents: 0, notes: {} });
    const merged = { ...entry.notes };
    for (const [kind, count] of Object.entries(notes)) {
      const noteKind = kind as TransformNoteKind;
      merged[noteKind] = (merged[noteKind] ?? 0) + count;
    }
    summary[key] = { documents: entry.documents + documents, notes: merged };
  };

  const transformDocs = (
    targetKey: string,
    docs: readonly Record<string, unknown>[],
    scope?: string,
  ): Record<string, unknown>[] => {
    const notes: Partial<Record<TransformNoteKind, number>> = {};
    const transformed: Record<string, unknown>[] = [];
    for (const doc of docs) {
      const result = transformer.transform(
        targetKey,
        doc,
        scope === undefined ? undefined : { scope },
      );
      for (const note of result.notes) {
        notes[note.kind] = (notes[note.kind] ?? 0) + 1;
      }
      transformed.push(result.doc);
    }
    if (docs.length > 0) record(targetKey, notes, docs.length);
    return transformed;
  };

  for (const target of plan.targets.values()) {
    if (target.bucket === "multiModels") continue;
    const docs = docsOf(state, target);
    if (docs.length === 0) continue;
    const transformed = transformDocs(target.key, docs);
    switch (target.bucket) {
      case "collections":
        out.collections[target.collection] = { content: transformed };
        break;
      case "multiCollections":
        (out.multiCollections[target.collection] ??= {
          content: [],
        }).content.push(...transformed);
        break;
      case "scopedMultiCollections":
        (out.scopedMultiCollections[target.collection] ??= {
          content: [],
        }).content.push(...transformed);
        break;
    }
  }
  for (const [name, { content }] of Object.entries(state.multiCollections)) {
    countSkipped("multiCollections", name, content);
    const metadata = content.filter(isMetadataDocument);
    if (metadata.length > 0) {
      (out.multiCollections[name] ??= { content: [] }).content.push(
        ...structuredClone(metadata),
      );
    }
  }
  for (const [name, { content }] of Object.entries(
    state.scopedMultiCollections,
  )) {
    countSkipped("scopedMultiCollections", name, content);
  }

  for (const [name, instance] of Object.entries(state.multiModels)) {
    countSkipped("multiModels", instance.modelType, instance.content);
    const content: Record<string, unknown>[] = structuredClone(
      instance.content.filter(isMetadataDocument),
    );
    for (const target of plan.targets.values()) {
      if (
        target.bucket !== "multiModels" ||
        target.collection !== instance.modelType
      ) {
        continue;
      }
      content.push(
        ...transformDocs(
          target.key,
          instance.content.filter((d) => d._type === target.type),
          name,
        ),
      );
    }
    out.multiModels[remapInstanceName(name)] = {
      modelType: instance.modelType,
      content,
    };
  }
  return { state: out, summary, skipped };
}

export async function extractCommand(
  options: ExtractCommandOptions = {},
): Promise<void> {
  const dryRun = options.dryRun || options["dry-run"] || false;
  const allowUnknown =
    options.allowUnknown || options["allow-unknown"] || false;
  const fromDb = options.fromDb || options["from-db"];
  const toDb = options.toDb || options["to-db"];
  const fromMigration = options.fromMigration || options["from-migration"];
  const shiftDaysRaw = options.shiftDays ?? options["shift-days"];
  const shiftDays = shiftDaysRaw === undefined ? 0 : Number(shiftDaysRaw);
  if (Number.isNaN(shiftDays)) throw new Error("--shift-days must be a number");

  const cwd = options.cwd || process.cwd();
  const config = await loadConfig({ configPath: options.configPath, cwd });
  const fromUri =
    options.from ||
    config.database?.connection?.uri ||
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
  const targetClient = dryRun ? undefined : new MongoClient(toUri);
  try {
    await sourceClient.connect();
    if (targetClient) {
      await targetClient.connect();
      await assertTargetIsNotSource(
        sourceClient,
        targetClient,
        sourceDb,
        toDb!,
      );
      const existing = await countDocuments(targetClient.db(toDb!));
      if (existing > 0 && !options.force) {
        throw new Error(
          `Database "${toDb}" already holds ${existing} document(s); extract only writes into an empty database (or pass --force)`,
        );
      }
    }

    const sourceSchemas =
      replay.length > 0
        ? resolveMigrationRef(chain, fromMigration!).schemas
        : schemas;
    const state = await readStateFromDatabase(
      sourceClient.db(sourceDb),
      sourceSchemas,
      {
        ...(options.scope !== undefined && { scope: options.scope }),
      },
    );
    const replayed = await applyMigrationsInMemory(state, replay);

    const shiftMs = shiftDays * 86_400_000;
    const transformer = createPrivacyTransformer({
      plan,
      schemas,
      secret,
      consistency,
      timeShiftMs: shiftMs,
    });
    const result = transformState(replayed.state, plan, transformer, (name) =>
      remapId(secret, name, shiftMs),
    );
    const violations = checkScenarioState({
      state: result.state,
      schemas,
      plan,
    }).filter((violation) => REPORTED_VIOLATIONS.has(violation.kind));
    const summary: ExtractSummary = {
      targets: result.summary,
      skipped: result.skipped,
      violations,
      secretDiscarded: discarded,
      applied: replayed.applied,
    };

    if (options.json) {
      console.log(JSON.stringify(summary, null, 2));
    } else {
      for (const [target, entry] of Object.entries(result.summary)) {
        const notes = Object.entries(entry.notes)
          .map(([k, n]) => `${k} ${n}`)
          .join(", ");
        console.log(
          `  ${target.padEnd(60)} ${String(entry.documents).padStart(7)}${
            notes ? dim(`   ${notes}`) : ""
          }`,
        );
      }
      console.log();
      for (const [collection, types] of Object.entries(result.skipped)) {
        const detail = Object.entries(types)
          .map(([type, n]) => `${type} ${n}`)
          .join(", ");
        console.log(
          yellow(
            `  ! ${collection}: documents of undeclared _type not copied (${detail})`,
          ),
        );
      }
      for (const violation of violations) {
        console.log(yellow(`  ! ${violation.target}: ${violation.message}`));
      }
      console.log();
    }
    if (!targetClient) {
      if (!options.json) console.log(yellow("Dry run: nothing written"));
      return;
    }

    const db = targetClient.db(toDb!);
    const written = await populateDatabase(db, result.state, {
      migration:
        head ??
        migrationDefinition("extract", "extract", {
          parent: null,
          schemas,
          migrate: (b) => b.compile(),
        }),
    });
    if (head) {
      const adopted = new Set(await getAppliedMigrationIds(db));
      for (const migration of chain) {
        if (adopted.has(migration.id)) continue;
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
    await sourceClient.close();
    await targetClient?.close();
  }
}

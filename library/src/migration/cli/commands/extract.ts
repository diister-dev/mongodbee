import { existsSync } from "node:fs";
import process from "node:process";
import { blue, bold, dim, green, red, yellow } from "../../../utils/colors.ts";
import * as path from "node:path";

import { type Db, MongoClient } from "../../../mongodb.ts";
import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import { loadProjectSchema } from "../../schema-validation.ts";
import { resolveMigrationRef } from "../utils/resolve-ref.ts";
import { parsePosture } from "./classify.ts";
import { COMPUTED_REVISION, COMPUTED_ROOT } from "../../../computed-guard.ts";
import { MULTI_COLLECTION_INFO_TYPE } from "../../multicollection-registry.ts";
import { isRecord } from "../../../utils/guards.ts";
import { migrationDefinition } from "../../definition.ts";
import { getAppliedMigrationIds, markMigrationAsAdopted } from "../../state.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  describeValueJoin,
  detectValueJoins,
  type PossibleValueJoin,
  type PrivacyConsistency,
  type PrivacyPlan,
  type PrivacyPosture,
  type RecomputeContext,
  SKIP_RECOMPUTE,
  type TransformNoteKind,
} from "../../../privacy/mod.ts";
import {
  applyMigrationsInMemory,
  checkScenarioState,
  countCollections,
  recomputeComputedFields,
  docsOf,
  isMetadataDocument,
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
  posture?: string;
  "shift-days"?: string | number;
  shiftDays?: number;
  scope?: string;
  "from-migration"?: string;
  fromMigration?: string;
  "allow-unknown"?: boolean;
  allowUnknown?: boolean;
  force?: boolean;
  allowViolations?: boolean;
  "allow-violations"?: boolean;
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
  readonly posture: PrivacyPosture;
  readonly possibleValueJoins: readonly PossibleValueJoin[];
  readonly scope?: {
    readonly scope: string;
    readonly copiedWhole: readonly string[];
  };
  readonly skipped: Record<string, Record<string, number>>;
  readonly violations: readonly ScenarioViolation[];
  readonly violationsAllowed: boolean;
  readonly secretDiscarded: boolean;
  readonly applied: readonly string[];
}

export interface TransformStateOptions {
  readonly schemas: SchemasDefinition;
  readonly remapInstanceName: (name: string) => string;
}

export function keepComputedRevision({
  path,
  original,
}: RecomputeContext): unknown {
  if (path !== COMPUTED_ROOT) return SKIP_RECOMPUTE;
  const revision = isRecord(original) ? original[COMPUTED_REVISION] : undefined;
  return typeof revision === "number"
    ? { [COMPUTED_REVISION]: revision }
    : SKIP_RECOMPUTE;
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
  "invalid_document",
  "unique_index",
  "mirror_mismatch",
  "unique_unchecked",
]);

const BLOCKING_VIOLATIONS: ReadonlySet<ScenarioViolation["kind"]> = new Set([
  "invalid_document",
  "unique_index",
  "mirror_mismatch",
]);

function resolveSecret(raw: string | undefined): {
  secret: string;
  discarded: boolean;
  literal: boolean;
} {
  if (raw === undefined || raw === "") {
    const bytes = crypto.getRandomValues(new Uint8Array(32));
    return {
      secret: Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join(
        "",
      ),
      discarded: true,
      literal: false,
    };
  }
  if (raw.startsWith("env:")) {
    const value = process.env[raw.slice(4)];
    if (!value) {
      throw new Error(`Environment variable ${raw.slice(4)} is empty`);
    }
    return { secret: value, discarded: false, literal: false };
  }
  return { secret: raw, discarded: false, literal: true };
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

function shiftedDate(value: unknown, shiftMs: number): unknown {
  return value instanceof Date ? new Date(value.getTime() + shiftMs) : value;
}

function sanitiseMetadata(
  doc: Record<string, unknown>,
  shiftMs: number,
): Record<string, unknown> {
  if (doc._type === MULTI_COLLECTION_INFO_TYPE) {
    return {
      _id: doc._id,
      _type: doc._type,
      collectionType: doc.collectionType,
      createdAt: shiftedDate(doc.createdAt, shiftMs),
    };
  }
  const operations = Array.isArray(doc.appliedMigrations)
    ? doc.appliedMigrations.filter(isRecord)
    : [];
  return {
    _id: doc._id,
    _type: doc._type,
    fromMigrationId: doc.fromMigrationId,
    mongodbeeVersion: doc.mongodbeeVersion,
    appliedMigrations: operations.map(({ error: _error, ...operation }) => ({
      ...operation,
      appliedAt: shiftedDate(operation.appliedAt, shiftMs),
    })),
  };
}

async function replayWithoutValues(
  state: DatabaseState,
  replay: readonly MigrationDefinition[],
): Promise<{ state: DatabaseState; applied: string[] }> {
  let current = state;
  const applied: string[] = [];
  for (const migration of replay) {
    try {
      const step = await applyMigrationsInMemory(current, [migration]);
      current = step.state;
      applied.push(...step.applied);
    } catch {
      throw new Error(
        `Replaying migration ${migration.id} in memory failed; the original message is withheld because it can quote source values. Run the migration on a copy of the source to see it.`,
      );
    }
  }
  return { state: current, applied };
}

async function resolveSourceStep(
  chain: readonly MigrationDefinition[],
  explicit: string | undefined,
  sourceDb: Db,
): Promise<MigrationDefinition | undefined> {
  if (chain.length === 0) return undefined;
  if (explicit) return resolveMigrationRef(chain, explicit);
  const applied = await getAppliedMigrationIds(sourceDb);
  const known = new Set(chain.map((m) => m.id));
  const foreign = applied.filter((id) => !known.has(id));
  if (foreign.length > 0) {
    throw new Error(
      `The source ledger holds ${foreign.length} migration(s) the local chain does not know (${foreign.join(", ")}); extract needs the migrations the source was built with`,
    );
  }
  const index = Math.max(
    ...applied.map((id) => chain.findIndex((m) => m.id === id)),
  );
  return index >= 0 && index < chain.length - 1 ? chain[index] : undefined;
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
  options: TransformStateOptions,
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
        ...metadata.map((doc) =>
          sanitiseMetadata(doc, transformer.timeShiftMs),
        ),
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
    const content: Record<string, unknown>[] = instance.content
      .filter(isMetadataDocument)
      .map((doc) => sanitiseMetadata(doc, transformer.timeShiftMs));
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
    out.multiModels[options.remapInstanceName(name)] = {
      modelType: instance.modelType,
      content,
    };
  }
  recomputeComputedFields(out, options.schemas);
  return { state: out, summary, skipped };
}

export async function extractCommand(
  options: ExtractCommandOptions = {},
): Promise<void> {
  const dryRun = options.dryRun || options["dry-run"] || false;
  const allowViolations =
    options.allowViolations || options["allow-violations"] || false;
  const allowUnknown =
    options.allowUnknown || options["allow-unknown"] || false;
  const fromDb = options.fromDb || options["from-db"];
  const toDb = options.toDb || options["to-db"];
  const fromMigration = options.fromMigration || options["from-migration"];
  const shiftDaysRaw = options.shiftDays ?? options["shift-days"];
  const shiftDays =
    shiftDaysRaw === undefined ? undefined : Number(shiftDaysRaw);
  if (shiftDays !== undefined && Number.isNaN(shiftDays)) {
    throw new Error("--shift-days must be a number");
  }

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
  const chain = buildMigrationChain(
    existsSync(migrationsDir) ? await loadAllMigrations(migrationsDir) : [],
  );
  let schemas: SchemasDefinition;
  let head: MigrationDefinition | undefined;
  if (chain.length > 0) {
    head = chain[chain.length - 1];
    schemas = head.schemas;
  } else {
    schemas = await loadProjectSchema(
      path.resolve(cwd, config.paths?.schemas || "./schemas.ts"),
    );
  }

  const plan = buildPrivacyPlan({
    schemas,
    posture: parsePosture(options.posture),
  });
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

  const { secret, discarded, literal } = resolveSecret(options.secret);
  if (literal) {
    console.error(
      yellow(
        "Warning: --secret was given as a literal, visible in process lists and shell history; prefer --secret env:NAME",
      ),
    );
  }
  if (plan.posture === "strict" && shiftDays === 0) {
    console.error(
      yellow(
        "Warning: --shift-days 0 keeps every creation time and date as in the source; omit the option to use the strict default shift",
      ),
    );
  }
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
      const existing = await countCollections(targetClient.db(toDb!));
      if (existing > 0 && !options.force) {
        throw new Error(
          `Database "${toDb}" already holds ${existing} collection(s); extract only writes into a database without collections (or pass --force)`,
        );
      }
    }

    const sourceStep = await resolveSourceStep(
      chain,
      fromMigration,
      sourceClient.db(sourceDb),
    );
    const replay = sourceStep
      ? chain.slice(chain.findIndex((m) => m.id === sourceStep.id) + 1)
      : [];
    if (!options.json && replay.length > 0) {
      console.log(
        dim(
          `Source ledger at ${sourceStep!.id}: replaying ${replay.length} migration(s) in memory`,
        ),
      );
    }
    const sourceSchemas = sourceStep ? sourceStep.schemas : schemas;
    const state = await readStateFromDatabase(
      sourceClient.db(sourceDb),
      sourceSchemas,
      {
        ...(options.scope !== undefined && { scope: options.scope }),
      },
    );
    const replayed = await replayWithoutValues(state, replay);

    const transformer = createPrivacyTransformer({
      plan,
      schemas,
      secret,
      consistency,
      ...(shiftDays !== undefined && { timeShiftMs: shiftDays * 86_400_000 }),
      ...(config.privacy?.resolveDynamic && {
        resolveDynamic: config.privacy.resolveDynamic,
      }),
      recompute: (context) => {
        const custom = config.privacy?.recompute?.(context);
        return custom === undefined || custom === SKIP_RECOMPUTE
          ? keepComputedRevision(context)
          : custom;
      },
    });
    const result = transformState(replayed.state, plan, transformer, {
      schemas,
      remapInstanceName: transformer.remapId,
    });
    const violations = checkScenarioState({
      state: result.state,
      schemas,
      plan,
    }).filter((violation) => REPORTED_VIOLATIONS.has(violation.kind));
    const possibleValueJoins = detectValueJoins(replayed.state, plan);
    const summary: ExtractSummary = {
      targets: result.summary,
      posture: plan.posture,
      possibleValueJoins,
      ...(options.scope !== undefined && {
        scope: {
          scope: transformer.remapId(options.scope),
          copiedWhole: [
            ...Object.keys(replayed.state.collections),
            ...Object.keys(replayed.state.multiCollections),
          ].filter(
            (name) =>
              [
                ...(replayed.state.collections[name]?.content ?? []),
                ...(replayed.state.multiCollections[name]?.content ?? []),
              ].length > 0,
          ),
        },
      }),
      skipped: result.skipped,
      violations,
      violationsAllowed: allowViolations,
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
      if (summary.scope) {
        console.log(
          yellow(
            `  ! --scope only filters scoped collections; copied whole: ${summary.scope.copiedWhole.join(", ") || "none"}`,
          ),
        );
      }
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
      for (const join of possibleValueJoins) {
        console.log(yellow(`  ! ${describeValueJoin(join)}`));
      }
      for (const violation of violations) {
        console.log(yellow(`  ! ${violation.target}: ${violation.message}`));
      }
      console.log();
    }
    const collisions = Object.values(result.summary).reduce(
      (total, entry) => total + (entry.notes.collision ?? 0),
      0,
    );
    const invalidNotes = Object.values(result.summary).reduce(
      (total, entry) => total + (entry.notes.invalid ?? 0),
      0,
    );
    const blocking = violations.filter((v) => BLOCKING_VIOLATIONS.has(v.kind));
    if ((invalidNotes > 0 || blocking.length > 0) && !allowViolations) {
      const kinds = [
        ...(invalidNotes > 0 ? [`invalid ${invalidNotes}`] : []),
        ...blocking.map((v) => `${v.kind} ${v.count ?? 1} (${v.target})`),
      ];
      throw new Error(
        `The extracted data breaks its own schemas (${kinds.join(", ")}); nothing was written. Fix the classification or pass --allow-violations`,
      );
    }
    if (collisions > 0) {
      throw new Error(
        `${collisions} unique value(s) could not be made distinct by the pseudonymisation; nothing was written`,
      );
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

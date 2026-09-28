import type * as m from "mongodb";
import {
  getIrreversibleOperations,
  getLossyOperations,
  migrationBuilder,
} from "../../migration/builder.ts";
import { getAppliedMigrationIds } from "../../migration/history.ts";
import { referencesTo } from "../../migration/validators/delete-checks.ts";
import type {
  DatabaseState,
  MigrationDefinition,
  MigrationRule,
} from "../../migration/types.ts";
import { buildCatalog, type CatalogEntry, META_TYPES } from "../catalog.ts";
import type { StudioContext } from "../context.ts";
import {
  compareIndexSpecs,
  declaredIndexes,
  entryUnder,
  type IndexSpec,
  toSpec,
} from "./indexes.ts";
import { describeOperation, type OperationDescription } from "./migrations.ts";
import { field, recordField, textField, textListField } from "./rule-fields.ts";

export const PLAN_TIME_LIMIT_MS = 5_000;
export const PLAN_COUNT_LIMIT = 1_000_000;
export const PLAN_SAMPLE_IDS = 500;
export const PLAN_SCAN_LIMIT = 2_000;

export interface Count {
  value: number;
  capped?: true;
  timedOut?: true;
}

export interface DuplicateSample {
  key: Record<string, unknown>;
  count: number;
  ids: unknown[];
}

export interface Duplicates {
  groups: number;
  documents: number;
  sample: DuplicateSample[];
  timedOut?: true;
}

export interface DanglingReference {
  location: string;
  path: string;
  count: number;
}

export interface OperationFlag {
  flag: "irreversible" | "lossy";
  reason: string;
}

export interface OperationImpact {
  documents?: Count;
  verb?: string;
  exists?: boolean;
  existingDocuments?: Count;
  scopes?: { all: boolean; values: string[]; present?: number };
  duplicates?: Duplicates;
  dangling?: {
    references: DanglingReference[];
    sampledIds: number;
    scanned: number;
  };
  note?: string;
}

export interface PlanOperation extends OperationDescription {
  impact: OperationImpact;
  reasons: OperationFlag[];
  blocking?: string;
}

export interface PlanIndexBuild {
  collection: string;
  name: string;
  key: Record<string, unknown>;
  unique: boolean;
  change: "new" | "changed";
  documents: Count;
  duplicates?: Duplicates;
}

export interface PlanMigration {
  id: string;
  name: string;
  fileName?: string;
  position: number;
  operations: PlanOperation[];
  indexes: PlanIndexBuild[];
  rollback: "possible" | "lossy" | "impossible";
  blocking: string[];
  command: string;
  compileError?: string;
}

export interface PlanReport {
  applied: number;
  pending: number;
  migrations: PlanMigration[];
  blocking: string[];
  commands: string[];
}

function isTimeout(error: unknown): boolean {
  const code = (error as { code?: number; codeName?: string }) ?? {};
  return code.code === 50 || code.codeName === "MaxTimeMSExpired";
}

export async function boundedCount(
  context: StudioContext,
  collection: string,
  filter: m.Filter<m.Document>,
  limit: number = PLAN_COUNT_LIMIT,
): Promise<Count> {
  try {
    const value = await context.db
      .collection(collection)
      .countDocuments(filter, {
        limit,
        maxTimeMS: PLAN_TIME_LIMIT_MS,
      });
    return value >= limit ? { value, capped: true } : { value };
  } catch (error) {
    if (isTimeout(error)) return { value: 0, timedOut: true };
    throw error;
  }
}

function addCounts(counts: Count[]): Count {
  const total: Count = { value: 0 };
  for (const count of counts) {
    total.value += count.value;
    if (count.capped) total.capped = true;
    if (count.timedOut) total.timedOut = true;
  }
  return total;
}

export async function findDuplicates(
  context: StudioContext,
  collection: string,
  fields: readonly string[],
  match: m.Document = {},
  collation?: m.CollationOptions,
): Promise<Duplicates> {
  const group: Record<string, string> = {};
  for (const field of fields) group[field.replaceAll(".", "_")] = `$${field}`;
  try {
    const [result] = await context.db
      .collection(collection)
      .aggregate<{
        sample: {
          _id: Record<string, unknown>;
          count: number;
          ids: unknown[];
        }[];
        totals: { groups: number; documents: number }[];
      }>(
        [
          { $match: match },
          {
            $group: { _id: group, count: { $sum: 1 }, ids: { $push: "$_id" } },
          },
          { $match: { count: { $gt: 1 } } },
          {
            $facet: {
              sample: [
                { $sort: { count: -1 } },
                { $limit: 5 },
                { $project: { count: 1, ids: { $slice: ["$ids", 5] } } },
              ],
              totals: [
                {
                  $group: {
                    _id: null,
                    groups: { $sum: 1 },
                    documents: { $sum: "$count" },
                  },
                },
              ],
            },
          },
        ],
        {
          allowDiskUse: true,
          maxTimeMS: PLAN_TIME_LIMIT_MS,
          ...(collation ? { collation } : {}),
        },
      )
      .toArray();
    return {
      groups: result?.totals[0]?.groups ?? 0,
      documents: result?.totals[0]?.documents ?? 0,
      sample: (result?.sample ?? []).map((row) => ({
        key: row._id,
        count: row.count,
        ids: row.ids,
      })),
    };
  } catch (error) {
    if (isTimeout(error)) {
      return { groups: 0, documents: 0, sample: [], timedOut: true };
    }
    throw error;
  }
}

interface Resolver {
  catalog: CatalogEntry[];
  byName: Map<string, CatalogEntry>;
  instancesOf(model: string): CatalogEntry[];
  state?: Promise<DatabaseState>;
}

function resolver(catalog: CatalogEntry[]): Resolver {
  return {
    catalog,
    byName: new Map(catalog.map((entry) => [entry.name, entry])),
    instancesOf: (model) =>
      catalog.filter(
        (entry) => entry.kind === "multiModelInstance" && entry.model === model,
      ),
  };
}

async function liveState(
  context: StudioContext,
  resolve: Resolver,
  excluded: ReadonlySet<string>,
): Promise<{ state: DatabaseState; scanned: number }> {
  const state: DatabaseState = {
    collections: {},
    multiCollections: {},
    multiModels: {},
    scopedMultiCollections: {},
  };
  let scanned = 0;
  for (const entry of resolve.catalog) {
    if (!entry.exists || entry.kind === "internal") continue;
    const docs = (await context.db
      .collection(entry.name)
      .find({}, { limit: PLAN_SCAN_LIMIT, maxTimeMS: PLAN_TIME_LIMIT_MS })
      .toArray()) as Record<string, unknown>[];
    const content = docs.filter(
      (doc) => !(typeof doc._id === "string" && excluded.has(doc._id)),
    );
    scanned += content.length;
    switch (entry.kind) {
      case "multiCollection":
        state.multiCollections[entry.name] = { content };
        break;
      case "multiModelInstance":
        state.multiModels[entry.name] = {
          content,
          modelType: entry.model ?? "",
        };
        break;
      case "scopedMultiCollection":
        state.scopedMultiCollections[entry.name] = { content };
        break;
      default:
        state.collections[entry.name] = { content };
    }
  }
  return { state, scanned };
}

async function danglingFor(
  context: StudioContext,
  resolve: Resolver,
  collection: string,
  filter: m.Filter<m.Document>,
): Promise<OperationImpact["dangling"]> {
  const doomed = await context.db
    .collection(collection)
    .find(filter, {
      projection: { _id: 1 },
      limit: PLAN_SAMPLE_IDS,
      maxTimeMS: PLAN_TIME_LIMIT_MS,
    })
    .toArray();
  const ids = new Set<string>();
  for (const doc of doomed) {
    const id: unknown = doc._id;
    if (typeof id === "string") ids.add(id);
  }
  if (ids.size === 0) return { references: [], sampledIds: 0, scanned: 0 };
  const { state, scanned } = await liveState(context, resolve, ids);
  return {
    references: referencesTo(state, ids),
    sampledIds: ids.size,
    scanned,
  };
}

function typeFilter(
  type: string | undefined,
  scopes?: readonly string[],
): m.Document {
  const filter: m.Document = {};
  if (type) filter._type = type;
  else filter._type = { $nin: [...META_TYPES] };
  if (scopes && scopes.length > 0) filter._scope = { $in: [...scopes] };
  return filter;
}

function merge(...parts: (m.Document | undefined)[]): m.Document {
  const present = parts.filter(
    (part): part is m.Document => !!part && Object.keys(part).length > 0,
  );
  if (present.length === 0) return {};
  if (present.length === 1) return present[0];
  return { $and: present };
}

async function scopesOf(
  context: StudioContext,
  collection: string,
  type: string,
  filter?: readonly string[],
): Promise<OperationImpact["scopes"]> {
  if (filter && filter.length > 0) return { all: false, values: [...filter] };
  try {
    const [row] = await context.db
      .collection(collection)
      .aggregate<{ values: string[]; present: number }>(
        [
          { $match: { _type: type } },
          { $group: { _id: "$_scope" } },
          {
            $group: {
              _id: null,
              present: { $sum: 1 },
              values: { $push: "$_id" },
            },
          },
          { $project: { present: 1, values: { $slice: ["$values", 8] } } },
        ],
        { maxTimeMS: PLAN_TIME_LIMIT_MS },
      )
      .toArray();
    return {
      all: true,
      values: (row?.values ?? []).map(String),
      present: row?.present ?? 0,
    };
  } catch (error) {
    if (isTimeout(error)) return { all: true, values: [] };
    throw error;
  }
}

function flagReasons(
  rule: MigrationRule,
  irreversible: boolean,
  lossy: boolean,
): OperationFlag[] {
  const reasons: OperationFlag[] = [];
  if (irreversible) {
    let reason = "Marked irreversible: rollback cannot undo it.";
    if (rule.type.startsWith("delete_") || rule.type.startsWith("dedupe_")) {
      reason =
        "Removed documents are not kept, so rollback cannot restore them.";
    } else if (rule.type === "flow" || rule.type === "flow_to_scope") {
      reason =
        field(rule, "sourceDisposition") === "consume"
          ? "Source documents are moved, not copied, so they cannot be put back."
          : "Declared irreversible: rollback does not remove the copies.";
    } else if (rule.type.startsWith("transform_")) {
      reason = "Declared irreversible: its down() is not meant to run.";
    }
    reasons.push({ flag: "irreversible", reason });
  }
  if (lossy) {
    let reason = "Rollback cannot restore everything this operation changes.";
    if (rule.type.startsWith("transform_")) {
      reason =
        "Rollback runs down(), which cannot restore every value up() rewrote.";
    } else if (rule.type === "rename_collection") {
      reason = "The existing target collection is dropped before the rename.";
    }
    reasons.push({ flag: "lossy", reason });
  }
  return reasons;
}

async function estimate(
  context: StudioContext,
  resolve: Resolver,
  rule: MigrationRule,
): Promise<{ impact: OperationImpact; blocking?: string }> {
  const collectionName = textField(rule, "collectionName");
  const modelType = textField(rule, "modelType") ?? "";
  const entry = collectionName ? resolve.byName.get(collectionName) : undefined;
  const exists = Boolean(entry?.exists);
  const type = rule.type;

  if (type.startsWith("create_")) {
    const impact: OperationImpact = { exists };
    if (exists && collectionName) {
      impact.existingDocuments = await boundedCount(
        context,
        collectionName,
        {},
      );
      if (impact.existingDocuments.value > 0) {
        return {
          impact,
          blocking: `${collectionName} already exists with documents; creating it again will fail`,
        };
      }
      impact.note = "Already exists and is empty";
    }
    return { impact };
  }

  if (type.startsWith("seed_")) {
    const seeded = field(rule, "documents");
    const documents = Array.isArray(seeded) ? seeded.length : 0;
    const impact: OperationImpact = {
      documents: { value: documents },
      verb: "inserted",
    };
    if (type === "seed_multimodel_instances_type") {
      const instances = resolve.instancesOf(modelType);
      impact.documents = { value: documents * instances.length };
      impact.note = `${documents} per instance, ${instances.length} instances`;
    }
    const seededScope = textField(rule, "scope");
    if (seededScope !== undefined)
      impact.scopes = { all: false, values: [seededScope] };
    return { impact };
  }

  if (type === "update_indexes") {
    return {
      impact: { note: "Index changes are listed under indexes to build" },
    };
  }

  if (type === "rename_collection") {
    const fromName = textField(rule, "from") ?? "";
    const toName = textField(rule, "to") ?? "";
    const source = resolve.byName.get(fromName);
    const target = resolve.byName.get(toName);
    const impact: OperationImpact = {
      documents: source?.exists
        ? await boundedCount(context, fromName, {})
        : { value: 0 },
      verb: "renamed",
      exists: Boolean(target?.exists),
    };
    if (target?.exists && !field(rule, "dropTarget")) {
      return {
        impact,
        blocking: `${toName} already exists; the rename will fail`,
      };
    }
    return { impact };
  }

  if (type === "flow") {
    const from = recordField(rule, "from");
    const fromCollection = textField(from, "collection") ?? "";
    const source = resolve.byName.get(fromCollection);
    return {
      impact: {
        documents: source?.exists
          ? await boundedCount(
              context,
              fromCollection,
              recordField(from, "where") ?? {},
            )
          : { value: 0 },
        verb:
          field(rule, "sourceDisposition") === "consume" ? "moved" : "copied",
      },
    };
  }

  if (type === "flow_to_scope") {
    const from = recordField(rule, "from");
    const fromKind = textField(from, "kind");
    const fromName = textField(from, "name") ?? "";
    const fromCollection = textField(from, "collectionName") ?? "";
    let documents: Count = { value: 0 };
    if (fromKind === "collection" && resolve.byName.get(fromName)?.exists) {
      documents = await boundedCount(
        context,
        fromName,
        recordField(from, "where") ?? {},
      );
    } else if (
      fromKind === "multiCollectionType" &&
      resolve.byName.get(fromCollection)?.exists
    ) {
      documents = await boundedCount(context, fromCollection, {
        _type: textField(from, "documentType"),
      });
    } else if (fromKind === "multiModelInstances") {
      documents = addCounts(
        await Promise.all(
          resolve
            .instancesOf(textField(from, "model") ?? "")
            .map((instance) =>
              boundedCount(context, instance.name, typeFilter(undefined)),
            ),
        ),
      );
    }
    return {
      impact: {
        documents,
        verb:
          field(rule, "sourceDisposition") === "consume" ? "moved" : "copied",
      },
    };
  }

  const documentType =
    textField(rule, "documentType") ?? textField(rule, "oldTypeName");
  const scoped = type.includes("scoped");
  const scopeFilter = textListField(rule, "scopeFilter");
  const isDelete = type.startsWith("delete_");
  const isDedupe = type.startsWith("dedupe_");
  const verb =
    isDelete || isDedupe
      ? "removed"
      : type.startsWith("rename_")
        ? "retyped"
        : type.startsWith("mark_")
          ? "marked"
          : "rewritten";

  const targets: string[] = [];
  if (type.includes("multimodel_instances")) {
    for (const instance of resolve.instancesOf(modelType))
      targets.push(instance.name);
  } else if (collectionName && exists) {
    targets.push(collectionName);
  }

  const kind =
    type.includes("multicollection") || type.includes("multimodel")
      ? "typed"
      : "plain";
  const base =
    kind === "typed" || scoped ? typeFilter(documentType, scopeFilter) : {};
  const filter = merge(base, recordField(rule, "where"));

  const impact: OperationImpact = { verb };

  if (isDedupe) {
    const by = textListField(rule, "by") ?? [];
    const results = await Promise.all(
      targets.map((target) =>
        findDuplicates(
          context,
          target,
          [...(scoped ? ["_scope"] : []), ...by],
          filter,
        ),
      ),
    );
    const removed = results.reduce(
      (sum, r) => sum + (r.documents - r.groups),
      0,
    );
    impact.documents = {
      value: removed,
      ...(results.some((r) => r.timedOut) ? { timedOut: true as const } : {}),
    };
    impact.duplicates = {
      groups: results.reduce((sum, r) => sum + r.groups, 0),
      documents: results.reduce((sum, r) => sum + r.documents, 0),
      sample: results.flatMap((r) => r.sample).slice(0, 5),
    };
  } else {
    impact.documents = addCounts(
      await Promise.all(
        targets.map((target) => boundedCount(context, target, filter)),
      ),
    );
  }

  if (scoped && documentType && collectionName && exists) {
    impact.scopes = await scopesOf(
      context,
      collectionName,
      documentType,
      scopeFilter,
    );
  }

  if (
    (isDelete || isDedupe) &&
    targets.length > 0 &&
    (impact.documents?.value ?? 0) > 0
  ) {
    const references: DanglingReference[] = [];
    let sampledIds = 0;
    let scanned = 0;
    for (const target of targets) {
      const found = await danglingFor(context, resolve, target, filter);
      if (!found) continue;
      references.push(...found.references);
      sampledIds += found.sampledIds;
      scanned = Math.max(scanned, found.scanned);
    }
    impact.dangling = { references, sampledIds, scanned };
  }

  if (targets.length === 0)
    impact.note = "Target collection does not exist yet";
  return { impact };
}

async function actualIndexes(
  context: StudioContext,
  name: string,
): Promise<IndexSpec[]> {
  return (await context.db.collection(name).listIndexes().toArray()).map(
    toSpec,
  );
}

async function indexBuilds(
  context: StudioContext,
  resolve: Resolver,
  migration: MigrationDefinition,
  previous: MigrationDefinition | undefined,
): Promise<PlanIndexBuild[]> {
  const builds: PlanIndexBuild[] = [];
  for (const entry of resolve.catalog) {
    if (!entry.exists) continue;
    const next = entryUnder(entry, migration.schemas);
    if (!next) continue;
    const wanted = (await declaredIndexes(next)) ?? [];
    let before: IndexSpec[];
    if (previous) {
      const prior = entryUnder(entry, previous.schemas);
      before = prior ? ((await declaredIndexes(prior)) ?? []) : [];
    } else {
      before = await actualIndexes(context, entry.name);
    }
    const byName = new Map(before.map((spec) => [spec.name, spec]));
    for (const spec of wanted) {
      const existing = byName.get(spec.name);
      if (existing && compareIndexSpecs(spec, existing).length === 0) continue;
      const build: PlanIndexBuild = {
        collection: entry.name,
        name: spec.name,
        key: spec.key,
        unique: Boolean(spec.unique),
        change: existing ? "changed" : "new",
        documents: await boundedCount(
          context,
          entry.name,
          (spec.partialFilterExpression as m.Document) ?? {},
        ),
      };
      if (spec.unique) {
        build.duplicates = await findDuplicates(
          context,
          entry.name,
          Object.keys(spec.key),
          (spec.partialFilterExpression as m.Document) ?? {},
          spec.collation as m.CollationOptions | undefined,
        );
      }
      builds.push(build);
    }
  }
  return builds;
}

function compile(migration: MigrationDefinition) {
  const state = migration.migrate(
    migrationBuilder({
      schemas: migration.schemas,
      parentSchemas: migration.parent?.schemas,
    }),
  );
  return state.operations;
}

export async function getPlan(context: StudioContext): Promise<PlanReport> {
  const applied = new Set(await getAppliedMigrationIds(context.db));
  const pending = context.migrations.filter(
    (migration) => !applied.has(migration.id),
  );
  const resolve = resolver(await buildCatalog(context));
  const migrations: PlanMigration[] = [];

  for (const [index, migration] of pending.entries()) {
    const position = context.migrations.indexOf(migration);
    const plan: PlanMigration = {
      id: migration.id,
      name: migration.name,
      position,
      operations: [],
      indexes: [],
      rollback: "possible",
      blocking: [],
      command: `mongodbee migrate --target ${migration.id}`,
    };
    const fileName = context.migrationFiles.get(migration.id);
    if (fileName) plan.fileName = fileName;

    let rules: MigrationRule[] = [];
    try {
      rules = compile(migration);
    } catch (error) {
      plan.compileError =
        error instanceof Error ? error.message : String(error);
      plan.blocking.push("The migration does not compile");
      migrations.push(plan);
      continue;
    }

    const irreversible = new Set(getIrreversibleOperations(rules));
    const lossy = new Set(getLossyOperations(rules));
    for (const rule of rules) {
      const flags: ("irreversible" | "lossy")[] = [];
      if (irreversible.has(rule)) flags.push("irreversible");
      if (lossy.has(rule)) flags.push("lossy");
      const { impact, blocking } = await estimate(context, resolve, rule);
      const operation: PlanOperation = {
        ...describeOperation(rule, flags),
        impact,
        reasons: flagReasons(rule, irreversible.has(rule), lossy.has(rule)),
      };
      if (blocking) {
        operation.blocking = blocking;
        plan.blocking.push(blocking);
      }
      plan.operations.push(operation);
    }

    plan.indexes = await indexBuilds(
      context,
      resolve,
      migration,
      index > 0 ? pending[index - 1] : undefined,
    );
    for (const build of plan.indexes) {
      if (build.duplicates && build.duplicates.groups > 0) {
        plan.blocking.push(
          `Unique index ${build.name} on ${build.collection} cannot be built: ${build.duplicates.groups} duplicate key groups exist`,
        );
      }
    }

    plan.rollback =
      irreversible.size > 0
        ? "impossible"
        : lossy.size > 0
          ? "lossy"
          : "possible";
    migrations.push(plan);
  }

  const blocking: string[] = [];
  if (pending.length > 0) {
    blocking.push(
      `mongodbee sync refuses to run while ${pending.length} migration${pending.length === 1 ? " is" : "s are"} pending; mongodbee migrate applies them and synchronizes indexes`,
    );
  }

  return {
    applied: context.migrations.length - pending.length,
    pending: pending.length,
    migrations,
    blocking,
    commands:
      pending.length > 0 ? ["mongodbee check", "mongodbee migrate"] : [],
  };
}

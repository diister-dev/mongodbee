import { keyEqual, normalizeIndexOptions } from "@diister/mongodbee";
import {
  getAppliedMigrationIds,
  type IndexPlanKind,
  type MigrationDefinition,
  plannedIndexes,
  type SchemasDefinition,
} from "@diister/mongodbee/inspect";
import { isRecord } from "../guards.ts";
import type { CatalogEntry } from "../catalog.ts";
import type { StudioContext } from "../context.ts";
import { requireEntry } from "./documents.ts";

export type IndexStatus = "matching" | "missing" | "extra" | "different";

export interface IndexSpec {
  name: string;
  key: Record<string, unknown>;
  unique?: boolean;
  collation?: unknown;
  partialFilterExpression?: unknown;
  expireAfterSeconds?: number;
}

export interface IndexUsage {
  ops: number;
  since?: string;
}

export interface IndexRow {
  name: string;
  status: IndexStatus;
  declared?: IndexSpec;
  actual?: IndexSpec;
  differences?: string[];
  hint?: IndexHint;
  usage?: IndexUsage;
  sizeBytes?: number;
}

export interface IndexStatsAvailability {
  usage: boolean;
  size: boolean;
  documents?: number;
}

export interface IndexHint {
  reason: "not-synced" | "pending-migration" | "no-migration";
  command: string;
}

export interface IndexReport {
  collection: string;
  exists: boolean;
  declaredKnown: boolean;
  rows: IndexRow[];
  summary: Record<IndexStatus, number>;
  stats: IndexStatsAvailability;
}

const STATS_TIMEOUT_MS = 2000;

async function indexUsage(
  context: StudioContext,
  name: string,
): Promise<Map<string, IndexUsage> | undefined> {
  try {
    const rows = await context.db
      .collection(name)
      .aggregate([{ $indexStats: {} }], { maxTimeMS: STATS_TIMEOUT_MS })
      .toArray();
    const usage = new Map<string, IndexUsage>();
    for (const row of rows as Array<{
      name?: string;
      accesses?: { ops?: unknown; since?: unknown };
    }>) {
      if (!row.name) continue;
      const since = row.accesses?.since;
      usage.set(row.name, {
        ops: Number(row.accesses?.ops ?? 0),
        ...(since instanceof Date ? { since: since.toISOString() } : {}),
      });
    }
    return usage;
  } catch {
    return undefined;
  }
}

async function indexSizes(
  context: StudioContext,
  name: string,
): Promise<{ sizes: Map<string, number>; documents?: number } | undefined> {
  try {
    const [row] = await context.db
      .collection(name)
      .aggregate([{ $collStats: { storageStats: {} } }], {
        maxTimeMS: STATS_TIMEOUT_MS,
      })
      .toArray();
    const storage = (
      row as
        | {
            storageStats?: {
              indexSizes?: Record<string, unknown>;
              count?: unknown;
            };
          }
        | undefined
    )?.storageStats;
    if (!storage?.indexSizes) return undefined;
    const sizes = new Map<string, number>();
    for (const [index, size] of Object.entries(storage.indexSizes)) {
      sizes.set(index, Number(size));
    }
    return {
      sizes,
      ...(storage.count !== undefined
        ? { documents: Number(storage.count) }
        : {}),
    };
  } catch {
    return undefined;
  }
}

const PLAN_KINDS: Partial<Record<CatalogEntry["kind"], IndexPlanKind>> = {
  collection: "collection",
  multiCollection: "multiCollection",
  multiModelInstance: "multiCollection",
  scopedMultiCollection: "scopedMultiCollection",
};

export async function declaredIndexes(
  entry: CatalogEntry,
): Promise<IndexSpec[] | undefined> {
  const kind = PLAN_KINDS[entry.kind];
  if (!kind) return undefined;
  const planned = await plannedIndexes({
    kind,
    name: entry.name,
    types: entry.types,
    scope: entry.scope,
  });
  return [{ name: "_id_", key: { _id: 1 } }, ...planned.map(toSpec)];
}

function plainKey(key: unknown): Record<string, unknown> {
  if (key instanceof Map) return Object.fromEntries(key);
  return isRecord(key) ? { ...key } : {};
}

export function toSpec(value: unknown): IndexSpec {
  const raw = isRecord(value) ? value : {};
  const spec: IndexSpec = { name: String(raw.name), key: plainKey(raw.key) };
  if (raw.unique) spec.unique = true;
  if (raw.collation) spec.collation = raw.collation;
  if (raw.partialFilterExpression) {
    spec.partialFilterExpression = raw.partialFilterExpression;
  }
  if (typeof raw.expireAfterSeconds === "number") {
    spec.expireAfterSeconds = raw.expireAfterSeconds;
  }
  return spec;
}

export function compareIndexSpecs(
  declared: IndexSpec,
  actual: IndexSpec,
): string[] {
  const differences: string[] = [];
  if (!keyEqual(declared.key, actual.key)) differences.push("key");
  const want = normalizeIndexOptions(declared);
  const have = normalizeIndexOptions(actual);
  if (want.unique !== have.unique) differences.push("unique");
  if (want.collation !== have.collation) differences.push("collation");
  if (want.partialFilterExpression !== have.partialFilterExpression) {
    differences.push("partialFilterExpression");
  }
  if (want.expireAfterSeconds !== have.expireAfterSeconds) {
    differences.push("expireAfterSeconds");
  }
  return differences;
}

export function diffIndexes(
  declared: readonly IndexSpec[] | undefined,
  actual: readonly IndexSpec[],
): IndexRow[] {
  const rows: IndexRow[] = [];
  const byName = new Map(actual.map((spec) => [spec.name, spec]));
  const seen = new Set<string>();
  for (const spec of declared ?? []) {
    const found = byName.get(spec.name);
    if (!found) {
      rows.push({ name: spec.name, status: "missing", declared: spec });
      continue;
    }
    seen.add(spec.name);
    const differences = compareIndexSpecs(spec, found);
    rows.push(
      differences.length === 0
        ? { name: spec.name, status: "matching", declared: spec, actual: found }
        : {
            name: spec.name,
            status: "different",
            declared: spec,
            actual: found,
            differences,
          },
    );
  }
  for (const spec of actual) {
    if (seen.has(spec.name)) continue;
    if (!declared && spec.name === "_id_") {
      rows.push({ name: spec.name, status: "matching", actual: spec });
      continue;
    }
    rows.push({ name: spec.name, status: "extra", actual: spec });
  }
  return rows;
}

export function entryUnder(
  entry: CatalogEntry,
  schemas: SchemasDefinition,
): CatalogEntry | undefined {
  switch (entry.kind) {
    case "collection": {
      const source = schemas.collections?.[entry.name];
      return source ? { ...entry, types: { [entry.name]: source } } : undefined;
    }
    case "multiCollection": {
      const types = schemas.multiCollections?.[entry.name];
      return types ? { ...entry, types } : undefined;
    }
    case "multiModelInstance": {
      const types = entry.model
        ? schemas.multiModels?.[entry.model]
        : undefined;
      return types ? { ...entry, types } : undefined;
    }
    case "scopedMultiCollection": {
      const scoped = schemas.scopedMultiCollections?.[entry.name];
      return scoped
        ? { ...entry, types: scoped.types, scope: scoped.scope }
        : undefined;
    }
    default:
      return undefined;
  }
}

async function declaredUnder(
  entry: CatalogEntry,
  migration: MigrationDefinition | undefined,
): Promise<Map<string, IndexSpec>> {
  const scoped = migration ? entryUnder(entry, migration.schemas) : undefined;
  const specs = scoped ? await declaredIndexes(scoped) : undefined;
  return new Map((specs ?? []).map((spec) => [spec.name, spec]));
}

async function attachHints(
  context: StudioContext,
  entry: CatalogEntry,
  rows: IndexRow[],
): Promise<void> {
  const applied = new Set(await getAppliedMigrationIds(context.db));
  const chain = context.migrations;
  const lastApplied = [...chain].reverse().find((m) => applied.has(m.id));
  const pending = chain.filter((m) => !applied.has(m.id)).length;
  const underApplied = await declaredUnder(entry, lastApplied);
  const underLatest = await declaredUnder(entry, chain[chain.length - 1]);

  for (const row of rows) {
    if (!row.declared) continue;
    if (row.status !== "missing" && row.status !== "different") continue;
    const inApplied = underApplied.get(row.name);
    const inLatest = underLatest.get(row.name);
    if (inApplied && compareIndexSpecs(row.declared, inApplied).length === 0) {
      row.hint =
        pending > 0
          ? { reason: "not-synced", command: "mongodbee migrate" }
          : { reason: "not-synced", command: "mongodbee sync" };
    } else if (
      inLatest &&
      compareIndexSpecs(row.declared, inLatest).length === 0
    ) {
      row.hint = { reason: "pending-migration", command: "mongodbee migrate" };
    } else {
      row.hint = { reason: "no-migration", command: "mongodbee generate" };
    }
  }
}

export async function getIndexReport(
  context: StudioContext,
  collectionName: string,
): Promise<IndexReport> {
  const entry = await requireEntry(context, collectionName);
  const declared = await declaredIndexes(entry);
  const actual = entry.exists
    ? (await context.db.collection(entry.name).listIndexes().toArray()).map(
        toSpec,
      )
    : [];
  const rows = diffIndexes(declared, actual);
  if (
    rows.some((row) => row.status === "missing" || row.status === "different")
  ) {
    await attachHints(context, entry, rows);
  }
  const summary: Record<IndexStatus, number> = {
    matching: 0,
    missing: 0,
    extra: 0,
    different: 0,
  };
  for (const row of rows) summary[row.status]++;
  const [usage, sizes] = entry.exists
    ? await Promise.all([
        indexUsage(context, entry.name),
        indexSizes(context, entry.name),
      ])
    : [undefined, undefined];
  for (const row of rows) {
    if (!row.actual) continue;
    const used = usage?.get(row.name);
    if (used) row.usage = used;
    const size = sizes?.sizes.get(row.name);
    if (size !== undefined) row.sizeBytes = size;
  }
  return {
    collection: entry.name,
    exists: entry.exists,
    declaredKnown: declared !== undefined,
    rows,
    summary,
    stats: {
      usage: usage !== undefined,
      size: sizes !== undefined,
      ...(sizes?.documents !== undefined ? { documents: sizes.documents } : {}),
    },
  };
}

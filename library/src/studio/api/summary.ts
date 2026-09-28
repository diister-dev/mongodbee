import { ObjectId } from "mongodb";
import { decodeTime } from "../../utils/ulid.ts";
import { type CollectionKind, isTyped, META_TYPES } from "../catalog.ts";
import type { StudioContext } from "../context.ts";
import { requireEntry } from "./documents.ts";
import {
  countScopes,
  idPrefixOf,
  QUERY_TIME_LIMIT_MS,
  type ScopeCount,
} from "./overview.ts";

export const SUMMARY_TOP_SCOPES = 12;
export const SCOPE_SIZE_BOUNDARIES = [1, 10, 100, 1_000, 10_000] as const;

export interface TypeSummary {
  name: string;
  count: number;
  declared: boolean;
  meta?: true;
  idPrefix?: string;
  firstId?: unknown;
  lastId?: unknown;
  scopes?: number;
}

export interface ScopeSizeBucket {
  from: number;
  to?: number;
  scopes: number;
}

export interface CollectionSummary {
  collection: string;
  kind: CollectionKind;
  total: number;
  types: TypeSummary[];
  scopes?: {
    distinct: number;
    top: ScopeCount[];
    sizes: ScopeSizeBucket[];
    unscoped: number;
  };
  created?: CreationTimeline;
}

export const CREATION_SAMPLE = 5_000;
export const FUTURE_SLACK_MS = 24 * 60 * 60 * 1000;

export interface CreationTimeline {
  sampled: number;
  dated: number;
  future: number;
  months: { month: string; count: number }[];
  newest?: number;
}

const ULID_SHAPE = /^[0-7][0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{25}$/;
const OBJECT_ID_HEX = /^[0-9a-fA-F]{24}$/;
const TYPED_ID = /^[A-Za-z][A-Za-z0-9_.-]*:(.+)$/;

export function idTimestamp(id: unknown): number | null {
  if (id instanceof ObjectId) return id.getTimestamp().getTime();
  if (typeof id !== "string") return null;
  const raw = TYPED_ID.exec(id)?.[1] ?? id;
  if (ULID_SHAPE.test(raw)) {
    try {
      return decodeTime(raw.toUpperCase());
    } catch {
      return null;
    }
  }
  if (OBJECT_ID_HEX.test(raw))
    return Number.parseInt(raw.slice(0, 8), 16) * 1000;
  return null;
}

function monthOf(time: number): string {
  const date = new Date(time);
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;
}

export function creationTimeline(
  ids: unknown[],
  now: number = Date.now(),
): CreationTimeline {
  const counts = new Map<string, number>();
  let dated = 0;
  let future = 0;
  let newest: number | undefined;
  for (const id of ids) {
    const time = idTimestamp(id);
    if (time === null) continue;
    if (time > now + FUTURE_SLACK_MS) {
      future++;
      continue;
    }
    dated++;
    if (newest === undefined || time > newest) newest = time;
    const month = monthOf(time);
    counts.set(month, (counts.get(month) ?? 0) + 1);
  }
  const months = [...counts.keys()].sort();
  const filled: { month: string; count: number }[] = [];
  if (months.length > 0) {
    let [year, month] = months[0].split("-").map(Number);
    const last = months[months.length - 1];
    for (;;) {
      const key = `${year}-${String(month).padStart(2, "0")}`;
      filled.push({ month: key, count: counts.get(key) ?? 0 });
      if (key === last || filled.length > 240) break;
      month++;
      if (month > 12) {
        month = 1;
        year++;
      }
    }
  }
  const timeline: CreationTimeline = {
    sampled: ids.length,
    dated,
    future,
    months: filled,
  };
  if (newest !== undefined) timeline.newest = newest;
  return timeline;
}

async function sampleCreation(
  context: StudioContext,
  name: string,
): Promise<CreationTimeline> {
  const rows = await context.db
    .collection(name)
    .aggregate<{ _id: unknown }>(
      [
        { $match: { _type: { $nin: [...META_TYPES] } } },
        { $sample: { size: CREATION_SAMPLE } },
        { $project: { _id: 1 } },
      ],
      { maxTimeMS: QUERY_TIME_LIMIT_MS },
    )
    .toArray();
  return creationTimeline(rows.map((row) => row._id));
}

interface TypeRow {
  _id: unknown;
  count: number;
  first: unknown;
  last: unknown;
}

async function summarizeTypes(
  context: StudioContext,
  name: string,
  declared: string[],
): Promise<TypeSummary[]> {
  const rows = await context.db
    .collection(name)
    .aggregate<TypeRow>(
      [
        {
          $group: {
            _id: "$_type",
            count: { $sum: 1 },
            first: { $min: "$_id" },
            last: { $max: "$_id" },
          },
        },
      ],
      { maxTimeMS: QUERY_TIME_LIMIT_MS },
    )
    .toArray();
  const byName = new Map<string, TypeRow>();
  for (const row of rows) {
    if (typeof row._id === "string") byName.set(row._id, row);
  }
  const names = [
    ...declared,
    ...[...byName.keys()]
      .filter((type) => !declared.includes(type))
      .sort((a, b) => a.localeCompare(b)),
  ];
  return names.map((type) => {
    const row = byName.get(type);
    const summary: TypeSummary = {
      name: type,
      count: row?.count ?? 0,
      declared: declared.includes(type),
    };
    if (META_TYPES.includes(type)) summary.meta = true;
    if (row && row.count > 0) {
      summary.firstId = row.first;
      summary.lastId = row.last;
    }
    return summary;
  });
}

async function scopeCoverage(
  context: StudioContext,
  name: string,
): Promise<{
  perType: Map<string, number>;
  sizes: ScopeSizeBucket[];
  unscoped: number;
}> {
  const collection = context.db.collection(name);
  const [coverage, sizes, unscoped] = await Promise.all([
    collection
      .aggregate<{ _id: unknown; scopes: number }>(
        [
          { $group: { _id: { scope: "$_scope", type: "$_type" } } },
          { $group: { _id: "$_id.type", scopes: { $sum: 1 } } },
        ],
        { maxTimeMS: QUERY_TIME_LIMIT_MS },
      )
      .toArray(),
    collection
      .aggregate<{ _id: unknown; scopes: number }>(
        [
          { $match: { _type: { $nin: [...META_TYPES] } } },
          { $group: { _id: "$_scope", docs: { $sum: 1 } } },
          {
            $bucket: {
              groupBy: "$docs",
              boundaries: [...SCOPE_SIZE_BOUNDARIES],
              default: "more",
              output: { scopes: { $sum: 1 } },
            },
          },
        ],
        { maxTimeMS: QUERY_TIME_LIMIT_MS },
      )
      .toArray(),
    collection.countDocuments(
      { _scope: { $exists: false } },
      { maxTimeMS: QUERY_TIME_LIMIT_MS },
    ),
  ]);
  const perType = new Map<string, number>();
  for (const row of coverage) {
    if (typeof row._id === "string") perType.set(row._id, row.scopes);
  }
  const counted = new Map<unknown, number>(
    sizes.map((row) => [row._id, row.scopes]),
  );
  const buckets: ScopeSizeBucket[] = SCOPE_SIZE_BOUNDARIES.map(
    (from, index) => {
      const to = SCOPE_SIZE_BOUNDARIES[index + 1];
      const bucket: ScopeSizeBucket = {
        from,
        scopes: counted.get(from) ?? 0,
      };
      if (to !== undefined) bucket.to = to - 1;
      return bucket;
    },
  );
  const last = buckets[buckets.length - 1];
  last.scopes += counted.get("more") ?? 0;
  return { perType, sizes: buckets, unscoped };
}

export async function getCollectionSummary(
  context: StudioContext,
  name: string,
): Promise<CollectionSummary> {
  const entry = await requireEntry(context, name);
  if (!isTyped(entry)) {
    const total = entry.exists
      ? await context.db
          .collection(entry.name)
          .estimatedDocumentCount({ maxTimeMS: QUERY_TIME_LIMIT_MS })
      : 0;
    const plain: CollectionSummary = {
      collection: entry.name,
      kind: entry.kind,
      total,
      types: [],
    };
    if (entry.exists) {
      const [bounds] = await context.db
        .collection(entry.name)
        .aggregate<{ first: unknown; last: unknown }>(
          [
            {
              $group: {
                _id: null,
                first: { $min: "$_id" },
                last: { $max: "$_id" },
              },
            },
          ],
          { maxTimeMS: QUERY_TIME_LIMIT_MS },
        )
        .toArray();
      const type: TypeSummary = {
        name: entry.name,
        count: total,
        declared: entry.kind === "collection",
      };
      if (bounds && total > 0) {
        type.firstId = bounds.first;
        type.lastId = bounds.last;
      }
      plain.types = [type];
      if (total > 0) plain.created = await sampleCreation(context, entry.name);
    }
    return plain;
  }
  const summary: CollectionSummary = {
    collection: entry.name,
    kind: entry.kind,
    total: 0,
    types: Object.keys(entry.types).map((type) => ({
      name: type,
      count: 0,
      declared: true,
    })),
  };
  if (entry.exists) {
    summary.types = await summarizeTypes(
      context,
      entry.name,
      Object.keys(entry.types),
    );
    summary.total = summary.types.reduce((sum, type) => sum + type.count, 0);
    if (summary.total > 0) {
      summary.created = await sampleCreation(context, entry.name);
    }
  }
  summary.types = summary.types.map((type) => {
    const prefix = idPrefixOf(entry.types[type.name]);
    return prefix && prefix !== type.name
      ? { ...type, idPrefix: prefix }
      : type;
  });
  if (entry.kind === "scopedMultiCollection") {
    if (!entry.exists) {
      summary.scopes = {
        distinct: 0,
        top: [],
        sizes: SCOPE_SIZE_BOUNDARIES.map((from, index) => {
          const to = SCOPE_SIZE_BOUNDARIES[index + 1];
          return to === undefined
            ? { from, scopes: 0 }
            : { from, to: to - 1, scopes: 0 };
        }),
        unscoped: 0,
      };
    } else {
      const [scopes, coverage] = await Promise.all([
        countScopes(context, entry.name, SUMMARY_TOP_SCOPES),
        scopeCoverage(context, entry.name),
      ]);
      summary.types = summary.types.map((type) => {
        const count = coverage.perType.get(type.name);
        return count === undefined ? type : { ...type, scopes: count };
      });
      summary.scopes = {
        distinct: scopes.distinct,
        top: scopes.top,
        sizes: coverage.sizes,
        unscoped: coverage.unscoped,
      };
    }
  }
  return summary;
}

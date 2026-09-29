import {
  checkComputed,
  type ComputedField,
  type ComputedLocation,
  type ComputedSchemas,
  type ComputedTopology,
  computedTopology,
  pendingComputed,
} from "@diister/mongodbee";
import { COMPUTED_REVISION, COMPUTED_ROOT } from "@diister/mongodbee/inspect";
import type { CatalogEntry } from "../catalog.ts";
import { isTyped } from "../catalog.ts";
import type { StudioContext } from "../context.ts";
import { encodeId, StudioHttpError } from "../http.ts";
import { requireEntry } from "./documents.ts";
import { QUERY_TIME_LIMIT_MS } from "./overview.ts";
import { describeComputed } from "./schema.ts";

export const COMPUTED_SAMPLE = 5_000;
export const DRIFT_CHECK_DEFAULT = 200;
export const DRIFT_CHECK_MAX = 2_000;
const DRIFT_EXAMPLES = 20;

export interface ComputedSource {
  collection: string;
  type?: string;
  scoped: boolean;
}

export type ComputedLiteral = string | number | boolean | null;

export interface ComputedFieldInfo {
  subject: string;
  name: string;
  description: string;
  source: ComputedSource;
  by: string;
  where: Record<string, ComputedLiteral | ComputedLiteral[]>;
  sameScope: boolean;
  through?: {
    source: ComputedSource;
    via: string;
    where: Record<string, ComputedLiteral | ComputedLiteral[]>;
  };
  aggregate:
    | { kind: "collect"; path: string; distinct: boolean; maxEntries?: number }
    | { kind: "count" };
  filled?: number;
  sampled?: number;
  pending?: number;
}

export interface ComputedReport {
  subjects: string[];
  fields: ComputedFieldInfo[];
  topologyError?: string;
  stats?: {
    sampled: number;
    revisions: Array<{ revision: number | null; count: number }>;
  };
  pending?: { count: number; oldestAgeMs: number | null };
}

export interface ComputedDriftExample {
  id: string;
  open: string;
  field: string;
  stored: unknown;
  truth: unknown;
  missing: boolean;
}

export interface ComputedDriftReport {
  subject: string;
  checked: number;
  complete: boolean;
  drifted: Record<string, number>;
  examples: ComputedDriftExample[];
}

const topologies = new WeakMap<
  StudioContext,
  { topology?: ComputedTopology; error?: string }
>();

function topologyOf(context: StudioContext): {
  topology?: ComputedTopology;
  error?: string;
} {
  const cached = topologies.get(context);
  if (cached) return cached;
  let result: { topology?: ComputedTopology; error?: string };
  try {
    const schemas: ComputedSchemas = context.schemas;
    result = { topology: computedTopology(schemas) };
  } catch (error) {
    result = { error: error instanceof Error ? error.message : String(error) };
  }
  topologies.set(context, result);
  return result;
}

function sourceOf(location: ComputedLocation): ComputedSource {
  return location.kind === "collection"
    ? { collection: location.collection, scoped: false }
    : {
        collection: location.collection,
        type: location.type,
        scoped: location.kind === "scoped",
      };
}

function copyWhere(
  where: Readonly<Record<string, ComputedLiteral | readonly ComputedLiteral[]>>,
): Record<string, ComputedLiteral | ComputedLiteral[]> {
  const result: Record<string, ComputedLiteral | ComputedLiteral[]> = {};
  for (const [path, value] of Object.entries(where)) {
    result[path] = isLiteralList(value) ? [...value] : value;
  }
  return result;
}

function isLiteralList(
  value: ComputedLiteral | readonly ComputedLiteral[],
): value is readonly ComputedLiteral[] {
  return Array.isArray(value);
}

function infoOf(field: ComputedField): ComputedFieldInfo {
  const { descriptor } = field;
  const info: ComputedFieldInfo = {
    subject: field.subject,
    name: field.name,
    description: describeComputed(descriptor),
    source: sourceOf(field.source),
    by: descriptor.by,
    where: copyWhere(descriptor.where),
    sameScope: descriptor.sameScope,
    aggregate: { ...descriptor.aggregate },
  };
  if (descriptor.through && field.far) {
    info.through = {
      source: sourceOf(field.far),
      via: descriptor.through.via,
      where: copyWhere(descriptor.through.where),
    };
  }
  return info;
}

function subjectsOf(entry: CatalogEntry, type: string | undefined): string[] {
  if (!isTyped(entry)) return [entry.name];
  if (type) return entry.types[type] ? [type] : [];
  return Object.keys(entry.types);
}

function fieldsFor(
  topology: ComputedTopology | undefined,
  entry: CatalogEntry,
  subjects: readonly string[],
): ComputedField[] {
  if (!topology) return [];
  return topology.fields.filter(
    (field) =>
      subjects.includes(field.subject) && field.at.collection === entry.name,
  );
}

function subjectFilter(
  entry: CatalogEntry,
  subject: string,
  scope: string | undefined,
): Record<string, unknown> {
  const filter: Record<string, unknown> = {};
  if (isTyped(entry)) filter._type = subject;
  if (scope) filter._scope = scope;
  return filter;
}

interface SampleRow {
  sampled: Array<{ n: number }>;
  filled: Array<Record<string, number>>;
  revisions: Array<{ _id: unknown; count: number }>;
}

async function sampleSubject(
  context: StudioContext,
  entry: CatalogEntry,
  subject: string,
  names: readonly string[],
  scope: string | undefined,
): Promise<{
  sampled: number;
  filled: Record<string, number>;
  revisions: Map<number | null, number>;
}> {
  const filter = subjectFilter(entry, subject, scope);
  const filledGroup: Record<string, unknown> = { _id: null };
  names.forEach((name, index) => {
    filledGroup[`f${index}`] = {
      $sum: {
        $cond: [
          {
            $in: [{ $type: `$${COMPUTED_ROOT}.${name}` }, ["missing", "null"]],
          },
          0,
          1,
        ],
      },
    };
  });
  const [row] = await context.db
    .collection(entry.name)
    .aggregate<SampleRow>(
      [
        ...(Object.keys(filter).length > 0 ? [{ $match: filter }] : []),
        { $sample: { size: COMPUTED_SAMPLE } },
        {
          $facet: {
            sampled: [{ $count: "n" }],
            filled: [{ $group: filledGroup }],
            revisions: [
              {
                $group: {
                  _id: `$${COMPUTED_ROOT}.${COMPUTED_REVISION}`,
                  count: { $sum: 1 },
                },
              },
            ],
          },
        },
      ],
      { maxTimeMS: QUERY_TIME_LIMIT_MS, allowDiskUse: false },
    )
    .toArray();
  const filledRow = row?.filled[0] ?? {};
  const revisions = new Map<number | null, number>();
  for (const group of row?.revisions ?? []) {
    const revision = typeof group._id === "number" ? group._id : null;
    revisions.set(revision, (revisions.get(revision) ?? 0) + group.count);
  }
  return {
    sampled: row?.sampled[0]?.n ?? 0,
    filled: Object.fromEntries(
      names.map((name, index) => [name, filledRow[`f${index}`] ?? 0]),
    ),
    revisions,
  };
}

export async function getComputedReport(
  context: StudioContext,
  collectionName: string,
  params: URLSearchParams,
): Promise<ComputedReport> {
  const entry = await requireEntry(context, collectionName);
  const type = params.get("type") || undefined;
  const scope = params.get("scope") || undefined;
  const withStats = params.get("stats") !== "false";
  const { topology, error } = topologyOf(context);
  const subjects = subjectsOf(entry, type);
  const fields = fieldsFor(topology, entry, subjects);
  const report: ComputedReport = {
    subjects: [...new Set(fields.map((field) => field.subject))],
    fields: fields.map(infoOf),
  };
  if (error) report.topologyError = error;
  if (!withStats || fields.length === 0 || !entry.exists) return report;

  const revisions = new Map<number | null, number>();
  let sampled = 0;
  for (const subject of report.subjects) {
    const names = fields
      .filter((field) => field.subject === subject)
      .map((field) => field.name);
    const found = await sampleSubject(context, entry, subject, names, scope);
    sampled += found.sampled;
    for (const info of report.fields) {
      if (info.subject !== subject) continue;
      info.filled = found.filled[info.name] ?? 0;
      info.sampled = found.sampled;
    }
    for (const [revision, count] of found.revisions) {
      revisions.set(revision, (revisions.get(revision) ?? 0) + count);
    }
  }
  report.stats = {
    sampled,
    revisions: [...revisions]
      .map(([revision, count]) => ({ revision, count }))
      .sort((a, b) => (a.revision ?? -1) - (b.revision ?? -1)),
  };

  const marks = await pendingComputed(context.db);
  let count = 0;
  for (const info of report.fields) {
    info.pending = marks.byField[`${info.subject}.${info.name}`] ?? 0;
    count += info.pending;
  }
  report.pending = {
    count,
    oldestAgeMs: count > 0 ? marks.oldestAgeMs : null,
  };
  return report;
}

function parseLimit(raw: string | null): number {
  if (raw === null || raw === "") return DRIFT_CHECK_DEFAULT;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > DRIFT_CHECK_MAX) {
    throw new StudioHttpError(
      400,
      `limit must be a whole number from 1 to ${DRIFT_CHECK_MAX}`,
    );
  }
  return value;
}

export async function checkComputedDrift(
  context: StudioContext,
  collectionName: string,
  params: URLSearchParams,
): Promise<ComputedDriftReport> {
  const entry = await requireEntry(context, collectionName);
  const type = params.get("type") || undefined;
  const scope = params.get("scope") || undefined;
  const limit = parseLimit(params.get("limit"));
  const { topology, error } = topologyOf(context);
  if (!topology) {
    throw new StudioHttpError(
      409,
      `The computed fields of this project cannot be checked: ${error ?? "no topology"}`,
    );
  }
  const subjects = subjectsOf(entry, type);
  const fields = fieldsFor(topology, entry, subjects);
  const subject = fields[0]?.subject;
  if (!subject || fields.some((field) => field.subject !== subject)) {
    throw new StudioHttpError(
      400,
      "Choose one type that declares computed fields to check",
    );
  }
  const result = await checkComputed(context.db, topology, {
    subject,
    scope,
    limit,
  });
  const drifted: Record<string, number> = Object.fromEntries(
    fields.map((field) => [field.name, 0]),
  );
  for (const drift of result.drifts) {
    drifted[drift.field] = (drifted[drift.field] ?? 0) + 1;
  }
  return {
    subject,
    checked: result.checked,
    complete: result.complete,
    drifted,
    examples: result.drifts.slice(0, DRIFT_EXAMPLES).map((drift) => ({
      id: typeof drift.id === "string" ? drift.id : drift.id.toHexString(),
      open: encodeId(drift.id),
      field: drift.field,
      stored: drift.stored,
      truth: drift.truth,
      missing: drift.missing,
    })),
  };
}

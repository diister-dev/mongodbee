import type { DatabaseState } from "../migration/types.ts";
import { docsOf } from "../scenario/state.ts";
import type { PrivacyPlan } from "./plan.ts";

export interface ValueJoinEnd {
  readonly target: string;
  readonly path: string;
}

export interface PossibleValueJoin {
  readonly target: string;
  readonly path: string;
  readonly joinsWith: ValueJoinEnd;
  readonly ratio: number;
  readonly distinct: number;
}

export interface ValueJoinOptions {
  readonly maxDistinct?: number;
  readonly minRatio?: number;
}

const DEFAULT_MAX_DISTINCT = 200;
const DEFAULT_MIN_RATIO = 0.8;
const MIN_DISTINCT = 2;
const REPLACED_TREATMENTS: ReadonlySet<string> = new Set(["fake", "pseudonym"]);
const SHARED_TREATMENTS: ReadonlySet<string> = new Set([
  "keep",
  "include",
  "remap",
]);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    !(value instanceof Date)
  );
}

function stringsAt(
  value: unknown,
  segments: readonly string[],
  prefix: readonly string[],
  out: string[],
  recordKeys: Map<string, Set<string>>,
): void {
  if (segments.length === 0) {
    if (typeof value === "string" && value !== "") out.push(value);
    if (isPlainObject(value)) {
      const name = prefix.join(".");
      const keys = recordKeys.get(name) ?? new Set<string>();
      recordKeys.set(name, keys);
      for (const key of Object.keys(value)) keys.add(key);
    }
    return;
  }
  const [head, ...rest] = segments;
  if (head === "*") {
    if (Array.isArray(value)) {
      for (const item of value) {
        stringsAt(item, rest, [...prefix, head], out, recordKeys);
      }
    } else if (isPlainObject(value)) {
      const name = [...prefix, head].join(".");
      const keys = recordKeys.get(name) ?? new Set<string>();
      recordKeys.set(name, keys);
      for (const [key, item] of Object.entries(value)) {
        keys.add(key);
        stringsAt(item, rest, [...prefix, head], out, recordKeys);
      }
    }
    return;
  }
  if (Array.isArray(value) || isPlainObject(value)) {
    stringsAt(
      (value as Record<string, unknown>)[head],
      rest,
      [...prefix, head],
      out,
      recordKeys,
    );
  }
}

interface Pool {
  readonly end: ValueJoinEnd;
  readonly values: Set<string>;
}

interface Candidate {
  readonly end: ValueJoinEnd;
  readonly values: Set<string>;
}

function collect(
  state: DatabaseState,
  plan: PrivacyPlan,
  maxDistinct: number,
): { pools: Pool[]; candidates: Candidate[] } {
  const pools: Pool[] = [];
  const candidates: Candidate[] = [];
  for (const target of plan.targets.values()) {
    const docs = docsOf(state, target);
    if (docs.length === 0) continue;
    const recordKeys = new Map<string, Set<string>>();
    const ids = new Set<string>();
    for (const doc of docs) {
      if (typeof doc._id === "string") ids.add(doc._id);
    }
    pools.push({ end: { target: target.key, path: "_id" }, values: ids });
    for (const path of target.paths) {
      const extract = path.treatment.extract;
      const replaced = REPLACED_TREATMENTS.has(extract);
      if (!replaced && !SHARED_TREATMENTS.has(extract)) continue;
      const found: string[] = [];
      const segments = path.path.split(".");
      for (const doc of docs)
        stringsAt(
          doc[segments[0]],
          segments.slice(1),
          [segments[0]],
          found,
          recordKeys,
        );
      const values = new Set(found);
      if (values.size === 0) continue;
      const end = { target: target.key, path: path.path };
      if (replaced) {
        if (values.size <= maxDistinct) candidates.push({ end, values });
      } else {
        pools.push({ end, values });
      }
    }
    for (const [name, keys] of recordKeys) {
      pools.push({
        end: { target: target.key, path: `${name} (keys)` },
        values: keys,
      });
    }
  }
  return { pools, candidates };
}

export function detectValueJoins(
  state: DatabaseState,
  plan: PrivacyPlan,
  options: ValueJoinOptions = {},
): PossibleValueJoin[] {
  const maxDistinct = options.maxDistinct ?? DEFAULT_MAX_DISTINCT;
  const minRatio = options.minRatio ?? DEFAULT_MIN_RATIO;
  const { pools, candidates } = collect(state, plan, maxDistinct);
  const joins: PossibleValueJoin[] = [];
  for (const candidate of candidates) {
    if (candidate.values.size < MIN_DISTINCT) continue;
    let best: { pool: Pool; ratio: number } | undefined;
    for (const pool of pools) {
      if (
        pool.end.target === candidate.end.target &&
        pool.end.path === candidate.end.path
      ) {
        continue;
      }
      let shared = 0;
      for (const value of candidate.values) {
        if (pool.values.has(value)) shared += 1;
      }
      const ratio = shared / candidate.values.size;
      if (ratio < minRatio) continue;
      if (
        best === undefined ||
        ratio > best.ratio ||
        (ratio === best.ratio && pool.values.size < best.pool.values.size)
      ) {
        best = { pool, ratio };
      }
    }
    if (best !== undefined) {
      joins.push({
        ...candidate.end,
        joinsWith: best.pool.end,
        ratio: Math.round(best.ratio * 100) / 100,
        distinct: candidate.values.size,
      });
    }
  }
  return joins.sort(
    (a, b) => a.target.localeCompare(b.target) || a.path.localeCompare(b.path),
  );
}

export function describeValueJoin(join: PossibleValueJoin): string {
  return `${join.target} ${join.path} looks joined by value to ${join.joinsWith.target} ${join.joinsWith.path} (${Math.round(join.ratio * 100)}% of ${join.distinct} values); declare it notPersonal or a vocabulary if so`;
}

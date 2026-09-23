/**
 * Guarded writes, shared by `collection`, `multiCollection` and the scoped
 * views so the three validate an update — and the document an upsert would
 * create — the same way.
 *
 * @module
 */

import * as v from "./schema.ts";
import { extractFieldsToRemove, sanitizeForMongoDB } from "./sanitizer.ts";

type AnyObjectSchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

/** Options of `updateWhere`. */
export type GuardedUpdateOptions<P> = {
  /** Per-field `$max`: the stored value only ever moves forward. */
  max?: P;
  /** Insert when nothing matches. */
  upsert?: boolean;
  /** Fields written only when the upsert inserts. */
  setOnInsert?: P;
};

/** Outcome of `updateWhere`. */
export type GuardedWriteResult = {
  matched: number;
  modified: number;
  upsertedId: string | null;
};

/** Update operators whose values are stored field values, validated against the dot-notation schema. */
export const VALUE_OPERATORS = [
  "$set",
  "$setOnInsert",
  "$max",
  "$min",
] as const;

export function isPlainRecord(
  value: unknown,
): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function isOperatorRecord(value: unknown): value is Record<string, unknown> {
  return (
    isPlainRecord(value) &&
    Object.keys(value).some((key) => key.startsWith("$"))
  );
}

/**
 * The equality conditions of a filter — what MongoDB itself copies into the
 * document an upsert inserts. Top-level `$` operators and the `ignored` keys
 * (the view's own injected fields) are left out.
 */
export function filterEqualities(
  filter: Record<string, unknown>,
  ignored: readonly string[] = [],
): Record<string, unknown> {
  const equalities: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(filter)) {
    if (key.startsWith("$") || ignored.includes(key)) continue;
    if (isOperatorRecord(value)) {
      if ("$eq" in value) equalities[key] = value.$eq;
      continue;
    }
    equalities[key] = value;
  }
  return equalities;
}

/** `{ "a.b": 1, c: 2 }` → `{ a: { b: 1 }, c: 2 }`, merging into existing objects. */
export function expandDottedPaths(
  flat: Record<string, unknown>,
): Record<string, unknown> {
  const root: Record<string, unknown> = {};
  for (const [path, value] of Object.entries(flat)) {
    const segments = path.split(".");
    let node = root;
    for (const segment of segments.slice(0, -1)) {
      if (!isPlainRecord(node[segment])) node[segment] = {};
      node = node[segment] as Record<string, unknown>;
    }
    node[segments[segments.length - 1]] = value;
  }
  return root;
}

/**
 * The leaves of `doc` that no written path covers, as dotted paths. A parent
 * whose child is written is descended into rather than dropped, so the insert
 * still gets its sibling fields.
 */
export function leavesNotWritten(
  doc: Record<string, unknown>,
  written: readonly string[],
  prefix = "",
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(doc)) {
    const path = prefix === "" ? key : `${prefix}.${key}`;
    if (written.includes(path)) continue;
    if (written.some((w) => w.startsWith(`${path}.`))) {
      if (isPlainRecord(value))
        Object.assign(out, leavesNotWritten(value, written, path));
      continue;
    }
    out[path] = value;
  }
  return out;
}

/**
 * The `$set` / `$unset` / `$max` of an `updateWhere`, each validated against
 * the dot-notation schema. A field cannot be both written and bounded by
 * `max` in one write.
 */
export function guardedUpdateOps(
  operation: string,
  dotSchema: AnyObjectSchema,
  doc: Record<string, unknown>,
  max: Record<string, unknown> | undefined,
): {
  ops: Record<string, unknown>;
  set: Record<string, unknown>;
  written: string[];
} {
  const { set, unset } = extractFieldsToRemove(doc);
  if (Object.keys(set).length > 0) v.parse(dotSchema, set);
  const ops: Record<string, unknown> = {};
  if (Object.keys(set).length > 0) ops.$set = sanitize(set);
  if (Object.keys(unset).length > 0) ops.$unset = unset;
  const written = [...Object.keys(set), ...Object.keys(unset)];
  if (max && Object.keys(max).length > 0) {
    for (const key of Object.keys(max)) {
      if (written.includes(key)) {
        throw new Error(
          `${operation}: "${key}" cannot be both set and bounded by max in one write`,
        );
      }
    }
    v.parse(dotSchema, max);
    ops.$max = sanitize(max);
    written.push(...Object.keys(max));
  }
  return { ops, set, written };
}

/**
 * The `$setOnInsert` an upsert needs: the whole document the insert would
 * create is parsed with `insertSchema` first — so an upsert never mints an
 * invalid document — then every path already written by the filter
 * equalities or another operator is left out (MongoDB refuses two operators
 * on one path). `injected` is merged into the candidate (a scope, a minted
 * `_id`) and `ignored` names fields the caller's filter carries on its own.
 */
export function upsertInsertFields(input: {
  insertSchema: AnyObjectSchema;
  filter: Record<string, unknown>;
  values: Record<string, unknown>;
  setOnInsert?: Record<string, unknown>;
  written: readonly string[];
  injected?: (candidate: Record<string, unknown>) => Record<string, unknown>;
  ignored?: readonly string[];
}): Record<string, unknown> {
  const ignored = input.ignored ?? [];
  const equalities = filterEqualities(input.filter, ignored);
  const candidate = expandDottedPaths({
    ...equalities,
    ...input.setOnInsert,
    ...input.values,
  });
  const parsed = v.parse(input.insertSchema, {
    ...candidate,
    ...input.injected?.(candidate),
  }) as Record<string, unknown>;
  const alreadyWritten = [
    ...Object.keys(equalities),
    ...input.written,
    ...ignored,
  ];
  return sanitize(leavesNotWritten(parsed, alreadyWritten));
}

/** The paths an update document writes, across every operator. */
export function writtenPaths(update: Record<string, unknown>): string[] {
  const paths: string[] = [];
  for (const [operator, fields] of Object.entries(update)) {
    if (operator === "$setOnInsert" || !isPlainRecord(fields)) continue;
    paths.push(...Object.keys(fields));
  }
  return paths;
}

/**
 * The values an insert-by-upsert would store for each operator — `$inc` stores
 * its delta, `$push` / `$addToSet` a one-element array, `$currentDate` now.
 */
export function insertedValues(
  update: Record<string, unknown>,
): Record<string, unknown> {
  const values: Record<string, unknown> = {};
  for (const [operator, fields] of Object.entries(update)) {
    if (!isPlainRecord(fields)) continue;
    for (const [path, value] of Object.entries(fields)) {
      switch (operator) {
        case "$set":
        case "$max":
        case "$min":
        case "$inc":
          values[path] = value;
          break;
        case "$push":
        case "$addToSet":
          values[path] =
            isPlainRecord(value) && Array.isArray(value.$each)
              ? value.$each
              : [value];
          break;
        case "$currentDate":
          values[path] = new Date();
          break;
      }
    }
  }
  return values;
}

function sanitize(fields: Record<string, unknown>): Record<string, unknown> {
  return sanitizeForMongoDB(fields, {
    undefinedBehavior: "remove",
    deep: true,
  }) as Record<string, unknown>;
}

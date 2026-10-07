/**
 * Guarded writes, shared by `collection`, `multiCollection` and the scoped
 * views so the three validate an update — and the document an upsert would
 * create — the same way.
 *
 * @module
 */

import * as v from "./schema.ts";
import { sanitizeForMongoDB } from "./sanitizer.ts";
import { refuseComputedWrite } from "./computed-guard.ts";
import {
  assertDisjointPaths,
  checkOperators,
  operatorDocuments,
  parseAgainst,
  refuseOperators,
  schemasAtPath,
  splitUpdate,
} from "./update-operators.ts";

type AnyObjectSchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

/** Options of `updateWhere`. */
export type GuardedUpdateOptions<P> = {
  /** Per-field `$max`: the stored value only ever moves forward. */
  max?: P;
  /** Insert when nothing matches. */
  upsert?: boolean;
  /** Fields written only when the upsert inserts. */
  setOnInsert?: P;
  /**
   * Conditions of the `$[name]` positional paths of the update, e.g.
   * `[{ "c.id": commentId }]` for `"comments.$[c].reactions": push(r)`.
   */
  arrayFilters?: Record<string, unknown>[];
};

/** Options of `findOneAndUpdate`. */
export type GuardedFindOneAndUpdateOptions<P> = GuardedUpdateOptions<P> & {
  /** The document returned: as it was, or as it became (the default). */
  returnDocument?: "before" | "after";
  /** Which match is updated when several do: the first in this order. */
  sort?: Record<string, 1 | -1>;
};

/** Options of a typed `updateOne`. */
export type TypedUpdateOneOptions = {
  /** Conditions of the `$[name]` positional paths of the update. */
  arrayFilters?: Record<string, unknown>[];
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

/** The schemas a typed update is checked against. */
export type UpdateSchemas = {
  /** The document schema, to resolve the field under a sentinel's path. */
  root: AnyObjectSchema;
  /** Its dot-notation schema, for `$set` values. */
  dot: AnyObjectSchema;
};

/**
 * The MongoDB update of a typed update document: `$set` / `$unset` from plain
 * values and `removeField()`, one operator per sentinel (`increment()`,
 * `push()`, ...), and `$max` from the `max` option. Every value is checked
 * against the schema at its path, and no path may be written twice.
 * `inserted` holds the values an upsert's insert would store.
 */
export function guardedUpdateOps(
  operation: string,
  schemas: UpdateSchemas,
  doc: Record<string, unknown>,
  max: Record<string, unknown> | undefined,
  undefinedBehavior: "remove" | "ignore" | "error" = "remove",
): {
  ops: Record<string, unknown>;
  set: Record<string, unknown>;
  written: string[];
  inserted: Record<string, unknown>;
} {
  const { set, unset, operators } = splitUpdate(doc);
  refuseOperators(operation, max);
  if (max && Object.keys(max).length > 0) {
    const bounded = (operators.$max ??= {});
    for (const [key, value] of Object.entries(max)) {
      if (
        key in set ||
        key in unset ||
        Object.values(operators).some((fields) => key in fields)
      ) {
        throw new Error(
          `${operation}: "${key}" cannot be both set and bounded by max in one write`,
        );
      }
      bounded[key] = value;
    }
  }
  if (Object.keys(set).length > 0) v.parse(schemas.dot, set);
  checkPositionalValues(schemas.root, set);
  checkOperators(operation, schemas.root, operators);
  const clean = (fields: Record<string, unknown>) =>
    sanitizeForMongoDB(fields, {
      undefinedBehavior,
      deep: true,
    }) as Record<string, unknown>;
  const ops: Record<string, unknown> = {};
  const sanitizedSet = clean(set);
  if (Object.keys(sanitizedSet).length > 0) ops.$set = sanitizedSet;
  if (Object.keys(unset).length > 0) ops.$unset = unset;
  Object.assign(ops, operatorDocuments(operators, clean));
  refuseComputedWrite(ops);
  const written = writtenPaths(ops);
  assertDisjointPaths(operation, written);
  const inserted = Object.fromEntries(
    Object.entries(insertedValues(ops)).filter(
      ([path]) => !POSITIONAL.test(path),
    ),
  );
  return { ops, set, written, inserted };
}

// `$[]` is resolved by the dot-notation schema; `$` and `$[name]` are not.
const POSITIONAL = /(^|\.)\$(\[[^\]]+\])?(\.|$)/;

/**
 * Checks the values written at positional paths (`"a.$.b"`,
 * `"a.$[x].b"`) against the array item's schema, which the dot-notation
 * schema does not resolve.
 */
export function checkPositionalValues(
  root: AnyObjectSchema,
  fields: Record<string, unknown>,
): void {
  for (const [path, value] of Object.entries(fields)) {
    if (!POSITIONAL.test(path)) continue;
    const nodes = schemasAtPath(root, path);
    if (nodes.length > 0) parseAgainst(nodes, value);
  }
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
  refuseOperators("setOnInsert", input.setOnInsert);
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

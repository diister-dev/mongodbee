import { extractIndexes } from "../indexes.ts";
import type {
  DatabaseState,
  SchemasDefinition,
  TypeSource,
} from "../migration/types.ts";
import * as v from "../schema.ts";
import { fieldsOf, indexesOf } from "../type-definition.ts";

export type PartialFilter = Readonly<Record<string, unknown>>;

export interface UniqueKey {
  readonly paths: readonly string[];
  readonly global: boolean;
  readonly caseInsensitive: boolean;
  readonly partialFilter?: PartialFilter;
}

export interface UniqueMembership {
  readonly unique: boolean;
  readonly composite: boolean;
  readonly caseInsensitive: boolean;
}

export interface UniqueTarget {
  readonly bucket: keyof DatabaseState;
  readonly collection: string;
  readonly type?: string;
}

const CASE_INSENSITIVE_STRENGTH = 2;
const DEFAULT_STRENGTH = 3;

const NOT_UNIQUE: UniqueMembership = {
  unique: false,
  composite: false,
  caseInsensitive: false,
};

function isCaseInsensitive(
  collation: { strength?: number } | undefined,
  insensitive: boolean | undefined,
): boolean {
  const strength =
    collation?.strength ??
    (insensitive ? CASE_INSENSITIVE_STRENGTH : DEFAULT_STRENGTH);
  return strength <= CASE_INSENSITIVE_STRENGTH;
}

const cache = new WeakMap<object, readonly UniqueKey[]>();

export function uniqueKeysOf(source: TypeSource): readonly UniqueKey[] {
  const cached = cache.get(source);
  if (cached) return cached;
  const keys: UniqueKey[] = [];
  const fields = fieldsOf(source);
  for (const { path, metadata } of extractIndexes(
    v.object(fields as v.ObjectEntries),
  )) {
    if (metadata.unique !== true) continue;
    keys.push({
      paths: [path],
      global: metadata.global === true,
      caseInsensitive: isCaseInsensitive(
        metadata.collation,
        metadata.insensitive,
      ),
      ...(metadata.partialFilterExpression !== undefined && {
        partialFilter: metadata.partialFilterExpression,
      }),
    });
  }
  for (const index of indexesOf(source)) {
    if (index.unique !== true) continue;
    keys.push({
      paths: Object.keys(index.key),
      global: index.global === true,
      caseInsensitive: isCaseInsensitive(index.collation, index.insensitive),
      ...(index.partialFilterExpression !== undefined && {
        partialFilter: index.partialFilterExpression,
      }),
    });
  }
  cache.set(source, keys);
  return keys;
}

export function sourceOfTarget(
  schemas: SchemasDefinition,
  target: UniqueTarget,
): TypeSource | undefined {
  switch (target.bucket) {
    case "collections":
      return schemas.collections?.[target.collection];
    case "multiCollections":
      return schemas.multiCollections?.[target.collection]?.[target.type ?? ""];
    case "multiModels":
      return schemas.multiModels?.[target.collection]?.[target.type ?? ""];
    case "scopedMultiCollections":
      return schemas.scopedMultiCollections?.[target.collection]?.types[
        target.type ?? ""
      ];
  }
}

export function uniqueKeysOfTarget(
  schemas: SchemasDefinition,
  target: UniqueTarget,
): readonly UniqueKey[] {
  const source = sourceOfTarget(schemas, target);
  return source === undefined ? [] : uniqueKeysOf(source);
}

export function indexPath(path: string): string {
  return path
    .split(".")
    .filter((segment) => segment !== "*")
    .join(".");
}

export function uniqueMembership(
  keys: readonly UniqueKey[],
  path: string,
): UniqueMembership {
  const at = indexPath(path);
  const containing = keys.filter((key) => key.paths.includes(at));
  if (containing.length === 0) return NOT_UNIQUE;
  return {
    unique: true,
    composite: containing.some((key) => key.paths.length > 1),
    caseInsensitive: containing.some((key) => key.caseInsensitive),
  };
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    !(value instanceof Date)
  );
}

export function valuesAt(doc: unknown, path: string): unknown[] {
  let current: unknown[] = [doc];
  for (const segment of path.split(".")) {
    const next: unknown[] = [];
    for (const value of current) {
      const items = Array.isArray(value) ? value : [value];
      for (const item of items) {
        if (isPlainObject(item) && Object.hasOwn(item, segment)) {
          next.push(item[segment]);
        }
      }
    }
    current = next;
  }
  return current.flatMap((value) => (Array.isArray(value) ? value : [value]));
}

function sameValue(a: unknown, b: unknown): boolean {
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }
  return JSON.stringify(a) === JSON.stringify(b);
}

function matchCondition(
  values: unknown[],
  condition: unknown,
): boolean | undefined {
  if (
    !isPlainObject(condition) ||
    !Object.keys(condition).some((k) => k.startsWith("$"))
  ) {
    return values.some((value) => sameValue(value, condition));
  }
  let all = true;
  for (const [operator, operand] of Object.entries(condition)) {
    let matched: boolean;
    switch (operator) {
      case "$exists":
        matched = values.length > 0 === Boolean(operand);
        break;
      case "$eq":
        matched = values.some((value) => sameValue(value, operand));
        break;
      case "$ne":
        matched = !values.some((value) => sameValue(value, operand));
        break;
      case "$in":
        if (!Array.isArray(operand)) return undefined;
        matched = values.some((value) =>
          operand.some((o) => sameValue(value, o)),
        );
        break;
      default:
        return undefined;
    }
    all &&= matched;
  }
  return all;
}

export function matchesPartialFilter(
  filter: PartialFilter,
  doc: Record<string, unknown>,
): boolean | undefined {
  let all = true;
  for (const [field, condition] of Object.entries(filter)) {
    let matched: boolean | undefined;
    if (field === "$and") {
      if (!Array.isArray(condition)) return undefined;
      matched = true;
      for (const part of condition) {
        if (!isPlainObject(part)) return undefined;
        const r = matchesPartialFilter(part, doc);
        if (r === undefined) return undefined;
        matched &&= r;
      }
    } else if (field.startsWith("$")) {
      return undefined;
    } else {
      matched = matchCondition(valuesAt(doc, field), condition);
    }
    if (matched === undefined) return undefined;
    all &&= matched;
  }
  return all;
}

export interface UniquePartition {
  readonly instance: string;
  readonly scope: string;
}

export type UniqueEntries =
  | { readonly covered: true; readonly entries: readonly string[] }
  | { readonly covered: false }
  | { readonly covered: undefined };

function foldValue(value: unknown, caseInsensitive: boolean): string {
  if (value === undefined || value === null) return "null";
  if (value instanceof Date) return JSON.stringify(value.toISOString());
  if (typeof value === "string" && caseInsensitive) {
    return JSON.stringify(value.toLowerCase());
  }
  return JSON.stringify(value) ?? String(value);
}

export function uniqueEntriesOf(
  key: UniqueKey,
  doc: Record<string, unknown>,
  partition: UniquePartition,
): UniqueEntries {
  if (key.partialFilter !== undefined) {
    const covered = matchesPartialFilter(key.partialFilter, doc);
    if (covered !== true) {
      return covered === false ? { covered: false } : { covered: undefined };
    }
  }
  const scope = key.global ? "" : partition.scope;
  let tuples: string[][] = [[]];
  for (const path of key.paths) {
    const values = valuesAt(doc, path);
    const folded = (values.length === 0 ? [null] : values).map((value) =>
      foldValue(value, key.caseInsensitive),
    );
    tuples = tuples.flatMap((tuple) => folded.map((f) => [...tuple, f]));
  }
  const prefix = `${key.paths.join(",")}|${partition.instance}|${scope}|`;
  return {
    covered: true,
    entries: [...new Set(tuples.map((tuple) => prefix + tuple.join("|")))],
  };
}

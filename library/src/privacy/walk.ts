import * as v from "../schema.ts";
import type { SchemaContent } from "../migration/types.ts";
import { COMPUTED_ROOT } from "../computed-guard.ts";
import { readPrivacyMetadata } from "./metadata.ts";

export const KEEP: unique symbol = Symbol("mongodbee.privacy.keep");
export const DROP: unique symbol = Symbol("mongodbee.privacy.drop");

export interface WalkLeaf {
  readonly path: string;
  readonly keys: readonly string[];
  readonly key: string;
  readonly value: unknown;
  readonly schema: unknown;
  readonly optional: boolean;
  readonly nullable: boolean;
  readonly doc: Record<string, unknown>;
}

export type WalkHandler = (leaf: WalkLeaf) => unknown;

export type WalkNoteKind = "unknown_key" | "no_variant";

export interface WalkNote {
  readonly path: string;
  readonly kind: WalkNoteKind;
}

export interface WalkResult {
  readonly doc: Record<string, unknown>;
  readonly notes: readonly WalkNote[];
}

export interface WalkKey {
  readonly path: string;
  readonly keys: readonly string[];
  readonly key: string;
  readonly schema: unknown;
}

export interface WalkOptions {
  readonly passthrough?: readonly string[];
  readonly mapKey?: (key: WalkKey) => string;
}

const WRAPPER_TYPES: ReadonlySet<string> = new Set([
  "optional",
  "nullable",
  "nullish",
  "non_optional",
  "non_nullable",
  "non_nullish",
  "undefinedable",
  "exact_optional",
]);

const OPTIONAL_WRAPPERS: ReadonlySet<string> = new Set([
  "optional",
  "nullish",
  "undefinedable",
  "exact_optional",
]);

const NULLABLE_WRAPPERS: ReadonlySet<string> = new Set(["nullable", "nullish"]);

const PLAIN_OBJECT_TYPES: ReadonlySet<string> = new Set([
  "object",
  "loose_object",
  "strict_object",
]);

const OBJECT_TYPES: ReadonlySet<string> = new Set([
  "object",
  "loose_object",
  "strict_object",
  "object_with_rest",
]);

const TUPLE_TYPES: ReadonlySet<string> = new Set([
  "tuple",
  "loose_tuple",
  "strict_tuple",
  "tuple_with_rest",
]);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function unwrap(schema: unknown): {
  schema: Record<string, unknown>;
  optional: boolean;
  nullable: boolean;
} {
  let current = schema as Record<string, unknown>;
  const chain: string[] = [];
  while (current && WRAPPER_TYPES.has(current.type as string)) {
    chain.push(current.type as string);
    current = current.wrapped as Record<string, unknown>;
  }
  let optional = false;
  let nullable = false;
  for (const type of chain.reverse()) {
    if (OPTIONAL_WRAPPERS.has(type)) optional = true;
    if (NULLABLE_WRAPPERS.has(type)) nullable = true;
    if (type === "non_optional" || type === "non_nullish") optional = false;
    if (type === "non_nullable" || type === "non_nullish") nullable = false;
  }
  return { schema: current, optional, nullable };
}

function mergeIntersect(
  schema: Record<string, unknown>,
): Record<string, unknown> | undefined {
  const entries: Record<string, unknown> = {};
  for (const option of schema.options as unknown[]) {
    const inner = unwrap(option).schema;
    if (!inner || !PLAIN_OBJECT_TYPES.has(inner.type as string)) {
      return undefined;
    }
    for (const [key, entry] of Object.entries(
      inner.entries as Record<string, unknown>,
    )) {
      const existing = entries[key];
      if (
        existing === undefined ||
        readPrivacyMetadata(existing).length === 0
      ) {
        entries[key] = entry;
      }
    }
  }
  return { type: "object", entries };
}

export function walkDocument(
  fields: SchemaContent,
  doc: Record<string, unknown>,
  handler: WalkHandler,
  options: WalkOptions = {},
): WalkResult {
  const notes: WalkNote[] = [];
  const passthrough = new Set(
    options.passthrough ?? ["_id", "_scope", "_type"],
  );

  const mapKey = (
    path: readonly string[],
    keys: readonly string[],
    key: string,
    schema: unknown,
  ): string =>
    options.mapKey
      ? options.mapKey({ path: path.join("."), keys, key, schema })
      : key;

  const visit = (
    rawSchema: unknown,
    value: unknown,
    path: readonly string[],
    keys: readonly string[],
    key: string,
  ): unknown => {
    if (value === undefined) return undefined;
    const { schema, optional, nullable } = unwrap(rawSchema);
    if (value === null || schema === undefined) {
      return value === null
        ? null
        : handler({
            path: path.join("."),
            keys,
            key,
            value,
            schema: rawSchema,
            optional,
            nullable,
            doc,
          });
    }
    const type = schema.type as string;

    const computedRoot = path.length === 1 && path[0] === COMPUTED_ROOT;
    if (
      computedRoot ||
      readPrivacyMetadata(rawSchema).some((m) => m.kind !== "dynamic")
    ) {
      const r = handler({
        path: path.join("."),
        keys,
        key,
        value,
        schema,
        optional,
        nullable,
        doc,
      });
      return r === KEEP ? value : r;
    }

    if (OBJECT_TYPES.has(type) && isPlainObject(value)) {
      const entries = schema.entries as Record<string, unknown>;
      const out: Record<string, unknown> = {};
      for (const [k, entry] of Object.entries(value)) {
        const declared = Object.hasOwn(entries, k);
        const entrySchema = declared
          ? entries[k]
          : type === "object_with_rest"
            ? schema.rest
            : undefined;
        if (entrySchema === undefined) {
          notes.push({ path: [...path, "*"].join("."), kind: "unknown_key" });
          continue;
        }
        const r = visit(
          entrySchema,
          entry,
          [...path, declared ? k : "*"],
          [...keys, k],
          k,
        );
        if (r !== DROP && r !== undefined) {
          out[declared ? k : mapKey(path, keys, k, undefined)] = r;
        }
      }
      return out;
    }
    if (type === "array" && Array.isArray(value)) {
      const out: unknown[] = [];
      value.forEach((item, i) => {
        const r = visit(
          schema.item,
          item,
          [...path, "*"],
          [...keys, String(i)],
          key,
        );
        if (r !== DROP && r !== undefined) out.push(r);
      });
      return out;
    }
    if (TUPLE_TYPES.has(type) && Array.isArray(value)) {
      const items = schema.items as unknown[];
      const out: unknown[] = [];
      value.forEach((item, i) => {
        const itemSchema =
          items[i] ?? (type === "tuple_with_rest" ? schema.rest : undefined);
        if (itemSchema === undefined) {
          notes.push({
            path: [...path, String(i)].join("."),
            kind: "unknown_key",
          });
          return;
        }
        const r = visit(
          itemSchema,
          item,
          [...path, i < items.length ? String(i) : "*"],
          [...keys, String(i)],
          String(i),
        );
        if (r !== DROP && r !== undefined) out.push(r);
      });
      return out;
    }
    if (type === "record" && isPlainObject(value)) {
      const out: Record<string, unknown> = {};
      for (const [k, entry] of Object.entries(value)) {
        const r = visit(schema.value, entry, [...path, "*"], [...keys, k], k);
        if (r !== DROP && r !== undefined) {
          out[mapKey(path, keys, k, schema.key)] = r;
        }
      }
      return out;
    }
    if (type === "union" || type === "variant") {
      const options = schema.options as unknown[];
      for (const option of options) {
        if (v.safeParse(option as v.GenericSchema, value).success) {
          return visit(option, value, path, keys, key);
        }
      }
      notes.push({ path: path.join("."), kind: "no_variant" });
      return handler({
        path: path.join("."),
        keys,
        key,
        value,
        schema,
        optional,
        nullable,
        doc,
      });
    }
    if (type === "intersect" && isPlainObject(value)) {
      const merged = mergeIntersect(schema);
      if (merged) return visit(merged, value, path, keys, key);
    }
    if (type === "lazy") {
      const getter = schema.getter as (input: unknown) => unknown;
      return visit(getter(value), value, path, keys, key);
    }
    const r = handler({
      path: path.join("."),
      keys,
      key,
      value,
      schema,
      optional,
      nullable,
      doc,
    });
    return r === KEEP ? value : r;
  };

  const out: Record<string, unknown> = {};
  for (const [k, value] of Object.entries(doc)) {
    const fieldSchema = Object.hasOwn(fields, k) ? fields[k] : undefined;
    if (fieldSchema === undefined) {
      if (passthrough.has(k)) {
        out[k] = value;
      } else {
        notes.push({ path: "*", kind: "unknown_key" });
      }
      continue;
    }
    const r = visit(fieldSchema, value, [k], [k], k);
    if (r !== DROP && r !== undefined) out[k] = r;
  }
  return { doc: out, notes };
}

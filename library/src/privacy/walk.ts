import * as v from "../schema.ts";
import type { SchemaContent } from "../migration/types.ts";
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

export interface WalkOptions {
  readonly passthrough?: readonly string[];
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

function unwrap(
  schema: unknown,
): { schema: Record<string, unknown>; optional: boolean } {
  let current = schema as Record<string, unknown>;
  let optional = false;
  while (current && WRAPPER_TYPES.has(current.type as string)) {
    optional = true;
    current = current.wrapped as Record<string, unknown>;
  }
  return { schema: current, optional };
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

  const visit = (
    rawSchema: unknown,
    value: unknown,
    path: readonly string[],
    keys: readonly string[],
    key: string,
  ): unknown => {
    if (value === undefined) return undefined;
    const { schema, optional } = unwrap(rawSchema);
    if (value === null || schema === undefined) {
      return value === null ? null : handler({
        path: path.join("."),
        keys,
        key,
        value,
        schema: rawSchema,
        optional,
        doc,
      });
    }
    const type = schema.type as string;

    if (readPrivacyMetadata(rawSchema).some((m) => m.kind !== "dynamic")) {
      const r = handler({
        path: path.join("."),
        keys,
        key,
        value,
        schema,
        optional,
        doc,
      });
      return r === KEEP ? value : r;
    }

    if (OBJECT_TYPES.has(type) && isPlainObject(value)) {
      const entries = schema.entries as Record<string, unknown>;
      const out: Record<string, unknown> = {};
      for (const [k, entry] of Object.entries(value)) {
        const entrySchema = entries[k] ??
          (type === "object_with_rest" ? schema.rest : undefined);
        if (entrySchema === undefined) {
          notes.push({ path: [...path, k].join("."), kind: "unknown_key" });
          continue;
        }
        const r = visit(entrySchema, entry, [...path, k], [...keys, k], k);
        if (r !== DROP && r !== undefined) out[k] = r;
      }
      return out;
    }
    if (type === "array" && Array.isArray(value)) {
      const out: unknown[] = [];
      for (const item of value) {
        const r = visit(schema.item, item, [...path, "*"], [
          ...keys,
          String(out.length),
        ], key);
        if (r !== DROP && r !== undefined) out.push(r);
      }
      return out;
    }
    if (TUPLE_TYPES.has(type) && Array.isArray(value)) {
      const items = schema.items as unknown[];
      const out: unknown[] = [];
      value.forEach((item, i) => {
        const itemSchema = items[i] ??
          (type === "tuple_with_rest" ? schema.rest : undefined);
        if (itemSchema === undefined) {
          notes.push({
            path: [...path, String(i)].join("."),
            kind: "unknown_key",
          });
          return;
        }
        const r = visit(itemSchema, item, [...path, String(i)], [
          ...keys,
          String(i),
        ], String(i));
        if (r !== DROP && r !== undefined) out.push(r);
      });
      return out;
    }
    if (type === "record" && isPlainObject(value)) {
      const out: Record<string, unknown> = {};
      for (const [k, entry] of Object.entries(value)) {
        const r = visit(schema.value, entry, [...path, "*"], [...keys, k], k);
        if (r !== DROP && r !== undefined) out[k] = r;
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
        doc,
      });
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
      doc,
    });
    return r === KEEP ? value : r;
  };

  const out: Record<string, unknown> = {};
  for (const [k, value] of Object.entries(doc)) {
    const fieldSchema = fields[k];
    if (fieldSchema === undefined) {
      if (passthrough.has(k)) {
        out[k] = value;
      } else {
        notes.push({ path: k, kind: "unknown_key" });
      }
      continue;
    }
    const r = visit(fieldSchema, value, [k], [k], k);
    if (r !== DROP && r !== undefined) out[k] = r;
  }
  return { doc: out, notes };
}

import * as v from "../schema.ts";
import type { SchemaContent } from "../migration/types.ts";
import { COMPUTED_ROOT } from "../computed-guard.ts";
import { readPrivacyMetadata } from "./metadata.ts";
import {
  OBJECT_TYPES,
  PLAIN_OBJECT_TYPES,
  TUPLE_TYPES,
  unwrap,
  unwrapSchema,
} from "./schema-shape.ts";

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

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function declaredEntries(option: unknown): Record<string, unknown> | undefined {
  return unwrapSchema(option)?.entries as Record<string, unknown> | undefined;
}

function chooseOption(
  schema: Record<string, unknown>,
  value: unknown,
): unknown | undefined {
  let options = schema.options as unknown[];
  if (
    schema.type === "variant" &&
    typeof schema.key === "string" &&
    isPlainObject(value)
  ) {
    const key = schema.key;
    const keyed = options.filter((option) => {
      const entry = declaredEntries(option)?.[key];
      return (
        entry !== undefined &&
        v.safeParse(entry as v.GenericSchema, value[key]).success
      );
    });
    if (keyed.length > 0) options = keyed;
  }
  const matching = options.filter(
    (option) => v.safeParse(option as v.GenericSchema, value).success,
  );
  if (matching.length <= 1 || !isPlainObject(value)) return matching[0];
  const keys = Object.keys(value);
  let best = matching[0];
  let bestCovered = -1;
  for (const option of matching) {
    const entries = declaredEntries(option);
    const covered = entries
      ? keys.filter((k) => Object.hasOwn(entries, k)).length
      : 0;
    if (covered > bestCovered) {
      best = option;
      bestCovered = covered;
    }
  }
  return best;
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

  const lazyEntries = new Map<unknown, readonly string[]>();

  const visit = (
    rawSchema: unknown,
    value: unknown,
    path: readonly string[],
    keys: readonly string[],
    key: string,
  ): unknown => {
    if (value === undefined) return undefined;
    const { schema, optional, nullable } = unwrap(rawSchema);
    if (value === null) return null;
    if (schema === undefined) {
      const r = handler({
        path: path.join("."),
        keys,
        key,
        value,
        schema: rawSchema,
        optional,
        nullable,
        doc,
      });
      return r === KEEP ? value : r;
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
      let kept = 0;
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
        // BSON arrays cannot hold a hole: a dropped slot becomes null so later items keep their position
        out.push(r === DROP || r === undefined ? null : r);
        if (r !== DROP && r !== undefined) kept = out.length;
      });
      return out.slice(0, kept);
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
      const option = chooseOption(schema, value);
      if (option !== undefined) return visit(option, value, path, keys, key);
      notes.push({ path: path.join("."), kind: "no_variant" });
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
    if (type === "intersect" && isPlainObject(value)) {
      const merged = mergeIntersect(schema);
      if (merged) return visit(merged, value, path, keys, key);
    }
    if (type === "lazy") {
      const getter = schema.getter as (input: unknown) => unknown;
      const outer = lazyEntries.get(getter);
      if (outer !== undefined) {
        return visit(getter(value), value, outer, keys, key);
      }
      lazyEntries.set(getter, path);
      try {
        return visit(getter(value), value, path, keys, key);
      } finally {
        lazyEntries.delete(getter);
      }
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

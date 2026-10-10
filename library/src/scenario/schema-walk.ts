import * as v from "../schema.ts";
import type { SchemaContent } from "../migration/types.ts";
import { COMPUTED_ROOT } from "../computed-guard.ts";
import {
  OBJECT_TYPES,
  type SchemaNode,
  TUPLE_TYPES,
  UNION_TYPES,
  unwrap,
} from "../privacy/schema-shape.ts";

const ROOT_PASSTHROUGH: ReadonlySet<string> = new Set([
  "_id",
  "_scope",
  "_type",
  COMPUTED_ROOT,
]);
const MAX_LAZY_DEPTH = 32;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function intersectEntries(
  schema: SchemaNode,
): Record<string, unknown> | undefined {
  const entries: Record<string, unknown> = {};
  for (const option of schema.options as unknown[]) {
    const inner = unwrap(option).schema;
    if (!inner || !OBJECT_TYPES.has(inner.type as string)) return undefined;
    Object.assign(entries, inner.entries as Record<string, unknown>);
  }
  return entries;
}

export function unknownKeyPaths(
  fields: SchemaContent,
  doc: Record<string, unknown>,
): string[] {
  const visit = (
    raw: unknown,
    value: unknown,
    path: string,
    depth: number,
  ): string[] => {
    if (value === undefined || value === null || depth > MAX_LAZY_DEPTH) {
      return [];
    }
    const schema = unwrap(raw).schema;
    if (!schema) return [];
    const type = schema.type as string;
    const at = (key: string) => (path === "" ? key : `${path}.${key}`);
    if (OBJECT_TYPES.has(type) && isPlainObject(value)) {
      const entries = schema.entries as Record<string, unknown>;
      return Object.entries(value).flatMap(([key, entry]) => {
        if (Object.hasOwn(entries, key)) {
          return visit(entries[key], entry, at(key), depth);
        }
        if (type === "object_with_rest") {
          return visit(schema.rest, entry, at(key), depth);
        }
        return type === "loose_object" ? [] : [at(key)];
      });
    }
    if (type === "array" && Array.isArray(value)) {
      return value.flatMap((item, i) =>
        visit(schema.item, item, at(String(i)), depth),
      );
    }
    if (TUPLE_TYPES.has(type) && Array.isArray(value)) {
      const items = schema.items as unknown[];
      return value.flatMap((item, i) => {
        const itemSchema = items[i] ?? schema.rest;
        return itemSchema === undefined
          ? []
          : visit(itemSchema, item, at(String(i)), depth);
      });
    }
    if (type === "record" && isPlainObject(value)) {
      return Object.entries(value).flatMap(([key, entry]) =>
        visit(schema.value, entry, at(key), depth),
      );
    }
    if (UNION_TYPES.has(type)) {
      const candidates = (schema.options as unknown[])
        .filter(
          (option) => v.safeParse(option as v.GenericSchema, value).success,
        )
        .map((option) => visit(option, value, path, depth));
      if (candidates.length === 0) return [];
      return candidates.reduce((best, next) =>
        next.length < best.length ? next : best,
      );
    }
    if (type === "intersect" && isPlainObject(value)) {
      const entries = intersectEntries(schema);
      return entries
        ? visit({ type: "object", entries }, value, path, depth)
        : [];
    }
    if (type === "lazy") {
      const getter = schema.getter as (input: unknown) => unknown;
      return visit(getter(value), value, path, depth + 1);
    }
    return [];
  };
  return Object.entries(doc).flatMap(([key, value]) => {
    if (Object.hasOwn(fields, key)) return visit(fields[key], value, key, 0);
    return ROOT_PASSTHROUGH.has(key) ? [] : [key];
  });
}

interface PathStep {
  readonly declared: boolean;
  readonly required: boolean;
}

function stepThrough(
  raw: unknown,
  segments: readonly string[],
  required: boolean,
  depth: number,
): PathStep {
  const { schema, optional } = unwrap(raw);
  const here = required && !optional;
  if (segments.length === 0) return { declared: true, required: here };
  if (!schema || depth > MAX_LAZY_DEPTH) {
    return { declared: false, required: false };
  }
  const [segment, ...rest] = segments;
  const type = schema.type as string;
  if (OBJECT_TYPES.has(type)) {
    const entries = schema.entries as Record<string, unknown>;
    if (Object.hasOwn(entries, segment)) {
      return stepThrough(entries[segment], rest, here, depth);
    }
    return type === "object_with_rest"
      ? stepThrough(schema.rest, rest, false, depth)
      : { declared: false, required: false };
  }
  const indexed = segment === "*" || /^\d+$/.test(segment);
  if (type === "array" && indexed) {
    return stepThrough(schema.item, rest, false, depth);
  }
  if (type === "record") return stepThrough(schema.value, rest, false, depth);
  if (TUPLE_TYPES.has(type) && indexed) {
    const items = schema.items as unknown[];
    const item = segment === "*" ? items[0] : items[Number(segment)];
    return item === undefined
      ? stepThrough(schema.rest, rest, false, depth)
      : stepThrough(item, rest, here, depth);
  }
  if (UNION_TYPES.has(type) || type === "intersect") {
    const steps = (schema.options as unknown[]).map((option) =>
      stepThrough(option, segments, here, depth),
    );
    const declared = steps.filter((s) => s.declared);
    return {
      declared: declared.length > 0,
      required:
        type === "intersect"
          ? declared.some((s) => s.required)
          : declared.length === steps.length &&
            declared.every((s) => s.required),
    };
  }
  if (type === "lazy") {
    const getter = schema.getter as (input: unknown) => unknown;
    return stepThrough(getter(undefined), segments, here, depth + 1);
  }
  return { declared: false, required: false };
}

function stepAt(fields: SchemaContent, path: string): PathStep {
  const [head, ...rest] = path.split(".");
  if (!Object.hasOwn(fields, head)) return { declared: false, required: false };
  return stepThrough(fields[head], rest, true, 0);
}

export function isDeclaredPath(fields: SchemaContent, path: string): boolean {
  return stepAt(fields, path).declared;
}

export function isRequiredPath(fields: SchemaContent, path: string): boolean {
  return stepAt(fields, path).required;
}

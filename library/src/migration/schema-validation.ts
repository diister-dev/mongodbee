/**
 * @fileoverview Schema validation utilities
 *
 * This module provides functions to validate that the last migration's schema
 * matches the current project schema defined in schemas.ts
 *
 * @module
 */

import * as path from "node:path";
import { INDEX_SYMBOL, indexMetadataOf } from "../indexes.ts";
import { PRIVACY_SYMBOL } from "../privacy/metadata.ts";
import { isTypeDefinition } from "../type-definition.ts";
import type { MigrationDefinition, SchemasDefinition } from "./types.ts";
import { pathToFileUrl } from "./utils/platform.ts";

/**
 * Loads the current project schema from schemas.ts
 *
 * @param schemaPath - Path to the schemas.ts file
 * @returns The loaded schema definition
 */
export async function loadProjectSchema(
  schemaPath: string,
): Promise<SchemasDefinition> {
  const fullPath = path.resolve(schemaPath);

  // Convert to file:// URL for dynamic import
  const importPath = pathToFileUrl(fullPath);

  try {
    const module = await import(importPath);

    if (!module.schemas) {
      throw new Error(`Schema file ${schemaPath} does not export 'schemas'`);
    }

    return module.schemas;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Failed to load project schema from ${schemaPath}: ${message}`,
    );
  }
}

/**
 * Properties to remove from Valibot schemas during simplification
 */
const CLEANUP_PROPERTIES = [
  "~standard",
  "async",
  "expects",
  "message",
  "default",
];

/**
 * Properties to skip in diff output (too verbose/technical for users)
 */
const DIFF_SKIPPED_PROPERTIES = ["kind", ...CLEANUP_PROPERTIES];

const VALUE_PROPERTIES = ["default", "fallback"] as const;
const UNCOMPARABLE = "@uncomparable";
const MAX_LAZY_DEPTH = 64;

type Simplify = (schema: any) => any;
type ExpandLazy = (schema: any) => any;

/**
 * Type-specific handlers for simplifying Valibot schemas. Each receives the
 * recursion to apply to its children, so a memoised simplification reaches
 * the whole tree.
 */
const simplifyHandlers: Record<
  string,
  (schema: any, simplify: Simplify) => any
> = {
  map: (schema, simplify) => ({
    ...schema,
    key: simplify(schema.key),
    value: simplify(schema.value),
  }),
  record: (schema, simplify) => ({
    ...schema,
    key: simplify(schema.key),
    value: simplify(schema.value),
  }),
  set: (schema, simplify) => ({
    ...schema,
    value: simplify(schema.value),
  }),
  object: (schema, simplify) => ({
    ...schema,
    entries: Object.fromEntries(
      Object.entries(schema.entries).map(([key, value]) => [
        key,
        simplify(value),
      ]),
    ),
  }),
  loose_object: (schema, simplify) =>
    simplifyHandlers["object"](schema, simplify),
  object_with_rest: (schema, simplify) => ({
    ...simplifyHandlers["object"](schema, simplify),
    rest: simplify(schema.rest),
  }),
  strict_object: (schema, simplify) =>
    simplifyHandlers["object"](schema, simplify),
  array: (schema, simplify) => ({
    ...schema,
    item: simplify(schema.item),
  }),
  tuple: (schema, simplify) => ({
    ...schema,
    items: schema.items.map((s: any) => simplify(s)),
  }),
  loose_tuple: (schema, simplify) =>
    simplifyHandlers["tuple"](schema, simplify),
  strict_tuple: (schema, simplify) =>
    simplifyHandlers["tuple"](schema, simplify),
  tuple_with_rest: (schema, simplify) => ({
    ...simplifyHandlers["tuple"](schema, simplify),
    rest: simplify(schema.rest),
  }),
  union: (schema, simplify) => ({
    ...schema,
    options: schema.options.map((s: any) => simplify(s)),
  }),
  intersect: (schema, simplify) => ({
    ...schema,
    options: schema.options.map((s: any) => simplify(s)),
  }),
  variant: (schema, simplify) => ({
    ...schema,
    options: schema.options.map((s: any) => simplify(s)),
  }),
  "#wrapped": (schema, simplify) => ({
    ...schema,
    wrapped: simplify(schema.wrapped),
  }),
  optional: (schema, simplify) =>
    simplifyHandlers["#wrapped"](schema, simplify),
  non_optional: (schema, simplify) =>
    simplifyHandlers["#wrapped"](schema, simplify),
  undefinedable: (schema, simplify) =>
    simplifyHandlers["#wrapped"](schema, simplify),
  nullable: (schema, simplify) =>
    simplifyHandlers["#wrapped"](schema, simplify),
  non_nullable: (schema, simplify) =>
    simplifyHandlers["#wrapped"](schema, simplify),
  nullish: (schema, simplify) => simplifyHandlers["#wrapped"](schema, simplify),
  non_nullish: (schema, simplify) =>
    simplifyHandlers["#wrapped"](schema, simplify),
  exact_optional: (schema, simplify) =>
    simplifyHandlers["#wrapped"](schema, simplify),
};

/**
 * Simplifies a Valibot schema by removing internal metadata and recursively
 * processing nested schemas
 *
 * @param schema - Valibot schema object
 * @returns Simplified schema with metadata removed
 */
export function simplifySchema(schema: any): any {
  return createSimplifier()(schema);
}

/**
 * A simplification that returns the same simplified object for every
 * occurrence of the same schema object: a sub-schema shared by many fields
 * is simplified once, and its simplified form is shared too, which lets the
 * schema fingerprint below visit it once.
 */
function createSimplifier(): Simplify {
  const shared = new WeakMap<object, any>();
  const unfolding: Array<{ node: object; getter: unknown }> = [];
  let lazyVisits = 0;

  const expandLazy: ExpandLazy = (node) => {
    lazyVisits++;
    const position = unfolding.findIndex(
      (entry) => entry.node === node || entry.getter === node.getter,
    );
    if (position !== -1) {
      return { type: "lazy", "@cycle": unfolding.length - position };
    }
    if (unfolding.length >= MAX_LAZY_DEPTH) {
      return {
        type: "lazy",
        [UNCOMPARABLE]: `v.lazy unfolds more than ${MAX_LAZY_DEPTH} levels without returning a schema it already returned; make the getter return one shared schema object`,
      };
    }
    let resolved: unknown;
    try {
      resolved = node.getter(undefined);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      return {
        type: "lazy",
        [UNCOMPARABLE]: `v.lazy getter fails without an input (${reason}); the comparison resolves it without one, so it must not read its input`,
      };
    }
    unfolding.push({ node, getter: node.getter });
    try {
      return { type: "lazy", "@resolved": simplify(resolved) };
    } finally {
      unfolding.pop();
    }
  };

  const simplify: Simplify = (schema) => {
    if (!schema || typeof schema !== "object") return schema;
    if (shared.has(schema)) return shared.get(schema);
    const lazyVisitsBefore = lazyVisits;
    const result = simplifyNode(schema, simplify, expandLazy);
    if (lazyVisits === lazyVisitsBefore) shared.set(schema, result);
    return result;
  };
  return simplify;
}

function isValibotNode(schema: any): boolean {
  return typeof schema.kind === "string" && typeof schema.type === "string";
}

function simplifyNode(
  schema: any,
  simplify: Simplify,
  expandLazy: ExpandLazy,
): any {
  if (!schema || typeof schema !== "object") {
    return schema;
  }

  if (isTypeDefinition(schema)) {
    const simplified = simplify(schema.schema.entries);
    return schema.indexes.length > 0
      ? { ...simplified, "@indexes": schema.indexes }
      : simplified;
  }

  if (!isValibotNode(schema)) {
    return simplifyEntries(schema, simplify);
  }

  // Index metadata (withIndex) drives real MongoDB indexes (unique, TTL,
  // collation, partial filters) and privacy metadata drives erasure, so both
  // survive; other metadata (title, description, i18n) is noise.
  if (schema.kind === "metadata") {
    const kept: Record<PropertyKey, unknown> = {};
    const indexMetadata = indexMetadataOf(schema.metadata);
    if (indexMetadata !== undefined) kept[INDEX_SYMBOL] = indexMetadata;
    const privacyMetadata = schema.metadata?.[PRIVACY_SYMBOL];
    if (privacyMetadata !== undefined) kept[PRIVACY_SYMBOL] = privacyMetadata;
    if (Object.getOwnPropertySymbols(kept).length > 0) {
      return { kind: schema.kind, type: schema.type, metadata: kept };
    }
    return undefined;
  }

  if (schema.type === "lazy" && typeof schema.getter === "function") {
    return expandLazy(schema);
  }

  const simplified = { ...schema };

  for (const prop of CLEANUP_PROPERTIES) {
    delete simplified[prop];
  }

  for (const prop of VALUE_PROPERTIES) {
    if (schema[prop] === undefined) continue;
    delete simplified[prop];
    const value = valueIdentity(schema[prop]);
    simplified[`@${prop}`] = value.identity;
    if (value.problem !== undefined) {
      simplified[UNCOMPARABLE] = `the ${prop} ${value.problem}`;
    }
  }

  if (schema.requirement instanceof RegExp) {
    simplified.requirement = String(schema.requirement);
  } else if (schema.requirement instanceof Date) {
    simplified.requirement = valueIdentity(schema.requirement).identity;
  }

  const code = opaqueCode(schema);
  if (code !== null) {
    if (code.held !== "closure") delete simplified[code.held];
    const named = code.held !== "closure" && typeof schema.message === "string";
    simplified["@check"] = named ? schema.message : null;
    if (!named) simplified[UNCOMPARABLE] = unnamedCheckReason(schema.type);
  }

  if (typeof schema.class === "function") {
    simplified.class = schema.class.name;
  }

  if ("pipe" in simplified && Array.isArray(simplified.pipe)) {
    simplified.pipe = simplified.pipe
      .map((s: any) => simplify(s))
      .filter((s: any) => s !== undefined);
  }

  const handler = simplifyHandlers[schema.type];
  if (handler) {
    return handler(simplified, simplify);
  }

  return simplified;
}

function simplifyEntries(entries: any, simplify: Simplify): any {
  const simplified = { ...entries };
  for (const key of Object.keys(entries)) {
    const value = entries[key];
    if (Array.isArray(value)) simplified[key] = value.map(simplify);
    else if (value && typeof value === "object") {
      simplified[key] = simplify(value);
    }
  }
  return simplified;
}

type OpaqueCode = { held: "requirement" | "check" } | { held: "closure" };

const USER_CODE_REQUIREMENTS = new Set([
  "check",
  "partial_check",
  "check_items",
  "some_item",
  "every_item",
  "guard",
]);

function opaqueCode(schema: any): OpaqueCode | null {
  if (
    USER_CODE_REQUIREMENTS.has(schema.type) &&
    typeof schema.requirement === "function"
  ) {
    return { held: "requirement" };
  }
  if (schema.type === "custom" && typeof schema.check === "function") {
    return { held: "check" };
  }
  if (schema.type === "raw_check") return { held: "closure" };
  return null;
}

function unnamedCheckReason(type: string): string {
  const api = `v.${type.replace(/_(\w)/g, (_, letter) => letter.toUpperCase())}`;
  return type === "raw_check"
    ? `${api} takes no message, so its code cannot be compared with the migration chain; use v.check or v.partialCheck with a message`
    : `${api} has no message; its code cannot be compared with the migration chain, so its message is its identity: give it one, and change it when the rule changes`;
}

type ValueIdentity = string | number | boolean | null | Record<string, unknown>;

function valueIdentity(value: unknown): {
  identity: ValueIdentity;
  problem?: string;
} {
  if (typeof value === "function") {
    return value.name
      ? { identity: { "@function": value.name } }
      : {
          identity: { "@function": null },
          problem:
            "is an anonymous function; name it (const now = () => new Date()), its name is its identity in the migration chain",
        };
  }
  try {
    const canonical = canonicalValue(value);
    return {
      identity:
        typeof canonical === "object" && canonical !== null
          ? { "@json": JSON.stringify(canonical) }
          : (canonical as ValueIdentity),
    };
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    return {
      identity: { "@unserialisable": true },
      problem: `cannot be serialised for comparison (${reason})`,
    };
  }
}

function canonicalValue(value: unknown): unknown {
  switch (typeof value) {
    case "string":
    case "boolean":
      return value;
    case "number":
      return Number.isFinite(value) ? value : { $number: String(value) };
    case "bigint":
      return { $bigint: String(value) };
    case "undefined":
      return { $undefined: true };
  }
  if (value === null) return null;
  if (value instanceof Date) return { $date: value.toISOString() };
  if (value instanceof RegExp) return { $regexp: String(value) };
  if (Array.isArray(value)) return Array.from(value, canonicalValue);
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError(
      `a ${prototype?.constructor?.name ?? "class"} instance has no canonical form`,
    );
  }
  const record = value as Record<string, unknown>;
  return Object.fromEntries(
    Object.keys(record)
      .sort()
      .map((key) => [key, canonicalValue(record[key])]),
  );
}

function findUncomparable(simplified: any): string[] {
  const found: string[] = [];
  const visited = new WeakSet<object>();
  const queue: Array<{ node: any; path: string }> = [
    { node: simplified, path: "" },
  ];
  for (const { node, path } of queue) {
    if (!node || typeof node !== "object" || visited.has(node)) continue;
    visited.add(node);
    if (typeof node[UNCOMPARABLE] === "string") {
      found.push(`${path}: ${node[UNCOMPARABLE]}`);
    }
    for (const key of Object.keys(node)) {
      const value = node[key];
      const childPath = path ? `${path}.${key}` : key;
      if (Array.isArray(value)) {
        for (const [index, item] of value.entries()) {
          queue.push({ node: item, path: `${childPath}[${index}]` });
        }
      } else {
        queue.push({ node: value, path: childPath });
      }
    }
  }
  return found;
}

/**
 * Flattens a simplified schema into dot-notation keys
 *
 * One pass over the tree that writes every key straight into the result:
 * building each level's own result and copying it into its parent made the
 * cost grow with the number of keys times their depth.
 *
 * @param schema - Simplified schema object
 * @returns Flattened object with dot-notation paths as keys
 */
export function flattenSchema(schema: any): Record<string, any> {
  const result: Record<string, any> = {};
  flattenInto(result, schema, "", NOT_FILTERED);
  return result;
}

const NOT_FILTERED = -1;
const STANDARD_KEY = "~standard";
const NESTED_STANDARD = ".~standard";

/**
 * Writes the keys of `schema` under `prefix`. A key nested below the top
 * level is dropped when the part of its path below its top-level key holds
 * `.~standard` (or is `~standard`): `filteredFrom` is where that part starts,
 * or `NOT_FILTERED` above it.
 */
function flattenInto(
  result: Record<string, any>,
  schema: any,
  prefix: string,
  filteredFrom: number,
): void {
  if (!schema || typeof schema !== "object") {
    return;
  }

  if (isTypeDefinition(schema)) {
    flattenInto(result, simplifySchema(schema), prefix, filteredFrom);
    return;
  }

  const keys = Object.keys(schema);
  // Engines list integer-like keys first: in the per-level objects the
  // previous implementation built, a leaf under such a key came before the
  // metadata entries, and the order of the result keeps that.
  let hoisted = 0;
  while (hoisted < keys.length && isArrayIndex(keys[hoisted])) {
    const key = keys[hoisted++];
    if (isLeafValue(schema[key])) {
      write(result, `${prefix}${key}`, schema[key], filteredFrom);
    }
  }

  // Symbol-keyed index metadata (withIndex) is invisible to Object.keys and
  // JSON.stringify — surface it under a stable string key so schema diffs
  // can see index changes (unique/TTL/collation/partialFilter/global).
  const indexMetadata = (schema as Record<PropertyKey, any>)[INDEX_SYMBOL];
  if (indexMetadata !== undefined) {
    write(result, `${prefix}@index`, indexMetadata, filteredFrom);
  }
  const privacyMetadata = (schema as Record<PropertyKey, any>)[PRIVACY_SYMBOL];
  if (privacyMetadata !== undefined) {
    flattenInto(result, privacyMetadata, `${prefix}@privacy.`, filteredFrom);
  }

  for (const [position, key] of keys.entries()) {
    // Skip ~standard properties as they add noise
    if (key === STANDARD_KEY || key.endsWith(NESTED_STANDARD)) {
      continue;
    }

    const value = schema[key];
    if (position < hoisted && isLeafValue(value)) {
      continue;
    }

    if (Array.isArray(value)) {
      for (const [index, subValue] of value.entries()) {
        const path = `${prefix}${key}[${index}]`;
        if (typeof subValue === "object" && subValue !== null) {
          const childPrefix = `${path}.`;
          flattenInto(
            result,
            subValue,
            childPrefix,
            filteredFrom === NOT_FILTERED ? childPrefix.length : filteredFrom,
          );
        } else {
          write(result, path, subValue, filteredFrom);
        }
      }
    } else if (typeof value === "object" && value !== null) {
      const nested = isTypeDefinition(value) ? simplifySchema(value) : value;
      const childPrefix = `${prefix}${key}.`;
      flattenInto(
        result,
        nested,
        childPrefix,
        filteredFrom === NOT_FILTERED ? childPrefix.length : filteredFrom,
      );
    } else {
      write(result, `${prefix}${key}`, value, filteredFrom);
    }
  }
}

const MAX_ARRAY_INDEX = 2 ** 32 - 2;

function isArrayIndex(key: string): boolean {
  return /^(?:0|[1-9]\d*)$/.test(key) && Number(key) <= MAX_ARRAY_INDEX;
}

function isLeafValue(value: unknown): boolean {
  return typeof value !== "object" || value === null;
}

function write(
  result: Record<string, any>,
  path: string,
  value: unknown,
  filteredFrom: number,
): void {
  if (
    filteredFrom !== NOT_FILTERED &&
    (path.indexOf(NESTED_STANDARD, filteredFrom) !== -1 ||
      (path.length - filteredFrom === STANDARD_KEY.length &&
        path.endsWith(STANDARD_KEY)))
  ) {
    return;
  }
  result[path] = value;
}

/**
 * Compares two flattened schemas and returns differences
 *
 * @param schema1 - First flattened schema
 * @param schema2 - Second flattened schema
 * @returns Array of differences with key and before/after values
 */
export function diffSchemas(
  schema1: Record<string, any>,
  schema2: Record<string, any>,
): Array<{ key: string; before?: any; after?: any }> {
  const diffs: Array<{ key: string; before?: any; after?: any }> = [];
  const keys = new Set([...Object.keys(schema1), ...Object.keys(schema2)]);

  for (const key of keys) {
    // Skip if key matches or ends with any skip property
    const shouldSkip = DIFF_SKIPPED_PROPERTIES.some(
      (prop) => key === prop || key.endsWith(`.${prop}`),
    );

    if (shouldSkip) {
      continue;
    }

    const valA = schema1[key];
    const valB = schema2[key];

    // Use JSON.stringify for deep comparison
    if (JSON.stringify(valA) !== JSON.stringify(valB)) {
      diffs.push({
        key,
        ...(key in schema1 ? { before: valA } : {}),
        ...(key in schema2 ? { after: valB } : {}),
      });
    }
  }

  return diffs;
}

/**
 * Compares schemas for one validation run.
 *
 * Each distinct schema object gets a fingerprint, interned to a number, that
 * encodes everything `flattenSchema` reads from it after simplification and
 * `diffSchemas` then compares: its keys (sorted when their order cannot
 * matter), its index and privacy metadata, and its leaves under a typed
 * encoding at least as strict as the `JSON.stringify` comparison of
 * `diffSchemas`, minus the leaves it skips. Equal fingerprints therefore
 * mean flattened schemas `diffSchemas` finds no difference between.
 * A sub-schema shared by many fields is fingerprinted once, so the cost
 * follows the number of distinct schema objects, not the size of the
 * expanded tree. Unequal fingerprints settle nothing: those schemas, and any
 * value the encoding cannot read, go through the full flatten-and-diff.
 */
export interface SchemaComparer {
  equal(schema1: any, schema2: any): boolean;
  uncomparable(schema: any): string[];
  /** Schema objects fingerprinted so far, the unit of the comparison's work. */
  readonly fingerprintedNodes: number;
}

const EMPTY_FLATTENED = -1;

export function createSchemaComparer(): SchemaComparer {
  const simplify = createSimplifier();
  const interned = new Map<string, number>();
  const fingerprints = new WeakMap<object, number>();
  let fingerprintedNodes = 0;

  const intern = (encoding: string): number => {
    let id = interned.get(encoding);
    if (id === undefined) {
      id = interned.size;
      interned.set(encoding, id);
    }
    return id;
  };

  const element = (value: unknown): string =>
    typeof value === "object" && value !== null
      ? `o${fingerprint(value)}`
      : `l${leaf(value)}`;

  const fingerprint = (node: object): number => {
    const known = fingerprints.get(node);
    if (known !== undefined) return known;
    fingerprintedNodes++;

    if (isTypeDefinition(node)) {
      const simplified = simplify(node);
      const id =
        typeof simplified === "object" && simplified !== null
          ? fingerprint(simplified)
          : EMPTY_FLATTENED;
      fingerprints.set(node, id);
      return id;
    }

    const parts: string[] = [];
    const indexMetadata = (node as Record<PropertyKey, any>)[INDEX_SYMBOL];
    if (indexMetadata !== undefined) {
      parts.push(`i${JSON.stringify(String(JSON.stringify(indexMetadata)))}`);
    }
    const privacyMetadata = (node as Record<PropertyKey, any>)[PRIVACY_SYMBOL];
    if (typeof privacyMetadata === "object" && privacyMetadata !== null) {
      parts.push(`p${fingerprint(privacyMetadata)}`);
    }
    const keys = Object.keys(node);
    const orderFree = keys.every(cannotCollide);
    const entries: string[] = [];
    for (const key of keys) {
      const value = (node as Record<string, unknown>)[key];
      if (orderFree && isSkippedLeaf(key, value)) continue;
      const encoded = Array.isArray(value)
        ? `[${Array.from(value, element).join(",")}]`
        : element(value);
      entries.push(`${JSON.stringify(key)}:${encoded}`);
    }
    if (orderFree) entries.sort();
    parts.push(orderFree ? "s" : "k", ...entries);

    const id = intern(parts.join(","));
    fingerprints.set(node, id);
    return id;
  };

  const rootFingerprint = (schema: any): number => {
    const simplified = simplify(schema);
    return typeof simplified === "object" && simplified !== null
      ? fingerprint(simplified)
      : EMPTY_FLATTENED;
  };

  return {
    equal(schema1, schema2) {
      let sameFingerprint = false;
      try {
        sameFingerprint = rootFingerprint(schema1) === rootFingerprint(schema2);
      } catch {
        sameFingerprint = false;
      }
      if (sameFingerprint) return true;
      const diffs = diffSchemas(
        flattenSchema(simplify(schema1)),
        flattenSchema(simplify(schema2)),
      );
      return diffs.length === 0;
    },
    uncomparable(schema) {
      return findUncomparable(simplify(schema));
    },
    get fingerprintedNodes() {
      return fingerprintedNodes;
    },
  };
}

/**
 * A leaf whose flattened path ends with a property `diffSchemas` skips: it
 * never takes part in the comparison, so it stays out of the fingerprint.
 */
function isSkippedLeaf(key: string, value: unknown): boolean {
  return (
    (typeof value !== "object" || value === null) &&
    DIFF_SKIPPED_PROPERTIES.includes(key)
  );
}

/**
 * Whether the flattened paths under `key` can never meet those of a sibling
 * key: the first path segment of each is then the key itself. When every key
 * of a node passes, their order changes nothing in the flattened result as a
 * set of paths, which is all `diffSchemas` compares, so the fingerprint
 * sorts them: a type declared in another order is the same schema. The
 * node's `@index` and `@privacy.` entries are always written before its
 * keys, by the flattening and by the fingerprint alike.
 */
function cannotCollide(key: string): boolean {
  return !key.includes(".") && !key.includes("[");
}

/**
 * A leaf as `flattenSchema` keeps it and `diffSchemas` compares it with
 * `JSON.stringify`: values that serialise to nothing (undefined, functions,
 * symbols) are one leaf, every other value keeps its type, so two leaves
 * with the same encoding always serialise the same. A bigint throws as
 * `JSON.stringify` does, so the full comparison reports it as it always did.
 */
function leaf(value: unknown): string {
  switch (typeof value) {
    case "string":
      return JSON.stringify(value);
    case "number":
      return `n${value}`;
    case "bigint":
      return `b${JSON.stringify(value)}`;
    case "boolean":
      return value ? "t" : "f";
    case "object":
      return "z";
    default:
      return "u";
  }
}

/**
 * Formats schema differences as a human-readable string with color codes
 *
 * @param diffs - Array of differences from diffSchemas
 * @returns Formatted string with ANSI color codes
 */
function formatSchemaDifferences(
  diffs: Array<{ key: string; before?: any; after?: any }>,
): string {
  if (diffs.length === 0) {
    return "\x1b[32m    ✓ No differences\x1b[0m";
  }

  const lines: string[] = [];

  for (const diff of diffs) {
    const hasBefore = "before" in diff;
    const hasAfter = "after" in diff;

    if (!hasBefore && hasAfter) {
      // Added field
      lines.push(
        `\x1b[32m    + ${diff.key} = ${JSON.stringify(diff.after)}\x1b[0m`,
      );
    } else if (hasBefore && !hasAfter) {
      // Removed field
      lines.push(
        `\x1b[31m    - ${diff.key} = ${JSON.stringify(diff.before)}\x1b[0m`,
      );
    } else {
      // Modified field
      lines.push(`\x1b[33m    ~ ${diff.key}\x1b[0m`);
      lines.push(`\x1b[31m      - ${JSON.stringify(diff.before)}\x1b[0m`);
      lines.push(`\x1b[32m      + ${JSON.stringify(diff.after)}\x1b[0m`);
    }
  }

  return lines.join("\n");
}

/**
 * Validates that the last migration's schema matches the project schema
 *
 * @param lastMigration - The last migration in the chain
 * @param projectSchema - The project schema from schemas.ts
 * @returns Validation result with errors if any, and `comparedNodes`, the
 * number of distinct schema objects the comparison fingerprinted: its work,
 * which grows with the distinct schema objects rather than the expanded tree
 */
export function validateLastMigrationMatchesProjectSchema(
  lastMigration: MigrationDefinition,
  projectSchema: SchemasDefinition,
): {
  valid: boolean;
  errors: string[];
  comparedNodes: number;
} {
  const comparer = createSchemaComparer();
  const errors = comparer
    .uncomparable(projectSchema)
    .map(
      (where) =>
        `Project schema cannot be compared with the migration chain at ${where}`,
    );

  // Check collections
  {
    const migrationCollections = lastMigration.schemas.collections || {};
    const projectCollections = projectSchema.collections || {};
    if (!comparer.equal(migrationCollections, projectCollections)) {
      const migrationNameSet = new Set(Object.keys(migrationCollections));
      const projectNameSet = new Set(Object.keys(projectCollections));

      const missingInMigration = projectNameSet.difference(migrationNameSet);
      const extraInMigration = migrationNameSet.difference(projectNameSet);

      if (missingInMigration.size > 0) {
        errors.push(
          `Collections missing in last migration: ${[...missingInMigration]
            .sort()
            .join(", ")}`,
        );
      }
      if (extraInMigration.size > 0) {
        errors.push(
          `Extra collections detected in last migration: ${[...extraInMigration]
            .sort()
            .join(", ")}`,
        );
      }

      if (missingInMigration.size === 0 && extraInMigration.size === 0) {
        // Same collection names but different schemas - find which ones differ
        for (const name of migrationNameSet) {
          const migrationSchema = migrationCollections[name];
          const projectSchema = projectCollections[name];

          if (!comparer.equal(migrationSchema, projectSchema)) {
            const simple1 = flattenSchema(simplifySchema(migrationSchema));
            const simple2 = flattenSchema(simplifySchema(projectSchema));
            const diffs = diffSchemas(simple1, simple2);

            errors.push(
              `Collection "${name}" schema differs:\n${formatSchemaDifferences(
                diffs,
              )}`,
            );
          }
        }
      }
    }
  }

  // Check multiCollections if they exist
  {
    const migrationMultiCollections =
      lastMigration.schemas.multiCollections || {};
    const projectMultiCollections = projectSchema.multiCollections || {};

    if (!comparer.equal(migrationMultiCollections, projectMultiCollections)) {
      const migrationNameSet = new Set(Object.keys(migrationMultiCollections));
      const projectNameSet = new Set(Object.keys(projectMultiCollections));

      const missingInMigration = projectNameSet.difference(migrationNameSet);
      const extraInMigration = migrationNameSet.difference(projectNameSet);

      if (missingInMigration.size > 0) {
        errors.push(
          `Multi-collections missing in last migration: ${[
            ...missingInMigration,
          ]
            .sort()
            .join(", ")}`,
        );
      }
      if (extraInMigration.size > 0) {
        errors.push(
          `Extra multi-collections in last migration not in project schema: ${[
            ...extraInMigration,
          ]
            .sort()
            .join(", ")}`,
        );
      }

      if (missingInMigration.size === 0 && extraInMigration.size === 0) {
        // Same multi-collection names but different schemas
        for (const name of migrationNameSet) {
          const migrationSchema = migrationMultiCollections[name];
          const projectSchema = projectMultiCollections[name];

          const migrationTypesSet = new Set(Object.keys(migrationSchema));
          const projectTypesSet = new Set(Object.keys(projectSchema));

          const missingTypesInMigration =
            projectTypesSet.difference(migrationTypesSet);
          const extraTypesInMigration =
            migrationTypesSet.difference(projectTypesSet);

          if (missingTypesInMigration.size > 0) {
            errors.push(
              `Multi-collection "${name}" is missing types in last migration: ${[
                ...missingTypesInMigration,
              ]
                .sort()
                .join(", ")}`,
            );
          }

          if (extraTypesInMigration.size > 0) {
            errors.push(
              `Multi-collection "${name}" has extra types in last migration not in project schema: ${[
                ...extraTypesInMigration,
              ]
                .sort()
                .join(", ")}`,
            );
          }

          if (
            missingTypesInMigration.size > 0 ||
            extraTypesInMigration.size > 0
          ) {
            // Skip further type comparison if types differ
            continue;
          }

          const commonTypes = [
            ...projectTypesSet.intersection(migrationTypesSet),
          ];
          for (const typeName of commonTypes) {
            const migrationTypeSchema = migrationSchema[typeName];
            const projectTypeSchema = projectSchema[typeName];
            if (!comparer.equal(migrationTypeSchema, projectTypeSchema)) {
              const simple1 = flattenSchema(
                simplifySchema(migrationTypeSchema),
              );
              const simple2 = flattenSchema(simplifySchema(projectTypeSchema));
              const diffs = diffSchemas(simple1, simple2);
              errors.push(
                `Multi-collection "${name}" type "${typeName}" schema differs:\n${formatSchemaDifferences(
                  diffs,
                )}`,
              );
            }
          }
        }
      }
    }
  }

  // Check multiModels if they exist
  {
    const migrationMultiModels = lastMigration.schemas.multiModels || {};
    const projectMultiModels = projectSchema.multiModels || {};
    if (!comparer.equal(migrationMultiModels, projectMultiModels)) {
      const migrationNameSet = new Set(Object.keys(migrationMultiModels));
      const projectNameSet = new Set(Object.keys(projectMultiModels));

      const missingInMigration = projectNameSet.difference(migrationNameSet);
      const extraInMigration = migrationNameSet.difference(projectNameSet);

      if (missingInMigration.size > 0) {
        errors.push(
          `Multi-models missing in last migration: ${[...missingInMigration]
            .sort()
            .join(", ")}`,
        );
      }
      if (extraInMigration.size > 0) {
        errors.push(
          `Extra multi-models in last migration not in project schema: ${[
            ...extraInMigration,
          ]
            .sort()
            .join(", ")}`,
        );
      }

      if (missingInMigration.size === 0 && extraInMigration.size === 0) {
        // Same multi-model names but different schemas
        for (const name of migrationNameSet) {
          const migrationSchema = migrationMultiModels[name];
          const projectSchema = projectMultiModels[name];

          const migrationTypesSet = new Set(Object.keys(migrationSchema));
          const projectTypesSet = new Set(Object.keys(projectSchema));

          const missingTypesInMigration =
            projectTypesSet.difference(migrationTypesSet);
          const extraTypesInMigration =
            migrationTypesSet.difference(projectTypesSet);

          if (missingTypesInMigration.size > 0) {
            errors.push(
              `Multi-model "${name}" is missing types in last migration: ${[
                ...missingTypesInMigration,
              ]
                .sort()
                .join(", ")}`,
            );
          }
          if (extraTypesInMigration.size > 0) {
            errors.push(
              `Multi-model "${name}" has extra types in last migration not in project schema: ${[
                ...extraTypesInMigration,
              ]
                .sort()
                .join(", ")}`,
            );
          }

          if (
            missingTypesInMigration.size > 0 ||
            extraTypesInMigration.size > 0
          ) {
            // Skip further type comparison if types differ
            continue;
          }

          const commonTypes = [
            ...projectTypesSet.intersection(migrationTypesSet),
          ];
          for (const typeName of commonTypes) {
            const migrationTypeSchema = migrationSchema[typeName];
            const projectTypeSchema = projectSchema[typeName];
            if (!comparer.equal(migrationTypeSchema, projectTypeSchema)) {
              const simple1 = flattenSchema(
                simplifySchema(migrationTypeSchema),
              );
              const simple2 = flattenSchema(simplifySchema(projectTypeSchema));
              const diffs = diffSchemas(simple1, simple2);
              errors.push(
                `Multi-model "${name}" type "${typeName}" schema differs:\n${formatSchemaDifferences(
                  diffs,
                )}`,
              );
            }
          }
        }
      }
    }
  }

  // Check scopedMultiCollections if they exist
  {
    const migrationScoped = lastMigration.schemas.scopedMultiCollections || {};
    const projectScoped = projectSchema.scopedMultiCollections || {};

    if (!comparer.equal(migrationScoped, projectScoped)) {
      const migrationNameSet = new Set(Object.keys(migrationScoped));
      const projectNameSet = new Set(Object.keys(projectScoped));

      const missingInMigration = projectNameSet.difference(migrationNameSet);
      const extraInMigration = migrationNameSet.difference(projectNameSet);

      if (missingInMigration.size > 0) {
        errors.push(
          `Scoped multi-collections missing in last migration: ${[
            ...missingInMigration,
          ]
            .sort()
            .join(", ")}`,
        );
      }
      if (extraInMigration.size > 0) {
        errors.push(
          `Extra scoped multi-collections in last migration not in project schema: ${[
            ...extraInMigration,
          ]
            .sort()
            .join(", ")}`,
        );
      }

      if (missingInMigration.size === 0 && extraInMigration.size === 0) {
        // Same scoped-collection names but different scope/type schemas
        for (const name of migrationNameSet) {
          const migrationEntry = migrationScoped[name];
          const projectEntry = projectScoped[name];

          if (!comparer.equal(migrationEntry.scope, projectEntry.scope)) {
            const simple1 = flattenSchema(simplifySchema(migrationEntry.scope));
            const simple2 = flattenSchema(simplifySchema(projectEntry.scope));
            const diffs = diffSchemas(simple1, simple2);
            errors.push(
              `Scoped multi-collection "${name}" scope schema differs:\n${formatSchemaDifferences(
                diffs,
              )}`,
            );
          }

          const migrationTypes = migrationEntry.types || {};
          const projectTypes = projectEntry.types || {};

          const migrationTypesSet = new Set(Object.keys(migrationTypes));
          const projectTypesSet = new Set(Object.keys(projectTypes));

          const missingTypesInMigration =
            projectTypesSet.difference(migrationTypesSet);
          const extraTypesInMigration =
            migrationTypesSet.difference(projectTypesSet);

          if (missingTypesInMigration.size > 0) {
            errors.push(
              `Scoped multi-collection "${name}" is missing types in last migration: ${[
                ...missingTypesInMigration,
              ]
                .sort()
                .join(", ")}`,
            );
          }
          if (extraTypesInMigration.size > 0) {
            errors.push(
              `Scoped multi-collection "${name}" has extra types in last migration not in project schema: ${[
                ...extraTypesInMigration,
              ]
                .sort()
                .join(", ")}`,
            );
          }

          if (
            missingTypesInMigration.size > 0 ||
            extraTypesInMigration.size > 0
          ) {
            // Skip further type comparison if types differ
            continue;
          }

          const commonTypes = [
            ...projectTypesSet.intersection(migrationTypesSet),
          ];
          for (const typeName of commonTypes) {
            const migrationTypeSchema = migrationTypes[typeName];
            const projectTypeSchema = projectTypes[typeName];
            if (!comparer.equal(migrationTypeSchema, projectTypeSchema)) {
              const simple1 = flattenSchema(
                simplifySchema(migrationTypeSchema),
              );
              const simple2 = flattenSchema(simplifySchema(projectTypeSchema));
              const diffs = diffSchemas(simple1, simple2);
              errors.push(
                `Scoped multi-collection "${name}" type "${typeName}" schema differs:\n${formatSchemaDifferences(
                  diffs,
                )}`,
              );
            }
          }
        }
      }
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    comparedNodes: comparer.fingerprintedNodes,
  };
}

/**
 * Validates the entire migration chain including schema consistency
 *
 * @param migrations - Array of migrations
 * @param projectSchemaPath - Path to schemas.ts
 * @returns Validation result
 */
export async function validateMigrationChainWithProjectSchema(
  migrations: MigrationDefinition[],
  projectSchemaPath: string,
): Promise<{
  valid: boolean;
  errors: string[];
  warnings: string[];
}> {
  const errors: string[] = [];
  const warnings: string[] = [];

  if (migrations.length === 0) {
    warnings.push(
      'No migrations found. Run "mongodbee generate" to create your first migration.',
    );
    return { valid: true, errors, warnings };
  }

  // Load project schema
  let projectSchema: SchemasDefinition;
  try {
    projectSchema = await loadProjectSchema(projectSchemaPath);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    errors.push(`Failed to load project schema: ${message}`);
    return { valid: false, errors, warnings };
  }

  // Check if project schema is empty
  const hasCollections =
    Object.keys(projectSchema.collections ?? {}).length > 0;
  const hasMultiCollections =
    Object.keys(projectSchema.multiCollections || {}).length > 0;
  const hasMultiModels =
    Object.keys(projectSchema.multiModels || {}).length > 0;
  const hasScopedMultiCollections =
    Object.keys(projectSchema.scopedMultiCollections || {}).length > 0;

  if (
    !hasCollections &&
    !hasMultiModels &&
    !hasMultiCollections &&
    !hasScopedMultiCollections
  ) {
    warnings.push("Project schema is empty. Define your schemas in schemas.ts");
  }

  // Validate last migration matches project schema
  const lastMigration = migrations[migrations.length - 1];
  const validation = validateLastMigrationMatchesProjectSchema(
    lastMigration,
    projectSchema,
  );

  if (!validation.valid) {
    errors.push(...validation.errors);
    errors.push(
      `\nThe last migration schema must match your project schema (schemas.ts).\n` +
        `Update your schemas.ts to match the last migration, or create a new migration to reflect your current schema.`,
    );
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

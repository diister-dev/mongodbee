/**
 * @fileoverview Deterministic ID generation for seeded migration documents.
 *
 * Seeds that omit an explicit `_id` used to get a **random** ULID at apply
 * time. That made rollback impossible across process restarts: the reverse
 * applier reconstructs the operation from the migration file and never sees
 * the random id that was actually written.
 *
 * The fix is to derive each seed's `_id` deterministically from stable
 * inputs — the migration id, the operation signature, and the document's
 * position. Apply and reverse therefore compute the *same* id every time,
 * in any process, so rollback can delete exactly what was inserted.
 *
 * @module
 */

import * as v from "../../schema.ts";

/**
 * Best-effort extraction of the `_id` type prefix from an id schema.
 *
 * Handles both shapes produced by this library:
 * - `dbId(type)` — an optional schema whose default is `"type:<ulid>"`;
 *   the prefix is read from that default.
 * - `refId(type)` (bare, no default) — a piped string with a
 *   `^type:[a-zA-Z0-9]+` regex; the prefix is parsed from the regex source.
 *
 * Returns `fallbackPrefix` when provided, else the extracted prefix, else
 * `""` (valid for plain `v.string()` ids that carry no prefix).
 */
export function extractIdPrefix(
  schemaIdField: unknown,
  fallbackPrefix = "",
): string {
  if (fallbackPrefix) return fallbackPrefix;
  if (!schemaIdField || typeof schemaIdField !== "object") return "";

  // 1. dbId(): default value like "user:<ulid>"
  try {
    const def: unknown = v.getDefault(schemaIdField as v.GenericSchema);
    if (typeof def === "string" && def.includes(":")) {
      return def.split(":")[0];
    }
  } catch {
    // no default — fall through to regex introspection
  }

  // 2. refId()/dbId(): parse the leading `^prefix:` of a regex validation.
  const schema = schemaIdField as Record<string, unknown>;
  const pipes: unknown[] = [];
  if (Array.isArray(schema.pipe)) pipes.push(...schema.pipe);
  const wrapped = schema.wrapped as Record<string, unknown> | undefined;
  if (wrapped && Array.isArray(wrapped.pipe)) pipes.push(...wrapped.pipe);

  for (const entry of pipes) {
    const action = entry as Record<string, unknown>;
    if (action?.type === "regex" && action.requirement instanceof RegExp) {
      const match = action.requirement.source.match(/^\^?([a-zA-Z0-9_-]+):/);
      if (match) return match[1];
    }
  }

  return "";
}

/** FNV-1a 32-bit hash. */
function fnv1a32(input: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

/** DJB2 32-bit hash. */
function djb2(input: string): number {
  let h = 5381;
  for (let i = 0; i < input.length; i++) {
    h = ((h << 5) + h + input.charCodeAt(i)) | 0;
  }
  return h >>> 0;
}

/**
 * Produce a deterministic, ID-safe identifier for a seeded document.
 *
 * The output has the shape `<prefix>:<base36-fingerprint>` and contains
 * only `[a-z0-9]` so it satisfies the `refId(prefix)` validation
 * (`^prefix:[a-zA-Z0-9]+`).
 *
 * Determinism comes from combining the migration id, an operation
 * signature (which distinguishes seed operations within a migration), and
 * the document index. Two different processes loading the same migration
 * file derive identical ids.
 *
 * @param prefix - The document-type prefix (e.g. `"user"`)
 * @param migrationId - The id of the migration emitting this seed
 * @param opSignature - A stable signature of the seed operation
 *   (e.g. `"users"` or `"catalog:product"`)
 * @param docIndex - The document's index within the seed batch
 * @returns A deterministic id such as `"user:1a2b3c4d5e6f00"`
 */
export function deterministicSeedId(
  prefix: string,
  migrationId: string,
  opSignature: string,
  docIndex: number,
): string {
  // Two independent 32-bit hashes over distinct orderings of the inputs
  // give ~64 bits of fingerprint — ample to avoid collisions across a
  // migration's seed set.
  const a = fnv1a32(`${migrationId}|${opSignature}|${docIndex}`)
    .toString(36)
    .padStart(7, "0");
  const b = djb2(`${docIndex}|${opSignature}|${migrationId}`)
    .toString(36)
    .padStart(7, "0");
  return `${prefix}:${a}${b}`;
}

/**
 * Deterministic target `_id` for a document produced by a `flow` operation.
 *
 * Derived from the migration id, the source collection, and the source
 * document's `_id`. Because it's a pure function of the source, a copy can
 * be reversed by recomputing the same ids from the still-present source —
 * no provenance log required.
 *
 * @param prefix - Target type prefix (from the target `_id` schema)
 * @param migrationId - Emitting migration id
 * @param sourceCollection - Source collection name
 * @param sourceId - Source document `_id`
 */
export function flowTargetId(
  prefix: string,
  migrationId: string,
  sourceCollection: string,
  sourceId: string,
): string {
  return deterministicSeedId(
    prefix,
    migrationId,
    `flow:${sourceCollection}:${sourceId}`,
    0,
  );
}

/**
 * Resolve a seed document's `_id`: honour an explicit `_id`, otherwise
 * derive a deterministic one. The type prefix is taken from `fallbackPrefix`
 * when given (e.g. a multi-collection's `documentType`), otherwise extracted
 * from the `_id` schema. Returned id is always a string.
 *
 * @param doc - The seed document (may or may not carry `_id`)
 * @param schemaIdField - The schema for the `_id` field (for prefix extraction)
 * @param fallbackPrefix - Explicit prefix to prefer (multi-collection type)
 * @param migrationId - Emitting migration id
 * @param opSignature - Stable seed-operation signature
 * @param docIndex - Document index within the batch
 */
export function resolveSeedId(
  doc: Record<string, unknown>,
  schemaIdField: unknown,
  fallbackPrefix: string,
  migrationId: string,
  opSignature: string,
  docIndex: number,
): string {
  if (doc._id !== undefined && doc._id !== null) {
    return String(doc._id);
  }
  const prefix = extractIdPrefix(schemaIdField, fallbackPrefix);
  return deterministicSeedId(prefix, migrationId, opSignature, docIndex);
}

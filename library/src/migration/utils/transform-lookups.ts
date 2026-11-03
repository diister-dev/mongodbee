import { getNestedValue } from "../../dot-notation.ts";
import type { TransformLookup, TransformLookupResults } from "../types.ts";

export const NO_LOOKUPS: TransformLookupResults = Object.freeze({});

type LookupKey = string | number;
type Found = Record<string, Record<string, unknown> | undefined>;

/** What the lookups gave one document: what they matched, or why its key
 *  could not be computed. A failed document is reported by the applier the
 *  way a failing `up` is, so it never aborts the documents around it. */
export type DocumentLookups = {
  readonly found: TransformLookupResults;
  readonly error?: Error;
};

const NO_DOCUMENT_LOOKUPS: DocumentLookups = { found: NO_LOOKUPS };

/** Reads the documents of `collection` whose `field` is one of `keys`: one
 *  `$in` query on MongoDB. */
export type LookupFetch = (
  collection: string,
  field: string,
  keys: readonly LookupKey[],
) => Promise<readonly Record<string, unknown>[]>;

/** `_id` order as MongoDB sorts string ids: by code unit, not by locale. */
function byId(a: Record<string, unknown>, b: Record<string, unknown>): number {
  const left = String(a._id);
  const right = String(b._id);
  return left < right ? -1 : left > right ? 1 : 0;
}

function keyError(
  lookup: TransformLookup,
  doc: Record<string, unknown>,
  reason: string,
  cause?: unknown,
): Error {
  return new Error(
    `lookup "${lookup.as}" could not compute a key for document ${String(doc._id)}: ${reason}`,
    cause === undefined ? undefined : { cause },
  );
}

function computeKey(
  lookup: TransformLookup,
  doc: Record<string, unknown>,
  found: TransformLookupResults,
): LookupKey | undefined | Error {
  let key: unknown;
  try {
    key = lookup.key(doc, found);
  } catch (error) {
    return keyError(
      lookup,
      doc,
      error instanceof Error ? error.message : String(error),
      error,
    );
  }
  if (key === undefined || typeof key === "string" || typeof key === "number") {
    return key;
  }
  return keyError(
    lookup,
    doc,
    `a key is a string, a number or undefined, got ${key === null ? "null" : typeof key}`,
  );
}

function stageKeys(
  documents: readonly Record<string, unknown>[],
  lookup: TransformLookup,
  found: readonly Found[],
  errors: (Error | undefined)[],
) {
  const keys = documents.map((doc, i) => {
    if (errors[i]) return undefined;
    const key = computeKey(lookup, doc, found[i]);
    if (key instanceof Error) {
      errors[i] = key;
      return undefined;
    }
    return key;
  });
  const wanted = [
    ...new Set(keys.filter((k): k is LookupKey => k !== undefined)),
  ];
  return { keys, wanted };
}

function assignStage(
  lookup: TransformLookup,
  keys: readonly (LookupKey | undefined)[],
  matches: readonly Record<string, unknown>[],
  found: Found[],
): void {
  const field = lookup.field ?? "_id";
  const byKey = new Map<unknown, Record<string, unknown>>();
  for (const match of matches) {
    const key = getNestedValue(match, field);
    if (!byKey.has(key)) byKey.set(key, match);
  }
  keys.forEach((key, i) => {
    found[i][lookup.as] = key === undefined ? undefined : byKey.get(key);
  });
}

function settle(
  found: readonly Found[],
  errors: readonly (Error | undefined)[],
): readonly DocumentLookups[] {
  return found.map((matched, i) => {
    const error = errors[i];
    return error ? { found: matched, error } : { found: matched };
  });
}

/**
 * Resolves `lookups` for a batch of documents, in declaration order: each
 * lookup reads its collection once for the whole batch, then hands what it
 * matched to the keys of the lookups after it. The result is aligned with
 * `documents`.
 */
export async function resolveLookups(
  documents: readonly Record<string, unknown>[],
  lookups: readonly TransformLookup[] | undefined,
  fetch: LookupFetch,
): Promise<readonly DocumentLookups[]> {
  if (!lookups || lookups.length === 0) {
    return documents.map(() => NO_DOCUMENT_LOOKUPS);
  }
  const found: Found[] = documents.map(() => ({}));
  const errors: (Error | undefined)[] = documents.map(() => undefined);
  for (const lookup of lookups) {
    const { keys, wanted } = stageKeys(documents, lookup, found, errors);
    const matches =
      wanted.length > 0
        ? await fetch(lookup.collection, lookup.field ?? "_id", wanted)
        : [];
    assignStage(lookup, keys, matches, found);
  }
  return settle(found, errors);
}

/** The same resolution over the simulated content of the memory applier:
 *  `contentOf` returns the documents of a physical collection, empty when the
 *  simulation does not hold it. */
export function resolveLookupsInMemory(
  documents: readonly Record<string, unknown>[],
  lookups: readonly TransformLookup[] | undefined,
  contentOf: (collection: string) => readonly Record<string, unknown>[],
): readonly DocumentLookups[] {
  if (!lookups || lookups.length === 0) {
    return documents.map(() => NO_DOCUMENT_LOOKUPS);
  }
  const found: Found[] = documents.map(() => ({}));
  const errors: (Error | undefined)[] = documents.map(() => undefined);
  for (const lookup of lookups) {
    const { keys, wanted } = stageKeys(documents, lookup, found, errors);
    const field = lookup.field ?? "_id";
    const wantedSet = new Set<unknown>(wanted);
    const matches = contentOf(lookup.collection)
      .filter((doc) => wantedSet.has(getNestedValue(doc, field)))
      .sort((a, b) => byId(a, b));
    assignStage(lookup, keys, matches, found);
  }
  return settle(found, errors);
}

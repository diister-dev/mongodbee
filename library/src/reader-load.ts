import type { ClientSession, Document } from "mongodb";
import type { Db } from "./mongodb.ts";
import * as v from "./schema.ts";
import { primaryCollection } from "./read-preference.ts";
import type { ComputedLocation } from "./computed-topology.ts";
import type { ReaderQueryDescriptor } from "./reader-query.ts";
import { ReaderArgumentError } from "./reader-errors.ts";
import { isRecord } from "./utils/guards.ts";

export function parseScope(
  reader: string,
  query: ReaderQueryDescriptor,
  value: unknown,
): unknown {
  if (!query.scope) return undefined;
  const parsed = v.safeParse(query.scope, value);
  if (!parsed.success) {
    throw new ReaderArgumentError(
      `reader "${reader}" received a scope its schema refuses: ${parsed.issues.map((issue) => issue.message).join("; ")}`,
    );
  }
  return parsed.output;
}

function queryFilter(
  query: ReaderQueryDescriptor,
  location: ComputedLocation,
  scope: unknown,
  keys: readonly unknown[] | undefined,
): Document {
  const filter: Document = {};
  if (location.kind !== "collection") filter._type = location.type;
  if (query.scope) filter._scope = scope;
  for (const [path, value] of Object.entries(query.where)) {
    filter[path] = Array.isArray(value) ? { $in: value } : value;
  }
  if (query.by !== undefined && keys !== undefined) {
    filter[query.by] = keys.length === 1 ? keys[0] : { $in: keys };
  }
  return filter;
}

function queryProjection(query: ReaderQueryDescriptor): Document {
  const paths = [...query.select];
  const by = query.by;
  if (
    by !== undefined &&
    !paths.some((path) => by === path || by.startsWith(`${path}.`))
  )
    paths.push(by);
  return Object.fromEntries([["_id", 1], ...paths.map((path) => [path, 1])]);
}

export function valueAt(document: Document, path: string): unknown {
  let current: unknown = document;
  for (const segment of path.split(".")) {
    if (!isRecord(current)) return undefined;
    current = current[segment];
  }
  return current;
}

export function selectedRow(
  query: ReaderQueryDescriptor,
  document: Document,
): Document {
  const row: Document = { _id: document._id };
  for (const field of query.select) {
    if (field in document) row[field] = document[field];
  }
  return row;
}

export async function loadRows(
  db: Db,
  session: ClientSession | undefined,
  query: ReaderQueryDescriptor,
  location: ComputedLocation,
  scope: unknown,
  keys: readonly unknown[] | undefined,
): Promise<Document[]> {
  const single = query.one && (keys === undefined || keys.length === 1);
  return await primaryCollection(db, location.collection)
    .find(queryFilter(query, location, scope, keys), {
      projection: queryProjection(query),
      sort: { _id: 1 },
      session,
      ...(single && { limit: 1 }),
    })
    .toArray();
}

export function shaped(
  query: ReaderQueryDescriptor,
  rows: readonly Document[],
): unknown {
  return query.one ? (rows[0] ?? null) : [...rows];
}

export function matchesWhere(
  query: ReaderQueryDescriptor,
  document: Document,
): boolean {
  return Object.entries(query.where).every(([path, accepted]) => {
    const value = valueAt(document, path) ?? null;
    return Array.isArray(accepted)
      ? accepted.includes(value as never)
      : value === accepted;
  });
}

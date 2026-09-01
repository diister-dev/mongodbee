import type { DatabaseState, MigrationRule } from "../types.ts";

const DOCUMENT_DELETES: ReadonlySet<MigrationRule["type"]> = new Set([
  "delete_collection_documents",
  "delete_multicollection_documents",
  "delete_multimodel_instance_documents",
  "delete_multimodel_instances_documents",
  "delete_scoped_multicollection_documents",
]);

export function isDocumentDelete(operation: MigrationRule): boolean {
  return DOCUMENT_DELETES.has(operation.type);
}

function eachDocument(
  state: DatabaseState,
  visit: (location: string, doc: Record<string, unknown>) => void,
): void {
  for (const [name, { content }] of Object.entries(state.collections)) {
    for (const doc of content) visit(`collections/${name}`, doc);
  }
  for (const [name, { content }] of Object.entries(state.multiCollections)) {
    for (const doc of content) visit(`multiCollections/${name}`, doc);
  }
  for (const [name, { content }] of Object.entries(state.multiModels)) {
    for (const doc of content) visit(`multiModels/${name}`, doc);
  }
  for (
    const [name, { content }] of Object.entries(state.scopedMultiCollections)
  ) {
    for (const doc of content) visit(`scopedMultiCollections/${name}`, doc);
  }
}

export function snapshotIds(state: DatabaseState): Set<string> {
  const ids = new Set<string>();
  eachDocument(state, (location, doc) => {
    if (typeof doc._id === "string") ids.add(`${location}#${doc._id}`);
  });
  return ids;
}

export function deletedIds(
  before: ReadonlySet<string>,
  state: DatabaseState,
): Set<string> {
  const remaining = snapshotIds(state);
  const deleted = new Set<string>();
  for (const key of before) {
    if (!remaining.has(key)) deleted.add(key.slice(key.indexOf("#") + 1));
  }
  return deleted;
}

interface Dangling {
  readonly location: string;
  readonly path: string;
  readonly count: number;
}

export function referencesTo(
  state: DatabaseState,
  ids: ReadonlySet<string>,
): Dangling[] {
  if (ids.size === 0) return [];
  const found = new Map<string, number>();
  const walk = (value: unknown, path: string, location: string) => {
    if (typeof value === "string") {
      if (ids.has(value)) {
        const key = `${location}|${path}`;
        found.set(key, (found.get(key) ?? 0) + 1);
      }
      return;
    }
    if (Array.isArray(value)) {
      for (const item of value) walk(item, `${path}.*`, location);
      return;
    }
    if (value && typeof value === "object" && !(value instanceof Date)) {
      for (const [k, v] of Object.entries(value)) {
        if (k === "_id") continue;
        walk(v, path ? `${path}.${k}` : k, location);
      }
    }
  };
  eachDocument(state, (location, doc) => walk(doc, "", location));
  return [...found.entries()].map(([key, count]) => {
    const [location, path] = key.split("|");
    return { location, path, count };
  }).sort((a, b) => b.count - a.count);
}

export function deleteWarnings(
  index: number,
  operation: MigrationRule,
  before: ReadonlySet<string>,
  state: DatabaseState,
): string[] {
  const deleted = deletedIds(before, state);
  const label = `Operation ${index + 1} (${operation.type})`;
  if (deleted.size === 0) {
    return [
      `${label} matched no simulated document: its "where" filter was not exercised by the simulation`,
    ];
  }
  const dangling = referencesTo(state, deleted);
  if (dangling.length === 0) return [];
  const total = dangling.reduce((sum, d) => sum + d.count, 0);
  const sites = dangling.slice(0, 3).map((d) =>
    `${d.location} ${d.path} (${d.count})`
  ).join(", ");
  return [
    `${label} deleted ${deleted.size} document(s) still referenced ${total} time(s): ${sites}${
      dangling.length > 3 ? `, ${dangling.length - 3} other site(s)` : ""
    }`,
  ];
}

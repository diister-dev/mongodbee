import type * as v from "@diister/mongodbee/schema";
import type { TypeSource } from "@diister/mongodbee/inspect";
import { MIGRATION_OPERATIONS_COLLECTION } from "@diister/mongodbee/inspect";
import {
  discoverMultiCollectionInstances,
  MULTI_COLLECTION_INFO_TYPE,
  MULTI_COLLECTION_MIGRATIONS_TYPE,
} from "@diister/mongodbee/inspect";
import type { StudioContext } from "./context.ts";

export type CollectionKind =
  | "collection"
  | "multiCollection"
  | "multiModelInstance"
  | "scopedMultiCollection"
  | "undeclared"
  | "internal";

export interface CatalogEntry {
  name: string;
  kind: CollectionKind;
  exists: boolean;
  types: Record<string, TypeSource>;
  model?: string;
  scope?: v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;
  internal?: boolean;
}

export const META_TYPES: readonly string[] = [
  MULTI_COLLECTION_INFO_TYPE,
  MULTI_COLLECTION_MIGRATIONS_TYPE,
];

export function isTyped(entry: CatalogEntry): boolean {
  return (
    entry.kind === "multiCollection" ||
    entry.kind === "multiModelInstance" ||
    entry.kind === "scopedMultiCollection"
  );
}

function isInternalName(name: string): boolean {
  return (
    name === MIGRATION_OPERATIONS_COLLECTION ||
    name.startsWith("mongodbee_") ||
    name.startsWith("__dbee")
  );
}

export async function buildCatalog(
  context: StudioContext,
): Promise<CatalogEntry[]> {
  const { db, schemas } = context;
  const listed = await db.listCollections({}, { nameOnly: true }).toArray();
  const physical = new Set(
    listed.map((c) => c.name).filter((name) => !name.startsWith("system.")),
  );
  const entries: CatalogEntry[] = [];
  const declared = new Set<string>();

  for (const [name, source] of Object.entries(schemas.collections ?? {})) {
    declared.add(name);
    entries.push({
      name,
      kind: "collection",
      exists: physical.has(name),
      types: { [name]: source },
    });
  }

  for (const [name, types] of Object.entries(schemas.multiCollections ?? {})) {
    declared.add(name);
    entries.push({
      name,
      kind: "multiCollection",
      exists: physical.has(name),
      types,
    });
  }

  for (const [name, scoped] of Object.entries(
    schemas.scopedMultiCollections ?? {},
  )) {
    declared.add(name);
    entries.push({
      name,
      kind: "scopedMultiCollection",
      exists: physical.has(name),
      types: scoped.types,
      scope: scoped.scope,
    });
  }

  for (const [model, types] of Object.entries(schemas.multiModels ?? {})) {
    const instances = await discoverMultiCollectionInstances(db, model, {
      onUnverifiedPrefixMatch: "skip",
    });
    for (const name of instances) {
      if (declared.has(name)) continue;
      declared.add(name);
      entries.push({
        name,
        kind: "multiModelInstance",
        exists: true,
        types,
        model,
      });
    }
  }

  for (const name of [...physical].sort((a, b) => a.localeCompare(b))) {
    if (declared.has(name)) continue;
    const internal = isInternalName(name);
    entries.push({
      name,
      kind: internal ? "internal" : "undeclared",
      exists: true,
      types: {},
      internal: internal || undefined,
    });
  }

  return entries;
}

export async function findCatalogEntry(
  context: StudioContext,
  name: string,
): Promise<CatalogEntry | undefined> {
  const catalog = await buildCatalog(context);
  return catalog.find((entry) => entry.name === name);
}

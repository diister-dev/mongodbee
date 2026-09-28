import {
  buildCatalog,
  type CatalogEntry,
  type CollectionKind,
  isTyped,
  META_TYPES,
} from "../catalog.ts";
import type { TypeSource } from "../../migration/types.ts";
import { fieldsOf } from "../../type-definition.ts";
import type { StudioContext } from "../context.ts";
import { schemaToNode } from "../schema-tree.ts";

export const QUERY_TIME_LIMIT_MS = 10_000;

export interface TypeCount {
  name: string;
  count: number;
  declared: boolean;
  meta?: true;
  idPrefix?: string;
}

export function idPrefixOf(source: TypeSource | undefined): string | undefined {
  if (!source) return undefined;
  try {
    const id = (fieldsOf(source) as Record<string, unknown>)._id;
    return id === undefined ? undefined : schemaToNode(id).ref;
  } catch {
    return undefined;
  }
}

function withIdPrefixes(entry: CatalogEntry, types: TypeCount[]): TypeCount[] {
  return types.map((type) => {
    const prefix = idPrefixOf(entry.types[type.name]);
    return prefix && prefix !== type.name
      ? { ...type, idPrefix: prefix }
      : type;
  });
}

export interface ScopeCount {
  scope: unknown;
  count: number;
  types: { type: string; count: number }[];
}

export interface CollectionOverview {
  name: string;
  kind: CollectionKind;
  model?: string;
  exists: boolean;
  internal?: true;
  total: number;
  types: TypeCount[];
  scopes?: { top: ScopeCount[]; distinct: number };
}

export interface OverviewResult {
  database: string;
  collections: CollectionOverview[];
}

async function countTypes(
  context: StudioContext,
  entry: CatalogEntry,
): Promise<TypeCount[]> {
  const collection = context.db.collection(entry.name);
  const declared = Object.keys(entry.types);
  const present = (await collection.distinct(
    "_type",
    {},
    { maxTimeMS: QUERY_TIME_LIMIT_MS },
  )) as unknown[];
  const names = new Set<string>(declared);
  for (const value of present) {
    if (typeof value === "string") names.add(value);
  }
  const ordered = [
    ...declared,
    ...[...names]
      .filter((name) => !declared.includes(name))
      .sort((a, b) => a.localeCompare(b)),
  ];
  return await Promise.all(
    ordered.map(async (name) => {
      const count = await collection.countDocuments(
        { _type: name },
        { maxTimeMS: QUERY_TIME_LIMIT_MS },
      );
      const item: TypeCount = {
        name,
        count,
        declared: declared.includes(name),
      };
      if (META_TYPES.includes(name)) item.meta = true;
      return item;
    }),
  );
}

export async function countScopes(
  context: StudioContext,
  collectionName: string,
  limit: number,
): Promise<{ top: ScopeCount[]; distinct: number }> {
  const [result] = await context.db
    .collection(collectionName)
    .aggregate<{
      top: { _id: unknown; count: number; types: ScopeCount["types"] }[];
      distinct: { n: number }[];
    }>(
      [
        {
          $group: {
            _id: { scope: "$_scope", type: "$_type" },
            count: { $sum: 1 },
          },
        },
        {
          $group: {
            _id: "$_id.scope",
            count: { $sum: "$count" },
            types: { $push: { type: "$_id.type", count: "$count" } },
          },
        },
        {
          $facet: {
            top: [{ $sort: { count: -1, _id: 1 } }, { $limit: limit }],
            distinct: [{ $count: "n" }],
          },
        },
      ],
      { maxTimeMS: QUERY_TIME_LIMIT_MS },
    )
    .toArray();
  return {
    top: (result?.top ?? []).map((row) => ({
      scope: row._id,
      count: row.count,
      types: [...row.types].sort((a, b) => a.type.localeCompare(b.type)),
    })),
    distinct: result?.distinct[0]?.n ?? 0,
  };
}

async function describeEntry(
  context: StudioContext,
  entry: CatalogEntry,
  topScopes: number,
): Promise<CollectionOverview> {
  const overview: CollectionOverview = {
    name: entry.name,
    kind: entry.kind,
    exists: entry.exists,
    total: 0,
    types: [],
  };
  if (entry.model) overview.model = entry.model;
  if (entry.internal) overview.internal = true;

  if (!entry.exists) {
    overview.types = withIdPrefixes(
      entry,
      Object.keys(entry.types).map((name) => ({
        name,
        count: 0,
        declared: true,
      })),
    );
    return overview;
  }

  const collection = context.db.collection(entry.name);
  overview.total = await collection.estimatedDocumentCount({
    maxTimeMS: QUERY_TIME_LIMIT_MS,
  });

  if (isTyped(entry)) {
    overview.types = await countTypes(context, entry);
  } else if (entry.kind === "collection") {
    overview.types = [
      { name: entry.name, count: overview.total, declared: true },
    ];
  }

  overview.types = withIdPrefixes(entry, overview.types);

  if (entry.kind === "scopedMultiCollection") {
    overview.scopes = await countScopes(context, entry.name, topScopes);
  }

  return overview;
}

export async function getOverview(
  context: StudioContext,
  options: { topScopes?: number } = {},
): Promise<OverviewResult> {
  const catalog = await buildCatalog(context);
  const collections = await Promise.all(
    catalog.map((entry) =>
      describeEntry(context, entry, options.topScopes ?? 10),
    ),
  );
  return { database: context.db.databaseName, collections };
}

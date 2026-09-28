interface TypeEntry {
  name: string;
  count: number;
  meta?: true;
  idPrefix?: string;
}

interface CollectionEntry {
  name: string;
  kind: string;
  types: TypeEntry[];
}

export interface ResolvedReference {
  collection: string;
  type?: string;
}

function answers(
  type: TypeEntry,
  prefix: string,
  key: "name" | "idPrefix",
): boolean {
  return type[key] === prefix && !type.meta;
}

export function resolveReference(
  collections: readonly CollectionEntry[] | undefined,
  prefix: string | undefined,
): ResolvedReference | undefined {
  if (!prefix || !collections) return undefined;
  for (const key of ["name", "idPrefix"] as const) {
    for (const collection of collections) {
      if (collection.kind === "collection") continue;
      const type = collection.types.find(
        (t) => answers(t, prefix, key) && t.count > 0,
      );
      if (type) return { collection: collection.name, type: type.name };
    }
  }
  const plain = collections.find(
    (c) =>
      c.kind === "collection" &&
      (c.name === prefix ||
        c.types.some((t) => answers(t, prefix, "idPrefix"))),
  );
  return plain ? { collection: plain.name } : undefined;
}

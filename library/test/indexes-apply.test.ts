import * as v from "../src/schema.ts";
import { test, expect } from "vitest";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { withIndex } from "../src/indexes.ts";
import { withDatabase } from "./+shared.ts";
import { defineModel } from "../src/multi-collection-model.ts";

test("applyIndexes - skip recreate when index spec and options identical", async () => {
  await withDatabase("applyIndexes - skip recreate when index spec and options identical", async (db) => {
    const schema1 = {
      username: withIndex(v.string(), {
        unique: true,
        collation: { locale: "en", strength: 2 },
      }),
    };

    // create collection first time
    const coll1 = await collection(db, "users", schema1, { schemaManagement: "auto" });
    const indexesBefore = await coll1.collection.listIndexes().toArray();
    const idx = indexesBefore.find((i: { key?: Record<string, number> }) =>
      i.key && i.key.username === 1
    );
    expect(idx).toBeDefined();

    // Re-init collection with identical schema
    const coll2 = await collection(db, "users", schema1, { schemaManagement: "auto" });
    const indexesAfter = await coll2.collection.listIndexes().toArray();
    const idxAfter = indexesAfter.find((i: { key?: Record<string, number> }) =>
      i.key && i.key.username === 1
    );
    expect(idxAfter).toBeDefined();

    // Ensure the index wasn't changed: same name and same unique flag and collation
    expect(idx!.name).toEqual(idxAfter!.name);
    expect(!!idx!.unique).toEqual(!!idxAfter!.unique);
    expect(
      JSON.stringify(idx!.collation || {}),
    ).toEqual(JSON.stringify(idxAfter!.collation || {}));
  });
});

test("applyIndexes - recreate index when options change", async () => {
  await withDatabase("applyIndexes - recreate index when options change", async (db) => {
    const schemaA = {
      name: withIndex(v.string(), {
        unique: true,
        collation: { locale: "en", strength: 2 },
      }),
    };

    const schemaB = {
      // same field but different collation -> should trigger recreate
      name: withIndex(v.string(), {
        unique: true,
        collation: { locale: "fr", strength: 2 },
      }),
    };

    const cA = await collection(db, "people", schemaA, { schemaManagement: "auto" });
    const before = (await cA.collection.listIndexes().toArray()).find((
      i: { key?: Record<string, number> },
    ) => i.key && i.key.name === 1)!;
    expect(before).toBeDefined();

    // Re-init with modified options
    const cB = await collection(db, "people", schemaB, { schemaManagement: "auto" });
    const after = (await cB.collection.listIndexes().toArray()).find((
      i: { key?: Record<string, number> },
    ) => i.key && i.key.name === 1)!;
    expect(after).toBeDefined();

    // Collation should have been updated (compare important fields only)
    const beforeColl = before.collation as Record<string, unknown> | undefined;
    const afterColl = after.collation as Record<string, unknown> | undefined;
    expect(beforeColl?.locale).toEqual("en");
    expect(beforeColl?.strength).toEqual(2);
    expect(afterColl?.locale).toEqual("fr");
    expect(afterColl?.strength).toEqual(2);
  });
});

test("multiCollection - index is created with partialFilterExpression scoped by type", async () => {
  await withDatabase("multiCollection - index is created with partialFilterExpression scoped by type", async (db) => {
    const schema = {
      product: {
        sku: withIndex(v.string(), { unique: true }),
        price: v.number(),
      },
      category: {
        slug: withIndex(v.string(), { unique: true }),
      },
    };

    const _mc = await multiCollection(
      db,
      "catalog",
      defineModel("catalog", { schema }),
      { schemaManagement: "auto" },
    );
    const indexes = await db.collection("catalog").listIndexes().toArray();

    // find product sku index by key
    const skuIndex = indexes.find((
      i: { key?: Record<string, number>; partialFilterExpression?: unknown },
    ) => i.key && i.key.sku === 1);
    const slugIndex = indexes.find((
      i: { key?: Record<string, number>; partialFilterExpression?: unknown },
    ) => i.key && i.key.slug === 1);

    expect(skuIndex).toBeDefined();
    expect(slugIndex).toBeDefined();

    // partialFilterExpression should include _type eq product / category
    const skuPFE = skuIndex!.partialFilterExpression as
      | Record<string, unknown>
      | undefined;
    const slugPFE = slugIndex!.partialFilterExpression as
      | Record<string, unknown>
      | undefined;
    expect((skuPFE?._type as Record<string, unknown>)?.$eq).toEqual("product");
    expect((slugPFE?._type as Record<string, unknown>)?.$eq).toEqual("category");
  });
});

test("applyIndexes - schema delta: adding an index creates it; removing from schema drops existing index", async () => {
  await withDatabase("applyIndexes - schema delta: adding an index creates it; removing from schema drops existing index", async (db) => {
    const schemaA = {
      a: withIndex(v.string(), { unique: true }),
      b: v.number(),
    };

    // initial create
    const cA = await collection(db, "delta", schemaA, { schemaManagement: "auto" });
    let idxs = await cA.collection.listIndexes().toArray();
    const aIdx = idxs.find((i: { key?: Record<string, number> }) =>
      i.key && i.key.a === 1
    );
    expect(aIdx).toBeDefined();

    // update schema: remove index on `a`, add index on `c`
    const schemaB = {
      a: v.string(),
      c: withIndex(v.string(), { unique: true }),
    };

    const cB = await collection(db, "delta", schemaB, { schemaManagement: "auto" });
    idxs = await cB.collection.listIndexes().toArray();

    // New behavior: orphaned index 'a' is automatically removed, new index 'c' is created
    const aIdxAfter = idxs.find((i: { key?: Record<string, number> }) =>
      i.key && i.key.a === 1
    );
    const cIdx = idxs.find((i: { key?: Record<string, number> }) =>
      i.key && i.key.c === 1
    );

    // Expect old index to be gone and new index to exist
    expect(aIdxAfter).toEqual(undefined); // Old index should be dropped
    expect(cIdx).toBeDefined(); // New index should exist
  });
});

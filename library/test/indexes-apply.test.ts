import * as v from "../src/schema.ts";
import { assertEquals, assertExists } from "jsr:@std/assert";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { withIndex } from "../src/indexes.ts";
import { withDatabase } from "./+shared.ts";

Deno.test("applyIndexes - skip recreate when index spec and options identical", async (t) => {
  await withDatabase(t.name, async (db) => {
    const schema1 = {
      username: withIndex(v.string(), { unique: true, collation: { locale: "en", strength: 2 } }),
    };

    // create collection first time
    const coll1 = await collection(db, "users", schema1);
    const indexesBefore = await coll1.collection.listIndexes().toArray();
    const idx = indexesBefore.find((i: { key?: Record<string, number> }) => i.key && i.key.username === 1);
    assertExists(idx);

    // Re-init collection with identical schema
    const coll2 = await collection(db, "users", schema1);
    const indexesAfter = await coll2.collection.listIndexes().toArray();
    const idxAfter = indexesAfter.find((i: { key?: Record<string, number> }) => i.key && i.key.username === 1);
    assertExists(idxAfter);

    // Ensure the index wasn't changed: same name and same unique flag and collation
    assertEquals(idx!.name, idxAfter!.name);
    assertEquals(!!idx!.unique, !!idxAfter!.unique);
    assertEquals(JSON.stringify(idx!.collation || {}), JSON.stringify(idxAfter!.collation || {}));
  });
});

Deno.test("applyIndexes - recreate index when options change", async (t) => {
  await withDatabase(t.name, async (db) => {
    const schemaA = {
      name: withIndex(v.string(), { unique: true, collation: { locale: "en", strength: 2 } }),
    };

    const schemaB = {
      // same field but different collation -> should trigger recreate
      name: withIndex(v.string(), { unique: true, collation: { locale: "fr", strength: 2 } }),
    };

    const cA = await collection(db, "people", schemaA);
    const before = (await cA.collection.listIndexes().toArray()).find((i: { key?: Record<string, number> }) => i.key && i.key.name === 1)!;
    assertExists(before);

    // Re-init with modified options
    const cB = await collection(db, "people", schemaB);
    const after = (await cB.collection.listIndexes().toArray()).find((i: { key?: Record<string, number> }) => i.key && i.key.name === 1)!;
    assertExists(after);

    // Collation should have been updated (compare important fields only)
    const beforeColl = before.collation as Record<string, unknown> | undefined;
    const afterColl = after.collation as Record<string, unknown> | undefined;
    assertEquals(beforeColl?.locale, "en");
    assertEquals(beforeColl?.strength, 2);
    assertEquals(afterColl?.locale, "fr");
    assertEquals(afterColl?.strength, 2);
  });
});

Deno.test("multiCollection - index is created with partialFilterExpression scoped by type", async (t) => {
  await withDatabase(t.name, async (db) => {
    const schema = {
      product: {
        sku: withIndex(v.string(), { unique: true }),
        price: v.number()
      },
      category: {
        slug: withIndex(v.string(), { unique: true })
      }
    };

    const _mc = await multiCollection(db, "catalog", schema);
    const indexes = await db.collection("catalog").listIndexes().toArray();

    // find product sku index by key
    const skuIndex = indexes.find((i: { key?: Record<string, number>; partialFilterExpression?: unknown }) => i.key && i.key.sku === 1);
    const slugIndex = indexes.find((i: { key?: Record<string, number>; partialFilterExpression?: unknown }) => i.key && i.key.slug === 1);

    assertExists(skuIndex);
    assertExists(slugIndex);

    // partialFilterExpression should include _type eq product / category
    const skuPFE = skuIndex!.partialFilterExpression as Record<string, unknown> | undefined;
    const slugPFE = slugIndex!.partialFilterExpression as Record<string, unknown> | undefined;
    assertEquals((skuPFE?._type as Record<string, unknown>)?.$eq, "product");
    assertEquals((slugPFE?._type as Record<string, unknown>)?.$eq, "category");
  });
});


Deno.test("applyIndexes - schema delta: adding an index creates it; removing from schema does not drop existing index", async (t) => {
  await withDatabase(t.name, async (db) => {
    const schemaA = {
      a: withIndex(v.string(), { unique: true }),
      b: v.number(),
    };

    // initial create
    const cA = await collection(db, "delta", schemaA);
    let idxs = await cA.collection.listIndexes().toArray();
    const aIdx = idxs.find((i: { key?: Record<string, number> }) => i.key && i.key.a === 1);
    assertExists(aIdx);

    // update schema: remove index on `a`, add index on `c`
    const schemaB = {
      a: v.string(),
      c: withIndex(v.string(), { unique: true }),
    };

    const cB = await collection(db, "delta", schemaB);
    idxs = await cB.collection.listIndexes().toArray();

    // Current behavior: previously existing index 'a' is NOT automatically removed
    const aIdxAfter = idxs.find((i: { key?: Record<string, number> }) => i.key && i.key.a === 1);
    const cIdx = idxs.find((i: { key?: Record<string, number> }) => i.key && i.key.c === 1);

    // Expect both to exist: old index remains, new index created
    assertExists(aIdxAfter);
    assertExists(cIdx);
  });
});

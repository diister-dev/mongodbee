import { test } from "./+harness.ts";
import {
  assert,
  assertEquals,
  assertExists,
  assertRejects,
} from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

async function makeCatalog(
  db: Parameters<Parameters<typeof withDatabase>[1]>[0],
) {
  return await scopedMultiCollection(db, "catalog", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: {
      artwork: {
        title: v.string(),
        year: v.number(),
      },
      artist: {
        name: v.string(),
      },
    },
  });
}

test(".scope(id) rejects empty / null / undefined", async () => {
  await withDatabase("smc-scope-empty", async (db) => {
    const catalog = await makeCatalog(db);

    assertRejectsLike(() => catalog.scope("" as any), "scope");
    assertRejectsLike(() => catalog.scope(null as any), "scope");
    assertRejectsLike(() => catalog.scope(undefined as any), "scope");
  });
});

test(".scope(id) rejects ids that do not validate against the scope schema", async () => {
  await withDatabase("smc-scope-invalid", async (db) => {
    const catalog = await makeCatalog(db);
    // refId("exposition") expects "exposition:..." — "foo" should fail
    assertRejectsLike(() => catalog.scope("foo" as any), "exposition");
  });
});

test(".scope(id).insertOne auto-injects _scope, _type, _id", async () => {
  await withDatabase("smc-insert-one", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    const id = await expo.insertOne("artwork", {
      title: "Mona Lisa",
      year: 1503,
    });
    assert(id.startsWith("artwork:"), `expected artwork:* id, got ${id}`);

    // Verify the raw document in MongoDB
    const raw = await db.collection("catalog").findOne({ _id: id as never });
    assertExists(raw);
    assertEquals(raw._scope, EXPO_A);
    assertEquals(raw._type, "artwork");
    assertEquals((raw as unknown as { title: string }).title, "Mona Lisa");
  });
});

test(".scope(id).insertOne rejects docs containing _scope or _type", async () => {
  await withDatabase("smc-insert-reserved", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    await assertRejects(
      () =>
        expo.insertOne("artwork", {
          _scope: EXPO_B,
          title: "x",
          year: 1,
        } as any),
      Error,
      "_scope",
    );

    await assertRejects(
      () =>
        expo.insertOne("artwork", {
          _type: "artist",
          title: "x",
          year: 1,
        } as any),
      Error,
      "_type",
    );
  });
});

test(".scope(id).insertMany inserts batch with scope+type auto-injected", async () => {
  await withDatabase("smc-insert-many", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    const ids = await expo.insertMany("artwork", [
      { title: "A", year: 1900 },
      { title: "B", year: 1901 },
      { title: "C", year: 1902 },
    ]);
    assertEquals(ids.length, 3);

    const docs = await db
      .collection("catalog")
      .find({
        _scope: EXPO_A,
        _type: "artwork",
      } as never)
      .toArray();
    assertEquals(docs.length, 3);
  });
});

test(".scope(id).find / findOne filter by scope + type", async () => {
  await withDatabase("smc-find", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    await expoA.insertOne("artwork", { title: "A1", year: 1 });
    await expoA.insertOne("artwork", { title: "A2", year: 2 });
    await expoB.insertOne("artwork", { title: "B1", year: 3 });

    const allA = await expoA.find("artwork");
    assertEquals(allA.length, 2);
    assert(allA.every((d) => d._scope === EXPO_A));

    const allB = await expoB.find("artwork");
    assertEquals(allB.length, 1);
    assertEquals(allB[0]._scope, EXPO_B);

    const oneA = await expoA.findOne("artwork", { title: "A1" });
    assertExists(oneA);
    assertEquals(oneA.year, 1);

    // findOne across scopes returns null — cannot leak doc B from scope A's view
    const leak = await expoA.findOne("artwork", { title: "B1" });
    assertEquals(leak, null);
  });
});

test(".scope(id).getById is scope-checked", async () => {
  await withDatabase("smc-getById", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    const idInA = await expoA.insertOne("artwork", { title: "A", year: 1 });

    const fromA = await expoA.getById("artwork", idInA);
    assertEquals(fromA.title, "A");

    // From scope B, the same id should throw — it does not belong to this scope
    await assertRejects(() => expoB.getById("artwork", idInA));
  });
});

test(".scope(id).countDocuments scoped count", async () => {
  await withDatabase("smc-count", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    await expoA.insertMany("artwork", [
      { title: "a", year: 1 },
      { title: "b", year: 2 },
    ]);
    await expoB.insertOne("artwork", { title: "c", year: 3 });

    assertEquals(await expoA.countDocuments("artwork"), 2);
    assertEquals(await expoB.countDocuments("artwork"), 1);
    assertEquals(await expoA.countDocuments("artwork", { title: "a" }), 1);
  });
});

test(".scope(id).updateOne is scope-checked", async () => {
  await withDatabase("smc-updateOne", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    const id = await expoA.insertOne("artwork", { title: "old", year: 1 });

    const modified = await expoA.updateOne("artwork", id, { title: "new" });
    assertEquals(modified, 1);

    const after = await expoA.getById("artwork", id);
    assertEquals(after.title, "new");

    // Updating from another scope should fail
    await assertRejects(() =>
      expoB.updateOne("artwork", id, { title: "hack" }),
    );
  });
});

test(".scope(id).deleteId is scope-checked; deleteMany scoped by filter", async () => {
  await withDatabase("smc-delete", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    const idA = await expoA.insertOne("artwork", { title: "a", year: 1 });
    await expoA.insertOne("artwork", { title: "b", year: 2 });
    await expoB.insertOne("artwork", { title: "c", year: 3 });

    // Cannot delete A's doc from scope B
    await assertRejects(() => expoB.deleteId("artwork", idA));

    // Delete in A
    const n = await expoA.deleteId("artwork", idA);
    assertEquals(n, 1);

    // deleteMany with empty filter only deletes within scope+type
    const removed = await expoA.deleteMany("artwork", {});
    assertEquals(removed, 1);

    // Scope B is untouched
    assertEquals(await expoB.countDocuments("artwork"), 1);
  });
});

test(".scope(id) cross-scope isolation: insert into A, never visible from B", async () => {
  await withDatabase("smc-isolation", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    await expoA.insertMany("artwork", [
      { title: "a1", year: 1 },
      { title: "a2", year: 2 },
    ]);
    await expoA.insertOne("artist", { name: "Picasso" });

    assertEquals(await expoB.find("artwork"), []);
    assertEquals(await expoB.find("artist"), []);
    assertEquals(await expoB.countDocuments("artwork"), 0);
  });
});

// [N1] The scope value stored on inserts must match what the view filters on.
// If the scope schema TRANSFORMS (trim/lowercase/…), `.scope(id)` must resolve
// to the parse OUTPUT — otherwise the view filters on the untransformed input
// and never sees the very documents it inserted.
test(".scope(id) applies the scope schema transform so inserts stay visible (N1)", async () => {
  await withDatabase("smc-scope-transform", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: v.pipe(v.string(), v.trim(), v.toLowerCase()),
      types: { artwork: { title: v.string() } },
    });

    // Mixed case + surrounding whitespace — the schema normalizes it.
    const expo = catalog.scope("  EXPO-Alpha  ");
    const id = await expo.insertOne("artwork", { title: "Mona Lisa" });

    // The stored _scope is the TRANSFORMED value.
    const raw = await db.collection("catalog").findOne({ _id: id as never });
    assertExists(raw);
    assertEquals(raw._scope, "expo-alpha");

    // The regression: the SAME view must see the doc it just inserted.
    const found = await expo.find("artwork");
    assertEquals(found.length, 1);
    assertEquals(found[0].title, "Mona Lisa");

    // A differently-cased/spaced id resolves to the same physical scope.
    const expo2 = catalog.scope("expo-alpha");
    assertEquals((await expo2.find("artwork")).length, 1);
  });
});

// [N2] Scoped updates must run Valibot validation on the $set paths (as
// multiCollection.updateOne does) rather than deferring to Mongo's opaque
// "Document failed validation". A `v.check()` predicate is invisible to the
// generated Mongo JSON-Schema validator, so without the fix an invalid update
// would silently persist.
test(".scope(id).updateOne validates $set values with Valibot (N2)", async () => {
  await withDatabase("smc-update-validate", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        item: {
          code: v.pipe(
            v.string(),
            v.check((s: string) => !s.includes(" "), "no spaces allowed"),
          ),
          qty: v.number(),
        },
      },
    });
    const expo = catalog.scope(EXPO_A);
    const id = await expo.insertOne("item", { code: "abc", qty: 1 });

    // Violates the valibot check (Mongo's validator can't see it) → must reject.
    await assertRejects(() =>
      expo.updateOne("item", id, { code: "has space" }),
    );
    // The invalid value must NOT have been written.
    assertEquals((await expo.getById("item", id)).code, "abc");

    // A plain type mismatch is likewise caught before hitting Mongo.
    await assertRejects(() =>
      expo.updateOne("item", id, { qty: "twelve" as any }),
    );

    // Reserved fields stay rejected on update.
    await assertRejects(
      () => expo.updateOne("item", id, { _scope: EXPO_B } as any),
      Error,
      "_scope",
    );

    // A valid update still goes through.
    assertEquals(await expo.updateOne("item", id, { code: "xyz", qty: 2 }), 1);
    const after = await expo.getById("item", id);
    assertEquals(after.code, "xyz");
    assertEquals(after.qty, 2);
  });
});

// [N2] updateMany validates each batch entry the same way.
test(".scope(id).updateMany validates $set values with Valibot (N2)", async () => {
  await withDatabase("smc-update-many-validate", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        item: {
          code: v.pipe(
            v.string(),
            v.check((s: string) => !s.includes(" "), "no spaces allowed"),
          ),
        },
      },
    });
    const expo = catalog.scope(EXPO_A);
    const id = await expo.insertOne("item", { code: "abc" });

    await assertRejects(() =>
      expo.updateMany({ item: { [id]: { code: "has space" } } }),
    );
    assertEquals((await expo.getById("item", id)).code, "abc");
  });
});

// [N8] The unscoped-disabled Proxy must tolerate inspection / thenable probes
// (then / toJSON / any symbol) so `console.log(catalog)` and accidental
// `await`s don't explode — while still guarding the real view methods.
test(".unscoped (disabled) tolerates inspection and thenable probes (N8)", async () => {
  await withDatabase("smc-unscoped-probe", async (db) => {
    const catalog = await makeCatalog(db); // allowUnscoped not set → disabled
    const unscoped = catalog.unscoped;

    // Thenable / serialization / inspection probes resolve to undefined.
    assertEquals((unscoped as { then?: unknown }).then, undefined);
    assertEquals((unscoped as { toJSON?: unknown }).toJSON, undefined);
    assertEquals(
      (unscoped as Record<symbol, unknown>)[Symbol.toStringTag],
      undefined,
    );

    // console.log of the parent object must not throw.
    console.log(catalog);
    // Awaiting the proxy resolves to itself (then === undefined) — no throw.
    assertEquals(await unscoped, unscoped);

    // Real view methods are still guarded.
    assertRejectsLike(() => (unscoped as any).find("artwork"), "allowUnscoped");
  });
});

// Helper: assertRejects for sync-throwing functions (scope() throws sync)
function assertRejectsLike(fn: () => unknown, msgIncludes: string) {
  try {
    fn();
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    assert(
      message.toLowerCase().includes(msgIncludes.toLowerCase()),
      `expected error message to include "${msgIncludes}", got: ${message}`,
    );
    return;
  }
  throw new Error(`expected ${fn} to throw, but it did not`);
}

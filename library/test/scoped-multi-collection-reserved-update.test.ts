// Reserved-field rejection is a SCOPE-HOPPING guard: a caller must never be
// able to smuggle `_scope` or `_type` through a write to move a document into
// another scope/type. insertOne rejection is covered in
// scoped-multi-collection-scope.test.ts; this file nails down the UPDATE and
// insertMany surfaces, which are the subtler vectors.

import { test } from "./+harness.ts";
import { assertEquals, assertRejects } from "./+assert.ts";
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
      artwork: { title: v.string(), year: v.number() },
      artist: { name: v.string() },
    },
  });
}

test("updateOne: rejects a payload carrying _scope (scope-hop) — nothing is written", async () => {
  await withDatabase("smc2-reserved-update-one-scope", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);
    const id = await expo.insertOne("artwork", { title: "orig", year: 1 });

    await assertRejects(
      () =>
        expo.updateOne("artwork", id, { _scope: EXPO_B, title: "hop" } as any),
      Error,
      "_scope",
    );

    // The doc kept its scope AND its original value — the whole update aborted.
    const raw = await db.collection("catalog").findOne({ _id: id as never });
    assertEquals(raw?._scope, EXPO_A);
    assertEquals((raw as unknown as { title: string }).title, "orig");
  });
});

test("updateOne: rejects a payload carrying _type (type-hop) — nothing is written", async () => {
  await withDatabase("smc2-reserved-update-one-type", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);
    const id = await expo.insertOne("artwork", { title: "orig", year: 1 });

    await assertRejects(
      () =>
        expo.updateOne("artwork", id, { _type: "artist", title: "x" } as any),
      Error,
      "_type",
    );

    const raw = await db.collection("catalog").findOne({ _id: id as never });
    assertEquals(raw?._type, "artwork");
    assertEquals((raw as unknown as { title: string }).title, "orig");
  });
});

test("updateMany: rejects when ANY entry carries a reserved field", async () => {
  await withDatabase("smc2-reserved-update-many", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);
    const id1 = await expo.insertOne("artwork", { title: "a", year: 1 });
    const id2 = await expo.insertOne("artwork", { title: "b", year: 2 });

    // The first entry is legit, the second smuggles _scope. The whole batch
    // must be rejected BEFORE any write lands (validation precedes bulkWrite).
    await assertRejects(
      () =>
        expo.updateMany({
          artwork: {
            [id1]: { title: "a-new" },
            [id2]: { _scope: EXPO_B, title: "hop" } as any,
          },
        }),
      Error,
      "_scope",
    );

    // No entry landed — not even the legit one — because the reserved-field
    // check throws while building the bulk ops, before bulkWrite runs.
    assertEquals((await expo.getById("artwork", id1)).title, "a");
    assertEquals((await expo.getById("artwork", id2)).title, "b");
  });
});

test("updateMany: rejects _type smuggled in an entry", async () => {
  await withDatabase("smc2-reserved-update-many-type", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);
    const id = await expo.insertOne("artwork", { title: "a", year: 1 });

    await assertRejects(
      () =>
        expo.updateMany({
          artwork: { [id]: { _type: "artist" } as any },
        }),
      Error,
      "_type",
    );
    assertEquals((await expo.getById("artwork", id)).title, "a");
  });
});

test("insertMany: rejects when any doc in the batch carries a reserved field", async () => {
  await withDatabase("smc2-reserved-insertmany", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    await assertRejects(
      () =>
        expo.insertMany("artwork", [
          { title: "ok", year: 1 },
          { _scope: EXPO_B, title: "smuggled", year: 2 } as any,
        ]),
      Error,
      "_scope",
    );

    await assertRejects(
      () =>
        expo.insertMany("artwork", [
          { _type: "artist", title: "smuggled", year: 3 } as any,
        ]),
      Error,
      "_type",
    );

    // The whole batch is validated up-front; a rejected batch inserts nothing.
    assertEquals(await expo.countDocuments("artwork"), 0);
  });
});

// Batch operations on a ScopedView: deleteIds, updateMany across multiple
// types, and the empty-insertMany edge case.

import { assert, assertEquals, assertRejects } from "@std/assert";
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
    scope: refId("exposition"),
    types: {
      artwork: { title: v.string(), year: v.number() },
      artist: { name: v.string() },
    },
  });
}

// -------- deleteIds -----------------------------------------------------

Deno.test("deleteIds: batch-deletes several ids within the scope and returns the real deletedCount", async () => {
  await withDatabase("smc2-delids-scope", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    const ids = await expo.insertMany("artwork", [
      { title: "A", year: 1 },
      { title: "B", year: 2 },
      { title: "C", year: 3 },
      { title: "D", year: 4 },
    ]);

    // Delete three of the four; the return value is the true removed count.
    const removed = await expo.deleteIds("artwork", [ids[0], ids[1], ids[2]]);
    assertEquals(removed, 3);

    const remaining = await expo.find("artwork");
    assertEquals(remaining.length, 1);
    assertEquals(remaining[0]._id, ids[3]);
  });
});

Deno.test("deleteIds: ids from another scope are NOT deleted (returns 0, no throw)", async () => {
  await withDatabase("smc2-delids-crossscope", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    const idsA = await expoA.insertMany("artwork", [
      { title: "A1", year: 1 },
      { title: "A2", year: 2 },
    ]);
    const idB = await expoB.insertOne("artwork", { title: "B1", year: 3 });

    // Scope B tries to delete A's ids (plus one of its own): only its own goes.
    const removed = await expoB.deleteIds("artwork", [...idsA, idB]);
    assertEquals(removed, 1, "only the in-scope id is removed");

    // A's docs are untouched — the cross-scope ids never matched.
    assertEquals(await expoA.countDocuments("artwork"), 2);

    // A pure cross-scope batch (all ids belong to A) removes nothing and does
    // NOT throw — deliberate divergence from multiCollection.deleteIds.
    const none = await expoB.deleteIds("artwork", idsA);
    assertEquals(none, 0);
    assertEquals(await expoA.countDocuments("artwork"), 2);
  });
});

Deno.test("deleteIds: empty id array removes nothing and returns 0", async () => {
  await withDatabase("smc2-delids-empty", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    await expo.insertMany("artwork", [
      { title: "A", year: 1 },
      { title: "B", year: 2 },
    ]);

    const removed = await expo.deleteIds("artwork", []);
    assertEquals(removed, 0);
    assertEquals(await expo.countDocuments("artwork"), 2, "nothing deleted");
  });
});

Deno.test("deleteIds: is type-scoped — an id of another type in the batch is ignored", async () => {
  await withDatabase("smc2-delids-type", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    const artId = await expo.insertOne("artwork", { title: "A", year: 1 });
    const artistId = await expo.insertOne("artist", { name: "Picasso" });

    // Deleting under "artwork" with an artist id mixed in only removes artwork.
    const removed = await expo.deleteIds("artwork", [artId, artistId]);
    assertEquals(removed, 1);
    // The artist survives — it was never a valid target for the artwork delete.
    assertEquals(await expo.countDocuments("artist"), 1);
  });
});

// -------- updateMany across multiple types ------------------------------

Deno.test("updateMany: applies per-id updates across MULTIPLE types in one ops object", async () => {
  await withDatabase("smc2-updmany-multitype", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    const artId = await expo.insertOne("artwork", { title: "old", year: 1900 });
    const art2Id = await expo.insertOne("artwork", {
      title: "keep",
      year: 2000,
    });
    const artistId = await expo.insertOne("artist", { name: "Old Name" });

    const modified = await expo.updateMany({
      artwork: {
        [artId]: { title: "new", year: 1901 },
      },
      artist: {
        [artistId]: { name: "New Name" },
      },
    });
    // Two docs touched across two types.
    assertEquals(modified, 2);

    assertEquals((await expo.getById("artwork", artId)).title, "new");
    assertEquals((await expo.getById("artwork", artId)).year, 1901);
    assertEquals((await expo.getById("artist", artistId)).name, "New Name");
    // The untouched artwork is unchanged.
    assertEquals((await expo.getById("artwork", art2Id)).title, "keep");
  });
});

Deno.test("updateMany: ids outside the scope match nothing and are excluded from the count", async () => {
  await withDatabase("smc2-updmany-crossscope", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    const idA = await expoA.insertOne("artwork", { title: "A", year: 1 });
    const idB = await expoB.insertOne("artwork", { title: "B", year: 2 });

    // From scope B, update its own doc AND (illegitimately) A's id.
    const modified = await expoB.updateMany({
      artwork: {
        [idB]: { title: "B-new" },
        [idA]: { title: "hacked" },
      },
    });
    assertEquals(modified, 1, "only the in-scope id was modified");

    // A's doc is untouched.
    assertEquals((await expoA.getById("artwork", idA)).title, "A");
    assertEquals((await expoB.getById("artwork", idB)).title, "B-new");
  });
});

// -------- empty insertMany ----------------------------------------------

Deno.test("insertMany([]) with an empty batch — asserts current driver behavior", async () => {
  await withDatabase("smc2-insertmany-empty", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    // The MongoDB driver rejects an empty insertMany with a clear argument
    // error ("Invalid BulkOperation, Batch cannot be empty"). The scoped view
    // forwards it unmodified; assert we get a sane, non-silent error rather
    // than a bogus success.
    const err = await assertRejects(
      () => expo.insertMany("artwork", []),
      Error,
    );
    assert(
      /empty|BulkOperation|batch/i.test(err.message),
      `expected an empty-batch error, got: ${err.message}`,
    );

    // Nothing was inserted.
    assertEquals(await expo.countDocuments("artwork"), 0);
  });
});

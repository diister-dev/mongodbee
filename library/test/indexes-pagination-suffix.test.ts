// Verrou — plain `withIndex` fields get the trailing `_id` key on all three
// surfaces, unique/TTL keep their bare shape, existing bare indexes migrate,
// and re-init does not oscillate.
//
// Regression it guards: per-field indexes were created WITHOUT the `_id`
// tie-break, so paginate's `(field, _id)` sort could ride no index — every
// sorted page fetched and blocking-sorted the whole filtered set (measured:
// 10 000 keys and docs examined per page of 25 on a 10k scope, every page).
// The reconcile checks of the simple and multi appliers also ignored the KEY
// itself, so a shape rollout would have been silently skipped for existing
// collections (the index is found by name, options match, stale key kept).

import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";

// deno-lint-ignore no-explicit-any
async function indexByName(db: any, coll: string, name: string) {
  const all = await db.collection(coll).listIndexes().toArray();
  // deno-lint-ignore no-explicit-any
  return all.find((i: any) => i.name === name);
}

Deno.test("withIndex: plain fields get the _id suffix; unique and TTL stay bare", async (t) => {
  await withDatabase(t.name, async (db) => {
    await collection(db, "people", {
      name: withIndex(v.string()),
      email: withIndex(v.string(), { unique: true }),
      seenAt: withIndex(v.date(), { expireAfterSeconds: 3600 }),
    }, { schemaManagement: "auto" });
    assertEquals((await indexByName(db, "people", "name")).key, {
      name: 1,
      _id: 1,
    });
    assertEquals((await indexByName(db, "people", "email")).key, { email: 1 });
    assertEquals((await indexByName(db, "people", "seenAt")).key, {
      seenAt: 1,
    });

    await multiCollection(db, "catalog", {
      participant: {
        badge: withIndex(v.string()),
        code: withIndex(v.string(), { unique: true }),
      },
    }, { schemaManagement: "auto" });
    assertEquals(
      (await indexByName(db, "catalog", "participant_badge")).key,
      { badge: 1, _id: 1 },
    );
    assertEquals(
      (await indexByName(db, "catalog", "participant_code")).key,
      { code: 1 },
    );

    await scopedMultiCollection(db, "scoped", {
      scope: refId("exposition"),
      types: {
        participant: {
          generatedAt: withIndex(v.date()),
          slug: withIndex(v.string(), { unique: true }),
        },
      },
    });
    assertEquals(
      (await indexByName(db, "scoped", "_scope__type_participant_generatedAt"))
        .key,
      { _scope: 1, _type: 1, generatedAt: 1, _id: 1 },
    );
    assertEquals(
      (await indexByName(db, "scoped", "_scope__type_participant_slug")).key,
      { _scope: 1, _type: 1, slug: 1 },
    );
  });
});

Deno.test("withIndex: a pre-suffix bare index migrates to the new shape, then re-init is stable", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Simulate an install created before the suffix rollout on all three
    // surfaces: same NAME, bare key. The applier must drop + recreate (the
    // reconcile checks compare the key, not just the options), and a second
    // init must leave the index untouched (no oscillation).
    await db.collection("people").createIndex({ name: 1 }, { name: "name" });
    await db.collection("catalog").createIndex({ badge: 1 }, {
      name: "participant_badge",
      partialFilterExpression: { _type: { $eq: "participant" } },
    });
    await db.collection("scoped").createIndex(
      { _scope: 1, _type: 1, generatedAt: 1 },
      {
        name: "_scope__type_participant_generatedAt",
        partialFilterExpression: { _type: { $eq: "participant" } },
      },
    );

    const init = async () => {
      await collection(db, "people", { name: withIndex(v.string()) }, {
        schemaManagement: "auto",
      });
      await multiCollection(db, "catalog", {
        participant: { badge: withIndex(v.string()) },
      }, { schemaManagement: "auto" });
      await scopedMultiCollection(db, "scoped", {
        scope: refId("exposition"),
        types: { participant: { generatedAt: withIndex(v.date()) } },
      });
    };

    await init();
    const shapes = async () => ({
      people: (await indexByName(db, "people", "name")).key,
      catalog: (await indexByName(db, "catalog", "participant_badge")).key,
      scoped: (await indexByName(
        db,
        "scoped",
        "_scope__type_participant_generatedAt",
      ))
        .key,
    });
    const after1 = await shapes();
    assertEquals(after1.people, { name: 1, _id: 1 });
    assertEquals(after1.catalog, { badge: 1, _id: 1 });
    assertEquals(after1.scoped, {
      _scope: 1,
      _type: 1,
      generatedAt: 1,
      _id: 1,
    });

    await init();
    const after2 = await shapes();
    assertEquals(after2, after1, "second init must not churn the indexes");
    for (const coll of ["people", "catalog", "scoped"]) {
      const all = await db.collection(coll).listIndexes().toArray();
      assert(
        // deno-lint-ignore no-explicit-any
        all.filter((i: any) => JSON.stringify(i.key).includes('"_id":1'))
          .length >= 1,
        `${coll}: suffixed index missing after re-init`,
      );
    }
  });
});

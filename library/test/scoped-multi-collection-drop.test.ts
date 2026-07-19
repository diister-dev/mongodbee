// drop(): the whole-collection nuke. Requires { force: true }; drops the
// underlying physical MongoDB collection (every scope at once).

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

async function collectionExists(
  db: Parameters<Parameters<typeof withDatabase>[1]>[0],
  name: string,
): Promise<boolean> {
  const cols = await db.listCollections({ name }).toArray();
  return cols.length > 0;
}

Deno.test("drop() without { force: true } throws and leaves the collection intact", async () => {
  await withDatabase("smc2-drop-noforce", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);
    await expo.insertOne("artwork", { title: "A", year: 1 });

    await assertRejects(
      // deno-lint-ignore no-explicit-any
      () => catalog.drop({} as any),
      Error,
      "force",
    );

    // Collection and its data are untouched.
    assert(await collectionExists(db, "catalog"), "collection still exists");
    assertEquals(await expo.countDocuments("artwork"), 1);
  });
});

Deno.test("drop({ force: true }) drops the physical collection and every scope in it", async () => {
  await withDatabase("smc2-drop-force", async (db) => {
    const catalog = await makeCatalog(db);
    await catalog.scope(EXPO_A).insertOne("artwork", { title: "A", year: 1 });
    await catalog.scope(EXPO_B).insertOne("artist", { name: "Picasso" });

    assert(await collectionExists(db, "catalog"), "precondition: exists");

    const dropped = await catalog.drop({ force: true });
    assertEquals(dropped, true);

    // The physical collection is gone (all scopes with it).
    assertEquals(
      await collectionExists(db, "catalog"),
      false,
      "collection dropped",
    );
  });
});

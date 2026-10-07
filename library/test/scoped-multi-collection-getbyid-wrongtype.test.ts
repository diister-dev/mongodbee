// getById is narrowed by _type as well as _scope: fetching a real id under the
// WRONG type must throw not-found, not return the doc of the other type. This
// guards against a caller accidentally trusting a type-punned read.

import { test } from "./+harness.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO_A = "exposition:expoaaaaa01";

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

test("getById: a right-scope id read under the wrong type throws not-found", async () => {
  await withDatabase("smc2-getbyid-wrongtype", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    const artworkId = await expo.insertOne("artwork", {
      title: "Mona Lisa",
      year: 1503,
    });

    // Correct type resolves.
    assertEquals((await expo.getById("artwork", artworkId)).title, "Mona Lisa");

    // Same id, wrong type → not-found (the _type predicate excludes it).
    const err = await assertRejects(
      () => expo.getById("artist", artworkId),
      Error,
    );
    assert(
      err.message.includes(artworkId),
      `error should name the missing id, got: ${err.message}`,
    );
    assert(
      err.message.includes(EXPO_A),
      `error should name the scope, got: ${err.message}`,
    );
  });
});

test("getById: type-punning both ways is rejected", async () => {
  await withDatabase("smc2-getbyid-wrongtype-both", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    const artistId = await expo.insertOne("artist", { name: "Picasso" });
    const artworkId = await expo.insertOne("artwork", {
      title: "Guernica",
      year: 1937,
    });

    // artist id read as artwork → throws
    await assertRejects(() => expo.getById("artwork", artistId), Error);
    // artwork id read as artist → throws
    await assertRejects(() => expo.getById("artist", artworkId), Error);

    // Each still resolves under its own type.
    assertEquals((await expo.getById("artist", artistId)).name, "Picasso");
    assertEquals((await expo.getById("artwork", artworkId)).title, "Guernica");
  });
});

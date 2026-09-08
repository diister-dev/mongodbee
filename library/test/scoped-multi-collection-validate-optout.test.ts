/**
 * `{ validate: false }` opt-out on scoped reads — skips the per-document
 * `v.safeParse` for trusted hot-path reads, returning the raw stored docs.
 */
import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO = "exposition:validateoptout";

test("validate:false returns the same docs as the validated read (transform-free schema)", async () => {
  await withDatabase("smc-validate-optout", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { title: v.string(), year: v.number() } },
      allowUnscoped: true,
    });
    const expo = catalog.scope(EXPO);
    await expo.insertMany("artwork", [
      { title: "Mona Lisa", year: 1503 },
      { title: "Guernica", year: 1937 },
    ]);

    const validated = await expo.find("artwork", {});
    const raw = await expo.find("artwork", {}, { validate: false });

    assertEquals(raw.length, 2);
    assertEquals(
      raw.map((d) => d.title).sort(),
      validated.map((d) => d.title).sort(),
    );
    // The opt-out path still carries the injected meta fields.
    for (const d of raw) {
      assertEquals(d._scope, EXPO);
      assertEquals(d._type, "artwork");
      assert(typeof d._id === "string");
    }
  });
});

test("validate:false works on findAny and the multi-scope view", async () => {
  await withDatabase("smc-validate-optout-any", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { title: v.string() }, artist: { name: v.string() } },
      allowUnscoped: true,
    });
    const expo = catalog.scope(EXPO);
    await expo.insertOne("artwork", { title: "Starry Night" });
    await expo.insertOne("artist", { name: "Van Gogh" });

    const any = await expo.findAny({}, { validate: false });
    assertEquals(any.length, 2);

    const viaScopes = await catalog.scopes([EXPO]).find(
      "artwork",
      {},
      {
        validate: false,
      },
    );
    assertEquals(viaScopes.length, 1);
    assertEquals((viaScopes[0] as { title: string }).title, "Starry Night");
  });
});

/**
 * `findProject` — typed projected reads: return only the listed fields plus the
 * meta fields (`_id`/`_type`/`_scope`), partial + unvalidated by construction.
 */
import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

Deno.test("findProject returns only the listed fields + meta, omits the rest", async () => {
  await withDatabase("smc-findproject", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: {
        artwork: { title: v.string(), year: v.number(), description: v.string() },
      },
      allowUnscoped: true,
    });
    const expo = catalog.scope("exposition:fp");
    await expo.insertMany("artwork", [
      { title: "A", year: 2000, description: "desc A" },
      { title: "B", year: 2001, description: "desc B" },
    ]);

    const docs = await expo.findProject("artwork", ["title"]);

    assertEquals(docs.length, 2);
    assertEquals(docs.map((d) => d.title).sort(), ["A", "B"]);
    for (const d of docs) {
      // listed field present
      assert(typeof d.title === "string");
      // omitted fields are absent
      assertEquals((d as Record<string, unknown>).description, undefined);
      assertEquals((d as Record<string, unknown>).year, undefined);
      // meta always kept (so the projected doc stays identifiable)
      assertEquals(d._type, "artwork");
      assertEquals(d._scope, "exposition:fp");
      assert(typeof d._id === "string");
    }
  });
});

Deno.test("findProject honours the filter and works on the multi-scope view", async () => {
  await withDatabase("smc-findproject-scopes", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: { artwork: { title: v.string(), year: v.number() } },
      allowUnscoped: true,
    });
    await catalog.scope("exposition:a").insertMany("artwork", [
      { title: "A1", year: 1 },
      { title: "A2", year: 9 },
    ]);
    await catalog.scope("exposition:b").insertOne("artwork", { title: "B1", year: 9 });

    // filter narrows within the projected read
    const filtered = await catalog.scope("exposition:a").findProject(
      "artwork",
      ["title"],
      { year: 9 },
    );
    assertEquals(filtered.length, 1);
    assertEquals(filtered[0].title, "A2");

    // multi-scope projected read spans both scopes
    const across = await catalog.scopes(["exposition:a", "exposition:b"])
      .findProject("artwork", ["title"], { year: 9 });
    assertEquals(across.map((d) => d.title).sort(), ["A2", "B1"]);
    for (const d of across) assertEquals((d as Record<string, unknown>).year, undefined);
  });
});

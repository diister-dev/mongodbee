import { test } from "../+harness.ts";
import * as v from "../../src/schema.ts";
import { refId } from "../../src/ids.ts";
import { multiCollection } from "../../src/multi-collection.ts";
import { defineModel } from "../../src/multi-collection-model.ts";
import { assert, assertEquals } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";

test("MultiCollection: a type's own _id schema is typed, not never", async (t) => {
  await withDatabase(t.name, async (db) => {
    const model = defineModel("catalog", {
      schema: {
        product: { _id: refId("product"), name: v.string() },
      },
    });
    const catalog = await multiCollection(db, "catalog", model);

    const minted = await catalog.insertOne("product", { name: "Phone" });
    assert(String(minted).startsWith("product:"));

    await catalog.insertOne("product", {
      _id: "product:custom",
      name: "Tablet",
    });
    const custom = await catalog.findOne("product", { name: "Tablet" });
    assertEquals(custom?._id, "product:custom");

    // Intersecting the default `_id` with the type's own used to infer `never`.
    type ReadId = NonNullable<typeof custom>["_id"];
    const readId: [ReadId] extends [never] ? "never" : "typed" = "typed";
    assertEquals(readId, "typed");
  });
});

import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { removeField } from "../src/sanitizer.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO = "exposition:expoaaaaa01";

Deno.test("updateOne: removeField() unsets a field (parity with multiCollection)", async () => {
  await withDatabase("smc-removefield", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: {
        user: {
          name: v.string(),
          nickname: v.optional(v.string()),
        },
      },
    });
    const expo = catalog.scope(EXPO);

    const id = await expo.insertOne("user", { name: "Alice", nickname: "Al" });

    // removeField() should $unset nickname while keeping name
    await expo.updateOne("user", id, { nickname: removeField() });

    const after = await expo.getById("user", id);
    assertEquals(after.name, "Alice");
    assert(!("nickname" in after), "nickname should be unset");
  });
});

Deno.test("updateMany: removeField() works across docs", async () => {
  await withDatabase("smc-removefield-many", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: {
        user: { name: v.string(), nickname: v.optional(v.string()) },
      },
    });
    const expo = catalog.scope(EXPO);

    const id1 = await expo.insertOne("user", { name: "A", nickname: "a" });
    const id2 = await expo.insertOne("user", { name: "B", nickname: "b" });

    await expo.updateMany({
      user: {
        [id1]: { nickname: removeField() },
        [id2]: { name: "B2" },
      },
    });

    const a = await expo.getById("user", id1);
    const b = await expo.getById("user", id2);
    assert(!("nickname" in a), "id1 nickname unset");
    assertEquals(b.name, "B2");
    assertEquals(b.nickname, "b");
  });
});

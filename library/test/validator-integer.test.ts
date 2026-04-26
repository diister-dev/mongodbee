import { assert } from "@std/assert";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";

Deno.test("integer validation rejects non-integer numbers", async (t) => {
  await withDatabase(t.name, async (db) => {
    const schema = {
      count: v.pipe(v.number(), v.integer()),
    } as const;
    const counts = await collection(db, "counts", schema);

    const okId = await counts.insertOne({ count: 5 });
    assert(okId, "Should insert integer");

    const okFloatId = await counts.insertOne({ count: 5.0 });
    assert(okFloatId, "Should accept integer-valued double");

    try {
      await counts.insertOne({ count: 1.5 });
      assert(false, "Should have failed for non-integer");
    } catch (error) {
      assert(error, "Should throw validation error for 1.5");
    }
  });
});

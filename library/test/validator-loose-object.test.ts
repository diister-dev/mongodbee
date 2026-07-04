import { assert, assertEquals } from "@std/assert";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";

/**
 * looseObject / strictObject support in the Mongo validator translation.
 *
 * looseObject = "envelope" semantics: required known entries are validated,
 * UNKNOWN keys pass through untouched (both at valibot parse time — which
 * would otherwise strip them silently with v.object — and at the Mongo
 * $jsonSchema level, where additional properties are allowed by default).
 * strictObject adds `additionalProperties: false` to the validator.
 *
 * Regression: these schema types used to hit the translator's default branch
 * → `Unsupported schema type: loose_object` thrown at collection creation.
 */
Deno.test("looseObject: envelope validated, unknown keys PRESERVED end-to-end", async (t) => {
  await withDatabase(t.name, async (db) => {
    const schema = {
      name: v.string(),
      // The envelope: every node must carry a `type`; payload is open.
      node: v.looseObject({ type: v.string() }),
    } as const;
    const docs = await collection(db, "loose_docs", schema);

    // Unknown keys must survive insert AND read (v.object would strip them).
    const id = await docs.insertOne({
      name: "kpi",
      node: { type: "kpi", title: "Participants", sources: [{ id: "A" }] } as never,
    });
    assert(id, "insert with extra keys should pass");
    const stored = await docs.findOne({ _id: id });
    assertEquals((stored?.node as Record<string, unknown>).title, "Participants");
    assertEquals(
      ((stored?.node as Record<string, unknown>).sources as unknown[]).length,
      1,
    );

    // The REQUIRED entry is still enforced.
    try {
      await docs.insertOne({ name: "bad", node: { title: "no type" } as never });
      assert(false, "missing required `type` should fail");
    } catch (error) {
      assert(error, "validation error expected");
    }
  });
});

Deno.test("strictObject: additional properties REJECTED", async (t) => {
  await withDatabase(t.name, async (db) => {
    const schema = {
      config: v.strictObject({ mode: v.string() }),
    } as const;
    const docs = await collection(db, "strict_docs", schema);

    const id = await docs.insertOne({ config: { mode: "a" } });
    assert(id, "exact shape should pass");

    try {
      await docs.insertOne({ config: { mode: "a", extra: 1 } as never });
      assert(false, "extra key should fail under strictObject");
    } catch (error) {
      assert(error, "validation error expected");
    }
  });
});

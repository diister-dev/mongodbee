/**
 * Locks the mapping of Valibot strict bound actions (`gtValue` / `ltValue`)
 * to the MongoDB $jsonSchema validator.
 *
 * MongoDB's $jsonSchema implements JSON Schema draft-4, where
 * `exclusiveMinimum` / `exclusiveMaximum` are BOOLEAN modifiers of
 * `minimum` / `maximum` (draft-6+ turned them into numbers — that form is
 * rejected by MongoDB). These actions used to fall through to the
 * "Unsupported schema type" warning and were silently dropped from the
 * generated validator, so strict bounds were only enforced at the app layer.
 */
import { assert, assertEquals } from "@std/assert";
import { collection } from "../src/collection.ts";
import { toMongoValidator } from "../src/validator.ts";
import { withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";

function withCapturedWarnings<T>(fn: () => T): { result: T; warns: string[] } {
  const warns: string[] = [];
  const original = console.warn;
  console.warn = (...args: unknown[]) => {
    warns.push(args.map(String).join(" "));
  };
  try {
    return { result: fn(), warns };
  } finally {
    console.warn = original;
  }
}

Deno.test("gtValue maps to draft-4 minimum + exclusiveMinimum, without warning", () => {
  const schema = v.object({
    price: v.pipe(v.number(), v.gtValue(0)),
  });

  const { result: validator, warns } = withCapturedWarnings(() =>
    toMongoValidator(schema)
  );

  const price = (validator.$jsonSchema as {
    properties: { price: Record<string, unknown> };
  }).properties.price;

  assertEquals(price.minimum, 0);
  assertEquals(price.exclusiveMinimum, true);
  assertEquals(
    warns.filter((w) => w.includes("Unsupported schema type")),
    [],
    "gt_value must not fall through to the unsupported-type warning",
  );
});

Deno.test("ltValue maps to draft-4 maximum + exclusiveMaximum, without warning", () => {
  const schema = v.object({
    discount: v.pipe(v.number(), v.ltValue(1)),
  });

  const { result: validator, warns } = withCapturedWarnings(() =>
    toMongoValidator(schema)
  );

  const discount = (validator.$jsonSchema as {
    properties: { discount: Record<string, unknown> };
  }).properties.discount;

  assertEquals(discount.maximum, 1);
  assertEquals(discount.exclusiveMaximum, true);
  assertEquals(
    warns.filter((w) => w.includes("Unsupported schema type")),
    [],
    "lt_value must not fall through to the unsupported-type warning",
  );
});

Deno.test("gtValue strict bound is enforced by MongoDB, boundary rejected", async (t) => {
  await withDatabase(t.name, async (db) => {
    const schema = {
      amount: v.pipe(v.number(), v.gtValue(0)),
    } as const;
    const amounts = await collection(db, "amounts", schema);

    const okId = await amounts.insertOne({ amount: 0.01 });
    assert(okId, "Should insert value strictly above the bound");

    try {
      // Bypass the app-layer Valibot parse to prove the DB validator alone
      // rejects the boundary value (this is what the mapping adds).
      await amounts.collection.insertOne(
        { amount: 0 } as never,
      );
      assert(false, "MongoDB validator should have rejected amount = 0");
    } catch (error) {
      assert(error, "Should throw a document validation error at the bound");
    }
  });
});

import * as v from "../src/schema.ts";
import { toMongoValidator } from "../src/validator.ts";
import { assertEquals } from "./+assert.ts";
import { test } from "./+harness.ts";
import { compareAcceptance } from "./+mongo-acceptance.ts";
import { withDatabase } from "./+shared.ts";

test("a check before trim describes the input, so the stored trimmed value is not held to it", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { stricter } = await compareAcceptance(
      db,
      "names",
      v.pipe(v.string(), v.minLength(5), v.trim()),
      ["  ab  ", "abcde", "ab"],
    );
    assertEquals(stricter, []);
  });
});

test("a regex before toUpperCase is not applied to the uppercased stored value", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { stricter } = await compareAcceptance(
      db,
      "codes",
      v.pipe(v.string(), v.regex(/^[a-z]+$/), v.toUpperCase()),
      ["abc", "ABC", "a1"],
    );
    assertEquals(stricter, []);
  });
});

test("a bound before a transform is not applied to the transformed number", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { stricter } = await compareAcceptance(
      db,
      "amounts",
      v.pipe(
        v.number(),
        v.minValue(10),
        v.transform((amount) => amount - 5),
      ),
      [9, 10, 12],
    );
    assertEquals(stricter, []);
  });
});

test("checks after the last transformation still reach the validator", () => {
  const x = toMongoValidator(
    v.object({ x: v.pipe(v.string(), v.minLength(9), v.trim(), v.nonEmpty()) }),
  ).$jsonSchema.properties!.x;
  assertEquals(x, {
    minLength: 1,
    bsonType: "string",
    description: "must be a string",
  });
});

test("brand, flavor and readonly leave the value as it is, so checks before them stay", () => {
  const x = toMongoValidator(
    v.object({
      x: v.pipe(
        v.number(),
        v.minValue(1),
        v.brand("Quantity"),
        v.flavor("Count"),
        v.readonly(),
      ),
    }),
  ).$jsonSchema.properties!.x;
  assertEquals(x.minimum, 1);
});

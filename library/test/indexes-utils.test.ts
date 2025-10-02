import { assertEquals, assert } from "@std/assert";
import { keyEqual, normalizeIndexOptions } from "../src/indexes.ts";

Deno.test("keyEqual returns true for identical simple specs", () => {
  const a = { foo: 1 };
  const b = { foo: 1 };
  assert(keyEqual(a, b));
});

Deno.test("keyEqual returns false for different specs", () => {
  const a = { foo: 1 };
  const b = { foo: -1 };
  assert(!keyEqual(a, b));
});

Deno.test("normalizeIndexOptions normalizes unique and collation", () => {
  const opts = { unique: true, collation: { locale: "en", strength: 2 } };
  const norm = normalizeIndexOptions(opts);
  assertEquals(norm.unique, true);
  assertEquals(typeof norm.collation, "string");
  assert(norm.collation?.includes('"locale":"en"'));
});

Deno.test("normalizeIndexOptions handles undefined safely", () => {
  const norm = normalizeIndexOptions(undefined);
  assertEquals(norm.unique, false);
  assertEquals(norm.collation, undefined);
  assertEquals(norm.partialFilterExpression, undefined);
});

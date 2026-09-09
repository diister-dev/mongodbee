import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import { keyEqual, normalizeIndexOptions } from "../src/indexes.ts";

test("keyEqual returns true for identical simple specs", () => {
  const a = { foo: 1 };
  const b = { foo: 1 };
  assert(keyEqual(a, b));
});

test("keyEqual returns false for different specs", () => {
  const a = { foo: 1 };
  const b = { foo: -1 };
  assert(!keyEqual(a, b));
});

test("normalizeIndexOptions normalizes unique and collation", () => {
  const opts = { unique: true, collation: { locale: "en", strength: 2 } };
  const norm = normalizeIndexOptions(opts);
  assertEquals(norm.unique, true);
  assertEquals(typeof norm.collation, "string");
  assert(norm.collation?.includes('"locale":"en"'));
});

test("normalizeIndexOptions handles undefined safely", () => {
  const norm = normalizeIndexOptions(undefined);
  assertEquals(norm.unique, false);
  assertEquals(norm.collation, undefined);
  assertEquals(norm.partialFilterExpression, undefined);
});

test("normalizeIndexOptions extracts expireAfterSeconds", () => {
  const norm = normalizeIndexOptions({ expireAfterSeconds: 3600 });
  assertEquals(norm.expireAfterSeconds, 3600);
});

test("normalizeIndexOptions preserves expireAfterSeconds: 0", () => {
  const norm = normalizeIndexOptions({ expireAfterSeconds: 0 });
  assertEquals(norm.expireAfterSeconds, 0);
});

test("normalizeIndexOptions returns undefined when expireAfterSeconds absent", () => {
  const norm = normalizeIndexOptions({ unique: true });
  assertEquals(norm.expireAfterSeconds, undefined);
});

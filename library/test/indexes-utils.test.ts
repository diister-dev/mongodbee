import { test, expect } from "vitest";
import { keyEqual, normalizeIndexOptions } from "../src/indexes.ts";

test("keyEqual returns true for identical simple specs", () => {
  const a = { foo: 1 };
  const b = { foo: 1 };
  expect(keyEqual(a, b)).toBeTruthy();
});

test("keyEqual returns false for different specs", () => {
  const a = { foo: 1 };
  const b = { foo: -1 };
  expect(!keyEqual(a, b)).toBeTruthy();
});

test("normalizeIndexOptions normalizes unique and collation", () => {
  const opts = { unique: true, collation: { locale: "en", strength: 2 } };
  const norm = normalizeIndexOptions(opts);
  expect(norm.unique).toEqual(true);
  expect(typeof norm.collation).toEqual("string");
  expect(norm.collation?.includes('"locale":"en"')).toBeTruthy();
});

test("normalizeIndexOptions handles undefined safely", () => {
  const norm = normalizeIndexOptions(undefined);
  expect(norm.unique).toEqual(false);
  expect(norm.collation).toEqual(undefined);
  expect(norm.partialFilterExpression).toEqual(undefined);
});

import * as v from "../src/schema.ts";
import { assertEquals } from "./+assert.ts";
import { test } from "./+harness.ts";
import { compareAcceptance, type FieldSchema } from "./+mongo-acceptance.ts";
import { withDatabase } from "./+shared.ts";

async function expectExact(
  t: { name: string },
  field: FieldSchema,
  probes: readonly string[],
) {
  await withDatabase(t.name, async (db) => {
    const { stricter, looser } = await compareAcceptance(
      db,
      "values",
      field,
      probes,
    );
    assertEquals({ stricter, looser }, { stricter: [], looser: [] });
  });
}

test("two regexes may match at different positions, as valibot tests each on its own", (t) =>
  expectExact(t, v.pipe(v.string(), v.regex(/abc/), v.regex(/def/)), [
    "abc def",
    "def abc",
    "x\nabc def",
    "abc",
    "def",
  ]));

test("anchored regexes keep their anchors once combined", (t) =>
  expectExact(
    t,
    v.pipe(v.string(), v.regex(/^[a-z]/), v.regex(/[0-9]$/), v.regex(/b/)),
    ["ab1", "a1", "b1", "1ab", "ab", "xb2"],
  ));

test("the i flag of uuid is honoured, so an uppercase uuid valibot accepts is stored", (t) =>
  expectExact(t, v.pipe(v.string(), v.uuid()), [
    "0E3F5B8A-7C1D-4E2F-9A3B-5C6D7E8F9A0B",
    "0e3f5b8a-7c1d-4e2f-9a3b-5c6d7e8f9a0b",
    "not a uuid",
  ]));

test("the m and s flags keep their meaning", (t) =>
  expectExact(
    t,
    v.pipe(v.string(), v.regex(/^b/m), v.regex(/a.b/s), v.regex(/B/i)),
    ["a\nb", "ab", "b", "xa\nb"],
  ));

test("a sticky regex is left to valibot rather than translated wrongly", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { stricter } = await compareAcceptance(
      db,
      "values",
      v.pipe(v.string(), v.regex(/abc/y)),
      ["abc", "xabc"],
    );
    assertEquals(stricter, []);
  });
});

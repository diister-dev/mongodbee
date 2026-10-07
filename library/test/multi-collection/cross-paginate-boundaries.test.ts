// Verrou — cross-type paginate where the SHARED sort field holds a different
// BSON bracket per _type (numbers / strings / dates), with null, missing and
// duplicates inside each type.
//
// The existing cross-paginate tests walk homogeneous values; the cross-type
// reality is the opposite: each _type stores its own shape, so every page
// boundary can be a bracket edge AND a type edge at once. This locks, against
// the server's own $sort:
// - the full forward walk with `total`/`position` on every page,
// - the backward round-trip landing on identical pages,
// - that a subset of types (["a","c"]) never leaks the excluded type through
//   the cursor's $type rungs (each branch must keep the _type pins).

import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { multiCollection } from "../../src/multi-collection.ts";
import * as v from "../../src/schema.ts";

const DOCS: { _id: string; _type: string; w?: unknown }[] = [
  // type a: numbers, with a null, a duplicate pair and a missing.
  { _id: "a:d01", _type: "a", w: 1 },
  { _id: "a:d02", _type: "a", w: 2 },
  { _id: "a:d03", _type: "a", w: 2 },
  { _id: "a:d04", _type: "a", w: null },
  { _id: "a:d05", _type: "a" },
  // type b: strings, one duplicated, one missing.
  { _id: "b:d01", _type: "b", w: "x" },
  { _id: "b:d02", _type: "b", w: "y" },
  { _id: "b:d03", _type: "b", w: "x" },
  { _id: "b:d04", _type: "b" },
  // type c: dates, one duplicated, one missing.
  { _id: "c:d01", _type: "c", w: new Date(Date.UTC(2026, 0, 1)) },
  { _id: "c:d02", _type: "c", w: new Date(Date.UTC(2026, 0, 2)) },
  { _id: "c:d03", _type: "c", w: new Date(Date.UTC(2026, 0, 1)) },
  { _id: "c:d04", _type: "c" },
];

async function truth(db: any, types: string[], dir: 1 | -1): Promise<string[]> {
  const rows = await db
    .collection("things")
    .aggregate([
      { $match: { _type: { $in: types } } },
      { $sort: { w: dir, _id: dir } },
    ])
    .toArray();
  return (rows as { _id: string }[]).map((r) => String(r._id));
}

async function setup(db: any) {
  const things = await multiCollection(db, "things", {
    a: { w: v.optional(v.nullable(v.unknown())) },
    b: { w: v.optional(v.nullable(v.unknown())) },
    c: { w: v.optional(v.nullable(v.unknown())) },
  });
  await db.collection("things").insertMany(structuredClone(DOCS) as never[]);
  return things;
}

test("cross-type paginate: mixed brackets per _type — forward walk with counts", async (t) => {
  await withDatabase(t.name, async (db) => {
    const things = await setup(db);

    for (const dir of [1, -1] as const) {
      const expected = await truth(db, ["a", "b", "c"], dir);
      const limit = 2;
      const seen: string[] = [];
      let afterId: string | undefined;
      let offset = 0;
      for (let guard = 0; guard < 20; guard++) {
        const p = await things.paginate(
          ["a", "b", "c"],
          {},
          {
            sort: { w: dir },
            limit,
            ...(afterId ? { afterId } : {}),
          },
        );
        assertEquals(p.total, expected.length, `dir ${dir}: total drifted`);
        assertEquals(
          p.position,
          offset,
          `dir ${dir}: position wrong at offset ${offset}`,
        );
        assertEquals(
          p.data.map((d) => String(d._id)),
          expected.slice(offset, offset + limit),
          `dir ${dir}: page at offset ${offset} diverged from $sort`,
        );
        for (const d of p.data) seen.push(String(d._id));
        offset += p.data.length;
        if (p.data.length < limit) break;
        afterId = String(p.data[p.data.length - 1]._id);
      }
      assertEquals(seen, expected, `dir ${dir}: walk did not cover the set`);
    }
  });
});

test("cross-type paginate: mixed brackets per _type — backward round-trip", async (t) => {
  await withDatabase(t.name, async (db) => {
    const things = await setup(db);

    for (const dir of [1, -1] as const) {
      const expected = await truth(db, ["a", "b", "c"], dir);
      const limit = 3;
      // Walk backward from the last doc; pages must tile the set minus the
      // anchor, in forward order.
      const seen: string[] = [];
      let beforeId: string | undefined = expected[expected.length - 1];
      for (let guard = 0; guard < 20; guard++) {
        const p: { data: { _id: string }[]; position?: number } =
          await things.paginate(
            ["a", "b", "c"],
            {},
            {
              sort: { w: dir },
              limit,
              beforeId,
            },
          );
        if (p.data.length === 0) break;
        seen.unshift(...p.data.map((d) => String(d._id)));
        assertEquals(
          p.position,
          expected.indexOf(String(p.data[0]._id)),
          `dir ${dir}: backward position drifted`,
        );
        beforeId = String(p.data[0]._id);
      }
      assertEquals(
        seen,
        expected.slice(0, -1),
        `dir ${dir}: backward walk did not tile the set`,
      );
    }
  });
});

test("cross-type paginate: type subset never leaks the excluded type", async (t) => {
  await withDatabase(t.name, async (db) => {
    const things = await setup(db);

    for (const dir of [1, -1] as const) {
      const expected = await truth(db, ["a", "c"], dir);
      const seen: string[] = [];
      let afterId: string | undefined;
      for (let guard = 0; guard < 20; guard++) {
        const p = await things.paginate(
          ["a", "c"],
          {},
          {
            sort: { w: dir },
            limit: 2,
            ...(afterId ? { afterId } : {}),
          },
        );
        if (p.data.length === 0) break;
        for (const d of p.data) {
          seen.push(String(d._id));
        }
        if (p.data.length < 2) break;
        afterId = String(p.data[p.data.length - 1]._id);
      }
      assertEquals(seen, expected, `dir ${dir}: subset walk diverged`);
      assertEquals(
        seen.some((id) => id.startsWith("b:")),
        false,
        `dir ${dir}: excluded type leaked through the cursor`,
      );
    }
  });
});

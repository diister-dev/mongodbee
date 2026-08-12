// Verrou — paging by a field that holds SEVERAL BSON types must walk the
// whole set in the server's own $sort order.
//
// Regression it guards: query `$gt`/`$lt` are type-bracketed — they never
// compare across BSON brackets — while `$sort` ranks the brackets (null <
// numbers < strings < objects < binData < objectId < bool < dates <
// timestamps). A cursor anchored on a number therefore never reached the
// strings, dates, … above it: the walk silently ended with most of the set
// unvisited. One schema migration that changed a field's type is enough to
// put two brackets in one collection. The rungs now carry a `$type` branch
// for the brackets ranked past the anchor's (see cursorRungBranches).
//
// Ground truth is MongoDB's own `$sort` on the same data — the type-order
// table is locked against the server, not against our reading of the docs.
//
// Anchors that CANNOT participate in a query-operator ladder (arrays: query
// operators match per element while $sort ranks by min/max element; regexes:
// an equality pin would be a pattern match) fail LOUD instead of silently
// corrupting every following page.

import { assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import * as v from "../src/schema.ts";
import { dbId } from "../src/ids.ts";
import { Binary, Long, ObjectId, Timestamp } from "mongodb";

const ThingSchema = {
  _id: dbId("thing"),
  label: v.string(),
  val: v.optional(v.unknown()),
};

/** One value per BSON bracket, duplicates included — the boundary zoo. */
const VALUES: unknown[] = [
  undefined, // missing
  undefined,
  null,
  null,
  NaN, // $sort ranks it below every number; range operators never match it
  NaN,
  1,
  2.5,
  2.5, // duplicate number (tie-break inside a bracket)
  Long.fromNumber(3),
  "alpha",
  "beta",
  "alpha", // duplicate string
  { x: 1 },
  { x: 2 },
  new Binary(new Uint8Array([1, 2, 3])),
  new ObjectId("65f000000000000000000001"),
  new ObjectId("65f000000000000000000002"),
  false,
  true,
  new Date(Date.UTC(2026, 0, 1)),
  new Date(Date.UTC(2026, 0, 2)),
  new Date(Date.UTC(2026, 0, 1)), // duplicate date
  Timestamp.fromBits(1, 1),
];

async function seed(db: Parameters<Parameters<typeof withDatabase>[1]>[0]) {
  const things = await collection(db, "things", ThingSchema);
  for (let i = 0; i < VALUES.length; i++) {
    await things.insertOne(
      {
        label: `t-${i}`,
        ...(VALUES[i] === undefined ? {} : { val: VALUES[i] }),
      } as never,
    );
  }
  return things;
}

// deno-lint-ignore no-explicit-any
async function groundTruth(db: any, dir: 1 | -1): Promise<string[]> {
  const rows = await db.collection("things").aggregate([
    { $sort: { val: dir, _id: dir } },
  ]).toArray();
  return (rows as { _id: string }[]).map((r) => String(r._id));
}

Deno.test("paginate: a mixed-type sort field walks the whole set in $sort order", async (t) => {
  await withDatabase(t.name, async (db) => {
    const things = await seed(db);

    for (const dir of [1, -1] as const) {
      const truth = await groundTruth(db, dir);
      for (const limit of [1, 3]) {
        const walked: string[] = [];
        let afterId: string | undefined = undefined;
        for (let guard = 0; guard < 60; guard++) {
          // deno-lint-ignore no-explicit-any
          const page: any = await things.paginate({}, {
            sort: { val: dir },
            limit,
            skipTotal: true,
            afterId,
          });
          if (page.data.length === 0) break;
          for (const d of page.data) walked.push(String(d._id));
          afterId = String(page.data[page.data.length - 1]._id);
        }
        assertEquals(
          walked.length,
          truth.length,
          `dir ${dir} limit ${limit}: visited ${walked.length} of ` +
            `${truth.length} — the cursor dead-ended at a type boundary`,
        );
        assertEquals(
          walked,
          truth,
          `dir ${dir} limit ${limit}: walk order diverged from $sort`,
        );
      }
    }
  });
});

Deno.test("paginate: an array or regex anchor fails loud, not silently wrong", async (t) => {
  await withDatabase(t.name, async (db) => {
    const things = await collection(db, "things", ThingSchema);
    const arrayId = await things.insertOne(
      { label: "arr", val: [1, 2] } as never,
    ) as string;
    const regexId = await things.insertOne(
      { label: "rx", val: /abc/ } as never,
    ) as string;

    await assertRejects(
      // deno-lint-ignore no-explicit-any
      () => things.paginate({}, { sort: { val: 1 }, afterId: arrayId } as any),
      Error,
      "ARRAY",
    );
    await assertRejects(
      // deno-lint-ignore no-explicit-any
      () => things.paginate({}, { sort: { val: 1 }, afterId: regexId } as any),
      Error,
      "REGEX",
    );
  });
});

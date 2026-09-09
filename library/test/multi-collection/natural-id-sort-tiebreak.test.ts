// Verrou — cross-type naturalIdSort must keep a total order when two types
// carry the SAME id suffix (custom ids are legal: `a:main` / `b:main`).
//
// Regression it guards: the effective sort used to be `{_ulid: <dir>}` with
// no tie-break — `_ulid` is a substring of `_id` and is NOT unique across
// types. `$sort` over the tie had no stable order, and the cursor rung
// `{_ulid: {$gt: anchor}}` skips equal values entirely: a 5-doc walk with
// twin suffixes silently returned 3 documents (measured on the parent
// commit). `_id` (which embeds the type) is re-appended as the final
// tie-break, following `_ulid`'s direction.
//
// Ground truth is the server's own $sort over the extracted `_ulid` with the
// `_id` tie-break — never a hand-written order.

import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { multiCollection } from "../../src/multi-collection.ts";
import * as v from "../../src/schema.ts";

const DOCS = [
  { _id: "a:s1", _type: "a", name: "a-s1" },
  { _id: "b:s1", _type: "b", name: "b-s1" },
  { _id: "a:s2", _type: "a", name: "a-s2" },
  { _id: "b:s2", _type: "b", name: "b-s2" },
  { _id: "a:s3", _type: "a", name: "a-s3" },
];

async function truth(db: any, dir: 1 | -1): Promise<string[]> {
  const rows = await db
    .collection("people")
    .aggregate([
      {
        $addFields: {
          _ulid: {
            $substr: ["$_id", { $add: [{ $indexOfCP: ["$_id", ":"] }, 1] }, -1],
          },
        },
      },
      { $sort: { _ulid: dir, _id: dir } },
    ])
    .toArray();
  return (rows as { _id: string }[]).map((r) => r._id);
}

test("naturalIdSort: twin id suffixes across types — full walk, no loss, no dupes", async () => {
  await withDatabase("mc-natural-tiebreak", async (db) => {
    const people = await multiCollection(db, "people", {
      a: { name: v.string() },
      b: { name: v.string() },
    });
    await db.collection("people").insertMany(structuredClone(DOCS) as never[]);

    for (const dir of [1, -1] as const) {
      const expected = await truth(db, dir);

      // Forward walk, one doc per page — every twin boundary is a cursor
      // anchor.
      const seen: string[] = [];
      let afterId: string | undefined;
      for (let guard = 0; guard < 10; guard++) {
        const p = await people.paginate(
          ["a", "b"],
          {},
          {
            naturalIdSort: true,
            sort: { _id: dir },
            limit: 1,
            ...(afterId ? { afterId } : {}),
          },
        );
        if (p.data.length === 0) break;
        for (const d of p.data) seen.push(d._id);
        afterId = p.data[p.data.length - 1]._id;
      }
      assertEquals(seen, expected, `forward walk (dir ${dir}) diverged`);

      // Backward walk from the last doc: same set, reversed page order.
      const seenBack: string[] = [];
      let beforeId: string | undefined = expected[expected.length - 1];
      for (let guard = 0; guard < 10; guard++) {
        const p: { data: { _id: string }[] } = await people.paginate(
          ["a", "b"],
          {},
          {
            naturalIdSort: true,
            sort: { _id: dir },
            limit: 1,
            beforeId,
          },
        );
        if (p.data.length === 0) break;
        seenBack.unshift(...p.data.map((d) => d._id));
        beforeId = p.data[0]._id;
      }
      assertEquals(
        seenBack,
        expected.slice(0, -1),
        `backward walk (dir ${dir}) diverged`,
      );
    }
  });
});

// Verrou — single-type naturalIdSort must behave as the plain `_id` sort:
// identical order (every _id of one type shares the `${type}:` prefix, so
// suffix order IS _id order) and NO computed-field pipeline.
//
// Regression it guards: the single-type case used to take the cross-type
// `_ulid` path — $addFields + $match + blocking $sort over the WHOLE
// filtered set on EVERY page (measured on the parent commit at 10k docs,
// page of 25: 10 000 docsExamined + hasSortStage per page; ~11ms/page
// server-side). The order equality is locked against the computed-`_ulid`
// pipeline's own output on adversarial suffixes (variable length, digits,
// mixed case), so the fast path can never drift from what the slow path
// would have returned.

import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { multiCollection } from "../../src/multi-collection.ts";
import * as v from "../../src/schema.ts";
import { newId } from "../../src/ids.ts";

const SUFFIXES = ["a", "ab", "B2", "b10", "0", "zz", "Z", "09x"];

Deno.test("naturalIdSort single type: order identical to the computed _ulid pipeline", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await multiCollection(db, "people", {
      p: { name: v.string() },
      q: { name: v.string() },
    });
    const raw = db.collection("people");
    await raw.insertMany(
      [
        ...SUFFIXES.map((s) => ({
          _id: `p:${s}`,
          _type: "p",
          name: `p-${s}`,
        })),
        // Noise from another type — must not appear in a single-type walk.
        { _id: "q:a", _type: "q", name: "q-a" },
        { _id: "q:zz", _type: "q", name: "q-zz" },
      ] as never[],
    );

    for (const dir of [1, -1] as const) {
      // Ground truth: the computed-_ulid pipeline the cross-type path runs.
      const rows = await raw.aggregate([
        { $match: { _type: "p" } },
        {
          $addFields: {
            _ulid: {
              $substr: [
                "$_id",
                { $add: [{ $indexOfCP: ["$_id", ":"] }, 1] },
                -1,
              ],
            },
          },
        },
        { $sort: { _ulid: dir, _id: dir } },
      ]).toArray();
      const expected = rows.map((r) => String(r._id));

      const seen: string[] = [];
      let afterId: string | undefined;
      for (let guard = 0; guard < 12; guard++) {
        const p = await people.paginate(["p"], {}, {
          naturalIdSort: true,
          sort: { _id: dir },
          limit: 2,
          ...(afterId ? { afterId } : {}),
        });
        if (p.data.length === 0) break;
        for (const d of p.data) seen.push(String(d._id));
        if (p.data.length < 2) break;
        afterId = String(p.data[p.data.length - 1]._id);
      }
      assertEquals(seen, expected, `dir ${dir}: fast path diverged`);

      // Backward from the last doc tiles the same order.
      const seenBack: string[] = [];
      let beforeId: string | undefined = expected[expected.length - 1];
      for (let guard = 0; guard < 12; guard++) {
        const p: { data: { _id: string }[] } = await people.paginate(
          ["p"],
          {},
          {
            naturalIdSort: true,
            sort: { _id: dir },
            limit: 2,
            beforeId,
          },
        );
        if (p.data.length === 0) break;
        seenBack.unshift(...p.data.map((d) => String(d._id)));
        beforeId = String(p.data[0]._id);
      }
      assertEquals(
        seenBack,
        expected.slice(0, -1),
        `dir ${dir}: backward fast path diverged`,
      );
    }
  });
});

Deno.test("naturalIdSort single type: no blocking sort, bounded reads", async () => {
  await withDatabase("mc-natural-single-perf", async (db) => {
    const people = await multiCollection(db, "people", {
      p: { name: v.string() },
      q: { name: v.string() },
    });
    const raw = db.collection("people");
    const docs: Record<string, unknown>[] = [];
    for (let i = 0; i < 2000; i++) {
      const type = i % 2 === 0 ? "p" : "q";
      docs.push({ _id: `${type}:${newId()}`, _type: type, name: `n-${i}` });
    }
    await raw.insertMany(docs as never[]);

    await db.collection("system.profile").drop().catch(() => {});
    await db.command({ profile: 2 });
    let afterId: string | undefined;
    for (let page = 0; page < 4; page++) {
      const p = await people.paginate(["p"], {}, {
        naturalIdSort: true,
        limit: 25,
        skipTotal: true,
        ...(afterId ? { afterId } : {}),
      });
      if (p.data.length === 0) break;
      afterId = String(p.data[p.data.length - 1]._id);
    }
    await db.command({ profile: 0 });

    const profile = await db.collection("system.profile").find({
      ns: `${db.databaseName}.people`,
    }).toArray();
    assert(profile.length >= 4, "walk ops missing from the profile");
    for (const op of profile) {
      assert(
        op.hasSortStage !== true,
        `a walk op needed a blocking sort: ${JSON.stringify(op.command)}`,
      );
      // Some profile rows (getMore, killCursors) carry no docsExamined —
      // the bound only applies to rows that report one.
      if (typeof op.docsExamined !== "number") continue;
      assert(
        op.docsExamined <= 150,
        `a walk op examined ${op.docsExamined} docs — the single-type fast ` +
          `path lost its bounded read (parent behavior: 2000 per page)`,
      );
    }
  });
});

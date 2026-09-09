// Verrou — full paginate conformance on one boundary-heavy dataset: multi-key
// sorts with MIXED directions over two optional fields (null + missing +
// duplicates at every tie level), walked forward and backward, on all three
// surfaces, with `total`/`position` asserted on every page against MongoDB's
// own $sort order.
//
// What this locks that the per-defect verrous do not:
// - the per-rung direction flip of the ladder for `{a: 1, b: -1}`-style
//   sorts, and the equality pinning of previous keys across their own
//   null/missing boundaries;
// - `total` and `position` under the $or cursor branches, page by page;
// - backward (`beforeId`) paging across the same boundaries, and a
//   forward-then-backward round-trip landing on identical pages.
//
// Every expectation is a slice of the server's own `$sort` output — never a
// hand-written order. The `_id` tie-break follows the LAST explicit field's
// direction, mirroring normalizePaginateSort.

import { test } from "./+harness.ts";
import { assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { dbId, refId } from "../src/ids.ts";

const EXPO = "exposition:expoaaaaa01";

/** Optional×optional rows: null, missing and duplicates at every tie level. */
const ROWS: { a?: number | null; b?: string | null }[] = [
  {},
  {},
  { a: null },
  { a: null, b: "x" },
  { b: null },
  { a: 1 },
  { a: 1, b: null },
  { a: 1, b: "x" },
  { a: 1, b: "x" },
  { a: 1, b: "y" },
  { a: 2, b: "x" },
  { a: 2, b: "y" },
  { a: 2, b: "y" },
  { a: 2 },
  { a: 3, b: "x" },
  { a: 3, b: "z" },
  { a: 3 },
  { a: 3, b: null },
  { a: 2, b: "z" },
  { a: 1, b: "z" },
  { b: "x" },
  { b: null, a: null },
];

const SORTS: Record<string, 1 | -1>[] = [
  { a: 1, b: 1 },
  { a: 1, b: -1 },
  { a: -1, b: 1 },
  { a: -1, b: -1 },
];

type Page = { data: { _id: string }[]; total?: number; position?: number };
type PageFn = (opts: {
  limit: number;
  sort: Record<string, 1 | -1>;
  afterId?: string;
  beforeId?: string;
}) => Promise<Page>;

/** Effective sort with the direction-following `_id` tie-break appended. */
function effectiveSort(sort: Record<string, 1 | -1>): Record<string, 1 | -1> {
  const fields = Object.keys(sort);
  return { ...sort, _id: sort[fields[fields.length - 1]] };
}

async function truthFor(
  db: any,
  coll: string,
  baseMatch: Record<string, unknown>,
  sort: Record<string, 1 | -1>,
): Promise<string[]> {
  const rows = await db
    .collection(coll)
    .aggregate([{ $match: baseMatch }, { $sort: effectiveSort(sort) }])
    .toArray();
  return (rows as { _id: string }[]).map((r) => String(r._id));
}

/** Forward walk asserting order, total and position on EVERY page. */
async function assertForwardWalk(
  page: PageFn,
  sort: Record<string, 1 | -1>,
  limit: number,
  truth: string[],
  label: string,
) {
  let afterId: string | undefined;
  let offset = 0;
  for (let guard = 0; guard < 40; guard++) {
    const p = await page({ limit, sort, ...(afterId ? { afterId } : {}) });
    assertEquals(p.total, truth.length, `${label}: total drifted`);
    assertEquals(
      p.position,
      offset,
      `${label}: position wrong at offset ${offset}`,
    );
    assertEquals(
      p.data.map((d) => String(d._id)),
      truth.slice(offset, offset + limit),
      `${label}: page at offset ${offset} diverged from $sort`,
    );
    offset += p.data.length;
    if (p.data.length < limit) break;
    afterId = String(p.data[p.data.length - 1]._id);
  }
  assertEquals(offset, truth.length, `${label}: walk did not cover the set`);
}

/**
 * Backward walk from the document at `truth[anchorIdx]`: pages must be the
 * slices immediately BEFORE the anchor, in forward order, with `position`
 * equal to each page's start offset — the round-trip mirror of the forward
 * walk over the same boundaries.
 */
async function assertBackwardWalk(
  page: PageFn,
  sort: Record<string, 1 | -1>,
  limit: number,
  truth: string[],
  anchorIdx: number,
  label: string,
) {
  let end = anchorIdx; // exclusive end of the expected previous page
  let beforeId = truth[anchorIdx];
  for (let guard = 0; guard < 40 && end > 0; guard++) {
    const p = await page({ limit, sort, beforeId });
    const start = Math.max(0, end - limit);
    assertEquals(
      p.data.map((d) => String(d._id)),
      truth.slice(start, end),
      `${label}: backward page [${start}, ${end}) diverged`,
    );
    assertEquals(p.total, truth.length, `${label}: backward total drifted`);
    assertEquals(
      p.position,
      start,
      `${label}: backward position wrong at ${start}`,
    );
    if (start === 0) break;
    beforeId = truth[start];
    end = start;
  }
}

async function runConformance(
  page: PageFn,
  truthOf: (sort: Record<string, 1 | -1>) => Promise<string[]>,
  surface: string,
) {
  for (const sort of SORTS) {
    const truth = await truthOf(sort);
    assertEquals(truth.length, ROWS.length, `${surface}: seed drifted`);
    const label = `${surface} sort ${JSON.stringify(sort)}`;
    for (const limit of [3, 5]) {
      await assertForwardWalk(
        page,
        sort,
        limit,
        truth,
        `${label} lim ${limit}`,
      );
    }
    await assertBackwardWalk(page, sort, 4, truth, 13, label);
    await assertBackwardWalk(page, sort, 5, truth, 5, label);
  }
}

test("paginate conformance (collection): mixed-direction multi-key, both directions, positions exact", async (t) => {
  await withDatabase(t.name, async (db) => {
    const things = await collection(db, "things", {
      _id: dbId("thing"),
      a: v.optional(v.nullable(v.number())),
      b: v.optional(v.nullable(v.string())),
    });
    for (const row of ROWS) await things.insertOne(row as never);

    await runConformance(
      (o) => things.paginate({}, o as any) as Promise<Page>,
      (sort) => truthFor(db, "things", {}, sort),
      "collection",
    );
  });
});

test("paginate conformance (multiCollection): mixed-direction multi-key, both directions, positions exact", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await multiCollection(db, "catalog", {
      participant: {
        a: v.optional(v.nullable(v.number())),
        b: v.optional(v.nullable(v.string())),
      },
    });
    for (const row of ROWS) {
      await catalog.insertOne("participant", row as never);
    }

    await runConformance(
      (o) => catalog.paginate("participant", {}, o as any) as Promise<Page>,
      (sort) => truthFor(db, "catalog", { _type: "participant" }, sort),
      "multi",
    );
  });
});

test("paginate conformance (scoped): mixed-direction multi-key, both directions, positions exact", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        participant: {
          a: v.optional(v.nullable(v.number())),
          b: v.optional(v.nullable(v.string())),
        },
      },
    });
    const view = catalog.scope(EXPO);
    for (const row of ROWS) {
      await view.insertOne("participant", row as never);
    }

    await runConformance(
      (o) => view.paginate("participant", undefined, o as any) as Promise<Page>,
      (sort) =>
        truthFor(db, "catalog", { _scope: EXPO, _type: "participant" }, sort),
      "scoped",
    );
  });
});

// sortPipeline strategy: the sort keys cross a $lookup boundary (one joined
// field DESC, one own optional field ASC — mixed directions over the hidden
// normalized keys), with parents lacking the joined doc entirely.
test("paginate conformance (scoped sortPipeline): mixed-direction joined+own keys, positions exact, backward round-trip", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        participant: {
          name: v.string(),
          a: v.optional(v.nullable(v.number())),
        },
        badge: { participantId: v.string(), generatedAt: v.date() },
      },
    });
    const view = catalog.scope(EXPO);

    const N = 18;
    for (let i = 0; i < N; i++) {
      const pid = await view.insertOne("participant", {
        name: `p-${String(i).padStart(2, "0")}`,
        // duplicates + missing on the own key
        ...(i % 4 === 0 ? {} : { a: i % 3 }),
      });
      // A third of the parents have no badge at all.
      if (i % 3 !== 0) {
        await view.insertOne("badge", {
          participantId: pid,
          // heavy duplicates on the joined key
          generatedAt: new Date(Date.UTC(2026, 0, 1 + (i % 4))),
        });
      }
    }

    const badgeSortPipeline = (s: any) => [
      s.lookup("badge", "_id", "participantId", { as: "badges" }),
      s.addFields({ badgeDoc: { $first: "$badges" } }),
    ];
    const sort = { "badgeDoc.generatedAt": -1, a: 1 } as const;

    const truthRows = await view.aggregate((s) => [
      s.match("participant", {}),
      ...badgeSortPipeline(s as any),
      s.sort({ "badgeDoc.generatedAt": -1, a: 1, _id: 1 }),
    ]);
    const truth = (truthRows as { _id: string }[]).map((r) => r._id);
    assertEquals(truth.length, N);

    const page: PageFn = (o) =>
      view.paginate("participant", undefined, {
        ...o,
        sortPipeline: badgeSortPipeline,
      } as any) as Promise<Page>;

    for (const limit of [4, 7]) {
      await assertForwardWalk(
        page,
        sort as Record<string, 1 | -1>,
        limit,
        truth,
        `sortPipeline lim ${limit}`,
      );
    }
    await assertBackwardWalk(
      page,
      sort as Record<string, 1 | -1>,
      4,
      truth,
      10,
      "sortPipeline",
    );
  });
});

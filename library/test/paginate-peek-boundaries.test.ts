// Verrou — `peek`/`hasMore` at every null/missing/type boundary, against the
// server's own $sort ground truth.
//
// The page walk itself is locked by the conformance and cross-type suites;
// `peek` rides the same ladder but adds its own mechanics (fetch limit+1,
// pop the extra row, report `hasMore`, keep `position` consistent after the
// pop). Nothing locked those mechanics at the exact boundaries where the
// ladder switches shape — the null block, the bracket edges, NaN, the first
// and last anchor of the set. This sweep anchors a peeked page on EVERY
// position of a boundary-heavy dataset, forward and backward, and asserts
// `data`, `hasMore`, `total` and `position` on each.
//
// hasMore ground truth: forward = rows remain past the returned page;
// backward = rows remain BEFORE the returned page (the walk direction).

import { assertEquals } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { dbId, refId } from "../src/ids.ts";
import { Binary, Long, ObjectId, Timestamp } from "mongodb";

const LIMIT = 3;

type Page = {
  data: { _id: string }[];
  total?: number;
  position?: number;
  hasMore?: boolean;
};
type PageFn = (opts: {
  afterId?: string;
  beforeId?: string;
  skipTotal?: boolean;
}) => Promise<Page>;

/**
 * Sweep every anchor of `truth`, forward and backward, asserting the peeked
 * page's data slice and hasMore (+ total/position when the caller keeps
 * counts on).
 */
async function sweepPeek(
  page: PageFn,
  truth: string[],
  label: string,
  counts: boolean,
) {
  const n = truth.length;
  // Page 1 (no anchor).
  const first = await page({ skipTotal: !counts });
  assertEquals(
    first.data.map((d) => String(d._id)),
    truth.slice(0, LIMIT),
    `${label}: page 1 data`,
  );
  assertEquals(first.hasMore, n > LIMIT, `${label}: page 1 hasMore`);
  if (counts) {
    assertEquals(first.total, n, `${label}: page 1 total`);
    assertEquals(first.position, 0, `${label}: page 1 position`);
  }
  for (let i = 0; i < n; i++) {
    const fwd = await page({ afterId: truth[i], skipTotal: !counts });
    assertEquals(
      fwd.data.map((d) => String(d._id)),
      truth.slice(i + 1, i + 1 + LIMIT),
      `${label}: afterId@${i} data`,
    );
    assertEquals(
      fwd.hasMore,
      n - 1 - i > LIMIT,
      `${label}: afterId@${i} hasMore`,
    );
    if (counts) {
      assertEquals(fwd.total, n, `${label}: afterId@${i} total`);
      assertEquals(fwd.position, i + 1, `${label}: afterId@${i} position`);
    }

    const back = await page({ beforeId: truth[i], skipTotal: !counts });
    assertEquals(
      back.data.map((d) => String(d._id)),
      truth.slice(Math.max(0, i - LIMIT), i),
      `${label}: beforeId@${i} data`,
    );
    assertEquals(back.hasMore, i > LIMIT, `${label}: beforeId@${i} hasMore`);
    if (counts) {
      assertEquals(back.total, n, `${label}: beforeId@${i} total`);
      assertEquals(
        back.position,
        Math.max(0, i - LIMIT),
        `${label}: beforeId@${i} position`,
      );
    }
  }
}

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
  { a: 2 },
  { a: 3, b: "z" },
  { a: 3 },
  { b: "x" },
];

Deno.test("peek: null/missing boundaries — every anchor, both directions, counts on", async (t) => {
  await withDatabase(t.name, async (db) => {
    const items = await collection(db, "items", {
      _id: dbId("item"),
      a: v.optional(v.nullable(v.number())),
      b: v.optional(v.nullable(v.string())),
    });
    for (const row of ROWS) await items.insertOne(row as never);

    for (
      const sort of [{ a: 1, b: -1 }, { a: -1, b: 1 }] as Record<
        string,
        1 | -1
      >[]
    ) {
      const tieDir = sort.b;
      const rows = await db.collection("items").aggregate([
        { $sort: { ...sort, _id: tieDir } },
      ]).toArray();
      const truth = rows.map((r) => String(r._id));

      await sweepPeek(
        (opts) =>
          // deno-lint-ignore no-explicit-any
          items.paginate({}, {
            sort,
            limit: LIMIT,
            peek: true,
            ...opts,
            // deno-lint-ignore no-explicit-any
          } as any) as Promise<Page>,
        truth,
        `collection sort=${JSON.stringify(sort)}`,
        true,
      );
    }
  });
});

/** One value per BSON bracket, duplicates included — the boundary zoo. */
const ZOO: unknown[] = [
  undefined,
  undefined,
  null,
  NaN,
  NaN,
  1,
  2.5,
  2.5,
  Long.fromNumber(3),
  "alpha",
  "beta",
  "alpha",
  { x: 1 },
  new Binary(new Uint8Array([1, 2, 3])),
  new ObjectId("65f000000000000000000001"),
  false,
  true,
  new Date(Date.UTC(2026, 0, 1)),
  new Date(Date.UTC(2026, 0, 2)),
  new Date(Date.UTC(2026, 0, 1)),
  Timestamp.fromBits(1, 1),
];

Deno.test("peek: cross-BSON-type boundaries — every anchor, both directions", async (t) => {
  await withDatabase(t.name, async (db) => {
    const things = await collection(db, "things", {
      _id: dbId("thing"),
      label: v.string(),
      val: v.optional(v.unknown()),
    });
    for (let i = 0; i < ZOO.length; i++) {
      await things.insertOne(
        {
          label: `t-${i}`,
          ...(ZOO[i] === undefined ? {} : { val: ZOO[i] }),
        } as never,
      );
    }

    for (const dir of [1, -1] as const) {
      const rows = await db.collection("things").aggregate([
        { $sort: { val: dir, _id: dir } },
      ]).toArray();
      const truth = rows.map((r) => String(r._id));

      await sweepPeek(
        (opts) =>
          // deno-lint-ignore no-explicit-any
          things.paginate({}, {
            sort: { val: dir },
            limit: LIMIT,
            peek: true,
            ...opts,
            // deno-lint-ignore no-explicit-any
          } as any) as Promise<Page>,
        truth,
        `zoo dir=${dir}`,
        false,
      );
    }
  });
});

Deno.test("peek: multi surface, two _types sharing an optional field", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await multiCollection(db, "people", {
      a: { w: v.optional(v.number()) },
      b: { w: v.optional(v.number()) },
    });
    // Interleave values, ties across types, and missing on both types.
    const raw = db.collection("people");
    await raw.insertMany([
      { _id: "a:p1", _type: "a", w: 1 },
      { _id: "b:p1", _type: "b", w: 1 },
      { _id: "a:p2", _type: "a" },
      { _id: "b:p2", _type: "b" },
      { _id: "a:p3", _type: "a", w: 2 },
      { _id: "b:p3", _type: "b", w: 3 },
      { _id: "a:p4", _type: "a", w: 3 },
      { _id: "b:p4", _type: "b" },
    ] as never[]);

    for (const dir of [1, -1] as const) {
      const rows = await raw.aggregate([
        { $sort: { w: dir, _id: dir } },
      ]).toArray();
      const truth = rows.map((r) => String(r._id));

      await sweepPeek(
        (opts) =>
          people.paginate(["a", "b"], {}, {
            sort: { w: dir },
            limit: LIMIT,
            peek: true,
            ...opts,
          }) as Promise<Page>,
        truth,
        `multi cross-type dir=${dir}`,
        true,
      );
    }
  });
});

Deno.test("peek: scoped surface across the null boundary", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: { item: { w: v.optional(v.number()) } },
    });
    const view = catalog.scope("exposition:expoaaaaa01");
    for (const w of [undefined, undefined, 1, 1, 2, 3, undefined, 4]) {
      await view.insertOne("item", (w === undefined ? {} : { w }) as never);
    }

    for (const dir of [1, -1] as const) {
      const rows = await db.collection("catalog").aggregate([
        { $sort: { w: dir, _id: dir } },
      ]).toArray();
      const truth = rows.map((r) => String(r._id));

      await sweepPeek(
        (opts) =>
          view.paginate("item", undefined, {
            sort: { w: dir },
            limit: LIMIT,
            peek: true,
            ...opts,
            // deno-lint-ignore no-explicit-any
          } as any) as Promise<Page>,
        truth,
        `scoped dir=${dir}`,
        true,
      );
    }
  });
});

Deno.test("peek: sortPipeline path across the missing-join boundary", async (t) => {
  await withDatabase(t.name, async (db) => {
    const items = await collection(db, "items", {
      _id: dbId("item"),
      refId: v.optional(v.string()),
      name: v.string(),
    });
    // A third of the parents have no joined doc — their sort key normalizes
    // to null and sits at the boundary the hidden-key machinery guards.
    const meta = db.collection("meta");
    for (let i = 0; i < 9; i++) {
      await items.insertOne(
        {
          name: `n-${i}`,
          ...(i % 3 === 0 ? {} : { refId: `m-${i}` }),
        } as never,
      );
      if (i % 3 !== 0) {
        await meta.insertOne({ key: `m-${i}`, rank: (i * 7) % 5 });
      }
    }

    const rows = await db.collection("items").aggregate([
      {
        $lookup: {
          from: "meta",
          localField: "refId",
          foreignField: "key",
          as: "metaDocs",
        },
      },
      { $addFields: { meta: { $first: "$metaDocs" } } },
      // $sort ranks a missing "meta.rank" with null, so the raw path is a
      // faithful ground truth for the normalized hidden key.
      { $sort: { "meta.rank": -1, _id: -1 } },
    ]).toArray();
    const truth = rows.map((r) => String(r._id));

    await sweepPeek(
      (opts) =>
        // deno-lint-ignore no-explicit-any
        items.paginate({}, {
          sort: { "meta.rank": -1 } as Record<string, 1 | -1>,
          // deno-lint-ignore no-explicit-any
          sortPipeline: (s: any) => [
            s.externalLookup("meta", "refId", "key", { as: "metaDocs" }),
            s.addFields({ meta: { $first: "$metaDocs" } }),
          ],
          limit: LIMIT,
          peek: true,
          ...opts,
          // deno-lint-ignore no-explicit-any
        } as any) as Promise<Page>,
      truth,
      "sortPipeline",
      false,
    );
  });
});

Deno.test("peek + filter(doc): hasMore means a non-empty NEXT page, filter included", async (t) => {
  await withDatabase(t.name, async (db) => {
    const items = await collection(db, "items", {
      _id: dbId("item"),
      n: v.number(),
    });
    for (let i = 0; i < 10; i++) await items.insertOne({ n: i });
    const evens = ({ n }: { n: number }) => n % 2 === 0; // 5 pass

    // limit 3, evens only: page 1 = 0,2,4 and 6,8 remain → hasMore true.
    // deno-lint-ignore no-explicit-any
    const p1: any = await items.paginate({}, {
      limit: 3,
      peek: true,
      skipTotal: true,
      filter: evens,
      // deno-lint-ignore no-explicit-any
    } as any);
    assertEquals(p1.data.map((d: { n: number }) => d.n), [0, 2, 4]);
    assertEquals(p1.hasMore, true, "two passing rows remain");

    // After 4: exactly 6,8 pass ≤ limit → hasMore false even though raw
    // rows 5..9 remain past the page.
    // deno-lint-ignore no-explicit-any
    const p2: any = await items.paginate({}, {
      limit: 3,
      peek: true,
      skipTotal: true,
      filter: evens,
      afterId: String(p1.data[2]._id),
      // deno-lint-ignore no-explicit-any
    } as any);
    assertEquals(p2.data.map((d: { n: number }) => d.n), [6, 8]);
    assertEquals(p2.hasMore, false, "no passing row remains");
  });
});

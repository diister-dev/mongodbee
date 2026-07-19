// paginate() with a CUSTOM sort on a NESTED (dot-path) field. The compound
// cursor ladder resolves anchor values through getNestedValue(doc, "meta.rank")
// and emits `{ "meta.rank": ... }` match conditions. The existing custom-sort
// coverage (scoped-multi-collection-paginate-sort.test.ts) only sorts on a
// TOP-LEVEL field (`seat`), so the dot-path branch is exercised here.
//
// Method: seed docs whose nested sort value is DUPLICATED (so the `_id`
// tie-breaker must decide order across ties), walk every page via afterId in
// tiny pages (>=3 pages), and assert the concatenation equals MongoDB's own
// single-page authoritative order.

import { assertEquals } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO = "exposition:expoaaaaa01";

async function makeCatalog(
  db: Parameters<Parameters<typeof withDatabase>[1]>[0],
) {
  return await scopedMultiCollection(db, "catalog", {
    scope: refId("exposition"),
    types: {
      participant: {
        name: v.string(),
        // Nested object → sort target is the dot path "meta.rank".
        meta: v.object({ rank: v.number(), tier: v.string() }),
      },
    },
  });
}

/**
 * Seed `count` participants whose nested `meta.rank` repeats (rank = floor(i/3))
 * so many docs share a sort value and the `_id` tie-breaker decides their order.
 */
async function seed(
  // deno-lint-ignore no-explicit-any
  view: any,
  count: number,
): Promise<void> {
  for (let i = 0; i < count; i++) {
    await view.insertOne("participant", {
      name: `P${String(i).padStart(3, "0")}`,
      meta: { rank: Math.floor(i / 3), tier: i % 2 === 0 ? "gold" : "silver" },
    });
  }
}

/** Walk every page via afterId in pages of `limit`; return concatenated ids. */
async function walkAll(
  // deno-lint-ignore no-explicit-any
  view: any,
  sort: Record<string, 1 | -1>,
  limit: number,
): Promise<{ ids: string[]; pages: number }> {
  const ids: string[] = [];
  let afterId: string | undefined = undefined;
  let pages = 0;
  for (let guard = 0; guard < 1000; guard++) {
    const page = await view.paginate("participant", undefined, {
      limit,
      sort,
      afterId,
    });
    pages++;
    ids.push(...(page.data as { _id: string }[]).map((d) => d._id));
    if (page.data.length < limit) break;
    afterId = page.data[page.data.length - 1]._id as string;
  }
  return { ids, pages };
}

/** Ground truth: a single page large enough to hold everything = Mongo's order. */
async function groundTruth(
  // deno-lint-ignore no-explicit-any
  view: any,
  sort: Record<string, 1 | -1>,
): Promise<string[]> {
  const page = await view.paginate("participant", undefined, {
    limit: 100_000,
    sort,
  });
  return (page.data as { _id: string }[]).map((d) => d._id);
}

Deno.test("paginate nested-path sort ASC: afterId walk over 3+ pages == Mongo's (meta.rank asc, _id asc)", async () => {
  await withDatabase("smc2-pag-nested-sort-asc", async (db) => {
    const catalog = await makeCatalog(db);
    const view = catalog.scope(EXPO);
    await seed(view, 31); // 31 / 4 → 8 pages, well past the "3+ pages" bar

    const { ids: walked, pages } = await walkAll(view, { "meta.rank": 1 }, 4);
    const truth = await groundTruth(view, { "meta.rank": 1 });

    assertEquals(pages >= 3, true, `expected 3+ pages, walked ${pages}`);
    assertEquals(walked.length, 31, "every doc returned exactly once");
    assertEquals(new Set(walked).size, 31, "no duplicates across page seams");
    assertEquals(
      walked,
      truth,
      "nested-path cursor walk == single-page Mongo order (incl. across ties)",
    );
  });
});

Deno.test("paginate nested-path sort DESC: afterId walk over 3+ pages == Mongo's (meta.rank desc, _id desc)", async () => {
  await withDatabase("smc2-pag-nested-sort-desc", async (db) => {
    const catalog = await makeCatalog(db);
    const view = catalog.scope(EXPO);
    await seed(view, 31);

    const { ids: walked, pages } = await walkAll(view, { "meta.rank": -1 }, 4);
    const truth = await groundTruth(view, { "meta.rank": -1 });

    assertEquals(pages >= 3, true, `expected 3+ pages, walked ${pages}`);
    assertEquals(walked.length, 31);
    assertEquals(new Set(walked).size, 31);
    assertEquals(
      walked,
      truth,
      "nested-path desc walk == single-page Mongo order",
    );
  });
});

Deno.test("paginate nested-path sort: position advances correctly across afterId pages", async () => {
  await withDatabase("smc2-pag-nested-sort-pos", async (db) => {
    const catalog = await makeCatalog(db);
    const view = catalog.scope(EXPO);
    await seed(view, 30);

    const p1 = await view.paginate("participant", undefined, {
      limit: 10,
      sort: { "meta.rank": 1 },
    });
    assertEquals(p1.total, 30);
    assertEquals(p1.position, 0);

    const p2 = await view.paginate("participant", undefined, {
      limit: 10,
      sort: { "meta.rank": 1 },
      afterId: p1.data[p1.data.length - 1]._id as string,
    });
    assertEquals(p2.position, 10, "position advances under a nested-path sort");

    const p3 = await view.paginate("participant", undefined, {
      limit: 10,
      sort: { "meta.rank": 1 },
      afterId: p2.data[p2.data.length - 1]._id as string,
    });
    assertEquals(p3.position, 20);
    assertEquals(p3.data.length, 10, "the final third of 30");
  });
});

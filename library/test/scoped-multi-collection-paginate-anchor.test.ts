// paginate() cursor-anchor edge cases:
//  - an afterId/beforeId whose anchor was DELETED must fail loud (the new
//    explicit-throw behavior — the anchor is fetched within scope+type, so a
//    now-missing doc resolves to a null cursor filter → throw).
//  - beforeId combined with peek, and beforeId combined with skipTotal.
//
// The "never existed" and "other scope" anchor cases live in
// scoped-multi-collection-paginate.test.ts (N9); here we hit the DELETED-anchor
// path specifically, plus the beforeId option combinations.

import { assert, assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO_A = "exposition:expoaaaaa01";

async function makeCatalog(
  db: Parameters<Parameters<typeof withDatabase>[1]>[0],
) {
  return await scopedMultiCollection(db, "catalog", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: {
      participant: { name: v.string(), seat: v.number() },
    },
  });
}

/** Seed participants with deterministic, lexicographically-sortable ids. */
async function seedDeterministic(
  // deno-lint-ignore no-explicit-any
  view: any,
  count: number,
): Promise<string[]> {
  const ids: string[] = [];
  for (let i = 0; i < count; i++) {
    const id = `participant:p${String(i).padStart(2, "0")}`;
    await view.insertOne("participant", { _id: id, name: `P${i}`, seat: i });
    ids.push(id);
  }
  return ids;
}

Deno.test("paginate: afterId anchoring a now-DELETED doc throws (naming id + scope)", async () => {
  await withDatabase("smc2-pag-anchor-deleted-after", async (db) => {
    const catalog = await makeCatalog(db);
    const view = catalog.scope(EXPO_A);
    const ids = await seedDeterministic(view, 10);

    // Page 1, then delete its last row — the id a naive caller would page after.
    const p1 = await view.paginate("participant", undefined, { limit: 5 });
    const anchor = p1.data[p1.data.length - 1]._id as string;
    const removed = await view.deleteId("participant", anchor);
    assertEquals(removed, 1);

    // Paging after the vanished anchor must fail loud rather than silently
    // restart at page 1 with a bogus position.
    const err = await assertRejects(
      () =>
        view.paginate("participant", undefined, { limit: 5, afterId: anchor }),
      Error,
    );
    assert(err.message.includes(anchor), "error names the missing id");
    assert(err.message.includes(EXPO_A), "error names the scope");

    // Sanity: after the delete there are 9 docs and page 1 is unaffected.
    assertEquals(ids.length, 10);
    assertEquals(
      (await view.paginate("participant", undefined, { limit: 100 })).total,
      9,
    );
  });
});

Deno.test("paginate: beforeId anchoring a now-DELETED doc throws (naming id + scope)", async () => {
  await withDatabase("smc2-pag-anchor-deleted-before", async (db) => {
    const catalog = await makeCatalog(db);
    const view = catalog.scope(EXPO_A);
    await seedDeterministic(view, 10);

    const anchor = "participant:p05";
    assertEquals(await view.deleteId("participant", anchor), 1);

    const err = await assertRejects(
      () =>
        view.paginate("participant", undefined, { limit: 5, beforeId: anchor }),
      Error,
    );
    assert(err.message.includes(anchor));
    assert(err.message.includes(EXPO_A));
  });
});

Deno.test("paginate: beforeId + peek — hasMore reflects rows still before the page, extra row dropped", async () => {
  await withDatabase("smc2-pag-before-peek", async (db) => {
    const catalog = await makeCatalog(db);
    const view = catalog.scope(EXPO_A);
    const ids = await seedDeterministic(view, 30); // p00..p29, _id-asc order

    // Anchor at p20 → the page BEFORE it (limit 10) is p10..p19, and there ARE
    // rows (p00..p09) still further back, so peek must report hasMore = true.
    const back = await view.paginate("participant", undefined, {
      limit: 10,
      beforeId: ids[20],
      peek: true,
    });
    assertEquals(back.data.length, 10, "the peeked extra row is dropped");
    assertEquals(back.data.map((d) => d._id), ids.slice(10, 20));
    assertEquals(back.hasMore, true, "more rows exist before this page");

    // Anchor near the start: the page before p08 (limit 10) is p00..p07 — only
    // 8 rows, nothing further back → hasMore = false, page not padded.
    const start = await view.paginate("participant", undefined, {
      limit: 10,
      beforeId: ids[8],
      peek: true,
    });
    assertEquals(start.data.map((d) => d._id), ids.slice(0, 8));
    assertEquals(start.hasMore, false, "no rows before the first page");
  });
});

Deno.test("paginate: beforeId + skipTotal — forward-ordered page, total/position omitted", async () => {
  await withDatabase("smc2-pag-before-skiptotal", async (db) => {
    const catalog = await makeCatalog(db);
    const view = catalog.scope(EXPO_A);
    const ids = await seedDeterministic(view, 30);

    const back = await view.paginate("participant", undefined, {
      limit: 10,
      beforeId: ids[20],
      skipTotal: true,
    });

    // Data is still the correct page in forward order …
    assertEquals(back.data.map((d) => d._id), ids.slice(10, 20));
    // … but the counts are skipped.
    assertEquals(back.total, undefined, "skipTotal omits total");
    assertEquals(back.position, undefined, "skipTotal omits position");
  });
});

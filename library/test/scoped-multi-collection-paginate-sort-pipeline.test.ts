// Coverage for scopedMultiCollection.paginate({ sortPipeline }) — sorting on
// a field COMPUTED by aggregation stages (a `$lookup`ed doc's field). The
// page-2 cases are the point: a page-1-only test passes even when the cursor
// is wrong. DESC with missing joined docs is the regression test for the
// `$expr` vs query-operator cursor semantics (query `$gt`/`$lt` never match a
// missing field, so parents with no joined doc vanish from page 2 onward).

import { assertEquals, assertExists, assertRejects } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import {
  scopedMultiCollection,
  type ScopedStageBuilder,
} from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO = "exposition:expoaaaaa01";
const OTHER = "exposition:expobbbbb02";

const types = {
  participant: { name: v.string() },
  badge: { participantId: v.string(), generatedAt: v.number() },
};

async function makeCatalog(
  db: Parameters<Parameters<typeof withDatabase>[1]>[0],
) {
  return await scopedMultiCollection(db, "catalog", {
    scope: refId("exposition"),
    types,
  });
}

const badgeSortPipeline = (s: ScopedStageBuilder<typeof types>) => [
  s.lookup("badge", "_id", "participantId", { as: "badgeDocs" }),
  s.addFields({ badgeDoc: { $first: "$badgeDocs" } }),
];

/**
 * Seed 21 participants; those with `i % 3 !== 0` get a badge (14 badged,
 * 7 badgeless). `generatedAt` repeats every 4 badges so the `_id` tie-breaker
 * is exercised on top of the joined sort key.
 */
async function seed(
  // deno-lint-ignore no-explicit-any
  catalog: any,
): Promise<{ ids: string[]; badgeless: Set<string> }> {
  const view = catalog.scope(EXPO);
  const ids: string[] = [];
  const badgeless = new Set<string>();
  for (let i = 0; i < 21; i++) {
    const id = await view.insertOne("participant", {
      name: `P${String(i).padStart(2, "0")}`,
    });
    ids.push(id);
    if (i % 3 !== 0) {
      await view.insertOne("badge", {
        participantId: id,
        generatedAt: 1000 + (i % 4) * 100,
      });
    } else {
      badgeless.add(id);
    }
  }
  return { ids, badgeless };
}

/** Walk every page via afterId; returns rows + per-page total/position. */
// deno-lint-ignore no-explicit-any
async function walkAll(view: any, sort: Record<string, 1 | -1>, limit: number) {
  // deno-lint-ignore no-explicit-any
  const all: any[] = [];
  const pages: { total?: number; position?: number; size: number }[] = [];
  let afterId: string | undefined = undefined;
  for (let guard = 0; guard < 1000; guard++) {
    const page = await view.paginate("participant", undefined, {
      limit,
      sort,
      afterId,
      sortPipeline: badgeSortPipeline,
    });
    all.push(...page.data);
    pages.push({
      total: page.total,
      position: page.position,
      size: page.data.length,
    });
    if (page.data.length < limit) break;
    afterId = page.data[page.data.length - 1]._id as string;
  }
  return { all, pages };
}

/**
 * Independent ground truth: raw aggregate with MongoDB's own `$sort` on the
 * joined field — no paginate machinery involved. A correct cursor walk must
 * reproduce this exact id sequence.
 */
// deno-lint-ignore no-explicit-any
async function groundTruth(view: any, dir: 1 | -1): Promise<string[]> {
  const rows = await view.aggregate((s: ScopedStageBuilder<typeof types>) => [
    s.match("participant", {}),
    ...badgeSortPipeline(s),
    s.sort({ "badgeDoc.generatedAt": dir, _id: 1 }),
  ]);
  return (rows as { _id: string }[]).map((r) => r._id);
}

Deno.test("sortPipeline ASC: page walk == MongoDB's joined-field order, once each", async () => {
  await withDatabase("smc-sortpipe-asc", async (db) => {
    const catalog = await makeCatalog(db);
    await seed(catalog);
    const view = catalog.scope(EXPO);

    const { all } = await walkAll(view, { "badgeDoc.generatedAt": 1 }, 4);
    const walked = all.map((d) => d._id as string);
    const truth = await groundTruth(view, 1);

    assertEquals(walked.length, 21, "every participant returned exactly once");
    assertEquals(new Set(walked).size, 21, "no duplicates across pages");
    assertEquals(walked, truth, "walk == raw $sort on the joined field");
  });
});

Deno.test("sortPipeline DESC with missing joined docs: badgeless parents survive page 2+", async () => {
  await withDatabase("smc-sortpipe-desc-missing", async (db) => {
    const catalog = await makeCatalog(db);
    const { badgeless } = await seed(catalog);
    const view = catalog.scope(EXPO);

    const { all } = await walkAll(view, { "badgeDoc.generatedAt": -1 }, 4);
    const walked = all.map((d) => d._id as string);
    const truth = await groundTruth(view, -1);

    // DESC puts the null/missing sort keys LAST — precisely where a
    // query-operator cursor would silently drop them from page 2 onward.
    assertEquals(walked.length, 21, "badgeless participants must not vanish");
    assertEquals(new Set(walked).size, 21);
    assertEquals(walked, truth);
    const returnedBadgeless = walked.filter((id) => badgeless.has(id));
    assertEquals(returnedBadgeless.length, badgeless.size);

    // The joined field survives into the returned docs.
    const withBadge = all.find((d) => !badgeless.has(d._id as string));
    assertExists(withBadge);
    assertEquals(typeof withBadge.badgeDoc?.generatedAt, "number");
  });
});

Deno.test("sortPipeline: total + position stay consistent across pages", async () => {
  await withDatabase("smc-sortpipe-position", async (db) => {
    const catalog = await makeCatalog(db);
    await seed(catalog);
    const view = catalog.scope(EXPO);

    const { pages } = await walkAll(view, { "badgeDoc.generatedAt": -1 }, 4);
    let offset = 0;
    for (const page of pages) {
      assertEquals(page.total, 21);
      assertEquals(page.position, offset, `position at offset ${offset}`);
      offset += page.size;
    }
    assertEquals(offset, 21);
  });
});

Deno.test("sortPipeline: beforeId walks backward over the same order", async () => {
  await withDatabase("smc-sortpipe-before", async (db) => {
    const catalog = await makeCatalog(db);
    await seed(catalog);
    const view = catalog.scope(EXPO);
    const sort = { "badgeDoc.generatedAt": -1 as const };
    const truth = await groundTruth(view, -1);

    // Anchor on the 9th doc: the backward page must be exactly the 4 docs
    // preceding it, in forward order, with the matching absolute position.
    const anchor = truth[8];
    const page = await view.paginate("participant", undefined, {
      limit: 4,
      sort,
      beforeId: anchor,
      sortPipeline: badgeSortPipeline,
    });
    assertEquals(
      page.data.map((d: { _id: string }) => d._id),
      truth.slice(4, 8),
    );
    assertEquals(page.total, 21);
    assertEquals(page.position, 4);
  });
});

Deno.test("sortPipeline: joined docs from another scope never feed the sort", async () => {
  await withDatabase("smc-sortpipe-scope", async (db) => {
    const catalog = await makeCatalog(db);
    const view = catalog.scope(EXPO);
    const other = catalog.scope(OTHER);

    const a = await view.insertOne("participant", { name: "A" });
    const b = await view.insertOne("participant", { name: "B" });
    await view.insertOne("badge", { participantId: a, generatedAt: 100 });
    // Cross-scope badge for B with a HUGE sort value: if scope safety leaked,
    // B would sort after A. It must behave as badgeless (null → first, ASC).
    await other.insertOne("badge", { participantId: b, generatedAt: 9999 });

    const page = await view.paginate("participant", undefined, {
      limit: 10,
      sort: { "badgeDoc.generatedAt": 1 },
      sortPipeline: badgeSortPipeline,
    });
    assertEquals(page.data.map((d: { _id: string }) => d._id), [b, a]);
  });
});

Deno.test("sortPipeline: sort key produced by `pipeline` throws, pointing at sortPipeline", async () => {
  await withDatabase("smc-sortpipe-verrou", async (db) => {
    const catalog = await makeCatalog(db);
    await seed(catalog);
    const view = catalog.scope(EXPO);

    await assertRejects(
      () =>
        view.paginate("participant", undefined, {
          limit: 4,
          sort: { "badgeDoc.generatedAt": 1 },
          // Join placed in the AFTER-sort slot — the sort cannot see it.
          pipeline: badgeSortPipeline,
        }),
      Error,
      "sortPipeline",
    );
  });
});

Deno.test("sortPipeline: anchor dropped by the sort pipeline fails loud", async () => {
  await withDatabase("smc-sortpipe-dropped-anchor", async (db) => {
    const catalog = await makeCatalog(db);
    const { ids } = await seed(catalog);
    const view = catalog.scope(EXPO);

    await assertRejects(
      () =>
        view.paginate("participant", undefined, {
          limit: 4,
          sort: { "badgeDoc.generatedAt": 1 },
          afterId: ids[0], // P00 — excluded by the $match below
          sortPipeline: (s) => [
            s.match("participant", { name: { $ne: "P00" } }),
            ...badgeSortPipeline(s),
          ],
        }),
      Error,
      "dropped by `sortPipeline`",
    );
  });
});

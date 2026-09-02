// Coverage for scopedMultiCollection.paginate() under a CUSTOM sort — the
// compound cursor ladder (`$or` over sort fields + `_id` tie-breaker, via
// getNestedValue) that the existing paginate tests never exercise (they all
// use the default `_id` sort). Duplicate sort values are seeded on purpose so
// the tie-breaker branch (sortValue == anchor AND _id > anchor) is hit.

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
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: {
      participant: { name: v.string(), seat: v.number() },
      membership: { participantId: v.string(), org: v.string() },
    },
  });
}

/**
 * Seed `count` participants whose `seat` repeats (seat = floor(i/2)), so many
 * docs share a sort value and the `_id` tie-breaker decides their order.
 */
async function seed(
  // deno-lint-ignore no-explicit-any
  catalog: any,
  count: number,
): Promise<void> {
  const view = catalog.scope(EXPO);
  for (let i = 0; i < count; i++) {
    await view.insertOne("participant", {
      name: `P${String(i).padStart(3, "0")}`,
      seat: Math.floor(i / 2), // 0,0,1,1,2,2,...
    });
  }
}

/** Walk every page via afterId and return the concatenated rows. */
// deno-lint-ignore no-explicit-any
async function walkAll(view: any, sort: Record<string, 1 | -1>, limit: number) {
  // deno-lint-ignore no-explicit-any
  const all: any[] = [];
  let afterId: string | undefined = undefined;
  // hard stop so a cursor bug can't loop forever
  for (let guard = 0; guard < 1000; guard++) {
    const page = await view.paginate("participant", undefined, {
      limit,
      sort,
      afterId,
    });
    all.push(...page.data);
    if (page.data.length < limit) break;
    afterId = page.data[page.data.length - 1]._id as string;
  }
  return all;
}

/**
 * Ground truth = a SINGLE page large enough to hold everything, i.e. MongoDB's
 * own authoritative sort with no cursor involved. A correct compound-cursor
 * walk must reproduce this exact id sequence.
 */
// deno-lint-ignore no-explicit-any
async function groundTruth(view: any, sort: Record<string, 1 | -1>) {
  const page = await view.paginate("participant", undefined, {
    limit: 100_000,
    sort,
  });
  return (page.data as { _id: string }[]).map((d) => d._id);
}

Deno.test("paginate custom sort ASC: walk reproduces MongoDB's sorted order, once each", async () => {
  await withDatabase("smc-paginate-sort-asc", async (db) => {
    const catalog = await makeCatalog(db);
    await seed(catalog, 47); // odd count → a partial last page
    const view = catalog.scope(EXPO);

    const walked = (await walkAll(view, { seat: 1 }, 10)).map((d) => d._id);
    const truth = await groundTruth(view, { seat: 1 });

    assertEquals(walked.length, 47, "every doc returned exactly once");
    assertEquals(
      new Set(walked).size,
      47,
      "no duplicates across page boundaries",
    );
    assertEquals(
      walked,
      truth,
      "paginated walk == MongoDB's (seat asc, _id asc) order, incl. across ties",
    );
  });
});

Deno.test("paginate custom sort DESC: walk reproduces MongoDB's sorted order, once each", async () => {
  await withDatabase("smc-paginate-sort-desc", async (db) => {
    const catalog = await makeCatalog(db);
    await seed(catalog, 47);
    const view = catalog.scope(EXPO);

    const walked = (await walkAll(view, { seat: -1 }, 10)).map((d) => d._id);
    const truth = await groundTruth(view, { seat: -1 });

    assertEquals(walked.length, 47);
    assertEquals(new Set(walked).size, 47);
    assertEquals(walked, truth, "paginated walk == MongoDB's seat-desc order");
  });
});

Deno.test("paginate custom sort: position + total are correct under a custom sort", async () => {
  await withDatabase("smc-paginate-sort-pos", async (db) => {
    const catalog = await makeCatalog(db);
    await seed(catalog, 30);
    const view = catalog.scope(EXPO);

    const p1 = await view.paginate("participant", undefined, {
      limit: 10,
      sort: { seat: 1 },
    });
    assertEquals(p1.total, 30);
    assertEquals(p1.position, 0);

    const p2 = await view.paginate("participant", undefined, {
      limit: 10,
      sort: { seat: 1 },
      afterId: p1.data[p1.data.length - 1]._id as string,
    });
    assertEquals(
      p2.position,
      10,
      "position advances correctly under custom sort",
    );
    assertEquals(p2.data.length, 10);
  });
});

Deno.test("paginate custom sort + pipeline: walk == Mongo order over JOIN survivors", async () => {
  await withDatabase("smc-paginate-sort-pipeline", async (db) => {
    const catalog = await makeCatalog(db);
    const view = catalog.scope(EXPO);

    // 40 participants with duplicated seats; every 3rd gets a membership.
    const ids: string[] = [];
    for (let i = 0; i < 40; i++) {
      ids.push(
        await view.insertOne("participant", {
          name: `P${String(i).padStart(3, "0")}`,
          seat: Math.floor(i / 2),
        }),
      );
    }
    for (let i = 0; i < 40; i += 3) {
      await view.insertOne("membership", {
        participantId: ids[i],
        org: "org:x",
      });
    }

    // INNER-JOIN filter: keep participants with >=1 membership, sorted by seat.
    // deno-lint-ignore no-explicit-any
    const pipeline = (stage: any) => [
      stage.lookup("membership", "_id", "participantId", "m"),
      { $match: { "m.0": { $exists: true } } },
    ];

    // Ground truth: a single big page (Mongo's own order over the survivors).
    const truthPage = await view.paginate("participant", undefined, {
      limit: 100_000,
      sort: { seat: 1 },
      pipeline,
    });
    const truth = (truthPage.data as { _id: string }[]).map((d) => d._id);

    // Walk the same query in tiny pages via afterId.
    const walked: string[] = [];
    let afterId: string | undefined = undefined;
    for (let guard = 0; guard < 1000; guard++) {
      const page = await view.paginate("participant", undefined, {
        limit: 4,
        sort: { seat: 1 },
        pipeline,
        afterId,
      });
      walked.push(...(page.data as { _id: string }[]).map((d) => d._id));
      if (page.data.length < 4) break;
      afterId = page.data[page.data.length - 1]._id as string;
    }

    assertEquals(walked.length, truth.length, "same survivor count");
    assertEquals(new Set(walked).size, walked.length, "no dup across pages");
    assertEquals(
      walked,
      truth,
      "paginated walk over the JOIN == single-page Mongo order (sort-before-pipeline holds)",
    );
  });
});

Deno.test("paginate custom sort: beforeId returns the prior page in forward order", async () => {
  await withDatabase("smc-paginate-sort-before", async (db) => {
    const catalog = await makeCatalog(db);
    await seed(catalog, 30);
    const view = catalog.scope(EXPO);

    // Forward to get page 2's anchor, then walk back from its first row.
    const p1 = await view.paginate("participant", undefined, {
      limit: 10,
      sort: { seat: 1 },
    });
    const p2 = await view.paginate("participant", undefined, {
      limit: 10,
      sort: { seat: 1 },
      afterId: p1.data[p1.data.length - 1]._id as string,
    });

    const back = await view.paginate("participant", undefined, {
      limit: 10,
      sort: { seat: 1 },
      beforeId: p2.data[0]._id as string,
    });

    assertEquals(back.data.length, 10);
    // The page before page 2 IS page 1 → same ids, same forward order.
    assertEquals(
      back.data.map((d) => d._id),
      p1.data.map((d) => d._id),
      "beforeId under a custom sort reconstructs page 1 exactly",
    );
  });
});

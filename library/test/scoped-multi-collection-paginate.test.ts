// Verrou — scopedMultiCollection.paginate() MUST expose `position` (and the
// full pagination surface), at parity with collection.paginate() and
// multiCollection.paginate().
//
// Regression it guards: a previous impl returned only `{ total, data }`. A
// downstream wrapper computing `hasMore = position + data.length < total`
// then evaluated `undefined + len < total` → `NaN < total` → always `false`,
// silently killing "next/prev/load-more" on EVERY exposition-scoped list while
// still rendering "1–25 / 58". Page 1 `position` MUST be `0`.

import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

async function makeCatalog(
  db: Parameters<Parameters<typeof withDatabase>[1]>[0],
) {
  return await scopedMultiCollection(db, "catalog", {
    scope: refId("exposition"),
    types: {
      participant: {
        name: v.string(),
        seat: v.number(),
        vip: v.boolean(),
      },
      membership: {
        participantId: v.string(),
        org: v.string(),
      },
    },
  });
}

/** Seed `count` participants in `expo`, returns their ids in insertion order. */
async function seedParticipants(
  // deno-lint-ignore no-explicit-any
  catalog: any,
  expo: string,
  count: number,
): Promise<string[]> {
  const view = catalog.scope(expo);
  const ids: string[] = [];
  for (let i = 0; i < count; i++) {
    const id = await view.insertOne("participant", {
      name: `P${String(i).padStart(3, "0")}`,
      seat: i,
      vip: i % 5 === 0,
    });
    ids.push(id);
  }
  return ids;
}

// The exact downstream computation that regressed.
function hasMore(page: { position?: number; data: unknown[]; total?: number }) {
  return typeof page.total === "number"
    ? (page.position ?? NaN) + page.data.length < page.total
    : false;
}

Deno.test("paginate: page 1 exposes position=0 + total (the verrou)", async () => {
  await withDatabase("smc-paginate-pos", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 58);

    const page = await catalog.scope(EXPO_A).paginate(
      "participant",
      undefined,
      {
        limit: 25,
      },
    );

    assertEquals(page.total, 58);
    assertEquals(page.position, 0, "page 1 position must be 0, not undefined");
    assertEquals(page.data.length, 25);
    // The regression: this MUST be true (was `NaN < 58` === false before).
    assert(hasMore(page), "hasMore must be true on page 1 of 58/25");
  });
});

Deno.test("paginate: afterId advances position + returns the next slice", async () => {
  await withDatabase("smc-paginate-after", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 58);
    const view = catalog.scope(EXPO_A);

    // Authoritative default (_id-asc) order. ULIDs are NOT insertion-monotonic
    // within the same millisecond, so the order must be read from the DB rather
    // than assumed to equal the insertion order.
    const ordered: string[] =
      (await view.paginate("participant", undefined, { limit: 1000 }))
        .data.map((d: { _id: string }) => d._id);

    const p1 = await view.paginate("participant", undefined, { limit: 25 });
    const lastOfP1 = p1.data[p1.data.length - 1]._id as string;

    const p2 = await view.paginate("participant", undefined, {
      limit: 25,
      afterId: lastOfP1,
    });

    assertEquals(p2.total, 58);
    assertEquals(p2.position, 25, "page 2 starts after the first 25");
    assertEquals(p2.data.length, 25);
    // Page 2 begins at the 26th doc in the authoritative _id-asc order.
    assertEquals(p2.data[0]._id, ordered[25]);
    assert(hasMore(p2), "still more after page 2 (50<58)");

    const p3 = await view.paginate("participant", undefined, {
      limit: 25,
      afterId: p2.data[p2.data.length - 1]._id as string,
    });
    assertEquals(p3.position, 50);
    assertEquals(p3.data.length, 8, "last page holds the remaining 8");
    assert(!hasMore(p3), "no more after the final page (58<58 is false)");
  });
});

Deno.test("paginate: beforeId returns the prior page in forward order", async () => {
  await withDatabase("smc-paginate-before", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 58);
    const view = catalog.scope(EXPO_A);

    // Authoritative _id-asc order (ULIDs aren't insertion-monotonic, see above).
    const ordered: string[] =
      (await view.paginate("participant", undefined, { limit: 1000 }))
        .data.map((d: { _id: string }) => d._id);

    // Anchor at the 26th doc in that order, walk back → must rebuild page 1.
    const back = await view.paginate("participant", undefined, {
      limit: 25,
      beforeId: ordered[25],
    });

    assertEquals(back.data.length, 25);
    assertEquals(back.position, 0, "the page before page 2 is page 1");
    // Forward order restored: first row is order[0], last is order[24].
    assertEquals(back.data[0]._id, ordered[0]);
    assertEquals(back.data[back.data.length - 1]._id, ordered[24]);
  });
});

Deno.test("paginate: peek sets hasMore without a count", async () => {
  await withDatabase("smc-paginate-peek", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 30);
    const view = catalog.scope(EXPO_A);

    const more = await view.paginate("participant", undefined, {
      limit: 25,
      peek: true,
      skipTotal: true,
    });
    assertEquals(more.data.length, 25, "the peeked extra row is dropped");
    assertEquals(more.hasMore, true);
    assertEquals(more.total, undefined);
    assertEquals(more.position, undefined);

    const last = await view.paginate("participant", undefined, {
      limit: 25,
      afterId: more.data[more.data.length - 1]._id as string,
      peek: true,
      skipTotal: true,
    });
    assertEquals(last.data.length, 5);
    assertEquals(last.hasMore, false);
  });
});

Deno.test("paginate: stays scoped — never bleeds across expositions", async () => {
  await withDatabase("smc-paginate-scope", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 30);
    await seedParticipants(catalog, EXPO_B, 7);

    const a = await catalog.scope(EXPO_A).paginate("participant", undefined, {
      limit: 100,
    });
    const b = await catalog.scope(EXPO_B).paginate("participant", undefined, {
      limit: 100,
    });

    assertEquals(a.total, 30);
    assertEquals(b.total, 7);
    for (const row of b.data) assertEquals(row._scope, EXPO_B);
  });
});

Deno.test("paginate: filter(doc) shrinks the page, position/total untouched", async () => {
  await withDatabase("smc-paginate-filter", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 30);

    const page = await catalog.scope(EXPO_A).paginate(
      "participant",
      undefined,
      {
        limit: 25,
        // deno-lint-ignore no-explicit-any
        filter: (doc: any) => doc.vip === true,
      },
    );

    assertEquals(page.total, 30, "total reflects the DB query, not the filter");
    assertEquals(page.position, 0);
    // 1 in 5 is vip among the first scanned window.
    assert(page.data.length > 0 && page.data.length < 25);
    // deno-lint-ignore no-explicit-any
    for (const row of page.data) assertEquals((row as any).vip, true);
  });
});

Deno.test("paginate: pipeline count reflects docs surviving the JOIN", async () => {
  await withDatabase("smc-paginate-pipeline", async (db) => {
    const catalog = await makeCatalog(db);
    const ids = await seedParticipants(catalog, EXPO_A, 30);
    const view = catalog.scope(EXPO_A);

    // Give only the first 12 participants a membership.
    for (let i = 0; i < 12; i++) {
      await view.insertOne("membership", {
        participantId: ids[i],
        org: "org:acme",
      });
    }

    // INNER-JOIN-style filter: keep participants that have ≥1 membership.
    const page = await view.paginate("participant", undefined, {
      limit: 25,
      // deno-lint-ignore no-explicit-any
      pipeline: (stage: any) => [
        stage.lookup("membership", "_id", "participantId", "memberships"),
        { $match: { "memberships.0": { $exists: true } } },
      ],
    });

    assertEquals(
      page.total,
      12,
      "total must count only joined docs, not all 30",
    );
    assertEquals(page.position, 0);
    assertEquals(page.data.length, 12);
  });
});

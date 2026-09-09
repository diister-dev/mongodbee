// Verrou — scopedMultiCollection.paginate() MUST expose `position` (and the
// full pagination surface), at parity with collection.paginate() and
// multiCollection.paginate().
//
// Regression it guards: a previous impl returned only `{ total, data }`. A
// downstream wrapper computing `hasMore = position + data.length < total`
// then evaluated `undefined + len < total` → `NaN < total` → always `false`,
// silently killing "next/prev/load-more" on EVERY exposition-scoped list while
// still rendering "1–25 / 58". Page 1 `position` MUST be `0`.

import { test } from "./+harness.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
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
    schemaManagement: "auto",
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

test("paginate: page 1 exposes position=0 + total (the verrou)", async () => {
  await withDatabase("smc-paginate-pos", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 58);

    const page = await catalog
      .scope(EXPO_A)
      .paginate("participant", undefined, {
        limit: 25,
      });

    assertEquals(page.total, 58);
    assertEquals(page.position, 0, "page 1 position must be 0, not undefined");
    assertEquals(page.data.length, 25);
    // The regression: this MUST be true (was `NaN < 58` === false before).
    assert(hasMore(page), "hasMore must be true on page 1 of 58/25");
  });
});

test("paginate: afterId advances position + returns the next slice", async () => {
  await withDatabase("smc-paginate-after", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 58);
    const view = catalog.scope(EXPO_A);

    // Authoritative default (_id-asc) order. ULIDs are NOT insertion-monotonic
    // within the same millisecond, so the order must be read from the DB rather
    // than assumed to equal the insertion order.
    const ordered: string[] = (
      await view.paginate("participant", undefined, { limit: 1000 })
    ).data.map((d: { _id: string }) => d._id);

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

test("paginate: beforeId returns the prior page in forward order", async () => {
  await withDatabase("smc-paginate-before", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 58);
    const view = catalog.scope(EXPO_A);

    // Authoritative _id-asc order (ULIDs aren't insertion-monotonic, see above).
    const ordered: string[] = (
      await view.paginate("participant", undefined, { limit: 1000 })
    ).data.map((d: { _id: string }) => d._id);

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

test("paginate: peek sets hasMore without a count", async () => {
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

test("paginate: stays scoped — never bleeds across expositions", async () => {
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

test("paginate: filter(doc) shrinks the page, position/total untouched", async () => {
  await withDatabase("smc-paginate-filter", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 30);

    const page = await catalog
      .scope(EXPO_A)
      .paginate("participant", undefined, {
        limit: 25,
        filter: (doc: any) => doc.vip === true,
      });

    assertEquals(page.total, 30, "total reflects the DB query, not the filter");
    assertEquals(page.position, 0);
    // 1 in 5 is vip among the first scanned window.
    assert(page.data.length > 0 && page.data.length < 25);
    for (const row of page.data) assertEquals((row as any).vip, true);
  });
});

test("paginate: pipeline count reflects docs surviving the JOIN", async () => {
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

// [N9] An afterId/beforeId that does not resolve to an anchor within the bound
// scope+type must FAIL LOUD — naming the id, type and scope — instead of
// silently restarting at page 1 with a bogus position.
test("paginate: afterId with no anchor throws, naming id + scope (N9)", async () => {
  await withDatabase("smc-paginate-afterid-missing", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 10);
    await seedParticipants(catalog, EXPO_B, 5);
    const view = catalog.scope(EXPO_A);

    // Well-formed id (right prefix) but not present in this scope+type.
    const err = await assertRejects(
      () =>
        view.paginate("participant", undefined, {
          limit: 5,
          afterId: "participant:doesnotexist",
        }),
      Error,
    );
    assert(err.message.includes("participant:doesnotexist"));
    assert(err.message.includes(EXPO_A));

    // An id that exists only in ANOTHER scope must not silently reset to page 1.
    const bPage = await catalog
      .scope(EXPO_B)
      .paginate("participant", undefined, { limit: 5 });
    const bId = bPage.data[0]._id as string;
    await assertRejects(
      () => view.paginate("participant", undefined, { limit: 5, afterId: bId }),
      Error,
    );
  });
});

test("paginate: beforeId with no anchor throws, naming id + scope (N9)", async () => {
  await withDatabase("smc-paginate-beforeid-missing", async (db) => {
    const catalog = await makeCatalog(db);
    await seedParticipants(catalog, EXPO_A, 10);
    const view = catalog.scope(EXPO_A);

    const err = await assertRejects(
      () =>
        view.paginate("participant", undefined, {
          limit: 5,
          beforeId: "participant:nopenope",
        }),
      Error,
    );
    assert(err.message.includes("participant:nopenope"));
    assert(err.message.includes(EXPO_A));
  });
});

// [N6] With a filtering pipeline present, `beforeId`'s position must be
// computed through the SAME aggregate($count) shape as `total` — otherwise a
// plain countDocuments counts docs that the pipeline drops, and `position`
// disagrees with the pipeline-aware `total`.
test("paginate: beforeId position is pipeline-aware (N6)", async () => {
  await withDatabase("smc-paginate-before-pipeline", async (db) => {
    const catalog = await makeCatalog(db);
    const view = catalog.scope(EXPO_A);

    // Deterministic, lexicographically sortable ids → a fixed _id-asc order.
    const pid = (i: number) => `participant:p${String(i).padStart(2, "0")}`;
    for (let i = 0; i < 20; i++) {
      await view.insertOne("participant", {
        _id: pid(i),
        name: `P${i}`,
        seat: i,
        vip: false,
      });
    }
    // Only ODD-indexed participants get a membership → 10 JOIN survivors,
    // interleaved with non-survivors in _id order.
    for (let i = 1; i < 20; i += 2) {
      await view.insertOne("membership", {
        _id: `membership:m${String(i).padStart(2, "0")}`,
        participantId: pid(i),
        org: "org:x",
      });
    }

    const pipeline = (stage: any) => [
      stage.lookup("membership", "_id", "participantId", "m"),
      { $match: { "m.0": { $exists: true } } },
    ];

    const p1 = await view.paginate("participant", undefined, {
      limit: 5,
      pipeline,
    });
    assertEquals(p1.total, 10, "total counts JOIN survivors only");
    assertEquals(p1.position, 0);
    const p1ids = p1.data.map((d: { _id: string }) => d._id);
    assertEquals(p1ids, [pid(1), pid(3), pid(5), pid(7), pid(9)]);

    const p2 = await view.paginate("participant", undefined, {
      limit: 5,
      pipeline,
      afterId: p1ids[p1ids.length - 1],
    });
    assertEquals(p2.position, 5);
    const anchor = p2.data[0]._id as string; // pid(11)

    // Walk BACK from page 2's first row → must rebuild page 1 at position 0.
    // Before the fix the before-count ignored the pipeline and counted ALL 11
    // participants with _id < anchor → position = 6 (wrong).
    const back = await view.paginate("participant", undefined, {
      limit: 5,
      pipeline,
      beforeId: anchor,
    });
    assertEquals(back.position, 0, "beforeId position must be pipeline-aware");
    assertEquals(
      back.data.map((d: { _id: string }) => d._id),
      p1ids,
    );
  });
});

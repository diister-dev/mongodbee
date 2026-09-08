// Verrou — paging by an OPTIONAL sort field must walk the WHOLE set, on all
// three surfaces.
//
// Regression it guards: the index-strategy cursor is a query-operator ladder
// (`{ field: { $gt: anchorValue } }`). `$sort` ranks a MISSING field equal to
// null and both below every real value; query operators disagree —
// `{f: {$gt: null}}` matches nothing and `{f: {$lt: v}}` skips null. So an
// anchor sitting in the null block emitted a rung that matched nothing, and
// the walk dead-ended there: measured 6 of 12 documents reachable, with
// `position` jumping to 11 so the truncation read as the end of the list.
//
// This is the shape of a badge list sorted by generation date — the
// participants with no badge yet carry no value, and they are the majority
// early in an event.

import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { dbId, refId } from "../src/ids.ts";

const EXPO = "exposition:expoaaaaa01";

/** 12 documents with no value, then 12 with one — the boundary sits mid-set. */
const WITHOUT = 12;
const WITH = 12;

function valueAt(i: number) {
  return new Date(Date.UTC(2026, 0, i + 1));
}

/**
 * Walk every page forward with `afterId`, returning the ids seen in order.
 * Bounded so a cursor that fails to advance cannot loop forever.
 */
async function walkAll(
  page: (
    afterId: string | undefined,
  ) => Promise<{ data: { _id: string }[]; total?: number; position?: number }>,
): Promise<string[]> {
  const seen: string[] = [];
  let afterId: string | undefined = undefined;
  for (let guard = 0; guard < 50; guard++) {
    const p = await page(afterId);
    for (const doc of p.data) seen.push(doc._id);
    const done =
      p.data.length === 0 ||
      (p.position ?? 0) + p.data.length >= (p.total ?? 0);
    if (done) break;
    afterId = p.data[p.data.length - 1]._id;
  }
  return seen;
}

function assertWalkedAll(seen: string[], expected: string[], label: string) {
  assertEquals(
    seen.length,
    expected.length,
    `${label}: visited ${seen.length} of ${expected.length} documents — the ` +
      `cursor dead-ended at the null boundary`,
  );
  assertEquals(
    new Set(seen).size,
    seen.length,
    `${label}: a document repeated`,
  );
  for (const id of expected) {
    assert(seen.includes(id), `${label}: never reached ${id}`);
  }
}

test("paginate (scoped): an optional sort field walks the whole set", async () => {
  await withDatabase("paginate-optsort-scoped", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        participant: { name: v.string(), generatedAt: v.optional(v.date()) },
      },
    });
    const view = catalog.scope(EXPO);

    const ids: string[] = [];
    for (let i = 0; i < WITHOUT; i++) {
      ids.push(await view.insertOne("participant", { name: `none-${i}` }));
    }
    for (let i = 0; i < WITH; i++) {
      ids.push(
        await view.insertOne("participant", {
          name: `set-${i}`,
          generatedAt: valueAt(i),
        }),
      );
    }

    for (const dir of [1, -1] as const) {
      const seen = await walkAll(
        (afterId) =>
          view.paginate("participant", undefined, {
            sort: { generatedAt: dir, _id: 1 },
            limit: 5,
            ...(afterId ? { afterId } : {}),
          }) as any,
      );
      assertWalkedAll(seen, ids, `scoped sort ${dir}`);
    }
  });
});

test("paginate (multiCollection): an optional sort field walks the whole set", async () => {
  await withDatabase("paginate-optsort-multi", async (db) => {
    const catalog = await multiCollection(db, "catalog", {
      participant: { name: v.string(), generatedAt: v.optional(v.date()) },
    });

    const ids: string[] = [];
    for (let i = 0; i < WITHOUT; i++) {
      ids.push(await catalog.insertOne("participant", { name: `none-${i}` }));
    }
    for (let i = 0; i < WITH; i++) {
      ids.push(
        await catalog.insertOne("participant", {
          name: `set-${i}`,
          generatedAt: valueAt(i),
        }),
      );
    }

    for (const dir of [1, -1] as const) {
      const seen = await walkAll(
        (afterId) =>
          catalog.paginate("participant", undefined, {
            sort: { generatedAt: dir, _id: 1 },
            limit: 5,
            ...(afterId ? { afterId } : {}),
          }) as any,
      );
      assertWalkedAll(seen, ids, `multi sort ${dir}`);
    }
  });
});

test("paginate (collection): an optional sort field walks the whole set", async () => {
  await withDatabase("paginate-optsort-simple", async (db) => {
    const people = await collection(db, "people", {
      _id: dbId("person"),
      name: v.string(),
      generatedAt: v.optional(v.date()),
    });

    const ids: string[] = [];
    for (let i = 0; i < WITHOUT; i++) {
      ids.push(
        (await people.insertOne({ name: `none-${i}` } as never)) as string,
      );
    }
    for (let i = 0; i < WITH; i++) {
      ids.push(
        (await people.insertOne({
          name: `set-${i}`,
          generatedAt: valueAt(i),
        } as never)) as string,
      );
    }

    for (const dir of [1, -1] as const) {
      const seen = await walkAll(
        (afterId) =>
          people.paginate(
            {},
            {
              sort: { generatedAt: dir, _id: 1 },
              limit: 5,
              ...(afterId ? { afterId } : {}),
            },
          ) as any,
      );
      assertWalkedAll(seen, ids, `collection sort ${dir}`);
    }
  });
});

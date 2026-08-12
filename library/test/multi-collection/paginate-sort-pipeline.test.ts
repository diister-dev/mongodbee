// Coverage for multiCollection.paginate({ sortPipeline }) — sorting on a
// field COMPUTED by aggregation stages (a `$lookup`ed doc's field) — plus the
// `position` regression for naturalIdSort cursors (counts must run through
// the same assembly as the data pipeline). Page-2 cases are the point: a
// page-1-only test passes even when the cursor is wrong.

import { assertEquals, assertRejects } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import * as v from "../../src/schema.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

const eventModel = defineModel("event", {
  schema: {
    participant: { name: v.string() },
    badge: { participantId: v.string(), generatedAt: v.number() },
  },
});

// deno-lint-ignore no-explicit-any
const badgeSortPipeline = (s: any) => [
  s.lookup("badge", "_id", "participantId", { as: "badgeDocs" }),
  s.addFields({ badgeDoc: { $first: "$badgeDocs" } }),
];

/** 21 participants; `i % 3 !== 0` get a badge (7 badgeless), values repeat. */
// deno-lint-ignore no-explicit-any
async function seed(mc: any): Promise<{ ids: string[] }> {
  const ids: string[] = [];
  for (let i = 0; i < 21; i++) {
    const id = await mc.insertOne("participant", {
      name: `P${String(i).padStart(2, "0")}`,
    });
    ids.push(id);
    if (i % 3 !== 0) {
      await mc.insertOne("badge", {
        participantId: id,
        generatedAt: 1000 + (i % 4) * 100,
      });
    }
  }
  return { ids };
}

/**
 * Independent ground truth: raw $sort on the joined field, no paginate. The
 * `_id` tie-break follows the field's direction (normalizePaginateSort).
 */
// deno-lint-ignore no-explicit-any
async function groundTruth(mc: any, dir: 1 | -1): Promise<string[]> {
  // deno-lint-ignore no-explicit-any
  const rows = await mc.aggregate((s: any) => [
    s.match("participant", {}),
    ...badgeSortPipeline(s),
    s.sort({ "badgeDoc.generatedAt": dir, _id: dir }),
  ]);
  return (rows as { _id: string }[]).map((r) => r._id);
}

// deno-lint-ignore no-explicit-any
async function walkAll(mc: any, sort: Record<string, 1 | -1>, limit: number) {
  // deno-lint-ignore no-explicit-any
  const all: any[] = [];
  const pages: { total?: number; position?: number; size: number }[] = [];
  let afterId: string | undefined = undefined;
  for (let guard = 0; guard < 1000; guard++) {
    const page = await mc.paginate("participant", {}, {
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

Deno.test("mc sortPipeline DESC with missing joined docs: full coverage, exact order", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "event", eventModel);
    await seed(mc);

    const { all, pages } = await walkAll(
      mc,
      { "badgeDoc.generatedAt": -1 },
      4,
    );
    const walked = all.map((d) => d._id as string);
    const truth = await groundTruth(mc, -1);

    // DESC puts missing sort keys last — where a query-operator cursor would
    // silently drop the badgeless participants from page 2 onward.
    assertEquals(walked.length, 21);
    assertEquals(new Set(walked).size, 21);
    assertEquals(walked, truth);

    let offset = 0;
    for (const page of pages) {
      assertEquals(page.total, 21);
      assertEquals(page.position, offset);
      offset += page.size;
    }
  });
});

Deno.test("mc sortPipeline ASC: page walk == raw $sort order", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "event", eventModel);
    await seed(mc);

    const { all } = await walkAll(mc, { "badgeDoc.generatedAt": 1 }, 5);
    assertEquals(all.map((d) => d._id), await groundTruth(mc, 1));
  });
});

Deno.test("mc sortPipeline: sort key produced by `pipeline` throws, pointing at sortPipeline", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "event", eventModel);
    await seed(mc);

    await assertRejects(
      () =>
        mc.paginate("participant", {}, {
          limit: 4,
          sort: { "badgeDoc.generatedAt": 1 },
          pipeline: badgeSortPipeline,
        }),
      Error,
      "sortPipeline",
    );
  });
});

Deno.test("mc sortPipeline: naturalIdSort combination is refused", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "event", eventModel);

    await assertRejects(
      () =>
        mc.paginate(["participant", "badge"], {}, {
          limit: 4,
          naturalIdSort: true,
          sortPipeline: badgeSortPipeline,
        }),
      Error,
      "cannot be combined",
    );
  });
});

Deno.test("mc naturalIdSort: position is the page offset on page 2 (count mirrors _ulid extraction)", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "event", eventModel);
    // Alternate types so the cross-type ULID order differs from a per-type walk.
    for (let i = 0; i < 10; i++) {
      if (i % 2 === 0) {
        await mc.insertOne("participant", { name: `P${i}` });
      } else {
        await mc.insertOne("badge", { participantId: `x${i}`, generatedAt: i });
      }
    }

    const page1 = await mc.paginate(["participant", "badge"], {}, {
      limit: 3,
      naturalIdSort: true,
    });
    assertEquals(page1.total, 10);
    assertEquals(page1.position, 0);

    const page2 = await mc.paginate(["participant", "badge"], {}, {
      limit: 3,
      naturalIdSort: true,
      afterId: page1.data[page1.data.length - 1]._id as string,
    });
    assertEquals(page2.total, 10);
    // Before the fix, the `_ulid` cursor was counted against documents that
    // never ran ulidExtractStage → afterCount 0 → position == total (10).
    assertEquals(page2.position, 3);
    // Continuity: no overlap with page 1.
    const ids1 = new Set(page1.data.map((d: { _id: string }) => d._id));
    for (const d of page2.data as { _id: string }[]) {
      assertEquals(ids1.has(d._id), false);
    }
  });
});

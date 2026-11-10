// `paginate({ hint })` reaches the server on the page read AND on every count
// (total and position), on the three collection kinds and on both the find
// and the aggregate paths — read back from the database profiler, so what is
// asserted is what the server received. `countHint` replaces `hint` on the
// counts only; omitted, the counts keep `hint`. The anchor lookup by `_id`
// keeps no hint: forcing a sort index on it would scan that index instead of
// `_id_`.

import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { ObjectId } from "mongodb";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { refId } from "../src/ids.ts";

type Read = "data" | "count" | "anchor";
type ProfiledRead = { read: Read; hint: unknown };

const ROWS = 12;
const LIMIT = 5;
const EXPO = "exposition:expoaaaaa01";

function stageMatchesAnchor(match: unknown): boolean {
  if (typeof match !== "object" || match === null) return false;
  const id = (match as { _id?: unknown })._id;
  return typeof id === "string" || id instanceof ObjectId;
}

function classify(command: Record<string, unknown>): Read | null {
  if ("find" in command) {
    return stageMatchesAnchor(command.filter) ? "anchor" : "data";
  }
  if ("aggregate" in command) {
    const pipeline = command.pipeline as Record<string, unknown>[];
    if (stageMatchesAnchor(pipeline[0]?.$match)) return "anchor";
    const last = pipeline[pipeline.length - 1] ?? {};
    return "$count" in last || "$group" in last ? "count" : "data";
  }
  return null;
}

async function profiledReads(
  db: Db,
  collectionName: string,
  work: () => Promise<unknown>,
): Promise<ProfiledRead[]> {
  await db
    .collection("system.profile")
    .drop()
    .catch(() => {});
  await db.command({ profile: 2 });
  try {
    await work();
  } finally {
    await db.command({ profile: 0 });
  }
  const entries = await db
    .collection("system.profile")
    .find({
      ns: `${db.databaseName}.${collectionName}`,
      op: { $in: ["query", "command"] },
    })
    .toArray();
  const reads: ProfiledRead[] = [];
  for (const entry of entries) {
    const command = entry.command as Record<string, unknown>;
    const read = classify(command);
    if (read) reads.push({ read, hint: command.hint });
  }
  return reads;
}

function assertHinted(
  reads: ProfiledRead[],
  hint: unknown,
  expectedCounts: number,
  countHint: unknown = hint,
): void {
  const data = reads.filter((r) => r.read === "data");
  const counts = reads.filter((r) => r.read === "count");
  const anchors = reads.filter((r) => r.read === "anchor");
  assertEquals(data.length, 1, "one page read");
  assertEquals(counts.length, expectedCounts, "total + position counts");
  assert(anchors.length >= 1, "the afterId anchor was looked up");
  for (const r of data) {
    assertEquals(r.hint, hint, "page read carries the hint");
  }
  for (const r of counts) {
    assertEquals(r.hint, countHint, "count carries the count hint");
  }
  for (const r of anchors) {
    assertEquals(r.hint, undefined, "anchor lookup carries no hint");
  }
}

function assertUnhinted(reads: ProfiledRead[]): void {
  assert(reads.length > 0, "reads were profiled");
  for (const r of reads) {
    assertEquals(r.hint, undefined, `${r.read} read carries no hint`);
  }
}

test("paginate (collection): hint reaches the find page and its countDocuments", async () => {
  await withDatabase("paginate-hint-collection", async (db) => {
    const items = await collection(db, "items", { rank: v.number() });
    for (let i = 0; i < ROWS; i++) await items.insertOne({ rank: i });
    const first = await items.paginate({}, { limit: LIMIT });
    const afterId = first.data[LIMIT - 1]._id;
    const hint = { _id: 1 };

    for (const pipeline of [undefined, () => [{ $addFields: { x: 1 } }]]) {
      let hinted: { total?: number; position?: number; data: unknown[] } = {
        data: [],
      };
      const reads = await profiledReads(db, "items", async () => {
        hinted = await items.paginate(
          {},
          { limit: LIMIT, afterId, hint, ...(pipeline ? { pipeline } : {}) },
        );
      });
      assertHinted(reads, hint, 2);

      let plain: typeof hinted = { data: [] };
      const plainReads = await profiledReads(db, "items", async () => {
        plain = await items.paginate(
          {},
          { limit: LIMIT, afterId, ...(pipeline ? { pipeline } : {}) },
        );
      });
      assertUnhinted(plainReads);
      assertEquals(hinted, plain, "the hint does not change the page");
    }
  });
});

test("paginate (multi): hint reaches the page read and its counts", async () => {
  await withDatabase("paginate-hint-multi", async (db) => {
    const model = defineModel("catalog", {
      schema: { product: { rank: v.number() } },
    });
    const catalog = await multiCollection(db, "catalog", model);
    for (let i = 0; i < ROWS; i++) {
      await catalog.insertOne("product", { rank: i });
    }
    const first = await catalog.paginate("product", {}, { limit: LIMIT });
    const afterId = String(first.data[LIMIT - 1]._id);
    const hint = "_id_";

    for (const pipeline of [undefined, () => [{ $addFields: { x: 1 } }]]) {
      let hinted: unknown;
      const reads = await profiledReads(db, "catalog", async () => {
        hinted = await catalog.paginate(
          "product",
          {},
          { limit: LIMIT, afterId, hint, ...(pipeline ? { pipeline } : {}) },
        );
      });
      assertHinted(reads, hint, 2);

      let plain: unknown;
      const plainReads = await profiledReads(db, "catalog", async () => {
        plain = await catalog.paginate(
          "product",
          {},
          { limit: LIMIT, afterId, ...(pipeline ? { pipeline } : {}) },
        );
      });
      assertUnhinted(plainReads);
      assertEquals(hinted, plain, "the hint does not change the page");
    }
  });
});

test("paginate (scoped): hint reaches the page read and its counts", async () => {
  await withDatabase("paginate-hint-scoped", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { rank: v.number() } },
    });
    const view = catalog.scope(EXPO);
    for (let i = 0; i < ROWS; i++) {
      await view.insertOne("artwork", { rank: i });
    }
    const first = await view.paginate("artwork", undefined, { limit: LIMIT });
    const afterId = String(first.data[LIMIT - 1]._id);
    const hint = { _id: 1 };

    for (const pipeline of [undefined, () => [{ $addFields: { x: 1 } }]]) {
      let hinted: unknown;
      const reads = await profiledReads(db, "catalog", async () => {
        hinted = await view.paginate("artwork", undefined, {
          limit: LIMIT,
          afterId,
          hint,
          ...(pipeline ? { pipeline } : {}),
        });
      });
      assertHinted(reads, hint, 2);

      let plain: unknown;
      const plainReads = await profiledReads(db, "catalog", async () => {
        plain = await view.paginate("artwork", undefined, {
          limit: LIMIT,
          afterId,
          ...(pipeline ? { pipeline } : {}),
        });
      });
      assertUnhinted(plainReads);
      assertEquals(hinted, plain, "the hint does not change the page");
    }
  });
});

test("paginate (scoped): hint reaches the sortPipeline page and its counts", async () => {
  await withDatabase("paginate-hint-scoped-sort", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { rank: v.number() } },
    });
    const view = catalog.scope(EXPO);
    for (let i = 0; i < ROWS; i++) {
      await view.insertOne("artwork", { rank: i });
    }
    const sortPipeline = () => [
      { $addFields: { twice: { $multiply: ["$rank", 2] } } },
    ];
    const first = await view.paginate("artwork", undefined, {
      limit: LIMIT,
      sort: { twice: 1 },
      sortPipeline,
    });
    const afterId = String(first.data[LIMIT - 1]._id);
    const hint = { _id: 1 };

    const reads = await profiledReads(db, "catalog", () =>
      view.paginate("artwork", undefined, {
        limit: LIMIT,
        afterId,
        sort: { twice: 1 },
        sortPipeline,
        hint,
      }),
    );
    assertHinted(reads, hint, 2);
  });
});

const PAGE_HINT = "_id_";
const COUNT_HINT = { rank: 1 };
const PIPELINES = [undefined, () => [{ $addFields: { x: 1 } }]];

test("paginate (collection): countHint goes to the counts, hint to the page", async () => {
  await withDatabase("paginate-count-hint-collection", async (db) => {
    const items = await collection(db, "items", { rank: v.number() });
    for (let i = 0; i < ROWS; i++) await items.insertOne({ rank: i });
    await db.collection("items").createIndex(COUNT_HINT);
    const first = await items.paginate({}, { limit: LIMIT });
    const afterId = first.data[LIMIT - 1]._id;

    for (const pipeline of PIPELINES) {
      const extra = pipeline ? { pipeline } : {};
      let split: unknown;
      const reads = await profiledReads(db, "items", async () => {
        split = await items.paginate(
          {},
          {
            limit: LIMIT,
            afterId,
            hint: PAGE_HINT,
            countHint: COUNT_HINT,
            ...extra,
          },
        );
      });
      assertHinted(reads, PAGE_HINT, 2, COUNT_HINT);

      const sameReads = await profiledReads(db, "items", () =>
        items.paginate(
          {},
          { limit: LIMIT, afterId, hint: PAGE_HINT, ...extra },
        ),
      );
      assertHinted(sameReads, PAGE_HINT, 2);

      const plain = await items.paginate(
        {},
        { limit: LIMIT, afterId, ...extra },
      );
      assertEquals(split, plain, "the hints do not change the page");
    }
  });
});

test("paginate (multi): countHint goes to the counts, hint to the page", async () => {
  await withDatabase("paginate-count-hint-multi", async (db) => {
    const model = defineModel("catalog", {
      schema: { product: { rank: v.number() } },
    });
    const catalog = await multiCollection(db, "catalog", model);
    for (let i = 0; i < ROWS; i++) {
      await catalog.insertOne("product", { rank: i });
    }
    await db.collection("catalog").createIndex(COUNT_HINT);
    const first = await catalog.paginate("product", {}, { limit: LIMIT });
    const afterId = String(first.data[LIMIT - 1]._id);

    for (const pipeline of PIPELINES) {
      const extra = pipeline ? { pipeline } : {};
      let split: unknown;
      const reads = await profiledReads(db, "catalog", async () => {
        split = await catalog.paginate(
          "product",
          {},
          {
            limit: LIMIT,
            afterId,
            hint: PAGE_HINT,
            countHint: COUNT_HINT,
            ...extra,
          },
        );
      });
      assertHinted(reads, PAGE_HINT, 2, COUNT_HINT);

      const sameReads = await profiledReads(db, "catalog", () =>
        catalog.paginate(
          "product",
          {},
          { limit: LIMIT, afterId, hint: PAGE_HINT, ...extra },
        ),
      );
      assertHinted(sameReads, PAGE_HINT, 2);

      const plain = await catalog.paginate(
        "product",
        {},
        { limit: LIMIT, afterId, ...extra },
      );
      assertEquals(split, plain, "the hints do not change the page");
    }
  });
});

test("paginate (scoped): countHint goes to the counts, hint to the page", async () => {
  await withDatabase("paginate-count-hint-scoped", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { rank: v.number() } },
    });
    const view = catalog.scope(EXPO);
    for (let i = 0; i < ROWS; i++) {
      await view.insertOne("artwork", { rank: i });
    }
    await db.collection("catalog").createIndex(COUNT_HINT);
    const first = await view.paginate("artwork", undefined, { limit: LIMIT });
    const afterId = String(first.data[LIMIT - 1]._id);

    for (const pipeline of PIPELINES) {
      const extra = pipeline ? { pipeline } : {};
      let split: unknown;
      const reads = await profiledReads(db, "catalog", async () => {
        split = await view.paginate("artwork", undefined, {
          limit: LIMIT,
          afterId,
          hint: PAGE_HINT,
          countHint: COUNT_HINT,
          ...extra,
        });
      });
      assertHinted(reads, PAGE_HINT, 2, COUNT_HINT);

      const sameReads = await profiledReads(db, "catalog", () =>
        view.paginate("artwork", undefined, {
          limit: LIMIT,
          afterId,
          hint: PAGE_HINT,
          ...extra,
        }),
      );
      assertHinted(sameReads, PAGE_HINT, 2);

      const plain = await view.paginate("artwork", undefined, {
        limit: LIMIT,
        afterId,
        ...extra,
      });
      assertEquals(split, plain, "the hints do not change the page");
    }
  });
});

test("paginate (scoped): countHint reaches the sortPipeline counts", async () => {
  await withDatabase("paginate-count-hint-scoped-sort", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { rank: v.number() } },
    });
    const view = catalog.scope(EXPO);
    for (let i = 0; i < ROWS; i++) {
      await view.insertOne("artwork", { rank: i });
    }
    await db.collection("catalog").createIndex(COUNT_HINT);
    const sortPipeline = () => [
      { $addFields: { twice: { $multiply: ["$rank", 2] } } },
    ];
    const first = await view.paginate("artwork", undefined, {
      limit: LIMIT,
      sort: { twice: 1 },
      sortPipeline,
    });
    const afterId = String(first.data[LIMIT - 1]._id);

    const reads = await profiledReads(db, "catalog", () =>
      view.paginate("artwork", undefined, {
        limit: LIMIT,
        afterId,
        sort: { twice: 1 },
        sortPipeline,
        hint: PAGE_HINT,
        countHint: COUNT_HINT,
      }),
    );
    assertHinted(reads, PAGE_HINT, 2, COUNT_HINT);
  });
});

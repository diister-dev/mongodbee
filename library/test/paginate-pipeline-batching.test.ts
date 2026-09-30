import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assertEquals } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { refId } from "../src/ids.ts";
import { type Db, MongoClient } from "../src/mongodb.ts";
import type { Page } from "../src/page.ts";
import type { StoredDocument } from "../src/stored-document.ts";
import { closeAllWatchers } from "../src/change-stream.ts";
import { setLogSink } from "../src/utils/logger.ts";
import { TEST_URI } from "./+shared.ts";

const SCOPE = "exposition:aaaaaaaaaaaaaaaaaaaaaaaaaa";
const ROWS = 60;
const INVALID_EVERY = 7;
const DROPPED_EVERY = 5;

type Row = { _id: string; name: string; org?: unknown[]; badge?: unknown[] };
type PageOptions = {
  limit: number;
  afterId?: string;
  beforeId?: string;
  peek?: boolean;
  skipTotal?: boolean;
  filtering: boolean;
  jsFilter?: boolean;
};

type Surface = {
  kind: string;
  seed: (db: Db) => Promise<(options: PageOptions) => Promise<Page<Row>>>;
};

export type Commands = { name: string; command: Record<string, unknown> }[];

async function withMonitoredDatabase(
  work: (db: Db, commands: Commands) => Promise<void>,
) {
  const client = new MongoClient(TEST_URI, { monitorCommands: true });
  const commands: Commands = [];
  client.on("commandStarted", (event) => {
    commands.push({
      name: event.commandName,
      command: event.command as Record<string, unknown>,
    });
  });
  const db = client.db(
    `@TEST_pgbatch@${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`,
  );
  setLogSink(() => {});
  try {
    await work(db, commands);
  } finally {
    setLogSink(undefined);
    await closeAllWatchers(db);
    await db.dropDatabase();
    await client.close();
  }
}

function rowName(index: number) {
  return `N${String(index).padStart(3, "0")}`;
}

function isInvalid(index: number) {
  return index % INVALID_EVERY === 3;
}

function isDropped(index: number) {
  return index % DROPPED_EVERY === 1;
}

function stages(filtering: boolean, from: string) {
  return [
    {
      $lookup: {
        from: "orgs",
        localField: "orgId",
        foreignField: "_id",
        as: "org",
      },
    },
    ...(filtering
      ? [{ $match: { dropped: { $ne: true } } }, { $unset: "dropped" }]
      : []),
    {
      $lookup: {
        from,
        localField: "_id",
        foreignField: "ownerId",
        as: "badge",
      },
    },
  ];
}

function names(page: Page<Row>) {
  return page.data.map((row) => row.name);
}

function countedIds(filtering: boolean) {
  const invalid: number[] = [];
  const valid: number[] = [];
  for (let index = 0; index < ROWS; index++) {
    if (filtering && isDropped(index)) continue;
    (isInvalid(index) ? invalid : valid).push(index);
  }
  return [...invalid, ...valid].map(
    (index) => `person:${String(index).padStart(4, "0")}`,
  );
}

function expectedNames(filtering: boolean) {
  const out: string[] = [];
  for (let index = 0; index < ROWS; index++) {
    if (isInvalid(index)) continue;
    if (filtering && isDropped(index)) continue;
    out.push(rowName(index));
  }
  return out;
}

async function seedOrgs(db: Db) {
  await db.collection<{ _id: string; label: string }>("orgs").insertMany(
    Array.from({ length: 5 }, (_, i) => ({
      _id: `org:${i}`,
      label: `O${i}`,
    })),
  );
}

function rawRow(index: number) {
  return {
    name: isInvalid(index) ? index : rowName(index),
    orgId: `org:${index % 5}`,
    ...(isDropped(index) ? { dropped: true } : {}),
  };
}

const surfaces: Surface[] = [
  {
    kind: "collection",
    seed: async (db) => {
      const people = await collection(db, "people", {
        name: v.string(),
        orgId: v.string(),
        dropped: v.optional(v.boolean()),
      });
      await seedOrgs(db);
      for (let index = 0; index < ROWS; index++) {
        await people.collection.insertOne(
          {
            _id: `person:${String(index).padStart(4, "0")}`,
            ...rawRow(index),
          } as never,
          { bypassDocumentValidation: true },
        );
      }
      return (options) =>
        people.paginate(
          {},
          {
            limit: options.limit,
            afterId: options.afterId,
            beforeId: options.beforeId,
            peek: options.peek,
            skipTotal: options.skipTotal,
            sort: { name: 1 },
            pipeline: () => stages(options.filtering, "people"),
            ...(options.jsFilter
              ? { filter: (doc: { name: string }) => !doc.name.endsWith("2") }
              : {}),
          },
        ) as Promise<Page<Row>>;
    },
  },
  {
    kind: "multiCollection",
    seed: async (db) => {
      const catalog = await multiCollection(
        db,
        "catalog",
        {
          person: {
            name: v.string(),
            orgId: v.string(),
            dropped: v.optional(v.boolean()),
          },
        },
        { schemaManagement: "auto" },
      );
      await seedOrgs(db);
      for (let index = 0; index < ROWS; index++) {
        await db.collection<StoredDocument>("catalog").insertOne(
          {
            _id: `person:${String(index).padStart(4, "0")}`,
            _type: "person",
            ...rawRow(index),
          },
          { bypassDocumentValidation: true },
        );
      }
      return (options) =>
        catalog.paginate(
          "person",
          {},
          {
            limit: options.limit,
            afterId: options.afterId,
            beforeId: options.beforeId,
            peek: options.peek,
            skipTotal: options.skipTotal,
            sort: { name: 1 },
            pipeline: () => stages(options.filtering, "catalog") as never,
            ...(options.jsFilter
              ? { filter: (doc: { name: string }) => !doc.name.endsWith("2") }
              : {}),
          },
        ) as unknown as Promise<Page<Row>>;
    },
  },
  {
    kind: "scopedMultiCollection",
    seed: async (db) => {
      const scoped = await scopedMultiCollection(db, "+expositions", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          person: {
            name: v.string(),
            orgId: v.string(),
            dropped: v.optional(v.boolean()),
          },
        },
      });
      await seedOrgs(db);
      for (let index = 0; index < ROWS; index++) {
        await db.collection<StoredDocument>("+expositions").insertOne(
          {
            _id: `person:${String(index).padStart(4, "0")}`,
            _type: "person",
            _scope: SCOPE,
            ...rawRow(index),
          },
          { bypassDocumentValidation: true },
        );
      }
      const view = scoped.scope(SCOPE);
      return (options) =>
        view.paginate(
          "person",
          {},
          {
            limit: options.limit,
            afterId: options.afterId,
            beforeId: options.beforeId,
            peek: options.peek,
            skipTotal: options.skipTotal,
            sort: { name: 1 },
            pipeline: () => stages(options.filtering, "+expositions") as never,
            ...(options.jsFilter
              ? { filter: (doc: { name: string }) => !doc.name.endsWith("2") }
              : {}),
          },
        ) as unknown as Promise<Page<Row>>;
    },
  },
];

for (const surface of surfaces) {
  for (const filtering of [false, true]) {
    test(`paginate pipeline (${surface.kind}, ${filtering ? "filtering" : "display"}): forward and backward walks see every valid row once`, async () => {
      await withMonitoredDatabase(async (db) => {
        const page = await surface.seed(db);
        const expected = expectedNames(filtering);
        const counted = countedIds(filtering);
        const limit = 8;

        const forward: string[] = [];
        let afterId: string | undefined;
        let seen = 0;
        for (;;) {
          const current = await page({ limit, afterId, peek: true, filtering });
          assertEquals(current.total, counted.length);
          assertEquals(
            current.position,
            afterId === undefined ? 0 : counted.indexOf(afterId) + 1,
          );
          assertEquals(current.data.length <= limit, true);
          forward.push(...names(current));
          seen += current.data.length;
          assertEquals(current.hasMore, seen < expected.length);
          if (!current.hasMore) break;
          afterId = current.data[current.data.length - 1]._id;
        }
        assertEquals(forward, expected);

        const lastPage = await page({ limit, afterId, filtering });
        const backward: string[] = names(lastPage);
        let beforeId = lastPage.data[0]._id;
        for (;;) {
          const current = await page({
            limit,
            beforeId,
            peek: true,
            filtering,
          });
          backward.unshift(...names(current));
          assertEquals(current.total, counted.length);
          assertEquals(
            current.position,
            Math.max(0, counted.indexOf(beforeId) - current.data.length),
          );
          if (!current.hasMore) break;
          beforeId = current.data[0]._id;
        }
        assertEquals(backward, expected);
      });
    });
  }

  test(`paginate pipeline (${surface.kind}): lookups ride on the returned rows and invalid rows are counted as skipped`, async () => {
    await withMonitoredDatabase(async (db) => {
      const page = await surface.seed(db);
      const first = await page({ limit: 10, filtering: false });
      assertEquals(first.data.length, 10);
      assertEquals(
        first.skippedInvalid,
        Array.from({ length: ROWS }, (_, i) => i).filter(isInvalid).length,
      );
      for (const row of first.data) {
        assertEquals(Array.isArray(row.org), true);
        assertEquals((row.org as unknown[]).length, 1);
        assertEquals(Array.isArray(row.badge), true);
      }
    });
  });

  test(`paginate pipeline (${surface.kind}): a JS filter still fills the page past rejected rows`, async () => {
    await withMonitoredDatabase(async (db) => {
      const page = await surface.seed(db);
      const expected = expectedNames(false).filter(
        (name) => !name.endsWith("2"),
      );
      const current = await page({
        limit: 12,
        filtering: false,
        jsFilter: true,
        skipTotal: true,
      });
      assertEquals(names(current), expected.slice(0, 12));
    });
  });
}

function aggregates(commands: Commands) {
  return commands
    .filter((entry) => entry.name === "aggregate")
    .map((entry) => ({
      stages: (entry.command.pipeline as Record<string, unknown>[]).map(
        (stage) => Object.keys(stage)[0],
      ),
      batchSize: (entry.command.cursor as { batchSize?: number } | undefined)
        ?.batchSize,
    }));
}

for (const surface of surfaces) {
  test(`paginate pipeline (${surface.kind}): the server builds one page, and counts skip display joins`, async () => {
    await withMonitoredDatabase(async (db, commands) => {
      const page = await surface.seed(db);

      commands.length = 0;
      await page({ limit: 10, peek: true, filtering: false });
      const display = aggregates(commands);
      const data = display.find((entry) => entry.stages.includes("$sort"));
      const count = display.find((entry) => entry.stages.includes("$count"));
      assertEquals(data?.batchSize, 11);
      assertEquals(count?.stages.includes("$lookup"), false);

      commands.length = 0;
      await page({ limit: 10, filtering: true });
      const filtered = aggregates(commands).find((entry) =>
        entry.stages.includes("$count"),
      );
      assertEquals(filtered?.stages.slice(-3), ["$lookup", "$match", "$count"]);

      commands.length = 0;
      await page({ limit: 10, filtering: false, jsFilter: true });
      const unbatched = aggregates(commands).find((entry) =>
        entry.stages.includes("$sort"),
      );
      assertEquals(unbatched?.batchSize, undefined);
    });
  });
}

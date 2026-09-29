import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import {
  assert,
  assertEquals,
  assertRejects,
  assertThrows,
} from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { refId } from "../src/ids.ts";
import { type Db, MongoClient } from "../src/mongodb.ts";
import { closeAllWatchers } from "../src/change-stream.ts";
import { getSessionContext } from "../src/session.ts";
import {
  invalidateReads,
  invalidateReadsOnDriverWrites,
  requestReadStats,
  withRequestContext,
} from "../src/request-context.ts";
import { contextVariable } from "../src/context-variable.ts";
import { TEST_URI } from "./+shared.ts";

const READS = new Set(["find", "aggregate", "count"]);

async function withCountedDatabase(
  work: (db: Db, reads: () => number, client: MongoClient) => Promise<void>,
) {
  const client = new MongoClient(TEST_URI, { monitorCommands: true });
  let count = 0;
  client.on("commandStarted", (event) => {
    if (READS.has(event.commandName)) count++;
  });
  const db = client.db(
    `@TEST_reqctx@${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`,
  );
  try {
    await work(db, () => count, client);
  } finally {
    await closeAllWatchers(db);
    await db.dropDatabase();
    await client.close();
  }
}

const personSchema = {
  _id: refId("person"),
  name: v.string(),
  tags: v.array(v.string()),
};

function rawPeople(db: Db) {
  return db.collection<{ _id: string; name: string }>("+people");
}

async function scopedPeople(db: Db) {
  return await scopedMultiCollection(db, "+people", {
    scope: refId("expo"),
    types: { person: personSchema },
  });
}

test("request context: identical reads of one request reach MongoDB once", async () => {
  await withCountedDatabase(async (db, reads) => {
    const people = await scopedPeople(db);
    const expo = people.scope("expo:a");
    const id = await expo.insertOne("person", { name: "Ada", tags: ["x"] });

    const before = reads();
    await withRequestContext(
      async () => {
        await expo.find("person", { name: "Ada" });
        await expo.find("person", { name: "Ada" });
        await Promise.all([
          expo.findOne("person", { name: "Ada" }),
          expo.findOne("person", { name: "Ada" }),
          expo.getById("person", id),
          expo.getById("person", id),
        ]);
        assertEquals(requestReadStats(), {
          loaded: 3,
          reused: 3,
          invalidations: 0,
        });
      },
      { memoizeReads: true },
    );
    assertEquals(reads() - before, 3);
  });
});

test("request context: without memoizeReads every read reaches MongoDB", async () => {
  await withCountedDatabase(async (db, reads) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    await expo.insertOne("person", { name: "Ada", tags: [] });

    const before = reads();
    await expo.find("person", { name: "Ada" });
    await expo.find("person", { name: "Ada" });
    await withRequestContext(async () => {
      await expo.find("person", { name: "Ada" });
      await expo.find("person", { name: "Ada" });
    });
    assertEquals(reads() - before, 4);
  });
});

test("request context: each caller gets its own copy of a shared read", async () => {
  await withCountedDatabase(async (db) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    const id = await expo.insertOne("person", { name: "Ada", tags: ["x"] });

    await withRequestContext(
      async () => {
        const first = await expo.getById("person", id);
        first.tags.push("mutated");
        first.name = "Mutated";
        const second = await expo.getById("person", id);
        assert(first !== second);
        assertEquals(second.name, "Ada");
        assertEquals(second.tags, ["x"]);
      },
      { memoizeReads: true },
    );
  });
});

test("request context: a write through mongodbee makes the next read reload", async () => {
  await withCountedDatabase(async (db, reads) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    const id = await expo.insertOne("person", { name: "Ada", tags: [] });

    const before = reads();
    await withRequestContext(
      async () => {
        assertEquals((await expo.getById("person", id)).name, "Ada");
        await expo.updateOne("person", id, { name: "Grace" });
        assertEquals((await expo.getById("person", id)).name, "Grace");
        assertEquals((await expo.getById("person", id)).name, "Grace");
      },
      { memoizeReads: true },
    );
    assertEquals(reads() - before, 2);
  });
});

test("request context: a write elsewhere in the database also invalidates", async () => {
  await withCountedDatabase(async (db) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    const log = await collection(db, "log", { line: v.string() });
    await expo.insertOne("person", { name: "Ada", tags: [] });

    await withRequestContext(
      async () => {
        await expo.find("person", {});
        await log.insertOne({ line: "hello" });
        await expo.find("person", {});
        assertEquals(requestReadStats(), {
          loaded: 2,
          reused: 0,
          invalidations: 1,
        });
      },
      { memoizeReads: true },
    );
  });
});

test("request context: invalidateReads covers writes made with the raw driver", async () => {
  await withCountedDatabase(async (db) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    const id = await expo.insertOne("person", { name: "Ada", tags: [] });

    await withRequestContext(
      async () => {
        await expo.getById("person", id);
        await rawPeople(db).updateOne({ _id: id }, { $set: { name: "Raw" } });
        assertEquals((await expo.getById("person", id)).name, "Ada");
        invalidateReads();
        assertEquals((await expo.getById("person", id)).name, "Raw");
      },
      { memoizeReads: true },
    );
  });
});

test("request context: a watched client invalidates on raw driver writes", async () => {
  await withCountedDatabase(async (db, _reads, client) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    const id = await expo.insertOne("person", { name: "Ada", tags: [] });
    const unwatch = invalidateReadsOnDriverWrites(client);
    try {
      await withRequestContext(
        async () => {
          await expo.getById("person", id);
          await rawPeople(db).updateOne({ _id: id }, { $set: { name: "Raw" } });
          assertEquals((await expo.getById("person", id)).name, "Raw");
          await Promise.all([
            expo.getById("person", id),
            rawPeople(db).updateOne({ _id: id }, { $set: { name: "Racing" } }),
          ]);
          assertEquals((await expo.getById("person", id)).name, "Racing");
        },
        { memoizeReads: true },
      );
    } finally {
      unwatch();
    }
  });
});

test("request context: watching raw writes needs command monitoring", async () => {
  const client = new MongoClient(TEST_URI);
  assertThrows(() => invalidateReadsOnDriverWrites(client));
  await client.close();
});

test("request context: reads inside a transaction are never memoized", async () => {
  await withCountedDatabase(async (db, reads, client) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    const id = await expo.insertOne("person", { name: "Ada", tags: [] });
    const { withSession } = getSessionContext(client);

    const before = reads();
    await withRequestContext(
      async () => {
        await withSession(async () => {
          await expo.getById("person", id);
          await expo.getById("person", id);
        });
      },
      { memoizeReads: true },
    );
    assertEquals(reads() - before, 2);
  });
});

test("request context: a committed transaction invalidates what was read before it", async () => {
  await withCountedDatabase(async (db, _reads, client) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    const id = await expo.insertOne("person", { name: "Ada", tags: [] });
    const { withSession } = getSessionContext(client);

    await withRequestContext(
      async () => {
        await expo.getById("person", id);
        await withSession(async (session) => {
          await rawPeople(db).updateOne(
            { _id: id },
            { $set: { name: "Committed" } },
            { session },
          );
        });
        assertEquals((await expo.getById("person", id)).name, "Committed");
      },
      { memoizeReads: true },
    );
  });
});

test("request context: two requests never share their reads", async () => {
  await withCountedDatabase(async (db, reads) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    await expo.insertOne("person", { name: "Ada", tags: [] });

    const before = reads();
    const request = () =>
      withRequestContext(
        async () => {
          await expo.find("person", {});
          await expo.find("person", {});
        },
        { memoizeReads: true },
      );
    await Promise.all([request(), request()]);
    assertEquals(reads() - before, 2);
  });
});

test("request context: a failed read is not remembered", async () => {
  await withCountedDatabase(async (db) => {
    const expo = (await scopedPeople(db)).scope("expo:a");

    await withRequestContext(
      async () => {
        await assertRejects(() => expo.getById("person", "person:missing"));
        await expo.insertOne("person", {
          _id: "person:missing",
          name: "Late",
          tags: [],
        });
        assertEquals(
          (await expo.getById("person", "person:missing")).name,
          "Late",
        );
      },
      { memoizeReads: true },
    );
  });
});

test("request context: a read of more than 100 rows is not memoized", async () => {
  await withCountedDatabase(async (db, reads) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    await expo.insertMany(
      "person",
      Array.from({ length: 101 }, (_, i) => ({ name: `P${i}`, tags: [] })),
    );

    const before = reads();
    await withRequestContext(
      async () => {
        assertEquals((await expo.find("person", {})).length, 101);
        assertEquals((await expo.find("person", {})).length, 101);
        assertEquals(
          (await expo.find("person", {}, { limit: 100 })).length,
          100,
        );
        assertEquals(
          (await expo.find("person", {}, { limit: 100 })).length,
          100,
        );
      },
      { memoizeReads: true },
    );
    assertEquals(reads() - before, 3);
  });
});

test("request context: different filters, types and options stay apart", async () => {
  await withCountedDatabase(async (db, reads) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    await expo.insertOne("person", { name: "Ada", tags: [] });
    await expo.insertOne("person", { name: "Grace", tags: [] });

    const before = reads();
    await withRequestContext(
      async () => {
        assertEquals((await expo.find("person", { name: "Ada" })).length, 1);
        assertEquals((await expo.find("person", { name: "Grace" })).length, 1);
        assertEquals((await expo.find("person", {}, { limit: 1 })).length, 1);
        assertEquals((await expo.find("person", {})).length, 2);
        assertEquals(
          (await expo.findProject("person", ["name"], {})).length,
          2,
        );
      },
      { memoizeReads: true },
    );
    assertEquals(reads() - before, 5);
  });
});

test("request context: identical aggregations reach MongoDB once", async () => {
  await withCountedDatabase(async (db, reads) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    await expo.insertOne("person", { name: "Ada", tags: ["x"] });
    await expo.insertOne("person", { name: "Grace", tags: ["x"] });

    const before = reads();
    await withRequestContext(
      async () => {
        const names = () =>
          expo.aggregate<{ _id: string }>(() => [
            { $match: { _type: "person" } },
            { $group: { _id: "$name" } },
            { $sort: { _id: 1 } },
          ]);
        assertEquals(await names(), [{ _id: "Ada" }, { _id: "Grace" }]);
        assertEquals(await names(), [{ _id: "Ada" }, { _id: "Grace" }]);
      },
      { memoizeReads: true },
    );
    assertEquals(reads() - before, 1);
  });
});

test("request context: an aggregation that writes clears the memo", async () => {
  await withCountedDatabase(async (db) => {
    const expo = (await scopedPeople(db)).scope("expo:a");
    const id = await expo.insertOne("person", { name: "Ada", tags: [] });

    await withRequestContext(
      async () => {
        await expo.getById("person", id);
        await expo.aggregate(() => [
          { $match: { _id: id } },
          { $set: { name: "Merged" } },
          { $merge: { into: "+people", whenMatched: "merge" } },
        ]);
        assertEquals((await expo.getById("person", id)).name, "Merged");
      },
      { memoizeReads: true },
    );
  });
});

test("request context: multi-collections and collections are memoized too", async () => {
  await withCountedDatabase(async (db, reads) => {
    const catalog = await multiCollection(db, "catalog", {
      person: personSchema,
    });
    const people = await collection(db, "people", { name: v.string() });
    const catalogId = await catalog.insertOne("person", {
      name: "Ada",
      tags: [],
    });
    const peopleId = await people.insertOne({ name: "Ada" });

    const before = reads();
    await withRequestContext(
      async () => {
        await catalog.getById("person", catalogId);
        await catalog.getById("person", catalogId);
        await catalog.find("person", {});
        await catalog.find("person", {});
        await people.getById(peopleId);
        await people.getById(peopleId);
        await people.findOne({ name: "Ada" });
        await people.findOne({ name: "Ada" });
      },
      { memoizeReads: true },
    );
    assertEquals(reads() - before, 4);
  });
});

test("request context: stored values keep their BSON types through the copy", async () => {
  await withCountedDatabase(async (db) => {
    const events = await collection(db, "events", {
      at: v.date(),
      count: v.number(),
    });
    const id = await events.insertOne({
      at: new Date("2026-09-29T00:00:00Z"),
      count: 3,
    });

    await withRequestContext(
      async () => {
        await events.getById(id);
        const again = await events.getById(id);
        assert(again.at instanceof Date);
        assertEquals(again.at.toISOString(), "2026-09-29T00:00:00.000Z");
        assertEquals(again.count, 3);
      },
      { memoizeReads: true },
    );
  });
});

test("context variable: uses AsyncContext.Variable when the runtime has one", () => {
  const created: string[] = [];
  class FakeVariable<T> {
    #value: T | undefined;
    constructor(options?: { name?: string }) {
      created.push(options?.name ?? "");
    }
    get() {
      return this.#value;
    }
    run<R>(value: T, fn: () => R): R {
      const previous = this.#value;
      this.#value = value;
      try {
        return fn();
      } finally {
        this.#value = previous;
      }
    }
  }
  Reflect.set(globalThis, "AsyncContext", { Variable: FakeVariable });
  try {
    const variable = contextVariable<number>("probe");
    assertEquals(created, ["probe"]);
    assertEquals(
      variable.run(7, () => variable.get()),
      7,
    );
    assertEquals(variable.get(), undefined);
  } finally {
    Reflect.deleteProperty(globalThis, "AsyncContext");
  }
});

test("context variable: falls back to AsyncLocalStorage across awaits", async () => {
  const variable = contextVariable<string>("fallback");
  const seen = await variable.run("outer", async () => {
    await new Promise((resolve) => setTimeout(resolve, 1));
    return variable.get();
  });
  assertEquals(seen, "outer");
  assertEquals(variable.get(), undefined);
});

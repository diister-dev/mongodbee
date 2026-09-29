import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assertEquals } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { refId } from "../src/ids.ts";
import { type Db, MongoClient } from "../src/mongodb.ts";
import { closeAllWatchers } from "../src/change-stream.ts";
import { getSessionContext } from "../src/session.ts";
import {
  currentReadPreference,
  primaryCollection,
  readingCollection,
  withReadPreference,
} from "../src/read-preference.ts";
import { withRequestContext } from "../src/request-context.ts";
import { TEST_URI } from "./+shared.ts";

const READS = new Set(["find", "aggregate", "count", "distinct"]);

type Seen = { command: string; mode: string };

async function withObservedDatabase(
  work: (db: Db, seen: () => Seen[], client: MongoClient) => Promise<void>,
) {
  const client = new MongoClient(TEST_URI, { monitorCommands: true });
  const commands: Seen[] = [];
  client.on("commandStarted", (event) => {
    if (!READS.has(event.commandName)) return;
    const preference = event.command.$readPreference as
      | { mode?: string }
      | undefined;
    commands.push({
      command: event.commandName,
      mode: preference?.mode ?? "primary",
    });
  });
  const db = client.db(
    `@TEST_rpctx@${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`,
  );
  try {
    await work(
      db,
      () => {
        const snapshot = [...commands];
        commands.length = 0;
        return snapshot;
      },
      client,
    );
  } finally {
    await closeAllWatchers(db);
    await db.dropDatabase();
    await client.close();
  }
}

const person = { _id: refId("person"), name: v.string() };

async function people(db: Db) {
  const scoped = await scopedMultiCollection(db, "+people", {
    scope: refId("expo"),
    types: { person },
  });
  return scoped.scope("expo:a");
}

function modes(seen: readonly Seen[]): string[] {
  return [...new Set(seen.map((entry) => entry.mode))];
}

test("read preference context: every read of a scoped collection inside it carries it", async () => {
  await withObservedDatabase(async (db, seen) => {
    const expo = await people(db);
    const id = await expo.insertOne("person", { name: "Ada" });
    seen();

    await withReadPreference("secondaryPreferred", async () => {
      await expo.getById("person", id);
      await expo.findOne("person", { name: "Ada" });
      await expo.find("person", {});
      await expo.findProject("person", ["name"], {});
      await expo.aggregate(() => [{ $match: { _type: "person" } }]);
      await expo.countDocuments("person", {});
    });
    const inside = seen();
    assertEquals(inside.length >= 6, true);
    assertEquals(modes(inside), ["secondaryPreferred"]);

    await expo.find("person", {});
    assertEquals(modes(seen()), ["primary"]);
  });
});

test("read preference context: multi-collections and collections follow it too", async () => {
  await withObservedDatabase(async (db, seen) => {
    const catalog = await multiCollection(db, "catalog", { person });
    const plain = await collection(db, "plain", { name: v.string() });
    const catalogId = await catalog.insertOne("person", { name: "Ada" });
    const plainId = await plain.insertOne({ name: "Ada" });
    seen();

    await withReadPreference("secondaryPreferred", async () => {
      await catalog.getById("person", catalogId);
      await catalog.find("person", {});
      await plain.getById(plainId);
      await plain.findOne({ name: "Ada" });
      await plain.find({}).toArray();
    });
    const inside = seen();
    assertEquals(inside.length, 5);
    assertEquals(modes(inside), ["secondaryPreferred"]);
  });
});

test("read preference context: an explicit per-call preference wins over it", async () => {
  await withObservedDatabase(async (db, seen) => {
    const catalog = await multiCollection(db, "catalog", { person });
    const id = await catalog.insertOne("person", { name: "Ada" });
    seen();

    await withReadPreference("secondaryPreferred", async () => {
      await catalog.getById("person", id, { readPreference: "primary" });
    });
    assertEquals(modes(seen()), ["primary"]);
  });
});

test("read preference context: it overrides the collection's own preference", async () => {
  await withObservedDatabase(async (db, seen) => {
    const catalog = await multiCollection(
      db,
      "catalog",
      { person },
      { readPreference: "secondaryPreferred" },
    );
    await catalog.insertOne("person", { name: "Ada" });
    seen();

    await catalog.find("person", {});
    assertEquals(modes(seen()), ["secondaryPreferred"]);

    await withReadPreference("primaryPreferred", async () => {
      await catalog.find("person", {});
    });
    assertEquals(modes(seen()), ["primaryPreferred"]);
  });
});

test("read preference context: reads inside a transaction stay on the primary", async () => {
  await withObservedDatabase(async (db, seen, client) => {
    const expo = await people(db);
    const id = await expo.insertOne("person", { name: "Ada" });
    const { withSession } = getSessionContext(client);
    seen();

    await withReadPreference("secondaryPreferred", async () => {
      await withSession(async () => {
        await expo.getById("person", id);
        await expo.find("person", {});
        await expo.updateOne("person", id, { name: "Grace" });
      });
    });
    assertEquals(modes(seen()), ["primary"]);
  });
});

test("read preference context: internal primary reads ignore it", async () => {
  await withObservedDatabase(async (db, seen) => {
    await db.collection("raw").insertOne({ seen: true });
    seen();

    await withReadPreference("secondaryPreferred", async () => {
      await primaryCollection(db, "raw").findOne({});
    });
    assertEquals(modes(seen()), ["primary"]);
  });
});

test("read preference context: a raw handle from readingCollection follows it", async () => {
  await withObservedDatabase(async (db, seen) => {
    await db.collection("raw").insertOne({ seen: true });
    const raw = readingCollection(db, "raw");
    seen();

    await withReadPreference("secondaryPreferred", async () => {
      await raw.findOne({});
      await raw.aggregate([{ $match: {} }]).toArray();
      await raw.countDocuments({});
    });
    await raw.findOne({});
    assertEquals(
      seen().map((entry) => entry.mode),
      [
        "secondaryPreferred",
        "secondaryPreferred",
        "secondaryPreferred",
        "primary",
      ],
    );
  });
});

test("read preference context: nested contexts restore the outer one", async () => {
  const outer = await withReadPreference("secondaryPreferred", async () => {
    const inner = await withReadPreference("nearest", async () => {
      await new Promise((resolve) => setTimeout(resolve, 1));
      return currentReadPreference()?.mode;
    });
    return [inner, currentReadPreference()?.mode];
  });
  assertEquals(outer, ["nearest", "secondaryPreferred"]);
  assertEquals(currentReadPreference(), undefined);
});

test("read preference context: the request memo keeps primary and secondary reads apart", async () => {
  await withObservedDatabase(async (db, seen) => {
    const expo = await people(db);
    await expo.insertOne("person", { name: "Ada" });
    seen();

    await withRequestContext(
      async () => {
        await expo.find("person", {});
        await withReadPreference("secondaryPreferred", async () => {
          await expo.find("person", {});
          await expo.find("person", {});
        });
        await expo.find("person", {});
      },
      { memoizeReads: true },
    );
    assertEquals(
      seen().map((entry) => entry.mode),
      ["primary", "secondaryPreferred"],
    );
  });
});

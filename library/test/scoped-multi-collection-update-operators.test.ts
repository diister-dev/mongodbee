import { test } from "./+harness.ts";
import { assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import {
  addToSet,
  increment,
  max,
  pull,
  push,
} from "../src/update-operators.ts";

const EXPO = "exposition:expoaaaaa01";
const OTHER = "exposition:expobbbbb01";

const open = (db: Db) =>
  scopedMultiCollection(db, "catalog", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    allowUnscoped: true,
    types: {
      artwork: {
        title: v.string(),
        views: v.number(),
        tags: v.array(v.string()),
        usage: v.object({ count: v.number() }),
        lastSeen: v.optional(v.string()),
      },
      artist: { name: v.string() },
    },
  });

const artwork = (title: string) => ({
  title,
  views: 0,
  tags: [] as string[],
  usage: { count: 0 },
});

test("scoped updateOne / updateMany: operators land, dot paths included", async (t) => {
  await withDatabase(t.name, async (db) => {
    const expo = (await open(db)).scope(EXPO);
    const a = await expo.insertOne("artwork", artwork("a"));
    const b = await expo.insertOne("artwork", artwork("b"));

    await expo.updateOne("artwork", a, {
      views: increment(2),
      "usage.count": increment(1),
      tags: push("oil", "portrait"),
      lastSeen: max("m"),
    });
    await expo.updateMany({
      artwork: {
        [a]: { tags: pull("oil"), lastSeen: max("c") },
        [b]: { tags: addToSet("ink", "ink") },
      },
    });

    const first = await expo.getById("artwork", a);
    assertEquals(first.views, 2);
    assertEquals(first.usage.count, 1);
    assertEquals(first.tags, ["portrait"]);
    assertEquals(first.lastSeen, "m");
    assertEquals((await expo.getById("artwork", b)).tags, ["ink"]);
  });
});

test("scoped updateWhere / findOneAndUpdate: operators with the scope guard", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await open(db);
    const expo = catalog.scope(EXPO);
    const other = catalog.scope(OTHER);
    const id = await expo.insertOne("artwork", artwork("a"));

    assertEquals(
      await other.updateWhere("artwork", { _id: id }, { views: increment(1) }),
      { matched: 0, modified: 0, upsertedId: null },
    );
    const after = await expo.findOneAndUpdate(
      "artwork",
      { _id: id },
      { views: increment(5), "usage.count": increment(2) },
    );
    assertEquals(after?.views, 5);
    assertEquals(after?.usage.count, 2);

    const upsert = await expo.updateWhere(
      "artwork",
      { title: "new" },
      { views: increment(1), tags: push("fresh") },
      { upsert: true, setOnInsert: { usage: { count: 0 } } },
    );
    const created = await expo.getById("artwork", upsert.upsertedId!);
    assertEquals(created._scope, EXPO);
    assertEquals(created.views, 1);
    assertEquals(created.tags, ["fresh"]);
  });
});

test("scoped updates: operands are validated and reserved paths stay guarded", async (t) => {
  await withDatabase(t.name, async (db) => {
    const expo = (await open(db)).scope(EXPO);
    const id = await expo.insertOne("artwork", artwork("a"));

    await assertRejects(
      () => expo.updateOne("artwork", id, { title: increment(1) as never }),
      Error,
      'increment() needs a numeric field, "title" is not one',
    );
    await assertRejects(
      () => expo.updateOne("artwork", id, { tags: push(1 as never) }),
      v.ValiError,
    );
    await assertRejects(
      () => expo.updateOne("artwork", id, { "usage.count": push(1) } as never),
      Error,
      'push() needs an array field, "usage.count" is not one',
    );
    await assertRejects(
      () =>
        expo.updateOne("artwork", id, { "_scope.x": increment(1) } as never),
      Error,
      '"_scope" is owned by the scoped view',
    );
    await assertRejects(
      () => expo.updateOne("artwork", id, { _scope: OTHER } as never),
      Error,
      "_scope",
    );
    assertEquals((await expo.getById("artwork", id)).views, 0);
  });
});

test("scoped: an operator write inside withSession rolls back with it", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await open(db);
    const expo = catalog.scope(EXPO);
    const id = await expo.insertOne("artwork", artwork("a"));
    await assertRejects(
      () =>
        catalog.withSession(async () => {
          await expo.updateOne("artwork", id, {
            views: increment(3),
            tags: push("tx"),
          });
          throw new Error("abort");
        }),
      Error,
      "abort",
    );
    const stored = await expo.getById("artwork", id);
    assertEquals(stored.views, 0);
    assertEquals(stored.tags, []);
  });
});

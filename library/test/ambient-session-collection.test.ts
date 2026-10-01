import { test } from "./+harness.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import type * as m from "mongodb";
import * as v from "../src/schema.ts";
import { dbId, refId } from "../src/ids.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { ambientSessionCollection, getSessionContext } from "../src/session.ts";

const EXPO = "exposition:expoaaaaa01";

const abort = new Error("abort");

test("raw collection: writes without a session join the transaction and roll back", async (t) => {
  await withDatabase(t.name, async (db) => {
    const jobs = await collection(db, "jobs", {
      _id: dbId("job"),
      attempts: v.number(),
    });
    const id = await jobs.insertOne({ attempts: 0 });

    await assertRejects(
      () =>
        jobs.withSession(async () => {
          await jobs.collection.updateOne(
            { _id: id },
            { $inc: { attempts: 1 } },
          );
          await jobs.collection.insertOne({ _id: "job:ghost", attempts: 9 });
          assertEquals(
            (await jobs.collection.findOne({ _id: id }))?.attempts,
            1,
            "the raw read sees the transaction's own write",
          );
          throw abort;
        }),
      Error,
      "abort",
    );

    assertEquals((await jobs.getById(id)).attempts, 0);
    assertEquals(await jobs.countDocuments({}), 1);
  });
});

test("raw collection: an explicit session option, even undefined, is kept", async (t) => {
  await withDatabase(t.name, async (db) => {
    const jobs = await collection(db, "jobs", {
      _id: dbId("job"),
      attempts: v.number(),
    });
    const id = await jobs.insertOne({ attempts: 0 });

    await assertRejects(
      () =>
        jobs.withSession(async () => {
          await jobs.collection.updateOne(
            { _id: id },
            { $inc: { attempts: 1 } },
            { session: undefined },
          );
          throw abort;
        }),
      Error,
      "abort",
    );

    assertEquals((await jobs.getById(id)).attempts, 1);
  });
});

test("raw collection of a multi-collection and of a scoped one roll back too", async (t) => {
  await withDatabase(t.name, async (db) => {
    const shop = await multiCollection(db, "shop", {
      product: { stock: v.number() },
    });
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { views: v.number() } },
    });
    const product = await shop.insertOne("product", { stock: 1 });
    const art = await catalog.scope(EXPO).insertOne("artwork", { views: 1 });

    await assertRejects(
      () =>
        getSessionContext(db.client).withSession(async () => {
          await shop.collection.updateOne({ _id: product }, {
            $inc: { stock: 1 },
          } as never);
          await catalog.collection.bulkWrite([
            {
              updateOne: {
                filter: { _id: art },
                update: { $inc: { views: 1 } } as never,
              },
            },
          ]);
          throw abort;
        }),
      Error,
      "abort",
    );

    assertEquals((await shop.getById("product", product)).stock, 1);
    assertEquals((await catalog.scope(EXPO).getById("artwork", art)).views, 1);
  });
});

test("ambientSessionCollection: CRUD calls get the session, watch and DDL do not", async (t) => {
  await withDatabase(t.name, async (db) => {
    const calls: Record<string, unknown[]> = {};
    const record =
      (name: string) =>
      (...args: unknown[]) => {
        calls[name] = args;
        return undefined;
      };
    const fake = {
      db,
      updateOne: record("updateOne"),
      find: record("find"),
      watch: record("watch"),
      createIndex: record("createIndex"),
    } as unknown as m.Collection;
    const raw = ambientSessionCollection(fake);

    await getSessionContext(db.client).withSession(async (session) => {
      raw.updateOne({ a: 1 }, { $set: { a: 2 } });
      raw.find({});
      raw.watch([]);
      raw.createIndex({ a: 1 });
      assert(session !== undefined);
      assertEquals(
        (calls.updateOne[2] as { session?: unknown }).session,
        session,
      );
      assertEquals((calls.find[1] as { session?: unknown }).session, session);
    });

    assertEquals(calls.watch, [[]]);
    assertEquals(calls.createIndex, [{ a: 1 }]);
  });
});

import { test } from "./+harness.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { getSessionContext, outsideTransaction } from "../src/session.ts";
import { afterCommit, insideTransaction } from "../src/transaction-scope.ts";
import { increment, pull, push } from "../src/update-operators.ts";

const EXPO = "exposition:expoaaaaa01";

const board = (db: Db) =>
  scopedMultiCollection(db, "board", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: {
      post: {
        title: v.string(),
        rank: v.number(),
        status: v.picklist(["waitlisted", "confirmed"]),
        lifecycle: v.object({ phase: v.string(), count: v.number() }),
        comments: v.array(
          v.object({
            id: v.string(),
            text: v.string(),
            reactions: v.array(v.string()),
          }),
        ),
      },
    },
  });

const post = (title: string, rank: number) => ({
  title,
  rank,
  status: "waitlisted" as const,
  lifecycle: { phase: "draft", count: 0 },
  comments: [
    { id: "c1", text: "first", reactions: [] },
    { id: "c2", text: "second", reactions: ["seen"] },
  ],
});

test("outsideTransaction: a typed write inside it survives the caller's rollback", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await board(db);
    const expo = catalog.scope(EXPO);
    const id = await expo.insertOne("post", post("a", 1));
    const claim = await expo.insertOne("post", post("claim", 1));
    const { withSession, getSession } = getSessionContext(db.client);
    let ranImmediately = false;

    await assertRejects(
      () =>
        withSession(async () => {
          await expo.updateOne("post", id, { rank: increment(1) });
          await outsideTransaction(async () => {
            assertEquals(getSession(), undefined);
            assertEquals(insideTransaction(), false);
            await afterCommit(() => {
              ranImmediately = true;
            });
            await expo.updateOne("post", claim, {
              "lifecycle.count": increment(1),
            });
          });
          assert(getSession() !== undefined, "the caller's session is back");
          throw new Error("abort");
        }),
      Error,
      "abort",
    );

    assertEquals((await expo.getById("post", id)).rank, 1);
    assertEquals((await expo.getById("post", claim)).lifecycle.count, 1);
    assertEquals(ranImmediately, true);
  });
});

test("outsideTransaction: a withSession inside it is a transaction of its own", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await board(db);
    const expo = catalog.scope(EXPO);
    const id = await expo.insertOne("post", post("a", 1));
    const { withSession } = getSessionContext(db.client);

    await assertRejects(
      () =>
        withSession(async (outer) => {
          await outsideTransaction(() =>
            withSession(async (inner) => {
              assert(inner !== undefined && inner !== outer);
              await expo.updateOne("post", id, { rank: increment(5) });
            }),
          );
          throw new Error("abort");
        }),
      Error,
      "abort",
    );
    assertEquals((await expo.getById("post", id)).rank, 6);
  });
});

test("findOneAndUpdate: sort picks the head, upsert creates and returns", async (t) => {
  await withDatabase(t.name, async (db) => {
    const expo = (await board(db)).scope(EXPO);
    await expo.insertOne("post", post("late", 3));
    await expo.insertOne("post", post("head", 1));
    await expo.insertOne("post", post("middle", 2));

    const promoted = await expo.findOneAndUpdate(
      "post",
      { status: "waitlisted" },
      { status: "confirmed" },
      { sort: { rank: 1 } },
    );
    assertEquals(promoted?.title, "head");
    assertEquals(promoted?.status, "confirmed");

    const created = await expo.findOneAndUpdate(
      "post",
      { title: "fresh" },
      { rank: increment(4) },
      {
        upsert: true,
        setOnInsert: {
          status: "waitlisted",
          lifecycle: { phase: "draft", count: 0 },
          comments: [],
        },
      },
    );
    assertEquals(created?.title, "fresh");
    assertEquals(created?.rank, 4);
    assertEquals(created?._scope, EXPO);
  });
});

test("multiCollection findOneAndUpdate: sort and upsert", async (t) => {
  await withDatabase(t.name, async (db) => {
    const queue = await multiCollection(db, "queue", {
      job: { name: v.string(), priority: v.number(), runs: v.number() },
    });
    await queue.insertOne("job", { name: "b", priority: 2, runs: 0 });
    await queue.insertOne("job", { name: "a", priority: 1, runs: 0 });

    const first = await queue.findOneAndUpdate(
      "job",
      {},
      { runs: increment(1) },
      { sort: { priority: 1 } },
    );
    assertEquals(first?.name, "a");

    const made = await queue.findOneAndUpdate(
      "job",
      { name: "c" },
      { runs: increment(1) },
      { upsert: true, setOnInsert: { priority: 9 } },
    );
    assertEquals(made?.name, "c");
    assertEquals(made?.priority, 9);
    assertEquals(made?.runs, 1);
  });
});

test("arrayFilters: positional paths update the matching items, checked against the item schema", async (t) => {
  await withDatabase(t.name, async (db) => {
    const expo = (await board(db)).scope(EXPO);
    const id = await expo.insertOne("post", post("a", 1));

    await expo.updateOne(
      "post",
      id,
      {
        "comments.$[c].reactions": push("like"),
        "comments.$[c].text": "edited",
      },
      { arrayFilters: [{ "c.id": "c1" }] },
    );
    await expo.updateWhere(
      "post",
      { _id: id },
      { "comments.$[c].reactions": pull("seen") },
      { arrayFilters: [{ "c.id": "c2" }] },
    );

    const stored = await expo.getById("post", id);
    assertEquals(stored.comments, [
      { id: "c1", text: "edited", reactions: ["like"] },
      { id: "c2", text: "second", reactions: [] },
    ]);

    await assertRejects(
      () =>
        expo.updateOne(
          "post",
          id,
          { "comments.$[c].text": 3 as never },
          { arrayFilters: [{ "c.id": "c1" }] },
        ),
      v.ValiError,
    );
    await assertRejects(
      () =>
        expo.updateOne(
          "post",
          id,
          { "comments.$[c].reactions": push(7 as never) },
          { arrayFilters: [{ "c.id": "c1" }] },
        ),
      v.ValiError,
    );
  });
});

test("multiCollection arrayFilters on updateOne and findOneAndUpdate", async (t) => {
  await withDatabase(t.name, async (db) => {
    const shop = await multiCollection(db, "shop", {
      order: { lines: v.array(v.object({ sku: v.string(), qty: v.number() })) },
    });
    const id = await shop.insertOne("order", {
      lines: [
        { sku: "A", qty: 1 },
        { sku: "B", qty: 1 },
      ],
    });
    await shop.updateOne(
      "order",
      id,
      { "lines.$[l].qty": increment(2) },
      { arrayFilters: [{ "l.sku": "B" }] },
    );
    const after = await shop.findOneAndUpdate(
      "order",
      { _id: id },
      { "lines.$[l].qty": 10 },
      { arrayFilters: [{ "l.sku": "A" }] },
    );
    assertEquals(after?.lines, [
      { sku: "A", qty: 10 },
      { sku: "B", qty: 3 },
    ]);
  });
});

test("scoped dot paths are typed in updates and filters", async (t) => {
  await withDatabase(t.name, async (db) => {
    const expo = (await board(db)).scope(EXPO);
    const id = await expo.insertOne("post", post("a", 1));
    await expo.updateOne("post", id, { "lifecycle.phase": "live" });
    const live = await expo.find("post", { "lifecycle.phase": "live" });
    assertEquals(live.length, 1);
    await expo.updateWhere(
      "post",
      { "lifecycle.phase": "live" },
      { "lifecycle.count": increment(2) },
    );
    assertEquals((await expo.getById("post", id)).lifecycle.count, 2);

    const typed = () =>
      // @ts-expect-error a number on a string dot path
      expo.updateOne("post", id, { "lifecycle.phase": 3 });
    await typed().catch(() => undefined);
  });
});

test("findProject: nested dot paths return the nested shape", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await board(db);
    const expo = catalog.scope(EXPO);
    const id = await expo.insertOne("post", post("a", 1));

    const rows = await expo.findProject("post", [
      "lifecycle.phase",
      "comments.id",
    ]);
    const phase: string = rows[0].lifecycle.phase;
    const ids: string[] = rows[0].comments.map((c) => c.id);
    assertEquals(phase, "draft");
    assertEquals(ids, ["c1", "c2"]);
    assertEquals(rows, [
      {
        _id: id,
        _type: "post",
        _scope: EXPO,
        lifecycle: { phase: "draft" },
        comments: [{ id: "c1" }, { id: "c2" }],
      },
    ] as never);

    const shop = await multiCollection(db, "shop", {
      product: {
        name: v.string(),
        meta: v.object({ color: v.string(), size: v.number() }),
      },
    });
    const pid = await shop.insertOne("product", {
      name: "lamp",
      meta: { color: "red", size: 2 },
    });
    const products = await shop.findProject("product", ["meta.color"]);
    const color: string = products[0].meta.color;
    assertEquals(color, "red");
    assertEquals(products, [
      { _id: pid, _type: "product", meta: { color: "red" } },
    ] as never);
  });
});

test("collection: arrayFilters, positional checks, sort and outsideTransaction", async (t) => {
  await withDatabase(t.name, async (db) => {
    const orders = await collection(db, "orders", {
      rank: v.number(),
      lines: v.array(v.object({ sku: v.string(), qty: v.number() })),
    });
    const a = await orders.insertOne({
      rank: 2,
      lines: [{ sku: "A", qty: 1 }],
    });
    await orders.insertOne({ rank: 1, lines: [{ sku: "B", qty: 1 }] });

    await orders.updateOne(
      { _id: a },
      { $set: { "lines.$[l].qty": increment(4) } },
      { arrayFilters: [{ "l.sku": "A" }] },
    );
    assertEquals((await orders.getById(a)).lines, [{ sku: "A", qty: 5 }]);
    await assertRejects(
      () =>
        orders.updateOne(
          { _id: a },
          { $set: { "lines.$[l].qty": "many" } },
          { arrayFilters: [{ "l.sku": "A" }] },
        ),
      v.ValiError,
    );

    const head = await orders.findOneAndUpdate(
      {},
      { $set: { rank: increment(10) } },
      { sort: { rank: 1 }, returnDocument: "after" },
    );
    assertEquals(head?.rank, 11);

    await assertRejects(
      () =>
        orders.withSession(async () => {
          await outsideTransaction(() =>
            orders.updateOne({ _id: a }, { $set: { rank: 7 } }),
          );
          throw new Error("abort");
        }),
      Error,
      "abort",
    );
    assertEquals((await orders.getById(a)).rank, 7);
  });
});

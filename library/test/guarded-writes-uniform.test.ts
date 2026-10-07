/**
 * Guarded writes behave the same across `collection`, `multiCollection` and
 * the scoped views (the scoped side lives in
 * `scoped-multi-collection-guarded-writes.test.ts`): the guard is part of the
 * atomic write, value operators are validated against the schema, and an
 * upsert never mints a document its schema would refuse.
 */
import { test } from "./+harness.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import { removeField } from "../src/sanitizer.ts";
import * as v from "../src/schema.ts";

const inboxModel = defineModel("inbox", {
  schema: {
    counter: {
      owner: v.string(),
      seenUpTo: v.string(),
      muted: v.boolean(),
    },
    entry: {
      title: v.string(),
      supersededAt: v.nullable(v.date()),
      engagement: v.object({
        readAt: v.nullable(v.date()),
        archivedAt: v.nullable(v.date()),
      }),
    },
  },
});

const fresh = (title: string) => ({
  title,
  supersededAt: null,
  engagement: { readAt: null, archivedAt: null },
});

// -------- multiCollection ------------------------------------------------

test("multiCollection updateWhere: a guarded miss is counted, not thrown", async (t) => {
  await withDatabase(t.name, async (db) => {
    const inbox = await multiCollection(db, "inbox", inboxModel);
    const id = await inbox.insertOne("entry", fresh("a"));

    assertEquals(
      await inbox.updateWhere(
        "entry",
        { _id: id, supersededAt: null },
        { title: "b" },
      ),
      { matched: 1, modified: 1, upsertedId: null },
    );
    await inbox.updateOne("entry", id, { supersededAt: new Date() });
    assertEquals(
      await inbox.updateWhere(
        "entry",
        { _id: id, supersededAt: null },
        { title: "c" },
      ),
      { matched: 0, modified: 0, upsertedId: null },
    );
    assertEquals((await inbox.getById("entry", id)).title, "b");
  });
});

test("multiCollection updateWhere: max moves forward, upsert mints a valid document once", async (t) => {
  await withDatabase(t.name, async (db) => {
    const inbox = await multiCollection(db, "inbox", inboxModel);

    const first = await inbox.updateWhere(
      "counter",
      { owner: "alice" },
      {},
      { max: { seenUpTo: "k" }, upsert: true, setOnInsert: { muted: false } },
    );
    const upsertedId = first.upsertedId;
    assert(
      upsertedId !== null && upsertedId.startsWith("counter:"),
      "the _id is minted like insertOne",
    );

    await inbox.updateWhere(
      "counter",
      { owner: "alice" },
      {},
      {
        max: { seenUpTo: "c" },
        upsert: true,
        setOnInsert: { muted: false },
      },
    );
    await inbox.updateWhere(
      "counter",
      { owner: "alice" },
      { muted: true },
      {
        upsert: true,
        setOnInsert: { seenUpTo: "" },
      },
    );

    const stored = await inbox.getById("counter", upsertedId);
    assertEquals(
      stored.owner,
      "alice",
      "the filter equality is copied into the insert",
    );
    assertEquals(stored.seenUpTo, "k");
    assertEquals(stored.muted, true);
    assertEquals(await inbox.countDocuments("counter", {}), 1);
  });
});

test("multiCollection updateWhere: an upsert its schema would refuse writes nothing", async (t) => {
  await withDatabase(t.name, async (db) => {
    const inbox = await multiCollection(db, "inbox", inboxModel);
    await assertRejects(() =>
      inbox.updateWhere(
        "counter",
        { owner: "bob" },
        { muted: true },
        { upsert: true },
      ),
    );
    assertEquals(await inbox.countDocuments("counter", {}), 0);
  });
});

test("multiCollection updateWhere: every upsert call must be able to create — even when the document exists", async (t) => {
  await withDatabase(t.name, async (db) => {
    const inbox = await multiCollection(db, "inbox", inboxModel);
    await inbox.insertOne("counter", {
      owner: "carol",
      seenUpTo: "",
      muted: false,
    });
    await assertRejects(() =>
      inbox.updateWhere(
        "counter",
        { owner: "carol" },
        { muted: true },
        { upsert: true },
      ),
    );
    assertEquals(
      await inbox.updateWhere("counter", { owner: "carol" }, { muted: true }),
      { matched: 1, modified: 1, upsertedId: null },
      "a write to a document known to exist does not ask for an upsert",
    );
  });
});

test("multiCollection updateWhere: the multi-collection's own fields cannot be written", async (t) => {
  await withDatabase(t.name, async (db) => {
    const inbox = await multiCollection(db, "inbox", inboxModel);
    await assertRejects(
      () =>
        inbox.updateWhere("counter", { owner: "alice" }, {
          _id: "counter:x",
        } as never),
      Error,
      "cannot be written",
    );
  });
});

test("multiCollection updateWhere: removeField() and dotted paths go through the guard", async (t) => {
  await withDatabase(t.name, async (db) => {
    const inbox = await multiCollection(db, "inbox", inboxModel);
    const id = await inbox.insertOne("entry", fresh("a"));
    const at = new Date("2026-01-01T00:00:00Z");

    await inbox.updateWhere("entry", { _id: id }, {
      "engagement.readAt": at,
    } as never);
    const stored = await inbox.getById("entry", id);
    assertEquals(stored.engagement.readAt?.getTime(), at.getTime());
    assertEquals(
      stored.engagement.archivedAt,
      null,
      "a dotted write keeps its siblings",
    );

    const tags = await multiCollection(
      db,
      "tags",
      defineModel("tags", {
        schema: { tag: { name: v.string(), group: v.optional(v.string()) } },
      }),
    );
    const tagId = await tags.insertOne("tag", { name: "a", group: "g" });
    await tags.updateWhere(
      "tag",
      { _id: tagId, group: "g" },
      { group: removeField() },
    );
    assert(!("group" in (await tags.getById("tag", tagId))));
  });
});

test("multiCollection findOneAndUpdate: before, after, null — and one winner per race", async (t) => {
  await withDatabase(t.name, async (db) => {
    const inbox = await multiCollection(db, "inbox", inboxModel);
    const id = await inbox.insertOne("entry", fresh("head"));
    const at = new Date("2026-01-01T00:00:00Z");

    const before = await inbox.findOneAndUpdate(
      "entry",
      { _id: id, supersededAt: null },
      { supersededAt: at },
      {
        returnDocument: "before",
      },
    );
    assertEquals(before?.supersededAt, null);
    assertEquals(
      await inbox.findOneAndUpdate(
        "entry",
        { _id: id, supersededAt: null },
        { title: "x" },
      ),
      null,
    );
    assertEquals(
      (await inbox.findOneAndUpdate("entry", { _id: id }, { title: "y" }))
        ?.title,
      "y",
    );

    await inbox.insertOne("entry", fresh("next"));
    const retire = () =>
      inbox.findOneAndUpdate(
        "entry",
        { title: "next", supersededAt: null },
        { supersededAt: new Date() },
        {
          returnDocument: "before",
        },
      );
    const [a, b] = await Promise.all([retire(), retire()]);
    assertEquals([a, b].filter((won) => won !== null).length, 1);
  });
});

// -------- collection -----------------------------------------------------

const accountFields = {
  owner: v.string(),
  balance: v.pipe(v.number(), v.minValue(0)),
  plan: v.picklist(["free", "pro"]),
  tags: v.optional(v.array(v.string()), () => []),
};

test("collection: a value operator is validated against the schema before the write", async (t) => {
  await withDatabase(t.name, async (db) => {
    const accounts = await collection(db, "accounts", accountFields);
    await accounts.insertOne({ owner: "alice", balance: 10, plan: "free" });

    await assertRejects(() =>
      accounts.updateOne(
        { owner: "alice" },
        { $set: { plan: "gold" as never } },
      ),
    );
    await assertRejects(() =>
      accounts.updateOne({ owner: "alice" }, { $max: { balance: -1 } }),
    );
    await assertRejects(() =>
      accounts.findOneAndUpdate({ owner: "alice" }, { $set: { balance: -5 } }),
    );
    await assertRejects(() =>
      accounts.updateMany(
        { owner: "alice" },
        { $min: { plan: "gold" as never } },
      ),
    );

    const stored = await accounts.findOne({ owner: "alice" });
    assertEquals(stored?.plan, "free");
    assertEquals(stored?.balance, 10);
  });
});

test("collection: an upsert validates the document it would insert and fills its defaults", async (t) => {
  await withDatabase(t.name, async (db) => {
    const accounts = await collection(db, "accounts", accountFields);

    await assertRejects(() =>
      accounts.updateOne(
        { owner: "bob" },
        { $set: { balance: 5 } },
        { upsert: true },
      ),
    );
    assertEquals(
      await accounts.countDocuments({}),
      0,
      "a document missing its plan is never minted",
    );

    await accounts.updateOne(
      { owner: "bob" },
      { $inc: { balance: 5 }, $setOnInsert: { plan: "free" } },
      {
        upsert: true,
      },
    );
    const stored = await accounts.findOne({ owner: "bob" });
    assertEquals(stored?.balance, 5, "$inc stores its delta on insert");
    assertEquals(stored?.plan, "free");
    assertEquals(stored?.tags, [], "the schema default lands on insert");

    await accounts.updateOne(
      { owner: "bob" },
      { $inc: { balance: 2 }, $setOnInsert: { plan: "pro" } },
      {
        upsert: true,
      },
    );
    const again = await accounts.findOne({ owner: "bob" });
    assertEquals(again?.balance, 7);
    assertEquals(
      again?.plan,
      "free",
      "setOnInsert does not touch an existing document",
    );
  });
});

test("collection: a pipeline update passes as is", async (t) => {
  await withDatabase(t.name, async (db) => {
    const accounts = await collection(db, "accounts", accountFields);
    await accounts.insertOne({ owner: "alice", balance: 10, plan: "free" });

    await accounts.updateOne({ owner: "alice" }, [
      { $set: { balance: { $add: ["$balance", 1] } } },
    ]);
    assertEquals((await accounts.findOne({ owner: "alice" }))?.balance, 11);
  });
});

import { test } from "./+harness.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { removeField } from "../src/sanitizer.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO = "exposition:expoaaaaa01";
const OTHER = "exposition:expobbbbb01";

function catalogOf(db: Parameters<Parameters<typeof withDatabase>[1]>[0]) {
  return scopedMultiCollection(db, "inbox", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: {
      cursor: {
        _id: v.string(),
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
}

const fresh = (title: string) => ({
  title,
  supersededAt: null,
  engagement: { readAt: null, archivedAt: null },
});

test("updateWhere: the guard is part of the write — a miss is counted, not thrown", async () => {
  await withDatabase("smc-guarded-miss", async (db) => {
    const inbox = (await catalogOf(db)).scope(EXPO);
    const id = await inbox.insertOne("entry", fresh("a"));

    const live = await inbox.updateWhere(
      "entry",
      { _id: id, supersededAt: null },
      { title: "b" },
    );
    assertEquals(live, { matched: 1, modified: 1, upsertedId: null });

    await inbox.updateOne("entry", id, { supersededAt: new Date() });
    const stale = await inbox.updateWhere(
      "entry",
      { _id: id, supersededAt: null },
      { title: "c" },
    );
    assertEquals(stale, { matched: 0, modified: 0, upsertedId: null });
    assertEquals((await inbox.getById("entry", id)).title, "b");
  });
});

test("updateWhere: max only ever moves a field forward", async () => {
  await withDatabase("smc-guarded-max", async (db) => {
    const inbox = (await catalogOf(db)).scope(EXPO);
    await inbox.insertOne("cursor", {
      _id: "cursor-1",
      seenUpTo: "m",
      muted: false,
    });

    await inbox.updateWhere(
      "cursor",
      { _id: "cursor-1" },
      {},
      { max: { seenUpTo: "z" } },
    );
    await inbox.updateWhere(
      "cursor",
      { _id: "cursor-1" },
      {},
      { max: { seenUpTo: "c" } },
    );

    assertEquals((await inbox.getById("cursor", "cursor-1")).seenUpTo, "z");
  });
});

test("updateWhere: set and max on the same field is refused before any write", async () => {
  await withDatabase("smc-guarded-max-conflict", async (db) => {
    const inbox = (await catalogOf(db)).scope(EXPO);
    await assertRejects(
      () =>
        inbox.updateWhere(
          "cursor",
          { _id: "cursor-1" },
          { seenUpTo: "a" },
          { max: { seenUpTo: "b" } },
        ),
      Error,
      "cannot be both set and bounded by max",
    );
  });
});

test("updateWhere upsert: a singleton is minted on first write from the filter _id, then updated", async () => {
  await withDatabase("smc-guarded-upsert-singleton", async (db) => {
    const inbox = (await catalogOf(db)).scope(EXPO);

    const first = await inbox.updateWhere(
      "cursor",
      { _id: "cursor-1" },
      {},
      { max: { seenUpTo: "k" }, upsert: true, setOnInsert: { muted: false } },
    );
    assertEquals(first.upsertedId, "cursor-1");

    const second = await inbox.updateWhere(
      "cursor",
      { _id: "cursor-1" },
      { muted: true },
      { upsert: true, setOnInsert: { seenUpTo: "" } },
    );
    assertEquals(second, { matched: 1, modified: 1, upsertedId: null });

    const stored = await inbox.getById("cursor", "cursor-1");
    assertEquals(
      stored.seenUpTo,
      "k",
      "setOnInsert does not overwrite an existing document",
    );
    assertEquals(stored.muted, true);
  });
});

test("updateWhere upsert: the document it would insert is validated first — nothing invalid is ever minted", async () => {
  await withDatabase("smc-guarded-upsert-invalid", async (db) => {
    const inbox = (await catalogOf(db)).scope(EXPO);
    await assertRejects(() =>
      inbox.updateWhere(
        "cursor",
        { _id: "cursor-1" },
        { muted: true },
        { upsert: true },
      ),
    );
    assertEquals(await inbox.countDocuments("cursor"), 0);
  });
});

test("updateWhere upsert: a dotted write keeps the inserted document's sibling fields", async () => {
  await withDatabase("smc-guarded-upsert-dotted", async (db) => {
    const inbox = (await catalogOf(db)).scope(EXPO);
    const at = new Date("2026-01-01T00:00:00Z");

    const result = await inbox.updateWhere(
      "entry",
      { title: "welcome" },
      { "engagement.readAt": at } as never,
      {
        upsert: true,
        setOnInsert: {
          supersededAt: null,
          engagement: { readAt: null, archivedAt: null },
        },
      },
    );
    const upsertedId = result.upsertedId;
    assert(
      upsertedId !== null && upsertedId.startsWith("entry:"),
      "an _id absent from the filter is minted like insertOne",
    );

    const stored = await inbox.getById("entry", upsertedId);
    assertEquals(stored.title, "welcome");
    assertEquals(stored.engagement.readAt?.getTime(), at.getTime());
    assertEquals(stored.engagement.archivedAt, null);
  });
});

test("updateWhere: removeField() unsets through a guarded write", async () => {
  await withDatabase("smc-guarded-remove", async (db) => {
    const catalog = await scopedMultiCollection(db, "tags", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { tag: { name: v.string(), group: v.optional(v.string()) } },
    });
    const tags = catalog.scope(EXPO);
    const id = await tags.insertOne("tag", { name: "a", group: "g" });

    await tags.updateWhere(
      "tag",
      { _id: id, group: "g" },
      { group: removeField() },
    );
    assert(!("group" in (await tags.getById("tag", id))));
  });
});

test("updateWhere: a guarded write never reaches another scope", async () => {
  await withDatabase("smc-guarded-scope", async (db) => {
    const catalog = await catalogOf(db);
    const other = catalog.scope(OTHER);
    const id = await other.insertOne("entry", fresh("theirs"));

    const result = await catalog
      .scope(EXPO)
      .updateWhere("entry", { _id: id }, { title: "mine" });
    assertEquals(result.matched, 0);
    assertEquals((await other.getById("entry", id)).title, "theirs");
  });
});

test("findOneAndUpdate: returns the document before or after, and null on a miss", async () => {
  await withDatabase("smc-find-one-and-update", async (db) => {
    const inbox = (await catalogOf(db)).scope(EXPO);
    const id = await inbox.insertOne("entry", fresh("a"));
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

    const missed = await inbox.findOneAndUpdate(
      "entry",
      { _id: id, supersededAt: null },
      { title: "b" },
    );
    assertEquals(
      missed,
      null,
      "the guard no longer matches a retired document",
    );

    const after = await inbox.findOneAndUpdate(
      "entry",
      { _id: id },
      { title: "c" },
    );
    assertEquals(after?.title, "c");
    assertEquals(after?.supersededAt?.getTime(), at.getTime());
  });
});

test("findOneAndUpdate: of two racing retires of the same head, exactly one wins it", async () => {
  await withDatabase("smc-find-one-and-update-race", async (db) => {
    const inbox = (await catalogOf(db)).scope(EXPO);
    await inbox.insertOne("entry", fresh("head"));

    const retire = () =>
      inbox.findOneAndUpdate(
        "entry",
        { supersededAt: null },
        { supersededAt: new Date() },
        { returnDocument: "before" },
      );
    const [a, b] = await Promise.all([retire(), retire()]);

    assertEquals([a, b].filter((won) => won !== null).length, 1);
  });
});

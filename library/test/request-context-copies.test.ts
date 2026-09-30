import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assert, assertEquals } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { refId } from "../src/ids.ts";
import { type Db, MongoClient } from "../src/mongodb.ts";
import { BSON } from "mongodb";
import { closeAllWatchers } from "../src/change-stream.ts";
import {
  recordingReads,
  type RecordedRead,
  requestReadStats,
  withRequestContext,
} from "../src/request-context.ts";
import { TEST_URI } from "./+shared.ts";

async function withCountedDatabase(
  work: (db: Db, reads: () => number) => Promise<void>,
  clientOptions: Record<string, unknown> = {},
) {
  const client = new MongoClient(TEST_URI, {
    monitorCommands: true,
    ...clientOptions,
  });
  let count = 0;
  client.on("commandStarted", (event) => {
    if (event.commandName === "find" || event.commandName === "aggregate")
      count++;
  });
  const db = client.db(
    `@TEST_reqcopy@${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`,
  );
  try {
    await work(db, () => count);
  } finally {
    await closeAllWatchers(db);
    await db.dropDatabase();
    await client.close();
  }
}

const richSchema = {
  _id: refId("item"),
  label: v.string(),
  at: v.date(),
  ref: v.instance(BSON.ObjectId),
  amount: v.instance(BSON.Decimal128),
  blob: v.instance(BSON.Binary),
  big: v.number(),
  nested: v.object({
    list: v.array(v.object({ n: v.number(), when: v.date() })),
  }),
  maybe: v.nullable(v.string()),
};

function richItem(label: string) {
  return {
    label,
    at: new Date("2026-09-29T10:00:00.123Z"),
    ref: new BSON.ObjectId("65f000000000000000000001"),
    amount: BSON.Decimal128.fromString("12.340"),
    blob: new BSON.Binary(new Uint8Array([1, 2, 3, 250])),
    big: 2 ** 40 + 3,
    nested: {
      list: [
        { n: 1.5, when: new Date("2020-01-01T00:00:00Z") },
        { n: -0, when: new Date(0) },
      ],
    },
    maybe: null,
  };
}

async function items(db: Db) {
  return await scopedMultiCollection(db, "+items", {
    scope: refId("expo"),
    types: { item: richSchema },
  });
}

function sameValue(a: unknown, b: unknown) {
  assertEquals(
    BSON.EJSON.stringify(a as BSON.Document, { relaxed: false }),
    BSON.EJSON.stringify(b as BSON.Document, { relaxed: false }),
  );
}

test("request memo: a memoized read is indistinguishable from a direct one", async () => {
  await withCountedDatabase(async (db) => {
    const expo = (await items(db)).scope("expo:a");
    const id = await expo.insertOne("item", richItem("one"));
    await expo.insertOne("item", richItem("two"));

    const direct = await expo.getById("item", id);
    const directList = await expo.find("item", {});
    const directAggregate = await expo.aggregate(() => [
      { $match: { _type: "item" } },
      { $sort: { label: 1 } },
    ]);

    await withRequestContext(
      async () => {
        for (let round = 0; round < 2; round++) {
          const got = await expo.getById("item", id);
          sameValue(got, direct);
          assert(got.at instanceof Date);
          assert(got.ref instanceof BSON.ObjectId);
          assert(got.amount instanceof BSON.Decimal128);
          assert(got.blob instanceof BSON.Binary);
          assert(Object.is(got.nested.list[1].n, -0));
          sameValue(await expo.find("item", {}), directList);
          sameValue(
            await expo.aggregate(() => [
              { $match: { _type: "item" } },
              { $sort: { label: 1 } },
            ]),
            directAggregate,
          );
        }
        assertEquals(requestReadStats(), {
          loaded: 3,
          reused: 3,
          invalidations: 0,
        });
      },
      { memoizeReads: true },
    );
  });
});

test("request memo: the first caller's mutations never reach a concurrent caller", async () => {
  await withCountedDatabase(async (db, reads) => {
    const expo = (await items(db)).scope("expo:a");
    const id = await expo.insertOne("item", richItem("one"));

    const before = reads();
    await withRequestContext(
      async () => {
        const [first, second] = await Promise.all([
          expo.getById("item", id).then((doc) => {
            doc.label = "mutated";
            doc.nested.list.push({ n: 9, when: new Date() });
            doc.at.setFullYear(1999);
            return doc;
          }),
          expo.getById("item", id),
        ]);
        assert(first !== second);
        assertEquals(second.label, "one");
        assertEquals(second.nested.list.length, 2);
        assertEquals(second.at.toISOString(), "2026-09-29T10:00:00.123Z");
        const third = await expo.getById("item", id);
        assertEquals(third.label, "one");
        assertEquals(third.at.getUTCFullYear(), 2026);
      },
      { memoizeReads: true },
    );
    assertEquals(reads() - before, 1);
  });
});

test("request memo: values that serialize alike but query differently stay apart", async () => {
  await withCountedDatabase(async (db, reads) => {
    const things = await collection(db, "things", {
      key: v.unknown(),
      rank: v.number(),
      tag: v.nullish(v.string()),
    });
    const oid = new BSON.ObjectId("65f000000000000000000002");
    const when = new Date("2026-01-01T00:00:00.000Z");
    await things.insertOne({ key: oid.toHexString(), rank: 1 });
    await things.insertOne({ key: oid, rank: 2 });
    await things.insertOne({ key: when.toISOString(), rank: 3 });
    await things.insertOne({ key: when, rank: 4, tag: "t" });

    const before = reads();
    await withRequestContext(
      async () => {
        assertEquals(
          (await things.findOne({ key: oid.toHexString() }))?.rank,
          1,
        );
        assertEquals((await things.findOne({ key: oid }))?.rank, 2);
        assertEquals(
          (await things.findOne({ key: when.toISOString() }))?.rank,
          3,
        );
        assertEquals((await things.findOne({ key: when }))?.rank, 4);
        assertEquals(
          (await things.findOne({}, { sort: { rank: 1, key: 1 } }))?.rank,
          1,
        );
        assertEquals(
          (await things.findOne({}, { sort: { key: 1, rank: 1 } }))?.rank,
          3,
        );
        assertEquals(
          (await things.findOne({}, { sort: { rank: -1 } }))?.rank,
          4,
        );
        assertEquals((await things.findOne({ rank: 2 }))?.rank, 2);
        assertEquals((await things.findOne({ rank: 2.5 }))?.rank, undefined);
        assertEquals((await things.findOne({ tag: "t" }))?.rank, 4);
        assertEquals((await things.findOne({ tag: null }))?.rank, 1);
      },
      { memoizeReads: true },
    );
    assertEquals(reads() - before, 11);
  });
});

test("request memo: the read recorder sees the rows of a loaded read", async () => {
  await withCountedDatabase(async (db) => {
    const expo = (await items(db)).scope("expo:a");
    const id = await expo.insertOne("item", richItem("one"));
    const recorded: RecordedRead[] = [];
    await withRequestContext(
      () =>
        recordingReads(
          (read) => recorded.push(read),
          async () => {
            await expo.getById("item", id);
            await expo.getById("item", id);
            await expo.find("item", {});
          },
        ),
      { memoizeReads: true },
    );
    assertEquals(recorded.length, 2);
    assertEquals(recorded[0].documents.length, 1);
    assertEquals(recorded[0].documents[0]._id, id);
    assert(recorded[0].documents[0].at instanceof Date);
    assertEquals(recorded[1].documents.length, 1);
  });
});

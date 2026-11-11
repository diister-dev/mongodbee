import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import {
  assert,
  assertEquals,
  assertRejects,
  assertStringIncludes,
  assertThrows,
} from "./+assert.ts";
import { timeSeriesUnsupported, waitUntil, withDatabase } from "./+shared.ts";
import { desc, index, unique, withIndex } from "../src/indexes.ts";
import { outsideTransaction } from "../src/session.ts";
import { DocumentValidationError } from "../src/validation-error.ts";
import {
  defineTimeSeries,
  planTimeSeriesChange,
  TimeSeriesDefinitionError,
  TimeSeriesOptionsDriftError,
  TimeSeriesTransactionError,
  TimeSeriesUnsupportedError,
} from "../src/time-series.ts";
import { timeSeriesCollection } from "../src/time-series-collection.ts";
import { inspectTimeSeries } from "../src/utils/ensure-time-series.ts";

/**
 * Everything that reaches the server states what MongoDB 8.0 does; those
 * cases are reported as skipped on an older one (see timeSeriesUnsupported).
 */
const needs8 = await timeSeriesUnsupported();

/** The suite's default is "managed" (migrations own the schema); these cases own theirs. */
const AUTO = { schemaManagement: "auto" } as const;

const HOUR = 3_600_000;
const DAY = 24 * HOUR;

const scanSchema = v.object({
  at: v.date(),
  meta: v.object({ exposition: v.string(), point: v.string() }),
  scanId: v.string(),
  participantId: v.string(),
  receivedAt: v.date(),
  data: v.optional(v.record(v.string(), v.unknown())),
});

const scans = defineTimeSeries({
  schema: scanSchema,
  timeField: "at",
  metaField: "meta",
  granularity: "minutes",
  expireAfterSeconds: 2 * 365 * 24 * 3600,
  indexes: (f) => [
    index(f.scanId),
    index(f.participantId, desc(f.at)),
    index(f.receivedAt),
  ],
});

function scan(
  at: Date,
  overrides: Partial<v.InferInput<typeof scanSchema>> = {},
): v.InferInput<typeof scanSchema> {
  return {
    at,
    meta: { exposition: "expo-a", point: "door-1" },
    scanId: `scan-${at.getTime()}`,
    participantId: "p-1",
    receivedAt: new Date(),
    ...overrides,
  };
}

test("defineTimeSeries: refuses what MongoDB refuses, where it is declared", () => {
  const schema = v.object({
    at: v.date(),
    meta: v.object({ exposition: v.string() }),
    scanId: v.string(),
    label: v.optional(v.date()),
  });

  const uniqueIndex = assertThrows(
    () =>
      defineTimeSeries({
        schema,
        timeField: "at",
        indexes: (f) => [unique(f.scanId)],
      }),
    TimeSeriesDefinitionError,
  );
  assertStringIncludes(uniqueIndex.message, "unique");
  assertStringIncludes(uniqueIndex.message, "scanId");

  assertThrows(
    () =>
      defineTimeSeries({
        schema: v.object({
          at: v.date(),
          scanId: withIndex(v.string(), { unique: true }),
        }),
        timeField: "at",
      }),
    TimeSeriesDefinitionError,
    "unique",
  );
  assertThrows(
    () =>
      defineTimeSeries({
        schema,
        timeField: "at",
        indexes: (f) => [index(f.at).expiresAfter(60)],
      }),
    TimeSeriesDefinitionError,
    "expireAfterSeconds",
  );
  assertThrows(
    () =>
      defineTimeSeries({ schema, timeField: "at", metaField: "at" as never }),
    TimeSeriesDefinitionError,
    "metaField cannot be the timeField",
  );
  assertThrows(
    () => defineTimeSeries({ schema, timeField: "label" as never }),
    TimeSeriesDefinitionError,
    "must be required",
  );
  assertThrows(
    () => defineTimeSeries({ schema, timeField: "scanId" as never }),
    TimeSeriesDefinitionError,
    "must be a date",
  );
  assertThrows(
    () => defineTimeSeries({ schema, timeField: "missing" as never }),
    TimeSeriesDefinitionError,
    "does not exist",
  );
  assertThrows(
    () =>
      defineTimeSeries({
        schema,
        timeField: "at",
        granularity: "hours",
        bucketMaxSpanSeconds: 7200,
        bucketRoundingSeconds: 7200,
      }),
    TimeSeriesDefinitionError,
    "choose one",
  );
  assertThrows(
    () =>
      defineTimeSeries({
        schema,
        timeField: "at",
        bucketMaxSpanSeconds: 7200,
        bucketRoundingSeconds: 3600,
      }),
    TimeSeriesDefinitionError,
    "same value",
  );
  assertThrows(
    () => defineTimeSeries({ schema, timeField: "at", expireAfterSeconds: -1 }),
    TimeSeriesDefinitionError,
    "expireAfterSeconds",
  );
});

test("planTimeSeriesChange: only what collMod accepts is applicable", () => {
  const base = { timeField: "at", metaField: "meta", granularity: "minutes" };
  const plan = (from: object, to: object) =>
    planTimeSeriesChange(
      { ...base, ...from } as never,
      { ...base, ...to } as never,
    );

  assertEquals(plan({}, {}).inSync, true);

  const expire = plan({}, { expireAfterSeconds: 60 });
  assertEquals(expire.commands, [{ expireAfterSeconds: 60 }]);
  assertEquals(expire.blocked, []);
  assertEquals(expire.irreversible, false);
  assertEquals(plan({ expireAfterSeconds: 60 }, {}).commands, [
    { expireAfterSeconds: "off" },
  ]);

  const widen = plan({}, { granularity: "hours" });
  assertEquals(widen.commands, [{ timeseries: { granularity: "hours" } }]);
  assertEquals(widen.irreversible, true);

  const narrow = plan({}, { granularity: "seconds" });
  assertEquals(narrow.commands, []);
  assertEquals(narrow.blocked.length, 1);
  assertStringIncludes(narrow.blocked[0], "cannot narrow");

  // minutes is a 1 h rounding and a 24 h span: a custom bucket must reach both.
  const custom = (seconds: number) => ({
    granularity: undefined,
    bucketMaxSpanSeconds: seconds,
    bucketRoundingSeconds: seconds,
  });
  assertEquals(plan({}, custom(7200)).blocked.length, 1);
  assertEquals(plan({}, custom(86_400)).commands, [
    {
      timeseries: {
        bucketMaxSpanSeconds: 86_400,
        bucketRoundingSeconds: 86_400,
      },
    },
  ]);
  assertEquals(
    plan(custom(100_000), { granularity: "hours" }).blocked.length,
    1,
  );
  assertEquals(plan(custom(600), {}).commands, [
    { timeseries: { granularity: "minutes" } },
  ]);

  assertStringIncludes(plan({}, { timeField: "when" }).blocked[0], "timeField");
  assertStringIncludes(plan({}, { metaField: "tags" }).blocked[0], "metaField");
  assertStringIncludes(
    plan({}, { metaField: undefined }).blocked[0],
    "cannot become absent",
  );
});

test({
  name: "Time series: created as a time series collection, without a validator, and reopened as is",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      await timeSeriesCollection(db, "scans", scans, AUTO);
      const [info] = await db.listCollections({ name: "scans" }).toArray();
      assertEquals(info.type, "timeseries");
      const options = (info as { options: Record<string, any> }).options;
      assertEquals(options.timeseries.timeField, "at");
      assertEquals(options.timeseries.metaField, "meta");
      assertEquals(options.timeseries.granularity, "minutes");
      assertEquals(options.validator, undefined);

      const inspection = await inspectTimeSeries(db, "scans", scans.options);
      assert(inspection.state === "exists");
      assertEquals(inspection.stored.expireAfterSeconds, 63_072_000);
      assertEquals(inspection.plan.inSync, true);

      // A second boot finds nothing to do, and loses nothing.
      const reopened = await timeSeriesCollection(db, "scans", scans, AUTO);
      await reopened.insertOne(scan(new Date()));
      await timeSeriesCollection(db, "scans", scans, AUTO);
      assertEquals(await reopened.countDocuments(), 1);
    });
  },
});

test({
  name: "Time series: with the schema managed by migrations, an insert never creates a regular collection",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      // The default: nothing is created at open, the migrations do it.
      const collection = await timeSeriesCollection(db, "scans", scans);
      assertEquals((await db.listCollections().toArray()).length, 0);
      assertEquals(await collection.countDocuments(), 0);

      const early = await assertRejects(
        () => collection.insertOne(scan(new Date())),
        TimeSeriesUnsupportedError,
      );
      assertStringIncludes(early.message, "does not exist yet");
      assertStringIncludes(early.message, "createTimeSeriesCollection");
      await assertRejects(
        () => collection.insertMany([scan(new Date())]),
        TimeSeriesUnsupportedError,
      );
      assertEquals((await db.listCollections().toArray()).length, 0);

      // A regular collection under the name is not written into either.
      await db.createCollection("scans");
      await assertRejects(
        () => collection.insertOne(scan(new Date())),
        TimeSeriesOptionsDriftError,
        "regular collection",
      );
      await db.collection("scans").drop();

      // Once it exists as declared (here: what the migration would do), go.
      await timeSeriesCollection(db, "scans", scans, AUTO);
      await collection.insertOne(scan(new Date()));
      assertEquals(await collection.countDocuments(), 1);
    });
  },
});

test({
  name: "Time series: measurements inserted out of order and days late are read back by time range",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const collection = await timeSeriesCollection(db, "scans", scans, AUTO);
      const now = Date.now();
      const at = (offset: number) => new Date(now - offset);

      // An offline device syncs: newest first, then three-day-old scans, then
      // one from the middle, each batch after the previous one was stored.
      await collection.insertOne(scan(at(0), { scanId: "now" }));
      await collection.insertMany([
        scan(at(3 * DAY), { scanId: "three-days" }),
        scan(at(3 * DAY - HOUR), { scanId: "three-days-minus-1h" }),
      ]);
      await collection.insertOne(scan(at(DAY), { scanId: "yesterday" }));
      await collection.insertOne(scan(at(2 * HOUR), { scanId: "two-hours" }));

      const ordered = await collection.find({}).sort({ at: 1 }).toArray();
      assertEquals(
        ordered.map((doc) => doc.scanId),
        ["three-days", "three-days-minus-1h", "yesterday", "two-hours", "now"],
      );

      const window = await collection
        .find({ at: { $gte: at(DAY + HOUR), $lt: at(HOUR) } })
        .sort({ at: -1 })
        .toArray();
      assertEquals(
        window.map((doc) => doc.scanId),
        ["two-hours", "yesterday"],
      );

      const latest = await collection
        .find({})
        .sort({ at: -1 })
        .limit(2)
        .toArray();
      assertEquals(
        latest.map((doc) => doc.scanId),
        ["now", "two-hours"],
      );
      assert(latest[0].at instanceof Date);
      assert(latest[0]._id !== undefined);

      assertEquals(
        await collection.countDocuments({ at: { $lt: at(2 * DAY) } }),
        2,
      );
      const perDay = await collection
        .aggregate<{ _id: string; count: number }>([
          { $match: { at: { $gte: at(4 * DAY) } } },
          { $group: { _id: "$meta.exposition", count: { $sum: 1 } } },
        ])
        .toArray();
      assertEquals(perDay, [{ _id: "expo-a", count: 5 }]);

      const cursor = collection.find({}).sort({ at: 1 });
      const seen: string[] = [];
      for await (const doc of cursor) seen.push(doc.scanId);
      assertEquals(seen.length, 5);
    });
  },
});

test({
  name: "Time series: Valibot validates every insert and every read, with no server validator",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const collection = await timeSeriesCollection(db, "scans", scans, AUTO);

      await assertRejects(
        () =>
          collection.insertOne({
            ...scan(new Date()),
            participantId: 42 as never,
          }),
        DocumentValidationError,
      );
      await assertRejects(
        () =>
          collection.insertMany([
            scan(new Date()),
            { ...scan(new Date()), at: "yesterday" as never },
          ]),
        DocumentValidationError,
      );
      // insertMany validates the whole batch before sending any of it.
      assertEquals(await collection.countDocuments(), 0);

      // The server takes anything with a date: only the read tells.
      await collection.collection.insertOne({ at: new Date(), scanId: 7 });
      await assertRejects(
        () => collection.find({}).toArray(),
        DocumentValidationError,
      );
      await assertRejects(
        () => collection.findOne({}),
        DocumentValidationError,
      );
    });
  },
});

test({
  name: "Time series: declared secondary indexes are created, serve queries and are reconciled",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const collection = await timeSeriesCollection(db, "scans", scans, AUTO);
      const names = async () =>
        (await db.collection("scans").indexes()).map((i) => i.name).sort();

      assertEquals(await names(), [
        "_idx_participantId_asc_at_desc",
        "_idx_receivedAt_asc",
        "_idx_scanId_asc",
        "meta_1_at_1",
      ]);
      const byName = Object.fromEntries(
        (await db.collection("scans").indexes()).map((i) => [i.name, i]),
      );
      assertEquals(byName["_idx_participantId_asc_at_desc"].key, {
        participantId: 1,
        at: -1,
      });
      assertEquals(byName["_idx_scanId_asc"].unique, undefined);

      const now = Date.now();
      await collection.insertMany(
        Array.from({ length: 40 }, (_, i) =>
          scan(new Date(now - i * 60_000), {
            scanId: `s-${i}`,
            participantId: `p-${i % 4}`,
          }),
        ),
      );
      const found = await collection
        .find({ scanId: "s-17" }, { hint: "_idx_scanId_asc" })
        .toArray();
      assertEquals(found.length, 1);
      const recent = await collection
        .find(
          { participantId: "p-2" },
          { hint: "_idx_participantId_asc_at_desc" },
        )
        .sort({ at: -1 })
        .limit(3)
        .toArray();
      assertEquals(
        recent.map((doc) => doc.scanId),
        ["s-2", "s-6", "s-10"],
      );

      // Same declaration: nothing dropped, nothing rebuilt.
      await db
        .collection("scans")
        .createIndex({ "data.kind": 1 }, { name: "by_hand" });
      await timeSeriesCollection(db, "scans", scans, AUTO);
      assertEquals((await names()).includes("by_hand"), true);

      // One index removed, one added, one defined on a field with withIndex.
      const changed = defineTimeSeries({
        schema: v.object({
          ...scanSchema.entries,
          receivedAt: withIndex(v.date()),
        }),
        timeField: "at",
        metaField: "meta",
        granularity: "minutes",
        expireAfterSeconds: 2 * 365 * 24 * 3600,
        indexes: (f) => [
          index(f.scanId),
          index(f.meta.point, f.participantId).where(
            f.meta.exposition,
            "expo-a",
          ),
        ],
      });
      await timeSeriesCollection(db, "scans", changed, AUTO);
      assertEquals(await names(), [
        "_idx_meta.point_asc_participantId_asc",
        "_idx_scanId_asc",
        "by_hand",
        "meta_1_at_1",
        "receivedAt",
      ]);
      await timeSeriesCollection(db, "scans", changed, AUTO);
      assertEquals((await names()).length, 5);
    });
  },
});

test({
  name: "Time series: deleteMany deletes on a measurement field, on the meta and on time",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const collection = await timeSeriesCollection(db, "scans", scans, AUTO);
      const now = Date.now();
      await collection.insertMany([
        scan(new Date(now), { scanId: "a", participantId: "alice" }),
        scan(new Date(now - HOUR), { scanId: "b", participantId: "bob" }),
        scan(new Date(now - 2 * HOUR), { scanId: "c", participantId: "alice" }),
        scan(new Date(now - 3 * HOUR), {
          scanId: "d",
          participantId: "bob",
          meta: { exposition: "expo-b", point: "door-9" },
        }),
        scan(new Date(now - 4 * DAY), { scanId: "e", participantId: "carol" }),
      ]);

      // GDPR erasure of one participant: a measurement field, mid-bucket.
      const erased = await collection.deleteMany({ participantId: "alice" });
      assertEquals(erased.deletedCount, 2);
      assertEquals(
        (await collection.find({}).sort({ at: -1 }).toArray()).map(
          (d) => d.scanId,
        ),
        ["b", "d", "e"],
      );

      // A whole exposition: the meta.
      assertEquals(
        (await collection.deleteMany({ "meta.exposition": "expo-b" }))
          .deletedCount,
        1,
      );
      // And by time.
      assertEquals(
        (await collection.deleteMany({ at: { $lt: new Date(now - DAY) } }))
          .deletedCount,
        1,
      );
      assertEquals(await collection.countDocuments(), 1);

      await assertRejects(
        () => collection.deleteMany({}),
        Error,
        "the filter is empty",
      );
      assertEquals(await collection.countDocuments(), 1);
    });
  },
});

test({
  name: "Time series: a write inside withSession is refused before it reaches the server",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const collection = await timeSeriesCollection(db, "scans", scans, AUTO);
      await collection.insertOne(scan(new Date(), { scanId: "before" }));

      await collection.withSession(async (session) => {
        assert(session?.inTransaction(), "the test needs a real transaction");

        const insert = await assertRejects(
          () => collection.insertOne(scan(new Date())),
          TimeSeriesTransactionError,
        );
        assertStringIncludes(
          insert.message,
          'timeSeriesCollection("scans").insertOne',
        );
        assertStringIncludes(insert.message, "outsideTransaction");
        await assertRejects(
          () => collection.insertMany([scan(new Date())]),
          TimeSeriesTransactionError,
        );
        await assertRejects(
          () => collection.deleteMany({ scanId: "before" }),
          TimeSeriesTransactionError,
        );
        await assertRejects(
          () =>
            collection
              .withMeta({ exposition: "expo-a" })
              .insertOne(scan(new Date())),
          TimeSeriesTransactionError,
        );
        assertThrows(
          () => collection.collection.insertOne({ at: new Date() }),
          TimeSeriesTransactionError,
          "collection.insertOne",
        );

        // Reads are allowed in a transaction, and the refusals left it usable.
        assertEquals(await collection.countDocuments(), 1);
        assertEquals((await collection.find({}).toArray()).length, 1);
        assertEquals((await collection.aggregate([]).toArray()).length, 1);

        // The documented way out: a write that commits on its own.
        await outsideTransaction(() =>
          collection.insertOne(scan(new Date(), { scanId: "outside" })),
        );
      });

      assertEquals(
        (await collection.find({}).sort({ at: 1 }).toArray()).map(
          (d) => d.scanId,
        ),
        ["before", "outside"],
      );

      // An explicit session in a transaction is refused the same way.
      const session = db.client.startSession();
      try {
        session.startTransaction();
        await assertRejects(
          () => collection.insertOne(scan(new Date()), { session } as never),
          TimeSeriesTransactionError,
        );
        await session.abortTransaction();
      } finally {
        await session.endSession();
      }
    });
  },
});

test({
  name: "Time series: updates, replaces, upserts and change streams are neither typed nor silent",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const collection = await timeSeriesCollection(db, "scans", scans, AUTO);

      // @ts-expect-error updateOne is not part of a time series collection
      collection.updateOne;
      // @ts-expect-error nor is replaceOne
      collection.replaceOne;
      // @ts-expect-error nor is watch
      collection.watch;
      type InsertOptions = NonNullable<
        Parameters<typeof collection.insertOne>[1]
      >;
      // @ts-expect-error nor does a write take a session
      type Session = InsertOptions["session"];
      const noSession: Session | undefined = undefined;
      void noSession;

      const loose = collection as unknown as Record<
        string,
        (...args: unknown[]) => unknown
      >;
      for (const operation of [
        "updateOne",
        "updateMany",
        "replaceOne",
        "findOneAndUpdate",
        "findOneAndReplace",
        "findOneAndDelete",
        "bulkWrite",
        "upsert",
        "watch",
        "on",
      ]) {
        const error = assertThrows(
          () => loose[operation]({}, {}),
          TimeSeriesUnsupportedError,
        );
        assertStringIncludes(
          error.message,
          `timeSeriesCollection("scans").${operation} is not available`,
        );
      }
      assertEquals(Object.keys(collection).includes("updateOne"), false);

      await assertRejects(
        () =>
          timeSeriesCollection(db, "watched", scans, {
            ...AUTO,
            enableWatching: true,
          } as never),
        TimeSeriesUnsupportedError,
        "change streams",
      );
    });
  },
});

test({
  name: "Time series: withMeta binds a view to an exposition, as .scope(id) does",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const collection = await timeSeriesCollection(db, "scans", scans, AUTO);
      const now = Date.now();
      const expoA = collection.withMeta({ exposition: "expo-a" });
      const expoB = collection.withMeta({ exposition: "expo-b" });

      // The bound key is filled by the view; the rest of the meta is the caller's.
      await expoA.insertOne({
        at: new Date(now),
        meta: { point: "door-1" },
        scanId: "a1",
        participantId: "alice",
        receivedAt: new Date(),
      });
      await expoA.insertMany([
        { ...scan(new Date(now - HOUR)), scanId: "a2", participantId: "bob" },
      ]);
      await expoB.insertOne({
        at: new Date(now - 2 * HOUR),
        meta: { point: "door-7" },
        scanId: "b1",
        participantId: "alice",
        receivedAt: new Date(),
      });

      const stored = await collection.findOne({ scanId: "a1" });
      assertEquals(stored?.meta, { exposition: "expo-a", point: "door-1" });

      assertEquals(
        (await expoA.find({}).sort({ at: -1 }).toArray()).map((d) => d.scanId),
        ["a1", "a2"],
      );
      assertEquals(await expoB.countDocuments({ participantId: "alice" }), 1);
      assertEquals(await expoA.findOne({ scanId: "b1" }), null);
      assertEquals(
        await expoB
          .aggregate([{ $group: { _id: null, count: { $sum: 1 } } }])
          .toArray(),
        [{ _id: null, count: 1 }],
      );
      // A filter cannot widen the view, even naming the bound key.
      assertEquals(
        await expoA.countDocuments({ "meta.exposition": "expo-b" } as never),
        0,
      );
      assertEquals(
        await expoA.countDocuments({
          $or: [{ scanId: "b1" }, { scanId: "a1" }],
        }),
        1,
      );

      // A fully bound view makes the meta optional.
      const door = expoA.withMeta({ point: "door-1" });
      await door.insertOne({
        at: new Date(now - 3 * HOUR),
        scanId: "a3",
        participantId: "carol",
        receivedAt: new Date(),
      });
      assertEquals(door.boundMeta, { exposition: "expo-a", point: "door-1" });
      assertEquals(await door.countDocuments(), 3);
      assertEquals(
        await expoA.withMeta({ point: "door-2" }).countDocuments(),
        0,
      );

      await assertRejects(
        () =>
          expoA.insertOne(
            scan(new Date(), { meta: { exposition: "expo-b", point: "x" } }),
          ),
        Error,
        "this view is bound to",
      );
      assertThrows(
        () => expoA.withMeta({ exposition: "expo-b" }),
        TimeSeriesDefinitionError,
        "already bound",
      );
      assertThrows(
        () => collection.withMeta({ nope: "x" } as never),
        TimeSeriesDefinitionError,
        "not a key of meta",
      );
      assertThrows(
        () => collection.withMeta({ exposition: 12 } as never),
        DocumentValidationError,
      );
      assertThrows(
        () => collection.withMeta({}),
        TimeSeriesDefinitionError,
        "at least one meta key",
      );

      // Deleting through the view stays inside it.
      assertEquals(
        (await expoA.deleteMany({ participantId: "alice" })).deletedCount,
        1,
      );
      assertEquals(await expoB.countDocuments({ participantId: "alice" }), 1);
      await assertRejects(
        () => expoA.deleteMany({}),
        Error,
        "the filter is empty",
      );

      // Removing an exposition is an explicit filter on the collection.
      assertEquals(
        (await collection.deleteMany({ "meta.exposition": "expo-a" }))
          .deletedCount,
        2,
      );
      assertEquals(await collection.countDocuments(), 1);
    });
  },
});

test({
  name: "Time series: option drift is applied when collMod accepts it and refused otherwise",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const declare = (
        overrides: Partial<Parameters<typeof defineTimeSeries>[0]> = {},
      ) =>
        defineTimeSeries({
          schema: scanSchema,
          timeField: "at",
          metaField: "meta",
          granularity: "minutes",
          expireAfterSeconds: 3600,
          ...overrides,
        } as Parameters<typeof defineTimeSeries<typeof scanSchema, "meta">>[0]);
      const stored = async () => {
        const inspection = await inspectTimeSeries(db, "scans", scans.options);
        assert(inspection.state === "exists");
        return inspection.stored;
      };

      const collection = await timeSeriesCollection(
        db,
        "scans",
        declare(),
        AUTO,
      );
      await collection.insertOne(scan(new Date()));

      // expireAfterSeconds: changed, then removed.
      await timeSeriesCollection(
        db,
        "scans",
        declare({ expireAfterSeconds: 7200 }),
        AUTO,
      );
      assertEquals((await stored()).expireAfterSeconds, 7200);
      await timeSeriesCollection(
        db,
        "scans",
        declare({ expireAfterSeconds: undefined }),
        AUTO,
      );
      assertEquals((await stored()).expireAfterSeconds, undefined);

      // Buckets widen.
      await timeSeriesCollection(
        db,
        "scans",
        declare({ expireAfterSeconds: undefined, granularity: "hours" }),
        AUTO,
      );
      assertEquals((await stored()).granularity, "hours");

      // And never narrow back: reported, nothing changed.
      const narrow = await assertRejects(
        () =>
          timeSeriesCollection(
            db,
            "scans",
            declare({ expireAfterSeconds: 60 }),
            AUTO,
          ),
        TimeSeriesOptionsDriftError,
      );
      assertStringIncludes(narrow.message, "cannot narrow");
      assertEquals(narrow.blocked.length, 1);
      assertEquals((await stored()).granularity, "hours");
      assertEquals((await stored()).expireAfterSeconds, undefined);

      const otherMeta = await assertRejects(
        () =>
          timeSeriesCollection(
            db,
            "scans",
            defineTimeSeries({
              schema: scanSchema,
              timeField: "receivedAt",
              granularity: "hours",
            }),
            AUTO,
          ),
        TimeSeriesOptionsDriftError,
      );
      assertEquals(otherMeta.blocked.length, 2);
      assertStringIncludes(otherMeta.message, 'timeField is "at"');
      assertStringIncludes(otherMeta.message, 'metaField is "meta"');

      const inspection = await inspectTimeSeries(
        db,
        "scans",
        declare().options,
      );
      assert(inspection.state === "exists");
      assertEquals(inspection.plan.inSync, false);
      assertEquals(inspection.plan.blocked.length, 1);
      assertEquals(inspection.plan.changes.length, 1);

      await db.createCollection("plain");
      await assertRejects(
        () => timeSeriesCollection(db, "plain", declare(), AUTO),
        TimeSeriesOptionsDriftError,
        "already exists as a regular collection",
      );
      assertEquals(
        (await inspectTimeSeries(db, "nowhere", declare().options)).state,
        "missing",
      );
      assertEquals(await collection.countDocuments(), 1);
    });
  },
});

test({
  name: "Time series: expireAfterSeconds removes measurements once their bucket has expired",
  ignore: needs8,
  // The TTL monitor wakes every 60 s; a bucket goes when all of it has expired.
  timeout: 200_000,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const collection = await timeSeriesCollection(
        db,
        "scans",
        defineTimeSeries({
          schema: scanSchema,
          timeField: "at",
          metaField: "meta",
          expireAfterSeconds: 3600,
        }),
        AUTO,
      );
      const now = Date.now();
      // Late data older than the retention is accepted, then expires at once.
      await collection.insertMany([
        scan(new Date(now - 5 * HOUR), { scanId: "expired-1" }),
        scan(new Date(now - 5 * HOUR - 1000), { scanId: "expired-2" }),
        scan(new Date(now), { scanId: "kept" }),
      ]);
      assertEquals(await collection.countDocuments(), 3);

      let remaining: string[] = [];
      let polling = false;
      await waitUntil(
        () => {
          if (!polling) {
            polling = true;
            collection
              .find({})
              .toArray()
              .then((docs) => {
                remaining = docs.map((doc) => doc.scanId);
              })
              .finally(() => {
                polling = false;
              });
          }
          return remaining.length === 1;
        },
        "the expired bucket to be removed",
        180_000,
      );
      assertEquals(remaining, ["kept"]);
    });
  },
});

import { test } from "../+harness.ts";
import {
  assert,
  assertEquals,
  assertRejects,
  assertStringIncludes,
  assertThrows,
} from "../+assert.ts";
import { TEST_URI, timeSeriesUnsupported, withDatabase } from "../+shared.ts";
import { type Db, MongoClient } from "../../src/mongodb.ts";
import * as v from "../../src/schema.ts";
import { index } from "../../src/indexes.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import {
  getIrreversibleOperations,
  migrationBuilder,
} from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createSimulationValidator } from "../../src/migration/validators/simulation.ts";
import { validateLastMigrationMatchesProjectSchema } from "../../src/migration/schema-validation.ts";
import type {
  MigrationBuilder,
  MigrationDefinition,
  SchemasDefinition,
} from "../../src/migration/types.ts";
import { defineTimeSeries } from "../../src/time-series.ts";
import { timeSeriesCollection } from "../../src/time-series-collection.ts";
import { inspectTimeSeries } from "../../src/utils/ensure-time-series.ts";

/**
 * Everything that reaches the server states what MongoDB 8.0 does; those
 * cases are reported as skipped on an older one (see timeSeriesUnsupported).
 */
const needs8 = await timeSeriesUnsupported();

const measurement = {
  at: v.date(),
  meta: v.object({ exposition: v.string(), point: v.string() }),
  scanId: v.string(),
  participantId: v.string(),
};

type Overrides = {
  timeField?: "at";
  metaField?: "meta";
  granularity?: "seconds" | "minutes" | "hours";
  expireAfterSeconds?: number;
  extraIndex?: boolean;
  extraField?: boolean;
};

function scans(overrides: Overrides = {}) {
  // The variants share one static type; only the runtime declaration moves.
  const schema = (
    overrides.extraField
      ? v.object({ ...measurement, device: v.optional(v.string()) })
      : v.object(measurement)
  ) as v.ObjectSchema<typeof measurement, undefined>;
  return defineTimeSeries({
    schema,
    timeField: "at",
    metaField: ("metaField" in overrides
      ? overrides.metaField
      : "meta") as "meta",
    granularity: overrides.granularity ?? "minutes",
    ...("expireAfterSeconds" in overrides
      ? { expireAfterSeconds: overrides.expireAfterSeconds }
      : { expireAfterSeconds: 3600 }),
    indexes: (f) => [
      index(f.scanId),
      ...(overrides.extraIndex ? [index(f.participantId)] : []),
    ],
  });
}

function schemasWith(overrides?: Overrides): SchemasDefinition {
  return {
    collections: { expositions: { name: v.string() } },
    timeSeriesCollections: { scans: scans(overrides) },
  };
}

function initial(): MigrationDefinition {
  return migrationDefinition("001", "init", {
    parent: null,
    schemas: schemasWith(),
    migrate: (b) =>
      b
        .createCollection("expositions")
        .end()
        .createTimeSeriesCollection("scans")
        .end()
        .compile(),
  });
}

function next(
  parent: MigrationDefinition,
  overrides: Overrides,
  migrate: (b: MigrationBuilder) => MigrationBuilder = (b) => b,
  id = "002",
): MigrationDefinition {
  return migrationDefinition(id, "change", {
    parent,
    schemas: schemasWith(overrides),
    migrate: (b) => migrate(b).compile(),
  });
}

const updateOptions = (b: MigrationBuilder) =>
  b.timeSeriesCollection("scans").updateOptions().end();

function operationsOf(migration: MigrationDefinition) {
  return migration.migrate(
    migrationBuilder({
      schemas: migration.schemas,
      parentSchemas: migration.parent?.schemas,
    }),
  ).operations;
}

async function apply(
  db: Db,
  migration: MigrationDefinition,
  direction: "up" | "down" = "up",
) {
  await createMongodbApplier(db, migration).applyMigration(
    operationsOf(migration),
    direction,
  );
}

async function simulate(migration: MigrationDefinition) {
  return await createSimulationValidator({
    powerLevel: "quick",
  }).validateMigration(migration);
}

test("Time series migration: the snapshot carries the creation options and the builder exposes no document rewrite", () => {
  const migration = initial();
  const snapshot = migration.schemas.timeSeriesCollections?.scans;
  assertEquals(snapshot?.options, {
    timeField: "at",
    metaField: "meta",
    granularity: "minutes",
    expireAfterSeconds: 3600,
  });

  const [, create] = operationsOf(migration);
  assert(create.type === "create_timeseries_collection");
  assertEquals(create.schema.options, snapshot?.options);

  const builder = migrationBuilder({ schemas: migration.schemas });
  const series = builder.timeSeriesCollection("scans");
  // @ts-expect-error a time series collection has no transform
  series.transform;
  // @ts-expect-error nor a seed
  series.seed;

  const loose = series as unknown as Record<string, (arg?: unknown) => unknown>;
  const transform = assertThrows(() =>
    loose.transform({ up: (doc: unknown) => doc, down: (doc: unknown) => doc }),
  );
  assertStringIncludes(
    transform.message,
    'timeSeriesCollection("scans").transform() is not available',
  );
  assertStringIncludes(transform.message, "replaceOne");
  assertThrows(() => loose.seed([]), Error, "upsert");

  // The regular-collection builders refuse a time series by name.
  assertThrows(
    () => builder.collection("scans"),
    Error,
    'collection("scans"): "scans" is a time series collection',
  );
  assertThrows(
    () => builder.createCollection("scans"),
    Error,
    "createTimeSeriesCollection",
  );
  assertThrows(() => builder.updateIndexes("scans"), Error, "time series");
  assertThrows(
    () => builder.renameCollection("scans", "old_scans"),
    Error,
    "cannot rename",
  );
  assertThrows(
    () =>
      builder.flow({
        from: { collection: "expositions" },
        into: { collection: "scans" },
        map: (doc) => doc,
      }),
    Error,
    "time series",
  );
  assertThrows(
    () => builder.createTimeSeriesCollection("nope"),
    Error,
    "schema not found in migration.schemas.timeSeriesCollections",
  );
});

test("Time series migration: an option change is applied by updateOptions or refused as not applicable", async () => {
  const m1 = initial();

  // A new collection that migrate() never creates.
  const forgotten = migrationDefinition("001", "init", {
    parent: null,
    schemas: schemasWith(),
    migrate: (b) => b.createCollection("expositions").end().compile(),
  });
  const notCreated = await simulate(forgotten);
  assertEquals(notCreated.success, false);
  assertStringIncludes(
    notCreated.errors.join("\n"),
    'Time series collection "scans" is declared in schema but not created in migrate()',
  );
  assertEquals((await simulate(m1)).errors, []);

  // collMod-able change, declared but never applied.
  const silent = await simulate(next(m1, { expireAfterSeconds: 7200 }));
  assertEquals(silent.success, false);
  assertStringIncludes(
    silent.errors.join("\n"),
    "expireAfterSeconds goes from 3600s to 7200s, but migrate() never applies it",
  );
  assertStringIncludes(silent.errors.join("\n"), "updateOptions()");

  // The same change, applied: reversible.
  const expire = next(m1, { expireAfterSeconds: 7200 }, updateOptions);
  assertEquals((await simulate(expire)).errors, []);
  assertEquals(getIrreversibleOperations(operationsOf(expire)), []);

  // Wider buckets: applicable, and irreversible.
  const widen = next(m1, { granularity: "hours" }, updateOptions);
  assertEquals((await simulate(widen)).errors, []);
  assertEquals(
    getIrreversibleOperations(operationsOf(widen)).map((op) => op.type),
    ["update_timeseries_options"],
  );

  // What MongoDB fixes at creation: refused when the migration is built...
  const narrow = next(m1, { granularity: "seconds" }, updateOptions);
  const refused = assertThrows(() => operationsOf(narrow));
  assertStringIncludes(refused.message, "not applicable");
  assertStringIncludes(refused.message, "cannot narrow");

  // ...and reported by the simulation even when no operation mentions it.
  const unsaid = await simulate(next(m1, { metaField: undefined }));
  assertEquals(unsaid.success, false);
  assertStringIncludes(
    unsaid.errors.join("\n"),
    'Time series collection "scans": option change is not applicable. metaField is "meta" and cannot become absent',
  );

  // A changed measurement schema cannot be transformed: said, not hidden.
  const reshaped = await simulate(next(m1, { extraField: true }));
  assertEquals(reshaped.errors, []);
  assertStringIncludes(
    reshaped.warnings.join("\n"),
    "the measurement schema changed, and a migration cannot rewrite stored measurements",
  );

  // One name is one physical collection.
  const clash = await simulate(
    migrationDefinition("001", "init", {
      parent: null,
      schemas: {
        collections: { scans: { name: v.string() } },
        timeSeriesCollections: { scans: scans() },
      },
      migrate: (b) => b.createTimeSeriesCollection("scans").end().compile(),
    }),
  );
  assertStringIncludes(
    clash.errors.join("\n"),
    "is also declared in schemas.collections",
  );
});

test("Time series migration: schemas.ts drifting from the last migration is reported, options included", () => {
  const last = initial();
  const check = (overrides?: Overrides) =>
    validateLastMigrationMatchesProjectSchema(last, schemasWith(overrides));

  assertEquals(check().errors, []);

  const expire = check({ expireAfterSeconds: 60 });
  assertEquals(expire.valid, false);
  assertStringIncludes(
    expire.errors.join("\n"),
    'Time series collection "scans" options differ (last migration → project schema): expireAfterSeconds goes from 3600s to 60s',
  );
  assertStringIncludes(
    check({ granularity: "seconds" }).errors.join("\n"),
    "cannot narrow",
  );
  assertStringIncludes(
    check({ extraIndex: true }).errors.join("\n"),
    'Time series collection "scans" indexes differ',
  );
  assertStringIncludes(
    check({ extraField: true }).errors.join("\n"),
    'Time series collection "scans" schema differs',
  );

  const without = validateLastMigrationMatchesProjectSchema(last, {
    collections: { expositions: { name: v.string() } },
  });
  assertStringIncludes(
    without.errors.join("\n"),
    "Extra time series collections in last migration not in project schema: scans",
  );
  const added = validateLastMigrationMatchesProjectSchema(last, {
    ...schemasWith(),
    timeSeriesCollections: { scans: scans(), heartbeats: scans() },
  });
  assertStringIncludes(
    added.errors.join("\n"),
    "Time series collections missing in last migration: heartbeats",
  );
});

test({
  name: "Time series migration: create, update the options, roll back, against a real server",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const stored = async () => {
        const inspection = await inspectTimeSeries(
          db,
          "scans",
          scans().options,
        );
        return inspection.state === "exists"
          ? inspection.stored
          : inspection.state;
      };
      const indexNames = async () =>
        (await db.collection("scans").indexes()).map((i) => i.name).sort();

      // Up: the collection is a time series, with its options and indexes.
      const m1 = initial();
      await apply(db, m1);
      assertEquals(await stored(), {
        timeField: "at",
        metaField: "meta",
        granularity: "minutes",
        expireAfterSeconds: 3600,
      });
      assertEquals(await indexNames(), ["_idx_scanId_asc", "meta_1_at_1"]);
      const [info] = await db.listCollections({ name: "scans" }).toArray();
      assertEquals(info.type, "timeseries");
      // The regular collection of the same migration kept its validator.
      const [plain] = await db
        .listCollections({ name: "expositions" })
        .toArray();
      assert((plain as { options: { validator?: unknown } }).options.validator);

      // The application, schema managed by the migrations, writes into it.
      const collection = await timeSeriesCollection(db, "scans", scans());
      await collection.insertOne({
        at: new Date(),
        meta: { exposition: "expo-a", point: "door-1" },
        scanId: "s-1",
        participantId: "p-1",
      });

      // Applying the same migration again changes nothing and loses nothing.
      await apply(db, m1);
      assertEquals(await collection.countDocuments(), 1);

      // Up: expireAfterSeconds changes, an index is added.
      const m2 = next(
        m1,
        { expireAfterSeconds: 7200, extraIndex: true },
        updateOptions,
      );
      await apply(db, m2);
      assertEquals(
        ((await stored()) as { expireAfterSeconds?: number })
          .expireAfterSeconds,
        7200,
      );
      assertEquals(await indexNames(), [
        "_idx_participantId_asc",
        "_idx_scanId_asc",
        "meta_1_at_1",
      ]);

      // Down: both come back to the parent's.
      await apply(db, m2, "down");
      assertEquals(
        ((await stored()) as { expireAfterSeconds?: number })
          .expireAfterSeconds,
        3600,
      );
      assertEquals(await indexNames(), ["_idx_scanId_asc", "meta_1_at_1"]);
      assertEquals(await collection.countDocuments(), 1);

      // Up: wider buckets, which no rollback narrows back.
      const m3 = next(m1, { granularity: "hours" }, updateOptions, "003");
      await apply(db, m3);
      assertEquals(
        ((await stored()) as { granularity?: string }).granularity,
        "hours",
      );
      await assertRejects(
        () => apply(db, m3, "down"),
        Error,
        "irreversible operation(s) [update_timeseries_options]",
      );
      assertEquals(
        ((await stored()) as { granularity?: string }).granularity,
        "hours",
      );

      // Down of the creation drops the collection and its buckets.
      const fresh = await (async () => {
        const client = new MongoClient(TEST_URI);
        return {
          client,
          db: client.db(`@TEST_tsdown@${crypto.randomUUID().slice(0, 8)}`),
        };
      })();
      try {
        await apply(fresh.db, m1);
        await apply(fresh.db, m1, "down");
        // MongoDB keeps the (now empty) system.views it created for the
        // time series; the collection and its system.buckets are gone.
        assertEquals(
          (await fresh.db.listCollections().toArray())
            .map((c) => c.name)
            .filter((name) => name !== "system.views"),
          [],
        );
      } finally {
        await fresh.db.dropDatabase();
        await fresh.client.close();
      }
    });
  },
});

test({
  name: "Time series migration: drift between the database and the snapshot is reported, not applied",
  ignore: needs8,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const m1 = initial();

      // The name is taken by a regular collection: nothing is converted.
      await db.createCollection("scans");
      await assertRejects(
        () => apply(db, m1),
        Error,
        '"scans" already exists as a regular collection',
      );
      await db.collection("scans").drop();

      // A time series created by hand with another timeField.
      await db.createCollection("scans", {
        timeseries: { timeField: "receivedAt", metaField: "meta" },
      });
      const fixed = await assertRejects(() => apply(db, m1), Error);
      assertStringIncludes(
        fixed.message,
        'timeField is "receivedAt" and cannot become "at"',
      );
      const [kept] = await db.listCollections({ name: "scans" }).toArray();
      assertEquals(
        (kept as unknown as { options: { timeseries: { timeField: string } } })
          .options.timeseries.timeField,
        "receivedAt",
      );
      await db.collection("scans").drop();

      // An option changed behind the migrations' back: the next migration
      // says so and leaves it, since none of its operations asked for it.
      await apply(db, m1);
      await db.command({ collMod: "scans", expireAfterSeconds: 99 });
      const warnings: string[] = [];
      const { setLogSink } = await import("../../src/utils/logger.ts");
      setLogSink((record) => {
        if (record.level === "warn") warnings.push(record.message);
      });
      try {
        await apply(db, next(m1, { extraIndex: true }));
      } finally {
        setLogSink(undefined);
      }
      assertStringIncludes(
        warnings.join("\n"),
        'Time series "scans" differs from the migration\'s schema: expireAfterSeconds goes from 99s to 3600s',
      );
      const inspection = await inspectTimeSeries(db, "scans", scans().options);
      assert(inspection.state === "exists");
      assertEquals(inspection.stored.expireAfterSeconds, 99);

      // And a drift collMod cannot undo fails the migration that meets it.
      await db.command({
        collMod: "scans",
        timeseries: { granularity: "hours" },
      });
      await assertRejects(
        () => apply(db, next(m1, { extraIndex: true })),
        Error,
        "cannot narrow",
      );
    });
  },
});

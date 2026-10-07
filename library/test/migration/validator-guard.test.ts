/**
 * Validators a migration switches off can no longer stay off unnoticed.
 *
 * A migration disables the validators of the collections it touches, runs,
 * then restores them. Two ways left one off, silently, until a later
 * `ensureValidator` printed `validation was off/error, restoring strict/error`:
 *
 * - the restore stopped at the first failing collection (a unique index the
 *   data violates), so every LATER collection kept validation off;
 * - the process died between disabling and restoring (a timeout killing the
 *   CLI), and nothing recorded it.
 *
 * Locked here: the restore attempts every collection; a durable record is
 * written before disabling and cleared only after the restore; the CLI finds
 * the record and repairs explicitly.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals, assertRejects } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import type { Db } from "../../src/mongodb.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { markMigrationAsApplied } from "../../src/migration/state.ts";
import {
  getValidatorSuspension,
  recordValidatorSuspension,
  VALIDATOR_GUARD_COLLECTION,
} from "../../src/migration/validator-guard.ts";
import { repairSuspendedValidators } from "../../src/migration/cli/utils/validator-repair.ts";
import { withIndex } from "../../src/indexes.ts";
import { refId } from "../../src/ids.ts";
import { type LogRecord, setLogSink } from "../../src/utils/logger.ts";
import * as v from "../../src/schema.ts";

async function validationOf(db: Db, name: string) {
  const [info] = await db.listCollections({ name }).toArray();
  const options = (info as { options?: Record<string, unknown> })?.options;
  return {
    level: (options?.validationLevel as string | undefined) ?? "strict",
    rules: Object.keys((options?.validator as object | undefined) ?? {}).length,
  };
}

test("validator guard: a successful migration leaves no record", async () => {
  await withDatabase("vguard-clean", async (db) => {
    const S = {
      collections: { alpha: { name: v.string() } },
    };
    const m = migrationDefinition("001", "alpha", {
      parent: null,
      schemas: S,
      migrate: (b) => b.createCollection("alpha").end().compile(),
    });
    const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
    await createMongodbApplier(db, m).applyMigration(ops, "up");
    assertEquals(await getValidatorSuspension(db), null);
    assertEquals((await validationOf(db, "alpha")).level, "strict");
    // No bookkeeping collection left behind in the database's shape.
    const names = (await db.listCollections().toArray()).map((c) => c.name);
    assertEquals(names.includes(VALIDATOR_GUARD_COLLECTION), false);
  });
});

test("validator guard: one failing collection no longer leaves the later ones off", async () => {
  await withDatabase("vguard-partial", async (db) => {
    // `aaa` cannot get its unique index (duplicates); `zzz` comes after it.
    await db.createCollection("aaa");
    await db.collection("aaa").insertMany([{ name: "dup" }, { name: "dup" }]);
    await db.createCollection("zzz");
    const S = {
      collections: {
        aaa: { name: withIndex(v.string(), { unique: true }) },
        zzz: { name: v.string() },
      },
    };
    const m = migrationDefinition("001", "indexes", {
      parent: null,
      schemas: S,
      migrate: (b) => b.compile(),
    });

    await assertRejects(
      () => createMongodbApplier(db, m).applyMigration([], "up"),
      Error,
      "aaa",
    );

    const zzz = await validationOf(db, "zzz");
    assertEquals(
      zzz.level,
      "strict",
      "the later collection got its validator back",
    );
    assert(zzz.rules > 0, "with its rules");
    // The run did not complete: the record stays for the next command.
    const record = await getValidatorSuspension(db);
    assert(record !== null);
    assert(record.collections.includes("aaa"));
  });
});

test("validator guard: an interrupted run is found and repaired explicitly", async () => {
  await withDatabase("vguard-repair", async (db) => {
    const S = {
      collections: { alpha: { name: v.string() } },
    };
    const m = migrationDefinition("001", "alpha", {
      parent: null,
      schemas: S,
      migrate: (b) => b.createCollection("alpha").end().compile(),
    });
    const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
    await createMongodbApplier(db, m).applyMigration(ops, "up");
    await markMigrationAsApplied(db, m.id, m.name, 0);

    // What a run killed half-way leaves behind: the record, written before
    // disabling, and the validator off. Plus a collection the interrupted
    // migration had created, unknown to the applied head.
    await recordValidatorSuspension(db, {
      migrationId: "002",
      direction: "up",
      collections: ["alpha", "created_by_002"],
    });
    await db.command({
      collMod: "alpha",
      validator: {},
      validationLevel: "off",
    });
    await db.createCollection("created_by_002", { validationLevel: "off" });

    const lines: string[] = [];
    const repaired = await repairSuspendedValidators(db, [m], (line) =>
      lines.push(line),
    );

    assertEquals(repaired, true);
    const output = lines.join("\n");
    assert(output.includes("interrupted migration of 002"), output);
    assert(output.includes("alpha"), output);
    assertEquals(await getValidatorSuspension(db), null);
    const alpha = await validationOf(db, "alpha");
    assertEquals(alpha.level, "strict");
    assert(alpha.rules > 0);
    assertEquals((await validationOf(db, "created_by_002")).level, "strict");
    await assertRejects(() =>
      db.collection("alpha").insertOne({ name: 42 } as never),
    );

    // Nothing left: a second call is a no-op.
    assertEquals(await repairSuspendedValidators(db, [m], () => {}), false);
  });
});

test("validator guard: a normal migration does not report its own restore as a leftover", async () => {
  await withDatabase("vguard-quiet", async (db) => {
    const S = {
      scopedMultiCollections: {
        "+notes": {
          scope: refId("space"),
          types: { note: { text: v.string() } },
        },
      },
    };
    const m1 = migrationDefinition("001", "notes", {
      parent: null,
      schemas: S,
      migrate: (b) => b.createScopedMultiCollection("+notes").end().compile(),
    });
    const m2 = migrationDefinition("002", "noop", {
      parent: m1,
      schemas: S,
      migrate: (b) => b.compile(),
    });
    const records: LogRecord[] = [];
    setLogSink((record) => records.push(record));
    try {
      await createMongodbApplier(db, m1).applyMigration(
        m1.migrate(migrationBuilder({ schemas: S })).operations,
        "up",
      );
      await createMongodbApplier(db, m2).applyMigration([], "up");
    } finally {
      setLogSink(undefined);
    }
    const leftovers = records.filter((r) =>
      String(r.message).includes("validation was off"),
    );
    assertEquals(
      leftovers.map((r) => r.message),
      [],
    );
    assertEquals((await validationOf(db, "+notes")).level, "strict");
    assert((await validationOf(db, "+notes")).rules > 0);
  });
});

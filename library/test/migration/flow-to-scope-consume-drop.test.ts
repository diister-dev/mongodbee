/**
 * Regression: `flowToScope({ source: "consume" })` dropped the consumed source
 * collection with `await sourceColl.drop().catch(() => {})`, swallowing EVERY
 * error. A real drop failure (auth denial, write conflict, transient cluster
 * error) was therefore silently ignored: the migration reported success and
 * was recorded as applied, while the source collection survived with its data
 * intact — a consolidation falsely reported complete, on the exact diivento
 * consume path. flowToScope is irreversible, so there is no rollback.
 *
 * The fix tolerates ONLY "collection does not exist" (idempotent re-run) and
 * rethrows anything else.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals, assertRejects } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import type { Db } from "../../src/mongodb.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import * as v from "../../src/schema.ts";

const S = { collections: { roots: { _id: v.string() } } };

function consumeMigration() {
  return migrationDefinition(
    "2025_01_01_0000_AAAAAAAAAAAAAAAAAAAAAAAAAA@consume",
    "consume",
    {
      parent: null,
      schemas: S,
      migrate: (b) =>
        b
          .flowToScope({
            from: { kind: "collection", name: "roots" },
            into: { collection: "scoped" },
            toType: () => "info",
            scope: (d) => d._id as string,
            source: "consume",
          })
          .compile(),
    },
  );
}

/**
 * Wraps a Db so that `db.collection(targetName).drop()` rejects with `error`.
 * Every other operation (find/insert/listCollections/…) is delegated to the
 * real database, so the flow itself runs normally up to the drop.
 */
function dbWithDropFailure(realDb: Db, targetName: string, error: unknown): Db {
  return new Proxy(realDb, {
    get(target, prop, _receiver) {
      if (prop === "collection") {
        return (name: string, ...args: any[]) => {
          const coll = (
            target.collection as (n: string, ...a: unknown[]) => unknown
          )(name, ...args);
          if (name !== targetName) return coll;
          return new Proxy(coll as object, {
            get(ct, cprop) {
              if (cprop === "drop") return () => Promise.reject(error);
              const val = Reflect.get(ct, cprop);
              return typeof val === "function" ? val.bind(ct) : val;
            },
          });
        };
      }
      const val = Reflect.get(target, prop);
      return typeof val === "function" ? val.bind(target) : val;
    },
  }) as unknown as Db;
}

test("mongodb flowToScope consume: a real drop() failure fails the migration loudly (not green)", async () => {
  await withDatabase("flow-consume-drop-fail", async (db) => {
    await db.collection("roots").insertMany([
      { _id: "exposition:A", v: 1 },
      { _id: "exposition:B", v: 2 },
    ] as never);

    const m = consumeMigration();
    const ops = m.migrate(migrationBuilder({ schemas: S })).operations;

    // A non-"namespace not found" failure must propagate.
    const dropError = Object.assign(new Error("simulated drop failure"), {
      code: 8000,
      codeName: "AtlasError",
    });
    const wrapped = dbWithDropFailure(db, "roots", dropError);

    await assertRejects(
      () =>
        createMongodbApplier(wrapped, m, {
          currentMigrationId: m.id,
        }).applyMigration(ops, "up"),
      Error,
      "simulated drop failure",
    );

    // The source was NOT silently consumed (it is still there) — the loud
    // failure is exactly what lets an operator notice the incomplete state.
    assertEquals(await db.collection("roots").countDocuments(), 2);
  });
});

test("mongodb flowToScope consume: NamespaceNotFound on drop is tolerated (idempotent re-run)", async () => {
  await withDatabase("flow-consume-drop-missing", async (db) => {
    await db
      .collection("roots")
      .insertMany([{ _id: "exposition:A", v: 1 }] as never);

    const m = consumeMigration();
    const ops = m.migrate(migrationBuilder({ schemas: S })).operations;

    const nsError = Object.assign(new Error("ns not found"), {
      code: 26,
      codeName: "NamespaceNotFound",
    });
    const wrapped = dbWithDropFailure(db, "roots", nsError);

    // Should NOT throw: a missing source collection means it was already
    // consumed; the consolidation goal still holds.
    await createMongodbApplier(wrapped, m, {
      currentMigrationId: m.id,
    }).applyMigration(ops, "up");

    // The flow still happened: docs landed in the scoped target.
    const scoped = await db
      .collection("scoped")
      .find({} as never)
      .toArray();
    assert(
      scoped.length >= 1,
      "documents should have been flowed into the scoped collection",
    );
  });
});

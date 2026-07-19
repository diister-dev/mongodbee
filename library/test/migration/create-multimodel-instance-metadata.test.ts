/**
 * Regression: `create_multimodel_instance` gated bookkeeping-metadata creation
 * on whether a collection of that NAME exists (`collectionExists`). When a
 * collection already existed without metadata — a plain-collection name
 * collision, or a crash between `createCollection` and the metadata insert on a
 * non-transactional run — it took the "already exists" branch, ran only a
 * `collMod`, and NEVER wrote the `_information`/`_migrations` docs. The instance
 * was then invisible to `multiCollectionInstanceExists` and its per-instance
 * migration history was silently dropped, while the migration still reported
 * success.
 *
 * The fix gates metadata creation on REGISTRATION
 * (`multiCollectionInstanceExists`), not on collection-name existence.
 */
import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import {
  MULTI_COLLECTION_MIGRATIONS_TYPE,
  multiCollectionInstanceExists,
} from "../../src/migration/multicollection-registry.ts";
import * as v from "../../src/schema.ts";

const INSTANCE = "events:legacy";
const MIGRATION_ID =
  "2025_01_01_0000_AAAAAAAAAAAAAAAAAAAAAAAAAA@create_instance";

const S = {
  collections: {},
  multiModels: {
    events: {
      log: { _id: v.string(), msg: v.string() },
    },
  },
};

function createInstanceMigration() {
  return migrationDefinition(MIGRATION_ID, "create_instance", {
    parent: null,
    schemas: S,
    migrate: (b) => {
      b.createMultiModelInstance(INSTANCE, "events");
      return b.compile();
    },
  });
}

Deno.test("create_multimodel_instance: a pre-existing un-registered collection still gets its metadata", async () => {
  await withDatabase("create-mmi-metadata", async (db) => {
    // Simulate the collision / partial-prior-run state: the collection already
    // exists by name but has NO multi-collection bookkeeping.
    await db.createCollection(INSTANCE);
    assertEquals(
      await multiCollectionInstanceExists(db, INSTANCE),
      false,
      "precondition: collection exists but is not yet a registered instance",
    );

    const m = createInstanceMigration();
    const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
    await createMongodbApplier(db, m, { currentMigrationId: m.id })
      .applyMigration(ops, "up");

    // The instance must now be registered (the `_information` doc was written).
    assertEquals(
      await multiCollectionInstanceExists(db, INSTANCE),
      true,
      "instance should be registered even though the collection pre-existed",
    );

    // And its per-instance migration history must have been recorded.
    const migrationsDoc = await db.collection(INSTANCE).findOne(
      { _type: MULTI_COLLECTION_MIGRATIONS_TYPE } as never,
    ) as { appliedMigrations?: Array<{ id: string }> } | null;
    assert(migrationsDoc, "_migrations bookkeeping doc should exist");
    assert(
      (migrationsDoc!.appliedMigrations ?? []).some((x) =>
        x.id === MIGRATION_ID
      ),
      "the creating migration should be recorded in the instance history",
    );
  });
});

Deno.test("create_multimodel_instance: fresh creation still registers (no regression)", async () => {
  await withDatabase("create-mmi-fresh", async (db) => {
    const m = createInstanceMigration();
    const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
    await createMongodbApplier(db, m, { currentMigrationId: m.id })
      .applyMigration(ops, "up");

    assertEquals(await multiCollectionInstanceExists(db, INSTANCE), true);
  });
});

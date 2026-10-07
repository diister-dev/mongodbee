/**
 * Rolling back a migration that ADDED a type to a multi-collection removes
 * that type's indexes.
 *
 * The orphan sweep of `applyMultiCollectionIndexes` only recognised indexes
 * whose name starts with a type the schema still declares. After a rollback
 * the added type is gone from the schema, so its indexes matched no prefix
 * and survived: a unique `singleton` index of a type that no longer exists,
 * still enforcing its constraint.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { withIndex } from "../../src/indexes.ts";
import * as v from "../../src/schema.ts";

const grant = { key: withIndex(v.string(), { unique: true }) };
const setting = { singleton: withIndex(v.string(), { unique: true }) };

test("rollback of an added multi-collection type drops that type's indexes", async () => {
  await withDatabase("rollback-type-indexes", async (db) => {
    const S1 = { multiCollections: { platform: { grant } } };
    const S2 = { multiCollections: { platform: { grant, setting } } };
    const m1 = migrationDefinition("001", "platform", {
      parent: null,
      schemas: S1,
      migrate: (b) => b.createMultiCollection("platform").end().compile(),
    });
    const m2 = migrationDefinition("002", "setting", {
      parent: m1,
      schemas: S2,
      migrate: (b) => b.compile(),
    });

    await createMongodbApplier(db, m1).applyMigration(
      m1.migrate(migrationBuilder({ schemas: S1 })).operations,
      "up",
    );
    const ops2 = m2.migrate(
      migrationBuilder({ schemas: S2, parentSchemas: S1 }),
    ).operations;
    await createMongodbApplier(db, m2).applyMigration(ops2, "up");

    const names = async () =>
      (await db.collection("platform").indexes()).map((i) => i.name ?? "");
    assert(
      (await names()).some((n) => n.startsWith("setting_")),
      "the added type got its index",
    );

    // A user index on the same collection, without mongodbee's `_type` pin,
    // must be spared.
    await db
      .collection("platform")
      .createIndex({ note: 1 }, { name: "setting_note_by_hand" });

    await createMongodbApplier(db, m2).applyMigration(ops2, "down");

    const after = await names();
    assertEquals(
      after.filter(
        (n) => n.startsWith("setting_") && n !== "setting_note_by_hand",
      ),
      [],
      `indexes left: ${after.join(", ")}`,
    );
    assert(after.includes("setting_note_by_hand"), after.join(", "));
    assert(
      after.some((n) => n.startsWith("grant_")),
      "the remaining type keeps its index",
    );
  });
});

/**
 * Regression: rolling back a migration that contains an irreversible
 * operation used to fail *midway* — earlier operations were already
 * reversed before the irreversible one threw, leaving a partial rollback.
 *
 * The fix pre-scans operations before mutating anything: if any are
 * irreversible, applyMigration('down') throws up-front and the database is
 * left untouched.
 */
import { assert, assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import {
  getIrreversibleOperations,
  getLossyOperations,
} from "../../src/migration/builder.ts";
import * as v from "../../src/schema.ts";

const SCHEMAS = {
  collections: {
    users: { _id: v.string(), name: v.string(), secret: v.optional(v.string()) },
  },
};

function buildOps() {
  const m = migrationDefinition("001", "hash-secrets", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) =>
      b.createCollection("users")
        .seed([{ _id: "users:1", name: "Alice" }])
        .transform({
          up: (doc) => ({ ...doc, secret: "hashed" }),
          down: (doc) => doc,
          irreversible: true,
        })
        .end()
        .compile(),
  });
  return { m, ops: m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations };
}

Deno.test("getIrreversibleOperations / getLossyOperations detect flagged ops", () => {
  const { ops } = buildOps();
  assertEquals(getIrreversibleOperations(ops).length, 1);
  // createCollection marks the migration lossy as a property, but the op
  // itself is not a lossy *transform*; getLossyOperations only reports
  // transforms flagged lossy.
  assertEquals(getLossyOperations(ops).length, 0);
});

Deno.test("memory applier: down throws up-front on irreversible, leaves state intact", async () => {
  const { m, ops } = buildOps();
  const state = createEmptyDatabaseState();
  const applier = createMemoryApplier(m);

  await applier.applyMigration(state, ops, "up");
  const before = state.collections.users.content.length;
  assertEquals(before, 1);

  await assertRejects(
    () => applier.applyMigration(state, ops, "down"),
    Error,
    "irreversible",
  );

  // State must be untouched — the create_collection was NOT dropped.
  assert(state.collections.users, "collection should still exist");
  assertEquals(state.collections.users.content.length, before);
});

Deno.test("mongodb applier: down throws up-front on irreversible, leaves data intact", async () => {
  await withDatabase("irreversible-prescan", async (db) => {
    const { m, ops } = buildOps();
    const applier = createMongodbApplier(db, m, { currentMigrationId: m.id });

    await applier.applyMigration(ops, "up");
    assertEquals(await db.collection("users").countDocuments(), 1);

    await assertRejects(
      () => applier.applyMigration(ops, "down"),
      Error,
      "irreversible",
    );

    // The collection and its docs must still be there.
    assertEquals(
      await db.collection("users").countDocuments(),
      1,
      "irreversible rollback must not partially mutate the database",
    );
  });
});

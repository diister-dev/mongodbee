/**
 * Regression: seeds without an explicit `_id` could not be rolled back —
 * the forward applier generated a random ULID, but the reverse applier
 * tried to delete by `_id` from `operation.documents` (which had no `_id`
 * field), so the seeded documents stayed in the database after rollback.
 *
 * The fix is to derive a **deterministic** `_id` from the migration
 * context (migration id + document index) so apply and reverse compute
 * the same id, even across process restarts.
 */
import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { dbId, refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const SCHEMAS = {
  collections: {
    users: {
      _id: dbId("user"),
      name: v.string(),
      email: v.string(),
    },
  },
};

Deno.test("memory applier: seed without _id can be reversed cleanly", async () => {
  const state = createEmptyDatabaseState();
  const m = migrationDefinition("001", "seed-users", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) =>
      b.createCollection("users")
        .seed([
          { name: "Alice", email: "a@x" },
          { name: "Bob", email: "b@x" },
        ])
        .end()
        .compile(),
  });

  const applier = createMemoryApplier(m);
  const ops = m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;

  await applier.applyMigration(state, ops, "up");
  assertEquals(state.collections.users.content.length, 2);

  await applier.applyMigration(state, ops, "down");
  // After full rollback the create_collection is also undone, so the
  // collection is removed. Either way, no seeded docs must linger.
  assertEquals(
    state.collections.users?.content.length ?? 0,
    0,
    "rollback should leave no seeded docs",
  );
});

Deno.test("memory applier: seed-only rollback empties collection without dropping it", async () => {
  // Isolate seed reversal: collection pre-exists, migration only seeds.
  const state = createEmptyDatabaseState();
  state.collections.users = { content: [] };

  const m = migrationDefinition("001", "seed-users", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) =>
      b.collection("users")
        .seed([
          { name: "Alice", email: "a@x" },
          { name: "Bob", email: "b@x" },
        ])
        .end()
        .compile(),
  });
  const ops = m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;
  const applier = createMemoryApplier(m);

  await applier.applyMigration(state, ops, "up");
  assertEquals(state.collections.users.content.length, 2);

  await applier.applyMigration(state, ops, "down");
  assertEquals(
    state.collections.users.content.length,
    0,
    "seed rollback should empty the collection while keeping it",
  );
});

Deno.test("memory applier: seed IDs are deterministic across replays", async () => {
  // Build the same migration twice and run apply both times. The
  // generated `_id`s should be identical, which is what makes rollback
  // safe across process boundaries.
  const make = () =>
    migrationDefinition("001", "seed-users", {
      parent: null,
      schemas: SCHEMAS,
      migrate: (b) =>
        b.createCollection("users")
          .seed([
            { name: "Alice", email: "a@x" },
            { name: "Bob", email: "b@x" },
          ])
          .end()
          .compile(),
    });

  const m1 = make();
  const m2 = make();

  const s1 = createEmptyDatabaseState();
  const s2 = createEmptyDatabaseState();

  const ops1 = m1.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;
  const ops2 = m2.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;

  await createMemoryApplier(m1).applyMigration(s1, ops1, "up");
  await createMemoryApplier(m2).applyMigration(s2, ops2, "up");

  const ids1 = s1.collections.users.content.map((d) => d._id).sort();
  const ids2 = s2.collections.users.content.map((d) => d._id).sort();
  assertEquals(ids1, ids2, "same migration applied twice should produce the same _ids");
});

Deno.test("mongodb applier: bare refId _id schema (no default) gets a valid prefixed id", async () => {
  // Regression: when the _id schema is a bare refId (no auto-default), the
  // deterministic id must still carry the "user:" prefix so it satisfies the
  // schema's own `^user:` validator — not a prefix-less ":abc" id.
  const REFID_SCHEMAS = {
    collections: {
      members: {
        _id: refId("member"),
        name: v.string(),
      },
    },
  };

  await withDatabase("seed-bare-refid", async (db) => {
    const m = migrationDefinition("001", "seed-members", {
      parent: null,
      schemas: REFID_SCHEMAS,
      migrate: (b) =>
        b.createCollection("members")
          .seed([{ name: "Alice" }, { name: "Bob" }])
          .end()
          .compile(),
    });

    const applier = createMongodbApplier(db, m, { currentMigrationId: m.id });
    const ops = m.migrate(migrationBuilder({ schemas: REFID_SCHEMAS })).operations;

    // Would throw on insert if the generated id were ":abc" (fails ^member:)
    await applier.applyMigration(ops, "up");

    const docs = await db.collection("members").find({}).toArray();
    assertEquals(docs.length, 2);
    for (const d of docs) {
      assert(
        String(d._id).startsWith("member:"),
        `expected member:* id, got ${String(d._id)}`,
      );
    }

    await applier.applyMigration(ops, "down");
    assertEquals(await db.collection("members").countDocuments(), 0);
  });
});

Deno.test("mongodb applier: seed without _id can be reversed cleanly", async () => {
  await withDatabase("seed-reversal-mongo", async (db) => {
    const m = migrationDefinition("001", "seed-users", {
      parent: null,
      schemas: SCHEMAS,
      migrate: (b) =>
        b.createCollection("users")
          .seed([
            { name: "Alice", email: "a@x" },
            { name: "Bob", email: "b@x" },
            { name: "Carol", email: "c@x" },
          ])
          .end()
          .compile(),
    });

    const applier = createMongodbApplier(db, m, { currentMigrationId: m.id });
    const ops = m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;

    await applier.applyMigration(ops, "up");
    const afterUp = await db.collection("users").countDocuments();
    assertEquals(afterUp, 3);

    await applier.applyMigration(ops, "down");
    const afterDown = await db.collection("users").countDocuments();
    assertEquals(
      afterDown,
      0,
      "rollback must delete all seeded docs even when _id was auto-generated",
    );
  });
});

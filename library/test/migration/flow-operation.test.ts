/**
 * `flow` — cross-collection document movement (régime A).
 *
 * COPY (source: "keep") is reversible by construction: the target `_id` is
 * derived deterministically from the source `_id`, so rollback recomputes
 * the same ids (source is still present) and deletes the copies.
 *
 * MOVE (source: "consume") deletes the source; reversing it would require a
 * provenance log (régime B), so for now a move is marked irreversible and
 * the pre-rollback gate refuses it.
 */
import { assert, assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder, getIrreversibleOperations } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { dbId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const SCHEMAS = {
  collections: {
    users: { _id: dbId("user"), name: v.string(), active: v.boolean() },
    archived_users: {
      _id: dbId("archived"),
      name: v.string(),
      active: v.boolean(),
      archivedReason: v.string(),
    },
  },
};

function copyMigration(source: "keep" | "consume") {
  return migrationDefinition("002", "archive-inactive", {
    parent: migrationDefinition("001", "init", {
      parent: null,
      schemas: SCHEMAS,
      migrate: (b) =>
        b.createCollection("users").end()
          .createCollection("archived_users").end()
          .compile(),
    }),
    schemas: SCHEMAS,
    migrate: (b) =>
      b.flow({
        from: { collection: "users", where: { active: false } },
        into: { collection: "archived_users" },
        map: (doc) => ({ ...doc, archivedReason: "inactivity" }),
        source,
      }).compile(),
  });
}

Deno.test("memory flow COPY: copies matching docs, leaves source intact, reversible", async () => {
  const state = createEmptyDatabaseState();
  state.collections.users = {
    content: [
      { _id: "user:1", name: "Alice", active: true },
      { _id: "user:2", name: "Bob", active: false },
      { _id: "user:3", name: "Carol", active: false },
    ],
  };
  state.collections.archived_users = { content: [] };

  const m = copyMigration("keep");
  const ops = m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;
  const applier = createMemoryApplier(m);

  await applier.applyMigration(state, ops, "up");
  // Source untouched
  assertEquals(state.collections.users.content.length, 3);
  // Only inactive copied
  assertEquals(state.collections.archived_users.content.length, 2);
  assert(
    state.collections.archived_users.content.every((d) =>
      d.active === false && d.archivedReason === "inactivity"
    ),
  );

  // Rollback removes exactly the copies
  await applier.applyMigration(state, ops, "down");
  assertEquals(state.collections.archived_users.content.length, 0);
  assertEquals(state.collections.users.content.length, 3);
});

Deno.test("memory flow MOVE: consumes source and is irreversible", async () => {
  const state = createEmptyDatabaseState();
  state.collections.users = {
    content: [
      { _id: "user:1", name: "Alice", active: true },
      { _id: "user:2", name: "Bob", active: false },
    ],
  };
  state.collections.archived_users = { content: [] };

  const m = copyMigration("consume");
  const ops = m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;

  // The flow op should be flagged irreversible
  assertEquals(getIrreversibleOperations(ops).length, 1);

  const applier = createMemoryApplier(m);
  await applier.applyMigration(state, ops, "up");
  // inactive moved out of source
  assertEquals(state.collections.users.content.length, 1);
  assertEquals(state.collections.users.content[0]._id, "user:1");
  assertEquals(state.collections.archived_users.content.length, 1);

  // Rollback refused (irreversible) — state untouched
  await assertRejects(
    () => applier.applyMigration(state, ops, "down"),
    Error,
    "irreversible",
  );
  assertEquals(state.collections.users.content.length, 1);
});

Deno.test("memory flow COPY: target ids are deterministic across replays", async () => {
  const seed = () => {
    const s = createEmptyDatabaseState();
    s.collections.users = {
      content: [{ _id: "user:2", name: "Bob", active: false }],
    };
    s.collections.archived_users = { content: [] };
    return s;
  };
  const m = copyMigration("keep");
  const ops = () => m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;

  const s1 = seed();
  const s2 = seed();
  await createMemoryApplier(m).applyMigration(s1, ops(), "up");
  await createMemoryApplier(m).applyMigration(s2, ops(), "up");

  assertEquals(
    s1.collections.archived_users.content[0]._id,
    s2.collections.archived_users.content[0]._id,
  );
});

Deno.test("mongodb flow COPY: copies + reverses cleanly on a real DB", async () => {
  await withDatabase("flow-copy-mongo", async (db) => {
    const m = copyMigration("keep");
    // Set up the collections first via the parent migration
    const parentApplier = createMongodbApplier(db, m.parent!, {
      currentMigrationId: m.parent!.id,
    });
    await parentApplier.applyMigration(
      m.parent!.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
      "up",
    );

    await db.collection("users").insertMany([
      { _id: "user:1", name: "Alice", active: true },
      { _id: "user:2", name: "Bob", active: false },
      { _id: "user:3", name: "Carol", active: false },
    ] as never);

    const applier = createMongodbApplier(db, m, { currentMigrationId: m.id });
    const ops = m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;

    await applier.applyMigration(ops, "up");
    assertEquals(await db.collection("users").countDocuments(), 3);
    assertEquals(await db.collection("archived_users").countDocuments(), 2);

    await applier.applyMigration(ops, "down");
    assertEquals(await db.collection("archived_users").countDocuments(), 0);
    assertEquals(await db.collection("users").countDocuments(), 3);
  });
});

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
import { test } from "../+harness.ts";
import { assert, assertEquals, assertRejects } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import {
  getIrreversibleOperations,
  migrationBuilder,
} from "../../src/migration/builder.ts";
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
        b
          .createCollection("users")
          .end()
          .createCollection("archived_users")
          .end()
          .compile(),
    }),
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .flow({
          from: { collection: "users", where: { active: false } },
          into: { collection: "archived_users" },
          map: (doc) => ({ ...doc, archivedReason: "inactivity" }),
          source,
        })
        .compile(),
  });
}

test("memory flow COPY: copies matching docs, leaves source intact, reversible", async () => {
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
    state.collections.archived_users.content.every(
      (d) => d.active === false && d.archivedReason === "inactivity",
    ),
  );

  // Rollback removes exactly the copies
  await applier.applyMigration(state, ops, "down");
  assertEquals(state.collections.archived_users.content.length, 0);
  assertEquals(state.collections.users.content.length, 3);
});

test("memory flow MOVE: consumes source and is irreversible", async () => {
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

test("memory flow COPY: target ids are deterministic across replays", async () => {
  const seed = () => {
    const s = createEmptyDatabaseState();
    s.collections.users = {
      content: [{ _id: "user:2", name: "Bob", active: false }],
    };
    s.collections.archived_users = { content: [] };
    return s;
  };
  const m = copyMigration("keep");
  const ops = () =>
    m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;

  const s1 = seed();
  const s2 = seed();
  await createMemoryApplier(m).applyMigration(s1, ops(), "up");
  await createMemoryApplier(m).applyMigration(s2, ops(), "up");

  assertEquals(
    s1.collections.archived_users.content[0]._id,
    s2.collections.archived_users.content[0]._id,
  );
});

test("mongodb flow COPY: copies + reverses cleanly on a real DB", async () => {
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

// A flow whose TARGET is a multi-collection.
//
// The builder resolves the target's `_id` schema across plain, multi and scoped
// buckets and documents all three as supported, but the simulation used to look
// the target up in `collections` alone. A migration consolidating a standalone
// collection into a sub-type of a multi-collection was therefore impossible to
// validate, even though the mongodb applier writes it without trouble: it
// addresses the physical collection, which is the same one in every case.
//
// Setting `_type` is the caller's job, through `map`, exactly as `flowToScope`
// leaves it to `toType`.

const MULTI_SCHEMAS = {
  collections: {
    legacy_passwords: {
      _id: dbId("legacy_password"),
      userId: v.string(),
      hash: v.string(),
    },
  },
  multiCollections: {
    "+auth": {
      auth_password: { userId: v.string(), hash: v.string() },
    },
  },
};

function intoMultiCollection(source: "keep" | "consume") {
  return migrationDefinition("002", "passwords-into-auth", {
    parent: migrationDefinition("001", "init", {
      parent: null,
      schemas: MULTI_SCHEMAS,
      migrate: (b) => {
        b.createCollection("legacy_passwords").end();
        b.createMultiCollection("+auth");
        return b.compile();
      },
    }),
    schemas: MULTI_SCHEMAS,
    migrate: (b) => {
      return b
        .flow({
          from: { collection: "legacy_passwords" },
          into: { collection: "+auth" },
          map: (doc) => ({ ...doc, _type: "auth_password" }),
          source,
        })
        .compile();
    },
  });
}

test("memory flow into a multi-collection: lands under the target's own bucket", async () => {
  const state = createEmptyDatabaseState();
  state.multiCollections["+auth"] = { content: [] };
  state.collections.legacy_passwords = {
    content: [
      { _id: "legacy_password:1", userId: "user:1", hash: "h1" },
      { _id: "legacy_password:2", userId: "user:2", hash: "h2" },
    ],
  };

  const m = intoMultiCollection("consume");
  const ops = m.migrate(
    migrationBuilder({ schemas: MULTI_SCHEMAS }),
  ).operations;
  const applier = createMemoryApplier(m);

  await applier.applyMigration(state, ops, "up");

  const landed = state.multiCollections["+auth"].content;
  assertEquals(
    landed.length,
    2,
    "both documents must reach the multi-collection",
  );
  assert(
    landed.every((doc) => doc._type === "auth_password"),
    "the discriminator the map set must survive the flow",
  );
  assertEquals(
    state.collections.legacy_passwords.content.length,
    0,
    "a consume must empty the source",
  );
});

test("memory flow into a multi-collection: a copy stays reversible", async () => {
  const state = createEmptyDatabaseState();
  state.multiCollections["+auth"] = { content: [] };
  state.collections.legacy_passwords = {
    content: [{ _id: "legacy_password:1", userId: "user:1", hash: "h1" }],
  };

  const m = intoMultiCollection("keep");
  const ops = m.migrate(
    migrationBuilder({ schemas: MULTI_SCHEMAS }),
  ).operations;
  const applier = createMemoryApplier(m);

  await applier.applyMigration(state, ops, "up");
  assertEquals(state.multiCollections["+auth"].content.length, 1);

  await applier.applyMigration(state, ops, "down");
  assertEquals(
    state.multiCollections["+auth"].content.length,
    0,
    "rolling back a copy must remove it from the multi-collection too",
  );
  assertEquals(state.collections.legacy_passwords.content.length, 1);
});

test("memory flow FROM a multi-collection: the source resolves the same way", async () => {
  // The mirror of the target bug, and the reason the lookup is one named
  // function rather than a chain repeated at each endpoint: the simulation read
  // the SOURCE from `collections` alone too, so a flow out of a multi-collection
  // was refused here while the mongodb applier read it fine.
  const state = createEmptyDatabaseState();
  state.multiCollections["+auth"] = {
    content: [
      {
        _id: "auth_password:1",
        _type: "auth_password",
        userId: "user:1",
        hash: "h1",
      },
    ],
  };
  state.collections.legacy_passwords = { content: [] };

  const m = migrationDefinition("002", "auth-back-out", {
    parent: migrationDefinition("001", "init", {
      parent: null,
      schemas: MULTI_SCHEMAS,
      migrate: (b) => {
        b.createCollection("legacy_passwords").end();
        b.createMultiCollection("+auth");
        return b.compile();
      },
    }),
    schemas: MULTI_SCHEMAS,
    migrate: (b) =>
      b
        .flow({
          from: { collection: "+auth" },
          into: { collection: "legacy_passwords" },
          map: ({ _type: _dropped, ...rest }) => rest,
          source: "consume",
        })
        .compile(),
  });

  const ops = m.migrate(
    migrationBuilder({ schemas: MULTI_SCHEMAS }),
  ).operations;
  await createMemoryApplier(m).applyMigration(state, ops, "up");

  assertEquals(state.collections.legacy_passwords.content.length, 1);
  assertEquals(state.multiCollections["+auth"].content.length, 0);
});

test("flow into a multi-collection: the minted _id carries the sub-type prefix", async () => {
  // The regression this guards produced ids the WRITE accepted and every later
  // READ rejected: a multi-collection derives `_id` from the sub-type name, so
  // the sub-type entries declare no `_id` of their own, and the lookup that
  // asked them for one silently yielded no prefix at all.
  const state = createEmptyDatabaseState();
  state.collections.legacy_passwords = {
    content: [{ _id: "auth_password:01ABC", userId: "user:1", hash: "h1" }],
  };

  const m = migrationDefinition("002", "auth-consolidation", {
    parent: migrationDefinition("001", "init", {
      parent: null,
      schemas: MULTI_SCHEMAS,
      migrate: (b) => {
        b.createCollection("legacy_passwords").end();
        return b.compile();
      },
    }),
    schemas: MULTI_SCHEMAS,
    migrate: (b) => {
      b.createMultiCollection("+auth");
      return b
        .flow({
          from: { collection: "legacy_passwords" },
          into: { collection: "+auth" },
          map: (doc: Record<string, unknown>) => ({
            ...doc,
            _type: "auth_password",
          }),
          source: "consume",
        })
        .compile();
    },
  });

  const ops = m.migrate(
    migrationBuilder({ schemas: MULTI_SCHEMAS }),
  ).operations;
  await createMemoryApplier(m).applyMigration(state, ops, "up");

  const [moved] = state.multiCollections["+auth"].content;
  assertEquals(moved._type, "auth_password");
  assertEquals(
    String(moved._id).startsWith("auth_password:"),
    true,
    `minted id must be namespaced by its sub-type, got ${moved._id}`,
  );
});

test("flow into a multi-collection: a map that forgets _type is refused", async () => {
  // Loudly, and before anything is written. Minting a bare id instead would put
  // documents in place that only fail when something reads them back.
  const state = createEmptyDatabaseState();
  state.collections.legacy_passwords = {
    content: [{ _id: "auth_password:01ABC", userId: "user:1", hash: "h1" }],
  };

  const m = migrationDefinition("002", "auth-untyped", {
    parent: migrationDefinition("001", "init", {
      parent: null,
      schemas: MULTI_SCHEMAS,
      migrate: (b) => {
        b.createCollection("legacy_passwords").end();
        return b.compile();
      },
    }),
    schemas: MULTI_SCHEMAS,
    migrate: (b) => {
      b.createMultiCollection("+auth");
      return b
        .flow({
          from: { collection: "legacy_passwords" },
          into: { collection: "+auth" },
          map: (doc: Record<string, unknown>) => ({ ...doc }),
          source: "keep",
        })
        .compile();
    },
  });

  const ops = m.migrate(
    migrationBuilder({ schemas: MULTI_SCHEMAS }),
  ).operations;
  await assertRejects(
    () => createMemoryApplier(m).applyMigration(state, ops, "up"),
    Error,
    "_type",
  );
});

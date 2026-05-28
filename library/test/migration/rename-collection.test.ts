/**
 * `renameCollection` — the primitive behind a temp→final swap, e.g. landing a
 * new scoped multi-collection whose name collides with a legacy source: build
 * into a temp name, consume the sources, then rename the temp over the (now
 * free) final name. Reversible unless `dropTarget` drops an existing target.
 */
import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import {
  getIrreversibleOperations,
  getLossyOperations,
  migrationBuilder,
} from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import * as v from "../../src/schema.ts";

const S = { collections: { a: { _id: v.string() }, b: { _id: v.string() } } };

Deno.test("renameCollection: plain rename is reversible (memory up + down)", async () => {
  const state = createEmptyDatabaseState();
  state.collections["a"] = { content: [{ _id: "x:1" }] };

  const m = migrationDefinition("001", "rn", {
    parent: null,
    schemas: S,
    migrate: (b) => b.renameCollection("a", "b").compile(),
  });
  const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
  assertEquals(getIrreversibleOperations(ops).length, 0, "plain rename is reversible");

  const applier = createMemoryApplier(m);
  await applier.applyMigration(state, ops, "up");
  assertEquals(state.collections["a"], undefined);
  assertEquals(state.collections["b"]?.content.length, 1);

  await applier.applyMigration(state, ops, "down");
  assertEquals(state.collections["b"], undefined);
  assertEquals(state.collections["a"]?.content.length, 1);
});

Deno.test("renameCollection: dropTarget flags the op lossy", () => {
  const m = migrationDefinition("001", "rn", {
    parent: null,
    schemas: S,
    migrate: (b) => b.renameCollection("a", "b", { dropTarget: true }).compile(),
  });
  const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
  assertEquals(getLossyOperations(ops).length, 1);
});

Deno.test("renameCollection: temp scoped collection → final name (memory)", async () => {
  // The consolidation use case: a scoped collection built under a temp name is
  // renamed over the final name, carrying its scoped content.
  const state = createEmptyDatabaseState();
  state.scopedMultiCollections["+expositions__v2"] = {
    content: [
      { _id: "exposition:a", _type: "information", _scope: "exposition:a" },
      { _id: "participant:p1", _type: "participant", _scope: "exposition:a" },
    ],
  };

  const m = migrationDefinition("001", "swap", {
    parent: null,
    schemas: S,
    migrate: (b) => b.renameCollection("+expositions__v2", "+expositions").compile(),
  });
  const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
  await createMemoryApplier(m).applyMigration(state, ops, "up");

  assertEquals(state.scopedMultiCollections["+expositions__v2"], undefined);
  assertEquals(state.scopedMultiCollections["+expositions"]?.content.length, 2);
});

Deno.test("mongodb renameCollection: renames a real collection then reverses", async () => {
  await withDatabase("rename-collection-mongo", async (db) => {
    await db.collection("src_coll").insertMany(
      [{ _id: "x:1" }, { _id: "x:2" }] as never,
    );

    const SE = { collections: {} };
    const m = migrationDefinition("001", "rn", {
      parent: null,
      schemas: SE,
      migrate: (b) => b.renameCollection("src_coll", "dst_coll").compile(),
    });
    const ops = m.migrate(migrationBuilder({ schemas: SE })).operations;
    const applier = createMongodbApplier(db, m, { currentMigrationId: m.id });

    await applier.applyMigration(ops, "up");
    assertEquals(await db.collection("dst_coll").countDocuments(), 2);
    assertEquals(
      (await db.listCollections({ name: "src_coll" }).toArray()).length,
      0,
      "source collection is gone after rename",
    );

    await applier.applyMigration(ops, "down");
    assertEquals(await db.collection("src_coll").countDocuments(), 2);
    assert(
      (await db.listCollections({ name: "dst_coll" }).toArray()).length === 0,
      "rename reversed",
    );
  });
});

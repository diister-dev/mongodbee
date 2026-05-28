/**
 * End-to-end migration lifecycle for scoped multi-collections:
 *   migration #1 — create a scoped collection + seed two scopes
 *   migration #2 — add a field to a type via transform (all scopes / filtered)
 * Verified on both the in-memory simulator and a real MongoDB.
 */
import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

const SCHEMAS_V1 = {
  collections: {},
  scopedMultiCollections: {
    catalog: {
      scope: refId("exposition"),
      types: {
        artwork: { title: v.string(), year: v.number() },
      },
    },
  },
};

const SCHEMAS_V2 = {
  collections: {},
  scopedMultiCollections: {
    catalog: {
      scope: refId("exposition"),
      types: {
        artwork: { title: v.string(), year: v.number(), featured: v.boolean() },
      },
    },
  },
};

function migrationV1() {
  return migrationDefinition("001", "create-catalog", {
    parent: null,
    schemas: SCHEMAS_V1,
    migrate: (b) =>
      b.createScopedMultiCollection("catalog")
        .type("artwork")
          .seed(EXPO_A, [{ title: "Mona Lisa", year: 1503 }])
          .seed(EXPO_B, [
            { title: "Guernica", year: 1937 },
            { title: "The Scream", year: 1893 },
          ])
        .end()
        .end()
        .compile(),
  });
}

function migrationV2(parent: ReturnType<typeof migrationDefinition>) {
  return migrationDefinition("002", "add-featured", {
    parent,
    schemas: SCHEMAS_V2,
    migrate: (b) =>
      b.scopedMultiCollection("catalog")
        .type("artwork")
          .transform({
            up: (doc) => ({ ...doc, featured: false }),
            down: (doc) => {
              const { featured: _f, ...rest } = doc as Record<string, unknown>;
              return rest;
            },
          })
        .end()
        .end()
        .compile(),
  });
}

Deno.test("memory: create scoped + seed two scopes, then rollback", async () => {
  const state = createEmptyDatabaseState();
  const m = migrationV1();
  const ops = m.migrate(migrationBuilder({ schemas: SCHEMAS_V1 })).operations;
  const applier = createMemoryApplier(m);

  await applier.applyMigration(state, ops, "up");
  const docs = state.scopedMultiCollections.catalog.content;
  assertEquals(docs.length, 3);
  assertEquals(docs.filter((d) => d._scope === EXPO_A).length, 1);
  assertEquals(docs.filter((d) => d._scope === EXPO_B).length, 2);
  assert(docs.every((d) => d._type === "artwork" && String(d._id).startsWith("artwork:")));

  await applier.applyMigration(state, ops, "down");
  // create reversed → collection removed
  assertEquals(state.scopedMultiCollections.catalog, undefined);
});

Deno.test("memory: transform adds field across all scopes, reversible", async () => {
  const state = createEmptyDatabaseState();
  const m1 = migrationV1();
  const m2 = migrationV2(m1);

  await createMemoryApplier(m1).applyMigration(
    state,
    m1.migrate(migrationBuilder({ schemas: SCHEMAS_V1 })).operations,
    "up",
  );

  const ops2 = m2.migrate(
    migrationBuilder({ schemas: SCHEMAS_V2, parentSchemas: SCHEMAS_V1 }),
  ).operations;
  const applier2 = createMemoryApplier(m2);

  await applier2.applyMigration(state, ops2, "up");
  const docs = state.scopedMultiCollections.catalog.content;
  assert(docs.every((d) => d.featured === false), "all docs got featured");
  // meta preserved
  assert(docs.every((d) => d._type === "artwork" && d._scope));

  await applier2.applyMigration(state, ops2, "down");
  assert(
    state.scopedMultiCollections.catalog.content.every((d) => !("featured" in d)),
    "featured removed on rollback",
  );
});

Deno.test("memory: transform with scopeFilter only touches listed scopes", async () => {
  const state = createEmptyDatabaseState();
  const m1 = migrationV1();
  await createMemoryApplier(m1).applyMigration(
    state,
    m1.migrate(migrationBuilder({ schemas: SCHEMAS_V1 })).operations,
    "up",
  );

  const m2 = migrationDefinition("002", "feature-expo-a-only", {
    parent: m1,
    schemas: SCHEMAS_V2,
    migrate: (b) =>
      b.scopedMultiCollection("catalog")
        .type("artwork")
          .transform({
            up: (doc) => ({ ...doc, featured: true }),
            down: (doc) => {
              const { featured: _f, ...rest } = doc as Record<string, unknown>;
              return rest;
            },
            scopeFilter: [EXPO_A],
          })
        .end()
        .end()
        .compile(),
  });

  const ops2 = m2.migrate(
    migrationBuilder({ schemas: SCHEMAS_V2, parentSchemas: SCHEMAS_V1 }),
  ).operations;
  await createMemoryApplier(m2).applyMigration(state, ops2, "up");

  const docs = state.scopedMultiCollections.catalog.content;
  assertEquals(docs.filter((d) => d._scope === EXPO_A).every((d) => d.featured === true), true);
  assertEquals(docs.filter((d) => d._scope === EXPO_B).every((d) => !("featured" in d)), true);
});

Deno.test("mongodb: full scoped lifecycle create+seed+transform, then rollback", async () => {
  await withDatabase("scoped-migration-lifecycle", async (db) => {
    const m1 = migrationV1();
    const m2 = migrationV2(m1);

    // Apply migration #1 (create + seed)
    await createMongodbApplier(db, m1, { currentMigrationId: m1.id })
      .applyMigration(
        m1.migrate(migrationBuilder({ schemas: SCHEMAS_V1 })).operations,
        "up",
      );

    assertEquals(await db.collection("catalog").countDocuments({ _type: "artwork" } as never), 3);
    assertEquals(await db.collection("catalog").countDocuments({ _scope: EXPO_A } as never), 1);

    // Apply migration #2 (transform: add featured)
    const ops2 = m2.migrate(
      migrationBuilder({ schemas: SCHEMAS_V2, parentSchemas: SCHEMAS_V1 }),
    ).operations;
    await createMongodbApplier(db, m2, { currentMigrationId: m2.id })
      .applyMigration(ops2, "up");

    const afterTransform = await db.collection("catalog").find({} as never).toArray();
    assert(afterTransform.every((d) => (d as { featured?: boolean }).featured === false));

    // Rollback migration #2 → featured removed
    await createMongodbApplier(db, m2, { currentMigrationId: m2.id })
      .applyMigration(ops2, "down");
    const afterRollback = await db.collection("catalog").find({} as never).toArray();
    assert(afterRollback.every((d) => !("featured" in (d as object))));
    assertEquals(afterRollback.length, 3);
  });
});

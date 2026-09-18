/**
 * A scoped transform declaring `reads` receives the sibling documents of the
 * document's own scope, so a data repair can consult a per-scope vocabulary
 * (a room name → a room id) without a query per document and without leaking
 * one scope's vocabulary into another. Verified on both appliers.
 */
import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import type { TransformScope } from "../../src/migration/types.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

const SCHEMAS = {
  collections: {},
  scopedMultiCollections: {
    expo: {
      scope: refId("exposition"),
      types: {
        information: {
          locations: v.array(v.object({ id: v.string(), name: v.string() })),
        },
        program: { locationId: v.string() },
      },
    },
  },
};

type Location = { id: string; name: string };

function locationIdFrom(doc: Record<string, unknown>, scope?: TransformScope) {
  const information = scope?.siblings.information?.[0];
  const locations = (information?.locations ?? []) as Location[];
  const current = String(doc.locationId);
  if (locations.some((l) => l.id === current)) return doc;
  const byName = locations.find((l) => l.name === current);
  return byName ? { ...doc, locationId: byName.id } : doc;
}

function migrationV1() {
  return migrationDefinition("001", "create-expo", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .createScopedMultiCollection("expo")
        .type("information")
        .seed(EXPO_A, [{ locations: [{ id: "loc_1", name: "Salle A" }] }])
        .seed(EXPO_B, [{ locations: [{ id: "loc_9", name: "Salle A" }] }])
        .end()
        .type("program")
        .seed(EXPO_A, [{ locationId: "Salle A" }, { locationId: "loc_1" }])
        .seed(EXPO_B, [{ locationId: "Salle A" }])
        .end()
        .end()
        .compile(),
  });
}

function migrationV2(parent: ReturnType<typeof migrationDefinition>) {
  return migrationDefinition("002", "program-location-ids", {
    parent,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .scopedMultiCollection("expo")
        .type("program")
        .transform({
          reads: ["information"],
          up: locationIdFrom,
          down: (doc) => doc,
        })
        .end()
        .end()
        .compile(),
  });
}

function migrationWithoutReads(parent: ReturnType<typeof migrationDefinition>) {
  return migrationDefinition("002", "no-reads", {
    parent,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .scopedMultiCollection("expo")
        .type("program")
        .transform({
          up: (doc, scope) => ({
            ...doc,
            siblingTypes: Object.keys(scope?.siblings ?? {}),
            scope: scope?.scope,
          }),
          down: (doc) => doc,
        })
        .end()
        .end()
        .compile(),
  });
}

function locationIdsByScope(docs: readonly Record<string, unknown>[]) {
  return docs
    .filter((d) => d._type === "program")
    .map((d) => [d._scope, d.locationId])
    .sort();
}

const EXPECTED = [
  [EXPO_A, "loc_1"],
  [EXPO_A, "loc_1"],
  [EXPO_B, "loc_9"],
];

test("memory: a scoped transform reads the siblings of its own scope", async () => {
  const state = createEmptyDatabaseState();
  const m1 = migrationV1();
  const m2 = migrationV2(m1);
  await createMemoryApplier(m1).applyMigration(
    state,
    m1.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
    "up",
  );
  const ops2 = m2.migrate(
    migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }),
  ).operations;
  await createMemoryApplier(m2).applyMigration(state, ops2, "up");

  assertEquals(
    locationIdsByScope(state.scopedMultiCollections.expo.content),
    EXPECTED,
  );
});

test("memory: a transform without reads gets its scope and no siblings", async () => {
  const state = createEmptyDatabaseState();
  const m1 = migrationV1();
  const m2 = migrationWithoutReads(m1);
  await createMemoryApplier(m1).applyMigration(
    state,
    m1.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
    "up",
  );
  await createMemoryApplier(m2).applyMigration(
    state,
    m2.migrate(migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }))
      .operations,
    "up",
  );
  const programs = state.scopedMultiCollections.expo.content.filter(
    (d) => d._type === "program",
  );
  assertEquals(programs.length, 3);
  for (const doc of programs) {
    assertEquals(doc.siblingTypes, []);
    assertEquals(doc.scope, doc._scope);
  }
});

test("mongodb: a scoped transform reads the siblings of its own scope", async () => {
  await withDatabase("scoped-transform-reads", async (db) => {
    const m1 = migrationV1();
    const m2 = migrationV2(m1);
    await createMongodbApplier(db, m1, {
      currentMigrationId: m1.id,
    }).applyMigration(
      m1.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
      "up",
    );
    const ops2 = m2.migrate(
      migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }),
    ).operations;
    await createMongodbApplier(db, m2, {
      currentMigrationId: m2.id,
    }).applyMigration(ops2, "up");

    const docs = (await db
      .collection("expo")
      .find({} as never)
      .toArray()) as Record<string, unknown>[];
    assertEquals(locationIdsByScope(docs), EXPECTED);
  });
});

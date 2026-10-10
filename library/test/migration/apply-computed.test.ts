import { test } from "../+harness.ts";
import { assert, assertEquals, assertThrows } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import { defineType } from "../../src/type-definition.ts";
import { from } from "../../src/computed.ts";
import * as v from "../../src/schema.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

const membership = {
  participantId: withIndex(v.string()),
  organizationId: v.string(),
  status: v.picklist(["active", "withdrawn"]),
};

const participant = { name: v.string() };

const BEFORE = {
  collections: {},
  scopedMultiCollections: {
    expo: {
      scope: refId("exposition"),
      types: { participant, membership },
    },
  },
};

const AFTER = {
  collections: {},
  scopedMultiCollections: {
    expo: {
      scope: refId("exposition"),
      types: {
        participant: defineType({
          schema: v.object(participant),
          computed: {
            organizationIds: from("membership", membership)
              .by((row) => row.participantId)
              .where((row) => [row.status, "active"])
              .collect((row) => row.organizationId),
          },
        }),
        membership,
      },
    },
  },
};

const BULK_PARTICIPANTS = 600;

function bulkRows() {
  const participants = Array.from({ length: BULK_PARTICIPANTS }, (_, i) => ({
    _id: `participant:bulk${String(i).padStart(4, "0")}`,
    name: `Bulk ${i}`,
  }));
  const memberships = participants.flatMap((row, i) => [
    {
      _id: `membership:bulk${String(i).padStart(4, "0")}a`,
      participantId: row._id,
      organizationId: `organization:first${i}`,
      status: "active" as const,
    },
    {
      _id: `membership:bulk${String(i).padStart(4, "0")}b`,
      participantId: row._id,
      organizationId: `organization:second${i}`,
      status: "active" as const,
    },
  ]);
  return { participants, memberships };
}

function seedMigration(bulk: boolean) {
  const rows = bulkRows();
  return migrationDefinition("001", "seed", {
    parent: null,
    schemas: BEFORE,
    migrate: (b) =>
      b
        .createScopedMultiCollection("expo")
        .type("participant")
        .seed(EXPO_A, [
          { _id: "participant:ada", name: "Ada" },
          { _id: "participant:bob", name: "Bob" },
          ...(bulk ? rows.participants : []),
        ])
        .seed(EXPO_B, [{ _id: "participant:cyd", name: "Cyd" }])
        .end()
        .type("membership")
        .seed(EXPO_A, [
          {
            _id: "membership:m2",
            participantId: "participant:ada",
            organizationId: "organization:two",
            status: "active",
          },
          {
            _id: "membership:m1",
            participantId: "participant:ada",
            organizationId: "organization:one",
            status: "active",
          },
          {
            _id: "membership:m3",
            participantId: "participant:ada",
            organizationId: "organization:gone",
            status: "withdrawn",
          },
          {
            _id: "membership:m4",
            participantId: "participant:cyd",
            organizationId: "organization:elsewhere",
            status: "active",
          },
          ...(bulk ? rows.memberships : []),
        ])
        .seed(EXPO_B, [
          {
            _id: "membership:m9",
            participantId: "participant:cyd",
            organizationId: "organization:nine",
            status: "active",
          },
        ])
        .end()
        .end()
        .compile(),
  });
}

function computeMigration(parent: ReturnType<typeof migrationDefinition>) {
  return migrationDefinition("002", "organization-ids", {
    parent,
    schemas: AFTER,
    migrate: (b) =>
      b
        .scopedMultiCollection("expo")
        .type("participant")
        .applyComputed("organizationIds")
        .end()
        .end()
        .compile(),
  });
}

function organizationIds(docs: readonly Record<string, unknown>[]) {
  return Object.fromEntries(
    docs
      .filter(
        (doc) =>
          doc._type === "participant" &&
          !String(doc._id).startsWith("participant:bulk"),
      )
      .map((doc) => [
        String(doc._id),
        (doc._computed as { organizationIds?: unknown } | undefined)
          ?.organizationIds,
      ]),
  );
}

const EXPECTED = {
  "participant:ada": ["organization:one", "organization:two"],
  "participant:bob": [],
  "participant:cyd": ["organization:nine"],
};

function operationsOf(migration: ReturnType<typeof migrationDefinition>) {
  return migration.migrate(
    migrationBuilder({
      schemas: migration.schemas,
      parentSchemas: migration.parent?.schemas,
    }),
  ).operations;
}

test("applyComputed: the builder refuses a field the migration does not declare", () => {
  assertThrows(() =>
    migrationBuilder({ schemas: AFTER })
      .scopedMultiCollection("expo")
      .type("participant")
      .applyComputed("missing"),
  );
});

test("applyComputed (memory): fills the field from its sources, scope by scope, then removes it on the way down", async () => {
  const state = createEmptyDatabaseState();
  const seed = seedMigration(false);
  const compute = computeMigration(seed);
  await createMemoryApplier(seed).applyMigration(
    state,
    operationsOf(seed),
    "up",
  );
  await createMemoryApplier(compute).applyMigration(
    state,
    operationsOf(compute),
    "up",
  );

  assertEquals(
    organizationIds(state.scopedMultiCollections.expo.content),
    EXPECTED,
  );

  await createMemoryApplier(compute).applyMigration(
    state,
    operationsOf(compute),
    "down",
  );
  const participants = state.scopedMultiCollections.expo.content.filter(
    (doc) => doc._type === "participant",
  );
  assertEquals(
    participants.filter((doc) => "_computed" in doc),
    [],
  );
});

test("applyComputed (mongodb): matches the simulation, has no sibling cap, and rolls back", async () => {
  await withDatabase("apply-computed-migration", async (db) => {
    const seed = seedMigration(true);
    const compute = computeMigration(seed);
    await createMongodbApplier(db, seed, {
      currentMigrationId: seed.id,
    }).applyMigration(operationsOf(seed), "up");
    await createMongodbApplier(db, compute, {
      currentMigrationId: compute.id,
    }).applyMigration(operationsOf(compute), "up");

    const docs = (await db
      .collection("expo")
      .find({} as never)
      .toArray()) as Record<string, unknown>[];
    assertEquals(organizationIds(docs), EXPECTED);
    const bulk = docs.filter(
      (doc) => String(doc._id) === "participant:bulk0599",
    );
    assertEquals(
      (bulk[0]?._computed as { organizationIds?: unknown } | undefined)
        ?.organizationIds,
      ["organization:first599", "organization:second599"],
    );

    await createMongodbApplier(db, compute, {
      currentMigrationId: compute.id,
    }).applyMigration(operationsOf(compute), "down");
    const reverted = await db
      .collection("expo")
      .find({ _type: "participant", _computed: { $exists: true } } as never)
      .toArray();
    assertEquals(reverted.length, 0);
  });
});

test("applyComputed: the simulation bumps each subject's revision like the mongodb applier", async () => {
  const revisions = (docs: readonly Record<string, unknown>[]) =>
    Object.fromEntries(
      docs
        .filter((doc) => doc._type === "participant")
        .map((doc) => [
          String(doc._id),
          (doc._computed as { _rev?: number } | undefined)?._rev,
        ])
        .sort(([a], [b]) => String(a).localeCompare(String(b))),
    );
  const seed = seedMigration(false);
  const compute = computeMigration(seed);

  const state = createEmptyDatabaseState();
  await createMemoryApplier(seed).applyMigration(
    state,
    operationsOf(seed),
    "up",
  );
  await createMemoryApplier(compute).applyMigration(
    state,
    operationsOf(compute),
    "up",
  );
  const simulated = revisions(state.scopedMultiCollections.expo.content);

  await withDatabase("apply-computed-revision", async (db) => {
    await createMongodbApplier(db, seed, {
      currentMigrationId: seed.id,
    }).applyMigration(operationsOf(seed), "up");
    await createMongodbApplier(db, compute, {
      currentMigrationId: compute.id,
    }).applyMigration(operationsOf(compute), "up");
    const docs = (await db
      .collection("expo")
      .find({} as never)
      .toArray()) as Record<string, unknown>[];
    assertEquals(simulated, revisions(docs));
  });
  assert(Object.values(simulated).every((revision) => revision === 1));
});

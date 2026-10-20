/**
 * `moveType` on a scoped multi-collection: every document of a type leaves
 * one scoped collection for another as it is (`_id`, `_scope`, `_type` and
 * fields kept), in batches, and `down` puts them back. A run cut short between
 * a batch's copy and its delete resumes without duplicating or losing a
 * document, and the memory applier (`check`) ends where MongoDB ends.
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
import {
  createEmptyDatabaseState,
  type DatabaseState,
} from "../../src/migration/types.ts";
import type { Db } from "../../src/mongodb.ts";
import { dbId, refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

type Doc = Record<string, unknown>;

const KEEP = { _id: dbId("keep"), label: v.string() };
const READING = { _id: dbId("reading"), user: v.string(), seq: v.number() };

const BEFORE = {
  scopedMultiCollections: {
    "+old": { scope: refId("ws"), types: { keep: KEEP, reading: READING } },
  },
};
const AFTER = {
  scopedMultiCollections: {
    "+old": { scope: refId("ws"), types: { keep: KEEP } },
    "+new": { scope: refId("ws"), types: { reading: READING } },
  },
};

const SCOPES = ["ws:a", "ws:b", "ws:c"];
// Each case applies two migrations on a real server, validators and indexes
// included: several seconds on a cold runner, past the 5 s default.
const MONGO_TIMEOUT_MS = 60_000;

function readingsOf(count: number, scope: string) {
  return Array.from({ length: count }, (_, i) => ({
    user: `${scope}-u${i}`,
    seq: i,
  }));
}

function chain(readingsPerScope: number) {
  const parent = migrationDefinition("001", "seed", {
    parent: null,
    schemas: BEFORE,
    migrate: (b) => {
      b.createScopedMultiCollection("+old");
      const old = b.scopedMultiCollection("+old");
      for (const scope of SCOPES) {
        old.type("keep").seed(scope, [{ label: `k-${scope}` }]);
        if (readingsPerScope > 0) {
          old.type("reading").seed(scope, readingsOf(readingsPerScope, scope));
        }
      }
      return b.compile();
    },
  });
  const child = migrationDefinition("002", "move-readings", {
    parent,
    schemas: AFTER,
    migrate: (b) => {
      b.createScopedMultiCollection("+new");
      b.scopedMultiCollection("+old")
        .moveType("reading", { into: "+new" })
        .end();
      return b.compile();
    },
  });
  const parentOps = parent.migrate(
    migrationBuilder({ schemas: parent.schemas }),
  ).operations;
  const childOps = child.migrate(
    migrationBuilder({ schemas: child.schemas, parentSchemas: parent.schemas }),
  ).operations;
  return { parent, child, parentOps, childOps };
}

const byId = (docs: readonly Doc[]) =>
  [...docs].sort((a, b) => String(a._id).localeCompare(String(b._id)));

async function memoryRun(readingsPerScope: number) {
  const { parent, child, parentOps, childOps } = chain(readingsPerScope);
  let state = await createMemoryApplier(parent).applyMigration(
    createEmptyDatabaseState(),
    parentOps,
    "up",
  );
  const before = byId(state.scopedMultiCollections["+old"].content);
  const applier = createMemoryApplier(child);
  state = await applier.applyMigration(state, childOps, "up");
  const afterUp = snapshot(state);
  state = await applier.applyMigration(state, childOps, "down");
  return { before, afterUp, afterDown: snapshot(state) };
}

function snapshot(state: DatabaseState) {
  return {
    old: byId(state.scopedMultiCollections["+old"]?.content ?? []),
    new: byId(state.scopedMultiCollections["+new"]?.content ?? []),
  };
}

async function mongoSnapshot(db: Db) {
  const read = async (name: string) =>
    byId((await db.collection(name).find().toArray()) as Doc[]);
  return { old: await read("+old"), new: await read("+new") };
}

async function mongoRun(db: Db, readingsPerScope: number, batchSize: number) {
  const { parent, child, parentOps, childOps } = chain(readingsPerScope);
  await createMongodbApplier(db, parent, {
    currentMigrationId: parent.id,
  }).applyMigration(parentOps, "up");
  const before = (await mongoSnapshot(db)).old;
  const applier = createMongodbApplier(db, child, {
    currentMigrationId: child.id,
    batchSize,
  });
  await applier.applyMigration(childOps, "up");
  const afterUp = await mongoSnapshot(db);
  await applier.applyMigration(childOps, "down");
  return { before, afterUp, afterDown: await mongoSnapshot(db) };
}

function assertMoved(run: {
  before: Doc[];
  afterUp: { old: Doc[]; new: Doc[] };
  afterDown: { old: Doc[]; new: Doc[] };
}) {
  const readings = run.before.filter((doc) => doc._type === "reading");
  const kept = run.before.filter((doc) => doc._type === "keep");
  assertEquals(run.afterUp.new, readings);
  assertEquals(run.afterUp.old, kept);
  assertEquals(run.afterDown.old, run.before);
  assertEquals(run.afterDown.new, []);
}

test("moveType: the builder records a reversible move and refuses a target that does not declare the type", () => {
  const { childOps } = chain(1);
  assertEquals(
    childOps.find((op) => op.type === "move_scoped_multicollection_type"),
    {
      type: "move_scoped_multicollection_type",
      collectionName: "+old",
      documentType: "reading",
      into: { collection: "+new" },
    },
  );
  assertEquals(getIrreversibleOperations(childOps), []);
  const builder = migrationBuilder({ schemas: AFTER, parentSchemas: BEFORE });
  let refused = "";
  try {
    builder.scopedMultiCollection("+old").moveType("keep", { into: "+new" });
  } catch (error) {
    refused = (error as Error).message;
  }
  assert(refused.includes("not declared"));
  let self = "";
  try {
    builder.scopedMultiCollection("+old").moveType("reading", { into: "+old" });
  } catch (error) {
    self = (error as Error).message;
  }
  assert(self.includes("into the collection it is in"));
});

test("moveType (memory): every reading moves with its _id, _scope and fields, and down puts them back", async () => {
  const run = await memoryRun(4);
  assertEquals(run.before.filter((doc) => doc._type === "reading").length, 12);
  assertMoved(run);
});

test({
  name: "moveType (mongodb): batches smaller than the type, equal to it and larger move every document once, and down puts them back",
  timeout: MONGO_TIMEOUT_MS,
  fn: async () => {
    for (const batchSize of [2, 12, 1000]) {
      await withDatabase(`move-type-batch-${batchSize}`, async (db) => {
        const run = await mongoRun(db, 4, batchSize);
        assertEquals(
          run.before.filter((doc) => doc._type === "reading").length,
          12,
        );
        assertMoved(run);
      });
    }
  },
});

test({
  name: "moveType: check (memory) and MongoDB end in the same state, up and down",
  timeout: MONGO_TIMEOUT_MS,
  fn: async () => {
    const memory = await memoryRun(5);
    await withDatabase("move-type-parity", async (db) => {
      const mongo = await mongoRun(db, 5, 3);
      assertEquals(mongo.before, memory.before);
      assertEquals(mongo.afterUp, memory.afterUp);
      assertEquals(mongo.afterDown, memory.afterDown);
    });
  },
});

test({
  name: "moveType: an empty source moves nothing, both ways",
  timeout: MONGO_TIMEOUT_MS,
  fn: async () => {
    const memory = await memoryRun(0);
    assertMoved(memory);
    await withDatabase("move-type-empty", async (db) => {
      const mongo = await mongoRun(db, 0, 2);
      assertMoved(mongo);
      assertEquals(mongo, memory);
    });
  },
});

test({
  name: "moveType (mongodb): a run cut between a batch's copy and its delete resumes without duplicating or losing a document",
  timeout: MONGO_TIMEOUT_MS,
  fn: async () => {
    await withDatabase("move-type-resume", async (db) => {
      const { parent, child, parentOps, childOps } = chain(4);
      await createMongodbApplier(db, parent, {
        currentMigrationId: parent.id,
      }).applyMigration(parentOps, "up");
      const old = db.collection("+old");
      const readings = byId(
        (await old.find({ _type: "reading" }).toArray()) as Doc[],
      );
      // The state a crash leaves: three readings copied, only the first deleted.
      await db.createCollection("+new");
      await db.collection("+new").insertMany(readings.slice(0, 3) as never[]);
      await old.deleteOne({ _id: readings[0]._id } as never);

      await createMongodbApplier(db, child, {
        currentMigrationId: child.id,
        batchSize: 2,
      }).applyMigration(childOps, "up");

      const after = await mongoSnapshot(db);
      assertEquals(after.new, readings);
      assertEquals(
        after.old.every((doc) => doc._type === "keep"),
        true,
      );
    });
  },
});

test("moveType (memory): a cut run resumes the same way", async () => {
  const { parent, child, parentOps, childOps } = chain(4);
  const state = await createMemoryApplier(parent).applyMigration(
    createEmptyDatabaseState(),
    parentOps,
    "up",
  );
  const readings = byId(
    state.scopedMultiCollections["+old"].content.filter(
      (doc) => doc._type === "reading",
    ),
  );
  state.scopedMultiCollections["+new"] = {
    content: readings.slice(0, 3).map((doc) => ({ ...doc })),
  };
  state.scopedMultiCollections["+old"].content = state.scopedMultiCollections[
    "+old"
  ].content.filter((doc) => doc._id !== readings[0]._id);

  const after = snapshot(
    await createMemoryApplier(child).applyMigration(state, childOps, "up"),
  );
  assertEquals(after.new, readings);
  assert(after.old.every((doc) => doc._type === "keep"));
});

test({
  name: "moveType: a document of another type holding the same _id in the target stops the move, in both appliers",
  timeout: MONGO_TIMEOUT_MS,
  fn: async () => {
    const { parent, child, parentOps, childOps } = chain(2);
    const state = await createMemoryApplier(parent).applyMigration(
      createEmptyDatabaseState(),
      parentOps,
      "up",
    );
    const reading = state.scopedMultiCollections["+old"].content.find(
      (doc) => doc._type === "reading",
    );
    state.scopedMultiCollections["+new"] = {
      content: [{ ...reading, _type: "other" }],
    };
    await assertRejects(
      () => createMemoryApplier(child).applyMigration(state, childOps, "up"),
      Error,
      "is held there by a document of type other",
    );

    await withDatabase("move-type-clash", async (db) => {
      await createMongodbApplier(db, parent, {
        currentMigrationId: parent.id,
      }).applyMigration(parentOps, "up");
      const first = await db.collection("+old").findOne({ _type: "reading" });
      await db.createCollection("+new");
      await db
        .collection("+new")
        .insertOne({ ...first, _type: "other" } as never);
      await assertRejects(() =>
        createMongodbApplier(db, child, {
          currentMigrationId: child.id,
        }).applyMigration(childOps, "up"),
      );
      assertEquals(
        await db.collection("+old").countDocuments({ _type: "reading" }),
        6,
      );
    });
  },
});

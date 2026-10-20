/**
 * `moveType` on a scoped multi-collection: every document of a type leaves
 * one scoped collection for another as it is (`_id`, `_scope`, `_type` and
 * fields kept), in batches, and `down` puts them back. A run cut short between
 * a batch's copy and its delete resumes without duplicating or losing a
 * document, a write made in either collection during the move is kept, and the
 * memory applier (`check`) ends where MongoDB ends.
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
import { withIndex } from "../../src/indexes.ts";
import * as v from "../../src/schema.ts";

type Doc = Record<string, unknown>;

const KEEP = { _id: dbId("keep"), label: v.string() };
// `user` is unique per scope: the move carries a type with an index of its own.
const READING = {
  _id: dbId("reading"),
  user: withIndex(v.string(), { unique: true }),
  seq: v.number(),
};

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

// The applier's view of the database, where every write that copies a reading
// into "+new" is followed, before the move goes on, by `after(id)`: a writer of
// the application acting between a document's copy and its delete.
function probed(db: Db, after: (id: unknown) => Promise<void>): Db {
  const wrap = (collection: object): object =>
    new Proxy(collection, {
      get(target, key) {
        const value = Reflect.get(target, key, target);
        if (typeof value !== "function") return value;
        if (!["insertOne", "updateOne", "replaceOne"].includes(String(key)))
          return value.bind(target);
        return async (...args: unknown[]) => {
          const done = await value.apply(target, args);
          await after((args[0] as Doc)._id);
          return done;
        };
      },
    });
  return new Proxy(db, {
    get(target, key) {
      const value = Reflect.get(target, key, target);
      if (key !== "collection")
        return typeof value === "function" ? value.bind(target) : value;
      return (name: string, options?: unknown) => {
        const opened = (
          target.collection as (n: string, o?: unknown) => object
        )(name, options);
        return name === "+new" ? wrap(opened) : opened;
      };
    },
  }) as Db;
}

async function applyParent(db: Db, readingsPerScope: number) {
  const pair = chain(readingsPerScope);
  await createMongodbApplier(db, pair.parent, {
    currentMigrationId: pair.parent.id,
  }).applyMigration(pair.parentOps, "up");
  return pair;
}

test({
  name: "moveType (mongodb): writes made during the move, in the source after a copy or in the target after it, are all kept",
  timeout: MONGO_TIMEOUT_MS,
  fn: async () => {
    await withDatabase("move-type-concurrent", async (db) => {
      const { child, childOps } = await applyParent(db, 4);
      const old = db.collection("+old");
      const fresh = db.collection("+new");
      const readings = byId(
        (await old.find({ _type: "reading" }).toArray()) as Doc[],
      );
      const role = new Map(
        readings.map((doc, i) => [
          String(doc._id),
          (["source", "target", "gone"] as const)[i % 3],
        ]),
      );
      const touched = new Set<string>();
      const probe = probed(db, async (id) => {
        const key = String(id);
        if (touched.has(key)) return;
        touched.add(key);
        const kind = role.get(key);
        if (kind === "source") {
          await old.updateOne({ _id: id } as never, { $inc: { seq: 1000 } });
        } else if (kind === "target") {
          await fresh.updateOne({ _id: id } as never, { $set: { seq: -1 } });
        } else {
          await old.deleteOne({ _id: id } as never);
        }
      });
      await createMongodbApplier(probe, child, {
        currentMigrationId: child.id,
        batchSize: 2,
      }).applyMigration(childOps, "up");

      const expected = readings.flatMap((doc) => {
        const kind = role.get(String(doc._id));
        if (kind === "gone") return [];
        return [
          {
            ...doc,
            seq: kind === "source" ? (doc.seq as number) + 1000 : -1,
          },
        ];
      });
      const after = await mongoSnapshot(db);
      assertEquals(after.new, byId(expected));
      assert(after.old.every((doc) => doc._type === "keep"));
    });
  },
});

test({
  name: "moveType (mongodb): the source is read once with a cursor, 400 documents in batches of 50 examine about 400",
  timeout: MONGO_TIMEOUT_MS,
  fn: async () => {
    await withDatabase("move-type-linear", async (db) => {
      const { child, childOps } = await applyParent(db, 134);
      await db.command({ profile: 2 });
      try {
        await createMongodbApplier(db, child, {
          currentMigrationId: child.id,
          batchSize: 50,
        }).applyMigration(childOps, "up");
      } finally {
        await db.command({ profile: 0 });
      }
      const reads = await db
        .collection("system.profile")
        .find({ ns: `${db.databaseName}.+old`, op: "query" })
        .toArray();
      const examined = reads.reduce(
        (sum, entry) => sum + Number(entry.docsExamined ?? 0),
        0,
      );
      assertEquals(
        (await mongoSnapshot(db)).new.filter((doc) => doc._type === "reading")
          .length,
        402,
      );
      assert(examined <= 402 + 3 * 9, `examined ${examined} documents`);
    });
  },
});

test({
  name: "moveType (mongodb): a document written in the target after the move goes back with the others on down, and the unique index follows the type",
  timeout: MONGO_TIMEOUT_MS,
  fn: async () => {
    await withDatabase("move-type-down-new", async (db) => {
      const { child, childOps } = await applyParent(db, 2);
      const applier = createMongodbApplier(db, child, {
        currentMigrationId: child.id,
      });
      await applier.applyMigration(childOps, "up");
      const indexed = async (name: string) =>
        (await db.collection(name).indexes()).some(
          (index) => index.unique === true && "user" in (index.key ?? {}),
        );
      assertEquals(
        [await indexed("+new"), await indexed("+old")],
        [true, false],
      );

      const later = {
        _id: "reading:written-after",
        _type: "reading",
        _scope: "ws:a",
        user: "ws:a-late",
        seq: 9,
      };
      await db.collection("+new").insertOne(later as never);
      await applier.applyMigration(childOps, "down");

      const after = await mongoSnapshot(db);
      assertEquals(after.new, []);
      assert(after.old.some((doc) => doc._id === later._id && doc.seq === 9));
      assertEquals(
        after.old.filter((doc) => doc._type === "reading").length,
        7,
      );
      assertEquals(await indexed("+old"), true);
    });
  },
});

test({
  name: "moveType: a document the target holds with other content is a conflict, left in both; the others move, in both appliers",
  timeout: MONGO_TIMEOUT_MS,
  fn: async () => {
    const { parent, child, parentOps, childOps } = chain(2);
    const state = await createMemoryApplier(parent).applyMigration(
      createEmptyDatabaseState(),
      parentOps,
      "up",
    );
    const first = byId(
      state.scopedMultiCollections["+old"].content.filter(
        (doc) => doc._type === "reading",
      ),
    )[0];
    state.scopedMultiCollections["+new"] = {
      content: [{ ...first, seq: 77 }],
    };
    await assertRejects(
      () => createMemoryApplier(child).applyMigration(state, childOps, "up"),
      Error,
      "differ between the two collections",
    );
    const memory = snapshot(state);

    await withDatabase("move-type-conflict", async (db) => {
      await applyParent(db, 2);
      await db.createCollection("+new");
      await db.collection("+new").insertOne({ ...first, seq: 77 } as never);
      await assertRejects(
        () =>
          createMongodbApplier(db, child, {
            currentMigrationId: child.id,
          }).applyMigration(childOps, "up"),
        Error,
        "differ between the two collections",
      );
      const mongo = await mongoSnapshot(db);
      assertEquals(mongo, memory);
      assertEquals(
        mongo.old
          .filter((doc) => doc._type === "reading")
          .map((doc) => doc._id),
        [first._id],
      );
      assertEquals(mongo.new.length, 6);
    });
  },
});

test({
  name: "moveType: a document of another type holding the same _id in the target stops the move; what moved before it is in one collection only",
  timeout: MONGO_TIMEOUT_MS,
  fn: async () => {
    const { parent, child, parentOps, childOps } = chain(2);
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
    const clashing = readings[readings.length - 1];
    state.scopedMultiCollections["+new"] = {
      content: [{ ...clashing, _type: "other" }],
    };
    await assertRejects(
      () => createMemoryApplier(child).applyMigration(state, childOps, "up"),
      Error,
      "is held there by a document of type other",
    );
    const memory = snapshot(state);
    assertEquals(
      memory.old.filter((doc) => doc._type === "reading").map((doc) => doc._id),
      [clashing._id],
    );

    await withDatabase("move-type-clash", async (db) => {
      await applyParent(db, 2);
      await db.createCollection("+new");
      await db
        .collection("+new")
        .insertOne({ ...clashing, _type: "other" } as never);
      await assertRejects(() =>
        createMongodbApplier(db, child, {
          currentMigrationId: child.id,
          batchSize: 1,
        }).applyMigration(childOps, "up"),
      );
      const mongo = await mongoSnapshot(db);
      const ids = (docs: Doc[]) => docs.map((doc) => String(doc._id));
      const everywhere = [...ids(mongo.old), ...ids(mongo.new)];
      assertEquals(new Set(everywhere).size, everywhere.length - 1);
      assertEquals(mongo, memory);
    });
  },
});

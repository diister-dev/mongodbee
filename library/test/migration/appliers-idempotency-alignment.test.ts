/**
 * Regression coverage for the migration-applier fixes:
 *
 *  C3 — writes are idempotent on partial failure (a crashed run can retry):
 *       seeds and `flow` copies upsert on DETERMINISTIC ids; `flow_to_scope`
 *       mints deterministic target ids (never random UUIDs) so a replay
 *       recognises already-flowed docs instead of duplicating them.
 *  C4 — `onConflict: "skip"` + `source: "consume"` preserves the SKIPPED source
 *       docs: only the docs that actually landed are consumed.
 *  C5 — simulation (memory) and production (mongodb) agree: `flow_to_scope`
 *       conflicts on `_id` alone; `transform_scoped_multicollection_type`
 *       re-pins `_id/_type/_scope`; the memory `where` matcher understands the
 *       common Mongo operators + dot-paths and throws (never silently matches
 *       nothing) on an unsupported operator.
 *
 * DB prefixes are unique to this file to avoid colliding with sibling suites.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals, assertRejects } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import {
  deterministicSeedId,
  flowScopeTargetId,
} from "../../src/migration/utils/seed-id.ts";
import { createMultiCollectionInfo } from "../../src/migration/multicollection-registry.ts";
import { dbId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

// ───────────────────────────── C3: idempotency ─────────────────────────────

test("C3 mongodb seed: re-applying a seed is idempotent (deterministic upsert, no E11000)", async () => {
  await withDatabase("c3-seed-idem", async (db) => {
    const S = {
      collections: { widgets: { _id: dbId("widget"), n: v.number() } },
    };
    const m = migrationDefinition("001", "seed-widgets", {
      parent: null,
      schemas: S,
      migrate: (b) =>
        b
          .createCollection("widgets")
          .seed([{ n: 1 }, { n: 2 }, { n: 3 }])
          .end()
          .compile(),
    });
    const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
    const applier = createMongodbApplier(db, m, { currentMigrationId: m.id });

    // First apply.
    await applier.applyMigration(ops, "up");
    assertEquals(await db.collection("widgets").countDocuments(), 3);

    // Simulated crash-retry: applying the same seed AGAIN must not throw a
    // duplicate-key error and must not create duplicates.
    await applier.applyMigration(ops, "up");
    assertEquals(await db.collection("widgets").countDocuments(), 3);
  });
});

test("C3 mongodb seed: retry after a partial crash (some deterministic ids pre-inserted) succeeds", async () => {
  await withDatabase("c3-seed-partial", async (db) => {
    const S = {
      collections: { things: { _id: dbId("thing"), n: v.number() } },
    };
    const m = migrationDefinition("001", "seed-things", {
      parent: null,
      schemas: S,
      migrate: (b) =>
        b
          .createCollection("things")
          .seed([{ n: 10 }, { n: 20 }])
          .end()
          .compile(),
    });
    const ops = m.migrate(migrationBuilder({ schemas: S })).operations;

    // Emulate a crash that wrote the FIRST seed doc (deterministic id, sig =
    // collection name, index 0) but never recorded the migration.
    await db.createCollection("things");
    const crashedId = deterministicSeedId("thing", m.id, "things", 0);
    await db.collection("things").insertOne({ _id: crashedId, n: 10 } as never);

    // The retry must upsert cleanly (plain insertMany would throw E11000 here).
    await createMongodbApplier(db, m, {
      currentMigrationId: m.id,
    }).applyMigration(ops, "up");
    assertEquals(await db.collection("things").countDocuments(), 2);
  });
});

test("C3 mongodb flow COPY: re-running the copy is idempotent (upsert on deterministic target id)", async () => {
  await withDatabase("c3-flow-idem", async (db) => {
    const S = {
      collections: {
        users: { _id: dbId("user"), name: v.string(), active: v.boolean() },
        archived: {
          _id: dbId("archived"),
          name: v.string(),
          active: v.boolean(),
        },
      },
    };
    const m = migrationDefinition("001", "archive", {
      parent: null,
      schemas: S,
      migrate: (b) =>
        b
          .flow({
            from: { collection: "users", where: { active: false } },
            into: { collection: "archived" },
            map: (doc) => ({ ...doc }),
            source: "keep",
          })
          .compile(),
    });
    const [flowOp] = m.migrate(migrationBuilder({ schemas: S })).operations;

    await db.createCollection("users");
    await db.createCollection("archived");
    await db.collection("users").insertMany([
      { _id: "user:1", name: "Alice", active: true },
      { _id: "user:2", name: "Bob", active: false },
      { _id: "user:3", name: "Carol", active: false },
    ] as never);

    const applier = createMongodbApplier(db, m, { currentMigrationId: m.id });

    await applier.applyOperation(flowOp);
    assertEquals(await db.collection("archived").countDocuments(), 2);

    // Re-run the copy loop (simulated retry): deterministic target ids mean the
    // second pass upserts the same docs — no E11000, no duplicates.
    await applier.applyOperation(flowOp);
    assertEquals(await db.collection("archived").countDocuments(), 2);
  });
});

test("C3 mongodb flow_to_scope: minted target ids are deterministic — replay never duplicates", async () => {
  await withDatabase("c3-fts-detid", async (db) => {
    const S = { collections: { src: { _id: v.string() } } };
    const m = migrationDefinition("001", "flow-detid", {
      parent: null,
      schemas: S,
      migrate: (b) =>
        b
          .flowToScope({
            from: { kind: "collection", name: "src" },
            into: { collection: "scoped" },
            toType: () => "thing",
            scope: () => "exposition:z",
            // Drop `_id` → the applier must MINT a deterministic id.
            map: (d) => {
              const { _id: _drop, ...rest } = d as Record<string, unknown>;
              return rest;
            },
            onConflict: "merge",
            merge: (existing, incoming) => ({ ...existing, ...incoming }),
            source: "keep",
          })
          .compile(),
    });
    const [flowOp] = m.migrate(migrationBuilder({ schemas: S })).operations;

    await db.collection("src").insertMany([
      { _id: "u:1", v: 1 },
      { _id: "u:2", v: 2 },
    ] as never);

    const applier = createMongodbApplier(db, m, { currentMigrationId: m.id });

    await applier.applyOperation(flowOp);
    const first = await db
      .collection("scoped")
      .find({} as never)
      .toArray();
    assertEquals(first.length, 2);

    // Replay: deterministic ids collide with the already-flowed docs, so the
    // merge path replaces them in place. With the old random-UUID id every
    // replay would DOUBLE the collection.
    await applier.applyOperation(flowOp);
    const second = await db
      .collection("scoped")
      .find({} as never)
      .toArray();
    assertEquals(second.length, 2);

    // The minted ids match what flowScopeTargetId derives from the source.
    const expected = new Set([
      flowScopeTargetId("thing", m.id, "src", "u:1"),
      flowScopeTargetId("thing", m.id, "src", "u:2"),
    ]);
    assertEquals(new Set(second.map((d) => String(d._id))), expected);
  });
});

// ───────────────────── C4: skip + consume preserves skipped ─────────────────

test("C4 mongodb flow_to_scope skip+consume (collection): skipped source docs survive", async () => {
  await withDatabase("c4-skip-consume-coll", async (db) => {
    const S = { collections: { src: { _id: v.string() } } };
    const m = migrationDefinition("001", "skip-consume", {
      parent: null,
      schemas: S,
      migrate: (b) =>
        b
          .flowToScope({
            from: { kind: "collection", name: "src" },
            into: { collection: "scoped" },
            toType: () => "thing",
            scope: () => "s:1",
            onConflict: "skip",
            source: "consume",
          })
          .compile(),
    });
    const [flowOp] = m.migrate(migrationBuilder({ schemas: S })).operations;

    await db.collection("src").insertMany([
      { _id: "thing:keepme", v: 1 },
      { _id: "thing:moveme", v: 2 },
    ] as never);
    // Pre-existing target doc collides with "thing:keepme" → it will be skipped.
    await db.collection("scoped").insertOne({
      _id: "thing:keepme",
      _type: "thing",
      _scope: "s:1",
      pre: true,
    } as never);

    await createMongodbApplier(db, m, {
      currentMigrationId: m.id,
    }).applyOperation(flowOp);

    // The skipped doc is NOT consumed — it stays in the source.
    const remaining = await db
      .collection("src")
      .find({} as never)
      .toArray();
    assertEquals(remaining.length, 1);
    assertEquals(String(remaining[0]._id), "thing:keepme");

    // Target keeps the pre-existing (untouched) doc + the doc that landed.
    const scoped = await db
      .collection("scoped")
      .find({} as never)
      .toArray();
    assertEquals(scoped.length, 2);
    const kept = scoped.find((d) => String(d._id) === "thing:keepme")!;
    assertEquals((kept as { pre?: boolean }).pre, true); // not overwritten
    assert(scoped.some((d) => String(d._id) === "thing:moveme"));
  });
});

test("C4 mongodb flow_to_scope skip+consume (instances): instance with a skip is NOT dropped", async () => {
  await withDatabase("c4-skip-consume-inst", async (db) => {
    const S = { collections: {} };
    const m = migrationDefinition("001", "skip-consume-inst", {
      parent: null,
      schemas: S,
      migrate: (b) =>
        b
          .flowToScope({
            from: { kind: "multiModelInstances", model: "widget" },
            into: { collection: "scoped" },
            toType: () => "gadget",
            scope: (_d, ctx) => ctx.instanceName!,
            onConflict: "skip",
            source: "consume",
          })
          .compile(),
    });
    const [flowOp] = m.migrate(migrationBuilder({ schemas: S })).operations;

    // Two registered instances of model "widget", each holding one gadget doc.
    await createMultiCollectionInfo(db, "widget:1", "widget", m.id);
    await createMultiCollectionInfo(db, "widget:2", "widget", m.id);
    await db
      .collection("widget:1")
      .insertOne({ _id: "gadget:g1", _type: "gadget", v: 1 } as never);
    await db
      .collection("widget:2")
      .insertOne({ _id: "gadget:g2", _type: "gadget", v: 2 } as never);
    // Pre-existing target doc collides with widget:1's gadget → it is skipped.
    await db.collection("scoped").insertOne({
      _id: "gadget:g1",
      _type: "gadget",
      _scope: "widget:1",
      pre: true,
    } as never);

    await createMongodbApplier(db, m, {
      currentMigrationId: m.id,
    }).applyOperation(flowOp);

    // widget:1 had a skipped doc → the instance is NOT dropped and keeps g1.
    assertEquals(
      await db
        .collection("widget:1")
        .countDocuments({ _id: "gadget:g1" } as never),
      1,
    );
    // widget:2 fully landed → its collection is dropped.
    const collections = await db
      .listCollections({ name: "widget:2" } as never)
      .toArray();
    assertEquals(collections.length, 0);

    // Target holds the pre-existing g1 (untouched) + the landed g2.
    const scoped = await db
      .collection("scoped")
      .find({} as never)
      .toArray();
    assertEquals(scoped.length, 2);
    const g1 = scoped.find((d) => String(d._id) === "gadget:g1")!;
    assertEquals((g1 as { pre?: boolean }).pre, true);
  });
});

test("C4 memory flow_to_scope skip+consume: skipped source docs survive", async () => {
  const state = createEmptyDatabaseState();
  state.collections.src = {
    content: [
      { _id: "thing:keepme", v: 1 },
      { _id: "thing:moveme", v: 2 },
    ],
  };
  state.scopedMultiCollections.scoped = {
    content: [
      {
        _id: "thing:keepme",
        _type: "thing",
        _scope: "s:1",
        pre: true,
      },
    ],
  };

  const S = { collections: { src: { _id: v.string() } } };
  const m = migrationDefinition("001", "mem-skip-consume", {
    parent: null,
    schemas: S,
    migrate: (b) =>
      b
        .flowToScope({
          from: { kind: "collection", name: "src" },
          into: { collection: "scoped" },
          toType: () => "thing",
          scope: () => "s:1",
          onConflict: "skip",
          source: "consume",
        })
        .compile(),
  });
  const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
  await createMemoryApplier(m).applyMigration(state, ops, "up");

  // Source collection still exists and retains ONLY the skipped doc.
  assert(state.collections.src, "source collection must not be dropped");
  assertEquals(state.collections.src.content.length, 1);
  assertEquals(String(state.collections.src.content[0]._id), "thing:keepme");

  const scoped = state.scopedMultiCollections.scoped.content;
  assertEquals(scoped.length, 2);
  assertEquals(scoped.find((d) => String(d._id) === "thing:keepme")!.pre, true);
});

test("C4 memory flow_to_scope skip+consume (instances): a skip in one instance does NOT stop a sibling from being dropped", async () => {
  // Mirrors the mongodb "(instances)" test: skip/landed bookkeeping must be
  // PER SOURCE. widget:1 has a skipped doc → it survives; widget:2 is clean →
  // it is dropped WHOLE (key removed, including bookkeeping). A single global
  // skip flag would have wrongly kept widget:2 as an empty registered
  // instance, disagreeing with production on collection existence.
  const state = createEmptyDatabaseState();
  state.multiModels["widget:1"] = {
    modelType: "widget",
    content: [
      { _id: "_information", _type: "_information" },
      { _id: "gadget:g1", _type: "gadget", v: 1 },
    ],
  };
  state.multiModels["widget:2"] = {
    modelType: "widget",
    content: [
      { _id: "_information", _type: "_information" },
      { _id: "gadget:g2", _type: "gadget", v: 2 },
    ],
  };
  // Pre-existing target doc collides with widget:1's gadget → it is skipped.
  state.scopedMultiCollections.scoped = {
    content: [
      {
        _id: "gadget:g1",
        _type: "gadget",
        _scope: "widget:1",
        pre: true,
      },
    ],
  };

  const S = { collections: {} };
  const m = migrationDefinition("001", "mem-skip-consume-inst", {
    parent: null,
    schemas: S,
    migrate: (b) =>
      b
        .flowToScope({
          from: { kind: "multiModelInstances", model: "widget" },
          into: { collection: "scoped" },
          toType: () => "gadget",
          scope: (_d, ctx) => ctx.instanceName!,
          onConflict: "skip",
          source: "consume",
        })
        .compile(),
  });
  const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
  await createMemoryApplier(m).applyMigration(state, ops, "up");

  // widget:1 had a skipped doc → the instance is NOT dropped and keeps g1.
  assert(state.multiModels["widget:1"], "instance with a skip must survive");
  assertEquals(
    state.multiModels["widget:1"].content.some(
      (d) => String(d._id) === "gadget:g1",
    ),
    true,
  );
  // widget:2 fully landed → its instance is dropped WHOLE (bookkeeping too).
  assertEquals(
    state.multiModels["widget:2"],
    undefined,
    "clean instance must be dropped whole despite a sibling's skip",
  );

  // Target holds the pre-existing g1 (untouched) + the landed g2.
  const scoped = state.scopedMultiCollections.scoped.content;
  assertEquals(scoped.length, 2);
  assertEquals(scoped.find((d) => String(d._id) === "gadget:g1")!.pre, true);
  assert(
    scoped.some((d) => String(d._id) === "gadget:g2"),
    "widget:2's gadget must have landed in the target",
  );
});

// ──────────────────────── C5: simulation ↔ production ───────────────────────

test("C5a memory flow_to_scope: conflict is keyed on _id ALONE (like prod)", async () => {
  const state = createEmptyDatabaseState();
  state.collections.src = { content: [{ _id: "x:1", v: 1 }] };
  // Pre-existing target doc shares _id but a DIFFERENT _type/_scope. Under the
  // old (_scope && _type && _id) key this would not conflict and a duplicate
  // _id would be pushed — impossible in the real single-collection PK.
  state.scopedMultiCollections.scoped = {
    content: [{ _id: "x:1", _type: "typeA", _scope: "scopeA" }],
  };

  const S = { collections: { src: { _id: v.string() } } };
  const m = migrationDefinition("001", "id-conflict", {
    parent: null,
    schemas: S,
    migrate: (b) =>
      b
        .flowToScope({
          from: { kind: "collection", name: "src" },
          into: { collection: "scoped" },
          toType: () => "typeB",
          scope: () => "scopeB",
          onConflict: "error",
          source: "keep",
        })
        .compile(),
  });
  const ops = m.migrate(migrationBuilder({ schemas: S })).operations;

  await assertRejects(
    () => createMemoryApplier(m).applyMigration(state, ops, "up"),
    Error,
    "conflict",
  );
});

test("C5b transform_scoped_multicollection_type: mongodb re-pins _id/_type/_scope (aligned with memory)", async () => {
  const SCHEMAS = {
    collections: {},
    scopedMultiCollections: {
      catalog: {
        scope: v.string(),
        types: { artwork: { title: v.string() } },
      },
    },
  };
  // A transform that DROPS the discriminators — a common mistake. Both appliers
  // must re-pin them so the doc stays visible to scoped queries.
  const createAndSeed = migrationDefinition("001", "create-catalog", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .createScopedMultiCollection("catalog")
        .type("artwork")
        .seed("expo:a", [{ title: "Mona Lisa" }])
        .end()
        .end()
        .compile(),
  });
  const transform = migrationDefinition("002", "bad-transform", {
    parent: createAndSeed,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .scopedMultiCollection("catalog")
        .type("artwork")
        .transform({
          up: (doc) => {
            // Deliberately strip meta fields — the applier must restore them.
            const {
              _id: _i,
              _type: _t,
              _scope: _s,
              ...rest
            } = doc as Record<string, unknown>;
            return { ...rest, title: "renamed" };
          },
          down: (doc) => ({ ...doc }),
        })
        .end()
        .end()
        .compile(),
  });

  // ---- mongodb: the fix under test ----
  await withDatabase("c5b-repin", async (db) => {
    await createMongodbApplier(db, createAndSeed, {
      currentMigrationId: createAndSeed.id,
    }).applyMigration(
      createAndSeed.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
      "up",
    );

    const tOps = transform.migrate(
      migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }),
    ).operations;
    await createMongodbApplier(db, transform, {
      currentMigrationId: transform.id,
    }).applyMigration(tOps, "up");

    // The doc is still visible to a scoped query (discriminators re-pinned) and
    // carries the transformed field.
    const byScope = await db
      .collection("catalog")
      .find({ _scope: "expo:a", _type: "artwork" } as never)
      .toArray();
    assertEquals(byScope.length, 1);
    assertEquals((byScope[0] as { title?: string }).title, "renamed");
  });

  // ---- memory: same outcome (the simulation the gate trusts) ----
  const state = createEmptyDatabaseState();
  await createMemoryApplier(createAndSeed).applyMigration(
    state,
    createAndSeed.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
    "up",
  );
  await createMemoryApplier(transform).applyMigration(
    state,
    transform.migrate(
      migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }),
    ).operations,
    "up",
  );
  const memDocs = state.scopedMultiCollections.catalog.content;
  assertEquals(memDocs.length, 1);
  assertEquals(memDocs[0]._scope, "expo:a");
  assertEquals(memDocs[0]._type, "artwork");
  assertEquals(memDocs[0].title, "renamed");
});

test("C5c memory matchesWhere: supports operators + dot-paths, throws on unsupported", async () => {
  const S = {
    collections: {
      people: { _id: v.string() },
      adults: { _id: v.string() },
    },
  };
  const seedPeople = () => {
    const state = createEmptyDatabaseState();
    state.collections.people = {
      content: [
        { _id: "p:1", age: 30, profile: { tier: "gold" } },
        { _id: "p:2", age: 12, profile: { tier: "silver" } },
        { _id: "p:3", age: 18, profile: { tier: "gold" } },
      ],
    };
    state.collections.adults = { content: [] };
    return state;
  };

  const flowMigration = (where: Record<string, unknown>) =>
    migrationDefinition("001", "flow-where", {
      parent: null,
      schemas: S,
      migrate: (b) =>
        b
          .flow({
            from: { collection: "people", where },
            into: { collection: "adults" },
            map: (doc) => ({ ...doc }),
            source: "keep",
          })
          .compile(),
    });

  // NOTE: régime-A `flow` re-keys the copy (`_id` = flowTargetId), so we assert
  // on `age` — a user field that survives the copy — to identify which source
  // docs the `where` matched.
  const copiedAges = (state: ReturnType<typeof seedPeople>) =>
    new Set(state.collections.adults.content.map((d) => d.age));

  // $gte — only ages 30 and 18 flow.
  {
    const state = seedPeople();
    const m = flowMigration({ age: { $gte: 18 } });
    await createMemoryApplier(m).applyMigration(
      state,
      m.migrate(migrationBuilder({ schemas: S })).operations,
      "up",
    );
    assertEquals(copiedAges(state), new Set([30, 18]));
  }

  // $in on _id — source docs p:2 and p:3 (ages 12 and 18).
  {
    const state = seedPeople();
    const m = flowMigration({ _id: { $in: ["p:2", "p:3"] } });
    await createMemoryApplier(m).applyMigration(
      state,
      m.migrate(migrationBuilder({ schemas: S })).operations,
      "up",
    );
    assertEquals(copiedAges(state), new Set([12, 18]));
  }

  // nested dot-path — profile.tier == "gold" (ages 30 and 18).
  {
    const state = seedPeople();
    const m = flowMigration({ "profile.tier": "gold" });
    await createMemoryApplier(m).applyMigration(
      state,
      m.migrate(migrationBuilder({ schemas: S })).operations,
      "up",
    );
    assertEquals(copiedAges(state), new Set([30, 18]));
  }

  // Unsupported operator must FAIL LOUD, never silently match nothing.
  {
    const state = seedPeople();
    const m = flowMigration({ _id: { $regex: "p:" } });
    await assertRejects(
      () =>
        createMemoryApplier(m).applyMigration(
          state,
          m.migrate(migrationBuilder({ schemas: S })).operations,
          "up",
        ),
      Error,
      "not supported in simulation",
    );
  }
});

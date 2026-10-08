/**
 * Replay fidelity: the in-memory applier must leave the same physical
 * documents behind as the mongodb applier, so a scenario replayed in memory
 * and written to MongoDB is what a real `migrate` would have produced.
 *
 * Each case seeds the same documents into a memory state and a real database,
 * runs the same operations through both appliers, and compares every physical
 * collection (memory buckets flattened by name, mongodbee bookkeeping docs
 * left out on the Mongo side because the simulation does not model them).
 */
import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import {
  createEmptyDatabaseState,
  type DatabaseState,
  type MigrationBuilder,
  type MigrationRule,
  type SchemasDefinition,
} from "../../src/migration/types.ts";
import { createMultiCollectionInfo } from "../../src/migration/multicollection-registry.ts";
import * as v from "../../src/schema.ts";

type Doc = Record<string, unknown>;
type FlowContext = { sourceCollection?: string; documentType?: string };

type PhysicalSeed = {
  collections?: Record<string, Doc[]>;
  multiCollections?: Record<string, Doc[]>;
  scopedMultiCollections?: Record<string, Doc[]>;
  multiModels?: Record<string, { modelType: string; content: Doc[] }>;
};

type Outcome =
  | { threw: false; physical: Record<string, Doc[]> }
  | { threw: true };

function canonical(value: unknown): unknown {
  if (value instanceof Date) return { $date: value.toISOString() };
  if (Array.isArray(value)) return value.map(canonical);
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      const field = (value as Doc)[key];
      if (field !== undefined) out[key] = canonical(field);
    }
    return out;
  }
  return value;
}

function normalise(byName: Record<string, Doc[]>): Record<string, Doc[]> {
  const out: Record<string, Doc[]> = {};
  for (const name of Object.keys(byName).sort()) {
    const docs = byName[name]
      .filter((d) => !(typeof d._type === "string" && d._type.startsWith("_")))
      .map((d) => canonical(d) as Doc)
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    if (docs.length > 0) out[name] = docs;
  }
  return out;
}

function memoryState(seed: PhysicalSeed): DatabaseState {
  const state = createEmptyDatabaseState();
  for (const [name, docs] of Object.entries(seed.collections ?? {})) {
    state.collections[name] = { content: structuredClone(docs) };
  }
  for (const [name, docs] of Object.entries(seed.multiCollections ?? {})) {
    state.multiCollections[name] = { content: structuredClone(docs) };
  }
  for (const [name, docs] of Object.entries(
    seed.scopedMultiCollections ?? {},
  )) {
    state.scopedMultiCollections[name] = { content: structuredClone(docs) };
  }
  for (const [name, inst] of Object.entries(seed.multiModels ?? {})) {
    state.multiModels[name] = {
      modelType: inst.modelType,
      content: structuredClone(inst.content),
    };
  }
  return state;
}

function physicalOfState(state: DatabaseState): Record<string, Doc[]> {
  const out: Record<string, Doc[]> = {};
  for (const bucket of [
    state.collections,
    state.multiCollections,
    state.scopedMultiCollections,
    state.multiModels,
  ]) {
    for (const [name, coll] of Object.entries(bucket)) {
      (out[name] ??= []).push(...coll.content);
    }
  }
  return normalise(out);
}

async function runParity(
  name: string,
  schemas: SchemasDefinition,
  migrate: (b: MigrationBuilder) => MigrationBuilder,
  seed: PhysicalSeed,
): Promise<{ memory: Outcome; mongo: Outcome }> {
  const migration = migrationDefinition("001", name, {
    parent: null,
    schemas,
    migrate: (b) => migrate(b).compile(),
  });
  const ops: MigrationRule[] = migration.migrate(
    migrationBuilder({ schemas }),
  ).operations;

  let memory: Outcome;
  try {
    const state = memoryState(seed);
    await createMemoryApplier(migration).applyMigration(state, ops, "up");
    memory = { threw: false, physical: physicalOfState(state) };
  } catch {
    memory = { threw: true };
  }

  let mongo: Outcome = { threw: true };
  await withDatabase(`parity-${name}`, async (db) => {
    const inserts: [string, Doc[]][] = [
      ...Object.entries(seed.collections ?? {}),
      ...Object.entries(seed.multiCollections ?? {}),
      ...Object.entries(seed.scopedMultiCollections ?? {}),
      ...Object.entries(seed.multiModels ?? {}).map(
        ([n, inst]) => [n, inst.content] as [string, Doc[]],
      ),
    ];
    for (const [coll, docs] of inserts) {
      await db.createCollection(coll);
      if (docs.length > 0) {
        await db.collection(coll).insertMany(structuredClone(docs) as never);
      }
    }
    for (const [coll, inst] of Object.entries(seed.multiModels ?? {})) {
      await createMultiCollectionInfo(db, coll, inst.modelType);
    }

    const applier = createMongodbApplier(db, migration, {
      currentMigrationId: migration.id,
    });
    try {
      for (const op of ops) await applier.applyOperation(op);
    } catch {
      mongo = { threw: true };
      return;
    }
    const out: Record<string, Doc[]> = {};
    for (const info of await db.listCollections().toArray()) {
      if (info.name.startsWith("system.")) continue;
      out[info.name] = (await db
        .collection(info.name)
        .find({})
        .toArray()) as Doc[];
    }
    mongo = { threw: false, physical: normalise(out) };
  });

  return { memory, mongo };
}

async function assertParity(
  name: string,
  schemas: SchemasDefinition,
  migrate: (b: MigrationBuilder) => MigrationBuilder,
  seed: PhysicalSeed,
): Promise<Outcome> {
  const { memory, mongo } = await runParity(name, schemas, migrate, seed);
  assertEquals(memory, mongo);
  return mongo;
}

const EXPOSITIONS: Doc[] = [
  {
    _id: "participant:1",
    _type: "participant",
    _scope: "exposition:A",
    notificationSettings: { suppressed: true },
  },
  {
    _id: "participant:2",
    _type: "participant",
    _scope: "exposition:A",
    notificationSettings: { suppressed: false },
  },
  {
    _id: "participant:3",
    _type: "participant",
    _scope: "exposition:B",
    notificationSettings: { suppressed: true },
  },
  {
    _id: "exposition:A",
    _type: "information",
    _scope: "exposition:A",
    notificationSettings: { suppressed: true },
  },
];

const SCOPED_SCHEMAS = { collections: {} } satisfies SchemasDefinition;

for (const source of ["keep", "consume"] as const) {
  test(`parity flowToScope: scoped multi-collection source, where on _type (${source})`, async () => {
    const result = await assertParity(
      `fts-scoped-${source}`,
      SCOPED_SCHEMAS,
      (b) =>
        b.flowToScope({
          from: {
            kind: "collection",
            name: "+expositions",
            where: {
              _type: "participant",
              "notificationSettings.suppressed": true,
            },
          },
          into: { collection: "+notifications" },
          scope: (d: Doc) => d._scope as string,
          toType: () => "suppression",
          map: (d: Doc, ctx: FlowContext) => ({
            participantId: d._id,
            origin: ctx.sourceCollection,
          }),
          source,
        }),
      { scopedMultiCollections: { "+expositions": EXPOSITIONS } },
    );
    assertEquals(
      result.threw ? -1 : result.physical["+notifications"]?.length,
      2,
    );
  });
}

test("parity flowToScope: whole multi-collection consumed by name", async () => {
  await assertParity(
    "fts-multi-whole",
    SCOPED_SCHEMAS,
    (b) =>
      b.flowToScope({
        from: { kind: "collection", name: "catalog" },
        into: { collection: "+scoped" },
        scope: () => "exposition:A",
        source: "consume",
      }),
    {
      multiCollections: {
        catalog: [
          { _id: "item:1", _type: "item", n: 1 },
          { _id: "tag:1", _type: "tag", label: "x" },
        ],
      },
    },
  );
});

test("parity flowToScope: multiCollectionType read from a scoped collection", async () => {
  await assertParity(
    "fts-type-scoped",
    SCOPED_SCHEMAS,
    (b) =>
      b.flowToScope({
        from: {
          kind: "multiCollectionType",
          collectionName: "+legacy",
          documentType: "participant",
        },
        into: { collection: "+scoped" },
        scope: (d: Doc) => d._scope as string,
        map: (d: Doc, ctx: FlowContext) => ({ ...d, from: ctx.documentType }),
        source: "consume",
      }),
    { scopedMultiCollections: { "+legacy": EXPOSITIONS } },
  );
});

const WIDGETS = {
  collections: {
    widgets: {
      _id: v.string(),
      n: v.number(),
      label: v.optional(v.string(), "none"),
    },
    copies: { _id: v.string(), n: v.number() },
  },
} satisfies SchemasDefinition;

test("parity createCollection: an existing collection keeps its documents", async () => {
  await assertParity(
    "create-existing",
    WIDGETS,
    (b) => b.createCollection("widgets").end(),
    { collections: { widgets: [{ _id: "w:1", n: 1, label: "a" }] } },
  );
});

test("parity seed: schema defaults are applied and an existing _id is replaced", async () => {
  await assertParity(
    "seed-defaults",
    WIDGETS,
    (b) =>
      b
        .collection("widgets")
        .seed([
          { _id: "w:1", n: 1 },
          { _id: "w:2", n: 2, label: "two" },
        ])
        .end(),
    { collections: { widgets: [{ _id: "w:1", n: 0, label: "old" }] } },
  );
});

test("parity transform: a transform that drops _id keeps the document's _id", async () => {
  await assertParity(
    "transform-id",
    WIDGETS,
    (b) =>
      b
        .collection("widgets")
        .transform({
          up: ({ _id, ...rest }: Doc) => ({ ...rest, n: Number(rest.n) + 1 }),
          down: (d: Doc) => d,
        })
        .end(),
    {
      collections: {
        widgets: [
          { _id: "w:1", n: 1, label: "a" },
          { _id: "w:2", n: 2, label: "b" },
        ],
      },
    },
  );
});

test("parity flow: flowing the same documents twice upserts on the target id", async () => {
  await assertParity(
    "flow-twice",
    WIDGETS,
    (b) =>
      b
        .flow({
          from: { collection: "widgets" },
          into: { collection: "copies" },
          map: (d: Doc) => ({ n: d.n }),
          source: "keep",
        })
        .flow({
          from: { collection: "widgets" },
          into: { collection: "copies" },
          map: (d: Doc) => ({ n: d.n }),
          source: "keep",
        }),
    {
      collections: {
        widgets: [{ _id: "w:1", n: 1, label: "a" }],
        copies: [],
      },
    },
  );
});

test("parity renameCollection: a missing source fails on both sides", async () => {
  const outcome = await assertParity(
    "rename-missing",
    WIDGETS,
    (b) => b.renameCollection("ghost", "widgets2"),
    { collections: { widgets: [{ _id: "w:1", n: 1, label: "a" }] } },
  );
  assertEquals(outcome.threw, true);
});

test("parity renameCollection: an existing target without dropTarget fails on both sides", async () => {
  const outcome = await assertParity(
    "rename-target-exists",
    WIDGETS,
    (b) => b.renameCollection("widgets", "copies"),
    {
      collections: {
        widgets: [{ _id: "w:1", n: 1, label: "a" }],
        copies: [{ _id: "c:1", n: 9 }],
      },
    },
  );
  assertEquals(outcome.threw, true);
});

test("parity markMultiModelType: a multi-collection becomes an instance its next operation reaches", async () => {
  const schemas = {
    collections: {},
    multiModels: {
      exposition: { participant: { _id: v.string(), name: v.string() } },
    },
  } satisfies SchemasDefinition;
  await assertParity(
    "mark-multi",
    schemas,
    (b) =>
      b
        .markMultiModelType("exposition:A", "exposition")
        .type("participant")
        .transform({
          up: (d: Doc) => ({ ...d, name: String(d.name).toUpperCase() }),
          down: (d: Doc) => d,
        })
        .end()
        .end(),
    {
      multiCollections: {
        "exposition:A": [
          { _id: "participant:1", _type: "participant", name: "ann" },
        ],
      },
    },
  );
});

test("parity markMultiModelType: a missing collection fails on both sides", async () => {
  const schemas = {
    collections: {},
    multiModels: {
      exposition: { participant: { _id: v.string(), name: v.string() } },
    },
  } satisfies SchemasDefinition;
  const outcome = await assertParity(
    "mark-missing",
    schemas,
    (b) => b.markMultiModelType("exposition:Z", "exposition").end(),
    {},
  );
  assertEquals(outcome.threw, true);
});

test("parity where: null matches a missing field, a scalar matches an array element", async () => {
  const schemas = {
    collections: {
      rows: {
        _id: v.string(),
        tags: v.optional(v.array(v.string())),
        archivedAt: v.optional(v.nullable(v.string())),
      },
    },
  } satisfies SchemasDefinition;
  await assertParity(
    "where-null-array",
    schemas,
    (b) =>
      b
        .collection("rows")
        .deleteWhere({ archivedAt: null })
        .deleteWhere({ tags: "x" })
        .end(),
    {
      collections: {
        rows: [
          { _id: "r:1", archivedAt: "2024-01-01", tags: ["x", "y"] },
          { _id: "r:2", archivedAt: "2024-01-01", tags: ["y"] },
          { _id: "r:3", archivedAt: "2024-01-01" },
          { _id: "r:4", archivedAt: null },
          { _id: "r:5" },
        ],
      },
    },
  );
});

test("parity where: $in and $ne on an array field and on a missing field", async () => {
  const schemas = {
    collections: {
      rows: { _id: v.string(), tags: v.optional(v.array(v.string())) },
    },
  } satisfies SchemasDefinition;
  await assertParity(
    "where-in-ne",
    schemas,
    (b) =>
      b
        .flowToScope({
          from: {
            kind: "collection",
            name: "rows",
            where: { tags: { $in: ["x", null] } },
          },
          into: { collection: "+in" },
          scope: () => "s",
          toType: () => "row",
          source: "keep",
        })
        .flowToScope({
          from: {
            kind: "collection",
            name: "rows",
            where: { tags: { $ne: "y" } },
          },
          into: { collection: "+ne" },
          scope: () => "s",
          toType: () => "row",
          source: "keep",
        }),
    {
      collections: {
        rows: [
          { _id: "r:1", tags: ["x", "y"] },
          { _id: "r:2", tags: ["y"] },
          { _id: "r:3", tags: [] },
          { _id: "r:4" },
        ],
      },
    },
  );
});

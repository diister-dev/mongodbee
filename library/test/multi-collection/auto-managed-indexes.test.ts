import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { multiCollection } from "../../src/multi-collection.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import * as v from "../../src/schema.ts";
import type { Collection, Db } from "mongodb";

const TYPE_COUNT = 12;

const types = Object.fromEntries(
  Array.from({ length: TYPE_COUNT }, (_, i) => [
    `kind${i}`,
    {
      _id: refId(`kind${i}`),
      label: i % 3 === 0 ? withIndex(v.string()) : v.string(),
    },
  ]),
);

async function indexSet(collection: Collection) {
  const indexes = await collection.indexes();
  return indexes
    .map((index) =>
      JSON.stringify({
        name: index.name,
        key: index.key,
        unique: index.unique ?? false,
        partialFilterExpression: index.partialFilterExpression ?? null,
      }),
    )
    .sort();
}

async function managedIndexes(db: Db, name: string) {
  const schemas = { multiCollections: { [name]: types } };
  const migration = migrationDefinition("001", "create", {
    parent: null,
    schemas,
    migrate: (b) => {
      const created = b.createMultiCollection(name);
      for (const type of Object.keys(types)) created.type(type).end();
      return created.end().compile();
    },
  });
  const operations = migration.migrate(
    migrationBuilder({ schemas }),
  ).operations;
  await createMongodbApplier(db, migration, {
    currentMigrationId: migration.id,
  }).applyMigration(operations, "up");
  return indexSet(db.collection(name));
}

test("a multi-collection opened in auto mode has the indexes the managed migration creates", async () => {
  await withDatabase("auto-managed-indexes", async (db) => {
    const managed = await managedIndexes(db, "+managed");
    await multiCollection(db, "+auto", types, { schemaManagement: "auto" });
    const auto = await indexSet(db.collection("+auto"));

    assertEquals(auto.length, managed.length);
    assertEquals(auto, managed);
    assertEquals(auto.filter((index) => index.includes('"_type":1')).length, 1);
  });
});

test("reopening an auto multi-collection drops the per-type _type indexes older versions created", async () => {
  await withDatabase("auto-legacy-type-indexes", async (db) => {
    const managed = await managedIndexes(db, "+managed");
    const legacy = db.collection("+auto");
    await legacy.createIndex({ _type: 1 }, { name: "_type_1" });
    for (const type of Object.keys(types)) {
      await legacy.createIndex(
        { _type: 1, _id: 1 },
        {
          name: `${type}__type`,
          partialFilterExpression: { _type: { $eq: type } },
        },
      );
    }

    await multiCollection(db, "+auto", types, { schemaManagement: "auto" });

    assertEquals(await indexSet(legacy), managed);
  });
});

import * as v from "../../src/schema.ts";
import { assert, assertEquals } from "@std/assert";
import { newMultiCollection, createMultiCollectionInstance } from "../../src/multi-collection.ts";
import { defineModel } from "../../src/multi-collection-model.ts";
import { withDatabase } from "../+shared.ts";

const testModel = defineModel("test", {
  schema: {
    item: {
      name: v.string(),
      quantity: v.number(),
    },
  },
});

Deno.test("Metadata creation: newMultiCollection with raw schema should NOT create metadata", async (t) => {
  await withDatabase(t.name, async (db) => {
    const rawSchema = {
      product: {
        name: v.string(),
        price: v.number(),
      },
    };

    // Create collection with raw schema
    await newMultiCollection(db, "test_raw", rawSchema);

    // Check for metadata documents
    const collection = db.collection("test_raw");
    const infoDoc = await collection.findOne({ _type: "_information" });
    const migrationsDoc = await collection.findOne({ _type: "_migrations" });

    // Raw schema should NOT create metadata
    assertEquals(infoDoc, null, "newMultiCollection with raw schema should NOT create _information");
    assertEquals(migrationsDoc, null, "newMultiCollection with raw schema should NOT create _migrations");
  });
});

Deno.test("Metadata creation: createMultiCollectionInstance with model SHOULD create metadata", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create collection instance with model
    await createMultiCollectionInstance(db, "test_instance", testModel);

    // Check for metadata documents
    const collection = db.collection("test_instance");
    const infoDoc = await collection.findOne({ _type: "_information" }) as any;
    const migrationsDoc = await collection.findOne({ _type: "_migrations" }) as any;

    // Model-based instance SHOULD create metadata
    assert(infoDoc !== null, "createMultiCollectionInstance should create _information");
    assert(migrationsDoc !== null, "createMultiCollectionInstance should create _migrations");

    // Verify metadata structure
    assertEquals(infoDoc._id, "_information");
    assertEquals(infoDoc._type, "_information");
    assertEquals(infoDoc.collectionType, "test");
    assert(infoDoc.createdAt instanceof Date);

    assertEquals(migrationsDoc._id, "_migrations");
    assertEquals(migrationsDoc._type, "_migrations");
    assert(Array.isArray(migrationsDoc.appliedMigrations));
    assert(migrationsDoc.appliedMigrations.length > 0);
  });
});

Deno.test("Metadata creation: newMultiCollection with model SHOULD create metadata", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create collection with model (not instance)
    const collection = await newMultiCollection(db, "test_with_model", testModel.schema);

    // Check for metadata documents
    const rawCollection = db.collection("test_with_model");
    const infoDoc = await rawCollection.findOne({ _type: "_information" });
    const migrationsDoc = await rawCollection.findOne({ _type: "_migrations" });

    // Passing raw schema (even if from model) should NOT create metadata
    assertEquals(infoDoc, null, "newMultiCollection with model.schema should NOT create _information");
    assertEquals(migrationsDoc, null, "newMultiCollection with model.schema should NOT create _migrations");
  });
});

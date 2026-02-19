import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
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

test("Metadata creation: newMultiCollection with raw schema should NOT create metadata", async () => {
  await withDatabase("Metadata creation: newMultiCollection with raw schema should NOT create metadata", async (db) => {
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
    expect(infoDoc).toEqual(null);
    expect(migrationsDoc).toEqual(null);
  });
});

test("Metadata creation: createMultiCollectionInstance with model SHOULD create metadata", async () => {
  await withDatabase("Metadata creation: createMultiCollectionInstance with model SHOULD create metadata", async (db) => {
    // Create collection instance with model
    await createMultiCollectionInstance(db, "test_instance", testModel);

    // Check for metadata documents
    const collection = db.collection("test_instance");
    const infoDoc = await collection.findOne({ _type: "_information" }) as any;
    const migrationsDoc = await collection.findOne({ _type: "_migrations" }) as any;

    // Model-based instance SHOULD create metadata
    expect(infoDoc).not.toBeNull();
    expect(migrationsDoc).not.toBeNull();

    // Verify metadata structure
    expect(infoDoc._id).toEqual("_information");
    expect(infoDoc._type).toEqual("_information");
    expect(infoDoc.collectionType).toEqual("test");
    expect(infoDoc.createdAt instanceof Date).toBeTruthy();

    expect(migrationsDoc._id).toEqual("_migrations");
    expect(migrationsDoc._type).toEqual("_migrations");
    expect(Array.isArray(migrationsDoc.appliedMigrations)).toBeTruthy();
    expect(migrationsDoc.appliedMigrations.length > 0).toBeTruthy();
  });
});

test("Metadata creation: newMultiCollection with model SHOULD create metadata", async () => {
  await withDatabase("Metadata creation: newMultiCollection with model SHOULD create metadata", async (db) => {
    // Create collection with model (not instance)
    const collection = await newMultiCollection(db, "test_with_model", testModel.schema);

    // Check for metadata documents
    const rawCollection = db.collection("test_with_model");
    const infoDoc = await rawCollection.findOne({ _type: "_information" });
    const migrationsDoc = await rawCollection.findOne({ _type: "_migrations" });

    // Passing raw schema (even if from model) should NOT create metadata
    expect(infoDoc).toEqual(null);
    expect(migrationsDoc).toEqual(null);
  });
});

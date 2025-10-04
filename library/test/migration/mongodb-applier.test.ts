/**
 * Tests for MongoDB Migration Applier
 *
 * Tests the MongoDB applier with real database operations
 */

import { ObjectId } from "mongodb";

import * as v from "../../src/schema.ts";
import { assert, assertEquals, assertExists } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { MongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import type {
  CreateCollectionRule,
  CreateMultiCollectionInstanceRule,
  SeedCollectionRule,
  SeedMultiCollectionInstanceRule,
  TransformCollectionRule,
  TransformMultiCollectionTypeRule,
} from "../../src/migration/types.ts";
import {
  discoverMultiCollectionInstances,
  getMultiCollectionInfo,
  getMultiCollectionMigrations,
} from "../../src/migration/multicollection-registry.ts";

// ============================================================================
// Create Collection Tests
// ============================================================================

Deno.test("MongodbApplier - applyOperation creates collection", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    const operation: CreateCollectionRule = {
      type: "create_collection",
      collectionName: "users",
      schema: {
        _id: v.string(),
        name: v.string(),
      },
    };

    await applier.applyOperation(operation);

    // Check collection exists
    const collections = await db.listCollections({ name: "users" }).toArray();
    assertEquals(collections.length, 1);
  });
});

Deno.test("MongodbApplier - applyReverseOperation drops collection", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    // Create collection first
    await db.createCollection("users");

    const operation: CreateCollectionRule = {
      type: "create_collection",
      collectionName: "users",
    };

    await applier.applyReverseOperation(operation);

    // Check collection is gone
    const collections = await db.listCollections({ name: "users" }).toArray();
    assertEquals(collections.length, 0);
  });
});

Deno.test("MongodbApplier - creates collection with validator", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    const operation: CreateCollectionRule = {
      type: "create_collection",
      collectionName: "users",
      schema: {
        _id: v.string(),
        name: v.string(),
        email: v.pipe(v.string(), v.email()),
      },
    };

    await applier.applyOperation(operation);

    // Check validator was applied
    const collections = await db.listCollections({ name: "users" })
      .toArray() as any[];
    assertExists(collections[0].options?.validator);
  });
});

// ============================================================================
// Seed Collection Tests
// ============================================================================

Deno.test("MongodbApplier - seed inserts documents", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    // Create collection first
    await db.createCollection("users");

    const documents = [
      { _id: "1" as unknown as ObjectId, name: "Alice" },
      { _id: "2" as unknown as ObjectId, name: "Bob" },
      { _id: "3" as unknown as ObjectId, name: "Charlie" },
    ];

    const operation: SeedCollectionRule = {
      type: "seed_collection",
      collectionName: "users",
      documents,
    };

    await applier.applyOperation(operation);

    // Check documents were inserted
    const collection = db.collection("users");
    const count = await collection.countDocuments({});
    assertEquals(count, 3);

    const docs = await collection.find({}).toArray();
    assertEquals(docs.length, 3);
    assertEquals(String(docs[0]._id), "1");
  });
});

Deno.test("MongodbApplier - reverse seed removes documents", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    // Create collection and insert documents
    const collection = db.collection("users");
    const id1 = new ObjectId();
    const id2 = new ObjectId();
    const documents = [
      { _id: id1, name: "Alice" },
      { _id: id2, name: "Bob" },
    ];
    await collection.insertMany(documents);

    const operation: SeedCollectionRule = {
      type: "seed_collection",
      collectionName: "users",
      documents,
    };

    await applier.applyReverseOperation(operation);

    // Check documents were removed
    const count = await collection.countDocuments({});
    assertEquals(count, 0);
  });
});

// ============================================================================
// Transform Collection Tests
// ============================================================================

Deno.test("MongodbApplier - transform updates all documents", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    // Create collection and insert documents
    const collection = db.collection("users");
    await collection.insertMany([
      { _id: "1" as unknown as ObjectId, name: "Alice" },
      { _id: "2" as unknown as ObjectId, name: "Bob" },
    ]);

    const operation: TransformCollectionRule = {
      type: "transform_collection",
      collectionName: "users",
      up: (doc: Record<string, unknown>) => ({
        ...doc,
        age: 25,
      }),
      down: (doc: Record<string, unknown>) => {
        const { age: _age, ...rest } = doc;
        return rest;
      },
    };

    await applier.applyOperation(operation);

    // Check all documents were transformed
    const docs = await collection.find({}).toArray();
    assertEquals(docs.length, 2);
    assertEquals(docs[0].age, 25);
    assertEquals(docs[1].age, 25);
  });
});

Deno.test("MongodbApplier - reverse transform restores original documents", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    // Create collection with transformed documents
    const collection = db.collection("users");
    await collection.insertMany([
      { _id: "1" as unknown as ObjectId, name: "Alice", age: 25 },
      { _id: "2" as unknown as ObjectId, name: "Bob", age: 30 },
    ]);

    const operation: TransformCollectionRule = {
      type: "transform_collection",
      collectionName: "users",
      up: (doc: Record<string, unknown>) => ({
        ...doc,
        age: 25,
      }),
      down: (doc: Record<string, unknown>) => {
        const { age: _age, ...rest } = doc;
        return rest;
      },
    };

    await applier.applyReverseOperation(operation);

    // Check age field was removed
    const docs = await collection.find({}).toArray();
    assertEquals(docs.length, 2);
    assertEquals(docs[0].age, undefined);
    assertEquals(docs[1].age, undefined);
  });
});

// ============================================================================
// Multi-Collection Instance Tests
// ============================================================================

Deno.test("MongodbApplier - creates multi-collection instance with metadata", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);
    applier.setCurrentMigrationId("test_migration_001");

    const operation: CreateMultiCollectionInstanceRule = {
      type: "create_multicollection_instance",
      collectionName: "catalog_main",
      collectionType: "catalog",
    };

    await applier.applyOperation(operation);

    // Check collection was created
    const collections = await db.listCollections({ name: "catalog_main" })
      .toArray();
    assertEquals(collections.length, 1);

    // Check metadata documents
    const info = await getMultiCollectionInfo(db, "catalog_main");
    assertExists(info);
    assertEquals(info.collectionType, "catalog");

    const migrations = await getMultiCollectionMigrations(db, "catalog_main");
    assertExists(migrations);
    assertExists(migrations.fromMigrationId);
  });
});

Deno.test("MongodbApplier - reverse drops multi-collection instance", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    // Create instance first
    const collection = db.collection("catalog_main");
    await collection.insertOne({
      _id: "_information" as unknown as ObjectId,
      _type: "_information",
      collectionType: "catalog",
      createdAt: new Date(),
    });

    const operation: CreateMultiCollectionInstanceRule = {
      type: "create_multicollection_instance",
      collectionName: "catalog_main",
      collectionType: "catalog",
    };

    await applier.applyReverseOperation(operation);

    // Check collection was dropped
    const collections = await db.listCollections({ name: "catalog_main" })
      .toArray();
    assertEquals(collections.length, 0);
  });
});

// ============================================================================
// Multi-Collection Seed Tests
// ============================================================================

Deno.test("MongodbApplier - seeds multi-collection type with _type field", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    // Create instance first
    const collection = db.collection("catalog_main");
    await collection.insertOne({
      _id: "_information" as unknown as ObjectId,
      _type: "_information",
      collectionType: "catalog",
      createdAt: new Date(),
    });

    const products = [
      { name: "Product 1", price: 100 },
      { name: "Product 2", price: 200 },
    ];

    const operation: SeedMultiCollectionInstanceRule = {
      type: "seed_multicollection_instance",
      collectionName: "catalog_main",
      typeName: "product",
      documents: products,
    };

    await applier.applyOperation(operation);

    // Check documents have _type field
    const docs = await collection.find({ _type: "product" }).toArray();
    assertEquals(docs.length, 2);
    assertEquals(docs[0]._type, "product");
    assert(docs[0]._id.toString().startsWith("product:"));
  });
});

// ============================================================================
// Multi-Collection Transform Tests
// ============================================================================

Deno.test("MongodbApplier - transforms type across all instances", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);
    applier.setCurrentMigrationId("test_migration_001");

    // Create two instances
    const instance1 = db.collection("catalog_store1");
    await instance1.insertMany([
      {
        _id: "_information" as unknown as ObjectId,
        _type: "_information",
        collectionType: "catalog",
        createdAt: new Date(),
      },
      {
        _id: "_migrations" as unknown as ObjectId,
        _type: "_migrations",
        fromMigrationId: "test_migration_000",
        appliedMigrations: [],
      },
      { _id: "p1" as unknown as ObjectId, _type: "product", name: "Product 1" },
    ]);

    const instance2 = db.collection("catalog_store2");
    await instance2.insertMany([
      {
        _id: "_information" as unknown as ObjectId,
        _type: "_information",
        collectionType: "catalog",
        createdAt: new Date(),
      },
      {
        _id: "_migrations" as unknown as ObjectId,
        _type: "_migrations",
        fromMigrationId: "test_migration_000",
        appliedMigrations: [],
      },
      { _id: "p2" as unknown as ObjectId, _type: "product", name: "Product 2" },
    ]);

    const operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "catalog",
      typeName: "product",
      up: (doc: Record<string, unknown>) => ({
        ...doc,
        price: 0,
      }),
      down: (doc: Record<string, unknown>) => {
        const { price: _price, ...rest } = doc;
        return rest;
      },
    };

    await applier.applyOperation(operation);

    // Check both instances were transformed
    const docs1 = await instance1.find({ _type: "product" }).toArray();
    assertEquals(docs1.length, 1);
    assertEquals(docs1[0].price, 0);

    const docs2 = await instance2.find({ _type: "product" }).toArray();
    assertEquals(docs2.length, 1);
    assertEquals(docs2[0].price, 0);
  });
});

Deno.test("MongodbApplier - skips instances created after migration", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);
    applier.setCurrentMigrationId("2025_01_01_0000_AAAAAAAA@test");

    // Create instance created AFTER the migration
    const instance = db.collection("catalog_new");
    await instance.insertMany([
      {
        _id: "_information" as unknown as ObjectId,
        _type: "_information",
        collectionType: "catalog",
        createdAt: new Date(),
      },
      {
        _id: "_migrations" as unknown as ObjectId,
        _type: "_migrations",
        fromMigrationId: "2025_12_31_2359_ZZZZZZZZ@future", // Created in the future
        appliedMigrations: [],
      },
      { _id: "p1" as unknown as ObjectId, _type: "product", name: "Product 1" },
    ]);

    const operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "catalog",
      typeName: "product",
      up: (doc: Record<string, unknown>) => ({
        ...doc,
        price: 999,
      }),
      down: (doc: Record<string, unknown>) => {
        const { price: _price, ...rest } = doc;
        return rest;
      },
    };

    await applier.applyOperation(operation);

    // Check instance was NOT transformed
    const docs = await instance.find({ _type: "product" }).toArray();
    assertEquals(docs.length, 1);
    assertEquals(docs[0].price, undefined); // Should not have price field
  });
});

// ============================================================================
// Discovery Tests
// ============================================================================

Deno.test("MongodbApplier - discovers all instances of a type", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create multiple instances
    const instances = ["catalog_store1", "catalog_store2", "catalog_store3"];

    for (const instanceName of instances) {
      const collection = db.collection(instanceName);
      await collection.insertOne({
        _id: "_information" as unknown as ObjectId,
        _type: "_information",
        collectionType: "catalog",
        createdAt: new Date(),
      });
    }

    // Discover instances
    const discovered = await discoverMultiCollectionInstances(db, "catalog");

    assertEquals(discovered.length, 3);
    assert(discovered.includes("catalog_store1"));
    assert(discovered.includes("catalog_store2"));
    assert(discovered.includes("catalog_store3"));
  });
});

// ============================================================================
// Schema Synchronization Tests
// ============================================================================

Deno.test("MongodbApplier - synchronizeSchemas updates validators", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    // Create collection with initial schema
    await db.createCollection("users");

    const schemas = {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
        },
      },
    };

    await applier.synchronizeSchemas(schemas);

    // Check validator was applied
    const collections = await db.listCollections({ name: "users" })
      .toArray() as any[];
    assertExists(collections[0].options?.validator);
  });
});

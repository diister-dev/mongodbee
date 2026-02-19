/**
 * Tests for Multi-Collection Registry
 *
 * Tests discovery, tracking, and version filtering of multi-collection instances
 */

import { test, expect } from "vitest";
import { withDatabase } from "../+shared.ts";
import {
  createMultiCollectionInfo,
  discoverMultiCollectionInstances,
  getMultiCollectionInfo,
  getMultiCollectionMigrations,
  isInstanceCreatedAfterMigration,
  markAsMultiCollection,
  MULTI_COLLECTION_INFO_TYPE,
  MULTI_COLLECTION_MIGRATIONS_TYPE,
  multiCollectionInstanceExists,
  recordMultiCollectionMigration,
  shouldInstanceReceiveMigration,
} from "../../src/migration/multicollection-registry.ts";

// ============================================================================
// Multi-Collection Info Tests
// ============================================================================

test("createMultiCollectionInfo - creates metadata documents", async () => {
  await withDatabase("createMultiCollectionInfo - creates metadata documents", async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog", "mig_001");

    const collection = db.collection("catalog_main");

    // Check _information document
    const info = await collection.findOne({
      _type: MULTI_COLLECTION_INFO_TYPE,
    });
    expect(info).toBeDefined();
    expect(info._id as any).toEqual(MULTI_COLLECTION_INFO_TYPE);
    expect(info.collectionType).toEqual("catalog");
    expect(info.createdAt).toBeDefined();

    // Check _migrations document
    const migrations = await collection.findOne({
      _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
    });
    expect(migrations).toBeDefined();
    expect(migrations._id as any).toEqual(MULTI_COLLECTION_MIGRATIONS_TYPE);
    expect(migrations.fromMigrationId).toEqual("mig_001");
    expect(migrations.appliedMigrations).toBeDefined();
    expect(migrations.appliedMigrations.length).toEqual(1);
  });
});

test("getMultiCollectionInfo - retrieves information document", async () => {
  await withDatabase("getMultiCollectionInfo - retrieves information document", async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog");

    const info = await getMultiCollectionInfo(db, "catalog_main");

    expect(info).toBeDefined();
    expect(info.collectionType).toEqual("catalog");
    expect(info._type).toEqual(MULTI_COLLECTION_INFO_TYPE);
  });
});

test("getMultiCollectionInfo - returns null for non-existent collection", async () => {
  await withDatabase("getMultiCollectionInfo - returns null for non-existent collection", async (db) => {
    const info = await getMultiCollectionInfo(db, "nonexistent");

    expect(info).toEqual(null);
  });
});

// ============================================================================
// Migration Recording Tests
// ============================================================================

test("recordMultiCollectionMigration - adds migration to history", async () => {
  await withDatabase("recordMultiCollectionMigration - adds migration to history", async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog", "mig_001");

    await recordMultiCollectionMigration(db, "catalog_main", "mig_002");

    const migrations = await getMultiCollectionMigrations(db, "catalog_main");
    expect(migrations).toBeDefined();
    expect(migrations.appliedMigrations.length).toEqual(2);
    expect(migrations.appliedMigrations[1].id).toEqual("mig_002");
  });
});

test("getMultiCollectionMigrations - retrieves migration history", async () => {
  await withDatabase("getMultiCollectionMigrations - retrieves migration history", async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog", "mig_001");
    await recordMultiCollectionMigration(db, "catalog_main", "mig_002");
    await recordMultiCollectionMigration(db, "catalog_main", "mig_003");

    const migrations = await getMultiCollectionMigrations(db, "catalog_main");

    expect(migrations).toBeDefined();
    expect(migrations.fromMigrationId).toEqual("mig_001");
    expect(migrations.appliedMigrations.length).toEqual(3);
  });
});

// ============================================================================
// Instance Discovery Tests
// ============================================================================

test("discoverMultiCollectionInstances - finds all instances of a type", async () => {
  await withDatabase("discoverMultiCollectionInstances - finds all instances of a type", async (db) => {
    // Create multiple instances
    await createMultiCollectionInfo(db, "catalog_store1", "catalog");
    await createMultiCollectionInfo(db, "catalog_store2", "catalog");
    await createMultiCollectionInfo(db, "catalog_store3", "catalog");

    // Create instance of different type
    await createMultiCollectionInfo(db, "library_main", "library");

    const instances = await discoverMultiCollectionInstances(db, "catalog");

    expect(instances.length).toEqual(3);
    expect(instances.includes("catalog_store1")).toBeTruthy();
    expect(instances.includes("catalog_store2")).toBeTruthy();
    expect(instances.includes("catalog_store3")).toBeTruthy();
    expect(!instances.includes("library_main")).toBeTruthy();
  });
});

test("discoverMultiCollectionInstances - returns empty array when no instances", async () => {
  await withDatabase("discoverMultiCollectionInstances - returns empty array when no instances", async (db) => {
    const instances = await discoverMultiCollectionInstances(db, "nonexistent");

    expect(instances.length).toEqual(0);
  });
});

test("discoverMultiCollectionInstances - returns sorted results", async () => {
  await withDatabase("discoverMultiCollectionInstances - returns sorted results", async (db) => {
    // Create instances in random order
    await createMultiCollectionInfo(db, "catalog_c", "catalog");
    await createMultiCollectionInfo(db, "catalog_a", "catalog");
    await createMultiCollectionInfo(db, "catalog_b", "catalog");

    const instances = await discoverMultiCollectionInstances(db, "catalog");

    expect(instances).toEqual(["catalog_a", "catalog_b", "catalog_c"]);
  });
});

// ============================================================================
// Instance Existence Tests
// ============================================================================

test("multiCollectionInstanceExists - returns true for existing instance", async () => {
  await withDatabase("multiCollectionInstanceExists - returns true for existing instance", async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog");

    const exists = await multiCollectionInstanceExists(db, "catalog_main");

    expect(exists).toBeTruthy();
  });
});

test("multiCollectionInstanceExists - returns false for non-existent instance", async () => {
  await withDatabase("multiCollectionInstanceExists - returns false for non-existent instance", async (db) => {
    const exists = await multiCollectionInstanceExists(db, "nonexistent");

    expect(!exists).toBeTruthy();
  });
});

// ============================================================================
// Version Filtering Tests
// ============================================================================

test("isInstanceCreatedAfterMigration - compares migration IDs correctly", () => {
  // Instance created in January
  const instanceCreatedAt = "2025_01_01_0000_AAAAAAAA@initial";

  // Current migration in February
  const currentMigration = "2025_02_01_0000_BBBBBBBB@add_field";

  // Instance was created BEFORE current migration
  const result = isInstanceCreatedAfterMigration(
    instanceCreatedAt,
    currentMigration,
  );

  expect(!result).toBeTruthy();
});

test("isInstanceCreatedAfterMigration - detects future instances", () => {
  // Instance created in December
  const instanceCreatedAt = "2025_12_31_2359_ZZZZZZZZ@future";

  // Current migration in January
  const currentMigration = "2025_01_01_0000_AAAAAAAA@initial";

  // Instance was created AFTER current migration
  const result = isInstanceCreatedAfterMigration(
    instanceCreatedAt,
    currentMigration,
  );

  expect(result).toBeTruthy();
});

test("isInstanceCreatedAfterMigration - handles unknown migration IDs", () => {
  const instanceCreatedAt = "unknown";
  const currentMigration = "2025_01_01_0000_AAAAAAAA@initial";

  // Unknown should be treated as old (needs all migrations)
  const result = isInstanceCreatedAfterMigration(
    instanceCreatedAt,
    currentMigration,
  );

  expect(!result).toBeTruthy();
});

test("shouldInstanceReceiveMigration - returns true for old instances", async () => {
  await withDatabase("shouldInstanceReceiveMigration - returns true for old instances", async (db) => {
    const collection = db.collection("catalog_old");
    await collection.insertMany([
      {
        _id: MULTI_COLLECTION_INFO_TYPE as any,
        _type: MULTI_COLLECTION_INFO_TYPE,
        collectionType: "catalog",
        createdAt: new Date(),
      },
      {
        _id: MULTI_COLLECTION_MIGRATIONS_TYPE as any,
        _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
        fromMigrationId: "2025_01_01_0000_AAAAAAAA@initial",
        appliedMigrations: [],
      },
    ]);

    const shouldReceive = await shouldInstanceReceiveMigration(
      db,
      "catalog_old",
      "2025_12_31_2359_ZZZZZZZZ@current",
    );

    expect(shouldReceive).toBeTruthy();
  });
});

test("shouldInstanceReceiveMigration - returns false for new instances", async () => {
  await withDatabase("shouldInstanceReceiveMigration - returns false for new instances", async (db) => {
    const collection = db.collection("catalog_new");
    await collection.insertMany([
      {
        _id: MULTI_COLLECTION_INFO_TYPE as any,
        _type: MULTI_COLLECTION_INFO_TYPE,
        collectionType: "catalog",
        createdAt: new Date(),
      },
      {
        _id: MULTI_COLLECTION_MIGRATIONS_TYPE as any,
        _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
        fromMigrationId: "2025_12_31_2359_ZZZZZZZZ@future",
        appliedMigrations: [],
      },
    ]);

    const shouldReceive = await shouldInstanceReceiveMigration(
      db,
      "catalog_new",
      "2025_01_01_0000_AAAAAAAA@initial",
    );

    expect(!shouldReceive).toBeTruthy();
  });
});

// ============================================================================
// Mark As Multi-Collection Tests
// ============================================================================

test("markAsMultiCollection - converts existing collection", async () => {
  await withDatabase("markAsMultiCollection - converts existing collection", async (db) => {
    // Create a regular collection
    const collection = db.collection("existing_catalog");
    await collection.insertOne({
      _id: "product1" as any,
      _type: "product",
      name: "Product 1",
    });

    // Mark it as multi-collection
    await markAsMultiCollection(db, "existing_catalog", "catalog", "mig_001");

    // Check metadata was added
    const info = await getMultiCollectionInfo(db, "existing_catalog");
    expect(info).toBeDefined();
    expect(info.collectionType).toEqual("catalog");

    // Check original data is still there
    const product = await collection.findOne({ _id: "product1" as any });
    expect(product).toBeDefined();
    expect(product.name).toEqual("Product 1");
  });
});

test("markAsMultiCollection - throws if already marked", async () => {
  await withDatabase("markAsMultiCollection - throws if already marked", async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog");

    try {
      await markAsMultiCollection(db, "catalog_main", "catalog");
      throw new Error("Should have thrown");
    } catch (error) {
      expect(error instanceof Error).toBeTruthy();
      expect(error.message.includes("already marked")).toBeTruthy();
    }
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

test("Multi-collection registry - full lifecycle", async () => {
  await withDatabase("Multi-collection registry - full lifecycle", async (db) => {
    // 1. Create instance
    await createMultiCollectionInfo(db, "catalog_main", "catalog", "mig_001");

    // 2. Verify creation
    const exists = await multiCollectionInstanceExists(db, "catalog_main");
    expect(exists).toBeTruthy();

    // 3. Add data
    const collection = db.collection("catalog_main");
    await collection.insertOne({
      _id: "product1" as any,
      _type: "product",
      name: "Product 1",
    });

    // 4. Record migrations
    await recordMultiCollectionMigration(db, "catalog_main", "mig_002");
    await recordMultiCollectionMigration(db, "catalog_main", "mig_003");

    // 5. Verify migration history
    const migrations = await getMultiCollectionMigrations(db, "catalog_main");
    expect(migrations).toBeDefined();
    expect(migrations.appliedMigrations.length).toEqual(3);

    // 6. Discover instance
    const instances = await discoverMultiCollectionInstances(db, "catalog");
    expect(instances.length).toEqual(1);
    expect(instances[0]).toEqual("catalog_main");

    // 7. Check version filtering
    const shouldReceiveOld = await shouldInstanceReceiveMigration(
      db,
      "catalog_main",
      "mig_004",
    );
    expect(shouldReceiveOld).toBeTruthy(); // Should receive newer migrations

    const shouldReceiveVeryOld = await shouldInstanceReceiveMigration(
      db,
      "catalog_main",
      "mig_000",
    );
    expect(!shouldReceiveVeryOld).toBeTruthy(); // Should not receive older migrations
  });
});

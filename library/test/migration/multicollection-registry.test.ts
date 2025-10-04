/**
 * Tests for Multi-Collection Registry
 *
 * Tests discovery, tracking, and version filtering of multi-collection instances
 */

import { assert, assertEquals, assertExists } from "@std/assert";
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

Deno.test("createMultiCollectionInfo - creates metadata documents", async (t) => {
  await withDatabase(t.name, async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog", "mig_001");

    const collection = db.collection("catalog_main");

    // Check _information document
    const info = await collection.findOne({
      _type: MULTI_COLLECTION_INFO_TYPE,
    });
    assertExists(info);
    assertEquals(info._id as any, MULTI_COLLECTION_INFO_TYPE);
    assertEquals(info.collectionType, "catalog");
    assertExists(info.createdAt);

    // Check _migrations document
    const migrations = await collection.findOne({
      _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
    });
    assertExists(migrations);
    assertEquals(migrations._id as any, MULTI_COLLECTION_MIGRATIONS_TYPE);
    assertEquals(migrations.fromMigrationId, "mig_001");
    assertExists(migrations.appliedMigrations);
    assertEquals(migrations.appliedMigrations.length, 1);
  });
});

Deno.test("getMultiCollectionInfo - retrieves information document", async (t) => {
  await withDatabase(t.name, async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog");

    const info = await getMultiCollectionInfo(db, "catalog_main");

    assertExists(info);
    assertEquals(info.collectionType, "catalog");
    assertEquals(info._type, MULTI_COLLECTION_INFO_TYPE);
  });
});

Deno.test("getMultiCollectionInfo - returns null for non-existent collection", async (t) => {
  await withDatabase(t.name, async (db) => {
    const info = await getMultiCollectionInfo(db, "nonexistent");

    assertEquals(info, null);
  });
});

// ============================================================================
// Migration Recording Tests
// ============================================================================

Deno.test("recordMultiCollectionMigration - adds migration to history", async (t) => {
  await withDatabase(t.name, async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog", "mig_001");

    await recordMultiCollectionMigration(db, "catalog_main", "mig_002");

    const migrations = await getMultiCollectionMigrations(db, "catalog_main");
    assertExists(migrations);
    assertEquals(migrations.appliedMigrations.length, 2);
    assertEquals(migrations.appliedMigrations[1].id, "mig_002");
  });
});

Deno.test("getMultiCollectionMigrations - retrieves migration history", async (t) => {
  await withDatabase(t.name, async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog", "mig_001");
    await recordMultiCollectionMigration(db, "catalog_main", "mig_002");
    await recordMultiCollectionMigration(db, "catalog_main", "mig_003");

    const migrations = await getMultiCollectionMigrations(db, "catalog_main");

    assertExists(migrations);
    assertEquals(migrations.fromMigrationId, "mig_001");
    assertEquals(migrations.appliedMigrations.length, 3);
  });
});

// ============================================================================
// Instance Discovery Tests
// ============================================================================

Deno.test("discoverMultiCollectionInstances - finds all instances of a type", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create multiple instances
    await createMultiCollectionInfo(db, "catalog_store1", "catalog");
    await createMultiCollectionInfo(db, "catalog_store2", "catalog");
    await createMultiCollectionInfo(db, "catalog_store3", "catalog");

    // Create instance of different type
    await createMultiCollectionInfo(db, "library_main", "library");

    const instances = await discoverMultiCollectionInstances(db, "catalog");

    assertEquals(instances.length, 3);
    assert(instances.includes("catalog_store1"));
    assert(instances.includes("catalog_store2"));
    assert(instances.includes("catalog_store3"));
    assert(!instances.includes("library_main"));
  });
});

Deno.test("discoverMultiCollectionInstances - returns empty array when no instances", async (t) => {
  await withDatabase(t.name, async (db) => {
    const instances = await discoverMultiCollectionInstances(db, "nonexistent");

    assertEquals(instances.length, 0);
  });
});

Deno.test("discoverMultiCollectionInstances - returns sorted results", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create instances in random order
    await createMultiCollectionInfo(db, "catalog_c", "catalog");
    await createMultiCollectionInfo(db, "catalog_a", "catalog");
    await createMultiCollectionInfo(db, "catalog_b", "catalog");

    const instances = await discoverMultiCollectionInstances(db, "catalog");

    assertEquals(instances, ["catalog_a", "catalog_b", "catalog_c"]);
  });
});

// ============================================================================
// Instance Existence Tests
// ============================================================================

Deno.test("multiCollectionInstanceExists - returns true for existing instance", async (t) => {
  await withDatabase(t.name, async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog");

    const exists = await multiCollectionInstanceExists(db, "catalog_main");

    assert(exists);
  });
});

Deno.test("multiCollectionInstanceExists - returns false for non-existent instance", async (t) => {
  await withDatabase(t.name, async (db) => {
    const exists = await multiCollectionInstanceExists(db, "nonexistent");

    assert(!exists);
  });
});

// ============================================================================
// Version Filtering Tests
// ============================================================================

Deno.test("isInstanceCreatedAfterMigration - compares migration IDs correctly", () => {
  // Instance created in January
  const instanceCreatedAt = "2025_01_01_0000_AAAAAAAA@initial";

  // Current migration in February
  const currentMigration = "2025_02_01_0000_BBBBBBBB@add_field";

  // Instance was created BEFORE current migration
  const result = isInstanceCreatedAfterMigration(
    instanceCreatedAt,
    currentMigration,
  );

  assert(!result);
});

Deno.test("isInstanceCreatedAfterMigration - detects future instances", () => {
  // Instance created in December
  const instanceCreatedAt = "2025_12_31_2359_ZZZZZZZZ@future";

  // Current migration in January
  const currentMigration = "2025_01_01_0000_AAAAAAAA@initial";

  // Instance was created AFTER current migration
  const result = isInstanceCreatedAfterMigration(
    instanceCreatedAt,
    currentMigration,
  );

  assert(result);
});

Deno.test("isInstanceCreatedAfterMigration - handles unknown migration IDs", () => {
  const instanceCreatedAt = "unknown";
  const currentMigration = "2025_01_01_0000_AAAAAAAA@initial";

  // Unknown should be treated as old (needs all migrations)
  const result = isInstanceCreatedAfterMigration(
    instanceCreatedAt,
    currentMigration,
  );

  assert(!result);
});

Deno.test("shouldInstanceReceiveMigration - returns true for old instances", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    assert(shouldReceive);
  });
});

Deno.test("shouldInstanceReceiveMigration - returns false for new instances", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    assert(!shouldReceive);
  });
});

// ============================================================================
// Mark As Multi-Collection Tests
// ============================================================================

Deno.test("markAsMultiCollection - converts existing collection", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertExists(info);
    assertEquals(info.collectionType, "catalog");

    // Check original data is still there
    const product = await collection.findOne({ _id: "product1" as any });
    assertExists(product);
    assertEquals(product.name, "Product 1");
  });
});

Deno.test("markAsMultiCollection - throws if already marked", async (t) => {
  await withDatabase(t.name, async (db) => {
    await createMultiCollectionInfo(db, "catalog_main", "catalog");

    try {
      await markAsMultiCollection(db, "catalog_main", "catalog");
      throw new Error("Should have thrown");
    } catch (error) {
      assert(error instanceof Error);
      assert(error.message.includes("already marked"));
    }
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

Deno.test("Multi-collection registry - full lifecycle", async (t) => {
  await withDatabase(t.name, async (db) => {
    // 1. Create instance
    await createMultiCollectionInfo(db, "catalog_main", "catalog", "mig_001");

    // 2. Verify creation
    const exists = await multiCollectionInstanceExists(db, "catalog_main");
    assert(exists);

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
    assertExists(migrations);
    assertEquals(migrations.appliedMigrations.length, 3);

    // 6. Discover instance
    const instances = await discoverMultiCollectionInstances(db, "catalog");
    assertEquals(instances.length, 1);
    assertEquals(instances[0], "catalog_main");

    // 7. Check version filtering
    const shouldReceiveOld = await shouldInstanceReceiveMigration(
      db,
      "catalog_main",
      "mig_004",
    );
    assert(shouldReceiveOld); // Should receive newer migrations

    const shouldReceiveVeryOld = await shouldInstanceReceiveMigration(
      db,
      "catalog_main",
      "mig_000",
    );
    assert(!shouldReceiveVeryOld); // Should not receive older migrations
  });
});

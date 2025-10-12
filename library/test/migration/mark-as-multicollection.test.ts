/**
 * Tests for the markAsMultiCollection migration operation
 *
 * This tests the builder API and appliers for marking existing collections
 * as multi-collections during migrations.
 */

import { assertEquals, assertExists, assertRejects } from "@std/assert";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { MongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import type { MarkAsMultiModelTypeRule } from "../../src/migration/types.ts";
import { withDatabase } from "../+shared.ts";
import {
  MULTI_COLLECTION_INFO_TYPE,
  MULTI_COLLECTION_MIGRATIONS_TYPE,
} from "../../src/migration/multicollection-registry.ts";

// ============================================================================
// Builder API Tests
// ============================================================================

Deno.test("markAsMultiCollection - creates correct operation rule", () => {
  const builder = migrationBuilder({ schemas: {} });

  builder.markMultiModelType("catalog_main", "catalog");

  const state = builder.compile();
  assertEquals(state.operations.length, 1);
  assertEquals(state.operations[0].type, "mark_as_multicollection");
  assertEquals(
    (state.operations[0] as MarkAsMultiModelTypeRule).collectionName,
    "catalog_main",
  );
  assertEquals(
    (state.operations[0] as MarkAsMultiModelTypeRule).modelType,
    "catalog",
  );
});

Deno.test("markAsMultiCollection - supports method chaining", () => {
  const builder = migrationBuilder({ schemas: {} });

  const result = builder
    .markMultiModelType("catalog_main", "catalog")
    .markMultiModelType("product_main", "product");

  assertEquals(result, builder); // Returns same builder
  assertEquals(builder.compile().operations.length, 2);
});

// ============================================================================
// MongodbApplier Tests (real DB)
// ============================================================================

Deno.test("markAsMultiCollection - creates metadata documents", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    // Create a regular collection with some documents
    const collection = db.collection("test_catalog");
    await collection.insertMany([
      { name: "Product A", price: 100 },
      { name: "Product B", price: 200 },
    ]);

    // Mark as multi-collection
    const operation: MarkAsMultiModelTypeRule = {
      type: "mark_as_multicollection",
      collectionName: "test_catalog",
      modelType: "catalog",
    };

    await applier.applyOperation(operation);

    // Check that metadata documents were created
    const infoDoc = await collection.findOne({
      _type: MULTI_COLLECTION_INFO_TYPE,
    });
    assertExists(infoDoc, "Info document should be created");
    assertEquals(infoDoc.collectionType, "catalog");

    const migrationsDoc = await collection.findOne({
      _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
    });
    assertExists(migrationsDoc, "Migrations document should be created");

    // Original documents should still exist
    const count = await collection.countDocuments({
      _type: { $exists: false },
    });
    assertEquals(count, 2, "Original documents should remain");
  });
});

Deno.test("markAsMultiCollection - is idempotent", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    const collection = db.collection("test_catalog_2");

    // Create initial metadata (simulate already marked)
    await collection.insertMany([
      {
        _type: MULTI_COLLECTION_INFO_TYPE,
        collectionType: "catalog",
        migrationId: "mig_old",
      },
      { _type: MULTI_COLLECTION_MIGRATIONS_TYPE, migrations: [] },
      { name: "Product X" },
    ]);

    const operation: MarkAsMultiModelTypeRule = {
      type: "mark_as_multicollection",
      collectionName: "test_catalog_2",
      modelType: "catalog",
    };

    // Should not throw
    await applier.applyOperation(operation);

    // Metadata should still exist
    const infoDoc = await collection.findOne({
      _type: MULTI_COLLECTION_INFO_TYPE,
    });
    assertExists(infoDoc);

    const count = await collection.countDocuments({
      _type: { $exists: false },
    });
    assertEquals(count, 1, "Original document should remain");
  });
});

Deno.test("markAsMultiCollection - throws if collection doesn't exist", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    const operation: MarkAsMultiModelTypeRule = {
      type: "mark_as_multicollection",
      collectionName: "nonexistent_collection",
      modelType: "catalog",
    };

    await assertRejects(
      () => applier.applyOperation(operation),
      Error,
      "does not exist",
    );
  });
});

Deno.test("markAsMultiCollection - reverses by removing metadata", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    const collection = db.collection("test_catalog_3");

    // Create collection with metadata
    await collection.insertMany([
      {
        _type: MULTI_COLLECTION_INFO_TYPE,
        collectionType: "catalog",
        migrationId: "mig_test",
      },
      { _type: MULTI_COLLECTION_MIGRATIONS_TYPE, migrations: [] },
      { name: "Product Y", price: 300 },
      { name: "Product Z", price: 400 },
    ]);

    const operation: MarkAsMultiModelTypeRule = {
      type: "mark_as_multicollection",
      collectionName: "test_catalog_3",
      modelType: "catalog",
    };

    // Reverse the operation
    await applier.applyReverseOperation(operation);

    // Metadata should be removed
    const infoDoc = await collection.findOne({
      _type: MULTI_COLLECTION_INFO_TYPE,
    });
    assertEquals(infoDoc, null, "Info document should be removed");

    const migrationsDoc = await collection.findOne({
      _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
    });
    assertEquals(migrationsDoc, null, "Migrations document should be removed");

    // Original documents should remain
    const count = await collection.countDocuments({});
    assertEquals(count, 2, "Only original documents should remain");
  });
});

Deno.test("markAsMultiCollection - full apply-reverse cycle", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = new MongodbApplier(db);

    const collection = db.collection("test_catalog_4");

    // Start with regular collection
    await collection.insertMany([
      { name: "Item 1" },
      { name: "Item 2" },
    ]);

    const operation: MarkAsMultiModelTypeRule = {
      type: "mark_as_multicollection",
      collectionName: "test_catalog_4",
      modelType: "catalog",
    };

    // Apply: mark as multi-collection
    await applier.applyOperation(operation);

    let metadataCount = await collection.countDocuments({
      _type: { $exists: true },
    });
    assertEquals(metadataCount, 2, "Should have 2 metadata docs after apply");

    // Reverse: remove marking
    await applier.applyReverseOperation(operation);

    metadataCount = await collection.countDocuments({
      _type: { $exists: true },
    });
    assertEquals(
      metadataCount,
      0,
      "Should have no metadata docs after reverse",
    );

    const dataCount = await collection.countDocuments({});
    assertEquals(dataCount, 2, "Original data should be intact");
  });
});

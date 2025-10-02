/**
 * @fileoverview Advanced Multi-Collection Tests
 * 
 * Tests critical scenarios that were missing:
 * 1. Multi-collection transforms with real MongoDB
 * 2. Multi-collection transform rollback
 * 3. Version tracking in practice
 * 4. Edge cases and error handling
 */

import { assertEquals, assertExists, assert } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import {
  createMultiCollectionInfo,
  getMultiCollectionInfo,
  discoverMultiCollectionInstances,
  shouldInstanceReceiveMigration,
} from "../../src/migration/multicollection-registry.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import type { 
  TransformMultiCollectionTypeRule,
  CreateMultiCollectionInstanceRule,
  SeedMultiCollectionInstanceRule,
} from "../../src/migration/types.ts";

// ============================================================================
// CRITICAL TEST 1: Multi-Collection Transform with Real MongoDB
// ============================================================================

Deno.test("MongodbApplier - transforms ALL instances of a type", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = createMongodbApplier(db);

    // Create 3 instances of the same type
    await createMultiCollectionInfo(db, "blog_comments", "comments", "mig_001");
    await createMultiCollectionInfo(db, "forum_comments", "comments", "mig_001");
    await createMultiCollectionInfo(db, "main_comments", "comments", "mig_001");

    // Seed data in all instances
    await db.collection("blog_comments").insertOne({
      _type: "user_comment",
      content: "Blog comment",
      author: "Alice",
    });
    await db.collection("forum_comments").insertOne({
      _type: "user_comment",
      content: "Forum comment",
      author: "Bob",
    });
    await db.collection("main_comments").insertOne({
      _type: "user_comment",
      content: "Main comment",
      author: "Charlie",
    });

    // Transform should affect ALL instances
    const operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "comments",
      typeName: "user_comment",
      up: (doc) => ({
        ...doc,
        likes: 0,
      }),
      down: (doc) => {
        const { likes: _likes, ...rest } = doc;
        return rest;
      },
    };

    await applier.applyOperation(operation);

    // Verify ALL instances were transformed
    const blogDoc = await db.collection("blog_comments").findOne({ _type: "user_comment" });
    const forumDoc = await db.collection("forum_comments").findOne({ _type: "user_comment" });
    const mainDoc = await db.collection("main_comments").findOne({ _type: "user_comment" });

    assertExists(blogDoc);
    assertExists(forumDoc);
    assertExists(mainDoc);

    assertEquals(blogDoc.likes, 0);
    assertEquals(forumDoc.likes, 0);
    assertEquals(mainDoc.likes, 0);

    // Original fields should be preserved
    assertEquals(blogDoc.content, "Blog comment");
    assertEquals(forumDoc.author, "Bob");
  });
});

Deno.test("MongodbApplier - transform handles multiple documents per instance", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = createMongodbApplier(db);

    await createMultiCollectionInfo(db, "comments", "comments", "mig_001");

    // Insert multiple documents
    await db.collection("comments").insertMany([
      { _type: "user_comment", content: "First", author: "Alice" },
      { _type: "user_comment", content: "Second", author: "Bob" },
      { _type: "user_comment", content: "Third", author: "Charlie" },
    ]);

    const operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "comments",
      typeName: "user_comment",
      up: (doc) => ({
        ...doc,
        likes: 0,
        views: 0,
      }),
      down: (doc) => {
        const { likes: _likes, views: _views, ...rest } = doc;
        return rest;
      },
    };

    await applier.applyOperation(operation);

    // All documents should be transformed
    const docs = await db.collection("comments").find({ _type: "user_comment" }).toArray();
    assertEquals(docs.length, 3);

    for (const doc of docs) {
      assertEquals(doc.likes, 0);
      assertEquals(doc.views, 0);
      assertExists(doc.content);
      assertExists(doc.author);
    }
  });
});

// ============================================================================
// CRITICAL TEST 2: Multi-Collection Transform Rollback
// ============================================================================

Deno.test("MongodbApplier - reverses multi-collection transform", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = createMongodbApplier(db);

    await createMultiCollectionInfo(db, "comments", "comments", "mig_001");

    // Insert document with the "new" field (already transformed)
    await db.collection("comments").insertOne({
      _type: "user_comment",
      content: "Test",
      author: "Alice",
      likes: 5,
    });

    const operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "comments",
      typeName: "user_comment",
      up: (doc) => ({
        ...doc,
        likes: 0,
      }),
      down: (doc) => {
        const { likes: _likes, ...rest } = doc;
        return rest;
      },
    };

    // Apply reverse - should remove the 'likes' field
    await applier.applyReverseOperation(operation);

    const doc = await db.collection("comments").findOne({ _type: "user_comment" });
    assertExists(doc);

    // The 'likes' field should be removed
    assert(!("likes" in doc));

    // Other fields should be preserved
    assertEquals(doc.content, "Test");
    assertEquals(doc.author, "Alice");
  });
});

Deno.test("MongodbApplier - reverses transform across multiple instances", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = createMongodbApplier(db);

    // Create 2 instances
    await createMultiCollectionInfo(db, "blog_comments", "comments", "mig_001");
    await createMultiCollectionInfo(db, "forum_comments", "comments", "mig_001");

    // Both have transformed data
    await db.collection("blog_comments").insertOne({
      _type: "user_comment",
      content: "Blog",
      likes: 10,
    });
    await db.collection("forum_comments").insertOne({
      _type: "user_comment",
      content: "Forum",
      likes: 20,
    });

    const operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "comments",
      typeName: "user_comment",
      up: (doc) => ({
        ...doc,
        likes: 0,
      }),
      down: (doc) => {
        const { likes: _likes, ...rest } = doc;
        return rest;
      },
    };

    // Reverse should affect both instances
    await applier.applyReverseOperation(operation);

    const blogDoc = await db.collection("blog_comments").findOne({});
    const forumDoc = await db.collection("forum_comments").findOne({});

    assert(!("likes" in blogDoc!));
    assert(!("likes" in forumDoc!));
  });
});

// ============================================================================
// CRITICAL TEST 3: Version Tracking in Practice
// ============================================================================

Deno.test("MongodbApplier - respects version tracking during transform", async (t) => {
  await withDatabase(t.name, async (db) => {
    const _applier = createMongodbApplier(db);

    // Old instance (created at mig_001)
    await createMultiCollectionInfo(db, "comments_old", "comments", "mig_001");
    await db.collection("comments_old").insertOne({
      _type: "user_comment",
      content: "Old comment",
    });

    // New instance (created at mig_003 - after the transform migration)
    await createMultiCollectionInfo(db, "comments_new", "comments", "mig_003");
    await db.collection("comments_new").insertOne({
      _type: "user_comment",
      content: "New comment",
      likes: 0, // Already has the field!
    });

    // Transform in mig_002
    const _operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "comments",
      typeName: "user_comment",
      up: (doc) => ({
        ...doc,
        likes: 0,
      }),
      down: (doc) => {
        const { likes: _likes, ...rest } = doc;
        return rest;
      },
    };

    // Check version tracking logic
    const oldShouldReceive = await shouldInstanceReceiveMigration(
      db,
      "comments_old",
      "mig_002"
    );
    const newShouldReceive = await shouldInstanceReceiveMigration(
      db,
      "comments_new",
      "mig_002"
    );

    // Old instance should receive the migration
    assertEquals(oldShouldReceive, true);

    // New instance should NOT receive (created after mig_002)
    assertEquals(newShouldReceive, false);
  });
});

Deno.test("MongodbApplier - handles mixed versions gracefully", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create instances at different points in time
    await createMultiCollectionInfo(db, "v1_comments", "comments", "2024_01_01_0000_A@initial");
    await createMultiCollectionInfo(db, "v2_comments", "comments", "2024_06_01_0000_B@mid");
    await createMultiCollectionInfo(db, "v3_comments", "comments", "2024_12_01_0000_C@late");

    // Seed data
    await db.collection("v1_comments").insertOne({ _type: "user_comment", content: "V1" });
    await db.collection("v2_comments").insertOne({ _type: "user_comment", content: "V2" });
    await db.collection("v3_comments").insertOne({ _type: "user_comment", content: "V3" });

    // Migration at 2024_09_01
    const migration_id = "2024_09_01_0000_M@transform";

    // Check who should receive
    const v1Should = await shouldInstanceReceiveMigration(db, "v1_comments", migration_id);
    const v2Should = await shouldInstanceReceiveMigration(db, "v2_comments", migration_id);
    const v3Should = await shouldInstanceReceiveMigration(db, "v3_comments", migration_id);

    // v1 and v2 created before migration → should receive
    assertEquals(v1Should, true);
    assertEquals(v2Should, true);

    // v3 created after migration → should skip
    assertEquals(v3Should, false);
  });
});

// ============================================================================
// Edge Cases: Error Handling
// ============================================================================

Deno.test("MongodbApplier - handles transform on non-existent type gracefully", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = createMongodbApplier(db);

    await createMultiCollectionInfo(db, "comments", "comments", "mig_001");

    // Insert data with different type
    await db.collection("comments").insertOne({
      _type: "admin_comment", // Different type!
      content: "Admin only",
    });

    // Try to transform non-existent type
    const operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "comments",
      typeName: "user_comment", // This type doesn't exist in DB
      up: (doc) => ({
        ...doc,
        likes: 0,
      }),
      down: (doc) => {
        const { likes: _likes, ...rest } = doc;
        return rest;
      },
    };

    // Should complete without error (no matching documents)
    await applier.applyOperation(operation);

    // Admin comment should be unchanged
    const doc = await db.collection("comments").findOne({ _type: "admin_comment" });
    assertExists(doc);
    assert(!("likes" in doc));
  });
});

Deno.test("MongodbApplier - handles empty instance collection", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = createMongodbApplier(db);

    // Create instance but don't insert any documents
    await createMultiCollectionInfo(db, "empty_comments", "comments", "mig_001");

    const operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "comments",
      typeName: "user_comment",
      up: (doc) => ({
        ...doc,
        likes: 0,
      }),
      down: (doc) => {
        const { likes: _likes, ...rest } = doc;
        return rest;
      },
    };

    // Should complete without error
    await applier.applyOperation(operation);

    // Verify collection still has metadata
    const info = await getMultiCollectionInfo(db, "empty_comments");
    assertExists(info);
  });
});

Deno.test("MongodbApplier - transform preserves metadata documents", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = createMongodbApplier(db);

    await createMultiCollectionInfo(db, "comments", "comments", "mig_001");

    await db.collection("comments").insertOne({
      _type: "user_comment",
      content: "Test",
    });

    const operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "comments",
      typeName: "user_comment",
      up: (doc) => ({
        ...doc,
        likes: 0,
      }),
      down: (doc) => {
        const { likes: _likes, ...rest } = doc;
        return rest;
      },
    };

    await applier.applyOperation(operation);

    // Metadata should still exist
    const info = await getMultiCollectionInfo(db, "comments");
    assertExists(info);
    assertEquals(info.collectionType, "comments");
  });
});

// ============================================================================
// Edge Cases: Multiple Types
// ============================================================================

Deno.test("MongodbApplier - transforms only specified type, not others", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = createMongodbApplier(db);

    await createMultiCollectionInfo(db, "catalog", "catalog", "mig_001");

    // Insert different types
    await db.collection("catalog").insertMany([
      { _type: "product", name: "Widget", price: 10 },
      { _type: "book", title: "Guide", pages: 100 },
      { _type: "service", name: "Consulting", rate: 50 },
    ]);

    // Transform only 'product' type
    const operation: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "catalog",
      typeName: "product",
      up: (doc) => ({
        ...doc,
        inStock: true,
      }),
      down: (doc) => {
        const { inStock: _inStock, ...rest } = doc;
        return rest;
      },
    };

    await applier.applyOperation(operation);

    const product = await db.collection("catalog").findOne({ _type: "product" });
    const book = await db.collection("catalog").findOne({ _type: "book" });
    const service = await db.collection("catalog").findOne({ _type: "service" });

    // Product should have the new field
    assertExists(product);
    assertEquals(product.inStock, true);

    // Book and service should NOT have the new field
    assertExists(book);
    assertExists(service);
    assert(!("inStock" in book));
    assert(!("inStock" in service));
  });
});

// ============================================================================
// Edge Cases: Discovery
// ============================================================================

Deno.test("MongodbApplier - discovers instances correctly", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create multiple instances of different types
    await createMultiCollectionInfo(db, "blog_comments", "comments", "mig_001");
    await createMultiCollectionInfo(db, "forum_comments", "comments", "mig_001");
    await createMultiCollectionInfo(db, "catalog_main", "catalog", "mig_001");

    // Discover comments instances
    const commentInstances = await discoverMultiCollectionInstances(db, "comments");
    assertEquals(commentInstances.length, 2);
    assert(commentInstances.includes("blog_comments"));
    assert(commentInstances.includes("forum_comments"));

    // Discover catalog instances
    const catalogInstances = await discoverMultiCollectionInstances(db, "catalog");
    assertEquals(catalogInstances.length, 1);
    assertEquals(catalogInstances[0], "catalog_main");
  });
});

// ============================================================================
// Edge Cases: Seed Operations
// ============================================================================

Deno.test("MongodbApplier - seed type adds _type automatically", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = createMongodbApplier(db);

    const createOp: CreateMultiCollectionInstanceRule = {
      type: "create_multicollection_instance",
      collectionName: "comments",
      collectionType: "comments",
    };

    await applier.applyOperation(createOp);

    const seedOp: SeedMultiCollectionInstanceRule = {
      type: "seed_multicollection_instance",
      collectionName: "comments",
      typeName: "user_comment",
      documents: [
        { content: "First", author: "Alice" },
        { content: "Second", author: "Bob" },
      ],
    };

    await applier.applyOperation(seedOp);

    // Documents should have _type field added
    const docs = await db.collection("comments").find({ _type: "user_comment" }).toArray();
    assertEquals(docs.length, 2);

    for (const doc of docs) {
      assertEquals(doc._type, "user_comment");
      assertExists(doc.content);
      assertExists(doc.author);
    }
  });
});

// ============================================================================
// Integration: Full Lifecycle
// ============================================================================

Deno.test("MongodbApplier - full multi-collection lifecycle", async (t) => {
  await withDatabase(t.name, async (db) => {
    const applier = createMongodbApplier(db);

    // 1. Create instance
    const createOp: CreateMultiCollectionInstanceRule = {
      type: "create_multicollection_instance",
      collectionName: "main_catalog",
      collectionType: "catalog",
    };
    await applier.applyOperation(createOp);

    // 2. Seed data
    const seedOp: SeedMultiCollectionInstanceRule = {
      type: "seed_multicollection_instance",
      collectionName: "main_catalog",
      typeName: "product",
      documents: [
        { name: "Widget", price: 10 },
        { name: "Gadget", price: 20 },
      ],
    };
    await applier.applyOperation(seedOp);

    // 3. Transform
    const transformOp: TransformMultiCollectionTypeRule = {
      type: "transform_multicollection_type",
      collectionType: "catalog",
      typeName: "product",
      up: (doc) => {
        const product = doc as { price: number };
        return {
          ...doc,
          priceWithTax: product.price * 1.2,
        };
      },
      down: (doc) => {
        const { priceWithTax: _priceWithTax, ...rest } = doc;
        return rest;
      },
    };
    await applier.applyOperation(transformOp);

    // Verify final state
    const products = await db.collection("main_catalog")
      .find({ _type: "product" })
      .toArray();

    assertEquals(products.length, 2);
    assertEquals(products[0].priceWithTax, 12);
    assertEquals(products[1].priceWithTax, 24);

    // 4. Rollback transform
    await applier.applyReverseOperation(transformOp);

    const productsAfterRollback = await db.collection("main_catalog")
      .find({ _type: "product" })
      .toArray();

    assert(!("priceWithTax" in productsAfterRollback[0]));
    assert(!("priceWithTax" in productsAfterRollback[1]));
  });
});


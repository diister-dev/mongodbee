import * as v from "../../src/schema.ts";
import { assertEquals, assertRejects } from "@std/assert";
import {
  multiCollection,
  newMultiCollection,
  createMultiCollectionInstance,
  discoverMultiCollectionInstances,
  getMultiCollectionInfo,
  getMultiCollectionMigrations,
  markAsMultiCollection,
  multiCollectionInstanceExists,
} from "../../src/multi-collection.ts";
import { collection } from "../../src/collection.ts";
import { withDatabase } from "../+shared.ts";
import assert from "node:assert";
import { defineModel } from "../../src/multi-collection-model.ts";
import { getSessionContext } from "../../src/session.ts";

// Test schemas
const catalogModel = defineModel("catalog", {
  schema: {
    product: {
      name: v.string(),
      price: v.number(),
      stock: v.number(),
    },
    category: {
      name: v.string(),
      description: v.optional(v.string()),
    },
  },
});

const userSchema = {
  name: v.string(),
  email: v.string(),
};

Deno.test("Create multiCollection in session: Basic creation and insert", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Get session context directly from the client
    const { withSession } = getSessionContext(db.client);

    // Use the session to create and populate a multiCollection
    const result = await withSession(async () => {
      // Create a new multiCollection within the session
      const catalog = await multiCollection(db, "catalog_store1", catalogModel);

      // Insert data in the same session
      const categoryId = await catalog.insertOne("category", {
        name: "Electronics",
        description: "Electronic devices",
      });

      const productId = await catalog.insertOne("product", {
        name: "Laptop",
        price: 999.99,
        stock: 10,
      });

      return { categoryId, productId };
    });

    // Verify data was committed
    const catalog = await multiCollection(db, "catalog_store1", catalogModel);

    const category = await catalog.findOne("category", { _id: result.categoryId });
    assert(category !== null);
    assertEquals(category.name, "Electronics");

    const product = await catalog.findOne("product", { _id: result.productId });
    assert(product !== null);
    assertEquals(product.name, "Laptop");
    assertEquals(product.price, 999.99);
  });
});

Deno.test("Create multiCollection in session: Rollback on error", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);

    // Try to create and populate, then fail
    await assertRejects(
      async () => {
        await withSession(async () => {
          // Create a new multiCollection
          const catalog = await multiCollection(db, "catalog_rollback", catalogModel);

          // Insert some data
          await catalog.insertOne("category", {
            name: "Will be rolled back",
          });

          await catalog.insertOne("product", {
            name: "Will also be rolled back",
            price: 100,
            stock: 5,
          });

          // Throw error to trigger rollback
          throw new Error("Intentional error for rollback test");
        });
      },
      Error,
      "Intentional error for rollback test",
    );

    // Verify collection exists but has no data (rollback)
    // Note: The collection itself might exist due to metadata creation
    const catalog = await multiCollection(db, "catalog_rollback", catalogModel);

    const categories = await catalog.find("category");
    const products = await catalog.find("product");

    // Products and categories should be empty due to rollback
    // Note: metadata documents (_information, _migrations) are created outside the transaction
    assertEquals(categories.length, 0, "Categories should be empty after rollback");
    assertEquals(products.length, 0, "Products should be empty after rollback");
  });
});

Deno.test("Create multiCollection in session: Multiple collections in same session", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);

    const result = await withSession(async () => {
      // Create multiple multiCollections in the same session
      const store1 = await multiCollection(db, "catalog_store1", catalogModel);
      const store2 = await multiCollection(db, "catalog_store2", catalogModel);

      // Also create a regular collection
      const users = await collection(db, "users", userSchema);

      // Insert data across all collections
      const userId = await users.insertOne({
        name: "Test User",
        email: "test@example.com",
      });

      const product1Id = await store1.insertOne("product", {
        name: "Product in Store 1",
        price: 50,
        stock: 100,
      });

      const product2Id = await store2.insertOne("product", {
        name: "Product in Store 2",
        price: 75,
        stock: 50,
      });

      return { userId, product1Id, product2Id };
    });

    // Verify all data
    const users = await collection(db, "users", userSchema);
    const user = await users.getById(result.userId);
    assertEquals(user.name, "Test User");

    const store1 = await multiCollection(db, "catalog_store1", catalogModel);
    const product1 = await store1.findOne("product", { _id: result.product1Id });
    assert(product1 !== null);
    assertEquals(product1.name, "Product in Store 1");

    const store2 = await multiCollection(db, "catalog_store2", catalogModel);
    const product2 = await store2.findOne("product", { _id: result.product2Id });
    assert(product2 !== null);
    assertEquals(product2.name, "Product in Store 2");
  });
});

Deno.test("Create multiCollection in session: Using collection's withSession", async (t) => {
  await withDatabase(t.name, async (db) => {
    // First create an existing collection to get withSession from
    const users = await collection(db, "users", userSchema);

    // Use the collection's withSession to create new multiCollections
    const result = await users.withSession(async () => {
      // Insert a user first
      const userId = await users.insertOne({
        name: "Session Owner",
        email: "owner@example.com",
      });

      // Create a new multiCollection in the same session
      // Works because default is now "managed" (no auto-apply of validators/indexes)
      const catalog = await multiCollection(db, "catalog_from_session", catalogModel);

      // Insert data in the new collection
      const productId = await catalog.insertOne("product", {
        name: "Session Product",
        price: 199.99,
        stock: 25,
      });

      // Read back within the same session
      const product = await catalog.findOne("product", { _id: productId });
      assert(product !== null);
      assertEquals(product.name, "Session Product");

      return { userId, productId };
    });

    // Verify outside the session
    const catalog = await multiCollection(db, "catalog_from_session", catalogModel);
    const product = await catalog.findOne("product", { _id: result.productId });
    assert(product !== null);
    assertEquals(product.price, 199.99);
  });
});

Deno.test("Create multiCollection in session: Nested withSession calls", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);

    const result = await withSession(async () => {
      // Create first multiCollection
      const store1 = await multiCollection(db, "store1", catalogModel);

      const cat1Id = await store1.insertOne("category", {
        name: "Category 1",
      });

      // Nested withSession - should reuse the same session
      const nestedResult = await store1.withSession(async () => {
        // Create another multiCollection in nested session
        const store2 = await multiCollection(db, "store2", catalogModel);

        const cat2Id = await store2.insertOne("category", {
          name: "Category 2",
        });

        return { cat2Id };
      });

      return { cat1Id, cat2Id: nestedResult.cat2Id };
    });

    // Verify both were committed
    const store1 = await multiCollection(db, "store1", catalogModel);
    const cat1 = await store1.findOne("category", { _id: result.cat1Id });
    assert(cat1 !== null);
    assertEquals(cat1.name, "Category 1");

    const store2 = await multiCollection(db, "store2", catalogModel);
    const cat2 = await store2.findOne("category", { _id: result.cat2Id });
    assert(cat2 !== null);
    assertEquals(cat2.name, "Category 2");
  });
});

Deno.test("Create multiCollection in session: With schemaManagement auto (explicit)", async (t) => {
  await withDatabase(t.name, async (db) => {
    // When using schemaManagement: "auto" explicitly, validators/indexes are applied
    // This can cause issues in transactions, so collections should be created BEFORE the session

    // Create the collection OUTSIDE the session with auto-apply
    const catalog = await multiCollection(db, "catalog_auto", catalogModel, {
      schemaManagement: "auto",
    });

    const { withSession } = getSessionContext(db.client);

    // Now use it inside a session for data operations
    const result = await withSession(async () => {
      const productId = await catalog.insertOne("product", {
        name: "Auto Product",
        price: 299.99,
        stock: 15,
      });

      return { productId };
    });

    // Verify data
    const product = await catalog.findOne("product", { _id: result.productId });
    assert(product !== null);
    assertEquals(product.name, "Auto Product");
  });
});

// ============================================================================
// Tests for utility functions in sessions
// ============================================================================

Deno.test("Utility functions in session: createMultiCollectionInstance", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);

    // Create instance metadata OUTSIDE session (DDL operations incompatible with transactions)
    await createMultiCollectionInstance(db, "catalog_store1", "catalog", {
      migrationId: "test-migration-001",
    });

    await createMultiCollectionInstance(db, "catalog_store2", "catalog", {
      migrationId: "test-migration-001",
    });

    // Verify instances were created, checking within a session to ensure session-aware reads work
    const result = await withSession(async () => {
      const exists1 = await multiCollectionInstanceExists(db, "catalog_store1");
      const exists2 = await multiCollectionInstanceExists(db, "catalog_store2");
      return { exists1, exists2 };
    });

    assertEquals(result.exists1, true, "catalog_store1 should exist");
    assertEquals(result.exists2, true, "catalog_store2 should exist");
  });
});

Deno.test("Utility functions in session: multiCollectionInstanceExists", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);

    // Create an instance first
    await createMultiCollectionInstance(db, "existing_catalog", "catalog");

    // Check existence in a session
    const result = await withSession(async () => {
      const exists = await multiCollectionInstanceExists(db, "existing_catalog");
      const notExists = await multiCollectionInstanceExists(db, "non_existing_catalog");

      return { exists, notExists };
    });

    assertEquals(result.exists, true, "existing_catalog should exist");
    assertEquals(result.notExists, false, "non_existing_catalog should not exist");
  });
});

Deno.test("Utility functions in session: discoverMultiCollectionInstances", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create multiple instances OUTSIDE session (DDL operations incompatible with transactions)
    await createMultiCollectionInstance(db, "catalog_paris", "catalog");
    await createMultiCollectionInstance(db, "catalog_lyon", "catalog");
    await createMultiCollectionInstance(db, "catalog_marseille", "catalog");
    await createMultiCollectionInstance(db, "inventory_warehouse1", "inventory");

    // Note: discoverMultiCollectionInstances uses listCollections which cannot run
    // in a transaction (MongoDB limitation), so we call it outside the session.
    // The individual findOne calls inside ARE session-aware though.
    const catalogInstances = await discoverMultiCollectionInstances(db, "catalog");
    const inventoryInstances = await discoverMultiCollectionInstances(db, "inventory");
    const unknownInstances = await discoverMultiCollectionInstances(db, "unknown");

    assertEquals(catalogInstances.length, 3, "Should find 3 catalog instances");
    assertEquals(inventoryInstances.length, 1, "Should find 1 inventory instance");
    assertEquals(unknownInstances.length, 0, "Should find 0 unknown instances");

    // Check specific instances are found
    assert(catalogInstances.includes("catalog_paris"));
    assert(catalogInstances.includes("catalog_lyon"));
    assert(catalogInstances.includes("catalog_marseille"));
  });
});

Deno.test("Utility functions in session: getMultiCollectionInfo", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);

    // Create an instance
    await createMultiCollectionInstance(db, "catalog_info_test", "catalog", {
      migrationId: "migration-v1",
    });

    // Get info in a session
    const result = await withSession(async () => {
      const info = await getMultiCollectionInfo(db, "catalog_info_test");
      const noInfo = await getMultiCollectionInfo(db, "non_existing");

      return { info, noInfo };
    });

    assert(result.info !== null, "Should get info for existing instance");
    assertEquals(result.info?.collectionType, "catalog");
    assertEquals(result.noInfo, null, "Should return null for non-existing instance");
  });
});

Deno.test("Utility functions in session: getMultiCollectionMigrations", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);

    // Create an instance with a migration ID
    await createMultiCollectionInstance(db, "catalog_migrations_test", "catalog", {
      migrationId: "migration-001",
    });

    // Get migrations in a session
    const result = await withSession(async () => {
      const migrations = await getMultiCollectionMigrations(db, "catalog_migrations_test");
      const noMigrations = await getMultiCollectionMigrations(db, "non_existing");

      return { migrations, noMigrations };
    });

    assert(result.migrations !== null, "Should get migrations for existing instance");
    assert(result.migrations?.appliedMigrations.length >= 1, "Should have at least one applied migration");
    assertEquals(result.noMigrations, null, "Should return null for non-existing instance");
  });
});

Deno.test("Utility functions in session: markAsMultiCollection", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);

    // First create a regular collection with some data
    const rawCollection = db.collection("legacy_catalog");
    await rawCollection.insertOne({ name: "Legacy Product", price: 50 });

    // Mark it as a multi-collection in a session
    await withSession(async () => {
      await markAsMultiCollection(db, "legacy_catalog", "catalog", "adoption-migration");
    });

    // Verify it's now marked
    const exists = await multiCollectionInstanceExists(db, "legacy_catalog");
    assertEquals(exists, true, "legacy_catalog should now be marked as multi-collection");

    const info = await getMultiCollectionInfo(db, "legacy_catalog");
    assert(info !== null);
    assertEquals(info.collectionType, "catalog");

    // Original data should still be there
    const doc = await rawCollection.findOne({ name: "Legacy Product" });
    assert(doc !== null, "Original data should still exist");
  });
});

Deno.test("Utility functions in session: Combined operations", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);

    // Create multiple instances OUTSIDE session (DDL operations incompatible with transactions)
    await createMultiCollectionInstance(db, "store_a", "catalog", {
      migrationId: "init-001",
    });
    await createMultiCollectionInstance(db, "store_b", "catalog", {
      migrationId: "init-001",
    });

    // Query info within a session to test session-aware reads
    const result = await withSession(async () => {
      // Check they exist
      const existsA = await multiCollectionInstanceExists(db, "store_a");
      const existsB = await multiCollectionInstanceExists(db, "store_b");

      // Get info for one
      const infoA = await getMultiCollectionInfo(db, "store_a");

      return {
        existsA,
        existsB,
        infoA,
      };
    });

    // Note: discoverMultiCollectionInstances uses listCollections which cannot run
    // in a transaction, so we call it after the session
    const instances = await discoverMultiCollectionInstances(db, "catalog");

    assertEquals(result.existsA, true);
    assertEquals(result.existsB, true);
    assertEquals(instances.length, 2);
    assert(result.infoA !== null);
    assertEquals(result.infoA?.collectionType, "catalog");
  });
});

// Test that createMultiCollectionInstance throws when called in a session
Deno.test("Utility functions in session: createMultiCollectionInstance throws in session", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { withSession } = getSessionContext(db.client);

    // Try to create instance within a session - should throw
    await assertRejects(
      async () => {
        await withSession(async () => {
          await createMultiCollectionInstance(db, "should_fail", "catalog");
        });
      },
      Error,
      "Cannot call createMultiCollectionInstance() within an active session/transaction",
    );

    // Verify nothing was created
    const exists = await multiCollectionInstanceExists(db, "should_fail");
    assertEquals(exists, false, "should_fail should not exist");
  });
});

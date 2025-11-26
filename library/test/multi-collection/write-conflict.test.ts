import * as v from "../../src/schema.ts";
import { assert, assertEquals, assertRejects } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

/**
 * Test suite for MongoDB write conflicts scenarios
 *
 * This test file validates how the library handles concurrent write operations
 * that can trigger "Write conflict during plan execution" errors in MongoDB.
 *
 * Write conflicts occur when:
 * 1. Multiple operations try to modify the same document concurrently
 * 2. Operations are executed within a transaction/session
 * 3. MongoDB detects a conflict during query plan execution
 */

const testModel = defineModel("test", {
  schema: {
    counter: {
      name: v.string(),
      value: v.number(),
      version: v.optional(v.number(), () => 0),
    },
    product: {
      name: v.string(),
      price: v.number(),
      stock: v.number(),
      version: v.optional(v.number(), () => 0),
    },
    user: {
      name: v.string(),
      email: v.string(),
      balance: v.number(),
      version: v.optional(v.number(), () => 0),
    },
  },
});

Deno.test({
  name: "Write Conflict: Concurrent updateOne on same document with Promise.all",
  sanitizeOps: false,  // Disable sanitizer due to expected timer leaks from concurrent retries
  sanitizeResources: false,
  fn: async (t) => {
  await withDatabase(t.name, async (db) => {
    const store = await multiCollection(db, "store", testModel);

    // Insert a counter document
    const counterId = await store.insertOne("counter", {
      name: "views",
      value: 0,
    });

    // Simulate multiple concurrent updates to the same document
    // This is a common pattern that can cause write conflicts
    const updatePromises = Array.from({ length: 5 }, (_, i) =>
      store.withSession(async () => {
        const current = await store.findOne("counter", { _id: counterId });
        assert(current !== null);

        // Simulate some processing time
        await new Promise((resolve) => setTimeout(resolve, Math.random() * 50));

        // Update the counter
        await store.updateOne("counter", counterId, {
          value: current.value + 1,
        });

        return i;
      })
    );

    // This will likely trigger write conflicts in a transactional environment
    // The test documents the current behavior
    try {
      await Promise.all(updatePromises);

      // If all updates succeed, verify final state
      const finalCounter = await store.findOne("counter", { _id: counterId });
      assert(finalCounter !== null);
      console.log(`Final counter value: ${finalCounter.value} (expected: 5)`);

      // Note: Due to race conditions, the final value might not be 5
      // This is expected without proper conflict resolution
    } catch (error) {
      // Document the error that occurs
      console.log("Write conflict occurred (expected):", error instanceof Error ? error.message : error);

      // Verify it's a write conflict error
      if (error instanceof Error) {
        const isWriteConflict = error.message.includes("Write conflict") ||
                               error.message.includes("plan execution");
        if (isWriteConflict) {
          console.log("✓ Write conflict error detected as expected");
        }
      }
    }
  });
},
});

Deno.test({
  name: "Write Conflict: Sequential vs concurrent updates comparison",
  sanitizeOps: false,  // Disable sanitizer due to expected timer leaks from concurrent retries
  sanitizeResources: false,
  fn: async (t) => {
  await withDatabase(t.name, async (db) => {
    const store = await multiCollection(db, "store", testModel);

    // Test 1: Sequential updates (should work fine)
    const counterId1 = await store.insertOne("counter", {
      name: "sequential",
      value: 0,
    });

    for (let i = 0; i < 5; i++) {
      await store.withSession(async () => {
        const current = await store.findOne("counter", { _id: counterId1 });
        assert(current !== null);
        await store.updateOne("counter", counterId1, {
          value: current.value + 1,
        });
      });
    }

    const sequentialResult = await store.findOne("counter", { _id: counterId1 });
    assert(sequentialResult !== null);
    assertEquals(sequentialResult.value, 5, "Sequential updates should all succeed");

    // Test 2: Concurrent updates (may cause conflicts)
    const counterId2 = await store.insertOne("counter", {
      name: "concurrent",
      value: 0,
    });

    const concurrentUpdates = Array.from({ length: 5 }, () =>
      store.withSession(async () => {
        const current = await store.findOne("counter", { _id: counterId2 });
        assert(current !== null);
        await store.updateOne("counter", counterId2, {
          value: current.value + 1,
        });
      })
    );

    try {
      await Promise.all(concurrentUpdates);
      const concurrentResult = await store.findOne("counter", { _id: counterId2 });
      assert(concurrentResult !== null);
      console.log(`Concurrent result: ${concurrentResult.value} (may differ from 5)`);
    } catch (error) {
      console.log("Concurrent updates failed with:", error instanceof Error ? error.message : error);
    }
  });
},
});

Deno.test("Write Conflict: Multiple updateOne in same withSession (should work)", async (t) => {
  await withDatabase(t.name, async (db) => {
    const store = await multiCollection(db, "store", testModel);

    // Insert test data
    const userId = await store.insertOne("user", {
      name: "Alice",
      email: "alice@example.com",
      balance: 100,
    });

    const productId = await store.insertOne("product", {
      name: "Widget",
      price: 50,
      stock: 10,
    });

    // Multiple updates to DIFFERENT documents in the same session should work fine
    await store.withSession(async () => {
      // Update user
      await store.updateOne("user", userId, {
        balance: 50,
      });

      // Update product
      await store.updateOne("product", productId, {
        stock: 9,
      });

      // Another update to user (same document, but sequential in same transaction)
      await store.updateOne("user", userId, {
        balance: 45,
      });
    });

    // Verify all updates succeeded
    const finalUser = await store.findOne("user", { _id: userId });
    assert(finalUser !== null);
    assertEquals(finalUser.balance, 45);

    const finalProduct = await store.findOne("product", { _id: productId });
    assert(finalProduct !== null);
    assertEquals(finalProduct.stock, 9);
  });
});

Deno.test("Write Conflict: Nested withSession calls on same document", async (t) => {
  await withDatabase(t.name, async (db) => {
    const store = await multiCollection(db, "store", testModel);

    const userId = await store.insertOne("user", {
      name: "Bob",
      email: "bob@example.com",
      balance: 1000,
    });

    // Test nested session calls
    await store.withSession(async () => {
      // First update in outer session
      await store.updateOne("user", userId, {
        balance: 900,
      });

      // Nested session (should reuse parent session)
      await store.withSession(async () => {
        await store.updateOne("user", userId, {
          balance: 800,
        });
      });

      // Another update in outer session
      await store.updateOne("user", userId, {
        balance: 700,
      });
    });

    const finalUser = await store.findOne("user", { _id: userId });
    assert(finalUser !== null);
    assertEquals(finalUser.balance, 700);
  });
});

Deno.test("Write Conflict: UpdateMany with concurrent operations", async (t) => {
  await withDatabase(t.name, async (db) => {
    const store = await multiCollection(db, "store", testModel);

    // Insert multiple products
    const productIds = await Promise.all([
      store.insertOne("product", { name: "Product A", price: 100, stock: 10 }),
      store.insertOne("product", { name: "Product B", price: 200, stock: 20 }),
      store.insertOne("product", { name: "Product C", price: 300, stock: 30 }),
    ]);

    // Test updateMany - updates different documents, should work fine
    await store.withSession(async () => {
      await store.updateMany({
        product: {
          [productIds[0]]: { stock: 9 },
          [productIds[1]]: { stock: 19 },
          [productIds[2]]: { stock: 29 },
        },
      });
    });

    // Verify all updates
    const products = await store.find("product");
    assertEquals(products.length, 3);
    assertEquals(products.find((p) => p._id === productIds[0])?.stock, 9);
    assertEquals(products.find((p) => p._id === productIds[1])?.stock, 19);
    assertEquals(products.find((p) => p._id === productIds[2])?.stock, 29);
  });
});

Deno.test({
  name: "Write Conflict: Concurrent transactions on same document",
  sanitizeOps: false,  // Disable sanitizer due to expected timer leaks from concurrent retries
  sanitizeResources: false,
  fn: async (t) => {
  await withDatabase(t.name, async (db) => {
    const store = await multiCollection(db, "store", testModel);

    const userId = await store.insertOne("user", {
      name: "Charlie",
      email: "charlie@example.com",
      balance: 500,
    });

    // Start two concurrent transactions that both try to update the same user
    const transaction1 = store.withSession(async () => {
      const user = await store.findOne("user", { _id: userId });
      assert(user !== null);

      // Simulate some processing time
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Transaction 1: Deduct 100
      await store.updateOne("user", userId, {
        balance: user.balance - 100,
      });

      return "transaction1";
    });

    const transaction2 = store.withSession(async () => {
      const user = await store.findOne("user", { _id: userId });
      assert(user !== null);

      // Simulate some processing time
      await new Promise((resolve) => setTimeout(resolve, 30));

      // Transaction 2: Deduct 200
      await store.updateOne("user", userId, {
        balance: user.balance - 200,
      });

      return "transaction2";
    });

    try {
      // Try to run both concurrently
      const results = await Promise.all([transaction1, transaction2]);
      console.log("Both transactions succeeded:", results);

      // Check final balance (may be inconsistent due to race condition)
      const finalUser = await store.findOne("user", { _id: userId });
      assert(finalUser !== null);
      console.log(`Final balance: ${finalUser.balance} (original: 500)`);

      // The expected behavior depends on transaction isolation
    } catch (error) {
      console.log("One or both transactions failed:", error instanceof Error ? error.message : error);

      // This is expected - concurrent transactions on the same document can conflict
      if (error instanceof Error) {
        const isWriteConflict = error.message.includes("Write conflict") ||
                               error.message.includes("plan execution");
        if (isWriteConflict) {
          console.log("✓ Write conflict detected in concurrent transactions (expected)");
        }
      }
    }
  });
},
});

Deno.test("Write Conflict: Rapid fire updates without sessions", async (t) => {
  await withDatabase(t.name, async (db) => {
    const store = await multiCollection(db, "store", testModel);

    const counterId = await store.insertOne("counter", {
      name: "rapid",
      value: 0,
    });

    // Rapid updates to the same document
    // Note: These still use sessions internally via sessionContext.getSession()
    const updates = Array.from({ length: 10 }, (_, i) =>
      store.updateOne("counter", counterId, {
        value: i + 1,
      })
    );

    try {
      await Promise.all(updates);

      const finalCounter = await store.findOne("counter", { _id: counterId });
      assert(finalCounter !== null);
      console.log(`Rapid fire final value: ${finalCounter.value}`);
    } catch (error) {
      console.log("Rapid fire updates failed:", error instanceof Error ? error.message : error);
    }
  });
});

Deno.test("Write Conflict: UpdateOne with version field (optimistic locking pattern)", async (t) => {
  await withDatabase(t.name, async (db) => {
    const store = await multiCollection(db, "store", testModel);

    const productId = await store.insertOne("product", {
      name: "Versioned Product",
      price: 100,
      stock: 50,
      version: 1,
    });

    // Simulate optimistic locking pattern
    // Read the document with its version
    const product = await store.findOne("product", { _id: productId });
    assert(product !== null);
    const currentVersion = product.version;

    // Update with version increment
    await store.updateOne("product", productId, {
      stock: product.stock - 1,
      version: (currentVersion || 0) + 1,
    });

    // Verify version was updated
    const updatedProduct = await store.findOne("product", { _id: productId });
    assert(updatedProduct !== null);
    assertEquals(updatedProduct.version, 2);
    assertEquals(updatedProduct.stock, 49);
  });
});

Deno.test("Write Conflict: Stress test with many concurrent operations", async (t) => {
  await withDatabase(t.name, async (db) => {
    const store = await multiCollection(db, "store", testModel);

    // Create multiple counters
    const counterIds = await Promise.all(
      Array.from({ length: 3 }, (_, i) =>
        store.insertOne("counter", {
          name: `counter_${i}`,
          value: 0,
        })
      )
    );

    // Generate lots of concurrent updates across multiple documents
    const updates = Array.from({ length: 30 }, (_, i) => {
      const counterId = counterIds[i % counterIds.length];
      return store.withSession(async () => {
        const counter = await store.findOne("counter", { _id: counterId });
        assert(counter !== null);

        await store.updateOne("counter", counterId, {
          value: counter.value + 1,
        });
      });
    });

    let successCount = 0;
    let failureCount = 0;

    // Run all updates concurrently
    const results = await Promise.allSettled(updates);

    results.forEach((result) => {
      if (result.status === "fulfilled") {
        successCount++;
      } else {
        failureCount++;
        if (result.reason instanceof Error) {
          const isWriteConflict = result.reason.message.includes("Write conflict") ||
                                 result.reason.message.includes("plan execution");
          if (isWriteConflict) {
            console.log("Write conflict detected in stress test");
          }
        }
      }
    });

    console.log(`Stress test results: ${successCount} succeeded, ${failureCount} failed`);

    // Verify final state
    const finalCounters = await store.find("counter");
    finalCounters.forEach((counter) => {
      console.log(`Counter ${counter.name}: ${counter.value}`);
    });
  });
});

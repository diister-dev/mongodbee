import * as v from "../src/schema.ts";
import { test, expect } from "vitest";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";

/**
 * Test suite for write conflict retry logic in simple collections
 *
 * This validates that the retry mechanism works for collection.ts,
 * not just multi-collection.ts
 */

const counterSchema = {
  name: v.string(),
  value: v.number(),
  lastUpdated: v.string(),
};

const productSchema = {
  name: v.string(),
  price: v.number(),
  stock: v.number(),
};

test("Collection Write Conflict: Sequential updates should work", async () => {
  await withDatabase("Collection Write Conflict: Sequential updates should work", async (db) => {
    const counters = await collection(db, "counters", counterSchema);

    // Insert a counter
    const counterId = await counters.insertOne({
      name: "test_counter",
      value: 0,
      lastUpdated: new Date().toISOString(),
    });

    // Perform sequential updates
    for (let i = 0; i < 10; i++) {
      const counter = await counters.getById(counterId);
      await counters.updateOne(
        { _id: counterId },
        {
          $set: {
            value: counter.value + 1,
            lastUpdated: new Date().toISOString(),
          },
        }
      );
    }

    const finalCounter = await counters.getById(counterId);
    expect(finalCounter.value).toEqual(10);
  });
});

test("Collection Write Conflict: Concurrent updates with retry", async () => {
  await withDatabase("Collection Write Conflict: Concurrent updates with retry", async (db) => {
    const counters = await collection(db, "counters", counterSchema);

    const counterId = await counters.insertOne({
      name: "concurrent_test",
      value: 0,
      lastUpdated: new Date().toISOString(),
    });

    // Create multiple concurrent update promises
    const updates = Array.from({ length: 5 }, (_, i) =>
      (async () => {
        const counter = await counters.getById(counterId);

        // Small random delay to create some overlap
        await new Promise((resolve) => setTimeout(resolve, Math.random() * 20));

        await counters.updateOne(
          { _id: counterId },
          {
            $set: {
              value: counter.value + 1,
              lastUpdated: new Date().toISOString(),
            },
          }
        );
      })()
    );

    // Execute all updates concurrently
    await Promise.all(updates);

    const finalCounter = await counters.getById(counterId);
    console.log(`Concurrent updates final value: ${finalCounter.value}`);
    expect(finalCounter.value >= 1).toBeTruthy();
  });
});

test("Collection Write Conflict: Rapid fire updates", async () => {
  await withDatabase("Collection Write Conflict: Rapid fire updates", async (db) => {
    const products = await collection(db, "products", productSchema);

    const productId = await products.insertOne({
      name: "Hot Product",
      price: 100,
      stock: 100,
    });

    // Rapid updates with different values
    const stockUpdates = [95, 90, 85, 80, 75].map((newStock, index) =>
      (async () => {
        // Stagger slightly
        await new Promise((resolve) => setTimeout(resolve, index * 10));

        await products.updateOne(
          { _id: productId },
          { $set: { stock: newStock } }
        );
      })()
    );

    // All should complete without errors
    await Promise.all(stockUpdates);

    const finalProduct = await products.getById(productId);
    console.log(`Rapid fire final stock: ${finalProduct.stock}`);
    expect(
      [75, 80, 85, 90, 95].includes(finalProduct.stock),
    ).toBeTruthy();
  });
});

test("Collection Write Conflict: UpdateMany with retry", async () => {
  await withDatabase("Collection Write Conflict: UpdateMany with retry", async (db) => {
    const products = await collection(db, "products", productSchema);

    // Insert multiple products
    const productIds = await Promise.all([
      products.insertOne({ name: "Product A", price: 100, stock: 10 }),
      products.insertOne({ name: "Product B", price: 200, stock: 20 }),
      products.insertOne({ name: "Product C", price: 300, stock: 30 }),
    ]);

    // Update all products at once
    const result = await products.updateMany(
      { _id: { $in: productIds } } as any,
      { $inc: { stock: -1 } }
    );

    expect(result.modifiedCount).toEqual(3);

    // Verify updates
    const allProducts = await products.find({});
    allProducts.forEach((product) => {
      const originalStock = productIds.indexOf(product._id as string) === 0
        ? 10
        : productIds.indexOf(product._id as string) === 1
        ? 20
        : 30;

      expect(product.stock < originalStock).toBeTruthy();
    });
  });
});

test("Collection Write Conflict: Simple sequential updates never fail", async () => {
  await withDatabase("Collection Write Conflict: Simple sequential updates never fail", async (db) => {
    const products = await collection(db, "products", productSchema);

    const productId = await products.insertOne({
      name: "Simple Product",
      price: 50,
      stock: 100,
    });

    // 20 sequential updates
    for (let i = 0; i < 20; i++) {
      await products.updateOne(
        { _id: productId },
        { $set: { stock: 100 - i - 1 } }
      );
    }

    const finalProduct = await products.getById(productId);
    expect(finalProduct.stock).toEqual(80);
  });
});

test("Collection Write Conflict: WithSession protects grouped operations", async () => {
  await withDatabase("Collection Write Conflict: WithSession protects grouped operations", async (db) => {
    const products = await collection(db, "products", productSchema);
    const counters = await collection(db, "counters", counterSchema);

    const productId = await products.insertOne({
      name: "Transactional Product",
      price: 100,
      stock: 50,
    });

    const counterId = await counters.insertOne({
      name: "sales_counter",
      value: 0,
      lastUpdated: new Date().toISOString(),
    });

    // Use withSession to ensure atomicity
    await products.withSession(async () => {
      // Decrement stock
      await products.updateOne(
        { _id: productId },
        { $inc: { stock: -1 } }
      );

      // Increment counter
      await counters.updateOne(
        { _id: counterId },
        { $inc: { value: 1 } }
      );
    });

    const finalProduct = await products.getById(productId);
    const finalCounter = await counters.getById(counterId);

    expect(finalProduct.stock).toEqual(49);
    expect(finalCounter.value).toEqual(1);
  });
});

test("Collection Write Conflict: Mixed operations don't interfere", async () => {
  await withDatabase("Collection Write Conflict: Mixed operations don't interfere", async (db) => {
    const counters = await collection(db, "counters", counterSchema);

    const counterId = await counters.insertOne({
      name: "mixed_ops",
      value: 0,
      lastUpdated: new Date().toISOString(),
    });

    // Mix reads and writes
    const operations = [];

    // Add writes
    for (let i = 0; i < 5; i++) {
      operations.push(async () => {
        const counter = await counters.getById(counterId);
        await counters.updateOne(
          { _id: counterId },
          {
            $set: {
              value: counter.value + 1,
              lastUpdated: new Date().toISOString(),
            },
          }
        );
      });
    }

    // Add reads
    for (let i = 0; i < 5; i++) {
      operations.push(async () => {
        const counter = await counters.getById(counterId);
        return counter.value;
      });
    }

    // Shuffle
    operations.sort(() => Math.random() - 0.5);

    // Execute all
    await Promise.all(operations.map((op) => op()));

    const finalCounter = await counters.getById(counterId);
    console.log(`Mixed ops final value: ${finalCounter.value}`);
    expect(finalCounter.value >= 1).toBeTruthy();
  });
});

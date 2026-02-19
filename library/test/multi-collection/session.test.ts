import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { ulid } from "../../src/schema.ts";
import assert from "node:assert";
import { defineModel } from "../../src/multi-collection-model.ts";

// Helper function for creating test schemas
const createTestSchema = () =>
  defineModel("test", {
    schema: {
      user: {
        name: v.string(),
        email: v.string(),
        age: v.number(),
      },
      product: {
        name: v.string(),
        price: v.number(),
        stock: v.number(),
      },
      order: {
        userId: v.string(),
        productIds: v.array(v.string()),
        total: v.number(),
        status: v.string(),
      },
    },
  });

test("MultiCollection Session: Basic transaction", async () => {
  await withDatabase("MultiCollection Session: Basic transaction", async (db) => {
    const store = await multiCollection(db, "store", createTestSchema());

    // Test basic session functionality
    const results = await store.withSession(async () => {
      // Create user and product
      const userId = await store.insertOne("user", {
        name: "John Doe",
        email: "john@example.com",
        age: 30,
      });

      const productId = await store.insertOne("product", {
        name: "Test Product",
        price: 99.99,
        stock: 10,
      });

      // Create order
      const orderId = await store.insertOne("order", {
        userId,
        productIds: [productId],
        total: 99.99,
        status: "pending",
      });

      return { userId, productId, orderId };
    });

    // Verify all data was inserted correctly
    const user = await store.findOne("user", { _id: results.userId });
    expect(user).not.toBeNull();
    expect(user.name).toEqual("John Doe");

    const product = await store.findOne("product", { _id: results.productId });
    expect(product).not.toBeNull();
    expect(product.name).toEqual("Test Product");

    const order = await store.findOne("order", { _id: results.orderId });
    expect(order).not.toBeNull();
    expect(order.total).toEqual(99.99);
    expect(order.status).toEqual("pending");
  });
});

test("MultiCollection Session: Transaction rollback", async () => {
  await withDatabase("MultiCollection Session: Transaction rollback", async (db) => {
    const store = await multiCollection(db, "store", createTestSchema());

    // Insert a product first (outside transaction)
    const productId = await store.insertOne("product", {
      name: "Existing Product",
      price: 50,
      stock: 5,
    });

    // Test transaction rollback
    await expect(
      async () => {
        await store.withSession(async () => {
          // Update existing product
          await store.updateById("product", productId, {
            stock: 4,
            price: 55,
          });

          // Add user
          const userId = await store.insertOne("user", {
            name: "Transaction User",
            email: "tx@example.com",
            age: 35,
          });

          // Create an order
          await store.insertOne("order", {
            userId,
            productIds: [productId],
            total: 55,
            status: "pending",
          });

          // Throw error to trigger rollback
          throw new Error("Intentional error for rollback test");
        });
      },
    ).rejects.toThrow("Intentional error for rollback test");

    // Verify that product wasn't updated (transaction rolled back)
    const product = await store.findOne("product", { _id: productId });
    expect(product).not.toBeNull();
    expect(product.stock).toEqual(5);
    expect(
      product.price,
    ).toEqual(50);

    // Verify no user was created
    const users = await store.find("user").toArray();
    expect(users.length).toEqual(0);

    // Verify no order was created
    const orders = await store.find("order").toArray();
    expect(orders.length).toEqual(0);
  });
});

test("MultiCollection Session: Complex multi-entity update", async () => {
  await withDatabase("MultiCollection Session: Complex multi-entity update", async (db) => {
    const store = await multiCollection(db, "store", createTestSchema());

    // Setup test data
    const setupData = await store.withSession(async () => {
      const userIds = [
        await store.insertOne("user", {
          name: "User 1",
          email: "user1@example.com",
          age: 30,
        }),
        await store.insertOne("user", {
          name: "User 2",
          email: "user2@example.com",
          age: 25,
        }),
      ];

      const productIds = [
        await store.insertOne("product", {
          name: "Product A",
          price: 10,
          stock: 100,
        }),
        await store.insertOne("product", {
          name: "Product B",
          price: 20,
          stock: 50,
        }),
        await store.insertOne("product", {
          name: "Product C",
          price: 30,
          stock: 75,
        }),
      ];

      const orderIds = [
        await store.insertOne("order", {
          userId: userIds[0],
          productIds: [productIds[0], productIds[1]],
          total: 30,
          status: "pending",
        }),
        await store.insertOne("order", {
          userId: userIds[1],
          productIds: [productIds[2]],
          total: 30,
          status: "pending",
        }),
      ];

      return { userIds, productIds, orderIds };
    });

    // Test a complex transaction with updateMany and multiple entity types
    await store.withSession(async () => {
      // Update both users
      await store.updateManyByIds({
        user: {
          [setupData.userIds[0]]: { age: 31 },
          [setupData.userIds[1]]: { age: 26 },
        },
      });

      // Update product stock for ordered items
      await store.updateManyByIds({
        product: {
          [setupData.productIds[0]]: { stock: 99 }, // -1 from order
          [setupData.productIds[1]]: { stock: 49 }, // -1 from order
          [setupData.productIds[2]]: { stock: 74 }, // -1 from order
        },
      });

      // Update all orders to 'processed'
      await store.updateManyByIds({
        order: {
          [setupData.orderIds[0]]: { status: "processed" },
          [setupData.orderIds[1]]: { status: "processed" },
        },
      });
    });

    // Verify all updates were applied
    const user1 = await store.findOne("user", { _id: setupData.userIds[0] });
    expect(user1).not.toBeNull();
    expect(user1.age).toEqual(31);

    const user2 = await store.findOne("user", { _id: setupData.userIds[1] });
    expect(user2).not.toBeNull();
    expect(user2.age).toEqual(26);

    const productA = await store.findOne("product", {
      _id: setupData.productIds[0],
    });
    expect(productA).not.toBeNull();
    expect(productA.stock).toEqual(99);

    const order1 = await store.findOne("order", { _id: setupData.orderIds[0] });
    expect(order1).not.toBeNull();
    expect(
      order1.status,
    ).toEqual("processed");
  });
});

test("MultiCollection Session: Read operations in transaction", async () => {
  await withDatabase("MultiCollection Session: Read operations in transaction", async (db) => {
    const store = await multiCollection(db, "store", createTestSchema());

    // Insert test data
    const userId = await store.insertOne("user", {
      name: "Read Test User",
      email: "read@example.com",
      age: 50,
    });

    // Test read operations within a transaction
    await store.withSession(async () => {
      // Find the user
      const user = await store.findOne("user", { _id: userId });
      expect(user).not.toBeNull();
      expect(user.name).toEqual("Read Test User");

      // Test find with filter
      const users = await store.find("user", { age: 50 }).toArray();
      expect(users.length).toEqual(1);
      expect((users[0] as any).email).toEqual("read@example.com");

      // Insert another user in the same transaction
      const newUserId = await store.insertOne("user", {
        name: "Another User",
        email: "another@example.com",
        age: 45,
      });

      // Should be able to find both users
      const allUsers = await store.find("user").toArray();
      expect(allUsers.length).toEqual(2);

      // Update the first user
      await store.updateById("user", userId, { age: 51 });

      // Find should reflect the update in the same transaction
      const updatedUser = await store.findOne("user", { _id: userId });
      expect(updatedUser).not.toBeNull();
      expect(updatedUser.age).toEqual(51);
    });
  });
});

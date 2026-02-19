import * as v from "../src/schema.ts";
import { test, expect } from "vitest";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { withDatabase } from "./+shared.ts";
import { defineModel } from "../src/multi-collection-model.ts";

// Test schemas
const userSchema = {
  name: v.string(),
  email: v.string(),
  age: v.number(),
};

const orderSchema = {
  items: v.array(v.object({
    productId: v.string(),
    quantity: v.number(),
    price: v.number(),
  })),
  total: v.number(),
  status: v.string(),
};

// Multi-collection schema
const catalogSchema = {
  product: {
    name: v.string(),
    description: v.string(),
    price: v.number(),
    stock: v.number(),
    category: v.string(),
  },
  category: {
    name: v.string(),
    parentId: v.optional(v.string()),
  },
};

const modelCatalog = defineModel("catalog", { schema: catalogSchema });

test("Combined Session: Collection and Multi-Collection in same transaction", async () => {
  await withDatabase("Combined Session: Collection and Multi-Collection in same transaction", async (db) => {
    // Create regular collections
    const users = await collection(db, "users", userSchema);
    const orders = await collection(db, "orders", orderSchema);

    // Create multi-collection
    const catalog = await multiCollection(db, "catalog", modelCatalog);

    // Use a transaction across all collections
    const results = await users.withSession(async () => {
      // Create a user in the regular collection
      const userId = await users.insertOne({
        name: "Combined Test User",
        email: "combined@example.com",
        age: 35,
      });

      // Create products in the multi-collection
      const productIds = [
        await catalog.insertOne("product", {
          name: "First Product",
          description: "Product description",
          price: 25.99,
          stock: 100,
          category: "electronics",
        }),
        await catalog.insertOne("product", {
          name: "Second Product",
          description: "Another description",
          price: 35.50,
          stock: 50,
          category: "electronics",
        }),
      ];

      // Create a category in multi-collection
      const categoryId = await catalog.insertOne("category", {
        name: "Electronics",
      });

      // Create an order in the regular collection that references the products
      const orderId = await orders.insertOne({
        items: [
          { productId: productIds[0], quantity: 2, price: 25.99 },
          { productId: productIds[1], quantity: 1, price: 35.50 },
        ],
        total: 2 * 25.99 + 35.50,
        status: "pending",
      });

      return { userId, productIds, categoryId, orderId };
    });

    // Verify all entities were created correctly
    const user = await users.getById(results.userId);
    expect(user.name).toEqual("Combined Test User");

    const product = await catalog.findOne("product", {
      _id: results.productIds[0],
    });
    expect(product).not.toBeNull();
    expect(product.name).toEqual("First Product");

    const category = await catalog.findOne("category", {
      _id: results.categoryId,
    });
    expect(category).not.toBeNull();
    expect(category.name).toEqual("Electronics");

    const order = await orders.getById(results.orderId);
    expect(order.items.length).toEqual(2);
    expect(order.total).toEqual(2 * 25.99 + 35.50);
  });
});

test("Combined Session: Transaction rollback across collection types", async () => {
  await withDatabase("Combined Session: Transaction rollback across collection types", async (db) => {
    // Create regular collections
    const users = await collection(db, "users", userSchema);
    const orders = await collection(db, "orders", orderSchema);

    // Create multi-collection
    const catalog = await multiCollection(db, "catalog", modelCatalog);

    // Insert an initial product outside the transaction
    const existingProductId = await catalog.insertOne("product", {
      name: "Existing Product",
      description: "Already in database",
      price: 99.99,
      stock: 20,
      category: "misc",
    });

    // Test rollback across collection types
    await expect(
      async () => {
        await users.withSession(async () => {
          // Create a user
          await users.insertOne({
            name: "Rollback Test User",
            email: "rollback@example.com",
            age: 40,
          });

          // Update the existing product
          await catalog.updateById("product", existingProductId, {
            stock: 19,
            price: 89.99,
          });

          // Add another product
          const newProductId = await catalog.insertOne("product", {
            name: "New Product",
            description: "Will be rolled back",
            price: 49.99,
            stock: 30,
            category: "misc",
          });

          // Create an order
          await orders.insertOne({
            items: [
              { productId: existingProductId, quantity: 1, price: 89.99 },
              { productId: newProductId, quantity: 2, price: 49.99 },
            ],
            total: 89.99 + 2 * 49.99,
            status: "pending",
          });

          // Throw error to trigger rollback
          throw new Error("Intentional error for combined rollback test");
        });
      },
    ).rejects.toThrow("Intentional error for combined rollback test");

    // Verify regular collection had rollback
    const userCount = await users.countDocuments({});
    expect(userCount).toEqual(0);

    const orderCount = await orders.countDocuments({});
    expect(orderCount).toEqual(0);

    // Verify multi-collection had rollback
    const product = await catalog.findOne("product", {
      _id: existingProductId,
    });
    expect(product).not.toBeNull();
    expect(product.stock).toEqual(20);
    expect(product.price).toEqual(99.99);

    const products = await catalog.find("product").toArray();
    expect(products.length).toEqual(1);
  });
});

test("Combined Session: Update operations across collection types", async () => {
  await withDatabase("Combined Session: Update operations across collection types", async (db) => {
    // Create regular collections
    const users = await collection(db, "users", userSchema);
    const orders = await collection(db, "orders", orderSchema);

    // Create multi-collection
    const catalog = await multiCollection(db, "catalog", modelCatalog);

    // Insert initial test data
    const initialData = await users.withSession(async () => {
      // Create user
      const userId = await users.insertOne({
        name: "Update Test User",
        email: "update@example.com",
        age: 30,
      });

      // Create products
      const productId = await catalog.insertOne("product", {
        name: "Test Product",
        description: "For testing updates",
        price: 15.99,
        stock: 10,
        category: "test",
      });

      // Create order
      const orderId = await orders.insertOne({
        items: [
          { productId, quantity: 1, price: 15.99 },
        ],
        total: 15.99,
        status: "pending",
      });

      return { userId, productId, orderId };
    });

    // Test update operations in a transaction
    await catalog.withSession(async () => {
      // Update user
      await users.updateOne(
        { _id: initialData.userId },
        { $set: { age: 31 } },
      );

      // Update product in multi-collection
      await catalog.updateById("product", initialData.productId, {
        stock: 9,
        price: 16.99,
      });

      // Update order
      await orders.updateOne(
        { _id: initialData.orderId },
        {
          $set: {
            "items.0.price": 16.99,
            total: 16.99,
            status: "processed",
          },
        },
      );
    });

    // Verify all updates were applied
    const user = await users.getById(initialData.userId);
    expect(user.age).toEqual(31);

    const product = await catalog.findOne("product", {
      _id: initialData.productId,
    });
    expect(product).not.toBeNull();
    expect(product.stock).toEqual(9);
    expect(product.price).toEqual(16.99);

    const order = await orders.getById(initialData.orderId);
    expect(order.status).toEqual("processed");
    expect(order.total).toEqual(16.99);
  });
});

test("Combined Session: Shared session context", async () => {
  await withDatabase("Combined Session: Shared session context", async (db) => {
    // Create regular collections
    const users = await collection(db, "users", userSchema);
    const orders = await collection(db, "orders", orderSchema);

    // Create multi-collection
    const catalog = await multiCollection(db, "catalog", modelCatalog);

    // Test that the sessionContext is properly shared
    await users.withSession(async () => {
      // Begin a transaction from users collection

      // Insert data with both collection types
      const userId = await users.insertOne({
        name: "Shared Session User",
        email: "shared@example.com",
        age: 45,
      });

      const productId = await catalog.insertOne("product", {
        name: "Shared Session Product",
        description: "Testing shared sessions",
        price: 29.99,
        stock: 15,
        category: "test",
      });

      // Verify data is accessible within the same transaction
      const user = await users.getById(userId);
      expect(user.name).toEqual("Shared Session User");

      const product = await catalog.findOne("product", { _id: productId });
      expect(product).not.toBeNull();
      expect(product.name).toEqual("Shared Session Product");

      // Now start a "nested" session from the catalog
      await catalog.withSession(async () => {
        // This should reuse the same session, not create a new one

        // Create an order that references both
        const orderId = await orders.insertOne({
          items: [
            { productId: productId, quantity: 1, price: 29.99 },
          ],
          total: 29.99,
          status: "pending",
        });

        // Verify order creation worked
        const order = await orders.getById(orderId);
        expect(order.total).toEqual(29.99);
      });
    });
  });
});

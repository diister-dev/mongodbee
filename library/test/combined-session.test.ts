import * as v from "../src/schema.ts";
import { assertEquals, assertRejects } from "jsr:@std/assert";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { withDatabase } from "./+shared.ts";
import { getSessionContext } from "../src/session.ts";
import { ObjectId } from "mongodb";

// Test schemas
const userSchema = {
  name: v.string(),
  email: v.string(),
  age: v.number()
};

const orderSchema = {
  items: v.array(v.object({
    productId: v.string(),
    quantity: v.number(),
    price: v.number()
  })),
  total: v.number(),
  status: v.string()
};

// Multi-collection schema
const catalogSchema = {
  product: {
    name: v.string(),
    description: v.string(),
    price: v.number(),
    stock: v.number(),
    category: v.string()
  },
  category: {
    name: v.string(),
    parentId: v.optional(v.string())
  }
};

Deno.test("Combined Session: Collection and Multi-Collection in same transaction", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create regular collections
    const users = await collection(db, "users", userSchema);
    const orders = await collection(db, "orders", orderSchema);
    
    // Create multi-collection
    const catalog = await multiCollection(db, "catalog", catalogSchema);
    
    // Use a transaction across all collections
    const results = await users.withSession(async () => {
      // Create a user in the regular collection
      const userId = await users.insertOne({
        name: "Combined Test User",
        email: "combined@example.com",
        age: 35
      });
      
      // Create products in the multi-collection
      const productIds = [
        await catalog.insertOne("product", {
          name: "First Product",
          description: "Product description",
          price: 25.99,
          stock: 100,
          category: "electronics"
        }),
        await catalog.insertOne("product", {
          name: "Second Product",
          description: "Another description",
          price: 35.50,
          stock: 50,
          category: "electronics"
        })
      ];
      
      // Create a category in multi-collection
      const categoryId = await catalog.insertOne("category", {
        name: "Electronics",
        parentId: undefined,
      });
      
      // Create an order in the regular collection that references the products
      const orderId = await orders.insertOne({
        items: [
          { productId: productIds[0], quantity: 2, price: 25.99 },
          { productId: productIds[1], quantity: 1, price: 35.50 }
        ],
        total: 2 * 25.99 + 35.50,
        status: "pending"
      });
      
      return { userId, productIds, categoryId, orderId };
    });
    
    // Verify all entities were created correctly
    const user = await users.findOne({ _id: results.userId });
    assertEquals(user.name, "Combined Test User");
    
    const product = await catalog.findOne("product", { _id: results.productIds[0] });
    assertEquals(product.name, "First Product");
    
    const category = await catalog.findOne("category", { _id: results.categoryId });
    assertEquals(category.name, "Electronics");
    
    const order = await orders.findOne({ _id: results.orderId });
    assertEquals(order.items.length, 2);
    assertEquals(order.total, 2 * 25.99 + 35.50);
  });
});

Deno.test("Combined Session: Transaction rollback across collection types", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create regular collections
    const users = await collection(db, "users", userSchema);
    const orders = await collection(db, "orders", orderSchema);
    
    // Create multi-collection
    const catalog = await multiCollection(db, "catalog", catalogSchema);
    
    // Insert an initial product outside the transaction
    const existingProductId = await catalog.insertOne("product", {
      name: "Existing Product",
      description: "Already in database",
      price: 99.99,
      stock: 20,
      category: "misc"
    });
    
    // Test rollback across collection types
    await assertRejects(
      async () => {
        await users.withSession(async () => {
          // Create a user
          const userId = await users.insertOne({
            name: "Rollback Test User",
            email: "rollback@example.com",
            age: 40
          });
          
          // Update the existing product
          await catalog.updateOne("product", existingProductId, {
            stock: 19,
            price: 89.99
          });
          
          // Add another product
          const newProductId = await catalog.insertOne("product", {
            name: "New Product",
            description: "Will be rolled back",
            price: 49.99,
            stock: 30,
            category: "misc"
          });
          
          // Create an order
          await orders.insertOne({
            items: [
              { productId: existingProductId, quantity: 1, price: 89.99 },
              { productId: newProductId, quantity: 2, price: 49.99 }
            ],
            total: 89.99 + 2 * 49.99,
            status: "pending"
          });
          
          // Throw error to trigger rollback
          throw new Error("Intentional error for combined rollback test");
        });
      },
      Error,
      "Intentional error for combined rollback test"
    );
    
    // Verify regular collection had rollback
    const userCount = await users.countDocuments({});
    assertEquals(userCount, 0, "No users should exist after rollback");
    
    const orderCount = await orders.countDocuments({});
    assertEquals(orderCount, 0, "No orders should exist after rollback");
    
    // Verify multi-collection had rollback
    const product = await catalog.findOne("product", { _id: existingProductId });
    assertEquals(product.stock, 20, "Stock should be unchanged");
    assertEquals(product.price, 99.99, "Price should be unchanged");
    
    const products = await catalog.find("product");
    assertEquals(products.length, 1, "Only the original product should exist");
  });
});

Deno.test("Combined Session: Update operations across collection types", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create regular collections
    const users = await collection(db, "users", userSchema);
    const orders = await collection(db, "orders", orderSchema);
    
    // Create multi-collection
    const catalog = await multiCollection(db, "catalog", catalogSchema);
    
    // Insert initial test data
    const initialData = await users.withSession(async () => {
      // Create user
      const userId = await users.insertOne({
        name: "Update Test User",
        email: "update@example.com",
        age: 30
      });
      
      // Create products
      const productId = await catalog.insertOne("product", {
        name: "Test Product",
        description: "For testing updates",
        price: 15.99,
        stock: 10,
        category: "test"
      });
      
      // Create order
      const orderId = await orders.insertOne({
        items: [
          { productId, quantity: 1, price: 15.99 }
        ],
        total: 15.99,
        status: "pending"
      });
      
      return { userId, productId, orderId };
    });
    
    // Test update operations in a transaction
    await catalog.withSession(async () => {
      // Update user
      await users.updateOne(
        { _id: initialData.userId },
        { $set: { age: 31 } }
      );
      
      // Update product in multi-collection
      await catalog.updateOne("product", initialData.productId, {
        stock: 9,
        price: 16.99
      });
      
      // Update order
      await orders.updateOne(
        { _id: initialData.orderId },
        { 
          $set: { 
            "items.0.price": 16.99,
            total: 16.99,
            status: "processed"
          }
        }
      );
    });
    
    // Verify all updates were applied
    const user = await users.findOne({ _id: initialData.userId });
    assertEquals(user.age, 31, "User age should be updated");
    
    const product = await catalog.findOne("product", { _id: initialData.productId });
    assertEquals(product.stock, 9, "Product stock should be updated");
    assertEquals(product.price, 16.99, "Product price should be updated");
    
    const order = await orders.findOne({ _id: initialData.orderId });
    assertEquals(order.status, "processed", "Order status should be updated");
    assertEquals(order.total, 16.99, "Order total should be updated");
  });
});

Deno.test("Combined Session: Shared session context", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create regular collections
    const users = await collection(db, "users", userSchema);
    const orders = await collection(db, "orders", orderSchema);
    
    // Create multi-collection
    const catalog = await multiCollection(db, "catalog", catalogSchema);
    
    // Test that the sessionContext is properly shared
    await users.withSession(async () => {
      // Begin a transaction from users collection
      
      // Insert data with both collection types
      const userId = await users.insertOne({
        name: "Shared Session User",
        email: "shared@example.com",
        age: 45
      });
      
      const productId = await catalog.insertOne("product", {
        name: "Shared Session Product",
        description: "Testing shared sessions",
        price: 29.99,
        stock: 15,
        category: "test"
      });
      
      // Verify data is accessible within the same transaction
      const user = await users.findOne({ _id: userId });
      assertEquals(user.name, "Shared Session User");
      
      const product = await catalog.findOne("product", { _id: productId });
      assertEquals(product.name, "Shared Session Product");
      
      // Now start a "nested" session from the catalog
      await catalog.withSession(async () => {
        // This should reuse the same session, not create a new one
        
        // Create an order that references both
        const orderId = await orders.insertOne({
          items: [
            { productId: productId, quantity: 1, price: 29.99 }
          ],
          total: 29.99,
          status: "pending"
        });
        
        // Verify order creation worked
        const order = await orders.findOne({ _id: orderId });
        assertEquals(order.total, 29.99);
      });
    });
  });
});

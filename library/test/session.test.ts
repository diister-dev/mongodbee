import * as v from "../src/schema.ts";
import { assertEquals, assertRejects } from "jsr:@std/assert";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import { MongoClient, ObjectId } from "mongodb";
import { getSessionContext } from "../src/session.ts";

// Simple test schemas
const userSchema = {
  name: v.string(),
  email: v.string(),
  age: v.number()
};

const productSchema = {
  name: v.string(),
  price: v.number(),
  stock: v.number()
};

Deno.test("Session: Basic session creation and usage", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create a collection with the schema
    const users = await collection(db, "users", userSchema);
    
    // Test basic session functionality
    const userId = await users.withSession(async () => {
      const id = await users.insertOne({
        name: "John Doe",
        email: "john@example.com",
        age: 30
      });
      return id;
    });
    
    // Verify that the data was inserted correctly
    const user = await users.findOne({ _id: userId });
    assertEquals(user.name, "John Doe");
    assertEquals(user.email, "john@example.com");
    assertEquals(user.age, 30);
  });
});

Deno.test("Session: Transaction rollback on error", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create collections with schemas
    const users = await collection(db, "users", userSchema);
    const products = await collection(db, "products", productSchema);
    
    // Insert initial product
    const productId = await products.insertOne({
      name: "Test Product",
      price: 100,
      stock: 5
    });
    
    // Test transaction rollback
    await assertRejects(
      async () => {
        await users.withSession(async () => {
          // This should succeed
          await products.updateOne(
            { _id: productId },
            { $set: { stock: 4 } }
          );
          
          // Insert user 
          await users.insertOne({
            name: "Jane Doe",
            email: "jane@example.com",
            age: 25
          });
          
          // Throw an error to cause rollback
          throw new Error("Intentional error to trigger rollback");
        });
      },
      Error,
      "Intentional error to trigger rollback"
    );
    
    // Verify that the product stock wasn't updated (transaction rolled back)
    const product = await products.findOne({ _id: productId });
    assertEquals(product.stock, 5, "Transaction should have rolled back the stock update");
    
    // Verify that no user was added
    await assertRejects(
      async () => {
        await users.findOne({ name: "Jane Doe" });
      },
      Error,
      "Document not found"
    );
  });
});

Deno.test("Session: Nested transactions", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create collections with schemas
    const users = await collection(db, "users", userSchema);
    const products = await collection(db, "products", productSchema);
    
    // Test nested sessions (should use the outer session)
    const results = await users.withSession(async () => {
      // Insert a product in the outer session
      const productId = await products.insertOne({
        name: "Nested Test Product",
        price: 200,
        stock: 10
      });
      
      // Nested session should reuse the outer session
      const userId = await users.withSession(async () => {
        return await users.insertOne({
          name: "Nested User",
          email: "nested@example.com",
          age: 40
        });
      });
      
      return { productId, userId };
    });
    
    // Verify both operations succeeded
    const product = await products.findOne({ _id: results.productId });
    assertEquals(product.name, "Nested Test Product");
    
    const user = await users.findOne({ _id: results.userId });
    assertEquals(user.name, "Nested User");
  });
});

Deno.test("Session: Multiple collections in the same transaction", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create collections with schemas
    const users = await collection(db, "users", userSchema);
    const products = await collection(db, "products", productSchema);
    
    // Test using multiple collections in the same transaction
    const { userId, productId } = await users.withSession(async () => {
      const userId = await users.insertOne({
        name: "Multi Collection User",
        email: "multi@example.com",
        age: 35
      });
      
      const productId = await products.insertOne({
        name: "Multi Collection Product",
        price: 150,
        stock: 20
      });
      
      return { userId, productId };
    });
    
    // Verify both operations succeeded
    const user = await users.findOne({ _id: userId });
    assertEquals(user.name, "Multi Collection User");
    
    const product = await products.findOne({ _id: productId });
    assertEquals(product.name, "Multi Collection Product");
  });
});

Deno.test("Session: Direct session access via getSession", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create a collection with the schema
    const users = await collection(db, "users", userSchema);
    
    // Test directly accessing session context
    await users.withSession(async () => {
      // Insert a user using collection's own session handling
      const userId = await users.insertOne({
        name: "Direct Session User",
        email: "direct@example.com",
        age: 45
      });
      
      // Find the user and verify
      const user = await users.findOne({ _id: userId });
      assertEquals(user.name, "Direct Session User");
    });
  });
});

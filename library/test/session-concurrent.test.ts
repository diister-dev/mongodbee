import * as v from "../src/schema.ts";
import { assert, assertEquals, assertRejects } from "@std/assert";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { withDatabase } from "./+shared.ts";
import { ObjectId } from "mongodb";
import { defineModel } from "../src/multi-collection-model.ts";

// Simple test schemas
const userSchema = {
  name: v.string(),
  email: v.string(),
  age: v.number(),
  version: v.optional(v.number(), () => 1),
};

const productSchema = {
  name: v.string(),
  price: v.number(),
  stock: v.number(),
  version: v.optional(v.number(), () => 1),
};

// Multi-collection schema
const storeSchema = {
  product: {
    name: v.string(),
    price: v.number(),
    stock: v.number(),
    version: v.optional(v.number(), () => 1),
  },
  category: {
    name: v.string(),
    productCount: v.number(),
  },
};

const storeModel = defineModel("store", { schema: storeSchema });

/**
 * This test set validates the behavior when operations occur both inside and outside
 * of a session concurrently, ensuring that transactions properly isolate changes
 * and handle conflicts correctly.
 */

Deno.test("Session Concurrent: Regular collection operations inside and outside session", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create collections with schemas
    const users = await collection(db, "users", userSchema);
    const products = await collection(db, "products", productSchema);

    // Insert initial data outside any session
    const userId = await users.insertOne({
      name: "John Doe",
      email: "john@example.com",
      age: 30,
    });

    const productId = await products.insertOne({
      name: "Test Product",
      price: 100,
      stock: 20,
    });

    // Start a transaction that will modify data
    const sessionUpdatePromise = users.withSession(async () => {
      // Update user in session
      await users.updateOne(
        { _id: userId },
        { $set: { age: 31, version: 2 } },
      );

      // Update product in session
      await products.updateOne(
        { _id: productId },
        { $set: { stock: 19, version: 2 } },
      );

      // Read values within session should see session's changes
      const userInSession = await users.getById(userId);
      assertEquals(userInSession.age, 31);
      assertEquals(userInSession.version, 2);

      const productInSession = await products.getById(productId);
      assertEquals(productInSession.stock, 19);
      assertEquals(productInSession.version, 2);

      // Sleep to simulate long-running transaction
      await new Promise((resolve) => setTimeout(resolve, 300));

      return { userInSession, productInSession };
    });

    // While the transaction is running, make a conflicting update outside the session
    // Short delay to ensure transaction has started
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Read values outside session should still see the original values
    // until transaction is committed
    const userOutsideSession = await users.getById(userId);
    assertEquals(userOutsideSession.age, 30);
    assertEquals(userOutsideSession.version, 1);

    const productOutsideSession = await products.getById(productId);
    assertEquals(productOutsideSession.stock, 20);
    assertEquals(productOutsideSession.version, 1);

    // Make a separate update outside the session
    await products.updateOne(
      { _id: productId },
      { $set: { price: 95 } },
    );

    // Wait for transaction to complete
    const sessionResults = await sessionUpdatePromise;

    // After transaction, check final state
    const finalUser = await users.getById(userId);
    assertEquals(finalUser.age, 31);
    assertEquals(finalUser.version, 2);

    const finalProduct = await products.getById(productId);
    assertEquals(finalProduct.stock, 19);
    assertEquals(finalProduct.price, 95); // This update was made outside the transaction
    assertEquals(finalProduct.version, 2);
  });
});

Deno.test("Session Concurrent: MultiCollection operations inside and outside session", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create multi-collection
    const store = await multiCollection(db, "store", storeModel);

    // Insert initial data outside any session
    const productId = await store.insertOne("product", {
      name: "Test Product",
      price: 100,
      stock: 20,
    });

    const categoryId = await store.insertOne("category", {
      name: "Electronics",
      productCount: 1,
    });

    // Start a transaction that will modify data
    const sessionUpdatePromise = store.withSession(async () => {
      // Update product in session
      await store.updateOne("product", productId, {
        stock: 19,
        version: 2,
      });

      // Update category in session
      await store.updateOne("category", categoryId, {
        productCount: 2, // Simulating adding a product
      });

      // Read values within session should see session's changes
      const productInSession = await store.findOne("product", {
        _id: productId,
      });
      assert(productInSession !== null);
      assertEquals(productInSession.stock, 19);
      assertEquals(productInSession.version, 2);

      const categoryInSession = await store.findOne("category", {
        _id: categoryId,
      });
      assert(categoryInSession !== null);
      assertEquals(categoryInSession.productCount, 2);

      // Sleep to simulate long-running transaction
      await new Promise((resolve) => setTimeout(resolve, 300));

      return { productInSession, categoryInSession };
    });

    // While the transaction is running, read and update outside the session
    // Short delay to ensure transaction has started
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Read values outside session should still see the original values
    // until transaction is committed
    const productOutsideSession = await store.findOne("product", {
      _id: productId,
    });
    assert(productOutsideSession !== null);
    assertEquals(productOutsideSession.stock, 20);
    assertEquals(productOutsideSession.version, 1);

    const categoryOutsideSession = await store.findOne("category", {
      _id: categoryId,
    });
    assert(categoryOutsideSession !== null);
    assertEquals(categoryOutsideSession.productCount, 1);

    // Make a separate update outside the session
    await store.updateOne("product", productId, {
      price: 95,
    });

    // Wait for transaction to complete
    const sessionResults = await sessionUpdatePromise;

    // After transaction, check final state
    const finalProduct = await store.findOne("product", { _id: productId });
    assert(finalProduct !== null);
    assertEquals(finalProduct.stock, 19);
    assertEquals(finalProduct.version, 2);
    assertEquals(finalProduct.price, 95); // This update was made outside the transaction

    const finalCategory = await store.findOne("category", { _id: categoryId });
    assert(finalCategory !== null);
    assertEquals(finalCategory.productCount, 2);
  });
});

Deno.test("Session Concurrent: Mixed collection types with concurrent operations", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create regular collection and multi-collection
    const users = await collection(db, "users", userSchema);
    const store = await multiCollection(db, "store", storeModel);

    // Insert initial data outside any session
    const userId = await users.insertOne({
      name: "Jane Smith",
      email: "jane@example.com",
      age: 35,
    });

    const productId = await store.insertOne("product", {
      name: "Smartphone",
      price: 599,
      stock: 50,
    });

    // Start a transaction involving both collection types
    const sessionUpdatePromise = users.withSession(async () => {
      // Update user in regular collection
      await users.updateOne(
        { _id: userId },
        { $set: { age: 36, version: 2 } },
      );

      // Update product in multi-collection
      await store.updateOne("product", productId, {
        stock: 49,
        version: 2,
      });

      // Read values within session
      const userInSession = await users.getById(userId);
      assert(userInSession !== null);
      assertEquals(userInSession.age, 36);

      const productInSession = await store.findOne("product", {
        _id: productId,
      });
      assert(productInSession !== null);
      assertEquals(productInSession.stock, 49);

      // Sleep to simulate long-running transaction
      await new Promise((resolve) => setTimeout(resolve, 300));

      return { userInSession, productInSession };
    });

    // While the transaction is running, make concurrent changes
    // Short delay to ensure transaction has started
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Read values outside session
    const userOutsideSession = await users.getById(userId);
    assert(userOutsideSession !== null);
    assertEquals(userOutsideSession.age, 35);
    assertEquals(userOutsideSession.version, 1);

    const productOutsideSession = await store.findOne("product", {
      _id: productId,
    });
    assert(productOutsideSession !== null);
    assertEquals(productOutsideSession.stock, 50);
    assertEquals(productOutsideSession.version, 1);

    // Make separate updates outside the session
    await users.updateOne(
      { _id: userId },
      { $set: { email: "jane.smith@example.com" } },
    );

    await store.updateOne("product", productId, {
      price: 549, // Sale price
    });

    // Wait for transaction to complete
    const sessionResults = await sessionUpdatePromise;

    // After transaction, check final state
    const finalUser = await users.getById(userId);
    assert(finalUser !== null);
    assertEquals(finalUser.age, 36);
    assertEquals(finalUser.version, 2);
    assertEquals(finalUser.email, "jane.smith@example.com");

    const finalProduct = await store.findOne("product", { _id: productId });
    assert(finalProduct !== null);
    assertEquals(finalProduct.stock, 49);
    assertEquals(finalProduct.version, 2);
    assertEquals(finalProduct.price, 549);
  });
});

Deno.test("Session Concurrent: Rollback shouldn't affect outside operations", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create regular collection and multi-collection
    const users = await collection(db, "users", userSchema);
    const store = await multiCollection(db, "store", storeModel);

    // Insert initial data outside any session
    const userId = await users.insertOne({
      name: "Alex Brown",
      email: "alex@example.com",
      age: 40,
    });

    const productId = await store.insertOne("product", {
      name: "Laptop",
      price: 1200,
      stock: 10,
    });

    // Define a function to handle the transaction that will throw an error
    const runTransactionThatWillFail = async () => {
      // Will catch and return the error from the withSession call
      try {
        await users.withSession(async () => {
          // Update user in regular collection
          await users.updateOne(
            { _id: userId },
            { $set: { age: 41, version: 2 } },
          );

          // Update product in multi-collection
          await store.updateOne("product", productId, {
            stock: 9,
            version: 2,
          });

          // Sleep to simulate long-running transaction
          await new Promise((resolve) => setTimeout(resolve, 100));

          // Force a rollback
          throw new Error("Intentional error to trigger rollback");
        });
      } catch (error) {
        return error;
      }
    };

    // Start the transaction in the background but don't wait for it to complete yet
    const transactionPromise = runTransactionThatWillFail();

    // Short delay to ensure transaction has started
    await new Promise((resolve) => setTimeout(resolve, 50));

    // While the transaction is running, make concurrent changes outside the session
    await users.updateOne(
      { _id: userId },
      { $set: { email: "alex.brown@example.com" } },
    );

    await store.updateOne("product", productId, {
      price: 1100, // Discount price
    });

    // Now wait for the transaction to complete and check the error
    const error = await transactionPromise;
    if (error instanceof Error) {
      assertEquals(error.message, "Intentional error to trigger rollback");
    } else {
      throw new Error("Expected an Error but did not receive one");
    }

    // After rollback, check final state
    const finalUser = await users.getById(userId);
    assert(finalUser !== null);
    assertEquals(finalUser.age, 40); // Session changes rolled back
    assertEquals(finalUser.version, 1); // Session changes rolled back
    assertEquals(finalUser.email, "alex.brown@example.com"); // Outside changes preserved

    const finalProduct = await store.findOne("product", { _id: productId });
    assert(finalProduct !== null);
    assertEquals(finalProduct.stock, 10); // Session changes rolled back
    assertEquals(finalProduct.version, 1); // Session changes rolled back
    assertEquals(finalProduct.price, 1100); // Outside changes preserved
  });
});

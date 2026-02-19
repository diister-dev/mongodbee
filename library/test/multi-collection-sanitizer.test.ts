import * as v from "../src/schema.ts";
import { collection } from "../src/collection.ts";
import { test, expect } from "vitest";
import { withDatabase } from "./+shared.ts";

const userSchema = {
  name: v.string(),
  email: v.optional(v.string()),
  phone: v.optional(v.string()),
  age: v.optional(v.number()),
  status: v.optional(v.null()),
};

const productSchema = {
  name: v.string(),
  price: v.number(),
  description: v.optional(v.string()),
  category: v.optional(v.string()),
  tags: v.optional(v.array(v.string())),
};

test("Multi-collection: Different undefined behaviors", async () => {
  await withDatabase("mc_sanitizer_diff_behaviors", async (db) => {
    // Collection 1: Remove undefined values (default)
    const users = await collection(db, "users", userSchema, {
      undefinedBehavior: "remove",
    });

    // Collection 2: Error on undefined values
    const strictProducts = await collection(
      db,
      "strict_products",
      productSchema,
      {
        undefinedBehavior: "error",
      },
    );

    // Collection 3: Default behavior (should be 'remove')
    const products = await collection(db, "products", productSchema);

    // Test users collection (remove behavior)
    await users.insertOne({
      name: "John",
      email: undefined, // Should be removed
      phone: "123-456-7890",
      age: undefined, // Should be removed
    });

    const insertedUser = await users.findOne({ name: "John" });
    expect(insertedUser).not.toBeNull();
    expect(!("email" in insertedUser!)).toBeTruthy();
    expect(!("age" in insertedUser!)).toBeTruthy();
    expect(insertedUser!.name).toEqual("John");
    expect(insertedUser!.phone).toEqual("123-456-7890");

    // Test products collection (default = remove behavior)
    await products.insertOne({
      name: "Laptop",
      price: 999.99,
      description: undefined, // Should be removed
      category: "Electronics",
    });

    const insertedProduct = await products.findOne({ name: "Laptop" });
    expect(insertedProduct).not.toBeNull();
    expect(!("description" in insertedProduct!)).toBeTruthy();
    expect(insertedProduct!.name).toEqual("Laptop");
    expect(insertedProduct!.price).toEqual(999.99);
    expect(insertedProduct!.category).toEqual("Electronics");

    // Test strict products collection (error behavior)
    await expect(
      async () => {
        await strictProducts.insertOne({
          name: "Mouse",
          price: 29.99,
          description: undefined, // Should cause error
          category: "Electronics",
        });
      },
    ).rejects.toThrow("Undefined values are not allowed");

    // But it should work fine without undefined values
    await strictProducts.insertOne({
      name: "Mouse",
      price: 29.99,
      category: "Electronics",
    });

    const insertedStrictProduct = await strictProducts.findOne({
      name: "Mouse",
    });
    expect(insertedStrictProduct).not.toBeNull();
    expect(insertedStrictProduct!.name).toEqual("Mouse");
    expect(insertedStrictProduct!.price).toEqual(29.99);
    expect(insertedStrictProduct!.category).toEqual("Electronics");
  });
});

test("Multi-collection: Same schema, different configurations", async () => {
  await withDatabase("mc_sanitizer_same_schema", async (db) => {
    // Same schema, different undefined behaviors
    const strictUsers = await collection(db, "strict_users", userSchema, {
      undefinedBehavior: "error",
    });

    const lenientUsers = await collection(db, "lenient_users", userSchema, {
      undefinedBehavior: "remove",
    });

    const testData = {
      name: "Alice",
      email: "alice@example.com",
      phone: undefined, // This is the key difference
      age: 25,
    };

    // Strict collection should reject undefined
    await expect(
      async () => {
        await strictUsers.insertOne(testData);
      },
    ).rejects.toThrow("Undefined values are not allowed");

    // Lenient collection should accept and remove undefined
    await lenientUsers.insertOne(testData);

    const insertedUser = await lenientUsers.findOne({ name: "Alice" });
    expect(insertedUser).not.toBeNull();
    expect(!("phone" in insertedUser!)).toBeTruthy();
    expect(insertedUser!.name).toEqual("Alice");
    expect(insertedUser!.email).toEqual("alice@example.com");
    expect(insertedUser!.age).toEqual(25);
  });
});

test("Multi-collection: insertMany with different behaviors", async () => {
  await withDatabase("mc_sanitizer_insertmany", async (db) => {
    const users = await collection(db, "users_many", userSchema, {
      undefinedBehavior: "remove",
    });

    const strictUsers = await collection(db, "strict_users_many", userSchema, {
      undefinedBehavior: "error",
    });

    const testData = [
      {
        name: "User1",
        email: "user1@example.com",
        phone: undefined, // Will be removed in lenient, error in strict
        age: 30,
      },
      {
        name: "User2",
        email: undefined, // Will be removed in lenient, error in strict
        phone: "123-456-7890",
        age: 25,
      },
    ];

    // Lenient collection should work
    await users.insertMany(testData);

    const insertedUsers = await users.find({}).toArray();
    expect(insertedUsers.length).toEqual(2);

    // Check first user
    const user1 = insertedUsers.find((u) => u.name === "User1");
    expect(user1 !== undefined).toBeTruthy();
    expect(!("phone" in user1!)).toBeTruthy();
    expect(user1!.email).toEqual("user1@example.com");

    // Check second user
    const user2 = insertedUsers.find((u) => u.name === "User2");
    expect(user2 !== undefined).toBeTruthy();
    expect(!("email" in user2!)).toBeTruthy();
    expect(user2!.phone).toEqual("123-456-7890");

    // Strict collection should reject
    await expect(
      async () => {
        await strictUsers.insertMany(testData);
      },
    ).rejects.toThrow("Undefined values are not allowed");
  });
});

test("Multi-collection: replaceOne with different behaviors", async () => {
  await withDatabase("mc_sanitizer_replaceone", async (db) => {
    const users = await collection(db, "users_replace", userSchema, {
      undefinedBehavior: "remove",
    });

    const strictUsers = await collection(
      db,
      "strict_users_replace",
      userSchema,
      {
        undefinedBehavior: "error",
      },
    );

    // Insert initial data
    await users.insertOne({
      name: "TestUser",
      email: "test@example.com",
      phone: "123-456-7890",
      age: 30,
    });

    await strictUsers.insertOne({
      name: "TestUser",
      email: "test@example.com",
      phone: "123-456-7890",
      age: 30,
    });

    const updateData = {
      name: "TestUser Updated",
      email: "updated@example.com",
      phone: undefined, // This will cause different behaviors
      age: 35,
    };

    // Lenient collection should work (remove undefined)
    await users.replaceOne({ name: "TestUser" }, updateData);

    const updatedUser = await users.findOne({ name: "TestUser Updated" });
    expect(updatedUser).not.toBeNull();
    expect(!("phone" in updatedUser!)).toBeTruthy();
    expect(updatedUser!.email).toEqual("updated@example.com");
    expect(updatedUser!.age).toEqual(35);

    // Strict collection should reject undefined
    await expect(
      async () => {
        await strictUsers.replaceOne({ name: "TestUser" }, updateData);
      },
    ).rejects.toThrow("Undefined values are not allowed");
  });
});

test("Multi-collection: Cross-collection data consistency", async () => {
  await withDatabase("mc_sanitizer_cross_coll", async (db) => {
    // Create collections with different behaviors
    const mainUsers = await collection(db, "main_users", userSchema, {
      undefinedBehavior: "remove",
    });

    const auditUsers = await collection(db, "audit_users", userSchema, {
      undefinedBehavior: "error",
    });

    // Data that works in main but not in audit
    const userData = {
      name: "CrossCollectionUser",
      email: "cross@example.com",
      phone: undefined, // Problematic for audit
      age: 28,
    };

    // Insert into main collection (should work)
    await mainUsers.insertOne(userData);

    const mainUser = await mainUsers.findOne({ name: "CrossCollectionUser" });
    expect(mainUser).not.toBeNull();
    expect(!("phone" in mainUser!)).toBeTruthy();

    // Trying to insert same data into audit should fail
    await expect(
      async () => {
        await auditUsers.insertOne(userData);
      },
    ).rejects.toThrow("Undefined values are not allowed");

    // But we can sanitize data for audit by removing undefined fields first
    const cleanUserData = {
      name: userData.name,
      email: userData.email,
      age: userData.age,
      // phone is not included
    };

    await auditUsers.insertOne(cleanUserData);

    const auditUser = await auditUsers.findOne({ name: "CrossCollectionUser" });
    expect(auditUser).not.toBeNull();
    expect(!("phone" in auditUser!)).toBeTruthy();
    expect(auditUser!.name).toEqual("CrossCollectionUser");
    expect(auditUser!.email).toEqual("cross@example.com");
    expect(auditUser!.age).toEqual(28);
  });
});

test("Multi-collection: Performance with large number of collections", async () => {
  await withDatabase("mc_sanitizer_perf", async (db) => {
    const collections = [];
    const collectionCount = 5; // Keep it reasonable for testing

    // Create multiple collections with different configurations
    for (let i = 0; i < collectionCount; i++) {
      const collectionName = `perf_collection_${i}`;
      const undefinedBehavior = i % 2 === 0 ? "remove" : "error";

      const coll = await collection(db, collectionName, userSchema, {
        undefinedBehavior,
      });

      collections.push({ coll, undefinedBehavior, name: collectionName });
    }

    // Test each collection with appropriate data
    for (let i = 0; i < collections.length; i++) {
      const { coll, undefinedBehavior } = collections[i];

      if (undefinedBehavior === "remove") {
        // Can insert data with undefined
        await coll.insertOne({
          name: `User_${i}`,
          email: `user${i}@example.com`,
          phone: undefined,
          age: 20 + i,
        });

        const user = await coll.findOne({ name: `User_${i}` });
        expect(user).not.toBeNull();
        expect(!("phone" in user!)).toBeTruthy();
      } else {
        // Must insert clean data
        await coll.insertOne({
          name: `User_${i}`,
          email: `user${i}@example.com`,
          age: 20 + i,
        });

        const user = await coll.findOne({ name: `User_${i}` });
        expect(user).not.toBeNull();
        expect(user!.name).toEqual(`User_${i}`);
      }
    }

    // Verify all collections work independently
    expect(collections.length).toEqual(collectionCount);
  });
});

test("Multi-collection: Mixed undefined behaviors in transactions", async () => {
  await withDatabase("mc_sanitizer_mixed_tx", async (db) => {
    const lenientCollection = await collection(db, "lenient_tx", userSchema, {
      undefinedBehavior: "remove",
    });

    const strictCollection = await collection(db, "strict_tx", userSchema, {
      undefinedBehavior: "error",
    });

    // Test that each collection maintains its own behavior even in complex scenarios
    const testData = {
      name: "TxUser",
      email: "tx@example.com",
      phone: undefined,
      age: 30,
    };

    // This should work for lenient
    await lenientCollection.insertOne(testData);

    const lenientUser = await lenientCollection.findOne({ name: "TxUser" });
    expect(lenientUser).not.toBeNull();
    expect(!("phone" in lenientUser!)).toBeTruthy();

    // This should fail for strict
    await expect(
      async () => {
        await strictCollection.insertOne(testData);
      },
    ).rejects.toThrow("Undefined values are not allowed");

    // Verify lenient still works after strict failed
    await lenientCollection.insertOne({
      name: "TxUser2",
      email: undefined,
      phone: "123-456-7890",
      age: 25,
    });

    const lenientUser2 = await lenientCollection.findOne({ name: "TxUser2" });
    expect(lenientUser2).not.toBeNull();
    expect(!("email" in lenientUser2!)).toBeTruthy();
    expect(lenientUser2!.phone).toEqual("123-456-7890");
  });
});

test("Multi-collection: Null validation", async () => {
  await withDatabase("mc_sanitizer_null", async (db) => {
    const users = await collection(db, "null_test_users", userSchema, {
      undefinedBehavior: "remove",
    });

    // Test inserting with null status
    await users.insertOne({
      name: "NullUser",
      email: "null@example.com",
      phone: "123-456-7890",
      age: 30,
      status: null,
    });

    const insertedUser = await users.findOne({ name: "NullUser" });
    expect(insertedUser).not.toBeNull();
    expect(insertedUser!.status).toEqual(null);
    expect(insertedUser!.name).toEqual("NullUser");

    // Test inserting without status (should be undefined, not null)
    await users.insertOne({
      name: "NoStatusUser",
      email: "nostatus@example.com",
      age: 25,
    });

    const noStatusUser = await users.findOne({ name: "NoStatusUser" });
    expect(noStatusUser).not.toBeNull();
    expect(!("status" in noStatusUser!)).toBeTruthy(); // status should not be present

    // Test updating to null
    await users.updateOne(
      { name: "NoStatusUser" },
      { $set: { status: null } },
    );

    const updatedUser = await users.findOne({ name: "NoStatusUser" });
    expect(updatedUser).not.toBeNull();
    expect(updatedUser!.status).toEqual(null);
  });
});

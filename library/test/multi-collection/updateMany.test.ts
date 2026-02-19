import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import assert from "node:assert";
import { defineModel } from "../../src/multi-collection-model.ts";
import { removeField } from "../../src/sanitizer.ts";

test("updateMany: Basic multiple update", async () => {
  await withDatabase("updateMany: Basic multiple update", async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          age: v.number(),
        },
        group: {
          name: v.string(),
          members: v.array(v.string()),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert users
    const userIds = await collection.insertMany("user", [
      { name: "Alice", age: 20 },
      { name: "Bob", age: 30 },
      { name: "Charlie", age: 40 },
    ]);

    // Update multiple users
    const updatedCount = await collection.updateManyByIds({
      user: {
        [userIds[0]]: { age: 21 },
        [userIds[1]]: { name: "Bobby" },
      },
    });
    expect(updatedCount).toEqual(2);

    const userA = await collection.findOne("user", { _id: userIds[0] });
    const userB = await collection.findOne("user", { _id: userIds[1] });
    expect(userA).not.toBeNull();
    expect(userB).not.toBeNull();
    expect(userA.age).toEqual(21);
    expect(userB.name).toEqual("Bobby");
  });
});

test("updateMany: Update nested and array fields", async () => {
  await withDatabase("updateMany: Update nested and array fields", async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          tags: v.array(v.string()),
          profile: v.object({
            age: v.number(),
            city: v.string(),
          }),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    const userIds = await collection.insertMany("user", [
      { name: "A", tags: ["x"], profile: { age: 10, city: "Paris" } },
      { name: "B", tags: ["y"], profile: { age: 20, city: "London" } },
    ]);

    const updatedCount = await collection.updateManyByIds({
      user: {
        [userIds[0]]: { "profile.city": "Berlin", "tags.0": "z" },
        [userIds[1]]: { name: "Bee", "profile.age": 21 },
      },
    });
    expect(updatedCount).toEqual(2);

    const userA = await collection.findOne("user", { _id: userIds[0] });
    const userB = await collection.findOne("user", { _id: userIds[1] });
    expect(userA).not.toBeNull();
    expect(userB).not.toBeNull();
    expect(userA.profile.city).toEqual("Berlin");
    expect(userA.tags[0]).toEqual("z");
    expect(userB.name).toEqual("Bee");
    expect(userB.profile.age).toEqual(21);
  });
});

test("updateMany: Error on wrong id or type", async () => {
  await withDatabase("updateMany: Error on wrong id or type", async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          age: v.number(),
        },
        group: {
          name: v.string(),
          members: v.array(v.string()),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);
    const userIds = await collection.insertMany("user", [
      { name: "A", age: 1 },
      { name: "B", age: 2 },
    ]);
    const groupIds = await collection.insertMany("group", [
      { name: "G", members: [userIds[0]] },
    ]);
    // Wrong id format
    await expect(async () => {
      await collection.updateManyByIds({
        user: { "invalid:id": { name: "fail" } },
      });
    }).rejects.toThrow();
    // Wrong type
    await expect(async () => {
      await collection.updateManyByIds({
        user: { [groupIds[0]]: { name: "fail" } },
      });
    }).rejects.toThrow();
    // No element to update
    await expect(
      async () => {
        await collection.updateManyByIds({});
      },
    ).rejects.toThrow("No element to update");
  });
});

test("updateMany: Remove fields with removeField()", async () => {
  await withDatabase("updateMany: Remove fields with removeField()", async (db) => {
    const testModel = defineModel("test", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          description: v.optional(v.string()),
          category: v.optional(v.string()),
          tags: v.optional(v.array(v.string())),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert products with optional fields
    const productIds = await collection.insertMany("product", [
      {
        name: "Product A",
        price: 100,
        description: "Description A",
        category: "Cat A",
        tags: ["tag1", "tag2"],
      },
      {
        name: "Product B",
        price: 200,
        description: "Description B",
        category: "Cat B",
        tags: ["tag3"],
      },
      {
        name: "Product C",
        price: 300,
        description: "Description C",
        category: "Cat C",
      },
    ]);

    // Update multiple products, removing different fields
    const updatedCount = await collection.updateManyByIds({
      product: {
        [productIds[0]]: {
          description: removeField(), // Remove description
          price: 150,
        },
        [productIds[1]]: {
          category: removeField(), // Remove category
          tags: removeField(), // Remove tags
        },
        [productIds[2]]: {
          name: "Product C Updated",
          description: removeField(), // Remove description
        },
      },
    });

    expect(updatedCount).toEqual(3);

    // Verify Product A
    const productA = await collection.findOne("product", { _id: productIds[0] });
    expect(productA).not.toBeNull();
    expect(productA.name).toEqual("Product A");
    expect(productA.price).toEqual(150);
    expect(productA.description).toEqual(undefined); // Removed
    expect(productA.category).toEqual("Cat A"); // Unchanged
    expect(productA.tags).toEqual(["tag1", "tag2"]); // Unchanged

    // Verify Product B
    const productB = await collection.findOne("product", { _id: productIds[1] });
    expect(productB).not.toBeNull();
    expect(productB.name).toEqual("Product B");
    expect(productB.price).toEqual(200); // Unchanged
    expect(productB.description).toEqual("Description B"); // Unchanged
    expect(productB.category).toEqual(undefined); // Removed
    expect(productB.tags).toEqual(undefined); // Removed

    // Verify Product C
    const productC = await collection.findOne("product", { _id: productIds[2] });
    expect(productC).not.toBeNull();
    expect(productC.name).toEqual("Product C Updated");
    expect(productC.price).toEqual(300); // Unchanged
    expect(productC.description).toEqual(undefined); // Removed
    expect(productC.category).toEqual("Cat C"); // Unchanged
  });
});

test("updateMany: Mix updates and removes across different types", async () => {
  await withDatabase("updateMany: Mix updates and removes across different types", async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          email: v.string(),
          phone: v.optional(v.string()),
        },
        product: {
          name: v.string(),
          price: v.number(),
          discount: v.optional(v.number()),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    const userId = await collection.insertOne("user", {
      name: "John",
      email: "john@example.com",
      phone: "123-456",
    });

    const productId = await collection.insertOne("product", {
      name: "Laptop",
      price: 1000,
      discount: 10,
    });

    // Update across different types
    await collection.updateManyByIds({
      user: {
        [userId]: {
          name: "John Doe",
          phone: removeField(),
        },
      },
      product: {
        [productId]: {
          price: 900,
          discount: removeField(),
        },
      },
    });

    // Verify user
    const user = await collection.findOne("user", { _id: userId });
    expect(user).not.toBeNull();
    expect(user.name).toEqual("John Doe");
    expect(user.email).toEqual("john@example.com");
    expect(user.phone).toEqual(undefined);

    // Verify product
    const product = await collection.findOne("product", { _id: productId });
    expect(product).not.toBeNull();
    expect(product.name).toEqual("Laptop");
    expect(product.price).toEqual(900);
    expect(product.discount).toEqual(undefined);
  });
});

import * as v from "../../src/schema.ts";
import { assertEquals, assertRejects } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import assert from "node:assert";
import { defineModel } from "../../src/multi-collection-model.ts";
import { removeField } from "../../src/sanitizer.ts";

Deno.test("updateMany: Basic multiple update", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    const updatedCount = await collection.updateMany({
      user: {
        [userIds[0]]: { age: 21 },
        [userIds[1]]: { name: "Bobby" },
      },
    });
    assertEquals(updatedCount, 2);

    const userA = await collection.findOne("user", { _id: userIds[0] });
    const userB = await collection.findOne("user", { _id: userIds[1] });
    assert(userA !== null && userB !== null);
    assertEquals(userA.age, 21);
    assertEquals(userB.name, "Bobby");
  });
});

Deno.test("updateMany: Update nested and array fields", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    const updatedCount = await collection.updateMany({
      user: {
        [userIds[0]]: { "profile.city": "Berlin", "tags.0": "z" },
        [userIds[1]]: { name: "Bee", "profile.age": 21 },
      },
    });
    assertEquals(updatedCount, 2);

    const userA = await collection.findOne("user", { _id: userIds[0] });
    const userB = await collection.findOne("user", { _id: userIds[1] });
    assert(userA !== null && userB !== null);
    assertEquals(userA.profile.city, "Berlin");
    assertEquals(userA.tags[0], "z");
    assertEquals(userB.name, "Bee");
    assertEquals(userB.profile.age, 21);
  });
});

Deno.test("updateMany: Error on wrong id or type", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    await assertRejects(async () => {
      await collection.updateMany({
        user: { "invalid:id": { name: "fail" } },
      });
    });
    // Wrong type
    await assertRejects(async () => {
      await collection.updateMany({
        user: { [groupIds[0]]: { name: "fail" } },
      });
    });
    // No element to update
    await assertRejects(
      async () => {
        await collection.updateMany({});
      },
      Error,
      "No element to update",
    );
  });
});

Deno.test("updateMany: Remove fields with removeField()", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    const updatedCount = await collection.updateMany({
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

    assertEquals(updatedCount, 3);

    // Verify Product A
    const productA = await collection.findOne("product", { _id: productIds[0] });
    assert(productA !== null);
    assertEquals(productA.name, "Product A");
    assertEquals(productA.price, 150);
    assertEquals(productA.description, undefined); // Removed
    assertEquals(productA.category, "Cat A"); // Unchanged
    assertEquals(productA.tags, ["tag1", "tag2"]); // Unchanged

    // Verify Product B
    const productB = await collection.findOne("product", { _id: productIds[1] });
    assert(productB !== null);
    assertEquals(productB.name, "Product B");
    assertEquals(productB.price, 200); // Unchanged
    assertEquals(productB.description, "Description B"); // Unchanged
    assertEquals(productB.category, undefined); // Removed
    assertEquals(productB.tags, undefined); // Removed

    // Verify Product C
    const productC = await collection.findOne("product", { _id: productIds[2] });
    assert(productC !== null);
    assertEquals(productC.name, "Product C Updated");
    assertEquals(productC.price, 300); // Unchanged
    assertEquals(productC.description, undefined); // Removed
    assertEquals(productC.category, "Cat C"); // Unchanged
  });
});

Deno.test("updateMany: Mix updates and removes across different types", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    await collection.updateMany({
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
    assert(user !== null);
    assertEquals(user.name, "John Doe");
    assertEquals(user.email, "john@example.com");
    assertEquals(user.phone, undefined);

    // Verify product
    const product = await collection.findOne("product", { _id: productId });
    assert(product !== null);
    assertEquals(product.name, "Laptop");
    assertEquals(product.price, 900);
    assertEquals(product.discount, undefined);
  });
});

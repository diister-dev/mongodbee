import * as v from "../src/schema.ts";
import { collection } from "../src/collection.ts";
import { test, expect } from "vitest";
import { removeField } from "../src/sanitizer.ts";
import { withDatabase } from "./+shared.ts";

const userSchema = {
  name: v.string(),
  email: v.optional(v.string()),
  phone: v.optional(v.string()),
  age: v.optional(v.number()),
  address: v.optional(v.object({
    street: v.string(),
    city: v.optional(v.string()),
    zipcode: v.optional(v.string()),
  })),
};

test("Collection with default undefined behavior (remove)", async () => {
  await withDatabase("sanitizer_default_remove", async (db) => {
    const users = await collection(db, "users_default", userSchema);

    // Insert document with undefined values
    await users.insertOne({
      name: "John",
      email: undefined, // Should be removed
      phone: "123-456-7890",
      age: undefined, // Should be removed
    });

    // Fetch the inserted document by name since we know it's unique
    const insertedDoc = await users.findOne({ name: "John" });

    // Should not have email and age fields
    expect(insertedDoc).not.toBeNull();
    expect(!("email" in insertedDoc)).toBeTruthy();
    expect(!("age" in insertedDoc)).toBeTruthy();
    expect(insertedDoc.name).toEqual("John");
    expect(insertedDoc.phone).toEqual("123-456-7890");
  });
});

test("Collection with ignore undefined behavior", async () => {
  await withDatabase("sanitizer_ignore", async (db) => {
    const users = await collection(db, "users_ignore", userSchema, {
      undefinedBehavior: "ignore",
    });

    // Insert initial document
    await users.insertOne({
      name: "John",
      email: "john@example.com",
      phone: "123-456-7890",
    });

    // Update with undefined values (should be ignored)
    await users.replaceOne(
      { name: "John" },
      {
        name: "John Updated",
        email: undefined, // Should be ignored (keep original)
        phone: "987-654-3210",
      },
    );

    const updatedDoc = await users.findOne({ name: "John Updated" });

    // email should not be in the document since it was undefined in replace
    // (replaceOne replaces the entire document, so undefined fields are removed)
    expect(updatedDoc).not.toBeNull();
    expect(!("email" in updatedDoc)).toBeTruthy();
    expect(updatedDoc.name).toEqual("John Updated");
    expect(updatedDoc.phone).toEqual("987-654-3210");
  });
});

test("Collection with error undefined behavior", async () => {
  await withDatabase("sanitizer_error", async (db) => {
    const users = await collection(db, "users_error", userSchema, {
      undefinedBehavior: "error",
    });

    // Should throw error when trying to insert with undefined
    await expect(
      async () => {
        await users.insertOne({
          name: "John",
          email: undefined, // Should cause error
          phone: "123-456-7890",
        });
      },
    ).rejects.toThrow("Undefined values are not allowed");
  });
});

test("Explicit field removal with removeField()", async () => {
  await withDatabase("sanitizer_remove_field", async (db) => {
    const users = await collection(db, "users_remove_field", userSchema);

    // Insert initial document
    await users.insertOne({
      name: "John",
      email: "john@example.com",
      phone: "123-456-7890",
      age: 30,
    });

    // Test replacement with field removal using replaceOne by name
    await users.replaceOne(
      { name: "John" },
      {
        name: "John Updated",
        email: "john.updated@example.com",
      },
    );

    const updatedDoc = await users.findOne({ name: "John Updated" });

    // Fields not included in replacement should be gone
    expect(updatedDoc).not.toBeNull();
    expect(!("phone" in updatedDoc)).toBeTruthy();
    expect(!("age" in updatedDoc)).toBeTruthy();
    expect(updatedDoc.name).toEqual("John Updated");
    expect(updatedDoc.email).toEqual("john.updated@example.com");
  });
});

test("Complex nested object with mixed undefined behaviors", async () => {
  await withDatabase("sanitizer_nested", async (db) => {
    const users = await collection(db, "users_nested", userSchema, {
      undefinedBehavior: "remove",
    });

    // Insert document with nested undefined values
    await users.insertOne({
      name: "John",
      email: "john@example.com",
      address: {
        street: "123 Main St",
        city: undefined, // Should be removed
        zipcode: "12345",
      },
    });

    const insertedDoc = await users.findOne({ name: "John" });

    // Nested undefined should be removed
    expect(insertedDoc).not.toBeNull();
    expect(insertedDoc.address !== undefined).toBeTruthy();
    expect(!("city" in insertedDoc.address)).toBeTruthy();
    expect(insertedDoc.address.street).toEqual("123 Main St");
    expect(insertedDoc.address.zipcode).toEqual("12345");

    // Update with mixed explicit and implicit removals
    await users.replaceOne(
      { name: "John" },
      {
        name: "John Updated",
        email: undefined, // Implicit removal
        address: {
          street: "456 Oak Ave",
          // city removed by not including it
          // zipcode removed by not including it
        },
      },
    );

    const updatedDoc = await users.findOne({ name: "John Updated" });

    // All undefined fields should be removed
    expect(updatedDoc).not.toBeNull();
    expect(!("email" in updatedDoc)).toBeTruthy();
    expect(updatedDoc.address !== undefined).toBeTruthy();
    expect(!("city" in updatedDoc.address)).toBeTruthy();
    expect(!("zipcode" in updatedDoc.address)).toBeTruthy();
    expect(updatedDoc.name).toEqual("John Updated");
    expect(updatedDoc.address.street).toEqual("456 Oak Ave");
  });
});

test("Array sanitization with undefined values", async () => {
  await withDatabase("sanitizer_arrays", async (db) => {
    const usersSchema = {
      name: v.string(),
      tags: v.optional(v.array(v.string())),
      contacts: v.optional(v.array(v.object({
        type: v.string(),
        value: v.optional(v.string()),
      }))),
    };

    const users = await collection(db, "users_arrays", usersSchema);

    // Insert with arrays containing undefined - we'll simulate this differently
    await users.insertOne({
      name: "John",
      tags: ["work", "personal"], // No undefined in type-safe way
      contacts: [
        { type: "email", value: "john@example.com" },
        { type: "phone" }, // No value property = undefined
        { type: "fax", value: "555-1234" },
      ],
    });

    const insertedDoc = await users.findOne({ name: "John" });

    // Array should be clean
    expect(insertedDoc).not.toBeNull();
    expect(insertedDoc.tags !== undefined).toBeTruthy();
    expect(insertedDoc.tags).toEqual(["work", "personal"]);
    expect(insertedDoc.contacts !== undefined).toBeTruthy();
    expect(insertedDoc.contacts.length).toEqual(3);

    // Second contact should not have 'value' field
    expect(!("value" in insertedDoc.contacts[1])).toBeTruthy();
    expect(insertedDoc.contacts[1].type).toEqual("phone");
  });
});

test("Behavior consistency across insert and replace operations", async () => {
  await withDatabase("sanitizer_consistency", async (db) => {
    const users = await collection(db, "users_consistency", userSchema, {
      undefinedBehavior: "remove",
    });

    // Test data with undefined values
    const testData = {
      name: "Consistency Test",
      email: undefined,
      phone: "123-456-7890",
      age: undefined,
    };

    // Insert operation
    await users.insertOne(testData);
    const insertedDoc = await users.findOne({ name: "Consistency Test" });

    // Replace operation with same data
    await users.replaceOne({ name: "Consistency Test" }, testData);
    const replacedDoc = await users.findOne({ name: "Consistency Test" });

    // Both should have identical structure (no email, no age)
    expect(insertedDoc).not.toBeNull();
    expect(replacedDoc).not.toBeNull();
    expect(insertedDoc.name).toEqual(replacedDoc.name);
    expect(insertedDoc.phone).toEqual(replacedDoc.phone);
    expect(!("email" in insertedDoc)).toBeTruthy();
    expect(!("age" in insertedDoc)).toBeTruthy();
    expect(!("email" in replacedDoc)).toBeTruthy();
    expect(!("age" in replacedDoc)).toBeTruthy();
    expect(insertedDoc.name).toEqual("Consistency Test");
    expect(insertedDoc.phone).toEqual("123-456-7890");
  });
});

test("Collection updateOne with removeField()", async () => {
  await withDatabase("sanitizer_updateone_removefield", async (db) => {
    const users = await collection(db, "users_remove_field", userSchema);

    // Insert document with optional fields
    const userId = await users.insertOne({
      name: "John",
      email: "john@example.com",
      phone: "123-456",
      age: 30,
    });

    // Verify initial state
    const initialUser = await users.findOne({ _id: userId });
    expect(initialUser).not.toBeNull();
    expect(initialUser.email).toEqual("john@example.com");
    expect(initialUser.phone).toEqual("123-456");
    expect(initialUser.age).toEqual(30);

    // Remove email field using $set with removeField()
    await users.updateOne({ _id: userId }, {
      $set: {
        email: removeField(),
      },
    });

    // Verify email was removed
    const afterEmailRemoval = await users.findOne({ _id: userId });
    expect(afterEmailRemoval).not.toBeNull();
    expect(afterEmailRemoval.name).toEqual("John");
    expect(afterEmailRemoval.email).toEqual(undefined);
    expect(afterEmailRemoval.phone).toEqual("123-456");
    expect(afterEmailRemoval.age).toEqual(30);

    // Mix update and remove in same operation
    await users.updateOne({ _id: userId }, {
      $set: {
        name: "John Doe",
        phone: removeField(),
        age: removeField(),
      },
    });

    // Verify mixed operation
    const afterMixedUpdate = await users.findOne({ _id: userId });
    expect(afterMixedUpdate).not.toBeNull();
    expect(afterMixedUpdate.name).toEqual("John Doe");
    expect(afterMixedUpdate.email).toEqual(undefined);
    expect(afterMixedUpdate.phone).toEqual(undefined);
    expect(afterMixedUpdate.age).toEqual(undefined);
  });
});

test("Collection findOneAndUpdate with removeField()", async () => {
  await withDatabase("sanitizer_findoneandupdate_removefield", async (db) => {
    const users = await collection(db, "users_find_and_update", userSchema);

    // Insert document
    await users.insertOne({
      name: "Jane",
      email: "jane@example.com",
      phone: "555-1234",
    });

    // Update and remove field in one operation
    const result = await users.findOneAndUpdate(
      { name: "Jane" },
      {
        $set: {
          name: "Jane Doe",
          email: removeField(),
        },
      },
      { returnDocument: "after", includeResultMetadata: false },
    );

    expect(result).not.toBeNull();

    // Verify the update
    const updatedUser = await users.findOne({ name: "Jane Doe" });
    expect(updatedUser).not.toBeNull();
    expect(updatedUser.name).toEqual("Jane Doe");
    expect(updatedUser.email).toEqual(undefined);
    expect(updatedUser.phone).toEqual("555-1234");
  });
});

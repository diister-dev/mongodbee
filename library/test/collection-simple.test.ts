import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";
import { ObjectId } from "mongodb";
import { test, expect } from "vitest";

// Simple test schema
const userSchema = {
  name: v.string(),
  age: v.number(),
  email: v.optional(v.string()),
  status: v.null(),
} as const;

test("Collection: Basic operations coverage", async () => {
  await withDatabase("Collection: Basic operations coverage", async (db) => {
    const users = await collection(db, "users", userSchema);

    // Test insertOne
    const _insertResult = await users.insertOne({
      name: "John",
      age: 30,
      email: "john@example.com",
      status: null,
    });

    // Test findOne
    const found = await users.findOne({ name: "John" });
    expect(found).not.toBeNull();
    expect(found.name).toEqual("John");

    // Test find
    const allUsers = await users.find({}).toArray();
    expect(allUsers.length).toEqual(1);

    // Test updateOne
    await users.updateOne({ name: "John" }, { $set: { age: 31 } });
    const updated = await users.findOne({ name: "John" });
    expect(updated?.age).toEqual(31);

    // Test deleteOne
    const deleteResult = await users.deleteOne({ name: "John" });
    expect(deleteResult.deletedCount).toEqual(1);

    // Test that document was deleted
    const countAfterDelete = await users.countDocuments({ name: "John" });
    expect(countAfterDelete).toEqual(0);

    // Test insertMany
    await users.insertMany([
      { name: "Alice", age: 25, status: null },
      { name: "Bob", age: 35, status: null },
    ]);

    // Test count
    const count = await users.countDocuments({});
    expect(count).toEqual(2);

    // Test distinct
    const names = await users.distinct("name", {});
    expect(names.length).toEqual(2);

    // Test aggregate
    const pipeline = [{ $match: { age: { $gte: 30 } } }];
    const results = await users.aggregate(pipeline).toArray();
    expect(results.length).toEqual(1);

    // Test replaceOne
    await users.replaceOne({ name: "Alice" }, {
      name: "Alice",
      age: 26,
      status: null,
    });
    const replaced = await users.findOne({ name: "Alice" });
    expect(replaced?.age).toEqual(26);

    // Test updateMany
    await users.updateMany({}, { $set: { email: "updated@example.com" } });
    const allUpdated = await users.find({}).toArray();
    allUpdated.forEach((user) => {
      expect(user.email).toEqual("updated@example.com");
    });

    // Test deleteMany
    await users.deleteMany({ age: { $gte: 0 } }); // Delete all documents
    const finalCount = await users.countDocuments({});
    expect(finalCount).toEqual(0);
  });
});

test("Collection: Error handling coverage", async () => {
  await withDatabase("Collection: Error handling coverage", async (db) => {
    const users = await collection(db, "users", userSchema);

    // Test updateOne with non-existent document
    const updateResult = await users.updateOne({ name: "NonExistent" }, {
      $set: { age: 40 },
    });
    expect(updateResult.modifiedCount).toEqual(0);

    // Test deleteOne with non-existent document
    const deleteResult = await users.deleteOne({ name: "NonExistent" });
    expect(deleteResult.deletedCount).toEqual(0);

    // Test replaceOne with non-existent document
    const replaceResult = await users.replaceOne({ name: "NonExistent" }, {
      name: "New",
      age: 20,
      status: null,
    });
    expect(replaceResult.modifiedCount).toEqual(0);

    // Test count with empty collection
    const emptyCount = await users.countDocuments({});
    expect(emptyCount).toEqual(0);
  });
});

test("Collection: getById functionality", async () => {
  await withDatabase("Collection: getById functionality", async (db) => {
    const users = await collection(db, "users", userSchema);

    // Insert a document
    const userId = await users.insertOne({
      name: "Test User",
      age: 25,
      email: "test@example.com",
      status: null,
    });

    // Test getById with valid ID
    const foundById = await users.getById(userId);
    expect(foundById).toBeTruthy();
    expect(foundById.name).toEqual("Test User");
    expect(foundById.age).toEqual(25);
    expect(foundById.email).toEqual("test@example.com");
    expect(foundById._id).toEqual(userId);

    // Test getById with non-existent ID
    await expect(async () => {
      await users.getById("nonexistent-id");
    }).rejects.toThrow();

    // Test getById with ObjectId
    const objectId = new ObjectId();
    await expect(async () => {
      await users.getById(objectId);
    }).rejects.toThrow();
  });
});

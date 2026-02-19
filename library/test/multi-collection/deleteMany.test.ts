import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

test("deleteMany - basic functionality", async () => {
  await withDatabase("deleteMany - basic functionality", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          age: v.number(),
          active: v.boolean(),
        },
        group: {
          name: v.string(),
          members: v.array(v.string()),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Insert test data
    await collection.insertOne("user", { name: "John", age: 25, active: true });
    await collection.insertOne("user", {
      name: "Jane",
      age: 30,
      active: false,
    });
    await collection.insertOne("user", { name: "Bob", age: 35, active: true });
    await collection.insertOne("group", { name: "Admins", members: [] });

    // Test deleteMany with filter
    const deletedCount = await collection.deleteMany("user", { active: false });
    expect(deletedCount).toEqual(1);

    // Verify only Jane was deleted
    const remainingUsers = await collection.find("user").toArray();
    expect(remainingUsers.length).toEqual(2);
    expect(remainingUsers.map((u) => u.name).sort()).toEqual(["Bob", "John"]);

    // Verify groups are untouched
    const groups = await collection.find("group").toArray();
    expect(groups.length).toEqual(1);
  });
});

test("deleteMany - multiple matches", async () => {
  await withDatabase("deleteMany - multiple matches", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          age: v.number(),
          active: v.boolean(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Insert test data
    await collection.insertOne("user", { name: "John", age: 25, active: true });
    await collection.insertOne("user", { name: "Jane", age: 30, active: true });
    await collection.insertOne("user", { name: "Bob", age: 35, active: false });

    // Delete all active users
    const deletedCount = await collection.deleteMany("user", { active: true });
    expect(deletedCount).toEqual(2);

    // Verify only Bob remains
    const remainingUsers = await collection.find("user").toArray();
    expect(remainingUsers.length).toEqual(1);
    expect(remainingUsers[0].name).toEqual("Bob");
  });
});

test("deleteMany - no matches", async () => {
  await withDatabase("deleteMany - no matches", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          age: v.number(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Insert test data
    await collection.insertOne("user", { name: "John", age: 25 });

    // Try to delete non-existent records
    const deletedCount = await collection.deleteMany("user", { age: 50 });
    expect(deletedCount).toEqual(0);

    // Verify no users were deleted
    const remainingUsers = await collection.find("user").toArray();
    expect(remainingUsers.length).toEqual(1);
  });
});

test("deleteMany - only affects specified type", async () => {
  await withDatabase("deleteMany - only affects specified type", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
        },
        group: {
          name: v.string(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Insert test data
    await collection.insertOne("user", { name: "John" });
    await collection.insertOne("group", { name: "John" });

    // Delete users with name "John"
    const deletedCount = await collection.deleteMany("user", { name: "John" });
    expect(deletedCount).toEqual(1);

    // Verify only user was deleted, not group
    const remainingUsers = await collection.find("user").toArray();
    expect(remainingUsers.length).toEqual(0);

    const remainingGroups = await collection.find("group").toArray();
    expect(remainingGroups.length).toEqual(1);
    expect(remainingGroups[0].name).toEqual("John");
  });
});

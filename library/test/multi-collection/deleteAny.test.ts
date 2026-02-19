import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

test("deleteAny - basic functionality", async () => {
  await withDatabase("deleteAny - basic functionality", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string(), active: v.boolean() },
        group: { name: v.string(), active: v.boolean() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Insert test data
    await collection.insertOne("user", { name: "John", active: false });
    await collection.insertOne("user", { name: "Jane", active: true });
    await collection.insertOne("group", { name: "Admins", active: false });
    await collection.insertOne("group", { name: "Users", active: true });

    // Delete all inactive items regardless of type
    const deletedCount = await collection.deleteAny({ active: false });
    expect(deletedCount).toEqual(2);

    // Verify only active items remain
    const remainingUsers = await collection.find("user").toArray();
    expect(remainingUsers.length).toEqual(1);
    expect(remainingUsers[0].name).toEqual("Jane");

    const remainingGroups = await collection.find("group").toArray();
    expect(remainingGroups.length).toEqual(1);
    expect(remainingGroups[0].name).toEqual("Users");
  });
});

test("deleteAny - by _type field", async () => {
  await withDatabase("deleteAny - by _type field", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string() },
        group: { name: v.string() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Insert test data
    await collection.insertOne("user", { name: "John" });
    await collection.insertOne("user", { name: "Jane" });
    await collection.insertOne("group", { name: "Admins" });

    // Delete all users using _type filter (dangerous operation)
    const deletedCount = await collection.deleteAny({ _type: "user" });
    expect(deletedCount).toEqual(2);

    // Verify only groups remain
    const remainingUsers = await collection.find("user").toArray();
    expect(remainingUsers.length).toEqual(0);

    const remainingGroups = await collection.find("group").toArray();
    expect(remainingGroups.length).toEqual(1);
  });
});

test("deleteAny - complex filter", async () => {
  await withDatabase("deleteAny - complex filter", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string(), age: v.number() },
        group: { name: v.string(), memberCount: v.number() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Insert test data
    await collection.insertOne("user", { name: "John", age: 25 });
    await collection.insertOne("user", { name: "Jane", age: 30 });
    await collection.insertOne("group", { name: "Small", memberCount: 5 });
    await collection.insertOne("group", { name: "Large", memberCount: 100 });

    // Delete items using $or query (either young users or small groups)
    const deletedCount = await collection.deleteAny({
      $or: [
        { _type: "user", age: { $lt: 30 } },
        { _type: "group", memberCount: { $lt: 10 } },
      ],
    });
    expect(deletedCount).toEqual(2);

    // Verify only Jane and Large group remain
    const remainingUsers = await collection.find("user").toArray();
    expect(remainingUsers.length).toEqual(1);
    expect(remainingUsers[0].name).toEqual("Jane");

    const remainingGroups = await collection.find("group").toArray();
    expect(remainingGroups.length).toEqual(1);
    expect(remainingGroups[0].name).toEqual("Large");
  });
});

test("deleteAny - no matches", async () => {
  await withDatabase("deleteAny - no matches", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    // Insert test data
    await collection.insertOne("user", { name: "John" });

    // Try to delete non-existent records
    const deletedCount = await collection.deleteAny({ name: "NonExistent" });
    expect(deletedCount).toEqual(0);

    // Verify no users were deleted
    const remainingUsers = await collection.find("user").toArray();
    expect(remainingUsers.length).toEqual(1);
  });
});

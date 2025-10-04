import * as v from "../../src/schema.ts";
import { assertEquals } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

Deno.test("deleteAny - basic functionality", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertEquals(deletedCount, 2);

    // Verify only active items remain
    const remainingUsers = await collection.find("user");
    assertEquals(remainingUsers.length, 1);
    assertEquals(remainingUsers[0].name, "Jane");

    const remainingGroups = await collection.find("group");
    assertEquals(remainingGroups.length, 1);
    assertEquals(remainingGroups[0].name, "Users");
  });
});

Deno.test("deleteAny - by _type field", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertEquals(deletedCount, 2);

    // Verify only groups remain
    const remainingUsers = await collection.find("user");
    assertEquals(remainingUsers.length, 0);

    const remainingGroups = await collection.find("group");
    assertEquals(remainingGroups.length, 1);
  });
});

Deno.test("deleteAny - complex filter", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertEquals(deletedCount, 2);

    // Verify only Jane and Large group remain
    const remainingUsers = await collection.find("user");
    assertEquals(remainingUsers.length, 1);
    assertEquals(remainingUsers[0].name, "Jane");

    const remainingGroups = await collection.find("group");
    assertEquals(remainingGroups.length, 1);
    assertEquals(remainingGroups[0].name, "Large");
  });
});

Deno.test("deleteAny - no matches", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertEquals(deletedCount, 0);

    // Verify no users were deleted
    const remainingUsers = await collection.find("user");
    assertEquals(remainingUsers.length, 1);
  });
});

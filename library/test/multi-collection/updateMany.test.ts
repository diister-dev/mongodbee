import * as v from "../../src/schema.ts";
import { assertEquals, assertRejects } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import assert from "node:assert";
import { defineModel } from "../../src/multi-collection-model.ts";

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

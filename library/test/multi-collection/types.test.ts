import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

test("Types test - deleteMany and deleteAny should compile", async () => {
  await withDatabase("Types test - deleteMany and deleteAny should compile", async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          active: v.boolean(),
        },
        group: {
          name: v.string(),
          active: v.boolean(),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert test data
    await collection.insertOne("user", { name: "John", active: true });
    await collection.insertOne("group", { name: "Admins", active: false });

    // Test that deleteMany accepts correct filter types
    const deletedUsers = await collection.deleteMany("user", { active: true });
    expect(deletedUsers).toEqual(1);

    // Test that deleteAny accepts any filter (dangerous but should compile)
    const deletedAny = await collection.deleteAny({ active: false });
    expect(deletedAny).toEqual(1);

    // Verify all documents are deleted
    const remainingUsers = await collection.find("user").toArray();
    const remainingGroups = await collection.find("group").toArray();
    expect(remainingUsers.length).toEqual(0);
    expect(remainingGroups.length).toEqual(0);
  });
});

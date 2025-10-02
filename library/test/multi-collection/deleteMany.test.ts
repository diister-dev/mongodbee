import * as v from "../../src/schema.ts";
import { assertEquals, assertRejects, assert } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { createMultiCollectionModel } from "../../src/multi-collection-model.ts";

Deno.test("deleteMany - basic functionality", async (t) => {
    await withDatabase(t.name, async (db) => {
        const model = createMultiCollectionModel("test", {
            schema: {
                user: {
                    name: v.string(),
                    age: v.number(),
                    active: v.boolean(),
                },
                group: {
                    name: v.string(),
                    members: v.array(v.string()),
                }
            }
        });

        const collection = await multiCollection(db, "test", model);

        // Insert test data
        await collection.insertOne("user", { name: "John", age: 25, active: true });
        await collection.insertOne("user", { name: "Jane", age: 30, active: false });
        await collection.insertOne("user", { name: "Bob", age: 35, active: true });
        await collection.insertOne("group", { name: "Admins", members: [] });

        // Test deleteMany with filter
        const deletedCount = await collection.deleteMany("user", { active: false });
        assertEquals(deletedCount, 1);

        // Verify only Jane was deleted
        const remainingUsers = await collection.find("user");
        assertEquals(remainingUsers.length, 2);
        assertEquals(remainingUsers.map(u => u.name).sort(), ["Bob", "John"]);

        // Verify groups are untouched
        const groups = await collection.find("group");
        assertEquals(groups.length, 1);
    });
});

Deno.test("deleteMany - multiple matches", async (t) => {
    await withDatabase(t.name, async (db) => {
        const model = createMultiCollectionModel("test", {
            schema: {
                user: {
                    name: v.string(),
                    age: v.number(),
                    active: v.boolean(),
                }
            }
        });

        const collection = await multiCollection(db, "test", model);

        // Insert test data
        await collection.insertOne("user", { name: "John", age: 25, active: true });
        await collection.insertOne("user", { name: "Jane", age: 30, active: true });
        await collection.insertOne("user", { name: "Bob", age: 35, active: false });

        // Delete all active users
        const deletedCount = await collection.deleteMany("user", { active: true });
        assertEquals(deletedCount, 2);

        // Verify only Bob remains
        const remainingUsers = await collection.find("user");
        assertEquals(remainingUsers.length, 1);
        assertEquals(remainingUsers[0].name, "Bob");
    });
});

Deno.test("deleteMany - no matches", async (t) => {
    await withDatabase(t.name, async (db) => {
        const model = createMultiCollectionModel("test", {
            schema: {
                user: {
                    name: v.string(),
                    age: v.number(),
                }
            }
        });

        const collection = await multiCollection(db, "test", model);

        // Insert test data
        await collection.insertOne("user", { name: "John", age: 25 });

        // Try to delete non-existent records
        const deletedCount = await collection.deleteMany("user", { age: 50 });
        assertEquals(deletedCount, 0);

        // Verify no users were deleted
        const remainingUsers = await collection.find("user");
        assertEquals(remainingUsers.length, 1);
    });
});

Deno.test("deleteMany - only affects specified type", async (t) => {
    await withDatabase(t.name, async (db) => {
        const model = createMultiCollectionModel("test", {
            schema: {
                user: {
                    name: v.string(),
                },
                group: {
                    name: v.string(),
                }
            }
        });

        const collection = await multiCollection(db, "test", model);

        // Insert test data
        await collection.insertOne("user", { name: "John" });
        await collection.insertOne("group", { name: "John" });

        // Delete users with name "John"
        const deletedCount = await collection.deleteMany("user", { name: "John" });
        assertEquals(deletedCount, 1);

        // Verify only user was deleted, not group
        const remainingUsers = await collection.find("user");
        assertEquals(remainingUsers.length, 0);

        const remainingGroups = await collection.find("group");
        assertEquals(remainingGroups.length, 1);
        assertEquals(remainingGroups[0].name, "John");
    });
});

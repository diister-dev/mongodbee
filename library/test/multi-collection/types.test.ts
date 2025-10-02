import * as v from "../../src/schema.ts";
import { assertEquals } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { createMultiCollectionModel } from "../../src/multi-collection-model.ts";

Deno.test("Types test - deleteMany and deleteAny should compile", async (t) => {
    await withDatabase(t.name, async (db) => {
        const testModel = createMultiCollectionModel("test", {
            schema: {
                user: {
                    name: v.string(),
                    active: v.boolean(),
                },
                group: {
                    name: v.string(),
                    active: v.boolean(),
                }
            }
        });

        const collection = await multiCollection(db, "test", testModel);

        // Insert test data
        await collection.insertOne("user", { name: "John", active: true });
        await collection.insertOne("group", { name: "Admins", active: false });

        // Test that deleteMany accepts correct filter types
        const deletedUsers = await collection.deleteMany("user", { active: true });
        assertEquals(deletedUsers, 1);

        // Test that deleteAny accepts any filter (dangerous but should compile)
        const deletedAny = await collection.deleteAny({ active: false });
        assertEquals(deletedAny, 1);

        // Verify all documents are deleted
        const remainingUsers = await collection.find("user");
        const remainingGroups = await collection.find("group");
        assertEquals(remainingUsers.length, 0);
        assertEquals(remainingGroups.length, 0);
    });
});

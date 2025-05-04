import * as v from "../src/schema.ts";
import { assertEquals } from "jsr:@std/assert";
import { multiCollection } from "../src/multi-collection.ts";
import { withDatabase } from "./+shared.ts";

Deno.test("Basic test", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                mail: v.string(),
            },
            group: {
                name: v.string(),
                members: v.array(v.string()),
            }
        });

        const userA = await collection.insertOne("user", {
            name: "John",
            mail: "john@doe.d"
        });

        const userB = await collection.insertOne("user", {
            name: "Jane",
            mail: "jane@doe.d"
        });

        await collection.insertOne("group", {
            name: "John",
            members: [userA]
        });

        const users = await collection.find("user");
        assertEquals(users.length, 2);

        const groups = await collection.find("group");
        assertEquals(groups.length, 1);

        const findUserB = await collection.findOne("user", { _id: userB });
        assertEquals(findUserB, {
            _id: userB,
            name: "Jane",
            mail: "jane@doe.d"
        });

        const deleteUserB = await collection.deleteOne("user", { _id: userB });
        assertEquals(deleteUserB, 1);
    });
});

Deno.test("Only delete valid target", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                mail: v.string(),
            },
            group: {
                name: v.string(),
                members: v.array(v.string()),
            }
        });

        const userA = await collection.insertOne("user", {
            name: "John",
            mail: "john@doe.d"
        });

        await collection.insertOne("group", {
            name: "John",
            members: [userA]
        });

        await collection.insertOne("user", {
            name: "John",
            mail: "jane@doe.d"
        });

        const deleteUserB = await collection.deleteOne("user", { name: "John" });
        assertEquals(deleteUserB, 1);

        const groups = await collection.find("group");
        assertEquals(groups.length, 1);

        const deleteUserA = await collection.deleteOne("user", { name: "John" });
        assertEquals(deleteUserA, 1);

        const groupsAfter = await collection.find("group");
        assertEquals(groupsAfter.length, 1);

        const usersAfter = await collection.find("user");
        assertEquals(usersAfter.length, 0);

        // Ensure that we can't delete a group with the same name as a user, if there is no user
        {
            const deleteRandom = await collection.deleteOne("user", { name: "John" }).catch(() => 0);
            assertEquals(deleteRandom, 0);

            const groupsAfter = await collection.find("group");
            assertEquals(groupsAfter.length, 1);

            const usersAfter = await collection.find("user");
            assertEquals(usersAfter.length, 0);
        }
    });
});
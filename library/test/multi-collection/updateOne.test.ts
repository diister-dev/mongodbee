import * as v from "../../src/schema.ts";
import { assertEquals, assertRejects } from "jsr:@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import assert from "node:assert";

Deno.test("UpdateOne: Basic update test", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                mail: v.string(),
                age: v.number(),
            },
            group: {
                name: v.string(),
                members: v.array(v.string()),
                metadata: v.object({
                    createdAt: v.string(),
                    type: v.string(),
                }),
            }
        });

        // Insert a user to update later
        const userId = await collection.insertOne("user", {
            name: "John",
            mail: "john@example.com",
            age: 30
        });

        // Update simple property
        await collection.updateOne("user", userId, {
            name: "John Smith",
        });

        // Verify update
        const updatedUser = await collection.findOne("user", { _id: userId });
        assert(updatedUser !== null);
        assertEquals(updatedUser.name, "John Smith");
        assertEquals(updatedUser.mail, "john@example.com");
        assertEquals(updatedUser.age, 30);

        // Insert a group with nested object
        const groupId = await collection.insertOne("group", {
            name: "Team A",
            members: [userId],
            metadata: {
                createdAt: "2023-01-01",
                type: "public"
            }
        });

        // Update nested property
        await collection.updateOne("group", groupId, {
            "metadata.type": "private",
        });

        // Verify nested update
        const updatedGroup = await collection.findOne("group", { _id: groupId });
        assert(updatedGroup !== null);
        assertEquals(updatedGroup.name, "Team A");
        assertEquals(updatedGroup.metadata.createdAt, "2023-01-01");
        assertEquals(updatedGroup.metadata.type, "private");
    });
});

Deno.test("UpdateOne: Array updates test", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            group: {
                name: v.string(),
                members: v.array(v.string()),
                tags: v.array(v.string()),
                nested: v.object({
                    key: v.string(),
                    value: v.string()
                }),
                nestedData: v.array(v.object({
                    key: v.string(),
                    value: v.string()
                }))
            }
        });

        // Create test users
        const members = [
            "user:abc123",
            "user:def456"
        ];

        // Insert a group with array
        const groupId = await collection.insertOne("group", {
            name: "Team A",
            members,
            tags: ["important", "active"],
            nestedData: [
                { key: "location", value: "office" },
                { key: "priority", value: "high" }
            ],
            nested: {
                key: "status",
                value: "active"
            },
        });

        // Update array element
        await collection.updateOne("group", groupId, {
            tags: ["important", "inactive"],
        });

        // Update nested object in array
        await collection.updateOne("group", groupId, {
            "nestedData.0.value": "home",
        });

        // Verify array updates
        const updatedGroup = await collection.findOne("group", { _id: groupId });
        assert(updatedGroup !== null);
        assertEquals(updatedGroup.tags, ["important", "inactive"]);
        assertEquals(updatedGroup.nestedData[0].value, "home");
        assertEquals(updatedGroup.nestedData[0].key, "location");
        assertEquals(updatedGroup.nestedData[1].key, "priority");
    });
});

Deno.test("UpdateOne: Non-existent document", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                age: v.number(),
            }
        });

        // Try to update non-existent document
        await assertRejects(
            async () => {
                await collection.updateOne("user", "user:nonexistent", {
                    name: "Updated Name"
                });
            },
            Error,
            "No element that match the filter to update"
        );
    });
});

Deno.test("UpdateOne: Invalid id format test", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                age: v.number(),
            }
        });

        // Insert test user
        const userId = await collection.insertOne("user", {
            name: "John",
            age: 30
        });

        // Try to update with wrong id format
        await assertRejects(
            async () => {
                await collection.updateOne("user", "invalidformat", {
                    name: "John Smith"
                });
            }
        );

        // Try to update with id from wrong collection type
        await assertRejects(
            async () => {
                await collection.updateOne("user", "group:abc123", {
                    name: "John Smith"
                });
            }
        );
    });
});

Deno.test("UpdateOne: Support optional object entry", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                age: v.number(),
                address: v.optional(v.object({
                    city: v.string(),
                    country: v.string()
                }))
            }
        });

        // Insert test user
        const userId = await collection.insertOne("user", {
            name: "John",
            age: 30,
        });

        // Update with optional field
        await collection.updateOne("user", userId, {
            address: {
                city: "New York",
                country: "USA"
            }
        });

        // Verify update
        const updatedUser = await collection.findOne("user", { _id: userId });
        assert(updatedUser !== null);
        assertEquals(updatedUser.name, "John");
        assertEquals(updatedUser.age, 30);
        assertEquals(updatedUser.address?.city, "New York");
        assertEquals(updatedUser.address?.country, "USA");
    });
});

Deno.test("UpdateOne: Multiple updates at once", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                email: v.string(),
                profile: v.object({
                    age: v.number(),
                    address: v.object({
                        city: v.string(),
                        country: v.string(),
                    })
                }),
                tags: v.array(v.string())
            }
        });

        // Insert test user with nested structure
        const userId = await collection.insertOne("user", {
            name: "John Doe",
            email: "john@example.com",
            profile: {
                age: 30,
                address: {
                    city: "New York",
                    country: "USA"
                }
            },
            tags: ["developer", "admin"]
        });

        // Update multiple fields at different levels in a single call
        await collection.updateOne("user", userId, {
            name: "Jane Doe",
            "profile.age": 28,
            "profile.address.city": "San Francisco",
            "tags.0": "designer",
        });

        // Verify all updates were applied
        const updatedUser = await collection.findOne("user", { _id: userId });
        assert(updatedUser !== null);
        assertEquals(updatedUser.name, "Jane Doe");
        assertEquals(updatedUser.email, "john@example.com"); // unchanged
        assertEquals(updatedUser.profile.age, 28);
        assertEquals(updatedUser.profile.address.city, "San Francisco");
        assertEquals(updatedUser.profile.address.country, "USA"); // unchanged
        assertEquals(updatedUser.tags[0], "designer");
        assertEquals(updatedUser.tags[1], "admin"); // unchanged
    });
});

Deno.test("UpdateOne: Update Complex array", async (t) => {
    await withDatabase(t.name, async (db) => {
        const collection = await multiCollection(db, "test", {
            user: {
                name: v.string(),
                email: v.string(),
                tags: v.array(v.object({
                    name: v.string(),
                    value: v.string()
                }))
            }
        });

        // Insert test user with nested structure
        const userId = await collection.insertOne("user", {
            name: "John Doe",
            email: "john@example.com",
            tags: [
                { name: "role", value: "admin" },
                { name: "status", value: "active" }
            ]
        });

        // Update multiple fields at different levels in a single call
        await collection.updateOne("user", userId, {
            name: "Jane Doe",
            "tags.0.value": "super-admin",
            "tags.1.name": "state",
        });

        // Verify all updates were applied
        const updatedUser = await collection.findOne("user", { _id: userId });
        assert(updatedUser !== null);
        assertEquals(updatedUser.name, "Jane Doe");
        assertEquals(updatedUser.email, "john@example.com"); // unchanged
        assertEquals(updatedUser.tags[0].name, "role");
        assertEquals(updatedUser.tags[0].value, "super-admin");
        assertEquals(updatedUser.tags[1].name, "state");
        assertEquals(updatedUser.tags[1].value, "active");

        // Change an entire array element
        await collection.updateOne("user", userId, {
            "tags.1": { name: "location", value: "USA" },
            "tags.2": { name: "extra", value: "new" }
        });

        // Verify the entire array element was changed
        const updatedUser2 = await collection.findOne("user", { _id: userId });
        assert(updatedUser2 !== null);
        assertEquals(updatedUser2.tags[0].name, "role");
        assertEquals(updatedUser2.tags[0].value, "super-admin");
        assertEquals(updatedUser2.tags[1].name, "location");
        assertEquals(updatedUser2.tags[1].value, "USA");
        assertEquals(updatedUser2.tags[2].name, "extra");
        assertEquals(updatedUser2.tags[2].value, "new");
        assertEquals(updatedUser2.name, "Jane Doe");
        assertEquals(updatedUser.email, "john@example.com");
    });
});
import * as v from "../src/schema.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { assert, assertEquals } from "@std/assert";
import { MongoClient } from "../src/mongodb.ts";
import { createMultiCollectionModel } from "../src/multi-collection-model.ts";

// Mock MongoDB setup for testing
let client: MongoClient;
let db: ReturnType<MongoClient["db"]>;

async function setupTestDb() {
    const mongoUrl = Deno.env.get("MONGODB_URL") || "mongodb://localhost:27017";
    client = new MongoClient(mongoUrl);
    await client.connect();
    db = client.db("test_deep_sanitization");
}

async function cleanupTestDb() {
    if (db) {
        await db.dropDatabase();
    }
    if (client) {
        await client.close();
    }
}

const deepSchema = {
    name: v.string(),
    profile: v.optional(v.object({
        bio: v.optional(v.string()),
        settings: v.optional(v.object({
            theme: v.optional(v.string()),
            notifications: v.optional(v.object({
                email: v.optional(v.boolean()),
                push: v.optional(v.boolean())
            }))
        })),
        tags: v.optional(v.array(v.string()))
    })),
    metadata: v.optional(v.array(v.object({
        key: v.string(),
        value: v.optional(v.string()),
        nested: v.optional(v.object({
            level1: v.optional(v.string()),
            level2: v.optional(v.object({
                deepValue: v.optional(v.string())
            }))
        }))
    })))
};

Deno.test("Deep sanitization: Collection removes nested undefined values", async () => {
    await setupTestDb();
    
    try {
        const users = await collection(db, "users_deep", deepSchema, {
            undefinedBehavior: 'remove'
        });
        
        await users.insertOne({
            name: "DeepUser",
            profile: {
                bio: "Test bio",
                settings: {
                    theme: undefined,  // Should be removed
                    notifications: {
                        email: true,
                        push: undefined  // Should be removed
                    }
                },
                tags: ["tag1", "tag2"]
            },
            metadata: [
                {
                    key: "key1",
                    value: undefined,  // Should be removed
                    nested: {
                        level1: "value1",
                        level2: {
                            deepValue: undefined  // Should be removed
                        }
                    }
                },
                {
                    key: "key2",
                    value: "value2",
                    nested: undefined  // Should be removed
                }
            ]
        });
        
        const user = await users.findOne({ name: "DeepUser" });
        assert(user !== null);
        
        // Check top level
        assertEquals(user.name, "DeepUser");
        assert("profile" in user);
        assert("metadata" in user);
        
        // Check profile level
        assert(user.profile !== undefined);
        assertEquals(user.profile.bio, "Test bio");
        assert("settings" in user.profile);
        assert("tags" in user.profile);
        
        // Check settings level
        assert(user.profile.settings !== undefined);
        assert(!("theme" in user.profile.settings));  // Should be removed
        assert("notifications" in user.profile.settings);
        
        // Check notifications level
        assert(user.profile.settings.notifications !== undefined);
        assertEquals(user.profile.settings.notifications.email, true);
        assert(!("push" in user.profile.settings.notifications));  // Should be removed
        
        // Check metadata array
        assert(user.metadata !== undefined);
        assertEquals(user.metadata.length, 2);
        
        // Check first metadata item
        const meta1 = user.metadata[0];
        assertEquals(meta1.key, "key1");
        assert(!("value" in meta1));  // Should be removed
        assert("nested" in meta1);
        
        // Check nested in first metadata
        assert(meta1.nested !== undefined);
        assertEquals(meta1.nested.level1, "value1");
        assert("level2" in meta1.nested);
        assert(meta1.nested.level2 !== undefined);
        assert(!("deepValue" in meta1.nested.level2));  // Should be removed
        
        // Check second metadata item
        const meta2 = user.metadata[1];
        assertEquals(meta2.key, "key2");
        assertEquals(meta2.value, "value2");
        assert(!("nested" in meta2));  // Should be removed
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Deep sanitization: MultiCollection removes nested undefined values", async () => {
    await setupTestDb();

    const model = createMultiCollectionModel("deep_docs", {
        schema: {
            users: deepSchema
        }
    })
    
    try {
        const mc = await multiCollection(db, "deep_docs", model, {
            undefinedBehavior: 'remove'
        });
        
        const userId = await mc.insertOne("users", {
            name: "DeepMultiUser",
            profile: {
                bio: undefined,  // Should be removed
                settings: {
                    theme: "dark",
                    notifications: {
                        email: undefined,  // Should be removed
                        push: false
                    }
                },
                tags: undefined  // Should be removed
            },
            metadata: [
                {
                    key: "nested_key",
                    value: "nested_value",
                    nested: {
                        level1: undefined,  // Should be removed
                        level2: {
                            deepValue: "deep!"
                        }
                    }
                }
            ]
        });
        
        const user = await mc.findOne("users", { _id: userId });
        assert(user !== null);
        
        assertEquals(user.name, "DeepMultiUser");
        assert("profile" in user);
        assert("metadata" in user);
        
        // Check profile
        assert(user.profile !== undefined);
        assert(!("bio" in user.profile));  // Should be removed
        assert(!("tags" in user.profile));  // Should be removed
        assert("settings" in user.profile);
        
        // Check settings
        assert(user.profile.settings !== undefined);
        assertEquals(user.profile.settings.theme, "dark");
        assert("notifications" in user.profile.settings);
        
        // Check notifications
        assert(user.profile.settings.notifications !== undefined);
        assert(!("email" in user.profile.settings.notifications));  // Should be removed
        assertEquals(user.profile.settings.notifications.push, false);
        
        // Check metadata
        assert(user.metadata !== undefined);
        assertEquals(user.metadata.length, 1);
        
        const meta = user.metadata[0];
        assertEquals(meta.key, "nested_key");
        assertEquals(meta.value, "nested_value");
        assert("nested" in meta);
        
        // Check deep nested
        assert(meta.nested !== undefined);
        assert(!("level1" in meta.nested));  // Should be removed
        assert("level2" in meta.nested);
        assert(meta.nested.level2 !== undefined);
        assertEquals(meta.nested.level2.deepValue, "deep!");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Deep sanitization: Arrays with undefined items", async () => {
    await setupTestDb();
    
    try {
        const arraySchema = {
            name: v.string(),
            items: v.optional(v.array(v.object({
                id: v.string(),
                data: v.optional(v.string()),
                nested: v.optional(v.object({
                    value: v.optional(v.string())
                }))
            })))
        };
        
        const coll = await collection(db, "array_test", arraySchema, {
            undefinedBehavior: 'remove'
        });
        
        await coll.insertOne({
            name: "ArrayTest",
            items: [
                {
                    id: "1",
                    data: "data1",
                    nested: {
                        value: undefined  // Should be removed
                    }
                },
                {
                    id: "2",
                    data: undefined,  // Should be removed
                    nested: undefined  // Should be removed
                },
                {
                    id: "3",
                    data: "data3",
                    nested: {
                        value: "value3"
                    }
                }
            ]
        });
        
        const doc = await coll.findOne({ name: "ArrayTest" });
        assert(doc !== null);
        
        assert(doc.items !== undefined);
        assertEquals(doc.items.length, 3);
        
        // First item
        assertEquals(doc.items[0].id, "1");
        assertEquals(doc.items[0].data, "data1");
        assert("nested" in doc.items[0]);
        assert(doc.items[0].nested !== undefined);
        assert(!("value" in doc.items[0].nested));  // Should be removed
        
        // Second item
        assertEquals(doc.items[1].id, "2");
        assert(!("data" in doc.items[1]));  // Should be removed
        assert(!("nested" in doc.items[1]));  // Should be removed
        
        // Third item
        assertEquals(doc.items[2].id, "3");
        assertEquals(doc.items[2].data, "data3");
        assert("nested" in doc.items[2]);
        assert(doc.items[2].nested !== undefined);
        assertEquals(doc.items[2].nested.value, "value3");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Deep sanitization: Comparison with shallow mode", async () => {
    await setupTestDb();
    
    try {
        // Test that our fix ensures deep sanitization by default
        const simpleSchema = {
            name: v.string(),
            nested: v.optional(v.object({
                value: v.optional(v.string())
            }))
        };
        
        const coll = await collection(db, "deep_test", simpleSchema, {
            undefinedBehavior: 'remove'
        });
        
        await coll.insertOne({
            name: "Test",
            nested: {
                value: undefined  // Should be removed with deep=true
            }
        });
        
        const doc = await coll.findOne({ name: "Test" });
        assert(doc !== null);
        
        assertEquals(doc.name, "Test");
        assert("nested" in doc);
        assert(doc.nested !== undefined);
        
        // With deep=true (our fix), this undefined should be removed
        assert(!("value" in doc.nested));
        
        // The nested object should still exist but be empty
        assertEquals(Object.keys(doc.nested).length, 0);
        
    } finally {
        await cleanupTestDb();
    }
});

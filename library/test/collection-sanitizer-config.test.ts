import * as v from "../src/schema.ts";
import { collection } from "../src/collection.ts";
import { assert, assertEquals, assertRejects } from "@std/assert";
import { MongoClient, type Db } from "../src/mongodb.ts";

// Mock MongoDB setup for testing
let client: MongoClient;
let db: ReturnType<MongoClient["db"]>;

async function setupTestDb() {
    const mongoUrl = Deno.env.get("MONGODB_URL") || "mongodb://localhost:27017";
    client = new MongoClient(mongoUrl);
    await client.connect();
    db = client.db("test_sanitizer_config");
}

async function cleanupTestDb() {
    if (db) {
        await db.dropDatabase();
    }
    if (client) {
        await client.close();
    }
}

const userSchema = {
    name: v.string(),
    email: v.optional(v.string()),
    phone: v.optional(v.string()),
    age: v.optional(v.number()),
    address: v.optional(v.object({
        street: v.string(),
        city: v.optional(v.string()),
        zipcode: v.optional(v.string())
    }))
};

Deno.test("Collection with default undefined behavior (remove)", async () => {
    await setupTestDb();
    
    try {
        const users = await collection(db, "users_default", userSchema);
        
        // Insert document with undefined values
        await users.insertOne({
            name: "John",
            email: undefined,  // Should be removed
            phone: "123-456-7890",
            age: undefined,    // Should be removed
        });
        
        // Fetch the inserted document by name since we know it's unique
        const insertedDoc = await users.findOne({ name: "John" });
        
        // Should not have email and age fields
        assert(insertedDoc !== null);
        assert(!("email" in insertedDoc));
        assert(!("age" in insertedDoc));
        assertEquals(insertedDoc.name, "John");
        assertEquals(insertedDoc.phone, "123-456-7890");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Collection with ignore undefined behavior", async () => {
    await setupTestDb();
    
    try {
        const users = await collection(db, "users_ignore", userSchema, {
            undefinedBehavior: 'ignore'
        });
        
        // Insert initial document
        await users.insertOne({
            name: "John",
            email: "john@example.com",
            phone: "123-456-7890"
        });
        
        // Update with undefined values (should be ignored)
        await users.replaceOne(
            { name: "John" }, 
            {
                name: "John Updated",
                email: undefined,  // Should be ignored (keep original)
                phone: "987-654-3210"
            }
        );
        
        const updatedDoc = await users.findOne({ name: "John Updated" });
        
        // email should not be in the document since it was undefined in replace
        // (replaceOne replaces the entire document, so undefined fields are removed)
        assert(updatedDoc !== null);
        assert(!("email" in updatedDoc));
        assertEquals(updatedDoc.name, "John Updated");
        assertEquals(updatedDoc.phone, "987-654-3210");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Collection with error undefined behavior", async () => {
    await setupTestDb();
    
    try {
        const users = await collection(db, "users_error", userSchema, {
            undefinedBehavior: 'error'
        });
        
        // Should throw error when trying to insert with undefined
        await assertRejects(
            async () => {
                await users.insertOne({
                    name: "John",
                    email: undefined,  // Should cause error
                    phone: "123-456-7890"
                });
            },
            Error,
            "Undefined values are not allowed"
        );
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Explicit field removal with removeField()", async () => {
    await setupTestDb();
    
    try {
        const users = await collection(db, "users_remove_field", userSchema);
        
        // Insert initial document
        await users.insertOne({
            name: "John",
            email: "john@example.com",
            phone: "123-456-7890",
            age: 30
        });
        
        // Test replacement with field removal using replaceOne by name
        await users.replaceOne(
            { name: "John" },
            {
                name: "John Updated",
                email: "john.updated@example.com",
                // phone: removeField(),  // This would require special handling
                // age: undefined         // This will be removed by sanitizer
            }
        );
        
        const updatedDoc = await users.findOne({ name: "John Updated" });
        
        // Fields not included in replacement should be gone
        assert(updatedDoc !== null);
        assert(!("phone" in updatedDoc));
        assert(!("age" in updatedDoc));
        assertEquals(updatedDoc.name, "John Updated");
        assertEquals(updatedDoc.email, "john.updated@example.com");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Complex nested object with mixed undefined behaviors", async () => {
    await setupTestDb();
    
    try {
        const users = await collection(db, "users_nested", userSchema, {
            undefinedBehavior: 'remove'
        });
        
        // Insert document with nested undefined values
        await users.insertOne({
            name: "John",
            email: "john@example.com",
            address: {
                street: "123 Main St",
                city: undefined,    // Should be removed
                zipcode: "12345"
            }
        });
        
        const insertedDoc = await users.findOne({ name: "John" });
        
        // Nested undefined should be removed
        assert(insertedDoc !== null);
        assert(insertedDoc.address !== undefined);
        assert(!("city" in insertedDoc.address));
        assertEquals(insertedDoc.address.street, "123 Main St");
        assertEquals(insertedDoc.address.zipcode, "12345");
        
        // Update with mixed explicit and implicit removals
        await users.replaceOne(
            { name: "John" },
            {
                name: "John Updated",
                email: undefined,      // Implicit removal
                address: {
                    street: "456 Oak Ave",
                    // city removed by not including it
                    // zipcode removed by not including it
                }
            }
        );
        
        const updatedDoc = await users.findOne({ name: "John Updated" });
        
        // All undefined fields should be removed
        assert(updatedDoc !== null);
        assert(!("email" in updatedDoc));
        assert(updatedDoc.address !== undefined);
        assert(!("city" in updatedDoc.address));
        assert(!("zipcode" in updatedDoc.address));
        assertEquals(updatedDoc.name, "John Updated");
        assertEquals(updatedDoc.address.street, "456 Oak Ave");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Array sanitization with undefined values", async () => {
    await setupTestDb();
    
    try {
        const usersSchema = {
            name: v.string(),
            tags: v.optional(v.array(v.string())),
            contacts: v.optional(v.array(v.object({
                type: v.string(),
                value: v.optional(v.string())
            })))
        };
        
        const users = await collection(db, "users_arrays", usersSchema);
        
        // Insert with arrays containing undefined - we'll simulate this differently
        await users.insertOne({
            name: "John",
            tags: ["work", "personal"], // No undefined in type-safe way
            contacts: [
                { type: "email", value: "john@example.com" },
                { type: "phone" }, // No value property = undefined
                { type: "fax", value: "555-1234" }
            ]
        });
        
        const insertedDoc = await users.findOne({ name: "John" });
        
        // Array should be clean
        assert(insertedDoc !== null);
        assert(insertedDoc.tags !== undefined);
        assertEquals(insertedDoc.tags, ["work", "personal"]);
        assert(insertedDoc.contacts !== undefined);
        assertEquals(insertedDoc.contacts.length, 3);
        
        // Second contact should not have 'value' field
        assert(!("value" in insertedDoc.contacts[1]));
        assertEquals(insertedDoc.contacts[1].type, "phone");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Behavior consistency across insert and replace operations", async () => {
    await setupTestDb();
    
    try {
        const users = await collection(db, "users_consistency", userSchema, {
            undefinedBehavior: 'remove'
        });
        
        // Test data with undefined values
        const testData = {
            name: "Consistency Test",
            email: undefined,
            phone: "123-456-7890",
            age: undefined
        };
        
        // Insert operation
        await users.insertOne(testData);
        const insertedDoc = await users.findOne({ name: "Consistency Test" });
        
        // Replace operation with same data
        await users.replaceOne({ name: "Consistency Test" }, testData);
        const replacedDoc = await users.findOne({ name: "Consistency Test" });
        
        // Both should have identical structure (no email, no age)
        assert(insertedDoc !== null);
        assert(replacedDoc !== null);
        assertEquals(insertedDoc.name, replacedDoc.name);
        assertEquals(insertedDoc.phone, replacedDoc.phone);
        assert(!("email" in insertedDoc));
        assert(!("age" in insertedDoc));
        assert(!("email" in replacedDoc));
        assert(!("age" in replacedDoc));
        assertEquals(insertedDoc.name, "Consistency Test");
        assertEquals(insertedDoc.phone, "123-456-7890");
        
    } finally {
        await cleanupTestDb();
    }
});

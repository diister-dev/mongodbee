import * as v from "../src/schema.ts";
import { collection } from "../src/collection.ts";
import { assert, assertEquals, assertRejects } from "jsr:@std/assert";
import { MongoClient } from "../src/mongodb.ts";

// Mock MongoDB setup for testing
let client: MongoClient;
let db: ReturnType<MongoClient["db"]>;

async function setupTestDb() {
    const mongoUrl = Deno.env.get("MONGODB_URL") || "mongodb://localhost:27017";
    client = new MongoClient(mongoUrl);
    await client.connect();
    db = client.db("test_multi_collection_sanitizer");
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
    status: v.optional(v.null())
};

const productSchema = {
    name: v.string(),
    price: v.number(),
    description: v.optional(v.string()),
    category: v.optional(v.string()),
    tags: v.optional(v.array(v.string()))
};

Deno.test("Multi-collection: Different undefined behaviors", async () => {
    await setupTestDb();
    
    try {
        // Collection 1: Remove undefined values (default)
        const users = await collection(db, "users", userSchema, {
            undefinedBehavior: 'remove'
        });
        
        // Collection 2: Error on undefined values
        const strictProducts = await collection(db, "strict_products", productSchema, {
            undefinedBehavior: 'error'
        });
        
        // Collection 3: Default behavior (should be 'remove')
        const products = await collection(db, "products", productSchema);
        
        // Test users collection (remove behavior)
        await users.insertOne({
            name: "John",
            email: undefined,  // Should be removed
            phone: "123-456-7890",
            age: undefined     // Should be removed
        });
        
        const insertedUser = await users.findOne({ name: "John" });
        assert(insertedUser !== null);
        assert(!("email" in insertedUser));
        assert(!("age" in insertedUser));
        assertEquals(insertedUser.name, "John");
        assertEquals(insertedUser.phone, "123-456-7890");
        
        // Test products collection (default = remove behavior)
        await products.insertOne({
            name: "Laptop",
            price: 999.99,
            description: undefined,  // Should be removed
            category: "Electronics"
        });
        
        const insertedProduct = await products.findOne({ name: "Laptop" });
        assert(insertedProduct !== null);
        assert(!("description" in insertedProduct));
        assertEquals(insertedProduct.name, "Laptop");
        assertEquals(insertedProduct.price, 999.99);
        assertEquals(insertedProduct.category, "Electronics");
        
        // Test strict products collection (error behavior)
        await assertRejects(
            async () => {
                await strictProducts.insertOne({
                    name: "Mouse",
                    price: 29.99,
                    description: undefined,  // Should cause error
                    category: "Electronics"
                });
            },
            Error,
            "Undefined values are not allowed"
        );
        
        // But it should work fine without undefined values
        await strictProducts.insertOne({
            name: "Mouse",
            price: 29.99,
            category: "Electronics"
        });
        
        const insertedStrictProduct = await strictProducts.findOne({ name: "Mouse" });
        assert(insertedStrictProduct !== null);
        assertEquals(insertedStrictProduct.name, "Mouse");
        assertEquals(insertedStrictProduct.price, 29.99);
        assertEquals(insertedStrictProduct.category, "Electronics");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Multi-collection: Same schema, different configurations", async () => {
    await setupTestDb();
    
    try {
        // Same schema, different undefined behaviors
        const strictUsers = await collection(db, "strict_users", userSchema, {
            undefinedBehavior: 'error'
        });
        
        const lenientUsers = await collection(db, "lenient_users", userSchema, {
            undefinedBehavior: 'remove'
        });
        
        const testData = {
            name: "Alice",
            email: "alice@example.com",
            phone: undefined,  // This is the key difference
            age: 25
        };
        
        // Strict collection should reject undefined
        await assertRejects(
            async () => {
                await strictUsers.insertOne(testData);
            },
            Error,
            "Undefined values are not allowed"
        );
        
        // Lenient collection should accept and remove undefined
        await lenientUsers.insertOne(testData);
        
        const insertedUser = await lenientUsers.findOne({ name: "Alice" });
        assert(insertedUser !== null);
        assert(!("phone" in insertedUser));
        assertEquals(insertedUser.name, "Alice");
        assertEquals(insertedUser.email, "alice@example.com");
        assertEquals(insertedUser.age, 25);
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Multi-collection: insertMany with different behaviors", async () => {
    await setupTestDb();
    
    try {
        const users = await collection(db, "users_many", userSchema, {
            undefinedBehavior: 'remove'
        });
        
        const strictUsers = await collection(db, "strict_users_many", userSchema, {
            undefinedBehavior: 'error'
        });
        
        const testData = [
            {
                name: "User1",
                email: "user1@example.com",
                phone: undefined,  // Will be removed in lenient, error in strict
                age: 30
            },
            {
                name: "User2",
                email: undefined,  // Will be removed in lenient, error in strict
                phone: "123-456-7890",
                age: 25
            }
        ];
        
        // Lenient collection should work
        await users.insertMany(testData);
        
        const insertedUsers = await users.find({}).toArray();
        assertEquals(insertedUsers.length, 2);
        
        // Check first user
        const user1 = insertedUsers.find(u => u.name === "User1");
        assert(user1 !== undefined);
        assert(!("phone" in user1));
        assertEquals(user1.email, "user1@example.com");
        
        // Check second user  
        const user2 = insertedUsers.find(u => u.name === "User2");
        assert(user2 !== undefined);
        assert(!("email" in user2));
        assertEquals(user2.phone, "123-456-7890");
        
        // Strict collection should reject
        await assertRejects(
            async () => {
                await strictUsers.insertMany(testData);
            },
            Error,
            "Undefined values are not allowed"
        );
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Multi-collection: replaceOne with different behaviors", async () => {
    await setupTestDb();
    
    try {
        const users = await collection(db, "users_replace", userSchema, {
            undefinedBehavior: 'remove'
        });
        
        const strictUsers = await collection(db, "strict_users_replace", userSchema, {
            undefinedBehavior: 'error'
        });
        
        // Insert initial data
        await users.insertOne({
            name: "TestUser",
            email: "test@example.com",
            phone: "123-456-7890",
            age: 30
        });
        
        await strictUsers.insertOne({
            name: "TestUser",
            email: "test@example.com",
            phone: "123-456-7890",
            age: 30
        });
        
        const updateData = {
            name: "TestUser Updated",
            email: "updated@example.com",
            phone: undefined,  // This will cause different behaviors
            age: 35
        };
        
        // Lenient collection should work (remove undefined)
        await users.replaceOne({ name: "TestUser" }, updateData);
        
        const updatedUser = await users.findOne({ name: "TestUser Updated" });
        assert(updatedUser !== null);
        assert(!("phone" in updatedUser));
        assertEquals(updatedUser.email, "updated@example.com");
        assertEquals(updatedUser.age, 35);
        
        // Strict collection should reject undefined
        await assertRejects(
            async () => {
                await strictUsers.replaceOne({ name: "TestUser" }, updateData);
            },
            Error,
            "Undefined values are not allowed"
        );
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Multi-collection: Cross-collection data consistency", async () => {
    await setupTestDb();
    
    try {
        // Create collections with different behaviors
        const mainUsers = await collection(db, "main_users", userSchema, {
            undefinedBehavior: 'remove'
        });
        
        const auditUsers = await collection(db, "audit_users", userSchema, {
            undefinedBehavior: 'error'
        });
        
        // Data that works in main but not in audit
        const userData = {
            name: "CrossCollectionUser",
            email: "cross@example.com",
            phone: undefined,  // Problematic for audit
            age: 28
        };
        
        // Insert into main collection (should work)
        await mainUsers.insertOne(userData);
        
        const mainUser = await mainUsers.findOne({ name: "CrossCollectionUser" });
        assert(mainUser !== null);
        assert(!("phone" in mainUser));
        
        // Trying to insert same data into audit should fail
        await assertRejects(
            async () => {
                await auditUsers.insertOne(userData);
            },
            Error,
            "Undefined values are not allowed"
        );
        
        // But we can sanitize data for audit by removing undefined fields first
        const cleanUserData = {
            name: userData.name,
            email: userData.email,
            age: userData.age
            // phone is not included
        };
        
        await auditUsers.insertOne(cleanUserData);
        
        const auditUser = await auditUsers.findOne({ name: "CrossCollectionUser" });
        assert(auditUser !== null);
        assert(!("phone" in auditUser));
        assertEquals(auditUser.name, "CrossCollectionUser");
        assertEquals(auditUser.email, "cross@example.com");
        assertEquals(auditUser.age, 28);
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Multi-collection: Performance with large number of collections", async () => {
    await setupTestDb();
    
    try {
        const collections = [];
        const collectionCount = 5; // Keep it reasonable for testing
        
        // Create multiple collections with different configurations
        for (let i = 0; i < collectionCount; i++) {
            const collectionName = `perf_collection_${i}`;
            const undefinedBehavior = i % 2 === 0 ? 'remove' : 'error';
            
            const coll = await collection(db, collectionName, userSchema, {
                undefinedBehavior
            });
            
            collections.push({ coll, undefinedBehavior, name: collectionName });
        }
        
        // Test each collection with appropriate data
        for (let i = 0; i < collections.length; i++) {
            const { coll, undefinedBehavior } = collections[i];
            
            if (undefinedBehavior === 'remove') {
                // Can insert data with undefined
                await coll.insertOne({
                    name: `User_${i}`,
                    email: `user${i}@example.com`,
                    phone: undefined,
                    age: 20 + i
                });
                
                const user = await coll.findOne({ name: `User_${i}` });
                assert(user !== null);
                assert(!("phone" in user));
                
            } else {
                // Must insert clean data
                await coll.insertOne({
                    name: `User_${i}`,
                    email: `user${i}@example.com`,
                    age: 20 + i
                });
                
                const user = await coll.findOne({ name: `User_${i}` });
                assert(user !== null);
                assertEquals(user.name, `User_${i}`);
            }
        }
        
        // Verify all collections work independently
        assertEquals(collections.length, collectionCount);
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Multi-collection: Mixed undefined behaviors in transactions", async () => {
    await setupTestDb();
    
    try {
        const lenientCollection = await collection(db, "lenient_tx", userSchema, {
            undefinedBehavior: 'remove'
        });
        
        const strictCollection = await collection(db, "strict_tx", userSchema, {
            undefinedBehavior: 'error'
        });
        
        // Test that each collection maintains its own behavior even in complex scenarios
        const testData = {
            name: "TxUser",
            email: "tx@example.com",
            phone: undefined,
            age: 30
        };
        
        // This should work for lenient
        await lenientCollection.insertOne(testData);
        
        const lenientUser = await lenientCollection.findOne({ name: "TxUser" });
        assert(lenientUser !== null);
        assert(!("phone" in lenientUser));
        
        // This should fail for strict
        await assertRejects(
            async () => {
                await strictCollection.insertOne(testData);
            },
            Error,
            "Undefined values are not allowed"
        );
        
        // Verify lenient still works after strict failed
        await lenientCollection.insertOne({
            name: "TxUser2",
            email: undefined,
            phone: "123-456-7890",
            age: 25
        });
        
        const lenientUser2 = await lenientCollection.findOne({ name: "TxUser2" });
        assert(lenientUser2 !== null);
        assert(!("email" in lenientUser2));
        assertEquals(lenientUser2.phone, "123-456-7890");
        
    } finally {
        await cleanupTestDb();
    }
});

Deno.test("Multi-collection: Null validation", async () => {
    await setupTestDb();
    
    try {
        const users = await collection(db, "null_test_users", userSchema, {
            undefinedBehavior: 'remove'
        });
        
        // Test inserting with null status
        await users.insertOne({
            name: "NullUser",
            email: "null@example.com",
            phone: "123-456-7890",
            age: 30,
            status: null
        });
        
        const insertedUser = await users.findOne({ name: "NullUser" });
        assert(insertedUser !== null);
        assertEquals(insertedUser.status, null);
        assertEquals(insertedUser.name, "NullUser");
        
        // Test inserting without status (should be undefined, not null)
        await users.insertOne({
            name: "NoStatusUser",
            email: "nostatus@example.com",
            age: 25
        });
        
        const noStatusUser = await users.findOne({ name: "NoStatusUser" });
        assert(noStatusUser !== null);
        assert(!("status" in noStatusUser)); // status should not be present
        
        // Test updating to null
        await users.updateOne(
            { name: "NoStatusUser" },
            { $set: { status: null } }
        );
        
        const updatedUser = await users.findOne({ name: "NoStatusUser" });
        assert(updatedUser !== null);
        assertEquals(updatedUser.status, null);
        
    } finally {
        await cleanupTestDb();
    }
});

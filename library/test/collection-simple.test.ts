import { assert, assertEquals } from "jsr:@std/assert";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";

// Simple test schema
const userSchema = {
    name: v.string(),
    age: v.number(),
    email: v.optional(v.string()),
} as const;

Deno.test("Collection: Basic operations coverage", async (t) => {
    await withDatabase(t.name, async (db) => {
        const users = await collection(db, "users", userSchema);
        
        // Test insertOne
        const _insertResult = await users.insertOne({
            name: "John",
            age: 30,
            email: "john@example.com"
        });
        
        // Test findOne
        const found = await users.findOne({ name: "John" });
        assert(found);
        assertEquals(found.name, "John");
        
        // Test find
        const allUsers = await users.find({}).toArray();
        assertEquals(allUsers.length, 1);
        
        // Test updateOne
        await users.updateOne({ name: "John" }, { $set: { age: 31 } });
        const updated = await users.findOne({ name: "John" });
        assertEquals(updated?.age, 31);
        
        // Test deleteOne
        const deleteResult = await users.deleteOne({ name: "John" });
        assertEquals(deleteResult.deletedCount, 1);
        
        // Test that document was deleted
        const countAfterDelete = await users.countDocuments({ name: "John" });
        assertEquals(countAfterDelete, 0);
        
        // Test insertMany
        await users.insertMany([
            { name: "Alice", age: 25 },
            { name: "Bob", age: 35 }
        ]);
        
        // Test count
        const count = await users.countDocuments({});
        assertEquals(count, 2);
        
        // Test distinct
        const names = await users.distinct("name", {});
        assertEquals(names.length, 2);
        
        // Test aggregate
        const pipeline = [{ $match: { age: { $gte: 30 } } }];
        const results = await users.aggregate(pipeline).toArray();
        assertEquals(results.length, 1);
        
        // Test replaceOne
        await users.replaceOne({ name: "Alice" }, { name: "Alice", age: 26 });
        const replaced = await users.findOne({ name: "Alice" });
        assertEquals(replaced?.age, 26);
        
        // Test updateMany
        await users.updateMany({}, { $set: { email: "updated@example.com" } });
        const allUpdated = await users.find({}).toArray();
        allUpdated.forEach(user => {
            assertEquals(user.email, "updated@example.com");
        });
        
        // Test deleteMany
        await users.deleteMany({ age: { $gte: 0 } }); // Delete all documents
        const finalCount = await users.countDocuments({});
        assertEquals(finalCount, 0);
    });
});

Deno.test("Collection: Error handling coverage", async (t) => {
    await withDatabase(t.name, async (db) => {
        const users = await collection(db, "users", userSchema);
        
        // Test updateOne with non-existent document
        const updateResult = await users.updateOne({ name: "NonExistent" }, { $set: { age: 40 } });
        assertEquals(updateResult.modifiedCount, 0);
        
        // Test deleteOne with non-existent document
        const deleteResult = await users.deleteOne({ name: "NonExistent" });
        assertEquals(deleteResult.deletedCount, 0);
        
        // Test replaceOne with non-existent document
        const replaceResult = await users.replaceOne({ name: "NonExistent" }, { name: "New", age: 20 });
        assertEquals(replaceResult.modifiedCount, 0);
        
        // Test count with empty collection
        const emptyCount = await users.countDocuments({});
        assertEquals(emptyCount, 0);
    });
});

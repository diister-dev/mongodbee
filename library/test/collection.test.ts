import * as v from "../src/schema.ts";
import { assertEquals, assertExists } from "jsr:@std/assert";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import { MongoClient } from "mongodb";

Deno.test("Collection watcher events test", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Test variables to track events
    const events: { [key: string]: number } = {
      insert: 0,
      update: 0,
      replace: 0,
      delete: 0
    };

    // Define a simple schema
    const userSchema = {
      name: v.string(),
      email: v.string(),
      age: v.number()
    };

    // insert a collection with the schema
    const users = await collection(db, "users", userSchema);
    
    // Register event listeners
    users.on("insert", () => { events.insert++; });
    users.on("update", () => { events.update++; });
    users.on("replace", () => { events.replace++; });
    users.on("delete", () => { events.delete++; });

    // Insert a document - should trigger 'insert' event
    const userId = await users.insertOne({
      name: "John Doe",
      email: "john@example.com",
      age: 30
    });

    // Update the document - should trigger 'update' event
    await users.updateOne(
      { _id: userId },
      { $set: { age: 31 } }
    );

    // Replace the document - should trigger 'replace' event
    await users.replaceOne(
      { _id: userId },
      {
        name: "John Doe",
        email: "john.updated@example.com",
        age: 32
      }
    );

    // Delete the document - should trigger 'delete' event
    await users.deleteOne({ _id: userId });

    // Wait for events to be processed
    // MongoDB change streams might have a slight delay
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Assert events were fired
    assertEquals(events.insert, 1, "Insert event should be triggered once");
    assertEquals(events.update, 1, "Update event should be triggered once");
    assertEquals(events.replace, 1, "Replace event should be triggered once");
    assertEquals(events.delete, 1, "Delete event should be triggered once");
  });
});

Deno.test("Collection watcher event unsubscribe", async (t) => {
  await withDatabase(t.name, async (db) => {
    let insertCount = 0;
    let updateCount = 0;

    // Define a simple schema
    const userSchema = {
      name: v.string(),
      email: v.string()
    };

    // Create a collection with the schema
    const users = await collection(db, "users", userSchema);
    
    // Register event listeners with the returned unsubscribe functions
    const unsubscribeInsert = users.on("insert", () => { insertCount++; });
    const unsubscribeUpdate = users.on("update", () => { updateCount++; });
    
    // Insert a document - both listeners should be triggered
    await users.insertOne({
      name: "Jane Doe",
      email: "jane@example.com"
    });
    
    await users.updateOne(
        { name: "Jane Doe" },
        { $set: { email: "hello@example.com" } }
    );
    
    // Unsubscribe one of the listeners
    unsubscribeInsert();
    
    // Insert another document - only update listener should be triggered
    const user2Id = await users.insertOne({
      name: "Bob Smith",
      email: "bob@example.com"
    });
    
    // Update the document 
    await users.updateOne(
      { _id: user2Id },
      { $set: { email: "bob.updated@example.com" } }
    );
    
    // Wait for events to be processed
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Insert count should still be 1, update count should be 2
    assertEquals(insertCount, 1, "Insert listener should have been called once before unsubscribing");
    assertEquals(updateCount, 2, "Update listener should have been called twice");
    
    // Unsubscribe the other listener
    unsubscribeUpdate();
    
    // Update again, but no listeners should be triggered
    await users.updateOne(
      { _id: user2Id },
      { $set: { email: "bob.final@example.com" } }
    );
    
    // Wait for events to be processed
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Counts should remain the same
    assertEquals(insertCount, 1, "Insert count should remain 1");
    assertEquals(updateCount, 2, "Update count should remain 2");
  });
});

Deno.test("Multiple collections with watchers", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create two different collections and ensure events don't cross-contaminate
    const events = {
      usersInsert: 0,
      postsInsert: 0
    };

    const userSchema = {
      name: v.string()
    };
    
    const postSchema = {
      title: v.string(),
      content: v.string()
    };

    const users = await collection(db, "users", userSchema);
    const posts = await collection(db, "posts", postSchema);
    
    users.on("insert", () => { events.usersInsert++; });
    posts.on("insert", () => { events.postsInsert++; });
    
    await users.insertOne({ name: "User 1" });
    await posts.insertOne({ title: "Post 1", content: "Content 1" });
    await posts.insertOne({ title: "Post 2", content: "Content 2" });

    // Wait for events to be processed
    await new Promise(resolve => setTimeout(resolve, 300));
    
    assertEquals(events.usersInsert, 1, "Should have 1 user insert event");
    assertEquals(events.postsInsert, 2, "Should have 2 post insert events");
  });
});

Deno.test("FinalizationRegistry cleanup test", async (t) => {
  // This test is more theoretical as it's hard to verify garbage collection,
  // but we can check that creating and disposing collections doesn't cause errors
  await withDatabase(t.name, async (db) => {
    for (let i = 0; i < 5; i++) {
      const collName = `test_coll_${i}`;
      const testColl = await collection(db, collName, {
        name: v.string()
      });
      
      // Add some listeners
      const unsub1 = testColl.on("insert", () => {});
      const unsub2 = testColl.on("update", () => {});
      
      // Manually unsubscribe some listeners
      if (i % 2 === 0) {
        unsub1();
      } else {
        unsub2();
      }
      
      // Insert a document to trigger events
      await testColl.insertOne({ name: `Test ${i}` });
      
      // We don't have direct access to the watchers WeakMap, so we're just 
      // verifying that the code runs without errors
      assertExists(testColl);
    }
    
    // Success if we make it here without errors
    assertEquals(true, true);
  });
});

Deno.test("Collection destroy and recreate test", async (t) => {
  await withDatabase(t.name, async (db) => {
    let insertEvents = 0;
    
    // Define a simple schema
    const userSchema = {
      name: v.string(),
      email: v.string()
    };

    // Create a collection with the schema
    let users = await collection(db, "users", userSchema);
    
    // Register event listener
    users.on("insert", () => { insertEvents++; });
    
    // Insert a document - should trigger 'insert' event
    await users.insertOne({
      name: "Test User",
      email: "test@example.com"
    });
    
    // Wait for event to be processed
    await new Promise(resolve => setTimeout(resolve, 300));
    
    assertEquals(insertEvents, 1, "Insert event should be triggered once");
    
    // Drop the collection
    await users.drop();
    
    // Recreate the same collection
    users = await collection(db, "users", userSchema);
    
    // No need to re-register the event listener, it should still be active
    // users.on("insert", () => { insertEvents++; });
    
    // Insert a document in the recreated collection
    await users.insertOne({
      name: "Test User 2",
      email: "test2@example.com"
    });
    
    // Wait for event to be processed
    await new Promise(resolve => setTimeout(resolve, 300));
    
    // Event count should now be 2
    assertEquals(insertEvents, 2, "Insert event should be triggered after collection recreation");
  });
});

Deno.test("Database drop and recreate test", async (t) => {
    await withDatabase(t.name, async (db) => {        
        let insertEvents = 0;
        let updateEvents = 0;
        
        // Define a simple schema
        const userSchema = {
        name: v.string(),
        age: v.number()
        };

        // Create a collection with the schema
        let users = await collection(db, "users", userSchema);
        
        // Register event listeners
        users.on("insert", () => { insertEvents++; });
        users.on("update", () => { updateEvents++; });
        
        // Insert a document - should trigger 'insert' event
        const userId = await users.insertOne({
        name: "John Smith",
        age: 30
        });
        
        // Update the document - should trigger 'update' event
        await users.updateOne(
        { _id: userId },
        { $set: { age: 31 } }
        );
        
        // Wait for events to be processed
        await new Promise(resolve => setTimeout(resolve, 300));
        
        assertEquals(insertEvents, 1, "Insert event should be triggered once");
        assertEquals(updateEvents, 1, "Update event should be triggered once");
        
        // Drop the entire database
        await db.dropDatabase();
        
        users = await collection(db, "users", userSchema);
        
        // Register event listeners again
        users.on("insert", () => { insertEvents++; });
        users.on("update", () => { updateEvents++; });
        
        // Insert and update documents in the recreated collection
        const newUserId = await users.insertOne({
            name: "Jane Smith",
            age: 25
        });
        
        await users.updateOne(
            { _id: newUserId },
            { $set: { age: 26 } }
        );
        
        // Wait for events to be processed
        await new Promise(resolve => setTimeout(resolve, 300));
        
        // Event counts should be updated
        assertEquals(insertEvents, 2, "Insert event should be triggered after database recreation");
        assertEquals(updateEvents, 2, "Update event should be triggered after database recreation");
    });
});

Deno.test("Database cascade creation", async (t) => {
    await withDatabase(t.name, async (db) => {
        // Create a collection with a schema
        const userSchema = {
            name: v.string(),
            email: v.string()
        };

        const users = await collection(db, "users", userSchema);

        // Register event listener
        let insertCount = 0;
        const unsubscribeInsert = users.on("insert", () => {
            insertCount++;
            if(insertCount == 10) {
                unsubscribeInsert();
            } else {
                users.insertOne({
                    name: "Cascade User",
                    email: "cascade@example.com"
                });
            }
        });

        // Start the cascade by inserting the first document
        await users.insertOne({
            name: "Initial User",
            email: "initial@example.com"
        });

        // Wait for events to be processed
        await new Promise(resolve => setTimeout(resolve, 1000));
        // Assert that the cascade stopped after 10 inserts
        assertEquals(insertCount, 10, "Insert event should be triggered 10 times");
        // Check that the collection has 11 documents (1 initial + 10 cascaded)
        const count = await users.countDocuments();
        assertEquals(count, 10, "Collection should have 10 documents after cascade");
    });
});
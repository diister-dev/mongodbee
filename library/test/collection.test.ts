import * as v from "../src/schema.ts";
import { test, expect } from "vitest";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";
import { MongoClient, ObjectId } from "mongodb";
import { closeAllWatchers } from "../src/change-stream.ts";

test("Collection watcher events test", async () => {
  await withDatabase("Collection watcher events test", async (db) => {
    // Test variables to track events
    const events: { [key: string]: number } = {
      insert: 0,
      update: 0,
      replace: 0,
      delete: 0,
    };

    // Define a simple schema
    const userSchema = {
      name: v.string(),
      email: v.string(),
      age: v.number(),
    };

    // insert a collection with the schema and enable watching
    const users = await collection(db, "users", userSchema, {
      enableWatching: true,
    });

    // Register event listeners
    users.on("insert", () => {
      events.insert++;
    });
    users.on("update", () => {
      events.update++;
    });
    users.on("replace", () => {
      events.replace++;
    });
    users.on("delete", () => {
      events.delete++;
    });

    // Insert a document - should trigger 'insert' event
    const userId = await users.insertOne({
      name: "John Doe",
      email: "john@example.com",
      age: 30,
    });

    // Update the document - should trigger 'update' event
    await users.updateOne(
      { _id: new ObjectId(userId) },
      { $set: { age: 31 } },
    );

    // Replace the document - should trigger 'replace' event
    await users.replaceOne(
      { _id: new ObjectId(userId) },
      {
        name: "John Doe",
        email: "john.updated@example.com",
        age: 32,
      },
    );

    // Delete the document - should trigger 'delete' event
    await users.deleteOne({ _id: new ObjectId(userId) });

    // Wait for events to be processed
    // MongoDB change streams might have a slight delay
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Assert events were fired
    expect(events.insert).toEqual(1);
    expect(events.update).toEqual(1);
    expect(events.replace).toEqual(1);
    expect(events.delete).toEqual(1);
  });
});

test("Collection watcher event unsubscribe", async () => {
  await withDatabase("Collection watcher event unsubscribe", async (db) => {
    let insertCount = 0;
    let updateCount = 0;

    // Define a simple schema
    const userSchema = {
      name: v.string(),
      email: v.string(),
    };

    // Create a collection with the schema and enable watching
    const users = await collection(db, "users", userSchema, {
      enableWatching: true,
    });

    // Register event listeners with the returned unsubscribe functions
    const unsubscribeInsert = users.on("insert", () => {
      insertCount++;
    });
    const unsubscribeUpdate = users.on("update", () => {
      updateCount++;
    });

    // Insert a document - both listeners should be triggered
    await users.insertOne({
      name: "Jane Doe",
      email: "jane@example.com",
    });

    await users.updateOne(
      { name: "Jane Doe" },
      { $set: { email: "hello@example.com" } },
    );

    // Unsubscribe one of the listeners
    unsubscribeInsert();

    // Insert another document - only update listener should be triggered
    const user2Id = await users.insertOne({
      name: "Bob Smith",
      email: "bob@example.com",
    });

    // Update the document
    await users.updateOne(
      { _id: new ObjectId(user2Id) },
      { $set: { email: "bob.updated@example.com" } },
    );

    // Wait for events to be processed
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Insert count should still be 1, update count should be 2
    expect(
      insertCount,
    ).toEqual(1);
    expect(
      updateCount,
    ).toEqual(2);

    // Unsubscribe the other listener
    unsubscribeUpdate();

    // Update again, but no listeners should be triggered
    await users.updateOne(
      { _id: new ObjectId(user2Id) },
      { $set: { email: "bob.final@example.com" } },
    );

    // Wait for events to be processed
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Counts should remain the same
    expect(insertCount).toEqual(1);
    expect(updateCount).toEqual(2);
  });
});

test("Multiple collections with watchers", async () => {
  await withDatabase("Multiple collections with watchers", async (db) => {
    // Create two different collections and ensure events don't cross-contaminate
    const events = {
      usersInsert: 0,
      postsInsert: 0,
    };

    const userSchema = {
      name: v.string(),
    };

    const postSchema = {
      title: v.string(),
      content: v.string(),
    };

    const users = await collection(db, "users", userSchema, {
      enableWatching: true,
    });
    const posts = await collection(db, "posts", postSchema, {
      enableWatching: true,
    });

    users.on("insert", () => {
      events.usersInsert++;
    });
    posts.on("insert", () => {
      events.postsInsert++;
    });

    await users.insertOne({ name: "User 1" });
    await posts.insertOne({ title: "Post 1", content: "Content 1" });
    await posts.insertOne({ title: "Post 2", content: "Content 2" });

    // Wait for events to be processed
    await new Promise((resolve) => setTimeout(resolve, 1000));

    expect(events.usersInsert).toEqual(1);
    expect(events.postsInsert).toEqual(2);
  });
});

test("FinalizationRegistry cleanup test", async () => {
  // This test is more theoretical as it's hard to verify garbage collection,
  // but we can check that creating and disposing collections doesn't cause errors
  await withDatabase("FinalizationRegistry cleanup test", async (db) => {
    for (let i = 0; i < 5; i++) {
      const collName = `test_coll_${i}`;
      const testColl = await collection(db, collName, {
        name: v.string(),
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
      expect(testColl).toBeDefined();
    }

    // Success if we make it here without errors
    expect(true).toEqual(true);
  });
});

test("Collection destroy and recreate test", async () => {
  await withDatabase("Collection destroy and recreate test", async (db) => {
    let insertEvents = 0;

    // Define a simple schema
    const userSchema = {
      name: v.string(),
      email: v.string(),
    };

    // Create a collection with the schema and enable watching
    let users = await collection(db, "users", userSchema, {
      enableWatching: true,
    });

    // Register event listener
    users.on("insert", () => {
      insertEvents++;
    });

    // Insert a document - should trigger 'insert' event
    await users.insertOne({
      name: "Test User",
      email: "test@example.com",
    });

    // Wait for event to be processed
    await new Promise((resolve) => setTimeout(resolve, 1000));

    expect(insertEvents).toEqual(1);

    // Drop the collection
    await users.drop();

    // Recreate the same collection with watching enabled
    users = await collection(db, "users", userSchema, { enableWatching: true });

    // No need to re-register the event listener, it should still be active
    // users.on("insert", () => { insertEvents++; });

    // Insert a document in the recreated collection
    await users.insertOne({
      name: "Test User 2",
      email: "test2@example.com",
    });

    // Wait for event to be processed
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Event count should now be 2
    expect(
      insertEvents,
    ).toEqual(2);
  });
});

test("Database drop and recreate test", async () => {
  await withDatabase("Database drop and recreate test", async (db) => {
    let insertEvents = 0;
    let updateEvents = 0;

    // Define a simple schema
    const userSchema = {
      name: v.string(),
      age: v.number(),
    };

    // Create a collection with the schema and enable watching
    let users = await collection(db, "users", userSchema, {
      enableWatching: true,
      schemaManagement: "auto",
    });

    // Register event listeners
    users.on("insert", () => {
      insertEvents++;
    });
    users.on("update", () => {
      updateEvents++;
    });

    // Insert a document - should trigger 'insert' event
    const userId = await users.insertOne({
      name: "John Smith",
      age: 30,
    });

    // Update the document - should trigger 'update' event
    await users.updateOne(
      { _id: new ObjectId(userId) },
      { $set: { age: 31 } },
    );

    // Wait for events to be processed
    await new Promise((resolve) => setTimeout(resolve, 1000));

    expect(insertEvents).toEqual(1);
    expect(updateEvents).toEqual(1);

    // Close all watchers before dropping the database
    await closeAllWatchers(db);

    // Drop the entire database
    await db.dropDatabase();

    users = await collection(db, "users", userSchema, { enableWatching: true, schemaManagement: "auto" });

    // Register event listeners again
    users.on("insert", () => {
      insertEvents++;
    });
    users.on("update", () => {
      updateEvents++;
    });

    // Insert and update documents in the recreated collection
    const newUserId = await users.insertOne({
      name: "Jane Smith",
      age: 25,
    });

    await users.updateOne(
      { _id: new ObjectId(newUserId) },
      { $set: { age: 26 } },
    );

    // Wait for events to be processed
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Event counts should be updated
    expect(
      insertEvents,
    ).toEqual(2);
    expect(
      updateEvents,
    ).toEqual(2);
  });
});

test("Database cascade creation", async () => {
  await withDatabase("Database cascade creation", async (db) => {
    // Create a collection with a schema
    const userSchema = {
      name: v.string(),
      email: v.string(),
    };

    const users = await collection(db, "users", userSchema, {
      enableWatching: true,
    });

    // Register event listener
    let insertCount = 0;
    const unsubscribeInsert = users.on("insert", () => {
      insertCount++;
      if (insertCount == 10) {
        unsubscribeInsert();
      } else {
        users.insertOne({
          name: "Cascade User",
          email: "cascade@example.com",
        });
      }
    });

    // Start the cascade by inserting the first document
    await users.insertOne({
      name: "Initial User",
      email: "initial@example.com",
    });

    // Wait for events to be processed
    await new Promise((resolve) => setTimeout(resolve, 1000));
    // Assert that the cascade stopped after 10 inserts
    expect(insertCount).toEqual(10);
    // Check that the collection has 11 documents (1 initial + 10 cascaded)
    const count = await users.countDocuments();
    expect(
      count,
    ).toEqual(10);
  });
});

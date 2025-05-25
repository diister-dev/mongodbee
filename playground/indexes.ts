import { MongoClient, collection, dbId, multiCollection } from "mongodbee";
import { withIndex } from "../library/src/indexes.ts"
import * as v from "mongodbee/schema.ts";
import { ulid } from "@std/ulid";
import { SchemaNavigator, computePath, createSimpleVisitor } from '../library/src/schema-navigator.ts';

const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);

const db = client.db("test_diivento");

const example = await collection(db, "user", {
    _id: dbId("user"),
    name: v.string(),
    email: withIndex(v.string(), { unique: true, insensitive: false }),
});

await example.collection.deleteMany({});

console.log("Inserting documents...");
await example.insertOne({
    name: "Alice",
    email: "alice@example.com",
});

await example.insertOne({
    name: "Bob",
    email: "blice@example.com",
});

// Delete document multi
await db.dropCollection("multi");

const multiple = await multiCollection(db, "multi", {
  user: {
    _id: dbId("user"),
    name: withIndex(v.string(), { unique: true }),
    email: withIndex(v.string(), { unique: true, insensitive: false }),
  },
  product: {
    _id: dbId("product"),
    name: withIndex(v.string(), { unique: true }),
    price: v.number(),
  },
});

await multiple.insertOne("user", {
  name: "Alice",
  email: "alice@example.com",
});

await multiple.insertOne("product", {
  name: "Alice",
  price: 19.99,
});

await multiple.insertOne("product", {
  name: "Widget",
  price: 20.99,
});

await multiple.insertOne("user", {
  name: "Blice",
  email: "bob@example.com",
});

client.close();
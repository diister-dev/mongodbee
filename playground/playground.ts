import { MongoClient, collection } from "mongodbee";
import * as v from "mongodbee/schema.ts";
import { ulid } from "@std/ulid";

const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);

const db = client.db("test_diivento");

const example = await collection(db, "user", {
    _id: v.optional(v.pipe(v.string(), v.regex(/^user:/)), () => `user:${ulid()}`),
    name: v.optional(v.string(), "John Doe"),
});

await example.collection.deleteMany({});

await example.insertOne({
})

const result = await example.find({}).toArray();
console.log(result);

client.close();
/**
 * mongodbee migration
 * ---
 * This is an empty initial migration file.
 * You can use it as a starting point for your own migrations.
 */

import * as v from "valibot";
import { collection, dbId } from "mongodbee";

function createMigration<T>(content: T): T {
    return content;
}

const parent = null; // No parent for the initial migration

const schema = {
    "+users": {
        _id: dbId("user"),
        firstname: v.string(),
        lastname: v.string(),
    }
}

async function up(migration: any) {
    // Create user collection
    const { db } = migration;
    const userCollection = await collection(db, "+users", schema['+users'], { noInit: true });
    // Seed initial data
    await userCollection.insertMany([
        { firstname: "John", lastname: "Doe" },
        { firstname: "Jane", lastname: "Smith" },
        { firstname: "Alice", lastname: "Johnson" },
        { firstname: "Bob", lastname: "Brown" },
    ]);
}

export default createMigration({
    name: "Initial Migration",
    id: "0000000000000",
    parent,
    schema,
    up,
})
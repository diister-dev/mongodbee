/**
 * Example application demonstrating MongoDBee usage
 *
 * This file shows how to:
 * - Connect to MongoDB and check migration status
 * - Work with regular collections
 * - Work with multi-collections
 * - Work with multi-model instances
 */

import { MongoClient, collection, multiCollection } from "@diister/mongodbee"
import { checkMigrationStatus, discoverMultiCollectionInstances } from "@diister/mongodbee/migration";
import { schemas } from "./schemas.ts"

console.log("🚀 Starting application...\n");

// Connect to MongoDB
const mongoUrl = Deno.env.get("MONGODB_URI")!;
const mongodbName = Deno.env.get("MONGODB_DATABASE")!;
const client = new MongoClient(mongoUrl);
await client.connect();
const db = client.db(mongodbName);

// Check that migrations are up to date
const migrationState = await checkMigrationStatus({ db });
if(!migrationState.ok) {
    console.error("❌ Migration check failed:", migrationState.message);
    Deno.exit(1);
}
console.log("✅ Database migrations are up to date!\n");

// Example: Working with collections
// NOTE: collection() is async, always use await!
// const users = await collection(db, "users", schemas.collections.users);
// const count = await users.countDocuments({});
// console.log(`Total users: ${count}`);
//
// // Find all users (NOTE: find() requires at least {})
// for await (const user of users.find({})) {
//   console.log(`- ${user.name} (${user.email})`);
// }

// Example: Working with multi-collections
// const analytics = await multiCollection(db, "analytics", schemas.multiCollections.analytics);
// // NOTE: API is find(type, query) - NOT find().type()
// const stats = await analytics.find("dailyStats", {});
// console.log(`Found ${stats.length} daily stats`);

// Example: Working with multi-models
// const instances = await discoverMultiCollectionInstances(db, "visitor");
// console.log(`Found ${instances.length} visitor tracking collections`);
// for (const instanceName of instances) {
//   const visitorCol = await multiCollection(db, instanceName, schemas.multiModels.visitor);
//   const entries = await visitorCol.find("entry", {});
//   console.log(`${instanceName}: ${entries.length} visitors`);
// }

console.log("✨ Application completed successfully!");
client.close();
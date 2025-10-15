/**
 * Utility script to list all collections in the database
 *
 * Usage:
 *   deno run -A --env-file=.env scripts/list_collections.ts
 *
 * This helps verify what collections exist after applying migrations.
 */

import { MongoClient } from "mongodb";

const client = new MongoClient(Deno.env.get("MONGODB_URI")!);

try {
  await client.connect();
  const db = client.db(Deno.env.get("MONGODB_DATABASE")!);

  console.log(`📊 Collections in database '${Deno.env.get("MONGODB_DATABASE")}':\n`);

  const collections = await db.listCollections().toArray();

  if (collections.length === 0) {
    console.log("  No collections found");
  } else {
    for (const coll of collections) {
      const collection = db.collection(coll.name);
      const count = await collection.countDocuments({});
      console.log(`  📁 ${coll.name} (${count} document${count !== 1 ? 's' : ''})`);
    }
  }

  console.log(`\n✅ Found ${collections.length} collection(s)`);

} catch (error) {
  console.error("❌ Error listing collections:", error);
  Deno.exit(1);
} finally {
  await client.close();
}

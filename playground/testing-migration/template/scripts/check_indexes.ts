/**
 * Utility script to inspect indexes on a collection
 *
 * Usage:
 *   deno run -A --env-file=.env scripts/check_indexes.ts <collectionName>
 *
 * Example:
 *   deno run -A --env-file=.env scripts/check_indexes.ts users
 *
 * This helps verify that indexes are created/updated correctly after migrations.
 */

import { MongoClient } from "mongodb";

const collectionName = Deno.args[0];

if (!collectionName) {
  console.error("❌ Usage: deno run -A --env-file=.env scripts/check_indexes.ts <collectionName>");
  Deno.exit(1);
}

const client = new MongoClient(Deno.env.get("MONGODB_URI")!);

try {
  await client.connect();
  const db = client.db(Deno.env.get("MONGODB_DATABASE")!);
  const collection = db.collection(collectionName);

  console.log(`🔍 Indexes on collection '${collectionName}':\n`);

  const indexes = await collection.listIndexes().toArray();

  if (indexes.length === 0) {
    console.log("  No indexes found");
  } else {
    for (const idx of indexes) {
      console.log(`📌 Index: ${idx.name}`);
      console.log(`   Key: ${JSON.stringify(idx.key)}`);
      console.log(`   Unique: ${idx.unique || false}`);
      console.log(`   Sparse: ${idx.sparse || false}`);
      if (idx.collation) {
        console.log(`   Collation: ${JSON.stringify(idx.collation)}`);
      }
      console.log();
    }
  }

  console.log(`✅ Found ${indexes.length} index(es)`);

} catch (error) {
  console.error("❌ Error checking indexes:", error);
  Deno.exit(1);
} finally {
  await client.close();
}

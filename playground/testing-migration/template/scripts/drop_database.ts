/**
 * Utility script to drop the test database
 *
 * Usage:
 *   deno run -A --env-file=.env scripts/drop_database.ts
 *
 * This is useful when you want to completely reset your test database
 * and start fresh with migrations.
 */

import { MongoClient } from "mongodb";

const client = new MongoClient(Deno.env.get("MONGODB_URI")!);

try {
  await client.connect();
  const dbName = Deno.env.get("MONGODB_DATABASE")!;

  console.log(`🗑️  Dropping database: ${dbName}`);
  await client.db(dbName).dropDatabase();
  console.log("✅ Database dropped successfully!");

} catch (error) {
  console.error("❌ Error dropping database:", error);
  Deno.exit(1);
} finally {
  await client.close();
}

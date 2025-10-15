import { type Db, MongoClient } from "../src/mongodb.ts";
import { closeAllWatchers } from "../src/change-stream.ts";

const MAX_DB_NAME_LENGTH = 63;
const RANDOM_SIZE = 8;

function computePrefix(prefix: string) {
  // Replace all non-alphanumeric characters with "_"
  const prefixSanitize = prefix.replace(/[^a-zA-Z0-9]/g, "_")
    .toLocaleLowerCase();
  // Remove all leading and trailing "_"
  const prefixFixed = prefixSanitize.replace(/^_+|_+$/g, "");

  const finalName = `@TEST_${prefixFixed}@`;
  return finalName.substring(0, MAX_DB_NAME_LENGTH - RANDOM_SIZE);
}

function randomDBName(prefix: string) {
  return `${computePrefix(prefix)}${
    crypto.randomUUID().replace(/-/g, "").substring(0, RANDOM_SIZE)
  }`;
}

async function deleteTestDatabase(client: MongoClient, prefix = "UNKNOWN") {
  const dbs = await client.db().admin().listDatabases();
  for (const db of dbs.databases) {
    if (db.name.startsWith(computePrefix(prefix))) {
      await client.db(db.name).dropDatabase();
    }
  }
}

export async function withDatabase(
  prefix: string,
  work: (db: Db) => Promise<void>,
) {
  const client = new MongoClient("mongodb://localhost:27017");
  await deleteTestDatabase(client, prefix);
  const db = client.db(randomDBName(prefix));
  try {
    await work(db);
  } finally {
    // Close all change streams before dropping the database
    closeAllWatchers(db);
    await db.dropDatabase(); // Uncomment to debug after test
    await client.close();
  }
}

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

/**
 * Test server URI — overridable so the suite can run against other MongoDB
 * versions (e.g. a dockerized 6.0/7.0 on another port). Planner-dependent
 * verrous are only MEASURED claims on the version they ran against; this is
 * the knob that lets them run elsewhere.
 */
export const TEST_URI = Deno.env.get("MONGODBEE_TEST_URI") ??
  "mongodb://localhost:27017";

export async function withDatabase(
  prefix: string,
  work: (db: Db) => Promise<void>,
) {
  const client = new MongoClient(TEST_URI);
  await deleteTestDatabase(client, prefix);
  const db = client.db(randomDBName(prefix));
  try {
    await work(db);
  } finally {
    // Close all change streams before dropping the database
    await closeAllWatchers(db);
    await db.dropDatabase(); // Uncomment to debug after test
    await client.close();
  }
}

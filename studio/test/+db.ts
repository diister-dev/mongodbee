import process from "node:process";
import { type Db, MongoClient } from "@diister/mongodbee";
import {
  MIGRATION_OPERATIONS_COLLECTION,
  type MigrationOperation,
  VERSION,
} from "@diister/mongodbee/inspect";

const MAX_DB_NAME_LENGTH = 63;
const RANDOM_SIZE = 8;

export const TEST_URI =
  process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017";

function prefixOf(name: string): string {
  const cleaned = name
    .replace(/[^a-zA-Z0-9]/g, "_")
    .toLocaleLowerCase()
    .replace(/^_+|_+$/g, "");
  return `@STUDIO_${cleaned}@`.substring(0, MAX_DB_NAME_LENGTH - RANDOM_SIZE);
}

export async function withDatabase(
  name: string,
  work: (db: Db) => Promise<void>,
): Promise<void> {
  const client = new MongoClient(TEST_URI);
  const prefix = prefixOf(name);
  const existing = await client.db().admin().listDatabases();
  for (const database of existing.databases) {
    if (database.name.startsWith(prefix)) {
      await client.db(database.name).dropDatabase();
    }
  }
  const db = client.db(
    `${prefix}${crypto.randomUUID().replace(/-/g, "").substring(0, RANDOM_SIZE)}`,
  );
  try {
    await work(db);
  } finally {
    await db.dropDatabase();
    await client.close();
  }
}

export async function recordOperation(
  db: Db,
  migrationId: string,
  migrationName: string,
  operation: MigrationOperation["operation"],
  duration?: number,
): Promise<void> {
  const record: Omit<MigrationOperation, "_id"> = {
    migrationId,
    migrationName,
    operation,
    executedAt: new Date(),
    duration,
    status: "success",
    mongodbeeVersion: VERSION,
  };
  await db
    .collection<Omit<MigrationOperation, "_id">>(
      MIGRATION_OPERATIONS_COLLECTION,
    )
    .insertOne(record);
}

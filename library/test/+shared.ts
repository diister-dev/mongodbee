import process from "node:process";
import { type Db, MongoClient } from "../src/mongodb.ts";
import { closeAllWatchers } from "../src/change-stream.ts";

const MAX_DB_NAME_LENGTH = 63;
const RANDOM_SIZE = 8;

function computePrefix(prefix: string) {
  // Replace all non-alphanumeric characters with "_"
  const prefixSanitize = prefix
    .replace(/[^a-zA-Z0-9]/g, "_")
    .toLocaleLowerCase();
  // Remove all leading and trailing "_"
  const prefixFixed = prefixSanitize.replace(/^_+|_+$/g, "");

  const finalName = `@TEST_${prefixFixed}@`;
  return finalName.substring(0, MAX_DB_NAME_LENGTH - RANDOM_SIZE);
}

function randomDBName(prefix: string) {
  return `${computePrefix(prefix)}${crypto
    .randomUUID()
    .replace(/-/g, "")
    .substring(0, RANDOM_SIZE)}`;
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
export const TEST_URI =
  process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017";

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

/**
 * Waits until `predicate` holds, or fails with `label` once `timeoutMs` passes.
 *
 * Change-stream tests used to sleep a fixed second and then assert. That is
 * fragile in both directions: it wastes a second when the events arrive
 * immediately, and it fails when the whole suite is running in one process and
 * the driver needs longer — which is exactly how the watcher tests started
 * failing under `node --test` while passing in isolation.
 *
 * The deadline is deliberately far longer than a change stream needs when the
 * machine is idle: the whole suite runs in one process, and under that load
 * MongoDB has been seen to take tens of seconds to deliver. Note this cannot
 * rescue an event that was never delivered — `enableWatching` returns before
 * the change stream is established, and the driver emits no readiness signal
 * in emitter mode, so a write issued immediately after can be missed outright.
 *
 * @param predicate Condition to wait for.
 * @param label What the caller was waiting for, used in the timeout message.
 * @param timeoutMs Deadline; generous on purpose, since the happy path returns
 *   as soon as the condition holds.
 */
export async function waitUntil(
  predicate: () => boolean,
  label: string,
  timeoutMs = 30_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out after ${timeoutMs}ms waiting for ${label}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

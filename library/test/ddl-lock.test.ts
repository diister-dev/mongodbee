import { test } from "./+harness.ts";
import { assertEquals, assertRejects } from "./+assert.ts";
import { MongoClient } from "../src/mongodb.ts";
import { lockedDatabases, withDatabaseDdlLock } from "../src/ddl-lock.ts";

const client = new MongoClient("mongodb://localhost:27017");
const otherClient = new MongoClient("mongodb://localhost:27017");

function gate() {
  let open: () => void = () => {};
  const opened = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { open, opened };
}

test("withDatabaseDdlLock serialises work on the same database", async () => {
  const db = client.db("ddl_lock_same");
  const first = gate();
  const events: string[] = [];

  const a = withDatabaseDdlLock(db, async () => {
    events.push("a:start");
    await first.opened;
    events.push("a:end");
  });
  const b = withDatabaseDdlLock(db, async () => {
    events.push("b:start");
  });

  await new Promise((resolve) => setTimeout(resolve, 20));
  assertEquals(events, ["a:start"]);

  first.open();
  await Promise.all([a, b]);
  assertEquals(events, ["a:start", "a:end", "b:start"]);
});

test("withDatabaseDdlLock lets different databases proceed concurrently", async () => {
  const first = gate();
  const events: string[] = [];

  const a = withDatabaseDdlLock(client.db("ddl_lock_one"), async () => {
    events.push("one:start");
    await first.opened;
  });
  const b = withDatabaseDdlLock(client.db("ddl_lock_two"), async () => {
    events.push("two:start");
  });

  await b;
  assertEquals(events, ["one:start", "two:start"]);
  first.open();
  await a;
});

test("withDatabaseDdlLock keys by client, not by database name alone", async () => {
  const first = gate();
  const events: string[] = [];

  const a = withDatabaseDdlLock(client.db("ddl_lock_shared"), async () => {
    events.push("client:start");
    await first.opened;
  });
  const b = withDatabaseDdlLock(otherClient.db("ddl_lock_shared"), async () => {
    events.push("other:start");
  });

  await b;
  assertEquals(events, ["client:start", "other:start"]);
  first.open();
  await a;
});

test("withDatabaseDdlLock releases the database after a failure", async () => {
  const db = client.db("ddl_lock_failure");

  await assertRejects(
    () =>
      withDatabaseDdlLock(db, () => Promise.reject(new Error("ddl failed"))),
    Error,
    "ddl failed",
  );

  const result = await withDatabaseDdlLock(db, () => Promise.resolve("next"));
  assertEquals(result, "next");
});

test("withDatabaseDdlLock forgets a database once nothing holds it", async () => {
  const db = client.db("ddl_lock_idle");
  const first = gate();

  const held = withDatabaseDdlLock(db, async () => {
    await first.opened;
  });
  const queued = withDatabaseDdlLock(db, () => Promise.resolve());

  assertEquals(lockedDatabases(client).includes("ddl_lock_idle"), true);
  first.open();
  await Promise.all([held, queued]);
  assertEquals(lockedDatabases(client).includes("ddl_lock_idle"), false);
});

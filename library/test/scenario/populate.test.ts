import process from "node:process";
import * as v from "../../src/schema.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { MongoClient } from "../../src/mongodb.ts";
import { populateDatabase } from "../../src/scenario/mod.ts";
import { assert, assertEquals, assertRejects } from "../+assert.ts";
import { test } from "../+harness.ts";

const TEST_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";

const BIRTH = migrationDefinition("2026_01_01_0900_BIRTH01@birth", "birth", {
  parent: null,
  schemas: { collections: { items: { label: v.string() } } },
  migrate: (b) => b.compile(),
});

test({
  name: "populate: a failed write removes what it inserted into a collection that already existed, and keeps what was there",
  timeout: 60_000,
  fn: async () => {
    const client = new MongoClient(TEST_URI);
    await client.connect();
    const name = `mongodbee_test_populate_${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`;
    try {
      const db = client.db(name);
      const items = db.collection<{ _id: string; label: string }>("items");
      await items.insertOne({ _id: "kept", label: "kept" });
      const state = createEmptyDatabaseState();
      state.collections.items = {
        content: [
          { _id: "a", label: "a" },
          { _id: "kept", label: "collides" },
          { _id: "b", label: "b" },
        ],
      };
      const error = await assertRejects(
        () => populateDatabase(db, state, { migration: BIRTH }),
        Error,
        "duplicate key",
      );
      assert(!error.message.includes("collides"));
      assertEquals(await items.find({}).toArray(), [
        { _id: "kept", label: "kept" },
      ]);
    } finally {
      await client.db(name).dropDatabase();
      await client.close();
    }
  },
});

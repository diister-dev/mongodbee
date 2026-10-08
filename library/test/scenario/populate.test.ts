import process from "node:process";
import * as v from "../../src/schema.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { MongoClient } from "../../src/mongodb.ts";
import { buildPrivacyPlan } from "../../src/privacy/mod.ts";
import {
  checkScenarioState,
  populateDatabase,
} from "../../src/scenario/mod.ts";
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
  name: "populate: a collection of the schemas that already holds documents is refused and left untouched",
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
      state.collections.items = { content: [{ _id: "a", label: "a" }] };
      await assertRejects(
        () => populateDatabase(db, state, { migration: BIRTH }),
        Error,
        "items",
      );
      assertEquals(await items.find({}).toArray(), [
        { _id: "kept", label: "kept" },
      ]);
      assertEquals((await items.indexes()).length, 1);
    } finally {
      await client.db(name).dropDatabase();
      await client.close();
    }
  },
});

test({
  name: "populate: a failed write leaves no collection behind and never echoes a value",
  timeout: 60_000,
  fn: async () => {
    const client = new MongoClient(TEST_URI);
    await client.connect();
    const name = `mongodbee_test_populate_${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`;
    try {
      const db = client.db(name);
      await db.createCollection("items");
      const state = createEmptyDatabaseState();
      state.collections.items = {
        content: [
          { _id: "a", label: "secret-label" },
          { _id: "a", label: "secret-label" },
        ],
      };
      const error = await assertRejects(
        () => populateDatabase(db, state, { migration: BIRTH }),
        Error,
        "duplicate key",
      );
      assert(!error.message.includes("secret-label"));
      assertEquals(await db.listCollections().toArray(), []);
    } finally {
      await client.db(name).dropDatabase();
      await client.close();
    }
  },
});

test("oracle: two types sharing one physical collection must not repeat an _id", () => {
  const schemas = {
    multiCollections: {
      events: {
        a: { _id: v.number(), label: v.string() },
        b: { _id: v.number(), label: v.string() },
      },
    },
  };
  const state = createEmptyDatabaseState();
  state.multiCollections.events = {
    content: [
      { _id: 1, _type: "a", label: "x" },
      { _id: 1, _type: "b", label: "y" },
      { _id: 2, _type: "b", label: "z" },
    ],
  };
  const violations = checkScenarioState({
    state,
    schemas: schemas as never,
    plan: buildPrivacyPlan({ schemas: schemas as never }),
  });
  assertEquals(
    violations
      .filter((violation) => violation.kind === "duplicate_id")
      .map((violation) => violation.count),
    [1],
  );
});

import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { DocumentValidationError } from "../src/validation-error.ts";
import {
  requestReadStats,
  withRequestContext,
} from "../src/request-context.ts";
import { MongoClient } from "../src/mongodb.ts";
import { TEST_URI, withDatabase } from "./+shared.ts";

const personSchema = {
  name: v.pipe(
    v.string(),
    v.check((name) => name !== "forbidden", "name is forbidden"),
  ),
  group: v.string(),
  rank: v.number(),
};

test("Collection findOne: a valid document comes back parsed", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", {
      name: v.pipe(
        v.string(),
        v.transform((name) => name.toUpperCase()),
      ),
    });
    const id = await people.insertOne({ name: "ada" });
    const found = await people.findOne({ _id: id });
    assertEquals(found?.name, "ADA");
    assertEquals(await people.findOne({ name: "nobody" }), null);
  });
});

test("Collection findOne: a document MongoDB's schema rejects is skipped, not returned", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", personSchema);
    await people.collection.insertOne(
      { _id: "broken", name: 42, group: "g", rank: 0 } as never,
      { bypassDocumentValidation: true },
    );
    assertEquals(await people.findOne({ _id: "broken" }), null);

    const validId = await people.insertOne({
      name: "Ada",
      group: "g",
      rank: 1,
    });
    const found = await people.findOne({ group: "g" });
    assertEquals(found?._id, validId);
    assertEquals(found?.name, "Ada");
  });
});

test("Collection findOne: with a sort, the first valid document in that order wins", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", personSchema);
    await people.collection.insertOne(
      { _id: "top", name: 7, group: "g", rank: 100 } as never,
      { bypassDocumentValidation: true },
    );
    await people.insertOne({ name: "Low", group: "g", rank: 1 });
    await people.insertOne({ name: "High", group: "g", rank: 50 });
    const found = await people.findOne({ group: "g" }, { sort: { rank: -1 } });
    assertEquals(found?.name, "High");
  });
});

test("Collection findOne: a rule only Valibot knows still fails the read", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", personSchema);
    await people.collection.insertOne({
      _id: "checked",
      name: "forbidden",
      group: "g",
      rank: 0,
    } as never);
    const failure = await assertRejects(() =>
      people.findOne({ _id: "checked" }),
    );
    assert(failure instanceof DocumentValidationError);
  });
});

test("Collection findOne: reads inside a session see the session's writes", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", personSchema);
    await people.withSession(async () => {
      const id = await people.insertOne({ name: "Ada", group: "s", rank: 1 });
      const found = await people.findOne({ _id: id });
      assertEquals(found?.name, "Ada");
    });
  });
});

test("Collection findOne: the request memo serves one copy per caller", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", personSchema);
    const id = await people.insertOne({ name: "Ada", group: "g", rank: 1 });
    await withRequestContext(
      async () => {
        const first = await people.findOne({ _id: id });
        assert(first);
        first.name = "Mutated";
        const second = await people.findOne({ _id: id });
        assertEquals(second?.name, "Ada");
        assertEquals(requestReadStats(), {
          loaded: 1,
          reused: 1,
          invalidations: 0,
        });
      },
      { memoizeReads: true },
    );
  });
});

test("Collection findOne: a valid read sends the caller's filter alone", async (t) => {
  await withDatabase(t.name, async (db) => {
    const client = new MongoClient(TEST_URI, { monitorCommands: true });
    const filters: Record<string, unknown>[] = [];
    client.on("commandStarted", (event) => {
      if (event.commandName === "find")
        filters.push(event.command.filter as Record<string, unknown>);
    });
    try {
      const people = await collection(
        client.db(db.databaseName),
        "people",
        personSchema,
      );
      const id = await people.insertOne({ name: "Ada", group: "g", rank: 1 });
      filters.length = 0;
      assertEquals((await people.findOne({ _id: id }))?.name, "Ada");
      assertEquals(filters, [{ _id: id }]);

      await people.collection.insertOne(
        { _id: "broken", name: 42, group: "g", rank: 0 } as never,
        { bypassDocumentValidation: true },
      );
      filters.length = 0;
      assertEquals(await people.findOne({ _id: "broken" }), null);
      assertEquals(filters.length, 2);
      assert("$jsonSchema" in filters[1]);
    } finally {
      await client.close();
    }
  });
});

test("Collection findOne: a document Valibot accepts is found like getById finds it", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", {
      name: v.string(),
      level: v.optional(v.number(), 1),
    });
    await people.collection.insertOne({ _id: "legacy", name: "Ada" } as never, {
      bypassDocumentValidation: true,
    });
    const byId = await people.getById("legacy");
    const found = await people.findOne({ _id: "legacy" });
    assertEquals(found, byId);
    assertEquals(found?.level, 1);
  });
});

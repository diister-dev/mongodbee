import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { DocumentValidationError } from "../src/validation-error.ts";
import { withDatabase } from "./+shared.ts";

test("Collection find: an invalid stored document fails every read path loudly", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", {
      name: v.string(),
      age: v.number(),
    });
    await people.insertOne({ name: "Ada", age: 36 });
    await people.collection.insertOne({ name: 42, age: "x" } as never, {
      bypassDocumentValidation: true,
    });

    const failure = await assertRejects(() => people.find({}).toArray());
    assert(
      failure instanceof DocumentValidationError,
      "toArray fails with a DocumentValidationError",
    );

    await assertRejects(async () => {
      for await (const _ of people.find({})) {
        continue;
      }
    });

    const cursor = people.find({}, { sort: { _id: 1 } });
    assertEquals((await cursor.next())?.name, "Ada");
    await assertRejects(() => cursor.next());
    await cursor.close();

    assertEquals(
      (await people.find({ name: "Ada" }).toArray()).map((p) => p.name),
      ["Ada"],
    );
    assertEquals((await people.findInvalid({}).toArray()).length, 1);
  });
});

test("Collection find: a rule MongoDB cannot express fails the read too", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", {
      name: v.pipe(
        v.string(),
        v.check((name) => name !== "forbidden", "name is forbidden"),
      ),
    });
    await people.insertOne({ name: "Ada" });
    await people.collection.insertOne({ name: "forbidden" });

    const failure = await assertRejects(() => people.find({}).toArray());
    assert(failure instanceof DocumentValidationError);
  });
});

test("Collection find: transforms of the schema apply to every read path", async (t) => {
  await withDatabase(t.name, async (db) => {
    const people = await collection(db, "people", {
      name: v.pipe(
        v.string(),
        v.transform((name) => name.toUpperCase()),
      ),
    });
    await people.collection.insertOne({ name: "ada" });

    assertEquals((await people.find({}).toArray())[0]?.name, "ADA");
    const cursor = people.find({});
    assertEquals((await cursor.next())?.name, "ADA");
    await cursor.close();
  });
});

import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assert, assertEquals } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";

const userSchema = { name: v.string(), age: v.number() };

test("Collection surface: findOneAnd* return the document by default and a ModifyResult on request", async (t) => {
  await withDatabase(t.name, async (db) => {
    const users = await collection(db, "users", userSchema);
    await users.insertOne({ name: "Ada", age: 36 });
    await users.insertOne({ name: "Bob", age: 40 });
    await users.insertOne({ name: "Cid", age: 50 });

    const updated = await users.findOneAndUpdate(
      { name: "Ada" },
      { $set: { age: 37 } },
      { returnDocument: "after" },
    );
    assertEquals(updated?.age, 37);

    const withMetadata = await users.findOneAndUpdate(
      { name: "Ada" },
      { $set: { age: 38 } },
      { returnDocument: "after", includeResultMetadata: true },
    );
    assertEquals(withMetadata.ok, 1);
    assertEquals(withMetadata.value?.age, 38);

    const replaced = await users.findOneAndReplace(
      { name: "Bob" },
      { name: "Bob", age: 41 },
      { returnDocument: "after" },
    );
    assertEquals(replaced?.age, 41);

    const deleted = await users.findOneAndDelete({ name: "Cid" });
    assertEquals(deleted?.name, "Cid");

    const deletedWithMetadata = await users.findOneAndDelete(
      { name: "Bob" },
      { includeResultMetadata: true },
    );
    assertEquals(deletedWithMetadata.value?.name, "Bob");
  });
});

test("Collection surface: paginate accepts synchronous prepare and format", async (t) => {
  await withDatabase(t.name, async (db) => {
    const users = await collection(db, "users", userSchema);
    await users.insertOne({ name: "Ada", age: 36 });
    await users.insertOne({ name: "Bob", age: 40 });

    const page = await users.paginate(
      {},
      {
        prepare: (doc) => ({ label: doc.name, age: doc.age }),
        format: (entry) => `${entry.label}:${entry.age}`,
      },
    );
    assertEquals(page.data.sort(), ["Ada:36", "Bob:40"]);
  });
});

test("Collection surface: db and indexes behave like the driver's", async (t) => {
  await withDatabase(t.name, async (db) => {
    const users = await collection(db, "users", userSchema);
    assertEquals(users.db.databaseName, db.databaseName);
    await users.insertOne({ name: "Ada", age: 36 });

    const full = await users.indexes();
    assert(Array.isArray(full), "indexes() lists index descriptions");
    assert(full.some((index) => index.name === "_id_"));

    const compact = await users.indexes({ full: false });
    assert(!Array.isArray(compact), "indexes({ full: false }) is compact");
    assert("_id_" in compact);
  });
});

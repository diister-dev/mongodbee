import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assertEquals, assertRejects } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";

async function seedPeople(
  db: Parameters<Parameters<typeof withDatabase>[1]>[0],
) {
  const people = await collection(db, "people", {
    _id: v.string(),
    name: v.string(),
  });
  const ids: string[] = [];
  for (const name of ["Ada", "Grace", "Linus", "Barbara"]) {
    ids.push(await people.insertOne({ _id: `person:${name}`, name }));
  }
  return { people, ids };
}

test("Collection deleteMany: safeDelete refuses an empty or _id-only filter", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { people, ids } = await seedPeople(db);
    await assertRejects(() => people.deleteMany({}), Error, "Filter is empty");
    await assertRejects(
      () => people.deleteMany({ _id: ids[0] }),
      Error,
      "only contains _id",
    );
    await assertRejects(
      () => people.deleteMany({ _id: { $in: ids } }),
      Error,
      "only contains _id",
    );
    await assertRejects(
      () => people.deleteMany({ name: undefined }),
      Error,
      "only contains _id",
    );
    assertEquals(await people.countDocuments({}), 4);
    assertEquals((await people.deleteMany({ name: "Ada" })).deletedCount, 1);
    assertEquals(await people.countDocuments({}), 3);
  });
});

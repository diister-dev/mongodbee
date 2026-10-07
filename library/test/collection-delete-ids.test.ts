import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assertEquals, assertRejects } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { withRequestContext } from "../src/request-context.ts";
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

test("Collection deleteIds: deletes exactly the listed ids", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { people, ids } = await seedPeople(db);
    assertEquals(await people.deleteIds([ids[0], ids[2], "person:nobody"]), 2);
    assertEquals(
      (await people.find({}, { sort: { _id: 1 } }).toArray()).map(
        (p) => p.name,
      ),
      ["Barbara", "Grace"],
    );
    assertEquals(await people.deleteIds([]), 0);
    assertEquals(await people.countDocuments({}), 2);
  });
});

test("Collection deleteIds: anything but an explicit id is refused before MongoDB", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { people, ids } = await seedPeople(db);
    const refused: unknown[] = [
      [{ $ne: null }],
      [null],
      [undefined],
      [/.*/],
      [Number.NaN],
      [ids[0], { $exists: true }],
      { $in: ids },
    ];
    for (const value of refused) {
      await assertRejects(
        () => people.deleteIds(value as string[]),
        TypeError,
        "deleteIds",
      );
    }
    assertEquals(await people.countDocuments({}), 4);
  });
});

test("Collection deleteIds: an id outside the schema is refused", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { people } = await seedPeople(db);
    await assertRejects(() => people.deleteIds([42]));
    assertEquals(await people.countDocuments({}), 4);
  });
});

test("Collection deleteIds: follows the ambient transaction", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { people, ids } = await seedPeople(db);
    await assertRejects(() =>
      people.withSession(async () => {
        assertEquals(await people.deleteIds(ids.slice(0, 2)), 2);
        throw new Error("rollback");
      }),
    );
    assertEquals(await people.countDocuments({}), 4);
    await people.withSession(async () => {
      await people.deleteIds(ids.slice(0, 2));
    });
    assertEquals(await people.countDocuments({}), 2);
  });
});

test("Collection deleteIds: the request memo forgets what it read before", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { people, ids } = await seedPeople(db);
    await withRequestContext(
      async () => {
        assertEquals((await people.findOne({ _id: ids[0] }))?.name, "Ada");
        await people.deleteIds([ids[0]]);
        assertEquals(await people.findOne({ _id: ids[0] }), null);
      },
      { memoizeReads: true },
    );
  });
});

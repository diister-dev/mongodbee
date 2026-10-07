import { test } from "./+harness.ts";
import { assertEquals, assertRejects, assertThrows } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { collection } from "../src/collection.ts";
import { partial, removeField } from "../src/sanitizer.ts";
import {
  addToSet,
  increment,
  max,
  min,
  pull,
  push,
  schemasAtPath,
  splitUpdate,
} from "../src/update-operators.ts";

const shop = (db: Db) =>
  multiCollection(db, "shop", {
    product: {
      name: v.string(),
      stock: v.number(),
      lowestPrice: v.optional(v.number()),
      tags: v.array(v.string()),
      lines: v.optional(
        v.array(v.object({ sku: v.string(), qty: v.number() })),
      ),
      usage: v.object({ count: v.number(), lastAt: v.optional(v.date()) }),
      counts: v.optional(v.record(v.string(), v.number())),
    },
    note: { text: v.string() },
  });

const product = {
  name: "Lamp",
  stock: 5,
  tags: ["home"],
  usage: { count: 0 },
};

test("update operators: splitUpdate sorts sentinels by operator and path", () => {
  const split = splitUpdate({
    name: "x",
    gone: removeField(),
    stock: increment(2),
    tags: push("a", "b"),
    usage: partial({ count: increment(1), label: addToSet("z") }),
  });
  assertEquals(split.set, { name: "x" });
  assertEquals(split.unset, { gone: 1 });
  assertEquals(split.operators, {
    $inc: { stock: 2, "usage.count": 1 },
    $push: { tags: ["a", "b"] },
    $addToSet: { "usage.label": ["z"] },
  });
});

test("update operators: a sentinel inside a replaced object is refused", () => {
  assertThrows(
    () => splitUpdate({ usage: { count: increment(1) } }),
    Error,
    'an update operator must be the value of a field path; write "usage.count"',
  );
});

test("update operators: increment only takes a finite number", () => {
  assertThrows(() => increment(Number.NaN), TypeError, "finite number");
});

test("update operators: schemasAtPath walks objects, arrays, records and unions", () => {
  const schema = v.object({
    a: v.optional(v.object({ b: v.array(v.object({ c: v.number() })) })),
    r: v.record(v.string(), v.string()),
    u: v.union([v.object({ x: v.string() }), v.object({ x: v.number() })]),
  });
  assertEquals(
    schemasAtPath(schema, "a.b.0.c").map((s) => s.type),
    ["number"],
  );
  assertEquals(
    schemasAtPath(schema, "a.b.$[].c").map((s) => s.type),
    ["number"],
  );
  assertEquals(
    schemasAtPath(schema, "r.anything").map((s) => s.type),
    ["string"],
  );
  assertEquals(
    schemasAtPath(schema, "u.x").map((s) => s.type),
    ["string", "number"],
  );
  assertEquals(schemasAtPath(schema, "a.missing"), []);
});

test("multiCollection updateOne: every operator lands, dot paths included", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await shop(db);
    const id = await catalog.insertOne("product", {
      ...product,
      lowestPrice: 20,
      lines: [
        { sku: "A", qty: 1 },
        { sku: "B", qty: 2 },
      ],
    });

    await catalog.updateOne("product", id, {
      stock: increment(-2),
      "usage.count": increment(3),
      tags: addToSet("home", "sale"),
      lines: pull({ sku: "A" }),
      lowestPrice: min(12),
      "counts.views": increment(4),
    });
    await catalog.updateOne("product", id, {
      tags: push("new"),
      lowestPrice: max(15),
    });

    const stored = await catalog.getById("product", id);
    assertEquals(stored.stock, 3);
    assertEquals(stored.usage.count, 3);
    assertEquals(stored.tags, ["home", "sale", "new"]);
    assertEquals(stored.lines, [{ sku: "B", qty: 2 }]);
    assertEquals(stored.lowestPrice, 15);
    assertEquals(stored.counts, { views: 4 });

    await catalog.updateOne("product", id, { tags: pull("home") });
    assertEquals((await catalog.getById("product", id)).tags, ["sale", "new"]);
  });
});

test("multiCollection updateOne: operands are checked against the field schema", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await shop(db);
    const id = await catalog.insertOne("product", product);

    await assertRejects(
      () =>
        catalog.updateOne("product", id, {
          name: increment(1) as never,
        }),
      Error,
      'increment() needs a numeric field, "name" is not one',
    );
    await assertRejects(
      () => catalog.updateOne("product", id, { stock: push(1) as never }),
      Error,
      'push() needs an array field, "stock" is not one',
    );
    await assertRejects(
      () => catalog.updateOne("product", id, { tags: addToSet(42 as never) }),
      v.ValiError,
    );
    await assertRejects(
      () =>
        catalog.updateOne("product", id, {
          lines: push({ sku: "C" } as never),
        }),
      v.ValiError,
    );
    await assertRejects(
      () => catalog.updateOne("product", id, { tags: pull(7 as never) }),
      v.ValiError,
    );
    await assertRejects(
      () => catalog.updateOne("product", id, { stock: min("low" as never) }),
      v.ValiError,
    );

    assertEquals(await catalog.getById("product", id), {
      _id: id,
      _type: "product",
      ...product,
    });
  });
});

test("multiCollection updateOne: a path written twice, or with its parent, is refused", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await shop(db);
    const id = await catalog.insertOne("product", product);

    await assertRejects(
      () =>
        catalog.updateOne("product", id, {
          usage: { count: 1 },
          "usage.count": increment(1),
        }),
      Error,
      "one update cannot write a field and its parent",
    );
    await assertRejects(
      () =>
        catalog.updateWhere(
          "product",
          { _id: id },
          { stock: increment(1) },
          { max: { stock: 9 } },
        ),
      Error,
      "cannot be both set and bounded by max",
    );
  });
});

test("multiCollection: reserved fields stay guarded, dot paths included", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await shop(db);
    const id = await catalog.insertOne("product", product);
    await assertRejects(
      () =>
        catalog.updateOne("product", id, {
          "_type.x": increment(1),
        } as never),
      Error,
      '"_type" cannot be written',
    );
  });
});

test("multiCollection updateMany, updateWhere and findOneAndUpdate take operators", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await shop(db);
    const a = await catalog.insertOne("product", product);
    const b = await catalog.insertOne("product", { ...product, name: "Desk" });

    await catalog.updateMany({
      product: {
        [a]: { stock: increment(1) },
        [b]: { tags: push("office") },
      },
    });
    assertEquals((await catalog.getById("product", a)).stock, 6);
    assertEquals((await catalog.getById("product", b)).tags, [
      "home",
      "office",
    ]);

    const written = await catalog.updateWhere(
      "product",
      { _id: a, stock: { $gte: 6 } },
      { stock: increment(-6), "usage.count": increment(1) },
    );
    assertEquals(written, { matched: 1, modified: 1, upsertedId: null });

    const after = await catalog.findOneAndUpdate(
      "product",
      { _id: a },
      { "usage.count": increment(1), tags: addToSet("sold-out") },
    );
    assertEquals(after?.stock, 0);
    assertEquals(after?.usage.count, 2);
    assertEquals(after?.tags, ["home", "sold-out"]);
  });
});

test("multiCollection updateWhere: an upsert stores each operator's insert value", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await shop(db);
    const result = await catalog.updateWhere(
      "product",
      { name: "Chair" },
      { stock: increment(3), tags: push("new") },
      { upsert: true, setOnInsert: { usage: { count: 0 } } },
    );
    const created = await catalog.getById("product", result.upsertedId!);
    assertEquals(created.stock, 3);
    assertEquals(created.tags, ["new"]);
    assertEquals(created.usage, { count: 0 });

    await catalog.updateWhere(
      "product",
      { name: "Chair" },
      { stock: increment(3), tags: push("again") },
      { upsert: true, setOnInsert: { usage: { count: 0 } } },
    );
    const again = await catalog.getById("product", result.upsertedId!);
    assertEquals(again.stock, 6);
    assertEquals(again.tags, ["new", "again"]);
  });
});

test("multiCollection updateWhere: a sentinel in setOnInsert or max is refused", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await shop(db);
    await assertRejects(
      () =>
        catalog.updateWhere(
          "product",
          { name: "x" },
          {},
          { max: { stock: increment(1) as never } },
        ),
      Error,
      '"stock" takes a plain value, not increment()',
    );
  });
});

test("multiCollection: an operator write inside withSession rolls back with it", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await shop(db);
    const id = await catalog.insertOne("product", product);
    await assertRejects(
      () =>
        catalog.withSession(async () => {
          await catalog.updateOne("product", id, {
            stock: increment(10),
            tags: push("tx"),
          });
          throw new Error("abort");
        }),
      Error,
      "abort",
    );
    const stored = await catalog.getById("product", id);
    assertEquals(stored.stock, 5);
    assertEquals(stored.tags, ["home"]);
  });
});

test("collection: sentinels in $set become checked operators, next to raw ones", async (t) => {
  await withDatabase(t.name, async (db) => {
    const items = await collection(db, "items", {
      name: v.string(),
      stock: v.number(),
      tags: v.array(v.string()),
      usage: v.object({ count: v.number() }),
    });
    const id = await items.insertOne({
      name: "Lamp",
      stock: 1,
      tags: [],
      usage: { count: 0 },
    });

    await items.updateOne(
      { _id: id },
      {
        $set: {
          stock: increment(4),
          "usage.count": increment(1),
          tags: push("a"),
        },
      },
    );
    await items.updateOne({ _id: id }, { $push: { tags: "b" } });
    const stored = await items.getById(id);
    assertEquals(stored.stock, 5);
    assertEquals(stored.usage.count, 1);
    assertEquals(stored.tags, ["a", "b"]);

    await assertRejects(
      () =>
        items.updateOne({ _id: id }, { $set: { name: increment(1) as never } }),
      Error,
      'increment() needs a numeric field, "name" is not one',
    );
    await assertRejects(
      () => items.updateOne({ _id: id }, { $set: { tags: push(3 as never) } }),
      v.ValiError,
    );
    await assertRejects(
      () =>
        items.updateOne(
          { _id: id },
          { $set: { stock: increment(1) }, $inc: { stock: 1 } },
        ),
      Error,
      '"stock" is written twice in one update',
    );
    await assertRejects(
      () =>
        items.updateOne(
          { _id: id },
          { $set: { "usage.count": increment(1) }, $unset: { usage: "" } },
        ),
      Error,
      "one update cannot write a field and its parent",
    );
    await assertRejects(
      () =>
        items.updateOne(
          { _id: id },
          { $inc: { stock: increment(1) } as never },
        ),
      Error,
      '"stock" takes a plain value, not increment()',
    );

    const upserted = await items.updateOne(
      { name: "Desk" },
      {
        $set: { stock: increment(2), tags: addToSet("x") },
        $setOnInsert: { usage: { count: 0 } },
      },
      { upsert: true },
    );
    const desk = await items.getById(upserted.upsertedId!);
    assertEquals(desk.stock, 2);
    assertEquals(desk.tags, ["x"]);
  });
});

test("update operators: the update document types only accept sentinels where they fit", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await shop(db);
    const id = await catalog.insertOne("product", product);
    const typed = async () => {
      await catalog.updateOne("product", id, {
        stock: increment(1),
        tags: push("a"),
        "usage.count": increment(1),
        lowestPrice: min(1),
      });
      // @ts-expect-error increment() on a string field
      await catalog.updateOne("product", id, { name: increment(1) });
      // @ts-expect-error push() on a number field
      await catalog.updateOne("product", id, { stock: push(1) });
      // @ts-expect-error a number pushed on a string array
      await catalog.updateOne("product", id, { tags: push(1) });
      // @ts-expect-error min() with a string on a number field
      await catalog.updateOne("product", id, { stock: min("a") });
    };
    await typed().catch(() => undefined);
    assertEquals((await catalog.getById("product", id)).stock, 6);
  });
});

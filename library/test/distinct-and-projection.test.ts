import { test } from "./+harness.ts";
import { assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";

const EXPO = "exposition:expoaaaaa01";
const OTHER = "exposition:expobbbbb01";

const sorted = <T>(values: T[]) => [...values].sort();

test("multiCollection distinct: typed values of one type, arrays unwound", async (t) => {
  await withDatabase(t.name, async (db) => {
    const shop = await multiCollection(db, "shop", {
      product: {
        name: v.string(),
        tags: v.array(v.string()),
        meta: v.object({ color: v.string() }),
      },
      label: { tags: v.array(v.string()) },
    });
    await shop.insertOne("product", {
      name: "a",
      tags: ["x", "y"],
      meta: { color: "red" },
    });
    await shop.insertOne("product", {
      name: "b",
      tags: ["y", "z"],
      meta: { color: "blue" },
    });
    await shop.insertOne("label", { tags: ["other"] });

    const tags: string[] = await shop.distinct("product", "tags");
    assertEquals(sorted(tags), ["x", "y", "z"]);
    const colors: string[] = await shop.distinct("product", "meta.color", {
      name: "a",
    });
    assertEquals(colors, ["red"]);

    // @ts-expect-error not a path of the type
    await shop.distinct("product", "missing").catch(() => undefined);
  });
});

test("multiCollection find: a projection is refused, findProject returns partial documents", async (t) => {
  await withDatabase(t.name, async (db) => {
    const shop = await multiCollection(db, "shop", {
      product: { name: v.string(), price: v.number() },
    });
    const id = await shop.insertOne("product", { name: "a", price: 3 });

    await assertRejects(
      () =>
        shop.find("product", {}, {
          projection: { name: 1 },
        } as never),
      Error,
      "use findProject(type, fields)",
    );
    await assertRejects(
      () => shop.findAny({}, { projection: { name: 1 } } as never),
      Error,
      "`projection` is not supported",
    );

    const rows = await shop.findProject("product", ["name"], { price: 3 });
    assertEquals(rows, [{ _id: id, _type: "product", name: "a" }]);
  });
});

test("scoped distinct: bound to the scope and the type", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      allowUnscoped: true,
      types: {
        artwork: { title: v.string(), tags: v.array(v.string()) },
        artist: { tags: v.array(v.string()) },
      },
    });
    await catalog.scope(EXPO).insertOne("artwork", {
      title: "a",
      tags: ["oil", "ink"],
    });
    await catalog.scope(OTHER).insertOne("artwork", {
      title: "b",
      tags: ["pastel"],
    });
    await catalog.scope(EXPO).insertOne("artist", { tags: ["artist-tag"] });

    const tags: string[] = await catalog
      .scope(EXPO)
      .distinct("artwork", "tags");
    assertEquals(sorted(tags), ["ink", "oil"]);
    assertEquals(
      sorted(await catalog.scopes([EXPO, OTHER]).distinct("artwork", "tags")),
      ["ink", "oil", "pastel"],
    );
    assertEquals(sorted(await catalog.unscoped.distinct("artwork", "_scope")), [
      EXPO,
      OTHER,
    ]);
    assertEquals(
      await catalog.scope(OTHER).distinct("artwork", "title", { title: "a" }),
      [],
    );
  });
});

test("scoped find: a projection needs validate: false or findProject", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { title: v.string(), year: v.number() } },
    });
    const expo = catalog.scope(EXPO);
    await expo.insertOne("artwork", { title: "a", year: 1 });

    await assertRejects(
      () => expo.find("artwork", {}, { projection: { title: 1 } }),
      Error,
      "findProject(type, fields), or validate: false",
    );
    await assertRejects(
      () => expo.findAny({}, { projection: { title: 1 } }),
      Error,
      "`projection` is not supported",
    );
    await assertRejects(
      () =>
        catalog
          .scopes([EXPO])
          .find("artwork", {}, { projection: { title: 1 } }),
      Error,
      "`projection` is not supported",
    );
    const raw = await expo.find(
      "artwork",
      {},
      { projection: { title: 1, _id: 0 }, validate: false },
    );
    assertEquals(raw, [{ title: "a" }] as never);
  });
});

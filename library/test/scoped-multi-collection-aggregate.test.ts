import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

async function makeCatalog(db: Parameters<Parameters<typeof withDatabase>[1]>[0]) {
  return await scopedMultiCollection(db, "catalog", {
    scope: refId("exposition"),
    types: {
      artwork: {
        title: v.string(),
        year: v.number(),
        artistId: v.string(),
      },
      artist: {
        name: v.string(),
      },
    },
  });
}

Deno.test("aggregate: scope is injected — never leaks docs from other scopes", async () => {
  await withDatabase("smc-agg-isolation", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    await expoA.insertMany("artwork", [
      { title: "a1", year: 1, artistId: "x" },
      { title: "a2", year: 2, artistId: "x" },
    ]);
    await expoB.insertOne("artwork", { title: "b1", year: 3, artistId: "x" });

    const fromA = await expoA.aggregate((stage) => [
      stage.match("artwork", {}),
    ]);
    assertEquals(fromA.length, 2);
    assert(fromA.every((d) => d._scope === EXPO_A));

    const fromB = await expoB.aggregate((stage) => [
      stage.match("artwork", {}),
    ]);
    assertEquals(fromB.length, 1);
    assertEquals(fromB[0]._scope, EXPO_B);
  });
});

Deno.test("aggregate: group by year in scope A only", async () => {
  await withDatabase("smc-agg-group", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    await expoA.insertMany("artwork", [
      { title: "a", year: 2000, artistId: "x" },
      { title: "b", year: 2000, artistId: "x" },
      { title: "c", year: 2001, artistId: "x" },
    ]);
    await expoB.insertOne("artwork", { title: "z", year: 2000, artistId: "x" });

    const result = await expoA.aggregate((stage) => [
      stage.match("artwork", {}),
      stage.group({ _id: "$year", count: { $sum: 1 } }),
      stage.sort({ _id: 1 }),
    ]);

    assertEquals(result.length, 2);
    assertEquals(result[0]._id, 2000);
    assertEquals(result[0].count, 2);
    assertEquals(result[1]._id, 2001);
    assertEquals(result[1].count, 1);
  });
});

Deno.test("aggregate: lookup is scope-isolated — does not match docs from other scopes", async () => {
  await withDatabase("smc-agg-lookup", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    // Two artists with the SAME id-suffix logic — but in different scopes
    const artistAId = await expoA.insertOne("artist", { name: "Picasso (A)" });
    const artistBId = await expoB.insertOne("artist", { name: "Picasso (B)" });

    await expoA.insertOne("artwork", {
      title: "in-A",
      year: 1900,
      artistId: artistAId,
    });
    await expoB.insertOne("artwork", {
      title: "in-B",
      year: 1900,
      artistId: artistBId,
    });

    // From scope A: lookup artist by artistId — must only see artist in A
    const fromA = await expoA.aggregate((stage) => [
      stage.match("artwork", {}),
      stage.lookup("artist", "artistId", "_id", "artistInfo"),
    ]);
    assertEquals(fromA.length, 1);
    assertEquals(fromA[0].artistInfo.length, 1);
    assertEquals(fromA[0].artistInfo[0].name, "Picasso (A)");
    assertEquals(fromA[0].artistInfo[0]._scope, EXPO_A);
  });
});

Deno.test("paginate: limit + sort", async () => {
  await withDatabase("smc-paginate-basic", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    for (let i = 0; i < 7; i++) {
      await expo.insertOne("artwork", {
        title: `item-${i}`,
        year: i,
        artistId: "x",
      });
    }

    const page = await expo.paginate("artwork", {}, { limit: 3 });
    assertEquals(page.data.length, 3);
    assertEquals(page.total, 7);
    // Default sort by _id ascending
    assert(page.data[0]._id < page.data[1]._id);
  });
});

Deno.test("paginate: afterId pagination is scope-bounded", async () => {
  await withDatabase("smc-paginate-after", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    const ids: string[] = [];
    for (let i = 0; i < 5; i++) {
      ids.push(
        await expoA.insertOne("artwork", {
          title: `a-${i}`,
          year: i,
          artistId: "x",
        }),
      );
    }
    await expoB.insertOne("artwork", { title: "b", year: 99, artistId: "x" });

    ids.sort();

    // After first 2 docs of A: page 3+
    const page = await expoA.paginate("artwork", {}, {
      limit: 10,
      afterId: ids[1],
    });
    assertEquals(page.data.length, 3);
    assert(page.data.every((d) => d._scope === EXPO_A));
  });
});

Deno.test("paginate: cross-scope isolation — totals reflect scope", async () => {
  await withDatabase("smc-paginate-isolation", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    for (let i = 0; i < 3; i++) {
      await expoA.insertOne("artwork", { title: `a${i}`, year: i, artistId: "x" });
    }
    for (let i = 0; i < 5; i++) {
      await expoB.insertOne("artwork", { title: `b${i}`, year: i, artistId: "x" });
    }

    const pageA = await expoA.paginate("artwork", {}, { limit: 100 });
    assertEquals(pageA.total, 3);
    assertEquals(pageA.data.length, 3);

    const pageB = await expoB.paginate("artwork", {}, { limit: 100 });
    assertEquals(pageB.total, 5);
    assertEquals(pageB.data.length, 5);
  });
});

import { assert, assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";
const EXPO_C = "exposition:expoccccc03";

async function seed(db: Parameters<Parameters<typeof withDatabase>[1]>[0]) {
  const catalog = await scopedMultiCollection(db, "catalog", {
    scope: refId("exposition"),
    types: {
      artwork: { title: v.string(), year: v.number() },
      artist: { name: v.string() },
    },
  });
  const a = catalog.scope(EXPO_A);
  await a.insertMany("artwork", [
    { title: "a1", year: 1 },
    { title: "a2", year: 2 },
  ]);
  await a.insertOne("artist", { name: "Picasso" });

  const b = catalog.scope(EXPO_B);
  await b.insertOne("artwork", { title: "b1", year: 3 });

  return { catalog };
}

Deno.test("listScopes returns distinct scope values", async () => {
  await withDatabase("smc-life-list", async (db) => {
    const { catalog } = await seed(db);
    const scopes = await catalog.listScopes();
    assertEquals(scopes.sort(), [EXPO_A, EXPO_B]);
  });
});

Deno.test("listScopes returns empty array when no docs exist", async () => {
  await withDatabase("smc-life-list-empty", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: { artwork: { title: v.string() } },
    });
    assertEquals(await catalog.listScopes(), []);
  });
});

Deno.test("scopeExists returns true for active scope and false otherwise", async () => {
  await withDatabase("smc-life-exists", async (db) => {
    const { catalog } = await seed(db);
    assertEquals(await catalog.scopeExists(EXPO_A), true);
    assertEquals(await catalog.scopeExists(EXPO_B), true);
    assertEquals(await catalog.scopeExists(EXPO_C), false);
  });
});

Deno.test("dropScope refuses without { confirm: true }", async () => {
  await withDatabase("smc-life-drop-confirm", async (db) => {
    const { catalog } = await seed(db);
    await assertRejects(
      // deno-lint-ignore no-explicit-any
      () => catalog.dropScope(EXPO_A, {} as any),
      Error,
      "confirm",
    );
  });
});

Deno.test("dropScope removes only docs of that scope", async () => {
  await withDatabase("smc-life-drop", async (db) => {
    const { catalog } = await seed(db);
    const removed = await catalog.dropScope(EXPO_A, { confirm: true });
    assertEquals(removed, 3); // 2 artwork + 1 artist

    assertEquals(await catalog.scopeExists(EXPO_A), false);
    assertEquals(await catalog.scopeExists(EXPO_B), true);
  });
});

Deno.test("scopeStats returns per-type counts within a scope", async () => {
  await withDatabase("smc-life-stats", async (db) => {
    const { catalog } = await seed(db);
    const stats = await catalog.scopeStats(EXPO_A);
    assertEquals(stats.total, 3);
    assertEquals(stats.byType.artwork, 2);
    assertEquals(stats.byType.artist, 1);
  });
});

Deno.test("scopeStats for missing scope returns zero counts", async () => {
  await withDatabase("smc-life-stats-missing", async (db) => {
    const { catalog } = await seed(db);
    const stats = await catalog.scopeStats(EXPO_C);
    assertEquals(stats.total, 0);
    assertEquals(Object.keys(stats.byType).length, 0);
  });
});

import { assert, assertEquals, assertExists } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";
const EXPO_C = "exposition:expoccccc03";

async function seed(db: Parameters<Parameters<typeof withDatabase>[1]>[0], opts?: { allowUnscoped?: boolean }) {
  const catalog = await scopedMultiCollection(db, "catalog", {
    scope: refId("exposition"),
    types: {
      artwork: { title: v.string(), year: v.number() },
      artist: { name: v.string() },
    },
    allowUnscoped: opts?.allowUnscoped,
  });

  const a = catalog.scope(EXPO_A);
  const b = catalog.scope(EXPO_B);
  const c = catalog.scope(EXPO_C);

  await a.insertOne("artwork", { title: "a1", year: 1 });
  await a.insertOne("artwork", { title: "a2", year: 2 });
  await b.insertOne("artwork", { title: "b1", year: 3 });
  await b.insertOne("artwork", { title: "b2", year: 4 });
  await c.insertOne("artwork", { title: "c1", year: 5 });

  return { catalog };
}

// -------- .scopes([ids]) (read-only) -----------------------------------

Deno.test(".scopes([ids]).find returns docs from those scopes only", async () => {
  await withDatabase("smc-scopes-find", async (db) => {
    const { catalog } = await seed(db);
    const view = catalog.scopes([EXPO_A, EXPO_B]);

    const docs = await view.find("artwork");
    assertEquals(docs.length, 4);
    const seen = new Set(docs.map((d) => d._scope));
    assertEquals(seen.has(EXPO_A), true);
    assertEquals(seen.has(EXPO_B), true);
    assertEquals(seen.has(EXPO_C), false);
  });
});

Deno.test(".scopes([id]) single-scope read view works like .scope but read-only", async () => {
  await withDatabase("smc-scopes-single", async (db) => {
    const { catalog } = await seed(db);
    const view = catalog.scopes([EXPO_A]);
    const docs = await view.find("artwork");
    assertEquals(docs.length, 2);
    assert(docs.every((d) => d._scope === EXPO_A));
  });
});

Deno.test(".scopes([]) returns empty results", async () => {
  await withDatabase("smc-scopes-empty", async (db) => {
    const { catalog } = await seed(db);
    const view = catalog.scopes([]);
    assertEquals(await view.find("artwork"), []);
    assertEquals(await view.countDocuments("artwork"), 0);
  });
});

Deno.test(".scopes(ids).findOne picks first match across scopes", async () => {
  await withDatabase("smc-scopes-findone", async (db) => {
    const { catalog } = await seed(db);
    const view = catalog.scopes([EXPO_A, EXPO_B]);
    const doc = await view.findOne("artwork", { title: "b1" });
    assertExists(doc);
    assertEquals(doc.title, "b1");
    assertEquals(doc._scope, EXPO_B);
  });
});

Deno.test(".scopes(ids).countDocuments counts only inside the given scopes", async () => {
  await withDatabase("smc-scopes-count", async (db) => {
    const { catalog } = await seed(db);
    assertEquals(
      await catalog.scopes([EXPO_A, EXPO_B]).countDocuments("artwork"),
      4,
    );
    assertEquals(
      await catalog.scopes([EXPO_A]).countDocuments("artwork"),
      2,
    );
    assertEquals(
      await catalog.scopes([EXPO_A, EXPO_C]).countDocuments("artwork"),
      3,
    );
  });
});

Deno.test(".scopes(ids).aggregate is scope-bounded", async () => {
  await withDatabase("smc-scopes-agg", async (db) => {
    const { catalog } = await seed(db);
    const view = catalog.scopes([EXPO_A, EXPO_B]);
    const result = await view.aggregate((stage) => [
      stage.match("artwork", {}),
      stage.group({ _id: "$_scope", count: { $sum: 1 } }),
      stage.sort({ _id: 1 }),
    ]);
    assertEquals(result.length, 2);
    assertEquals(result[0]._id, EXPO_A);
    assertEquals(result[0].count, 2);
    assertEquals(result[1]._id, EXPO_B);
    assertEquals(result[1].count, 2);
  });
});

Deno.test(".scopes(ids) rejects invalid scope ids (one bad apple)", async () => {
  await withDatabase("smc-scopes-invalid-id", async (db) => {
    const { catalog } = await seed(db);
    assertRejectsLike(
      // deno-lint-ignore no-explicit-any
      () => catalog.scopes([EXPO_A, "not-a-valid-scope" as any]),
      "exposition",
    );
  });
});

// -------- .unscoped (admin) --------------------------------------------

Deno.test(".unscoped throws when allowUnscoped is not enabled (default)", async () => {
  await withDatabase("smc-unscoped-disabled", async (db) => {
    const { catalog } = await seed(db);
    assertRejectsLike(
      () => catalog.unscoped.find("artwork"),
      "allowUnscoped",
    );
  });
});

Deno.test(".unscoped returns ALL docs across scopes when enabled", async () => {
  await withDatabase("smc-unscoped-enabled", async (db) => {
    const { catalog } = await seed(db, { allowUnscoped: true });
    const all = await catalog.unscoped.find("artwork");
    assertEquals(all.length, 5);
    const seen = new Set(all.map((d) => d._scope));
    assertEquals(seen.size, 3);
  });
});

Deno.test(".unscoped countDocuments returns total across scopes", async () => {
  await withDatabase("smc-unscoped-count", async (db) => {
    const { catalog } = await seed(db, { allowUnscoped: true });
    assertEquals(await catalog.unscoped.countDocuments("artwork"), 5);
  });
});

Deno.test(".unscoped aggregate sees everything", async () => {
  await withDatabase("smc-unscoped-agg", async (db) => {
    const { catalog } = await seed(db, { allowUnscoped: true });
    const counts = await catalog.unscoped.aggregate((stage) => [
      stage.match("artwork", {}),
      stage.group({ _id: "$_scope", count: { $sum: 1 } }),
      stage.sort({ _id: 1 }),
    ]);
    assertEquals(counts.length, 3);
    assertEquals(counts.map((c: { count: number }) => c.count), [2, 2, 1]);
  });
});

function assertRejectsLike(
  fn: () => unknown,
  msgIncludes: string,
) {
  try {
    fn();
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    assert(
      message.toLowerCase().includes(msgIncludes.toLowerCase()),
      `expected error message to include "${msgIncludes}", got: ${message}`,
    );
    return;
  }
  throw new Error(`expected ${fn} to throw, but it did not`);
}

import { assert, assertEquals, assertExists } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { dbId, refId } from "../src/ids.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

async function makeCatalog(db: Parameters<Parameters<typeof withDatabase>[1]>[0]) {
  return await scopedMultiCollection(db, "catalog", {
    scope: refId("exposition"),
    types: {
      org: { name: v.string(), entrepriseId: v.string() },
      member: { name: v.string(), orgId: v.string() },
    },
  });
}

Deno.test("findOneAny: cross-type filter, scoped — does not leak other scopes", async () => {
  await withDatabase("smc-any-findone", async (db) => {
    const catalog = await makeCatalog(db);
    const a = catalog.scope(EXPO_A);
    const b = catalog.scope(EXPO_B);

    await a.insertOne("org", { name: "Acme", entrepriseId: "ent:1" });
    await a.insertOne("member", { name: "Bob", orgId: "org:x" });
    await b.insertOne("org", { name: "Globex", entrepriseId: "ent:1" });

    // Untyped probe: any doc with entrepriseId "ent:1" in scope A → the org.
    const hit = await a.findOneAny({ _type: "org", entrepriseId: "ent:1" });
    assertExists(hit);
    assertEquals((hit as { name: string }).name, "Acme");
    assertEquals(hit._scope, EXPO_A);

    // Same filter from scope B → Globex, never Acme.
    const hitB = await b.findOneAny({ _type: "org", entrepriseId: "ent:1" });
    assertEquals((hitB as { name: string }).name, "Globex");
  });
});

Deno.test("findAny: returns all cross-type matches within the scope only", async () => {
  await withDatabase("smc-any-findall", async (db) => {
    const catalog = await makeCatalog(db);
    const a = catalog.scope(EXPO_A);
    const b = catalog.scope(EXPO_B);

    await a.insertOne("org", { name: "Acme", entrepriseId: "ent:1" });
    await a.insertOne("member", { name: "Bob", orgId: "org:x" });
    await b.insertOne("org", { name: "Globex", entrepriseId: "ent:1" });

    // No _type filter → both docs in scope A (org + member), none from B.
    const all = await a.findAny({});
    assertEquals(all.length, 2);
    assert(all.every((d) => d._scope === EXPO_A));
  });
});

Deno.test("withSession: scoped + plain collection commit together (session propagates)", async () => {
  await withDatabase("smc-any-session", async (db) => {
    // A global plain collection + a scoped multi-collection sharing one client.
    const audit = await collection(db, "audit", {
      _id: dbId("audit"),
      message: v.string(),
    });
    const catalog = await makeCatalog(db);

    // Cross-collection transaction: write to both inside one withSession.
    await catalog.withSession(async () => {
      await catalog.scope(EXPO_A).insertOne("org", {
        name: "Tx Co",
        entrepriseId: "ent:9",
      });
      await audit.insertOne({ message: "org created" });
    });

    // Both writes are visible → session propagated to both via AsyncLocalStorage.
    assertEquals(await catalog.scope(EXPO_A).countDocuments("org"), 1);
    assertEquals(await audit.countDocuments({}), 1);
  });
});

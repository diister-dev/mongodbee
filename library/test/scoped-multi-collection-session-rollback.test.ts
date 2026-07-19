// withSession ABORT path: a throw inside `withSession` must roll back BOTH a
// scoped-view write and a write to a plain sibling collection sharing the same
// MongoClient. The commit path is covered by
// scoped-multi-collection-any.test.ts ("withSession: scoped + plain collection
// commit together") — this is its mirror image on the failure side.

import { assert, assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { dbId, refId } from "../src/ids.ts";

const EXPO_A = "exposition:expoaaaaa01";

async function makeCatalog(
  db: Parameters<Parameters<typeof withDatabase>[1]>[0],
) {
  return await scopedMultiCollection(db, "catalog", {
    scope: refId("exposition"),
    types: {
      org: { name: v.string(), entrepriseId: v.string() },
      member: { name: v.string(), orgId: v.string() },
    },
  });
}

Deno.test("withSession: a throw rolls back the scoped write AND the plain collection write", async () => {
  await withDatabase("smc2-sess-rollback", async (db) => {
    const audit = await collection(db, "audit", {
      _id: dbId("audit"),
      message: v.string(),
    });
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    // Both writes happen inside one transaction; the final throw must abort it.
    const boom = new Error("boom — abort the whole transaction");
    await assertRejects(
      () =>
        catalog.withSession(async () => {
          await expo.insertOne("org", { name: "Tx Co", entrepriseId: "ent:9" });
          await audit.insertOne({ message: "org created" });
          throw boom;
        }),
      Error,
      "abort the whole transaction",
    );

    // Neither write survived — the transaction rolled everything back.
    assertEquals(
      await expo.countDocuments("org"),
      0,
      "scoped write rolled back",
    );
    assertEquals(await audit.countDocuments({}), 0, "plain write rolled back");
  });
});

Deno.test("withSession: a write that succeeds BEFORE the transaction still stands; only the aborted batch rolls back", async () => {
  await withDatabase("smc2-sess-rollback-pre", async (db) => {
    const catalog = await makeCatalog(db);
    const expo = catalog.scope(EXPO_A);

    // A committed write OUTSIDE the failing transaction.
    await expo.insertOne("org", { name: "Committed", entrepriseId: "ent:1" });

    await assertRejects(
      () =>
        catalog.withSession(async () => {
          await expo.insertOne("org", {
            name: "Doomed",
            entrepriseId: "ent:2",
          });
          await expo.insertOne("member", {
            name: "AlsoDoomed",
            orgId: "org:x",
          });
          throw new Error("rollback please");
        }),
      Error,
      "rollback please",
    );

    // Only the pre-existing doc remains; both doomed writes were rolled back.
    const orgs = await expo.find("org");
    assertEquals(orgs.length, 1);
    assertEquals(orgs[0].name, "Committed");
    assertEquals(await expo.countDocuments("member"), 0);
    assert(
      orgs.every((o) => o.entrepriseId === "ent:1"),
      "the doomed org (ent:2) must not have persisted",
    );
  });
});

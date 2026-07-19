/**
 * Regression coverage for two multi-model migration hazards:
 *
 * [C7] Prefix-only instance discovery widened the destructive blast radius.
 *   `discoverMultiCollectionInstances` treated ANY collection named
 *   `<model>:*` as an instance WITHOUT checking its `_information` marker. Fed
 *   into flow-to-scope `consume` (which DROPS each instance) or validator sync
 *   (`collMod`), an unrelated collection that merely matched the naming
 *   convention got flowed and dropped. The fix keeps the "don't silently skip
 *   real data" motivation but makes discovery fail LOUD: a NON-EMPTY prefix
 *   match without a valid marker throws (default) instead of being adopted.
 *   Empty prefix collections carry no data at risk and are still skipped so
 *   legitimate adoption / validator-sync paths keep working.
 *
 * [C8] Catch-up ignored flow_to_scope. `filterOperationsForModelType` hit
 *   `default: return false` for flow_to_scope, so a lagging multi-model
 *   instance that missed a consolidation received ZERO relevant ops and the
 *   caller recorded the migration as APPLIED while the instance kept its
 *   un-consolidated data. The fix returns the flow_to_scope op whenever its
 *   `from` reads from this model's instances.
 */
import { assert, assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import {
  createMultiCollectionInfo,
  discoverMultiCollectionInstances,
} from "../../src/migration/multicollection-registry.ts";
import { filterOperationsForModelType } from "../../src/migration/catch-up.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";

// ============================================================================
// [C7] discoverMultiCollectionInstances — fail loud, not fail destructive
// ============================================================================

Deno.test("[C7] discovery throws on a NON-EMPTY prefix collection with no marker (default)", async () => {
  await withDatabase("disc_guard_throw", async (db) => {
    // A properly registered instance (prefix name + valid `_information`).
    await createMultiCollectionInfo(db, "gadget:real", "gadget", "000");
    await db.collection("gadget:real").insertOne(
      { _id: "item:1", _type: "item", v: 1 } as never,
    );

    // An UNRELATED collection that merely matches the `gadget:` convention,
    // carrying real data but NO `_information` marker.
    await db.collection("gadget:intruder").insertOne(
      { _id: "keepme", secret: 42 } as never,
    );

    const err = await assertRejects(
      () => discoverMultiCollectionInstances(db, "gadget"),
      Error,
      "gadget:intruder",
    );
    // The message must guide the operator toward remediation.
    assert(
      err.message.includes("_information") &&
        (err.message.includes("register") || err.message.includes("rename")),
      "error should explain how to proceed",
    );
  });
});

Deno.test("[C7] discovery silently skips an EMPTY prefix collection (adoption-safe carve-out)", async () => {
  await withDatabase("disc_guard_empty", async (db) => {
    await createMultiCollectionInfo(db, "gadget:real", "gadget", "000");
    // Freshly-created / about-to-be-adopted instance: no marker yet, no data.
    await db.createCollection("gadget:pending");

    const instances = await discoverMultiCollectionInstances(db, "gadget");
    assertEquals(instances, ["gadget:real"]);
  });
});

Deno.test('[C7] onUnverifiedPrefixMatch "skip" excludes the suspicious collection without throwing', async () => {
  await withDatabase("disc_guard_skip", async (db) => {
    await createMultiCollectionInfo(db, "gadget:real", "gadget", "000");
    await db.collection("gadget:intruder").insertOne(
      { _id: "keepme", secret: 42 } as never,
    );

    const instances = await discoverMultiCollectionInstances(db, "gadget", {
      onUnverifiedPrefixMatch: "skip",
    });
    assertEquals(instances, ["gadget:real"]);
  });
});

Deno.test('[C7] onUnverifiedPrefixMatch "include" preserves legacy name-only discovery', async () => {
  await withDatabase("disc_guard_include", async (db) => {
    await createMultiCollectionInfo(db, "gadget:real", "gadget", "000");
    await db.collection("gadget:intruder").insertOne(
      { _id: "keepme", secret: 42 } as never,
    );

    const instances = await discoverMultiCollectionInstances(db, "gadget", {
      onUnverifiedPrefixMatch: "include",
    });
    assertEquals(instances, ["gadget:intruder", "gadget:real"]);
  });
});

Deno.test("[C7] a prefix collection registered to a DIFFERENT model is excluded, not thrown", async () => {
  await withDatabase("disc_guard_othertype", async (db) => {
    await createMultiCollectionInfo(db, "gadget:real", "gadget", "000");
    // Same `gadget:` prefix but a valid marker for another model — trusted as
    // not-ours, must neither be returned nor trigger the loud guard.
    await createMultiCollectionInfo(db, "gadget:owned_by_widget", "widget");
    await db.collection("gadget:owned_by_widget").insertOne(
      { _id: "w:1", _type: "w", v: 1 } as never,
    );

    const instances = await discoverMultiCollectionInstances(db, "gadget");
    assertEquals(instances, ["gadget:real"]);
  });
});

Deno.test("[C7] flow-to-scope consume aborts LOUDLY on an unregistered prefix collection (no data flowed or dropped)", async () => {
  await withDatabase("disc_guard_flow", async (db) => {
    // A legitimately registered instance with data.
    await createMultiCollectionInfo(db, "gadget:real", "gadget", "000");
    await db.collection("gadget:real").insertOne(
      { _id: "item:1", _type: "item", v: 1 } as never,
    );

    // An unrelated collection matching the convention, holding real data.
    await db.collection("gadget:intruder").insertOne(
      { _id: "keepme", secret: 42 } as never,
    );

    const S = { collections: {} };
    const m = migrationDefinition(
      "2026_01_01_0000_AAAAAAAAAAAAAAAAAAAAAAAAAA@consolidate",
      "consolidate",
      {
        parent: null,
        schemas: S,
        migrate: (b) =>
          b.flowToScope({
            from: { kind: "multiModelInstances", model: "gadget" },
            into: { collection: "gadget_scoped" },
            scope: (_d, ctx) => String(ctx.instanceName),
            source: "consume",
          }).compile(),
      },
    );
    const ops = m.migrate(migrationBuilder({ schemas: S })).operations;

    await assertRejects(
      () =>
        createMongodbApplier(db, m, { currentMigrationId: m.id })
          .applyMigration(ops, "up"),
      Error,
      "gadget:intruder",
    );

    // The guard fires DURING discovery, before any flow/drop — so both the
    // intruder and the real instance still hold their data, and nothing was
    // consolidated into the target.
    assertEquals(await db.collection("gadget:intruder").countDocuments(), 1);
    assertEquals(
      await db.collection("gadget:real").countDocuments({ _type: "item" }),
      1,
    );
    const scopedExists = (await db.listCollections().toArray()).some(
      (c) => c.name === "gadget_scoped",
    );
    // The target may be lazily created but must hold no flowed documents.
    if (scopedExists) {
      assertEquals(await db.collection("gadget_scoped").countDocuments(), 0);
    }
  });
});

// ============================================================================
// [C8] filterOperationsForModelType — flow_to_scope is model-relevant
// ============================================================================

function flowOps(
  from:
    | { kind: "multiModelInstances"; model: string }
    | { kind: "collection"; name: string }
    | {
      kind: "multiCollectionType";
      collectionName: string;
      documentType: string;
    },
) {
  return migrationBuilder({ schemas: { collections: {} } })
    .flowToScope({
      from,
      into: { collection: "scoped" },
      scope: (_d, ctx) =>
        String(ctx.instanceName ?? ctx.sourceCollection ?? ""),
      source: "consume",
    })
    .compile().operations;
}

Deno.test("[C8] flow_to_scope from this model's instances is relevant to that model", () => {
  const ops = flowOps({ kind: "multiModelInstances", model: "exposition" });

  const relevant = filterOperationsForModelType(ops, "exposition");
  assertEquals(relevant.length, 1);
  assertEquals(relevant[0].type, "flow_to_scope");

  // Not relevant to an unrelated model — otherwise catch-up would over-apply.
  assertEquals(filterOperationsForModelType(ops, "other").length, 0);
});

Deno.test("[C8] flow_to_scope from a plain collection or multi-collection type is NOT model-relevant", () => {
  const fromCollection = flowOps({ kind: "collection", name: "roots" });
  assertEquals(filterOperationsForModelType(fromCollection, "roots").length, 0);
  assertEquals(
    filterOperationsForModelType(fromCollection, "anything").length,
    0,
  );

  const fromMultiColl = flowOps({
    kind: "multiCollectionType",
    collectionName: "catalog",
    documentType: "book",
  });
  assertEquals(
    filterOperationsForModelType(fromMultiColl, "catalog").length,
    0,
  );
  assertEquals(filterOperationsForModelType(fromMultiColl, "book").length, 0);
});

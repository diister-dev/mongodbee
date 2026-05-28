/**
 * `flowToScope` — the composable primitive for consolidating N collections
 * (a global collection + every instance of a multi-model) into ONE scoped
 * multi-collection, deriving `_scope` per document.
 *
 * Mirrors the diivento consolidation shape: global `+expositions` roots
 * become `information` docs (`_id == scope`), each per-exposition instance's
 * docs move into the scope, the per-instance `information` singleton is
 * re-keyed to the scope so it MERGES with the root-derived one, and `system`
 * is re-keyed to a fresh id.
 */
import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder, getIrreversibleOperations } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import * as v from "../../src/schema.ts";

const SCHEMAS = { collections: { "+expositions": { _id: v.string() } } };

function consolidationMigration() {
  return migrationDefinition("001", "consolidate", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) =>
      b.flowToScope({
        // global roots → `information` docs (scope = their own _id)
        from: { kind: "collection", name: "+expositions" },
        into: { collection: "+expositions_scoped" },
        toType: () => "information",
        scope: (d) => d._id as string,
        source: "consume",
      })
        .flowToScope({
          // each per-expo instance's docs → its scope (= instance name)
          from: { kind: "multiModelInstances", model: "exposition" },
          into: { collection: "+expositions_scoped" },
          scope: (_d, ctx) => ctx.instanceName!,
          map: (d, ctx) =>
            d._type === "information"
              ? { ...d, _id: ctx.instanceName } // align with root → merge
              : d._type === "system"
              ? { ...d, _id: undefined } // re-key
              : d,
          onConflict: "merge",
          merge: (root, sub) => ({ ...sub, ...root }), // root fields win
          source: "consume",
        })
        .compile(),
  });
}

function seed() {
  const state = createEmptyDatabaseState();
  state.collections["+expositions"] = {
    content: [
      { _id: "exposition:A", name: "Expo A", createdBy: "user:1", modules: ["badges"] },
      { _id: "exposition:B", name: "Expo B", createdBy: "user:2" },
    ],
  };
  state.multiModels["exposition:A"] = {
    modelType: "exposition",
    content: [
      { _id: "information:0", _type: "information", description: "desc A", events: [] },
      { _id: "participant:p1", _type: "participant", name: "Alice" },
      { _id: "system:0", _type: "system", version: "1.0" },
    ],
  };
  state.multiModels["exposition:B"] = {
    modelType: "exposition",
    content: [
      { _id: "information:0", _type: "information", description: "desc B", events: [] },
      { _id: "participant:p2", _type: "participant", name: "Bob" },
      { _id: "system:0", _type: "system", version: "1.0" },
    ],
  };
  return state;
}

Deno.test("flowToScope: marked irreversible", () => {
  const m = consolidationMigration();
  const ops = m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;
  assertEquals(getIrreversibleOperations(ops).length, 2);
});

Deno.test("flowToScope: consolidates roots + instances, merges singleton, consumes source", async () => {
  const state = seed();
  const m = consolidationMigration();
  const ops = m.migrate(migrationBuilder({ schemas: SCHEMAS })).operations;
  await createMemoryApplier(m).applyMigration(state, ops, "up");

  const scoped = state.scopedMultiCollections["+expositions_scoped"].content;

  // sources consumed (fully — keys removed)
  assertEquals(state.collections["+expositions"], undefined);
  assertEquals(state.multiModels["exposition:A"], undefined);
  assertEquals(state.multiModels["exposition:B"], undefined);

  // scope A : ONE merged information (root name + sub description), id == scope
  const scopeA = scoped.filter((d) => d._scope === "exposition:A");
  const infosA = scopeA.filter((d) => d._type === "information");
  assertEquals(infosA.length, 1, "root + sub information merged into one");
  assertEquals(infosA[0]._id, "exposition:A");
  assertEquals(infosA[0].name, "Expo A"); // from root
  assertEquals(infosA[0].description, "desc A"); // from sub
  assertEquals(infosA[0].modules, ["badges"]); // from root
  assert(scopeA.some((d) => d._type === "participant" && d.name === "Alice"));
  const sysA = scopeA.filter((d) => d._type === "system");
  assertEquals(sysA.length, 1);
  assert(String(sysA[0]._id).startsWith("system:"), "system re-keyed");
  assert(sysA[0]._id !== "system:0", "system no longer literal");

  // scope B symmetric
  const scopeB = scoped.filter((d) => d._scope === "exposition:B");
  assertEquals(scopeB.filter((d) => d._type === "information").length, 1);
  assertEquals(scopeB.find((d) => d._type === "information")!.description, "desc B");
  assert(scopeB.some((d) => d._type === "participant" && d.name === "Bob"));

  // total: 2 scopes × (1 info + 1 participant + 1 system) = 6
  assertEquals(scoped.length, 6);
});

Deno.test("flowToScope: onConflict 'error' throws on duplicate target id", async () => {
  const state = createEmptyDatabaseState();
  state.collections["src"] = {
    content: [
      { _id: "x:1", v: 1 },
      { _id: "x:1", v: 2 }, // same id → same (scope,type,id)
    ],
  };
  const m = migrationDefinition("001", "dup", {
    parent: null,
    schemas: { collections: { src: { _id: v.string() } } },
    migrate: (b) =>
      b.flowToScope({
        from: { kind: "collection", name: "src" },
        into: { collection: "dst" },
        toType: () => "thing",
        scope: () => "exposition:Z",
        source: "keep",
      }).compile(),
  });
  const ops = m.migrate(migrationBuilder({ schemas: { collections: { src: { _id: v.string() } } } })).operations;

  let threw = false;
  try {
    await createMemoryApplier(m).applyMigration(state, ops, "up");
  } catch (e) {
    threw = e instanceof Error && e.message.includes("conflict");
  }
  assert(threw, "expected onConflict 'error' (default) to throw on duplicate");
});

Deno.test("mongodb flowToScope: collection → scoped, merge + consume on a real DB", async () => {
  await withDatabase("flow-to-scope-mongo", async (db) => {
    await db.collection("roots").insertMany([
      { _id: "exposition:A", name: "Expo A" },
      { _id: "exposition:B", name: "Expo B" },
    ] as never);
    await db.collection("details").insertMany([
      { _id: "exposition:A", note: "note A" },
      { _id: "exposition:B", note: "note B" },
    ] as never);

    const S = { collections: { roots: { _id: v.string() }, details: { _id: v.string() } } };
    const m = migrationDefinition("001", "consolidate", {
      parent: null,
      schemas: S,
      migrate: (b) =>
        b.flowToScope({
          from: { kind: "collection", name: "roots" },
          into: { collection: "scoped" },
          toType: () => "info",
          scope: (d) => d._id as string,
          source: "consume",
        })
          .flowToScope({
            from: { kind: "collection", name: "details" },
            into: { collection: "scoped" },
            toType: () => "info",
            scope: (d) => d._id as string,
            onConflict: "merge",
            merge: (existing, incoming) => ({ ...existing, ...incoming }),
            source: "consume",
          })
          .compile(),
    });
    const ops = m.migrate(migrationBuilder({ schemas: S })).operations;
    await createMongodbApplier(db, m, { currentMigrationId: m.id }).applyMigration(ops, "up");

    assertEquals(await db.collection("roots").countDocuments(), 0);
    assertEquals(await db.collection("details").countDocuments(), 0);

    const docs = await db.collection("scoped").find({} as never).toArray();
    assertEquals(docs.length, 2);
    const a = docs.find((d) => d._scope === "exposition:A")!;
    assertEquals(a._type, "info");
    assertEquals(String(a._id), "exposition:A");
    assertEquals((a as { name?: string }).name, "Expo A"); // from roots
    assertEquals((a as { note?: string }).note, "note A"); // merged from details
  });
});

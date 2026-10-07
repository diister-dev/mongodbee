/**
 * Production creates one multi-model instance per root entity, named after
 * the root `_id`. The simulator generated a single instance whatever the root
 * count, so a consolidation merging root with instance documents only ever
 * merged one scope — every other scope produced a document amputated of the
 * fields only the instance carries.
 *
 * Locks the cardinality AND its volume model: instances follow the entity
 * pool, one per pooled id, none invented; an empty pool keeps the
 * single-instance fallback; and the batch budget is per MODEL — divided
 * across instances — so multi-model volume stays linear in the pool size.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { createSimulationValidator } from "../../src/migration/validators/simulation.ts";
import {
  createEmptyDatabaseState,
  type MockGenerationFailure,
  type SimulationDatabaseState,
} from "../../src/migration/types.ts";
import {
  createCorrelationSession,
  getMockGenerationConfig,
  type MockPopulateContext,
  populateDeclaredBuckets,
} from "../../src/migration/validators/mock/mod.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

/** Root documents own the `exposition` space the model keys its instances on. */
const OWNED = {
  collections: {
    "+expositions": { _id: refId("exposition"), name: v.string() },
  },
  multiModels: {
    exposition: {
      information: { _id: refId("information"), description: v.string() },
    },
  },
};

/** No target mints `widget`, so the model's entity pool stays empty. */
const ORPHANED = {
  multiModels: {
    widget: { part: { _id: refId("part"), label: v.string() } },
  },
};

function contextFor(
  schemas: Parameters<typeof createCorrelationSession>[0]["schemas"],
): MockPopulateContext {
  return {
    config: getMockGenerationConfig("quick"),
    failures: [] as MockGenerationFailure[],
    session: createCorrelationSession({ schemas, seed: 7 }),
  };
}

function instanceNames(state: SimulationDatabaseState, model: string) {
  return Object.keys(state.multiModels).filter(
    (name) => state.multiModels[name].modelType === model,
  );
}

test("cardinality: every minted root id gets its own instance, none invented", () => {
  const state = createEmptyDatabaseState();
  const ctx = contextFor(OWNED);

  populateDeclaredBuckets(state, OWNED, "always", ctx);

  const rootIds = new Set(
    state.collections["+expositions"].content.map((d) => String(d._id)),
  );
  const names = instanceNames(state, "exposition");

  assertEquals(
    names.length,
    rootIds.size,
    `one instance per root entity: ${rootIds.size} roots, ${names.length} instances`,
  );
  assertEquals(
    new Set(names).size,
    names.length,
    "instance names are distinct",
  );
  for (const name of names) {
    assert(rootIds.has(name), `instance "${name}" is named after no root id`);
  }
  for (const name of names) {
    assert(
      state.multiModels[name].content.length > 0,
      `instance "${name}" was created empty`,
    );
  }
  assertEquals(
    ctx.failures.filter((f) => f.kind === "correlation"),
    [],
    "full coverage — nothing to report",
  );
});

test("cardinality: an empty entity pool keeps the single synthetic instance", () => {
  const state = createEmptyDatabaseState();
  const ctx = contextFor(ORPHANED);

  populateDeclaredBuckets(state, ORPHANED, "always", ctx);

  const names = instanceNames(state, "widget");
  assertEquals(names.length, 1, "no pooled entity — the fallback stands");
  assert(
    names[0].startsWith("widget:"),
    `fallback name must follow <model>:<id>, got "${names[0]}"`,
  );
});

test("cardinality: the batch budget is per model — volume stays linear in the pool size", () => {
  const state = createEmptyDatabaseState();
  const ctx = contextFor(OWNED);

  populateDeclaredBuckets(state, OWNED, "always", ctx);

  const names = instanceNames(state, "exposition");
  const config = getMockGenerationConfig("quick");
  const typeCount = Object.keys(OWNED.multiModels.exposition).length;
  const total = names.reduce(
    (sum, name) => sum + state.multiModels[name].content.length,
    0,
  );

  // Each instance carries at least one full batch, and the model total stays
  // within one collection budget (+ the per-instance floor), never N batches
  // per instance — the quadratic shape the budget replaced.
  for (const name of names) {
    assert(
      state.multiModels[name].content.length >= typeCount,
      `instance "${name}" holds less than one full batch`,
    );
  }
  assert(
    total <= config.DOCS_PER_COLLECTION_MAX * typeCount + names.length,
    `model volume must stay linear: ${total} docs across ${names.length} instances`,
  );

  // The single-instance fallback keeps the full budget — one instance, all
  // the batches.
  const orphanState = createEmptyDatabaseState();
  const orphanCtx = contextFor(ORPHANED);
  populateDeclaredBuckets(orphanState, ORPHANED, "always", orphanCtx);
  const [fallback] = instanceNames(orphanState, "widget");
  assertEquals(
    orphanState.multiModels[fallback].content.length,
    config.DOCS_PER_COLLECTION_MIN,
    "a lone instance receives the whole per-collection budget",
  );
});

// ---------------------------------------------------------------------------
// Acceptance: no amputated document survives a consolidation
// ---------------------------------------------------------------------------

const PARENT_SCHEMAS = {
  collections: {
    "+expositions": { _id: refId("exposition"), name: v.string() },
  },
  multiModels: {
    exposition: {
      information: { _id: refId("information"), description: v.string() },
      participant: { _id: refId("participant"), label: v.string() },
    },
  },
};

const CHILD_SCHEMAS = {
  scopedMultiCollections: {
    "+expositions_scoped": {
      scope: refId("exposition"),
      types: {
        information: {
          _id: refId("exposition"),
          // Optional so an amputated document is DATA, not a generation
          // error: the test counts them instead of reading them as failures.
          name: v.optional(v.string()),
          description: v.optional(v.string()),
        },
        participant: { _id: refId("participant"), label: v.string() },
      },
    },
  },
};

function consolidationChild() {
  const parent = migrationDefinition("001", "baseline", {
    parent: null,
    schemas: PARENT_SCHEMAS,
    migrate: (b) => b.compile(),
  });
  return migrationDefinition("002", "consolidate", {
    parent,
    schemas: CHILD_SCHEMAS,
    migrate: (b) =>
      b
        .flowToScope({
          from: { kind: "collection", name: "+expositions" },
          into: { collection: "+expositions_scoped" },
          toType: () => "information",
          scope: (d) => d._id as string,
          source: "consume",
        })
        .flowToScope({
          from: { kind: "multiModelInstances", model: "exposition" },
          into: { collection: "+expositions_scoped" },
          scope: (_d, ctx) => ctx.instanceName!,
          map: (d, ctx) =>
            d._type === "information" ? { ...d, _id: ctx.instanceName } : d,
          onConflict: "merge",
          merge: (root, sub) => ({ ...sub, ...root }),
          source: "consume",
        })
        .compile(),
  });
}

test("consolidation: EVERY scope produces a complete document, not just the first", async () => {
  const result = await createSimulationValidator({
    powerLevel: "quick",
  }).validateMigration(consolidationChild());

  assertEquals(
    result.errors,
    [],
    `consolidation must validate, got: ${result.errors.join(" | ")}`,
  );

  const state = result.data?.stateAfterMigration as SimulationDatabaseState;
  const information = state.scopedMultiCollections[
    "+expositions_scoped"
  ].content.filter((d) => d._type === "information");

  assert(information.length > 1, "the scenario must produce several scopes");

  const amputated = information.filter(
    (d) => typeof d.name !== "string" || typeof d.description !== "string",
  );
  assertEquals(
    amputated.map((d) => d._scope),
    [],
    `${amputated.length}/${information.length} scopes lost the fields only ` +
      `their instance carries — the merge branch skipped them`,
  );
});

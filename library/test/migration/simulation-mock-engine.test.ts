/**
 * Locks the semantics of the shared mock population engine
 * (`validators/mock/`) — the single implementation behind both
 * `buildMockStateFromSchemas` and `prepareStateForNextMigration`. The two
 * paths used to carry ~185 duplicated lines with six silent behavioral
 * divergences; each resolution is locked here:
 *
 *  - D1: the emptiness policy is explicit — `always` supplements non-empty
 *    collections, `ifEmpty` only fills empty ones, `ifSparse` tops up below
 *    the configured minimum.
 *  - D2: populate generates docCount BATCHES × all types; refresh (after
 *    retention) restores EXACTLY the pre-retention size — never beyond, so
 *    propagation cannot compound volume across a migration chain (the old
 *    ceil-per-type refresh could overshoot on multi-type collections).
 *  - D3: a generation failure aborts the target and is RECORDED, never
 *    swallowed — a swallowed failure left the collection empty, and empty
 *    collections assert nothing downstream (the false-green mechanism).
 *  - D4: multi-model population is decided per MODEL, not per whole bucket —
 *    a newly declared model must not ride green on another model's docs.
 *  - D5: existing state entries are preserved, never reassigned.
 *  - D6: buckets are processed in the DatabaseState declaration order.
 *
 * Plus the validator-level severity rule: a generation failure surfaces as
 * a warning, or as a blocking ERROR when the target ended up empty while
 * its schema declares documents.
 */
import { assert, assertEquals, assertThrows } from "@std/assert";
import * as v from "../../src/schema.ts";
import {
  createEmptyDatabaseState,
  type MockGenerationFailure,
} from "../../src/migration/types.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { createSimulationValidator } from "../../src/migration/validators/simulation.ts";
import {
  createCorrelationSession,
  getMockGenerationConfig,
  type MockPopulateContext,
  populateCollections,
  populateDeclaredBuckets,
  populateExistingMultiModelInstances,
  populateMultiCollections,
  populateSyntheticMultiModelInstances,
} from "../../src/migration/validators/mock/mod.ts";

const GOOD = { _id: v.string(), name: v.string() };
// v.never() makes the mock generator throw immediately — the reliable way to
// exercise the failure path without stubbing the generator.
const BROKEN = { _id: v.string(), impossible: v.never() };

// An empty-schema session correlates nothing — these tests lock the engine's
// population semantics, not the identity correlation (locked elsewhere).
function quickCtx(): MockPopulateContext {
  return {
    config: getMockGenerationConfig("quick"),
    failures: [] as MockGenerationFailure[],
    session: createCorrelationSession({ schemas: {}, seed: 42 }),
  };
}

// ============================================================================
// D1 — explicit emptiness policy
// ============================================================================

Deno.test("D1: policy 'always' supplements a non-empty collection (hybrid seeds + mocks)", () => {
  const state = createEmptyDatabaseState();
  state.collections["relics"] = {
    content: [{ _id: "relic:seed", name: "seed" }],
  };
  const ctx = quickCtx();

  populateCollections(state, { relics: GOOD }, "always", ctx);

  // quick preset: exactly 10 generated docs on top of the seed.
  assertEquals(state.collections["relics"].content.length, 11);
  assertEquals(state.collections["relics"].content[0]._id, "relic:seed");
  assertEquals(ctx.failures, []);
});

Deno.test("D1: policy 'ifEmpty' fills empty collections and leaves non-empty ones untouched", () => {
  const state = createEmptyDatabaseState();
  state.collections["seeded"] = {
    content: [{ _id: "relic:seed", name: "seed" }],
  };
  const ctx = quickCtx();

  populateCollections(state, { seeded: GOOD, empty: GOOD }, "ifEmpty", ctx);

  assertEquals(state.collections["seeded"].content.length, 1);
  assertEquals(state.collections["empty"].content.length, 10);
  assertEquals(ctx.failures, []);
});

Deno.test("D1: policy 'ifSparse' tops up instances below the minimum and skips full ones", () => {
  const state = createEmptyDatabaseState();
  const fullDocs = Array.from({ length: 10 }, (_, i) => ({
    _id: `doc:${i}`,
    _type: "t",
    name: `d${i}`,
  }));
  state.multiModels["m:full"] = { modelType: "m", content: [...fullDocs] };
  state.multiModels["m:sparse"] = {
    modelType: "m",
    content: [{ _id: "doc:s", _type: "t", name: "s" }],
  };
  const ctx = quickCtx();

  populateExistingMultiModelInstances(
    state,
    { m: { t: GOOD } },
    "ifSparse",
    ctx,
  );

  // At the quick minimum (10) the instance is full — untouched.
  assertEquals(state.multiModels["m:full"].content.length, 10);
  // Below the minimum: 10 batches × 1 type appended on top of the existing doc.
  assertEquals(state.multiModels["m:sparse"].content.length, 11);
  assertEquals(ctx.failures, []);
});

// ============================================================================
// D2 — volume arithmetic: populate (batches × types) vs refresh (exact size)
// ============================================================================

Deno.test("D2: populate generates docCount batches with every type represented equally", () => {
  const state = createEmptyDatabaseState();
  const ctx = quickCtx();

  populateMultiCollections(
    state,
    { events: { alpha: GOOD, beta: GOOD } },
    "ifEmpty",
    ctx,
  );

  const content = state.multiCollections["events"].content;
  assertEquals(content.length, 20, "10 batches × 2 types");
  assertEquals(content.filter((d) => d._type === "alpha").length, 10);
  assertEquals(content.filter((d) => d._type === "beta").length, 10);
  assertEquals(ctx.failures, []);
});

Deno.test("D2: refresh restores exactly the pre-retention size, even with multiple types", () => {
  const validator = createSimulationValidator({
    powerLevel: "quick",
    stateRetentionRatio: 0.5,
  });

  const state = createEmptyDatabaseState();
  state.multiCollections["events"] = {
    content: Array.from({ length: 10 }, (_, i) => ({
      _id: `event:${i}`,
      _type: i % 2 === 0 ? "alpha" : "beta",
      name: `e${i}`,
    })),
  };

  const prepared = validator.prepareStateForNextMigration(state, {
    multiCollections: { events: { alpha: GOOD, beta: GOOD } },
  });

  const content = prepared.multiCollections["events"].content;
  // 5 retained + exactly 5 fresh. The old ceil-per-type refresh produced
  // 5 + ceil(5/2)×2 = 11 — growth that compounds across a migration chain.
  assertEquals(content.length, 10);
  // Retention keeps the FIRST keepCount documents.
  assertEquals(content[0]._id, "event:0");
  assertEquals(content[4]._id, "event:4");
});

// ============================================================================
// D3 — generation failures are recorded, never swallowed
// ============================================================================

Deno.test("D3: a generation failure is recorded and the target aborted, not thrown or swallowed", () => {
  const state = createEmptyDatabaseState();
  const ctx = quickCtx();

  populateCollections(state, { relics: BROKEN }, "always", ctx);

  assertEquals(state.collections["relics"].content.length, 0);
  assertEquals(ctx.failures.length, 1);
  assertEquals(ctx.failures[0].bucket, "collections");
  assertEquals(ctx.failures[0].collection, "relics");
});

Deno.test("D3: a failing type aborts the whole target after recording one failure", () => {
  const state = createEmptyDatabaseState();
  const ctx = quickCtx();

  // Insertion order matters: the good type generates once, then the broken
  // type aborts the collection — no per-batch retry of a structural failure.
  populateMultiCollections(
    state,
    { events: { good: GOOD, bad: BROKEN } },
    "ifEmpty",
    ctx,
  );

  assertEquals(state.multiCollections["events"].content.length, 1);
  assertEquals(ctx.failures.length, 1);
  assert(
    ctx.failures[0].message.includes('type "bad"'),
    `failure must name the failing type, got: ${ctx.failures[0].message}`,
  );
});

// ============================================================================
// D4 — multi-model population is per MODEL, not per bucket
// ============================================================================

Deno.test("D4: 'ifEmpty' populates a model without instances even when the bucket is non-empty", () => {
  const state = createEmptyDatabaseState();
  state.multiModels["a:real1"] = {
    modelType: "a",
    content: [{ _id: "doc:1", _type: "t", name: "real" }],
  };
  const ctx = quickCtx();

  populateDeclaredBuckets(
    state,
    { multiModels: { a: { t: GOOD }, b: { t: GOOD } } },
    "ifEmpty",
    ctx,
  );

  // Model `a` already has an instance: untouched, no synthetic sibling.
  assertEquals(state.multiModels["a:real1"].content.length, 1);
  const aInstances = Object.values(state.multiModels).filter(
    (i) => i.modelType === "a",
  );
  assertEquals(aInstances.length, 1);
  // Model `b` had none: it gets a populated synthetic instance whose name
  // follows the real `<model>:<id>` convention (realized, not `b:instance1`).
  const bNames = Object.keys(state.multiModels).filter(
    (name) => state.multiModels[name].modelType === "b",
  );
  assertEquals(bNames.length, 1, "model b must get exactly one instance");
  assert(
    /^b:[a-zA-Z0-9]+$/.test(bNames[0]),
    `instance name must be a valid model id, got: ${bNames[0]}`,
  );
  assertEquals(state.multiModels[bNames[0]].content.length, 10);
  assertEquals(ctx.failures, []);
});

// ============================================================================
// D5 — existing entries are preserved, never reassigned
// ============================================================================

Deno.test("D5: an existing instance is never reassigned — synthetic names avoid taken ones", () => {
  const state = createEmptyDatabaseState();
  state.multiModels["m:real1"] = {
    modelType: "m",
    content: [{ _id: "keep-me", _type: "t", name: "original" }],
  };
  const ctx = quickCtx();

  populateSyntheticMultiModelInstances(
    state,
    { m: { t: GOOD } },
    "always",
    ctx,
  );

  // Realized names exclude existing entries, so the real instance survives
  // byte-identical and the synthetic documents land in a fresh sibling.
  const real = state.multiModels["m:real1"].content;
  assertEquals(real.length, 1, "existing instance must stay untouched");
  assertEquals(real[0]._id, "keep-me", "existing docs must survive");
  const synthetic = Object.keys(state.multiModels).filter(
    (name) => name !== "m:real1" && state.multiModels[name].modelType === "m",
  );
  assertEquals(synthetic.length, 1, "one synthetic sibling instance");
  assertEquals(state.multiModels[synthetic[0]].content.length, 10);
});

Deno.test("D5: 'ifSparse' has no defined meaning for synthetic instances and fails loud", () => {
  const state = createEmptyDatabaseState();
  assertThrows(
    () =>
      populateSyntheticMultiModelInstances(
        state,
        { m: { t: GOOD } },
        "ifSparse",
        quickCtx(),
      ),
    Error,
    "ifSparse",
  );
});

// ============================================================================
// D6 — canonical bucket order
// ============================================================================

Deno.test("D6: buckets are processed in the DatabaseState declaration order", () => {
  const state = createEmptyDatabaseState();
  const ctx = quickCtx();

  // One broken schema per bucket: the failure sequence exposes the order.
  populateDeclaredBuckets(
    state,
    {
      collections: { c: BROKEN },
      multiCollections: { mc: { t: BROKEN } },
      multiModels: { mm: { t: BROKEN } },
      scopedMultiCollections: {
        sc: { scope: v.string(), types: { t: BROKEN } },
      },
    },
    "always",
    ctx,
  );

  assertEquals(ctx.failures.map((f) => f.bucket), [
    "collections",
    "multiCollections",
    "multiModels",
    "scopedMultiCollections",
  ]);
});

// ============================================================================
// Severity rule — empty target = error, surviving documents = warning
// ============================================================================

Deno.test("gate: a generation failure leaving a declared collection empty FAILS the validation", async () => {
  const schemas = { collections: { relics: BROKEN } };
  const parent = migrationDefinition("001", "baseline", {
    parent: null,
    schemas,
    migrate: (b) => b.createCollection("relics").end().compile(),
  });
  const child = migrationDefinition("002", "noop-child", {
    parent,
    schemas,
    migrate: (b) => b.compile(),
  });

  // Standalone path: the initial mock state is built from the parent schemas,
  // generation fails, "relics" stays empty — the loops validating documents
  // would iterate nothing. Pre-refactor this returned a green result.
  const result = await createSimulationValidator({ powerLevel: "quick" })
    .validateMigration(child);

  assertEquals(result.success, false);
  assert(
    result.errors.some((e) =>
      e.includes('Mock data generation failed for collection "relics"')
    ),
    `expected a generation-failure error naming "relics", got: ${
      result.errors.join(" | ")
    }`,
  );
});

Deno.test("gate: preparation failures ride the state and block the next validation when the target is empty", async () => {
  const schemas = { collections: { relics: BROKEN } };
  const validator = createSimulationValidator({ powerLevel: "quick" });

  const prepared = validator.prepareStateForNextMigration(
    createEmptyDatabaseState(),
    schemas,
  );

  // The locked `(state, schemas) => state` signature leaves the state as the
  // only channel: failures must ride it.
  assert(
    prepared.mockGenerationFailures &&
      prepared.mockGenerationFailures.length > 0,
    "prepared state must carry the generation failures",
  );
  assertEquals(prepared.collections["relics"].content.length, 0);

  const parent = migrationDefinition("001", "baseline", {
    parent: null,
    schemas,
    migrate: (b) => b.createCollection("relics").end().compile(),
  });
  const child = migrationDefinition("002", "noop-child", {
    parent,
    schemas,
    migrate: (b) => b.compile(),
  });

  const result = await validator.validateMigration(child, prepared);
  assertEquals(result.success, false);
  assert(
    result.errors.some((e) =>
      e.includes('Mock data generation failed for collection "relics"')
    ),
    `expected the inherited failure to surface as an error, got: ${
      result.errors.join(" | ")
    }`,
  );
});

Deno.test("gate: a generation failure with surviving documents degrades to a warning, not an error", async () => {
  // Generation fails (the generator cannot guess the checked token) but the
  // schema VALIDATES fine — so retained documents keep the validation
  // meaningful and the failure must not block.
  const picky = {
    _id: v.string(),
    magic: v.pipe(v.string(), v.check((s: string) => s === "token-42")),
  };
  const schemas = { collections: { relics: picky } };

  const validator = createSimulationValidator({
    powerLevel: "quick",
    stateRetentionRatio: 0.5,
  });

  const state = createEmptyDatabaseState();
  state.collections["relics"] = {
    content: Array.from({ length: 4 }, (_, i) => ({
      _id: `relic:${i}`,
      magic: "token-42",
    })),
  };

  const prepared = validator.prepareStateForNextMigration(state, schemas);
  assert(
    prepared.mockGenerationFailures &&
      prepared.mockGenerationFailures.length > 0,
    "refresh generation must have failed",
  );
  // Retention kept documents: the target is NOT empty.
  assertEquals(prepared.collections["relics"].content.length, 2);

  const parent = migrationDefinition("001", "baseline", {
    parent: null,
    schemas,
    migrate: (b) => b.createCollection("relics").end().compile(),
  });
  const child = migrationDefinition("002", "noop-child", {
    parent,
    schemas,
    migrate: (b) => b.compile(),
  });

  const result = await validator.validateMigration(child, prepared);
  assertEquals(
    result.errors,
    [],
    `surviving documents must not block, got: ${result.errors.join(" | ")}`,
  );
  assertEquals(result.success, true);
  assert(
    result.warnings.some((e) =>
      e.includes('Mock data generation failed for collection "relics"')
    ),
    `expected a generation-failure warning, got: ${
      result.warnings.join(" | ")
    }`,
  );
});

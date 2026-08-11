/**
 * Production creates one multi-model instance per root entity, named after
 * the root `_id`. The simulator generated a single instance whatever the root
 * count, so a consolidation merging root with instance documents only ever
 * merged one scope — every other scope produced a document amputated of the
 * fields only the instance carries.
 *
 * Locks the cardinality: instances follow the entity pool, one per pooled id,
 * none invented; an empty pool keeps the single-instance fallback; and any
 * truncation by the power-level cap is reported, never silent.
 */
import { assert, assertEquals } from "@std/assert";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { createSimulationValidator } from "../../src/migration/validators/simulation.ts";
import {
  createEmptyDatabaseState,
  type MockGenerationFailure,
  type SimulationDatabaseState,
} from "../../src/migration/types.ts";
import {
  createCorrelationSession,
  foldMockGenerationFailures,
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
  maxInstances?: number,
): MockPopulateContext {
  const config = getMockGenerationConfig("quick");
  return {
    config: maxInstances === undefined
      ? config
      : { ...config, MAX_INSTANCES_PER_MODEL: maxInstances },
    failures: [] as MockGenerationFailure[],
    session: createCorrelationSession({ schemas, seed: 7 }),
  };
}

function instanceNames(state: SimulationDatabaseState, model: string) {
  return Object.keys(state.multiModels).filter(
    (name) => state.multiModels[name].modelType === model,
  );
}

Deno.test("cardinality: every minted root id gets its own instance, none invented", () => {
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
    "the quick cap equals the root count — nothing to report",
  );
});

Deno.test("cardinality: an empty entity pool keeps the single synthetic instance", () => {
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

Deno.test("cardinality: the instance cap truncates loudly and never blocks", () => {
  const state = createEmptyDatabaseState();
  const ctx = contextFor(OWNED, 3);

  populateDeclaredBuckets(state, OWNED, "always", ctx);

  const rootCount = state.collections["+expositions"].content.length;
  assertEquals(instanceNames(state, "exposition").length, 3);

  const capFindings = ctx.failures.filter(
    (f) => f.kind === "correlation" && f.space === "exposition",
  );
  assertEquals(capFindings.length, 1, "a truncated cardinality must be told");
  assert(
    capFindings[0].message.includes(String(rootCount - 3)),
    `the finding must name the entities left instance-less: ${
      capFindings[0].message
    }`,
  );

  const { errors, warnings } = foldMockGenerationFailures(
    ctx.failures,
    OWNED,
    state,
  );
  assertEquals(errors, [], "a cap finding is never blocking");
  assert(warnings.some((w) => w.includes("caps synthetic instances")));
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
      b.flowToScope({
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

Deno.test("consolidation: EVERY scope produces a complete document, not just the first", async () => {
  const result = await createSimulationValidator({ powerLevel: "quick" })
    .validateMigration(consolidationChild());

  assertEquals(
    result.errors,
    [],
    `consolidation must validate, got: ${result.errors.join(" | ")}`,
  );

  const state = result.data?.stateAfterMigration as SimulationDatabaseState;
  const information = state.scopedMultiCollections["+expositions_scoped"]
    .content.filter((d) => d._type === "information");

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

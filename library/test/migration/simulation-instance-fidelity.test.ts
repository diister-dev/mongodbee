/**
 * The simulator must model what production does. Three divergences shipped
 * silently because the flow primitives were only ever exercised against
 * hand-written, well-formed state — never against the state the simulator
 * generates for itself:
 *
 *  1. Mock instances were named `<model>@instance<N>`, while the real
 *     convention (and `discoverMultiCollectionInstances`) is `<model>:<id>`.
 *     `flowToScope` uses the instance name as the `_scope` value, so every
 *     simulated consolidation into a scope-constrained collection failed on a
 *     name the simulator itself had invented.
 *  2. The memory applier treated ANY entry with a matching `modelType` as an
 *     instance, including the bare `<model>` registry entry the simulator
 *     creates. The mongodb applier requires the `<model>:` prefix, so the two
 *     appliers disagreed on what an instance is.
 *  3. Mock generation honoured a declared `v.maxLength(N)` verbatim, so a
 *     schema bounding an array at 150k produced 150k items and the validator
 *     ran out of memory cloning the state.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { createSimulationValidator } from "../../src/migration/validators/simulation.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const EXPO_TYPES = {
  information: { _id: refId("exposition"), name: v.string() },
};

const SCOPED = {
  scopedMultiCollections: {
    "+expositions": { scope: refId("exposition"), types: EXPO_TYPES },
  },
};

test("simulation: instances flowed into a scope-constrained collection carry a valid _scope", async () => {
  // The parent owns the multi-model, so the validator mock-populates its
  // instances; the child consolidates them into a scoped collection whose
  // `scope` is regex-constrained. The instance NAME becomes `_scope`, so a
  // synthetic name that does not follow `<model>:<id>` fails here.
  const parent = migrationDefinition("001", "baseline", {
    parent: null,
    schemas: { multiModels: { exposition: EXPO_TYPES } },
    migrate: (b) => b.compile(),
  });
  const child = migrationDefinition("002", "consolidate", {
    parent,
    schemas: SCOPED,
    migrate: (b) =>
      b
        .flowToScope({
          from: { kind: "multiModelInstances", model: "exposition" },
          into: { collection: "+expositions" },
          scope: (_d, ctx) => ctx.instanceName!,
          map: (d, ctx) => ({ ...d, _id: ctx.instanceName }),
          onConflict: "merge",
          merge: (a, b2) => ({ ...b2, ...a }),
          source: "consume",
        })
        .compile(),
  });

  const result = await createSimulationValidator({
    powerLevel: "quick",
  }).validateMigration(child);

  assertEquals(
    result.errors,
    [],
    `simulated consolidation must produce valid scopes, got: ${result.errors.join(
      " | ",
    )}`,
  );
});

test("memory applier: the bare `<model>` registry entry is not an instance", async () => {
  const state = createEmptyDatabaseState();
  // What the simulator builds: the bare registry entry alongside real ones.
  state.multiModels["exposition"] = {
    modelType: "exposition",
    content: [{ _type: "information", _id: "exposition:ghost", name: "ghost" }],
  };
  state.multiModels["exposition:A"] = {
    modelType: "exposition",
    content: [{ _type: "information", _id: "exposition:A", name: "A" }],
  };

  const m = migrationDefinition("001", "consolidate", {
    parent: null,
    schemas: SCOPED,
    migrate: (b) =>
      b
        .flowToScope({
          from: { kind: "multiModelInstances", model: "exposition" },
          into: { collection: "+expositions" },
          scope: (_d, ctx) => ctx.instanceName!,
          source: "consume",
        })
        .compile(),
  });

  const ops = m.migrate(migrationBuilder({ schemas: SCOPED })).operations;
  await createMemoryApplier(m).applyMigration(state, ops, "up");

  const scopes = new Set(
    state.scopedMultiCollections["+expositions"].content.map((d) => d._scope),
  );
  assertEquals(scopes.has("exposition"), false, "bare entry must not flow");
  assertEquals(scopes.has("exposition:A"), true, "real instance must flow");
});

test("simulation: mock arrays stay bounded when a schema declares a huge maxLength", () => {
  const schemas = {
    collections: {
      "+maps": {
        _id: v.string(),
        // The shape that took the validator out of memory: the bound describes
        // the domain, not the size a mock needs.
        grids: v.pipe(
          v.array(v.pipe(v.array(v.number()), v.maxLength(150_000))),
          v.maxLength(50),
        ),
      },
    },
  };

  const state = createEmptyDatabaseState();
  state.collections["+maps"] = {
    content: [
      { _id: "map:1", grids: [] },
      { _id: "map:2", grids: [] },
    ],
  };

  const prepared = createSimulationValidator({
    powerLevel: "quick",
  }).prepareStateForNextMigration(state, schemas);

  for (const doc of prepared.collections["+maps"].content) {
    const grids = (doc.grids ?? []) as number[][];
    assert(grids.length <= 100, `outer array unbounded: ${grids.length}`);
    for (const run of grids) {
      assert(run.length <= 100, `inner array unbounded: ${run.length}`);
    }
  }
});

/**
 * Mirrors the diivento `consolidate_expositions_scoped` migration: root
 * documents in a global collection, a multi-model whose instances are named
 * after those SAME root ids, and a two-step `flowToScope` consolidation —
 * roots become `information` docs (scope = their own `_id`), instance docs
 * flow into scope = `ctx.instanceName` with the per-instance `information`
 * singleton re-keyed onto the scope so it MERGES with the root-derived one.
 *
 * Locks correlated identity generation: mock root ids and instance names
 * must coincide for the `onConflict: "merge"` branch to execute. The proof
 * is a document carrying BOTH a root-only field and an instance-only field —
 * it can only exist if the merge branch ran.
 */
import { assert, assertEquals } from "@std/assert";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { createSimulationValidator } from "../../src/migration/validators/simulation.ts";
import {
  createEmptyDatabaseState,
  type SimulationDatabaseState,
} from "../../src/migration/types.ts";
import { foldMockGenerationFailures } from "../../src/migration/validators/mock/mod.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const PARENT_SCHEMAS = {
  collections: {
    // Root documents own the "exposition" id space — the pool their ids fill
    // is where instance names are drawn from.
    "+expositions": {
      _id: refId("exposition"),
      name: v.string(), // root-only field
    },
  },
  multiModels: {
    exposition: {
      information: {
        _id: refId("information"),
        description: v.string(), // instance-only field
      },
      participant: {
        _id: refId("participant"),
        label: v.string(),
      },
    },
  },
};

const CHILD_SCHEMAS = {
  scopedMultiCollections: {
    "+expositions_scoped": {
      scope: refId("exposition"),
      types: {
        // Root singleton: `_id === _scope`. Optional fields because a root
        // without a matching instance contributes name only, and vice versa.
        information: {
          _id: refId("exposition"),
          name: v.optional(v.string()),
          description: v.optional(v.string()),
        },
        participant: {
          _id: refId("participant"),
          label: v.string(),
        },
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
        // global roots → `information` docs (scope = their own _id)
        from: { kind: "collection", name: "+expositions" },
        into: { collection: "+expositions_scoped" },
        toType: () => "information",
        scope: (d) => d._id as string,
        source: "consume",
      })
        .flowToScope({
          // each instance's docs → its scope (= instance name)
          from: { kind: "multiModelInstances", model: "exposition" },
          into: { collection: "+expositions_scoped" },
          scope: (_d, ctx) => ctx.instanceName!,
          map: (d, ctx) =>
            d._type === "information"
              ? { ...d, _id: ctx.instanceName } // align with root → merge
              : d,
          onConflict: "merge",
          merge: (root, sub) => ({ ...sub, ...root }), // root fields win
          source: "consume",
        })
        .compile(),
  });
}

Deno.test("simulation: correlated identities make the root↔instance merge branch executable", async () => {
  const result = await createSimulationValidator({ powerLevel: "quick" })
    .validateMigration(consolidationChild());

  assertEquals(
    result.errors,
    [],
    `consolidation must validate, got: ${result.errors.join(" | ")}`,
  );

  const state = result.data?.stateAfterMigration as SimulationDatabaseState;
  const scoped = state.scopedMultiCollections["+expositions_scoped"].content;

  // The proof: a document carrying BOTH the root-only field (`name`) and the
  // instance-only field (`description`) can only exist if a mock instance
  // was named after a real root id AND the merge branch executed on it.
  const merged = scoped.filter(
    (d) =>
      d._type === "information" &&
      typeof d.name === "string" &&
      typeof d.description === "string",
  );
  assert(
    merged.length > 0,
    "the onConflict: 'merge' branch never executed — no information " +
      "document carries both root and instance fields, so mock instance " +
      "names did not coincide with root ids",
  );
  // Root-singleton shape survives the merge: the document IS its scope's record.
  for (const doc of merged) {
    assertEquals(
      doc._id,
      doc._scope,
      "merged information keeps _id === _scope",
    );
  }

  // Instance documents landed in scopes that exist as root records — not in
  // a parallel universe of invented scopes.
  const informationScopes = new Set(
    scoped.filter((d) => d._type === "information").map((d) => d._scope),
  );
  for (const p of scoped.filter((d) => d._type === "participant")) {
    assert(
      informationScopes.has(p._scope),
      `participant scope "${p._scope}" has no information record`,
    );
  }
});

Deno.test("simulation: a validation replays identically — same migration, same state", async () => {
  // The session RNG replaced the engine's bare Math.random draws; the seed
  // derives from the migration id. Two standalone validations of the same
  // migration must therefore produce byte-identical simulated states — the
  // property that makes two simulation runs diffable.
  const first = await createSimulationValidator({ powerLevel: "quick" })
    .validateMigration(consolidationChild());
  const second = await createSimulationValidator({ powerLevel: "quick" })
    .validateMigration(consolidationChild());

  assertEquals(
    first.data?.stateAfterMigration,
    second.data?.stateAfterMigration,
  );
});

Deno.test("severity: a correlation finding is never blocking, even on an empty declared target", () => {
  // A generation failure on a declared-but-empty target is a blocking error
  // (zero documents = zero assertions). A correlation finding under the SAME
  // conditions stays a warning: the documents' identities are degraded, not
  // absent — the two kinds must never share severity.
  const schemas = { collections: { relics: { _id: v.string() } } };
  const state = createEmptyDatabaseState();
  state.collections["relics"] = { content: [] };

  const { errors, warnings } = foldMockGenerationFailures(
    [
      {
        bucket: "collections",
        collection: "relics",
        kind: "correlation",
        space: "relic",
        message:
          'Identifier space "relic" is referenced but no _id schema mints it',
      },
    ],
    schemas,
    state,
  );

  assertEquals(errors, [], "correlation findings must never block");
  assertEquals(warnings.length, 1);
  assert(warnings[0].includes('Identifier space "relic"'));
});

/**
 * Mirrors the STRUCTURE of a real migration chain, not a single migration:
 * several successive migrations validated with the exact CLI loop —
 * `validateMigration` + `prepareStateForNextMigration` between steps — so
 * identities must survive retention/refresh churn from one migration to the
 * next. A mono-migration scenario cannot see the decay this exercises: the
 * first prepared state is perfectly correlated, and every LATER preparation
 * is where roots and instances historically drifted apart.
 *
 * The chain shape mirrors diivento's: an init declaring root collection +
 * multi-model without instantiating anything, two field-level migrations
 * whose bucket/type keys are IDENTICAL (the real chains do this), then a
 * `flowToScope` consolidation whose merged type REQUIRES the instance-only
 * fields, then one more migration living on the consolidated state.
 */
import { assert, assertEquals } from "@std/assert";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { createSimulationValidator } from "../../src/migration/validators/simulation.ts";
import {
  createEmptyDatabaseState,
  type MigrationDefinition,
  type SimulationDatabaseState,
} from "../../src/migration/types.ts";
import { dbId, refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

// ---------------------------------------------------------------------------
// The chain
// ---------------------------------------------------------------------------

const M1_SCHEMAS = {
  collections: {
    "+expositions": {
      _id: dbId("exposition"),
      owner: v.string(),
      brand: v.optional(v.string()), // root-only field
    },
    "+users": {
      _id: dbId("user"),
      tag: v.string(),
    },
  },
  multiModels: {
    exposition: {
      information: {
        _id: v.literal("information:0"),
        owner: v.string(),
        name: v.string(), // instance-only, REQUIRED after consolidation
        events: v.array(v.string()), // instance-only, REQUIRED after consolidation
      },
      participant: {
        _id: dbId("participant"),
        label: v.string(),
      },
    },
  },
};

function buildChain(): MigrationDefinition[] {
  const m1 = migrationDefinition("001", "init", {
    parent: null,
    schemas: M1_SCHEMAS,
    migrate: (b) => {
      b.createCollection("+expositions");
      b.createCollection("+users");
      return b.compile();
    },
  });

  // Field-level change only — bucket/type keys identical to m1.
  const m2 = migrationDefinition("002", "rename_user_tag", {
    parent: m1,
    schemas: {
      collections: {
        ...M1_SCHEMAS.collections,
        "+users": { _id: dbId("user"), label: v.string() },
      },
      multiModels: { ...M1_SCHEMAS.multiModels },
    },
    migrate: (b) => {
      b.collection("+users").transform({
        up: (doc: Record<string, unknown>) => {
          const { tag, ...rest } = doc;
          return { ...rest, label: tag };
        },
        down: (doc: Record<string, unknown>) => {
          const { label, ...rest } = doc;
          return { ...rest, tag: label };
        },
      });
      return b.compile();
    },
  });

  // Same-key field rename on root + information — bucket/type keys still
  // identical, so two consecutive preparations share a schemas fingerprint.
  const m3 = migrationDefinition("003", "rename_owner", {
    parent: m2,
    schemas: {
      collections: {
        ...m2.schemas.collections,
        "+expositions": {
          _id: dbId("exposition"),
          createdBy: v.string(),
          brand: v.optional(v.string()),
        },
      },
      multiModels: {
        exposition: {
          ...M1_SCHEMAS.multiModels.exposition,
          information: {
            _id: v.literal("information:0"),
            createdBy: v.string(),
            name: v.string(),
            events: v.array(v.string()),
          },
        },
      },
    },
    migrate: (b) => {
      b.collection("+expositions").transform({
        up: (doc: Record<string, unknown>) => {
          const { owner, ...rest } = doc;
          return { ...rest, createdBy: owner };
        },
        down: (doc: Record<string, unknown>) => {
          const { createdBy, ...rest } = doc;
          return { ...rest, owner: createdBy };
        },
      });
      b.multiModelInstances("exposition").type("information").transform({
        up: (doc: Record<string, unknown>) => {
          const { owner, ...rest } = doc;
          return { ...rest, createdBy: owner };
        },
        down: (doc: Record<string, unknown>) => {
          const { createdBy, ...rest } = doc;
          return { ...rest, owner: createdBy };
        },
      });
      return b.compile();
    },
  });

  // Consolidation: roots + instances fuse into ONE scoped collection reusing
  // the roots' name. `name`/`events` are REQUIRED — a root whose instance
  // went missing during propagation produces an amputated document here.
  const scopedTypes = {
    information: {
      _id: refId("exposition"),
      createdBy: v.string(),
      name: v.string(),
      events: v.array(v.string()),
      brand: v.optional(v.string()),
    },
    participant: {
      _id: dbId("participant"),
      label: v.string(),
    },
  };
  const m4 = migrationDefinition("004", "consolidate", {
    parent: m3,
    schemas: {
      collections: { "+users": m3.schemas.collections["+users"] },
      scopedMultiCollections: {
        "+expositions": { scope: refId("exposition"), types: scopedTypes },
      },
    },
    migrate: (b) => {
      const TEMP = "+expositions__migrating";
      return b
        .flowToScope({
          from: { kind: "collection", name: "+expositions" },
          into: { collection: TEMP },
          toType: () => "information",
          scope: (doc) => doc._id as string,
          onConflict: "skip",
          source: "consume",
        })
        .flowToScope({
          from: { kind: "multiModelInstances", model: "exposition" },
          into: { collection: TEMP },
          scope: (_doc, ctx) => ctx.instanceName!,
          map: (doc, ctx) =>
            doc._type === "information"
              ? { ...doc, _id: ctx.instanceName }
              : doc,
          onConflict: "merge",
          merge: (root, sub) => ({ ...sub, ...root }),
          source: "consume",
        })
        .renameCollection(TEMP, "+expositions")
        .compile();
    },
  });

  // Life goes on after the consolidation: the amputees of m4 — if any — are
  // revalidated here, forward and after rollback.
  const m5 = migrationDefinition("005", "after_consolidation", {
    parent: m4,
    schemas: {
      collections: { ...m4.schemas.collections },
      scopedMultiCollections: { ...m4.schemas.scopedMultiCollections },
    },
    migrate: (b) => {
      b.scopedMultiCollection("+expositions").type("participant").transform({
        up: (doc: Record<string, unknown>) => doc,
        down: (doc: Record<string, unknown>) => doc,
      }).end();
      return b.compile();
    },
  });

  return [m1, m2, m3, m4, m5];
}

// ---------------------------------------------------------------------------
// Coverage measurement
// ---------------------------------------------------------------------------

interface Coverage {
  rootIds: string[];
  instanceNames: string[];
  rootsWithoutInstance: string[];
  orphanInstances: string[];
}

function measureCoverage(state: SimulationDatabaseState): Coverage {
  const rootIds = (state.collections["+expositions"]?.content ?? [])
    .map((d) => String(d._id));
  const instanceNames = Object.entries(state.multiModels)
    .filter(([, i]) => i.modelType === "exposition")
    .map(([name]) => name);
  const names = new Set(instanceNames);
  const roots = new Set(rootIds);
  return {
    rootIds,
    instanceNames,
    rootsWithoutInstance: rootIds.filter((id) => !names.has(id)),
    orphanInstances: instanceNames.filter((name) => !roots.has(name)),
  };
}

// ---------------------------------------------------------------------------
// The test — drives the EXACT loop of the CLI gate
// ---------------------------------------------------------------------------

Deno.test("chain propagation: root↔instance correlation survives every preparation, and the consolidation merges every scope", async () => {
  const validator = createSimulationValidator({ powerLevel: "quick" });
  let currentState: SimulationDatabaseState = createEmptyDatabaseState();

  for (const migration of buildChain()) {
    if (migration.schemas.multiModels) {
      // Every prepared state must keep the production invariant: an entity's
      // root document and its instance exist together, or not at all.
      const cov = measureCoverage(currentState);
      assertEquals(
        cov.rootsWithoutInstance,
        [],
        `before "${migration.name}": ${cov.rootsWithoutInstance.length}/` +
          `${cov.rootIds.length} roots have no instance — a consolidation ` +
          `would amputate them of the instance-only fields`,
      );
      assertEquals(
        cov.orphanInstances,
        [],
        `before "${migration.name}": ${cov.orphanInstances.length} instances ` +
          `have no root — they would consolidate into scopes no root backs`,
      );
    }

    const result = await validator.validateMigration(migration, currentState);
    assertEquals(
      result.errors,
      [],
      `"${migration.name}" must validate, got:\n${result.errors.join("\n")}`,
    );

    const after = result.data?.stateAfterMigration as SimulationDatabaseState;
    if (migration.name === "consolidate") {
      const information = after.scopedMultiCollections["+expositions"].content
        .filter((d) => d._type === "information");
      assert(
        information.length > 1,
        "the scenario must produce several scopes",
      );
      const amputated = information.filter(
        (d) => typeof d.name !== "string" || !Array.isArray(d.events),
      );
      assertEquals(
        amputated.map((d) => d._scope),
        [],
        `${amputated.length}/${information.length} information documents ` +
          `lost the fields only their instance carries`,
      );
    }

    currentState = validator.prepareStateForNextMigration(
      after,
      migration.schemas,
    );
  }
});

Deno.test("chain propagation: instance volume stays bounded across preparations", async () => {
  // Coverage repair must not grow the instance set step after step: dropped
  // roots take their instances along, fresh roots get exactly one each.
  const validator = createSimulationValidator({ powerLevel: "quick" });
  let currentState: SimulationDatabaseState = createEmptyDatabaseState();
  const counts: number[] = [];

  for (const migration of buildChain()) {
    if (migration.schemas.multiModels) {
      const cov = measureCoverage(currentState);
      if (cov.rootIds.length > 0) {
        counts.push(cov.instanceNames.length);
        assertEquals(
          cov.instanceNames.length,
          cov.rootIds.length,
          `before "${migration.name}": instance count must equal root count`,
        );
      }
    }
    const result = await validator.validateMigration(migration, currentState);
    currentState = validator.prepareStateForNextMigration(
      result.data?.stateAfterMigration as SimulationDatabaseState ??
        currentState,
      migration.schemas,
    );
  }
  assert(
    counts.length >= 2,
    "the chain must measure at least two preparations",
  );
});

// ---------------------------------------------------------------------------
// Irreversible migrations have no rollback state to validate
// ---------------------------------------------------------------------------

Deno.test("chain propagation: an irreversible status collapse is not validated against a rollback that can never run", async () => {
  // Mirrors a real status-machine collapse: the transform is declared
  // irreversible, so the real rollback path refuses the whole migration —
  // there is no after-rollback state whose documents could be checked
  // against the parent union.
  const parent = migrationDefinition("101", "registrations", {
    parent: null,
    schemas: {
      scopedMultiCollections: {
        "+registrations": {
          scope: refId("exposition"),
          types: {
            registration: {
              _id: dbId("registration"),
              status: v.picklist(["pending", "confirmed", "cancelled"]),
            },
          },
        },
      },
    },
    migrate: (b) => {
      b.createScopedMultiCollection("+registrations");
      return b.compile();
    },
  });

  const REMAP: Record<string, string> = {
    pending: "registered",
    confirmed: "registered",
    cancelled: "cancelled_invalidated",
  };
  const child = migrationDefinition("102", "collapse_statuses", {
    parent,
    schemas: {
      scopedMultiCollections: {
        "+registrations": {
          scope: refId("exposition"),
          types: {
            registration: {
              _id: dbId("registration"),
              status: v.picklist(["registered", "cancelled_invalidated"]),
            },
          },
        },
      },
    },
    migrate: (b) => {
      b.scopedMultiCollection("+registrations").type("registration").transform({
        irreversible: true,
        up: (doc: Record<string, unknown>) => ({
          ...doc,
          status: REMAP[String(doc.status)] ?? doc.status,
        }),
        down: (doc: Record<string, unknown>) => doc,
      }).end();
      return b.compile();
    },
  });

  const validator = createSimulationValidator({ powerLevel: "quick" });
  const first = await validator.validateMigration(parent);
  assertEquals(first.errors, []);
  const prepared = validator.prepareStateForNextMigration(
    first.data?.stateAfterMigration as SimulationDatabaseState,
    parent.schemas,
  );

  const result = await validator.validateMigration(child, prepared);
  assertEquals(
    result.errors,
    [],
    `an irreversible migration must not fail on rollback revalidation:\n${
      result.errors.join("\n")
    }`,
  );
  assertEquals(result.data?.hasIrreversibleProperty, true);
});

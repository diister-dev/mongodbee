/**
 * Regression tests for the N12 minor-sweep fixes.
 *
 *  - N12c: `deterministicSeedId` with an empty prefix must yield the bare
 *    fingerprint, never a leading colon (`:a1b2…`).
 *  - N12e: `flow`'s `targetIdSchema` lookup must also consult the
 *    multi-collection and scoped-multi-collection schema buckets, not only
 *    `schemas.collections`.
 *  - N12d: the simulation validator must not crash on a multi-collection that
 *    is declared in the schema but never created (undefined state content).
 *  - N12b: a `createScopedMultiCollection`-only migration is lossy, so the
 *    lossy-operation display must have an operation to describe.
 */

import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { refId } from "../../src/ids.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import {
  deterministicSeedId,
  extractIdPrefix,
} from "../../src/migration/utils/seed-id.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { createSimulationValidator } from "../../src/migration/validators/simulation.ts";

// ============================================================================
// N12c — deterministicSeedId with no prefix
// ============================================================================

test("N12c: deterministicSeedId with empty prefix yields a bare id (no leading colon)", () => {
  const id = deterministicSeedId("", "mig-001", "users", 0);
  assert(!id.startsWith(":"), `expected no leading colon, got "${id}"`);
  assert(
    /^[a-z0-9]+$/.test(id),
    `expected bare [a-z0-9] fingerprint, got "${id}"`,
  );
});

test("N12c: deterministicSeedId keeps the `prefix:fingerprint` shape when a prefix is present", () => {
  const id = deterministicSeedId("user", "mig-001", "users", 0);
  assert(id.startsWith("user:"), `expected "user:" prefix, got "${id}"`);
  assert(
    /^user:[a-z0-9]+$/.test(id),
    `expected "user:<fingerprint>", got "${id}"`,
  );
});

test("N12c: no-prefix id is deterministic across calls", () => {
  const a = deterministicSeedId("", "mig-42", "orders:line", 3);
  const b = deterministicSeedId("", "mig-42", "orders:line", 3);
  assertEquals(a, b);
});

// ============================================================================
// N12e — flow targetIdSchema resolution across schema buckets
// ============================================================================

// These two used to assert that a typed target resolves ONE `targetIdSchema`,
// taken from its first sub-type. That answer was only ever right by accident: a
// multi-collection derives each sub-type's `_id` from the type name, so a
// sub-type that declares none (the normal case) yielded `undefined` and every
// flowed document was written with a bare, prefix-less id. The contract is now
// that a typed target carries NO operation-wide id schema, and the applier reads
// the prefix off each mapped document's `_type`.
test("N12e: flow into a multi-collection defers the id prefix to each document", () => {
  const schemas = {
    collections: {
      source: { _id: v.string(), name: v.string() },
    },
    multiCollections: {
      events: {
        note: { _id: refId("note"), text: v.string() },
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .flow({
      from: { collection: "source" },
      into: { collection: "events" },
      map: (doc) => doc,
    })
    .compile();

  const op = state.operations.find((o) => o.type === "flow");
  assert(op && op.type === "flow", "expected a flow operation");
  assertEquals(op.targetIsTyped, true);
  assertEquals(
    op.targetIdSchema,
    undefined,
    "one schema cannot answer for a target whose sub-types each mint their own id space",
  );
});

test("N12e: flow into a scoped multi-collection defers the id prefix too", () => {
  const schemas = {
    collections: {
      source: { _id: v.string(), name: v.string() },
    },
    scopedMultiCollections: {
      "+scans": {
        scope: refId("exposition"),
        types: {
          scan: { _id: refId("scan"), badgeId: v.string() },
        },
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .flow({
      from: { collection: "source" },
      into: { collection: "+scans" },
      map: (doc) => doc,
    })
    .compile();

  const op = state.operations.find((o) => o.type === "flow");
  assert(op && op.type === "flow", "expected a flow operation");
  assertEquals(op.targetIsTyped, true);
  assertEquals(op.targetIdSchema, undefined);
});

test("N12e: flow into a plain collection still resolves one prefix for the operation", () => {
  const schemas = {
    collections: {
      source: { _id: v.string() },
      archived: { _id: refId("archived"), name: v.string() },
    },
  };

  const state = migrationBuilder({ schemas })
    .flow({
      from: { collection: "source" },
      into: { collection: "archived" },
      map: (doc) => doc,
    })
    .compile();

  const op = state.operations.find((o) => o.type === "flow");
  assert(op && op.type === "flow", "expected a flow operation");
  assertEquals(extractIdPrefix(op.targetIdSchema), "archived");
});

// ============================================================================
// N12d — simulation guard for a declared-but-never-created multi-collection
// ============================================================================

test("N12d: simulation does not crash on a declared-but-never-created multi-collection", async () => {
  // The multi-collection "events" is declared in the schema but the migrate()
  // function never calls createMultiCollection() — so its state content is
  // undefined. Pre-fix this threw "Cannot read properties of undefined".
  const migration = migrationDefinition("001", "declare-only-multicollection", {
    parent: null,
    schemas: {
      multiCollections: {
        events: {
          note: { _id: refId("note"), text: v.string() },
        },
      },
    },
    migrate: (b) => b.compile(),
  });

  const result = await createSimulationValidator({
    powerLevel: "quick",
  }).validateMigration(migration);

  // The simulation must run to completion (guard skips the missing collection)
  // instead of blowing up with a TypeError.
  assertEquals(result.data?.simulationCompleted, true);
  assert(
    !result.errors.some((e) => e.includes("Cannot read properties")),
    `unexpected TypeError leak: ${result.errors.join(" | ")}`,
  );
  // The creation check still reports the real problem.
  assert(
    result.errors.some(
      (e) =>
        e.includes('Multi-collection "events"') && e.includes("not created"),
    ),
    `expected a declared-but-not-created error, got: ${result.errors.join(
      " | ",
    )}`,
  );
});

// ============================================================================
// N12b — a scoped-multi-collection creation is a lossy operation to display
// ============================================================================

test("N12b: createScopedMultiCollection marks the migration lossy and records the op", () => {
  const schemas = {
    scopedMultiCollections: {
      "+scans": {
        scope: refId("exposition"),
        types: {
          scan: { _id: refId("scan"), badgeId: v.string() },
        },
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .createScopedMultiCollection("+scans")
    .end()
    .compile();

  // The lossy-operation display (migrate.ts) filters on this op type; before
  // the sweep it neither matched the filter nor produced a description, so a
  // lossy migration showed an empty detail list.
  assert(state.hasProperty("lossy"), "expected the migration to be lossy");
  assert(
    state.operations.some((o) => o.type === "create_scoped_multicollection"),
    "expected a create_scoped_multicollection operation",
  );
});

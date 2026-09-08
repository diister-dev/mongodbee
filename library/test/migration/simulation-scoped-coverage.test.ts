/**
 * Locks the simulation validator's coverage of `scopedMultiCollections`
 * DOCUMENTS — the second historical blind spot after the chain gate:
 * the simulation validated documents of collections / multi-collections /
 * multi-models against their schemas, but scoped multi-collection documents
 * were never validated, and no mock data was ever generated for them. An
 * invalid or incoherent scoped schema (or seed) passed the simulation
 * silently.
 *
 * Covered here:
 *  1. A seeded scoped document violating its type schema fails, naming
 *     bucket + collection + type.
 *  2. A seeded scoped document whose `_scope` violates the `scope` schema
 *     fails mentioning `_scope`.
 *  3. A document with an unknown `_type` fails.
 *  4. Valid seeds pass.
 *  5. Mock generation round-trip: state built from a parent with scoped
 *     schemas produces documents that validate (envelope `_type`/`_scope`
 *     included) — exercises valibot-mock against regex-constrained scopes.
 *  6. `prepareStateForNextMigration` populates empty scoped collections and
 *     applies retention (no unbounded growth) to non-empty ones.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import {
  createSimulationValidator,
  type SimulationValidator,
} from "../../src/migration/validators/simulation.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const SCAN_TYPES = {
  scan: {
    _id: v.string(),
    badgeId: v.string(),
    at: v.date(),
  },
};

const SCOPED_SCHEMAS = {
  scopedMultiCollections: {
    "+scans": {
      scope: refId("exposition"),
      types: SCAN_TYPES,
    },
  },
};

function quickValidator(): SimulationValidator {
  return createSimulationValidator({ powerLevel: "quick" });
}

test("simulation: scoped seed violating its type schema fails naming bucket/collection/type", async () => {
  const migration = migrationDefinition("001", "seed-invalid-doc", {
    parent: null,
    schemas: SCOPED_SCHEMAS,
    migrate: (b) =>
      b
        .createScopedMultiCollection("+scans")
        .type("scan")
        // BUG on purpose: `at` must be a Date, `badgeId` is missing.
        .seed("exposition:demo1", [
          { _id: "scan:bad1", at: "not-a-date" } as never,
        ])
        .end()
        .end()
        .compile(),
  });

  const result = await quickValidator().validateMigration(migration);

  assertEquals(result.success, false);
  assert(
    result.errors.some(
      (e) =>
        e.includes('scoped multi-collection "+scans"') &&
        e.includes('type "scan"') &&
        e.includes("does not match schema"),
    ),
    `expected a scoped document-mismatch error, got: ${result.errors.join(
      " | ",
    )}`,
  );
});

test("simulation: scoped seed with invalid _scope fails mentioning _scope", async () => {
  const migration = migrationDefinition("001", "seed-invalid-scope", {
    parent: null,
    schemas: SCOPED_SCHEMAS,
    migrate: (b) =>
      b
        .createScopedMultiCollection("+scans")
        .type("scan")
        // BUG on purpose: scope must match ^exposition:[a-zA-Z0-9]+
        .seed("visitor:wrongKind", [
          { _id: "scan:s1", badgeId: "badge:b1", at: new Date() },
        ])
        .end()
        .end()
        .compile(),
  });

  const result = await quickValidator().validateMigration(migration);

  assertEquals(result.success, false);
  assert(
    result.errors.some(
      (e) =>
        e.includes('scoped multi-collection "+scans"') &&
        e.includes("invalid _scope"),
    ),
    `expected an invalid-_scope error, got: ${result.errors.join(" | ")}`,
  );
});

test("simulation: scoped document with unknown _type fails", async () => {
  const parent = migrationDefinition("001", "baseline", {
    parent: null,
    schemas: SCOPED_SCHEMAS,
    migrate: (b) => b.createScopedMultiCollection("+scans").end().compile(),
  });
  const child = migrationDefinition("002", "noop-child", {
    parent,
    schemas: SCOPED_SCHEMAS,
    migrate: (b) => b.compile(),
  });

  const initialState = createEmptyDatabaseState();
  initialState.scopedMultiCollections["+scans"] = {
    content: [
      {
        _id: "ghost:g1",
        _type: "ghost",
        _scope: "exposition:demo1",
      },
    ],
  };

  const result = await quickValidator().validateMigration(child, initialState);

  assertEquals(result.success, false);
  assert(
    result.errors.some(
      (e) =>
        e.includes('scoped multi-collection "+scans"') &&
        e.includes('unknown type "ghost"'),
    ),
    `expected an unknown-type error, got: ${result.errors.join(" | ")}`,
  );
});

test("simulation: valid scoped seeds pass", async () => {
  const migration = migrationDefinition("001", "seed-valid", {
    parent: null,
    schemas: SCOPED_SCHEMAS,
    migrate: (b) =>
      b
        .createScopedMultiCollection("+scans")
        .type("scan")
        .seed("exposition:demo1", [
          { _id: "scan:s1", badgeId: "badge:b1", at: new Date() },
        ])
        .end()
        .end()
        .compile(),
  });

  const result = await quickValidator().validateMigration(migration);

  assertEquals(
    result.errors,
    [],
    `expected no errors, got: ${result.errors.join(" | ")}`,
  );
  assertEquals(result.success, true);
});

test("simulation: mock state built from a scoped parent validates (valimock round-trip incl. _scope)", async () => {
  const parent = migrationDefinition("001", "baseline", {
    parent: null,
    schemas: SCOPED_SCHEMAS,
    migrate: (b) => b.createScopedMultiCollection("+scans").end().compile(),
  });
  const child = migrationDefinition("002", "noop-child", {
    parent,
    schemas: SCOPED_SCHEMAS,
    migrate: (b) => b.compile(),
  });

  // No initialState: the validator builds a hybrid mock state from the
  // parent schemas — including scoped documents with generated _scope values
  // that MUST satisfy the refId regex, then validates them all.
  const result = await quickValidator().validateMigration(child);

  assertEquals(
    result.errors,
    [],
    `expected mock-populated scoped state to validate, got: ${result.errors.join(
      " | ",
    )}`,
  );
  assertEquals(result.success, true);
});

test("prepareStateForNextMigration: populates empty scoped collections and bounds non-empty ones", () => {
  const validator = quickValidator();

  const state = createEmptyDatabaseState();
  // Non-empty scoped collection: retention must not grow it.
  const seededDocs = Array.from({ length: 4 }, (_, i) => ({
    _id: `scan:s${i}`,
    _type: "scan",
    _scope: "exposition:demo1",
    badgeId: `badge:b${i}`,
    at: new Date(),
  }));
  state.scopedMultiCollections["+scans"] = { content: [...seededDocs] };

  const prepared = validator.prepareStateForNextMigration(
    state,
    SCOPED_SCHEMAS,
  );

  const scans = prepared.scopedMultiCollections["+scans"];
  assert(scans, "scoped collection must survive preparation");
  assert(
    scans.content.length > 0,
    "retention must keep the scoped collection non-empty",
  );
  assert(
    scans.content.length <= seededDocs.length,
    `retention must not grow a non-empty scoped collection (got ${scans.content.length} from ${seededDocs.length})`,
  );

  // Empty scoped collection declared in schema: must be mock-populated.
  const emptyState = createEmptyDatabaseState();
  const preparedEmpty = validator.prepareStateForNextMigration(
    emptyState,
    SCOPED_SCHEMAS,
  );
  const populated = preparedEmpty.scopedMultiCollections["+scans"];
  assert(populated, "declared scoped collection must be created");
  assert(
    populated.content.length > 0,
    "declared empty scoped collection must be mock-populated",
  );
  for (const doc of populated.content) {
    assertEquals(doc._type, "scan");
    assert(
      typeof doc._scope === "string" &&
        (doc._scope as string).startsWith("exposition:"),
      `generated _scope must satisfy the scope schema, got: ${doc._scope}`,
    );
  }
});

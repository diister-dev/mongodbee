/**
 * Locks the coverage of the chain-vs-project schema gate
 * (`validateLastMigrationMatchesProjectSchema`) for the buckets and facets
 * that were historically blind spots:
 *
 *  1. `scopedMultiCollections` — a scoped collection present in the project
 *     but absent from the last migration's snapshot (or with drifted
 *     scope/type schemas) MUST fail the gate with a speaking diff. This is
 *     the exact class of drift that let a real project ship a migration
 *     chain that never froze its `+scans` scoped collection, undetected.
 *  2. Index metadata (`withIndex`) — index config drives real MongoDB
 *     indexes (unique, TTL, collation, partial filters); it is symbol-keyed
 *     valibot metadata, invisible to Object.keys/JSON.stringify and
 *     stripped by schema simplification, so index drift used to pass the
 *     gate silently. It must now be compared (surfaced as `@index`).
 *  3. The simulation validator flags a scoped multi-collection that is
 *     declared in `schemas` but never created in `migrate()` (same guarantee
 *     as collections/multi-collections).
 *  4. A project schema containing ONLY scoped multi-collections is not
 *     reported as "empty".
 */
import { assert, assertEquals } from "@std/assert";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { validateLastMigrationMatchesProjectSchema } from "../../src/migration/schema-validation.ts";
import { SimulationValidator } from "../../src/migration/validators/simulation.ts";
import { withIndex } from "../../src/indexes.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const SCAN_TYPES = {
  scan: { _id: v.string(), badgeId: v.string(), at: v.date() },
};

function scopedSchemas(
  types: Record<string, Record<string, unknown>> = SCAN_TYPES,
) {
  return {
    collections: { users: { _id: v.string(), name: v.string() } },
    scopedMultiCollections: {
      "+scans": {
        scope: refId("exposition"),
        types: types as typeof SCAN_TYPES,
      },
    },
  };
}

function lastMigrationWith(schemas: ReturnType<typeof scopedSchemas>) {
  return migrationDefinition("001", "baseline", {
    parent: null,
    schemas,
    migrate: (b) => b.compile(),
  });
}

Deno.test("gate: scoped multi-collection missing in snapshot fails with a speaking error", () => {
  const lastMigration = migrationDefinition("001", "baseline-no-scans", {
    parent: null,
    schemas: {
      collections: { users: { _id: v.string(), name: v.string() } },
      // BUG on purpose: project declares "+scans" but the snapshot never froze it
    },
    migrate: (b) => b.compile(),
  });

  const result = validateLastMigrationMatchesProjectSchema(
    lastMigration,
    scopedSchemas(),
  );

  assertEquals(result.valid, false);
  assert(
    result.errors.some((e) =>
      e.includes("Scoped multi-collections missing") && e.includes("+scans")
    ),
    `expected a missing-scoped error naming "+scans", got: ${
      result.errors.join(" | ")
    }`,
  );
});

Deno.test("gate: scoped type drift fails naming the collection and the type", () => {
  const lastMigration = lastMigrationWith(scopedSchemas());

  const projectWithDriftedType = scopedSchemas({
    scan: {
      _id: v.string(),
      badgeId: v.string(),
      at: v.date(),
      // Drift: living schema gained a field the snapshot never froze
      deviceId: v.string(),
    },
  });

  const result = validateLastMigrationMatchesProjectSchema(
    lastMigration,
    projectWithDriftedType,
  );

  assertEquals(result.valid, false);
  assert(
    result.errors.some((e) =>
      e.includes('Scoped multi-collection "+scans"') && e.includes('"scan"')
    ),
    `expected a scoped type diff naming "+scans"/"scan", got: ${
      result.errors.join(" | ")
    }`,
  );
});

Deno.test("gate: scoped missing TYPE (not just field drift) is named explicitly", () => {
  const lastMigration = lastMigrationWith(scopedSchemas());

  const projectWithExtraType = scopedSchemas({
    ...SCAN_TYPES,
    checkpoint: { _id: v.string(), label: v.string() },
  });

  const result = validateLastMigrationMatchesProjectSchema(
    lastMigration,
    projectWithExtraType,
  );

  assertEquals(result.valid, false);
  assert(
    result.errors.some((e) =>
      e.includes("missing types") && e.includes("checkpoint")
    ),
    `expected a missing-types error naming "checkpoint", got: ${
      result.errors.join(" | ")
    }`,
  );
});

Deno.test("gate: identical scoped snapshot validates cleanly", () => {
  const lastMigration = lastMigrationWith(scopedSchemas());
  const result = validateLastMigrationMatchesProjectSchema(
    lastMigration,
    scopedSchemas(),
  );
  assertEquals(
    result.valid,
    true,
    `expected valid, got: ${result.errors.join(" | ")}`,
  );
});

Deno.test("gate: index metadata drift (withIndex) is detected", () => {
  const lastMigration = migrationDefinition("001", "baseline-no-index", {
    parent: null,
    schemas: {
      collections: {
        users: { _id: v.string(), email: v.string() },
      },
    },
    migrate: (b) => b.compile(),
  });

  const projectWithUniqueEmail = {
    collections: {
      users: { _id: v.string(), email: withIndex(v.string(), { unique: true }) },
    },
  };

  const result = validateLastMigrationMatchesProjectSchema(
    lastMigration,
    projectWithUniqueEmail,
  );

  assertEquals(
    result.valid,
    false,
    "a unique index present in the project but absent from the snapshot must fail the gate",
  );
  assert(
    result.errors.some((e) => e.includes('"users"')),
    `expected the diff to name the "users" collection, got: ${
      result.errors.join(" | ")
    }`,
  );
});

Deno.test("gate: identical index metadata on both sides validates cleanly", () => {
  const schemas = () => ({
    collections: {
      users: {
        _id: v.string(),
        email: withIndex(v.string(), { unique: true, insensitive: true }),
      },
    },
  });

  const lastMigration = migrationDefinition("001", "baseline-with-index", {
    parent: null,
    schemas: schemas(),
    migrate: (b) => b.compile(),
  });

  const result = validateLastMigrationMatchesProjectSchema(
    lastMigration,
    schemas(),
  );
  assertEquals(
    result.valid,
    true,
    `expected valid, got: ${result.errors.join(" | ")}`,
  );
});

Deno.test("gate: index CONFIG drift (TTL added) is detected, not just presence", () => {
  const snapshotSchemas = {
    collections: {
      sessions: {
        _id: v.string(),
        expiresAt: withIndex(v.date(), {}),
      },
    },
  };
  const projectSchemas = {
    collections: {
      sessions: {
        _id: v.string(),
        expiresAt: withIndex(v.date(), { expireAfterSeconds: 0 }),
      },
    },
  };

  const lastMigration = migrationDefinition("001", "baseline-plain-index", {
    parent: null,
    schemas: snapshotSchemas,
    migrate: (b) => b.compile(),
  });

  const result = validateLastMigrationMatchesProjectSchema(
    lastMigration,
    projectSchemas,
  );
  assertEquals(
    result.valid,
    false,
    "a TTL added to an existing index must fail the gate",
  );
});

Deno.test("simulation: declared-but-not-created scoped multi-collection is flagged", async () => {
  const m = migrationDefinition("001", "declare-scoped-without-create", {
    parent: null,
    schemas: scopedSchemas(),
    // BUG on purpose: never calls createScopedMultiCollection("+scans"),
    // but creates the regular collection so only the scoped one is missing
    migrate: (b) => b.createCollection("users").end().compile(),
  });

  const result = await new SimulationValidator().validateMigration(m);
  assertEquals(result.success, false);
  assert(
    result.errors.some((e) =>
      e.includes('Scoped multi-collection "+scans"') &&
      e.includes("not created")
    ),
    `expected a scoped declared-but-not-created error, got: ${
      result.errors.join(" | ")
    }`,
  );
});

Deno.test("simulation: properly created scoped multi-collection validates cleanly", async () => {
  const m = migrationDefinition("001", "create-scoped-properly", {
    parent: null,
    schemas: scopedSchemas(),
    migrate: (b) =>
      b.createCollection("users").end()
        .createScopedMultiCollection("+scans").end()
        .compile(),
  });

  const result = await new SimulationValidator().validateMigration(m);
  assertEquals(
    result.success,
    true,
    `expected clean validation, got: ${result.errors.join(" | ")}`,
  );
});

/**
 * Locks the schema-consistency guarantees of the simulation validator:
 *
 *  1. A collection declared in `schemas` but never created in `migrate()`
 *     is reported as an error.
 *  2. A migration whose operations do not produce the declared shape
 *     (a required field is declared but no transform adds it) fails when
 *     the mock documents are validated against the declared schema.
 *  3. A consistent migration validates cleanly.
 *
 * These guard against silent schema drift between the declarative `schemas`
 * field and the imperative `migrate()` body.
 */
import { assert, assertEquals } from "@std/assert";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { SimulationValidator } from "../../src/migration/validators/simulation.ts";
import * as v from "../../src/schema.ts";

Deno.test("schema consistency: declared-but-not-created collection is flagged", async () => {
  const m = migrationDefinition("001", "declare-without-create", {
    parent: null,
    schemas: {
      collections: {
        users: { _id: v.string(), name: v.string() },
      },
    },
    // BUG on purpose: never calls createCollection("users")
    migrate: (b) => b.compile(),
  });

  const result = await new SimulationValidator().validateMigration(m);
  assertEquals(result.success, false);
  assert(
    result.errors.some((e) => e.includes("users") && e.includes("not created")),
    `expected a "declared but not created" error, got: ${result.errors.join(" | ")}`,
  );
});

Deno.test("schema consistency: missing required field after migration is flagged", async () => {
  const parent = migrationDefinition("001", "init", {
    parent: null,
    schemas: {
      collections: { users: { _id: v.string(), name: v.string() } },
    },
    migrate: (b) => b.createCollection("users").end().compile(),
  });

  // Declares a REQUIRED `age` field but the migrate() body never adds it.
  const child = migrationDefinition("002", "add-age-declared-only", {
    parent,
    schemas: {
      collections: {
        users: { _id: v.string(), name: v.string(), age: v.number() },
      },
    },
    migrate: (b) => b.compile(), // no transform adding `age`
  });

  const result = await new SimulationValidator().validateMigration(child);
  assertEquals(
    result.success,
    false,
    `expected validation to fail; errors: ${result.errors.join(" | ")}`,
  );
});

Deno.test("schema consistency: a consistent migration validates cleanly", async () => {
  const parent = migrationDefinition("001", "init", {
    parent: null,
    schemas: {
      collections: { users: { _id: v.string(), name: v.string() } },
    },
    migrate: (b) => b.createCollection("users").end().compile(),
  });

  const child = migrationDefinition("002", "add-age-properly", {
    parent,
    schemas: {
      collections: {
        users: { _id: v.string(), name: v.string(), age: v.number() },
      },
    },
    migrate: (b) =>
      b.collection("users")
        .transform({
          up: (doc) => ({ ...doc, age: 0 }),
          down: (doc) => {
            const { age: _age, ...rest } = doc as Record<string, unknown>;
            return rest;
          },
        })
        .end()
        .compile(),
  });

  const result = await new SimulationValidator().validateMigration(child);
  assertEquals(
    result.success,
    true,
    `expected clean validation; errors: ${result.errors.join(" | ")}`,
  );
});

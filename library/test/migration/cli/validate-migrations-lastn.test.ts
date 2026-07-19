/**
 * Regression: `--last N` validation must NOT report skipped migrations as
 * valid.
 *
 * The fast-forward loop runs the full simulation for every skipped migration
 * (it needs `stateAfterMigration` to propagate state) but previously DISCARDED
 * the verdict and hardcoded `valid: true` — and its `catch` swallowed thrown
 * errors the same way. So a broken migration sitting in the skipped range
 * produced a false-green "✓ All migrations are valid and ready to apply!".
 *
 * Worse: `migrate --last N` validates only the last N but then applies ALL
 * pending migrations (cli/commands/migrate.ts), so a broken migration in the
 * skipped range would pass the safety gate (no throw) and get applied to a
 * real database under a green banner.
 *
 * Note on test construction: the validated migration is an INDEPENDENT root
 * whose schema does not mention the broken collection. That isolates the
 * skip-loop verdict from a separate confound — `validateMigration` mutates the
 * shared simulation state even on failure, so if the validated migration's
 * schema referenced the broken collection it would fail on the leaked state
 * regardless of the skip-loop verdict, masking the bug.
 */
import { assertRejects } from "@std/assert";
import { migrationDefinition } from "../../../src/migration/definition.ts";
import { validateMigrationsWithSimulation } from "../../../src/migration/cli/utils/validate-migrations.ts";
import * as v from "../../../src/schema.ts";

// A self-contained INVALID migration: declares `x` as a required number but
// seeds a document whose `x` is a string → simulation reports success:false.
function brokenMigration() {
  return migrationDefinition(
    "2025_01_01_0000_AAAAAAAAAAAAAAAAAAAAAAAAAA@broken",
    "broken",
    {
      parent: null,
      schemas: {
        collections: { a: { _id: v.string(), x: v.number() } },
        multiModels: {},
      },
      migrate(m) {
        m.createCollection("a");
        m.collection("a").seed([{ _id: "1", x: "not-a-number" }] as never);
        return m.compile();
      },
    },
  );
}

// A trivially valid migration on its OWN collection (independent of `a`).
function validMigration() {
  return migrationDefinition(
    "2025_01_02_0000_BBBBBBBBBBBBBBBBBBBBBBBBBB@valid",
    "valid",
    {
      parent: null,
      schemas: { collections: { b: { _id: v.string() } }, multiModels: {} },
      migrate(m) {
        m.createCollection("b");
        return m.compile();
      },
    },
  );
}

Deno.test("validate --last N: a broken migration in the SKIPPED range is not hidden as valid", async () => {
  const broken = brokenMigration();
  const valid = validMigration();
  // lastN=1 → only [valid] is deeply validated, [broken] is "skipped".
  // The broken migration's failing verdict must NOT be swallowed.
  await assertRejects(
    () => validateMigrationsWithSimulation([broken, valid], { lastN: 1 }),
    Error,
    "Migration validation failed",
  );
});

Deno.test("validate --last N: a fully valid chain still passes (no false negative)", async () => {
  const m1 = migrationDefinition(
    "2025_01_01_0000_CCCCCCCCCCCCCCCCCCCCCCCCCC@ok_root",
    "ok_root",
    {
      parent: null,
      schemas: { collections: { a: { _id: v.string() } }, multiModels: {} },
      migrate(m) {
        m.createCollection("a");
        return m.compile();
      },
    },
  );
  const m2 = migrationDefinition(
    "2025_01_02_0000_DDDDDDDDDDDDDDDDDDDDDDDDDD@ok_child",
    "ok_child",
    {
      parent: m1,
      schemas: {
        collections: { a: { _id: v.string() }, b: { _id: v.string() } },
        multiModels: {},
      },
      migrate(m) {
        m.createCollection("b");
        return m.compile();
      },
    },
  );
  // Should resolve without throwing.
  await validateMigrationsWithSimulation([m1, m2], { lastN: 1 });
});

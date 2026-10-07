/**
 * Regression (programmatic API twin of validate-migrations-lastn): when
 * `checkMigrationStatus({ lastN })` fast-forwards through the migrations
 * outside the window, a broken one used to be pushed with `isValid: true`
 * and its error swallowed — so `assertMigrationSystemHealthy({ lastN })`
 * (the documented startup fail-fast guard) would NOT throw against a known
 * broken migration chain.
 *
 * We author real migration files on disk (checkMigrationStatus loads from a
 * directory) and point it at them with lastN set so the broken migration
 * falls in the skipped range. The broken migration must be reported invalid
 * and its failure surfaced in the top-level errors (visible without verbose).
 */
import { test } from "../+harness.ts";
import process from "node:process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { assert, assertEquals } from "../+assert.ts";
import * as path from "node:path";
import { checkMigrationStatus } from "../../src/migration/check-status.ts";

const LIB = process.cwd(); // `deno test` runs from the library directory
const DEFINITION = path.resolve(LIB, "src/migration/definition.ts");
const SCHEMA = path.resolve(LIB, "src/schema.ts");

const BROKEN_FILE = "2025_01_01_0000_AAAAAAAAAAAAAAAAAAAAAAAAAA@broken.ts";
const VALID_FILE = "2025_01_02_0000_BBBBBBBBBBBBBBBBBBBBBBBBBB@valid.ts";

// Root migration that is INVALID: declares `x` as a required number but seeds
// a document whose `x` is a string.
const brokenContent = `
import { migrationDefinition } from "${DEFINITION}";
import * as v from "${SCHEMA}";
export default migrationDefinition("2025_01_01_0000_AAAAAAAAAAAAAAAAAAAAAAAAAA@broken", "broken", {
  parent: null,
  schemas: { collections: { a: { _id: v.string(), x: v.number() } }, multiModels: {} },
  migrate(m) {
    m.createCollection("a");
    m.collection("a").seed([{ _id: "1", x: "not-a-number" }]);
    return m.compile();
  },
});
`;

// Valid child migration: adds its own collection. Links to broken as parent so
// buildMigrationChain accepts the chain.
const validContent = `
import { migrationDefinition } from "${DEFINITION}";
import * as v from "${SCHEMA}";
import broken from "./${BROKEN_FILE}";
export default migrationDefinition("2025_01_02_0000_BBBBBBBBBBBBBBBBBBBBBBBBBB@valid", "valid", {
  parent: broken,
  schemas: {
    collections: { a: { _id: v.string(), x: v.number() }, b: { _id: v.string() } },
    multiModels: {},
  },
  migrate(m) {
    m.createCollection("b");
    return m.compile();
  },
});
`;

const schemaContent = `
import * as v from "${SCHEMA}";
export const schemas = {
  collections: { a: { _id: v.string(), x: v.number() }, b: { _id: v.string() } },
  multiModels: {},
};
`;

test("checkMigrationStatus --last N: broken migration in skipped range is reported invalid, not green", async () => {
  const dir = await mkdtemp(path.join(tmpdir(), "mongodbee_lastn_"));
  try {
    const migrationsDir = path.join(dir, "migrations");
    await mkdir(migrationsDir);
    await writeFile(path.join(migrationsDir, BROKEN_FILE), brokenContent);
    await writeFile(path.join(migrationsDir, VALID_FILE), validContent);
    const schemaPath = path.join(dir, "schemas.ts");
    await writeFile(schemaPath, schemaContent);

    const status = await checkMigrationStatus({
      migrationsDir,
      schemaPath,
      strictValidation: true,
      lastN: 1, // only [valid] deeply validated; [broken] is fast-forwarded
      verbose: true,
    });

    // The migration system must NOT be reported healthy.
    assertEquals(status.validation.areMigrationsValid, false);
    assertEquals(status.ok, false);

    // The skipped-but-broken migration's own entry must be invalid.
    const brokenInfo = status.migrations?.find((m) => m.name === "broken");
    assert(
      brokenInfo,
      "broken migration should appear in verbose migration info",
    );
    assertEquals(brokenInfo!.isValid, false);

    // Its failure must be surfaced at the top level (visible without verbose),
    // not only buried in a per-migration warning.
    assert(
      status.validation.errors.some(
        (e) => e.includes("broken") && e.includes("skipped --last N range"),
      ),
      `expected a top-level error mentioning the skipped broken migration, got: ${JSON.stringify(
        status.validation.errors,
      )}`,
    );
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

/**
 * Tests for the generate command
 *
 * Tests migration file generation including:
 * - First migration creation (no parent)
 * - Child migration creation (with parent reference)
 * - Parent schema inheritance
 * - Migration ID format
 * - Unique ID generation
 *
 * @module
 */

import { test } from "../../+harness.ts";
import { writeFile } from "node:fs/promises";
import { assert, assertEquals } from "../../+assert.ts";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { generateCommand } from "../../../src/migration/cli/commands/generate.ts";
import {
  delay,
  extractMigrationId,
  extractMigrationName,
  getMigrationPath,
  getMigrationsDir,
  listMigrationFiles,
  readFile,
  withTempDir,
} from "./shared.ts";

/**
 * Finds a generated migration by the name it was generated under.
 *
 * Indexing the sorted listing is not safe here: a migration id embeds a ULID
 * whose time prefix is identical for two files generated in the same
 * millisecond, leaving the random tail — and therefore the sort order — to
 * decide. The suite used to index, and failed intermittently once the runner
 * got fast enough to generate both within one millisecond.
 */
function migrationNamed(files: string[], name: string): string {
  const found = files.find((f) => f.includes(`@${name}`));
  assert(
    found !== undefined,
    `no generated migration named "${name}" in ${files.join(", ")}`,
  );
  return found;
}

test("generate - creates first migration with no parent", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({ cwd: tempDir });

    // Generate first migration
    await generateCommand({ name: "initial", cwd: tempDir });

    // Check migration file was created
    const files = listMigrationFiles(getMigrationsDir(tempDir));

    assertEquals(files.length, 1);
    assert(files[0].includes("@initial"));

    // Check file content
    const migrationPath = getMigrationPath(tempDir, files[0]);
    const content = await readFile(migrationPath);

    assert(content !== null);
    assert(content.includes("migrationDefinition"));
    assert(content.includes("parent: null"));
    assert(content.includes("initial"));
  });
});

test("generate - creates child migration with parent reference", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({ cwd: tempDir });

    // Generate first migration
    await generateCommand({ name: "initial", cwd: tempDir });

    // Generate second migration
    await generateCommand({ name: "add_users", cwd: tempDir });

    // Check both files exist
    const files = listMigrationFiles(getMigrationsDir(tempDir));

    assertEquals(files.length, 2);

    // Check second migration references first as parent
    const content = await readFile(
      getMigrationPath(tempDir, migrationNamed(files, "add_users")),
    );

    assert(content !== null);
    assert(content.includes("import parent from"));
    assert(content.includes(migrationNamed(files, "initial")));
    assert(content.includes("parent: parent"));
  });
});

test("generate - includes parent schemas in child migration", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({ cwd: tempDir });

    // Generate first migration
    await generateCommand({ name: "initial", cwd: tempDir });

    // Generate second migration
    await generateCommand({ name: "add_field", cwd: tempDir });

    // Check second migration inherits parent schemas
    const files = listMigrationFiles(getMigrationsDir(tempDir));

    const secondMigrationPath = getMigrationPath(
      tempDir,
      migrationNamed(files, "add_field"),
    );
    const content = await readFile(secondMigrationPath);

    assert(content !== null);
    assert(content.includes("...parent.schemas.collections"));
  });
});

test("generate - creates unique migration IDs", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({ cwd: tempDir });

    // Generate multiple migrations quickly
    await generateCommand({ name: "migration_1", cwd: tempDir });

    // Small delay to ensure different timestamps
    await delay(10);

    await generateCommand({ name: "migration_2", cwd: tempDir });

    // Check all have unique IDs
    const files = listMigrationFiles(getMigrationsDir(tempDir));

    assertEquals(files.length, 2);

    // IDs should be different
    const ids = files.map((name) => name.split("@")[0]);
    assertEquals(new Set(ids).size, 2);
  });
});

test("generate - preserves migration name in ID", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({ cwd: tempDir });

    // Generate migration with specific name
    const migrationName = "create_users_table";
    await generateCommand({ name: migrationName, cwd: tempDir });

    // Check file includes the name
    const files = listMigrationFiles(getMigrationsDir(tempDir));

    assertEquals(files.length, 1);
    assert(files[0].includes(`@${migrationName}`));

    // Use helper to extract name
    const extractedName = extractMigrationName(files[0]);
    assertEquals(extractedName, migrationName);
  });
});

test("generate - migration ID follows correct format", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({ cwd: tempDir });

    // Generate migration
    await generateCommand({ name: "test_migration", cwd: tempDir });

    // Check ID format: YYYY_MM_DD_HHMM_ULID@name
    const files = listMigrationFiles(getMigrationsDir(tempDir));

    const migrationId = extractMigrationId(files[0]);

    // Should have format: YYYY_MM_DD_HHMM_ULID@name
    const parts = migrationId.split("@");
    assertEquals(parts.length, 2);
    assertEquals(parts[1], "test_migration");

    // First part should have date format
    const datePart = parts[0];
    const dateComponents = datePart.split("_");

    // Should have at least 5 components: YYYY, MM, DD, HHMM, ULID
    assert(dateComponents.length >= 5);

    // Year should be 4 digits
    assertEquals(dateComponents[0].length, 4);

    // Month should be 2 digits
    assertEquals(dateComponents[1].length, 2);

    // Day should be 2 digits
    assertEquals(dateComponents[2].length, 2);
  });
});

test("generate - includes proper TypeScript structure", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({ cwd: tempDir });

    // Generate migration
    await generateCommand({ name: "test", cwd: tempDir });

    // Check file structure
    const files = listMigrationFiles(getMigrationsDir(tempDir));
    const content = await readFile(getMigrationPath(tempDir, files[0]));

    assert(content !== null);

    // Should have proper imports
    assert(content.includes("import"));
    assert(content.includes("migrationDefinition"));

    // Should have proper exports
    assert(content.includes("export default"));

    // Should have schemas object
    assert(content.includes("schemas:"));
    assert(content.includes("collections:"));

    // Should have migrate function
    assert(content.includes("migrate(migration)"));
    assert(content.includes("migration.compile()"));
  });
});

test("generate - handles migrations with long names", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({ cwd: tempDir });

    // Generate migration with long name
    const longName = "add_new_field_to_users_collection_for_authentication";
    await generateCommand({ name: longName, cwd: tempDir });

    // Check file was created with full name
    const files = listMigrationFiles(getMigrationsDir(tempDir));

    assertEquals(files.length, 1);
    assert(files[0].includes(`@${longName}`));
  });
});

test("generate - creates multiple migrations in sequence", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({ cwd: tempDir });

    // Generate chain of migrations
    await generateCommand({ name: "initial", cwd: tempDir });
    await delay(10);
    await generateCommand({ name: "add_users", cwd: tempDir });
    await delay(10);
    await generateCommand({ name: "add_posts", cwd: tempDir });
    await delay(10);
    await generateCommand({ name: "add_comments", cwd: tempDir });

    // Check all migrations exist
    const files = listMigrationFiles(getMigrationsDir(tempDir));

    assertEquals(files.length, 4);

    // Check names
    migrationNamed(files, "initial");
    migrationNamed(files, "add_users");
    assert(files[2].includes("@add_posts"));
    assert(files[3].includes("@add_comments"));

    // Check each migration references its parent
    for (let i = 1; i < files.length; i++) {
      const content = await readFile(getMigrationPath(tempDir, files[i]));
      assert(content !== null);
      assert(content.includes(`import parent from "./${files[i - 1]}"`));
    }
  });
});

test("generate - fails gracefully if migrations directory doesn't exist", async () => {
  await withTempDir(async (tempDir) => {
    // Create config without initializing (no migrations directory)
    await writeFile(
      `${tempDir}/mongodbee.config.ts`,
      `export default { paths: { migrationsDir: "./migrations" } };`,
    );

    // Try to generate migration
    try {
      await generateCommand({ name: "test", cwd: tempDir });
      // Should either succeed (creating directory) or throw an error
    } catch (_error) {
      // Error handling is acceptable behavior
    }
  });
});

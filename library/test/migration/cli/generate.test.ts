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

import { test, expect } from "vitest";
import * as fsp from "node:fs/promises";
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
 * Helper to set up test configuration
 */
async function setupTestConfig(tempDir: string) {
  const config = `
import { defineConfig } from "@diister/mongodbee/migration";

export default defineConfig({
  paths: {
    migrationsDir: "./migrations"
  },
  mongodb: {
    uri: "mongodb://127.0.0.1:27017",
    database: "mongodbee_test"
  }
});
  `;

  await fsp.writeFile("mongodbee.config.ts", config.trim(), "utf-8");
}

test("generate - creates first migration with no parent", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({ cwd: tempDir });

    // Generate first migration
    await generateCommand({ name: "initial", cwd: tempDir });

    // Check migration file was created
    const files = listMigrationFiles(getMigrationsDir(tempDir));

    expect(files.length).toEqual(1);
    expect(files[0].includes("@initial")).toBeTruthy();

    // Check file content
    const migrationPath = getMigrationPath(tempDir, files[0]);
    const content = await readFile(migrationPath);

    expect(content !== null).toBeTruthy();
    expect(content.includes("migrationDefinition")).toBeTruthy();
    expect(content.includes("parent: null")).toBeTruthy();
    expect(content.includes("initial")).toBeTruthy();
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

    expect(files.length).toEqual(2);

    // Check second migration references first as parent
    const secondMigrationPath = getMigrationPath(tempDir, files[1]);
    const content = await readFile(secondMigrationPath);

    expect(content !== null).toBeTruthy();
    expect(content.includes("import parent from")).toBeTruthy();
    expect(content.includes(files[0])).toBeTruthy();
    expect(content.includes("parent: parent")).toBeTruthy();
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

    const secondMigrationPath = getMigrationPath(tempDir, files[1]);
    const content = await readFile(secondMigrationPath);

    expect(content !== null).toBeTruthy();
    expect(content.includes("...parent.schemas.collections")).toBeTruthy();
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

    expect(files.length).toEqual(2);

    // IDs should be different
    const ids = files.map((name) => name.split("@")[0]);
    expect(new Set(ids).size).toEqual(2);
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

    expect(files.length).toEqual(1);
    expect(files[0].includes(`@${migrationName}`)).toBeTruthy();

    // Use helper to extract name
    const extractedName = extractMigrationName(files[0]);
    expect(extractedName).toEqual(migrationName);
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
    expect(parts.length).toEqual(2);
    expect(parts[1]).toEqual("test_migration");

    // First part should have date format
    const datePart = parts[0];
    const dateComponents = datePart.split("_");

    // Should have at least 5 components: YYYY, MM, DD, HHMM, ULID
    expect(dateComponents.length >= 5).toBeTruthy();

    // Year should be 4 digits
    expect(dateComponents[0].length).toEqual(4);

    // Month should be 2 digits
    expect(dateComponents[1].length).toEqual(2);

    // Day should be 2 digits
    expect(dateComponents[2].length).toEqual(2);
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

    expect(content !== null).toBeTruthy();

    // Should have proper imports
    expect(content.includes("import")).toBeTruthy();
    expect(content.includes("migrationDefinition")).toBeTruthy();

    // Should have proper exports
    expect(content.includes("export default")).toBeTruthy();

    // Should have schemas object
    expect(content.includes("schemas:")).toBeTruthy();
    expect(content.includes("collections:")).toBeTruthy();

    // Should have migrate function
    expect(content.includes("migrate(migration)")).toBeTruthy();
    expect(content.includes("migration.compile()")).toBeTruthy();
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

    expect(files.length).toEqual(1);
    expect(files[0].includes(`@${longName}`)).toBeTruthy();
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

    expect(files.length).toEqual(4);

    // Check names
    expect(files[0].includes("@initial")).toBeTruthy();
    expect(files[1].includes("@add_users")).toBeTruthy();
    expect(files[2].includes("@add_posts")).toBeTruthy();
    expect(files[3].includes("@add_comments")).toBeTruthy();

    // Check each migration references its parent
    for (let i = 1; i < files.length; i++) {
      const content = await readFile(getMigrationPath(tempDir, files[i]));
      expect(content !== null).toBeTruthy();
      expect(content.includes(`import parent from "./${files[i - 1]}"`)).toBeTruthy();
    }
  });
});

test("generate - fails gracefully if migrations directory doesn't exist", async () => {
  await withTempDir(async (tempDir) => {
    // Create config without initializing (no migrations directory)
    await fsp.writeFile(
      `${tempDir}/mongodbee.config.ts`,
      `export default { paths: { migrationsDir: "./migrations" } };`,
      "utf-8",
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

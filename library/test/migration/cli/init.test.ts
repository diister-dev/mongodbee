/**
 * Tests for the init command
 *
 * Tests initialization of MongoDBee migration system including:
 * - Config file creation
 * - Schema file creation
 * - Migrations directory creation
 * - Force flag behavior
 *
 * @module
 */

import { assert, assertEquals, assertExists } from "@std/assert";
import * as path from "@std/path";
import { existsSync } from "@std/fs";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { fileContains, withTempDir } from "./shared.ts";

Deno.test("init - creates config file and migrations directory", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    // Check config file was created
    const configPath = path.join(tempDir, "mongodbee.config.ts");
    assertExists(existsSync(configPath));
    assert(await fileContains(configPath, "defineConfig"));
    assert(await fileContains(configPath, "database"));

    // Check schemas file was created
    const schemasPath = path.join(tempDir, "schemas.ts");
    assertExists(existsSync(schemasPath));
    assert(await fileContains(schemasPath, "export const schemas"));

    // Check migrations directory was created
    const migrationsDir = path.join(tempDir, "migrations");
    assertExists(existsSync(migrationsDir));
  });
});

Deno.test("init - respects force flag to overwrite existing config", async () => {
  await withTempDir(async (tempDir) => {
    // First init
    await initCommand({ cwd: tempDir });

    // Modify config file
    const configPath = path.join(tempDir, "mongodbee.config.ts");
    await Deno.writeTextFile(configPath, "// Modified content");

    // Init without force should not overwrite
    await initCommand({ cwd: tempDir });
    assert(await fileContains(configPath, "// Modified content"));

    // Init with force should overwrite
    await initCommand({ force: true, cwd: tempDir });
    assert(await fileContains(configPath, "defineConfig"));
    assert(await fileContains(configPath, "database"));
    assert(!await fileContains(configPath, "// Modified content"));
  });
});

Deno.test("init - creates config with correct structure", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    const configPath = path.join(tempDir, "mongodbee.config.ts");
    const content = await Deno.readTextFile(configPath);

    // Check for essential config sections
    assert(content.includes("import"));
    assert(content.includes("defineConfig"));
    assert(content.includes("database"));
    assert(content.includes("connection"));
    assert(content.includes("uri:"));
    assert(content.includes("name:"));
    assert(content.includes("paths"));
    assert(content.includes("migrations:"));
  });
});

Deno.test("init - creates schemas file with correct structure", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    const schemasPath = path.join(tempDir, "schemas.ts");
    const content = await Deno.readTextFile(schemasPath);

    // Check for essential schema structure
    assert(content.includes("export const schemas"));
    assert(content.includes("collections:"));
  });
});

Deno.test("init - does not overwrite existing files without force", async () => {
  await withTempDir(async (tempDir) => {
    // Create custom config
    const configPath = path.join(tempDir, "mongodbee.config.ts");
    const customContent = "// Custom configuration";
    await Deno.writeTextFile(configPath, customContent);

    // Run init without force
    await initCommand({ cwd: tempDir });

    // File should still have custom content
    const content = await Deno.readTextFile(configPath);
    assertEquals(content, customContent);
  });
});

Deno.test("init - creates empty migrations directory", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    const migrationsDir = path.join(tempDir, "migrations");
    const files = [...Deno.readDirSync(migrationsDir)];

    // Directory should be empty initially
    assertEquals(files.length, 0);
  });
});

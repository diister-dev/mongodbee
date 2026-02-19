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

import { test, expect } from "vitest";
import * as path from "node:path";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import { existsSync } from "node:fs";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { fileContains, withTempDir } from "./shared.ts";

test("init - creates config file and migrations directory", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    // Check config file was created
    const configPath = path.join(tempDir, "mongodbee.config.ts");
    expect(existsSync(configPath)).toBeDefined();
    expect(await fileContains(configPath, "defineConfig")).toBeTruthy();
    expect(await fileContains(configPath, "database")).toBeTruthy();

    // Check schemas file was created
    const schemasPath = path.join(tempDir, "schemas.ts");
    expect(existsSync(schemasPath)).toBeDefined();
    expect(await fileContains(schemasPath, "export const schemas")).toBeTruthy();

    // Check migrations directory was created
    const migrationsDir = path.join(tempDir, "migrations");
    expect(existsSync(migrationsDir)).toBeDefined();
  });
});

test("init - respects force flag to overwrite existing config", async () => {
  await withTempDir(async (tempDir) => {
    // First init
    await initCommand({ cwd: tempDir });

    // Modify config file
    const configPath = path.join(tempDir, "mongodbee.config.ts");
    await fsp.writeFile(configPath, "// Modified content", "utf-8");

    // Init without force should not overwrite
    await initCommand({ cwd: tempDir });
    expect(await fileContains(configPath, "// Modified content")).toBeTruthy();

    // Init with force should overwrite
    await initCommand({ force: true, cwd: tempDir });
    expect(await fileContains(configPath, "defineConfig")).toBeTruthy();
    expect(await fileContains(configPath, "database")).toBeTruthy();
    expect(!await fileContains(configPath, "// Modified content")).toBeTruthy();
  });
});

test("init - creates config with correct structure", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    const configPath = path.join(tempDir, "mongodbee.config.ts");
    const content = await fsp.readFile(configPath, "utf-8");

    // Check for essential config sections
    expect(content.includes("import")).toBeTruthy();
    expect(content.includes("defineConfig")).toBeTruthy();
    expect(content.includes("database")).toBeTruthy();
    expect(content.includes("connection")).toBeTruthy();
    expect(content.includes("uri:")).toBeTruthy();
    expect(content.includes("name:")).toBeTruthy();
    expect(content.includes("paths")).toBeTruthy();
    expect(content.includes("migrations:")).toBeTruthy();
  });
});

test("init - creates schemas file with correct structure", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    const schemasPath = path.join(tempDir, "schemas.ts");
    const content = await fsp.readFile(schemasPath, "utf-8");

    // Check for essential schema structure
    expect(content.includes("export const schemas")).toBeTruthy();
    expect(content.includes("collections:")).toBeTruthy();
  });
});

test("init - does not overwrite existing files without force", async () => {
  await withTempDir(async (tempDir) => {
    // Create custom config
    const configPath = path.join(tempDir, "mongodbee.config.ts");
    const customContent = "// Custom configuration";
    await fsp.writeFile(configPath, customContent, "utf-8");

    // Run init without force
    await initCommand({ cwd: tempDir });

    // File should still have custom content
    const content = await fsp.readFile(configPath, "utf-8");
    expect(content).toEqual(customContent);
  });
});

test("init - creates empty migrations directory", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    const migrationsDir = path.join(tempDir, "migrations");
    const files = fs.readdirSync(migrationsDir, { withFileTypes: true });

    // Directory should be empty initially
    expect(files.length).toEqual(0);
  });
});

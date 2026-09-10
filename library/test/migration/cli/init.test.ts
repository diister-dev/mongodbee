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

import { test } from "../../+harness.ts";
import { readFile, writeFile } from "node:fs/promises";
import { assert, assertEquals, assertExists } from "../../+assert.ts";
import * as path from "node:path";
import { existsSync, readdirSync } from "node:fs";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { fileURLToPath } from "node:url";
import { fileContains, runScript, withTempDir } from "./shared.ts";

test("init - creates config file and migrations directory", async () => {
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

test("init - respects force flag to overwrite existing config", async () => {
  await withTempDir(async (tempDir) => {
    // First init
    await initCommand({ cwd: tempDir });

    // Modify config file
    const configPath = path.join(tempDir, "mongodbee.config.ts");
    await writeFile(configPath, "// Modified content");

    // Init without force should not overwrite
    await initCommand({ cwd: tempDir });
    assert(await fileContains(configPath, "// Modified content"));

    // Init with force should overwrite
    await initCommand({ force: true, cwd: tempDir });
    assert(await fileContains(configPath, "defineConfig"));
    assert(await fileContains(configPath, "database"));
    assert(!(await fileContains(configPath, "// Modified content")));
  });
});

test("init - creates config with correct structure", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    const configPath = path.join(tempDir, "mongodbee.config.ts");
    const content = await readFile(configPath, "utf8");

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

test("init - creates schemas file with correct structure", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    const schemasPath = path.join(tempDir, "schemas.ts");
    const content = await readFile(schemasPath, "utf8");

    // Check for essential schema structure
    assert(content.includes("export const schemas"));
    assert(content.includes("collections:"));
  });
});

test("init - does not overwrite existing files without force", async () => {
  await withTempDir(async (tempDir) => {
    // Create custom config
    const configPath = path.join(tempDir, "mongodbee.config.ts");
    const customContent = "// Custom configuration";
    await writeFile(configPath, customContent);

    // Run init without force
    await initCommand({ cwd: tempDir });

    // File should still have custom content
    const content = await readFile(configPath, "utf8");
    assertEquals(content, customContent);
  });
});

test("init - creates empty migrations directory", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({ cwd: tempDir });

    const migrationsDir = path.join(tempDir, "migrations");
    const files = [...readdirSync(migrationsDir, { withFileTypes: true })];

    // Directory should be empty initially
    assertEquals(files.length, 0);
  });
});

/**
 * The scaffold is the first TypeScript a new user ever sees, so it has to
 * compile. The assertions above only checked that the files exist and contain
 * a few substrings, which is how `schemas.ts` came to import
 * `SchemasDefinition` from the package root — a subpath that does not export
 * it — and fail to typecheck for everybody running `mongodbee init`.
 *
 * `paths` maps the package specifiers onto this repo's sources so the check
 * needs no packing or install, and `typeRoots` points at the suite's own
 * `@types` because the temp project has no `node_modules`.
 */
test({
  name: "init - the files it scaffolds typecheck against the package",
  // Spawning tsc over the package's own sources takes ~0.6 s warm and runs
  // past the 5 s default on a cold CI runner.
  timeout: 120_000,
  fn: async () => {
    await withTempDir(async (tempDir) => {
      await initCommand({ cwd: tempDir });

      const libRoot = fileURLToPath(new URL("../../../", import.meta.url));
      const tsconfig = {
        compilerOptions: {
          target: "esnext",
          module: "nodenext",
          moduleResolution: "nodenext",
          lib: ["esnext", "dom"],
          strict: true,
          noEmit: true,
          skipLibCheck: true,
          typeRoots: [path.join(libRoot, "node_modules/@types")],
          types: ["node"],
          allowImportingTsExtensions: true,
          baseUrl: ".",
          paths: {
            "@diister/mongodbee": [path.join(libRoot, "mod.ts")],
            "@diister/mongodbee/*": [path.join(libRoot, "src/*/mod.ts")],
          },
        },
        include: ["schemas.ts", "mongodbee.config.ts"],
      };
      await writeFile(
        path.join(tempDir, "tsconfig.json"),
        JSON.stringify(tsconfig, null, 2),
        "utf-8",
      );

      const tsc = path.join(libRoot, "node_modules/typescript/bin/tsc");
      const { code, stdout } = await runScript(
        tsc,
        ["-p", "tsconfig.json"],
        tempDir,
      );
      assertEquals(code, 0, `scaffolded files do not typecheck:\n${stdout}`);
    });
  },
});

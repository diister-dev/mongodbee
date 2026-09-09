/**
 * Shared test utilities for CLI command tests
 *
 * Provides common helpers like temporary directory management,
 * file operations, and test setup utilities.
 *
 * @module
 */

import {
  mkdir,
  mkdtemp,
  readFile as readFileRaw,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { existsSync, readdirSync } from "node:fs";
import { spawn } from "node:child_process";
import type { Readable } from "node:stream";
import { Buffer } from "node:buffer";
import process from "node:process";
import { fileURLToPath } from "node:url";
import * as path from "node:path";

/** Absolute path to the library root, i.e. the package under test. */
const LIBRARY_ROOT = fileURLToPath(new URL("../../../", import.meta.url));

/**
 * Makes a scratch project able to resolve `@diister/mongodbee`, as a real one can.
 *
 * The CLI generates migration and config files that import the package by
 * name, and those files are then loaded by a dynamic `import()`. Under Deno
 * that resolved for free — an import map is global to the program, so the
 * library's own map covered files anywhere on disk. Node and Bun resolve bare
 * specifiers by walking up from the importing FILE, so a generated migration
 * sitting in a temp directory resolves nothing at all.
 *
 * The stub files are `.js`, not `.ts`: Node and Deno both refuse to strip types
 * from anything under `node_modules` (`ERR_UNSUPPORTED_NODE_MODULES_TYPE_
 * STRIPPING`), which Bun tolerates — so a `.ts` stub passed under `bun test`
 * and failed the moment the same suite ran on the other two. A plain `.js`
 * re-export of the sources, which live outside `node_modules`, works on all
 * three without needing `dist/` to be built first.
 */
export async function installLibrary(projectDir: string): Promise<void> {
  const stub = path.join(projectDir, "node_modules", "@diister", "mongodbee");
  await mkdir(stub, { recursive: true });

  // A package.json makes the scratch directory look like a real project, so
  // node resolution finds the stub the way it would find an install.
  await writeFile(
    path.join(projectDir, "package.json"),
    JSON.stringify({ name: "scratch", type: "module", private: true }, null, 2),
  );

  const entries: Record<string, string> = {
    "index.js": "mod.ts",
    "schema.js": "schema.ts",
    "session.js": "session.ts",
    "telemetry.js": "telemetry.ts",
    "migration.js": "src/migration/mod.ts",
    "types.js": "src/types.ts",
    "ids.js": "src/ids.ts",
    "indexes.js": "src/indexes.ts",
    "schema-navigator.js": "src/schema-navigator.ts",
  };

  for (const [file, target] of Object.entries(entries)) {
    await writeFile(
      path.join(stub, file),
      `export * from ${JSON.stringify(path.join(LIBRARY_ROOT, target))};\n`,
    );
  }

  await writeFile(
    path.join(stub, "package.json"),
    JSON.stringify(
      {
        name: "@diister/mongodbee",
        version: "0.0.0-test",
        type: "module",
        exports: {
          ".": "./index.js",
          "./schema": "./schema.js",
          "./session": "./session.js",
          "./telemetry": "./telemetry.js",
          "./migration": "./migration.js",
          "./types": "./types.js",
          "./ids": "./ids.js",
          "./indexes": "./indexes.js",
          "./schema-navigator": "./schema-navigator.js",
        },
      },
      null,
      2,
    ),
  );

  // The generated schema and migration files import the runtime dependencies
  // directly, and those resolve from the generated file's own directory too.
  for (const dep of ["valibot", "mongodb", "@opentelemetry"]) {
    const target = path.join(LIBRARY_ROOT, "node_modules", dep);
    if (!existsSync(target)) continue;
    const link = path.join(projectDir, "node_modules", dep);
    await mkdir(path.dirname(link), { recursive: true });
    if (!existsSync(link)) await symlink(target, link, "dir");
  }
}

/**
 * Creates a temporary directory and executes work within it.
 * Automatically cleans up the directory after work completes.
 * Does NOT change the working directory - tests should pass cwd parameter to commands.
 *
 * @param work - Async function to execute with the temp directory
 *
 * @example
 * ```typescript
 * await withTempDir(async (tempDir) => {
 *   // Pass tempDir as cwd to commands
 *   await initCommand({ cwd: tempDir });
 * });
 * // tempDir is automatically cleaned up
 * ```
 */
export async function withTempDir(
  work: (tempDir: string) => Promise<void>,
): Promise<void> {
  const tempDir = await mkdtemp(path.join(tmpdir(), "mongodbee_test_"));

  try {
    await installLibrary(tempDir);
    await work(tempDir);
  } finally {
    try {
      await rm(tempDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors (may be locked on Windows)
    }
  }
}

/**
 * Checks if a file exists and contains a specific string
 *
 * @param filePath - Path to the file to check
 * @param content - Content to search for in the file
 * @returns true if file exists and contains the content
 *
 * @example
 * ```typescript
 * const hasConfig = await fileContains("config.ts", "defineConfig");
 * ```
 */
export async function fileContains(
  filePath: string,
  content: string,
): Promise<boolean> {
  if (!existsSync(filePath)) {
    return false;
  }
  const fileContent = await readFileRaw(filePath, "utf8");
  return fileContent.includes(content);
}

/**
 * Reads the content of a file
 *
 * @param filePath - Path to the file to read
 * @returns File content or null if file doesn't exist
 *
 * @example
 * ```typescript
 * const content = await readFile("migration.ts");
 * ```
 */
export async function readFile(filePath: string): Promise<string | null> {
  if (!existsSync(filePath)) {
    return null;
  }
  return await readFileRaw(filePath, "utf8");
}

/**
 * Lists all files in a directory
 *
 * @param dirPath - Path to the directory
 * @returns Array of file names
 *
 * @example
 * ```typescript
 * const files = listFiles("./migrations");
 * ```
 */
export function listFiles(dirPath: string): string[] {
  if (!existsSync(dirPath)) {
    return [];
  }
  return [...readdirSync(dirPath, { withFileTypes: true })]
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name);
}

/**
 * Lists all TypeScript migration files in a directory, sorted by name
 *
 * @param dirPath - Path to the directory
 * @returns Sorted array of .ts file names
 *
 * @example
 * ```typescript
 * const migrations = listMigrationFiles("./migrations");
 * ```
 */
export function listMigrationFiles(dirPath: string): string[] {
  return listFiles(dirPath)
    .filter((name) => name.endsWith(".ts"))
    .sort();
}

/**
 * Gets the full path to a migration file in the migrations directory
 *
 * @param tempDir - Base temporary directory
 * @param fileName - Migration file name
 * @returns Full path to the migration file
 */
export function getMigrationPath(tempDir: string, fileName: string): string {
  return path.join(tempDir, "migrations", fileName);
}

/**
 * Gets the migrations directory path
 *
 * @param tempDir - Base temporary directory
 * @returns Path to the migrations directory
 */
export function getMigrationsDir(tempDir: string): string {
  return path.join(tempDir, "migrations");
}

/**
 * Small delay utility for testing
 *
 * @param ms - Milliseconds to delay
 *
 * @example
 * ```typescript
 * await delay(100); // Wait 100ms
 * ```
 */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Extracts migration ID from a migration file name
 *
 * @param fileName - Migration file name (e.g., "2024_10_03_1200_ULID@name.ts")
 * @returns Migration ID (without .ts extension)
 *
 * @example
 * ```typescript
 * const id = extractMigrationId("2024_10_03_1200_ULID@initial.ts");
 * // Returns: "2024_10_03_1200_ULID@initial"
 * ```
 */
export function extractMigrationId(fileName: string): string {
  return fileName.replace(".ts", "");
}

/**
 * Extracts migration name from a migration file name
 *
 * @param fileName - Migration file name
 * @returns Migration name (part after @)
 *
 * @example
 * ```typescript
 * const name = extractMigrationName("2024_10_03_1200_ULID@initial.ts");
 * // Returns: "initial"
 * ```
 */
export function extractMigrationName(fileName: string): string {
  const withoutExt = fileName.replace(".ts", "");
  const parts = withoutExt.split("@");
  return parts[1] || "";
}

/** Absolute path to the CLI entry point, for the subprocess helpers below. */
export const CLI_ENTRY: string = fileURLToPath(
  new URL("../../../src/migration/cli/main.ts", import.meta.url),
);

/**
 * Runs a TypeScript entry point under the runtime executing this suite.
 *
 * `process.execPath` rather than a hard-coded binary: the same helper then
 * works under `bun test` and under Node (which strips types itself since
 * 22.18), instead of requiring a Deno installation to test a package that no
 * longer depends on Deno.
 */
export async function runScript(
  entry: string,
  args: string[],
  cwd?: string,
): Promise<{ code: number; stderr: string; stdout: string }> {
  const child = spawn(process.execPath, [entry, ...args], {
    cwd,
    stdio: ["ignore", "pipe", "pipe"],
  });

  const read = async (stream: Readable): Promise<string> => {
    const chunks: Buffer[] = [];
    for await (const chunk of stream) chunks.push(chunk as Buffer);
    return Buffer.concat(chunks).toString("utf8");
  };

  const [stdout, stderr, code] = await Promise.all([
    read(child.stdout),
    read(child.stderr),
    new Promise<number>((resolve) => child.on("close", (c) => resolve(c ?? 0))),
  ]);

  return { code, stdout, stderr };
}

/**
 * Runs the migration CLI as a real subprocess.
 *
 * Some behaviour only exists in `main.ts` (exit codes, global option mapping)
 * and is invisible to a test that calls a command function directly.
 */
export function runCli(
  cwd: string,
  args: string[],
): Promise<{ code: number; stderr: string; stdout: string }> {
  return runScript(CLI_ENTRY, args, cwd);
}

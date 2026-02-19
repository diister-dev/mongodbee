/**
 * Shared test utilities for CLI command tests
 *
 * Provides common helpers like temporary directory management,
 * file operations, and test setup utilities.
 *
 * @module
 */

import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LIBRARY_NODE_MODULES = path.resolve(__dirname, "..", "..", "..", "node_modules");

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
/**
 * Sets up node_modules in a temp directory by symlinking to the library's node_modules.
 * This allows dynamically imported migration files to resolve @diister/mongodbee/migration.
 */
export async function setupTempNodeModules(tempDir: string): Promise<void> {
  const target = path.join(tempDir, "node_modules");
  await fsp.symlink(
    LIBRARY_NODE_MODULES,
    target,
    os.platform() === "win32" ? "junction" : "dir",
  );
}

export async function withTempDir(
  work: (tempDir: string) => Promise<void>,
): Promise<void> {
  const tempDir = await fsp.mkdtemp(path.join(os.tmpdir(), "mongodbee_test_"));

  try {
    await setupTempNodeModules(tempDir);
    await work(tempDir);
  } finally {
    try {
      await fsp.rm(tempDir, { recursive: true });
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
  if (!fs.existsSync(filePath)) {
    return false;
  }
  const fileContent = await fsp.readFile(filePath, "utf-8");
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
  if (!fs.existsSync(filePath)) {
    return null;
  }
  return await fsp.readFile(filePath, "utf-8");
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
  if (!fs.existsSync(dirPath)) {
    return [];
  }
  return fs.readdirSync(dirPath, { withFileTypes: true })
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

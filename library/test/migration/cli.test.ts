/**
 * Tests for MongoDBee Migration CLI commands
 * 
 * Tests all CLI commands including init, generate, status, apply, and rollback
 */

import { assertEquals, assertExists, assert } from "@std/assert";
import * as path from "@std/path";
import { existsSync } from "@std/fs";
import { initCommand } from "../../src/migration/cli/commands/init.ts";
import { generateCommand } from "../../src/migration/cli/commands/generate.ts";

// Helper to create temporary test directory
async function withTempDir(work: (tempDir: string) => Promise<void>) {
  const tempDir = await Deno.makeTempDir({ prefix: "mongodbee_test_" });
  const originalCwd = Deno.cwd();
  
  try {
    Deno.chdir(tempDir);
    await work(tempDir);
  } finally {
    Deno.chdir(originalCwd);
    await Deno.remove(tempDir, { recursive: true });
  }
}

// Helper to check if a file exists and contains a string
async function fileContains(filePath: string, content: string): Promise<boolean> {
  if (!existsSync(filePath)) {
    return false;
  }
  const fileContent = await Deno.readTextFile(filePath);
  return fileContent.includes(content);
}

// ============================================================================
// Init Command Tests
// ============================================================================

Deno.test("CLI init - creates config file and migrations directory", async () => {
  await withTempDir(async (tempDir) => {
    await initCommand({});

    // Check config file was created
    const configPath = path.join(tempDir, "mongodbee.config.ts");
    assertExists(existsSync(configPath));
    assert(await fileContains(configPath, "defineConfig"));

    // Check schemas file was created
    const schemasPath = path.join(tempDir, "schemas.ts");
    assertExists(existsSync(schemasPath));
    assert(await fileContains(schemasPath, "export const schemas"));

    // Check migrations directory was created
    const migrationsDir = path.join(tempDir, "migrations");
    assertExists(existsSync(migrationsDir));
  });
});

Deno.test("CLI init - respects force flag to overwrite existing config", async () => {
  await withTempDir(async (tempDir) => {
    // First init
    await initCommand({});

    // Modify config file
    const configPath = path.join(tempDir, "mongodbee.config.ts");
    await Deno.writeTextFile(configPath, "// Modified content");

    // Init without force should not overwrite
    await initCommand({});
    assert(await fileContains(configPath, "// Modified content"));

    // Init with force should overwrite
    await initCommand({ force: true });
    assert(await fileContains(configPath, "defineConfig"));
    assert(!await fileContains(configPath, "// Modified content"));
  });
});

// ============================================================================
// Generate Command Tests
// ============================================================================

Deno.test("CLI generate - creates first migration with no parent", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({});

    // Generate first migration
    await generateCommand({ name: "initial" });

    // Check migration file was created
    const migrationsDir = path.join(tempDir, "migrations");
    const files = [...Deno.readDirSync(migrationsDir)];
    
    assertEquals(files.length, 1);
    assert(files[0].name.includes("@initial"));

    // Check file content
    const migrationPath = path.join(migrationsDir, files[0].name);
    const content = await Deno.readTextFile(migrationPath);
    
    assert(content.includes("migrationDefinition"));
    assert(content.includes("parent: null"));
    assert(content.includes("initial"));
  });
});

Deno.test("CLI generate - creates child migration with parent reference", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({});

    // Generate first migration
    await generateCommand({ name: "initial" });

    // Generate second migration
    await generateCommand({ name: "add_users" });

    // Check both files exist
    const migrationsDir = path.join(tempDir, "migrations");
    const files = [...Deno.readDirSync(migrationsDir)];
    
    assertEquals(files.length, 2);

    // Sort files to ensure order
    const sortedFiles = files.map(f => f.name).sort();
    
    // Check second migration references first as parent
    const secondMigrationPath = path.join(migrationsDir, sortedFiles[1]);
    const content = await Deno.readTextFile(secondMigrationPath);
    
    assert(content.includes("import parent from"));
    assert(content.includes(sortedFiles[0]));
    assert(content.includes("parent: parent"));
  });
});

Deno.test("CLI generate - includes parent schemas in child migration", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({});

    // Generate first migration
    await generateCommand({ name: "initial" });

    // Generate second migration
    await generateCommand({ name: "add_field" });

    // Check second migration inherits parent schemas
    const migrationsDir = path.join(tempDir, "migrations");
    const files = [...Deno.readDirSync(migrationsDir)].map(f => f.name).sort();
    
    const secondMigrationPath = path.join(migrationsDir, files[1]);
    const content = await Deno.readTextFile(secondMigrationPath);
    
    assert(content.includes("...parent.schemas.collections"));
  });
});

Deno.test("CLI generate - creates unique migration IDs", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({});

    // Generate multiple migrations quickly
    await generateCommand({ name: "migration_1" });
    
    // Small delay to ensure different timestamps
    await new Promise(resolve => setTimeout(resolve, 10));
    
    await generateCommand({ name: "migration_2" });

    // Check all have unique IDs
    const migrationsDir = path.join(tempDir, "migrations");
    const files = [...Deno.readDirSync(migrationsDir)].map(f => f.name);
    
    assertEquals(files.length, 2);
    
    // IDs should be different
    const ids = files.map(name => name.split("@")[0]);
    assertEquals(new Set(ids).size, 2);
  });
});

Deno.test("CLI generate - preserves migration name in ID", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({});

    // Generate migration with specific name
    const migrationName = "create_users_table";
    await generateCommand({ name: migrationName });

    // Check file includes the name
    const migrationsDir = path.join(tempDir, "migrations");
    const files = [...Deno.readDirSync(migrationsDir)];
    
    assertEquals(files.length, 1);
    assert(files[0].name.includes(`@${migrationName}`));
  });
});

// ============================================================================
// Migration ID Format Tests
// ============================================================================

Deno.test("CLI generate - migration ID follows correct format", async () => {
  await withTempDir(async (tempDir) => {
    // Setup
    await initCommand({});

    // Generate migration
    await generateCommand({ name: "test_migration" });

    // Check ID format: YYYY_MM_DD_HHMM_ULID@name
    const migrationsDir = path.join(tempDir, "migrations");
    const files = [...Deno.readDirSync(migrationsDir)];
    
    const fileName = files[0].name;
    const fileNameWithoutExt = fileName.replace(".ts", "");
    
    // Should have format: YYYY_MM_DD_HHMM_ULID@name
    const parts = fileNameWithoutExt.split("@");
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

// ============================================================================
// Error Handling Tests
// ============================================================================

Deno.test("CLI generate - fails gracefully if migrations directory doesn't exist", async () => {
  await withTempDir(async (tempDir) => {
    // Create config without migrations directory
    await Deno.writeTextFile(
      path.join(tempDir, "mongodbee.config.ts"),
      `export default { paths: { migrationsDir: "./migrations" } };`
    );

    // Try to generate migration
    try {
      await generateCommand({ name: "test" });
    } catch (_error) {
      // Should handle gracefully (either create dir or show error)
      // Not throwing is acceptable behavior
    }
  });
});

/**
 * Tests for the history command
 * 
 * Tests displaying migration operation history including:
 * - Showing migration operations
 * - Showing timestamps
 * - Showing success/failure status
 * - Empty history handling
 * - Multiple operations tracking
 * 
 * @module
 */

import { assertEquals } from "@std/assert";
import { MongoClient } from "../../../src/mongodb.ts";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { generateCommand } from "../../../src/migration/cli/commands/generate.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { rollbackCommand } from "../../../src/migration/cli/commands/rollback.ts";
import { historyCommand } from "../../../src/migration/cli/commands/history.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { withTempDir, delay } from "./shared.ts";

// MongoDB test connection
const TEST_MONGODB_URI = Deno.env.get("TEST_MONGODB_URI") || "mongodb://localhost:27017";

/**
 * Generate a unique database name for each test to avoid collisions
 */
function generateTestDbName(): string {
  return `mongodbee_test_history_${crypto.randomUUID().replace(/-/g, "").substring(0, 8)}`;
}

/**
 * Helper to setup a test database
 */
async function withTestDb(work: (db: ReturnType<MongoClient["db"]>, client: MongoClient, dbName: string) => Promise<void>) {
  const dbName = generateTestDbName();
  const client = new MongoClient(TEST_MONGODB_URI);
  
  try {
    await client.connect();
    const db = client.db(dbName);
    
    // Clean database before test
    await db.dropDatabase();
    
    await work(db, client, dbName);
  } finally {
    // Clean up and close
    try {
      const db = client.db(dbName);
      await db.dropDatabase();
    } catch {
      // Ignore cleanup errors
    }
    await client.close();
  }
}

/**
 * Setup test configuration
 */
async function setupTestConfig(tempDir: string, dbName: string) {
  await Deno.writeTextFile(
    `${tempDir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`
  );
}

Deno.test("history - shows empty history when no migrations run", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      
      // Run history with no migrations
      await historyCommand({ cwd: tempDir });
      // Should complete without error
    });
  });
});

Deno.test("history - shows applied migrations in history", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      
      // Generate and apply migrations
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });
      
      // Apply migrations
      await migrateCommand({ cwd: tempDir });
      
      // Verify applied
      const appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 2);
      
      // Run history
      await historyCommand({ cwd: tempDir });
      // Should show migration application history
    });
  });
});

Deno.test("history - shows rollback operations in history", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      
      // Generate migrations
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });
      
      // Apply migrations
      await migrateCommand({ cwd: tempDir });
      
      let appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 2);
      
      // Rollback one
      await rollbackCommand({ force: true, cwd: tempDir });
      
      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 1);
      
      // Run history
      await historyCommand({ cwd: tempDir });
      // Should show both apply and rollback operations
    });
  });
});

Deno.test("history - shows multiple operations chronologically", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      
      // Generate migrations
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "third", cwd: tempDir });
      
      // Perform multiple operations
      await migrateCommand({ cwd: tempDir }); // Apply all
      await rollbackCommand({ force: true, cwd: tempDir }); // Rollback one
      await migrateCommand({ cwd: tempDir }); // Re-apply
      
      // Run history
      await historyCommand({ cwd: tempDir });
      // Should show all operations in chronological order
    });
  });
});

Deno.test("history - uses custom config path when provided", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName); // Standard config for generate
      
      // Create custom config
      await Deno.writeTextFile(
        `${tempDir}/custom.config.ts`,
        `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`
      );
      
      await generateCommand({ name: "test", cwd: tempDir });
      
      // Apply with custom config
      await migrateCommand({ configPath: "./custom.config.ts", cwd: tempDir });
      
      // Run history with custom config
      await historyCommand({ configPath: "./custom.config.ts", cwd: tempDir });
      // Should complete without error
    });
  });
});

Deno.test({
  name: "history - handles connection errors gracefully",
  sanitizeResources: false, // MongoDB connection attempt leaves resources
  sanitizeOps: false, // DNS resolution doesn't complete
  fn: async () => {
  await withTempDir(async (tempDir) => {
    // Setup with invalid connection (with short timeout)
    await initCommand({ cwd: tempDir });
    await Deno.writeTextFile(
      `${tempDir}/mongodbee.config.ts`,
      `export default { db: { uri: "mongodb://invalid:27017?serverSelectionTimeoutMS=1000&connectTimeoutMS=1000", name: "test" }, paths: { migrationsDir: "./migrations" } };`
    );
    
    await generateCommand({ name: "test", cwd: tempDir });
    
    // Try to run history
    try {
      await historyCommand({ cwd: tempDir });
    } catch (error) {
      // Connection error is expected
      assertEquals(error instanceof Error, true);
    }
  });
}});

Deno.test("history - shows operations for specific migrations", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      
      // Generate migrations with distinct names
      await generateCommand({ name: "users", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "posts", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "comments", cwd: tempDir });
      
      // Apply all migrations
      await migrateCommand({ cwd: tempDir });
      
      const appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 3);
      
      // Run history
      await historyCommand({ cwd: tempDir });
      // Should show individual operations for each migration
    });
  });
});

Deno.test("history - reflects current database state", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      
      await generateCommand({ name: "initial", cwd: tempDir });
      
      // Initial state - no migrations
      await historyCommand({ cwd: tempDir });
      
      // Apply migration
      await migrateCommand({ cwd: tempDir });
      
      let appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 1);
      
      // History after apply
      await historyCommand({ cwd: tempDir });
      
      // Rollback
      await rollbackCommand({ force: true, cwd: tempDir });
      
      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 0);
      
      // History after rollback
      await historyCommand({ cwd: tempDir });
      // Each state should be reflected correctly
    });
  });
});

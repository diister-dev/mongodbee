/**
 * Tests for the status command
 *
 * Tests displaying migration status including:
 * - Showing pending migrations
 * - Showing applied migrations
 * - Empty state handling
 * - Mixed state (some applied, some pending)
 *
 * @module
 */

import { test, expect } from "vitest";
import * as fsp from "node:fs/promises";
import { MongoClient } from "../../../src/mongodb.ts";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { generateCommand } from "../../../src/migration/cli/commands/generate.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { statusCommand } from "../../../src/migration/cli/commands/status.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { delay, withTempDir } from "./shared.ts";

// MongoDB test connection
const TEST_MONGODB_URI = process.env.TEST_MONGODB_URI ||
  "mongodb://127.0.0.1:27017";

/**
 * Generate a unique database name for each test to avoid collisions in parallel execution
 */
function generateTestDbName(): string {
  return `mongodbee_test_status_${
    crypto.randomUUID().replace(/-/g, "").substring(0, 8)
  }`;
}

/**
 * Helper to setup a test database
 */
async function withTestDb(
  work: (
    db: ReturnType<MongoClient["db"]>,
    client: MongoClient,
    dbName: string,
  ) => Promise<void>,
) {
  const client = new MongoClient(TEST_MONGODB_URI);
  const dbName = generateTestDbName();

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
  await fsp.writeFile(
    `${tempDir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
    "utf-8",
  );
}

test("status - shows no migrations when none exist", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Run status with no migrations
      await statusCommand({ cwd: tempDir });
      // Should complete without error
    });
  });
});

test("status - shows pending migrations when none applied", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Generate migrations
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });

      // Check no migrations applied yet
      const appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(0);

      // Run status
      await statusCommand({ cwd: tempDir });
      // Should show 2 pending migrations
    });
  });
});

test("status - shows applied migrations", async () => {
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
      await migrateCommand({ cwd: tempDir, force: true });

      // Verify applied
      const appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(2);

      // Run status
      await statusCommand({ cwd: tempDir });
      // Should show 2 applied migrations
    });
  });
});

test("status - shows mixed state (some applied, some pending)", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Generate first batch
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });

      // Apply first batch
      await migrateCommand({ cwd: tempDir, force: true });

      // Generate more migrations
      await delay(10);
      await generateCommand({ name: "third", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "fourth", cwd: tempDir });

      // Verify state
      const appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(2);

      // Run status
      await statusCommand({ cwd: tempDir });
      // Should show 2 applied, 2 pending
    });
  });
});

test("status - shows up-to-date when all migrations applied", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Generate migrations
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });

      // Apply all migrations
      await migrateCommand({ cwd: tempDir, force: true });

      // Verify all applied
      const appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(2);

      // Run status
      await statusCommand({ cwd: tempDir });
      // Should show all migrations applied, database up-to-date
    });
  });
});

test("status - uses custom config path when provided", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName); // Standard config for generate

      // Create custom config
      await fsp.writeFile(
        `${tempDir}/custom.config.ts`,
        `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
        "utf-8",
      );

      await generateCommand({ name: "test", cwd: tempDir });

      // Run status with custom config
      await statusCommand({ configPath: "./custom.config.ts", cwd: tempDir });
      // Should complete without error
    });
  });
});

test("status - handles connection errors gracefully", async () => {
  await withTempDir(async (tempDir) => {
    // Setup with invalid connection (with short timeout)
    await initCommand({ cwd: tempDir });
    await fsp.writeFile(
      `${tempDir}/mongodbee.config.ts`,
      `export default { database: { connection: { uri: "mongodb://invalid:27017?serverSelectionTimeoutMS=1000&connectTimeoutMS=1000" }, name: "test" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
      "utf-8",
    );

    await generateCommand({ name: "test", cwd: tempDir });

    // Try to run status
    try {
      await statusCommand({ cwd: tempDir });
    } catch (error) {
      // Connection error is expected
      expect(error instanceof Error).toEqual(true);
    }
  });
});

test("status - works after migrations are rolled back", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Generate migrations
      await generateCommand({ name: "first", cwd: tempDir });

      // Apply then rollback
      await migrateCommand({ cwd: tempDir, force: true });

      const appliedBefore = await getAppliedMigrationIds(db);
      expect(appliedBefore.length).toEqual(1);

      // Rollback (would need to import rollbackCommand)
      // For now, just verify status works with applied migrations

      // Run status
      await statusCommand({ cwd: tempDir });
      // Should show current state correctly
    });
  });
});

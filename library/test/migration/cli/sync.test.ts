/**
 * Tests for the sync command
 *
 * Tests synchronizing schemas and indexes:
 * - Rejecting sync when migrations are pending
 * - Forcing sync even with pending migrations
 * - Successful sync when all migrations applied
 * - Sync with no migrations
 *
 * @module
 */

import { test, expect } from "vitest";
import * as fsp from "node:fs/promises";
import { MongoClient } from "../../../src/mongodb.ts";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { generateCommand } from "../../../src/migration/cli/commands/generate.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { syncCommand } from "../../../src/migration/cli/commands/sync.ts";
import { withTempDir } from "./shared.ts";

// MongoDB test connection
const TEST_MONGODB_URI = process.env.TEST_MONGODB_URI ||
  "mongodb://127.0.0.1:27017";

/**
 * Generate a unique database name for each test to avoid collisions in parallel execution
 */
function generateTestDbName(): string {
  return `mongodbee_test_sync_${
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

test("sync - succeeds when no migrations exist", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Run sync with no migrations
      await syncCommand({ cwd: tempDir });
      // Should complete without error
    });
  });
});

test("sync - rejects when pending migrations exist", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Generate a migration but don't apply it
      await generateCommand({ name: "first", cwd: tempDir });

      // Sync should fail because migration is pending
      await expect(
        async () => await syncCommand({ cwd: tempDir }),
      ).rejects.toThrow("Cannot sync: pending migrations detected");
    });
  });
});

test("sync - succeeds with --force even when pending migrations exist", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Generate a migration but don't apply it
      await generateCommand({ name: "first", cwd: tempDir });

      // Sync should succeed with --force flag
      await syncCommand({ cwd: tempDir, force: true });
      // Should complete without error
    });
  });
});

test("sync - succeeds when all migrations are applied", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Generate and apply a migration
      await generateCommand({ name: "first", cwd: tempDir });

      // Apply the migration
      await migrateCommand({ cwd: tempDir, force: true });

      // Sync should succeed since all migrations are applied
      await syncCommand({ cwd: tempDir });
      // Should complete without error
    });
  });
});

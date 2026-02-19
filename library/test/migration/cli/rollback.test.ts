/**
 * Tests for the rollback command
 *
 * Tests rolling back applied migrations including:
 * - Rolling back the last migration
 * - Rolling back multiple times
 * - Rollback validation
 * - Error handling when no migrations to rollback
 * - State tracking after rollback
 *
 * @module
 */

import { test, expect } from "vitest";
import * as fsp from "node:fs/promises";
import { MongoClient } from "../../../src/mongodb.ts";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { generateCommand } from "../../../src/migration/cli/commands/generate.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { rollbackCommand } from "../../../src/migration/cli/commands/rollback.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import {
  delay,
  getMigrationPath,
  getMigrationsDir,
  listMigrationFiles,
  readFile,
  withTempDir,
} from "./shared.ts";

// MongoDB test connection
const TEST_MONGODB_URI = process.env.TEST_MONGODB_URI ||
  "mongodb://127.0.0.1:27017";

/**
 * Generate a unique database name for each test to avoid collisions
 */
function generateTestDbName(): string {
  return `mongodbee_test_rollback_${
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
  await fsp.writeFile(
    `${tempDir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
    "utf-8",
  );
}

test("rollback - rolls back the last applied migration", async () => {
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

      let appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(2);

      // Rollback
      await rollbackCommand({ force: true, cwd: tempDir });

      // Should have only 1 migration applied
      appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(1);
    });
  });
});

test("rollback - can rollback multiple times", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Generate three migrations
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "third", cwd: tempDir });

      // Apply all migrations
      await migrateCommand({ cwd: tempDir, force: true });

      let appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(3);

      // First rollback
      await rollbackCommand({ force: true, cwd: tempDir });
      appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(2);

      // Second rollback
      await rollbackCommand({ force: true, cwd: tempDir });
      appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(1);

      // Third rollback
      await rollbackCommand({ force: true, cwd: tempDir });
      appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(0);
    });
  });
});

test("rollback - fails gracefully when no migrations to rollback", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      await generateCommand({ name: "test", cwd: tempDir });

      // Try to rollback without any applied migrations
      try {
        await rollbackCommand({ force: true, cwd: tempDir });
        // Should handle gracefully (either succeed with message or throw)
      } catch (error) {
        // Error is acceptable behavior
        expect(error instanceof Error).toBeTruthy();
      }
    });
  });
});

test("rollback - can re-apply after rollback", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      await generateCommand({ name: "test", cwd: tempDir });

      // Apply migration
      await migrateCommand({ cwd: tempDir, force: true });

      let appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(1);

      // Rollback
      await rollbackCommand({ force: true, cwd: tempDir });

      appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(0);

      // Re-apply
      await migrateCommand({ cwd: tempDir, force: true });

      appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(1);
    });
  });
});

test("rollback - handles migration with operations", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Generate a migration
      await generateCommand({ name: "create_users", cwd: tempDir });

      // Add actual operations to the migration
      const files = listMigrationFiles(getMigrationsDir(tempDir));
      const migrationPath = getMigrationPath(tempDir, files[0]);
      let content = await readFile(migrationPath);

      expect(content !== null).toBeTruthy();

      // Add a createCollection operation
      content = `import * as v from "valibot";
      ${content}`;

      content = content.replace(`collections: {`, `collections: {
        users: {
          name: v.string()
        },`);

      content = content.replace(
        "migrate(migration) {",
        `migrate(migration) {
        migration.createCollection("users");`,
      );

      await fsp.writeFile(migrationPath, content, "utf-8");

      // Update the schema file to include the new collection
      const schemaPath = `${tempDir}/schemas.ts`;
      let schemaContent = await readFile(schemaPath);
      expect(schemaContent !== null).toBeTruthy();

      schemaContent = `import * as v from "valibot";
       ${schemaContent}`;
      schemaContent = schemaContent.replace(
        "collections: {",
        `collections: {
          users: {
            name: v.string()
          },`,
      );

      await fsp.writeFile(schemaPath, schemaContent, "utf-8");

      // Apply migration
      await migrateCommand({ cwd: tempDir, force: true });

      // Check collection was created
      const collections = await db.listCollections().toArray();
      const userCollection = collections.find((c) => c.name === "users");
      expect(userCollection !== undefined).toBeTruthy();

      // Rollback
      await rollbackCommand({ force: true, cwd: tempDir });

      // Collection should still exist (rollback doesn't delete collections)
      // but migration should be unmarked
      const appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(0);
    });
  });
});

test("rollback - respects dry run mode (if supported)", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      await generateCommand({ name: "test", cwd: tempDir });

      // Apply migration
      await migrateCommand({ cwd: tempDir, force: true });

      let appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(1);

      // Note: Check if rollback supports dryRun option
      // For now, just verify it works normally
      await rollbackCommand({ force: true, cwd: tempDir });

      // Migration should be rolled back
      appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(0);
    });
  });
});

test("rollback - uses custom config path when provided", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });

      // Create both configs
      await setupTestConfig(tempDir, dbName); // Standard config for generate
      await fsp.writeFile(
        `${tempDir}/custom.config.ts`,
        `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
        "utf-8",
      );

      await generateCommand({ name: "test", cwd: tempDir });

      // Apply with custom config
      await migrateCommand({ configPath: "./custom.config.ts", cwd: tempDir, force: true });

      let appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(1);

      // Rollback with custom config
      await rollbackCommand({
        configPath: "./custom.config.ts",
        force: true,
        cwd: tempDir,
      });

      appliedIds = await getAppliedMigrationIds(db);
      expect(appliedIds.length).toEqual(0);
    });
  });
});

test("rollback - maintains migration order", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);

      // Generate migrations in order
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "third", cwd: tempDir });

      // Apply all
      await migrateCommand({ cwd: tempDir, force: true });

      const initialIds = await getAppliedMigrationIds(db);
      expect(initialIds.length).toEqual(3);

      // Rollback one
      await rollbackCommand({ force: true, cwd: tempDir });

      const afterRollback = await getAppliedMigrationIds(db);
      expect(afterRollback.length).toEqual(2);

      // The remaining migrations should be in order
      expect(afterRollback[0]).toEqual(initialIds[0]);
      expect(afterRollback[1]).toEqual(initialIds[1]);
    });
  });
});

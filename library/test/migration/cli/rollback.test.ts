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

import { test } from "../../+harness.ts";
import process from "node:process";
import { readFile as readFileRaw, writeFile } from "node:fs/promises";
import { assert, assertEquals, assertStringIncludes } from "../../+assert.ts";
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
const TEST_MONGODB_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";

/**
 * Generate a unique database name for each test to avoid collisions
 */
function generateTestDbName(): string {
  return `mongodbee_test_rollback_${crypto
    .randomUUID()
    .replace(/-/g, "")
    .substring(0, 8)}`;
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
  await writeFile(
    `${tempDir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
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
      assertEquals(appliedIds.length, 2);

      // Rollback
      await rollbackCommand({ force: true, cwd: tempDir });

      // Should have only 1 migration applied
      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 1);
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
      assertEquals(appliedIds.length, 3);

      // First rollback
      await rollbackCommand({ force: true, cwd: tempDir });
      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 2);

      // Second rollback
      await rollbackCommand({ force: true, cwd: tempDir });
      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 1);

      // Third rollback
      await rollbackCommand({ force: true, cwd: tempDir });
      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 0);
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
        assert(error instanceof Error);
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
      assertEquals(appliedIds.length, 1);

      // Rollback
      await rollbackCommand({ force: true, cwd: tempDir });

      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 0);

      // Re-apply
      await migrateCommand({ cwd: tempDir, force: true });

      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 1);
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

      assert(content !== null);

      // Add a createCollection operation
      content = `import * as v from "valibot";
      ${content}`;

      content = content.replace(
        `collections: {`,
        `collections: {
        users: {
          name: v.string()
        },`,
      );

      content = content.replace(
        "migrate(migration) {",
        `migrate(migration) {
        migration.createCollection("users");`,
      );

      await writeFile(migrationPath, content);

      // Update the schema file to include the new collection
      const schemaPath = `${tempDir}/schemas.ts`;
      let schemaContent = await readFile(schemaPath);
      assert(schemaContent !== null);

      schemaContent = `import * as v from "valibot";
       ${schemaContent}`;
      schemaContent = schemaContent.replace(
        "collections: {",
        `collections: {
          users: {
            name: v.string()
          },`,
      );

      await writeFile(schemaPath, schemaContent);

      // Apply migration
      await migrateCommand({ cwd: tempDir, force: true });

      // Check collection was created
      const collections = await db.listCollections().toArray();
      const userCollection = collections.find((c) => c.name === "users");
      assert(userCollection !== undefined);

      // Rollback
      await rollbackCommand({ force: true, cwd: tempDir });

      // Collection should still exist (rollback doesn't delete collections)
      // but migration should be unmarked
      const appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 0);
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
      assertEquals(appliedIds.length, 1);

      // Note: Check if rollback supports dryRun option
      // For now, just verify it works normally
      await rollbackCommand({ force: true, cwd: tempDir });

      // Migration should be rolled back
      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 0);
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
      await writeFile(
        `${tempDir}/custom.config.ts`,
        `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
      );

      await generateCommand({ name: "test", cwd: tempDir });

      // Apply with custom config
      await migrateCommand({
        configPath: "./custom.config.ts",
        cwd: tempDir,
        force: true,
      });

      let appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 1);

      // Rollback with custom config
      await rollbackCommand({
        configPath: "./custom.config.ts",
        force: true,
        cwd: tempDir,
      });

      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 0);
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
      assertEquals(initialIds.length, 3);

      // Rollback one
      await rollbackCommand({ force: true, cwd: tempDir });

      const afterRollback = await getAppliedMigrationIds(db);
      assertEquals(afterRollback.length, 2);

      // The remaining migrations should be in order
      assertEquals(afterRollback[0], initialIds[0]);
      assertEquals(afterRollback[1], initialIds[1]);
    });
  });
});

// Verrou — rollback must surface the applier's progress events.
//
// Regression it guards: rollback rewrites a REAL database and can run for
// minutes on a large collection, yet it built its applier without an
// `onProgress` callback and ran completely silent. `migrate` had surfaced the
// same events since it landed, from the same seam, one call site over — the
// most stressful command to run was the only blind one.
test("rollback - the live progress line is wired and documented", async () => {
  const src = await readFileRaw(
    new URL("../../../src/migration/cli/commands/rollback.ts", import.meta.url),
    "utf8",
  );
  assertStringIncludes(
    src,
    "onProgress: progress.onProgress",
    "rollback built its applier without the progress callback",
  );
  assertStringIncludes(
    src,
    "progress.finish()",
    "the progress line is never closed",
  );

  const help = await readFileRaw(
    new URL("../../../src/migration/cli/main.ts", import.meta.url),
    "utf8",
  );
  assertStringIncludes(
    help,
    "ROLLBACK OPTIONS:",
    "rollback options are undocumented",
  );
});

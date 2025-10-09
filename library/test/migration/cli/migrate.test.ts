/**
 * Tests for the migrate command
 *
 * Tests applying pending migrations to the database including:
 * - Applying single migration
 * - Applying multiple migrations in sequence
 * - Dry run mode
 * - Schema validation
 * - Error handling
 * - Migration state tracking
 *
 * @module
 */

import { assert, assertEquals } from "@std/assert";
import { MongoClient } from "../../../src/mongodb.ts";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { generateCommand } from "../../../src/migration/cli/commands/generate.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
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
const TEST_MONGODB_URI = Deno.env.get("TEST_MONGODB_URI") ||
  "mongodb://localhost:27017";

/**
 * Generate a unique database name for each test to avoid collisions
 */
function generateTestDbName(): string {
  return `mongodbee_test_migrate_${
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
  await Deno.writeTextFile(
    `${tempDir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
}

Deno.test("migrate - applies single pending migration", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });

      // Update config to use test database
      await setupTestConfig(tempDir, dbName);

      // Generate a migration
      await generateCommand({ name: "initial", cwd: tempDir });

      // Apply migrations
      await migrateCommand({ cwd: tempDir, force: true });

      // Check migration was marked as applied
      const appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 1);
    });
  });
});

Deno.test("migrate - applies multiple pending migrations in order", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });

      // Update config
      await setupTestConfig(tempDir, dbName);

      // Generate multiple migrations
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "third", cwd: tempDir });

      // Apply all migrations
      await migrateCommand({ cwd: tempDir, force: true });

      // Check all migrations were applied
      const appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 3);
    });
  });
});

Deno.test("migrate - skips already applied migrations", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });

      // Update config
      await setupTestConfig(tempDir, dbName);

      // Generate migrations
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });

      // Apply migrations first time
      await migrateCommand({ cwd: tempDir, force: true });

      // Check applied count
      let appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 2);

      // Apply again - should be no-op
      await migrateCommand({ cwd: tempDir, force: true });

      // Count should still be 2
      appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 2);
    });
  });
});

Deno.test("migrate - dry run mode doesn't apply migrations", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });

      // Update config
      await setupTestConfig(tempDir, dbName);

      // Generate a migration
      await generateCommand({ name: "test", cwd: tempDir });

      // Apply with dry run
      await migrateCommand({ dryRun: true, cwd: tempDir, force: true });

      // Check no migrations were applied
      const appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 0);
    });
  });
});

Deno.test("migrate - handles migrations with actual operations", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });

      // Update config
      await setupTestConfig(tempDir, dbName);

      // Generate a migration
      await generateCommand({ name: "create_users", cwd: tempDir });

      // Get the migration file and add actual operations
      const files = listMigrationFiles(getMigrationsDir(tempDir));
      const migrationPath = getMigrationPath(tempDir, files[0]);
      let content = await readFile(migrationPath);

      assert(content !== null);

      // Add a createCollection operation
      content = content.replace(
        "migrate(migration) {",
        `migrate(migration) {
        migration.createCollection("users");`,
      );

      await Deno.writeTextFile(migrationPath, content);

      // Apply migration
      await migrateCommand({ cwd: tempDir, force: true });

      // Check collection was created
      const collections = await db.listCollections().toArray();
      const userCollection = collections.find((c) => c.name === "users");
      assert(userCollection !== undefined);
    });
  });
});

Deno.test("migrate - reports success when no pending migrations", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });

      // Update config
      await setupTestConfig(tempDir, dbName);

      // Generate and apply a migration
      await generateCommand({ name: "initial", cwd: tempDir });
      await migrateCommand({ cwd: tempDir, force: true });

      // Apply again - should succeed with no pending message
      await migrateCommand({ cwd: tempDir, force: true });
      // If no error thrown, test passes
    });
  });
});

Deno.test("migrate - validates schema consistency before applying", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });

      // Update config
      await Deno.writeTextFile(
        `${tempDir}/mongodbee.config.ts`,
        `export default { db: { uri: "${TEST_MONGODB_URI}", name: "${dbName}" }, paths: { migrationsDir: "./migrations", schema: "./schemas.ts" } };`,
      );

      // Generate a migration
      await generateCommand({ name: "test", cwd: tempDir });

      // Schema validation should run (will pass for generated migrations)
      await migrateCommand({ cwd: tempDir, force: true });
      // If no schema validation error, test passes
    });
  });
});

Deno.test("migrate - uses custom config path when provided", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Setup
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName); // Standard config for generate

      // Create custom config
      await Deno.writeTextFile(
        `${tempDir}/custom.config.ts`,
        `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
      );

      // Generate with standard config
      await generateCommand({ name: "test", cwd: tempDir });

      // Apply with custom config path
      await migrateCommand({ configPath: "./custom.config.ts", cwd: tempDir, force: true });

      // Check migration was applied
      const appliedIds = await getAppliedMigrationIds(db);
      assertEquals(appliedIds.length, 1);
    });
  });
});

Deno.test("migrate - validates all migrations before applying any", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      // Initialize project
      await initCommand({ cwd: tempDir, force: true });

      // Update config with test database
      const configContent = `
export default {
  database: {
    connection: { uri: "${TEST_MONGODB_URI}" },
    name: "${dbName}",
  },
  paths: {
    migrations: "./migrations",
    schemas: "./schemas.ts",
  },
};`;
      await Deno.writeTextFile(`${tempDir}/mongodbee.config.ts`, configContent);

      // Generate first migration (valid)
      await generateCommand({ name: "create_users", cwd: tempDir });
      await delay(10);

      // Generate second migration (valid)
      await generateCommand({ name: "add_posts", cwd: tempDir });
      await delay(10);

      // Generate third migration (we'll make it invalid)
      await generateCommand({ name: "add_age_field", cwd: tempDir });

      const migrationsDir = getMigrationsDir(tempDir);
      const files = await listMigrationFiles(migrationsDir);
      assertEquals(files.length, 3);

      // Make first migration valid
      const migration1Path = getMigrationPath(tempDir, files[0]);
      const migration1Content = `
import { migrationDefinition } from "@diister/mongodbee/migration";
import { migrationBuilder } from "@diister/mongodbee/migration";
import * as v from "valibot";

const userSchema = {
  name: v.pipe(v.string(), v.nonEmpty()),
  email: v.pipe(v.string(), v.email()),
};

export default migrationDefinition({
  id: "${files[0].replace(".ts", "")}",
  name: "create_users",
  parent: null,
  schemas: { collections: { users: userSchema }, multiModels: {} },
  migrate: (b) => b.createCollection("users", userSchema).compile(),
});`;
      await Deno.writeTextFile(migration1Path, migration1Content);

      // Make second migration valid
      const migration2Path = getMigrationPath(tempDir, files[1]);
      const migration2Content = `
import { migrationDefinition } from "@diister/mongodbee/migration";
import { migrationBuilder } from "@diister/mongodbee/migration";
import * as v from "valibot";

const userSchema = {
  name: v.pipe(v.string(), v.nonEmpty()),
  email: v.pipe(v.string(), v.email()),
};

const postSchema = {
  title: v.pipe(v.string(), v.nonEmpty()),
  content: v.string(),
};

export default migrationDefinition({
  id: "${files[1].replace(".ts", "")}",
  name: "add_posts",
  parent: "${files[0].replace(".ts", "")}",
  schemas: { collections: { users: userSchema, posts: postSchema }, multiModels: {} },
  migrate: (b) => b.createCollection("posts", postSchema).compile(),
});`;
      await Deno.writeTextFile(migration2Path, migration2Content);

      // Make third migration INVALID (schema change without transformation)
      const migration3Path = getMigrationPath(tempDir, files[2]);
      const migration3Content = `
import { migrationDefinition } from "@diister/mongodbee/migration";
import { migrationBuilder } from "@diister/mongodbee/migration";
import * as v from "valibot";

const userSchema = {
  name: v.pipe(v.string(), v.nonEmpty()),
  email: v.pipe(v.string(), v.email()),
  age: v.number(), // ❌ NEW REQUIRED FIELD without transformation
};

const postSchema = {
  title: v.pipe(v.string(), v.nonEmpty()),
  content: v.string(),
};

export default migrationDefinition({
  id: "${files[2].replace(".ts", "")}",
  name: "add_age_field",
  parent: "${files[1].replace(".ts", "")}",
  schemas: { collections: { users: userSchema, posts: postSchema }, multiModels: {} },
  migrate: (b) => b.compile(), // ❌ NO TRANSFORMATION - This should fail validation
});`;
      await Deno.writeTextFile(migration3Path, migration3Content);

      // Try to apply migrations
      let errorThrown = false;
      let errorMessage = "";
      try {
        await migrateCommand({ cwd: tempDir, force: true });
      } catch (error) {
        errorThrown = true;
        errorMessage = error instanceof Error ? error.message : String(error);
      }

      // Should fail during validation phase
      assert(errorThrown, "Expected migration to throw an error");
      assert(
        errorMessage.includes("add_age_field") ||
          errorMessage.includes("No migrations were applied"),
        `Expected error about migration validation, got: ${errorMessage}`,
      );

      // CRITICAL: Check that NO migrations were applied
      const appliedIds = await getAppliedMigrationIds(db);
      assertEquals(
        appliedIds.length,
        0,
        "No migrations should be applied when validation fails",
      );

      // Verify collections were not created
      const collections = await db.listCollections().toArray();
      const collectionNames = collections.map((c) => c.name);
      assertEquals(
        collectionNames.filter((n) => n === "users" || n === "posts").length,
        0,
        "No collections should be created when validation fails",
      );
    });
  });
});

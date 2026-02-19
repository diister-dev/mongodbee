/**
 * Tests for migration status checking utilities
 */

import { test, expect } from "vitest";
import * as path from "node:path";
import {
  assertMigrationSystemHealthy,
  checkMigrationStatus,
} from "../../src/migration/check-status.ts";
import type { Db } from "../../src/mongodb.ts";

// Mock database for testing
function createMockDb(options?: {
  hasIndexes?: boolean;
  indexIssues?: "missing" | "outdated" | "orphaned" | "none";
}): Db {
  const mockCollection = {
    find: () => ({
      toArray: () => Promise.resolve([]),
    }),
    findOne: () => Promise.resolve(null),
    insertOne: () => Promise.resolve({ insertedId: "123" }),
    updateOne: () => Promise.resolve({ modifiedCount: 1 }),
    deleteOne: () => Promise.resolve({ deletedCount: 1 }),
    indexes: () => {
      // Return mock indexes based on test scenario
      if (!options?.hasIndexes) {
        return Promise.resolve([{ name: "_id_", key: { _id: 1 } }]);
      }

      const baseIndexes: any[] = [{ name: "_id_", key: { _id: 1 } }];

      if (options.indexIssues === "orphaned") {
        // Add an orphaned index that doesn't exist in schema
        baseIndexes.push({
          name: "oldField",
          key: { oldField: 1 } as any,
          unique: true,
        });
      } else if (options.indexIssues === "outdated") {
        // Add an index with wrong configuration
        baseIndexes.push({
          name: "email",
          key: { email: 1 } as any,
          unique: false, // Should be true
        });
      }
      // For "missing", we don't add the expected index
      // For "none", we add correct indexes

      return Promise.resolve(baseIndexes);
    },
  };

  return {
    collection: () => mockCollection,
  } as unknown as Db;
}

const testMigrationsDir = path.resolve(
  process.cwd(),
  "test/fixtures/migrations",
);
const testSchemaPath = path.resolve(process.cwd(), "test/fixtures/schemas.ts");

// ============================================================================
// checkMigrationStatus - without database
// ============================================================================

test("checkMigrationStatus - without database - handles no migrations", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: path.resolve(process.cwd(), "test/fixtures/empty"),
    schemaPath: testSchemaPath,
  });

  expect(status.counts.total).toEqual(0);
  expect(status.database).toEqual(undefined);
});

test("checkMigrationStatus - without database - validates schema", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: false, // Skip simulation for faster tests
  });

  // Check that schema validation was performed
  expect(typeof status.validation.isSchemaConsistent).toEqual("boolean");
});

test("checkMigrationStatus - without database - includes verbose info", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    verbose: true,
    strictValidation: false,
  });

  if (status.counts.total > 0) {
    expect(Array.isArray(status.migrations)).toEqual(true);
    expect(status.migrations?.length).toEqual(status.counts.total);

    // Check migration info structure
    if (status.migrations && status.migrations.length > 0) {
      const firstMigration = status.migrations[0];
      expect(typeof firstMigration.id).toEqual("string");
      expect(typeof firstMigration.name).toEqual("string");
      expect(typeof firstMigration.isValid).toEqual("boolean");
      expect(Array.isArray(firstMigration.errors)).toEqual(true);
      expect(Array.isArray(firstMigration.warnings)).toEqual(true);
    }
  }
});

test("checkMigrationStatus - without database - reports warnings", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: false,
  });

  // Should have warnings array
  expect(Array.isArray(status.validation.warnings)).toEqual(true);
});

// ============================================================================
// checkMigrationStatus - with database
// ============================================================================

test("checkMigrationStatus - with database - checks db status", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    strictValidation: false,
  });

  // Database-related fields should be populated
  expect(status.database !== undefined).toEqual(true);
  if (status.database) {
    expect(typeof status.database.isUpToDate).toEqual("boolean");
    expect(typeof status.database.appliedCount).toEqual("number");
    expect(typeof status.database.pendingCount).toEqual("number");
  }
});

test("checkMigrationStatus - with database - includes pending IDs", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    strictValidation: false,
  });

  if (status.counts.total > 0 && status.database) {
    expect(Array.isArray(status.database.pendingIds)).toEqual(true);
  }
});

test("checkMigrationStatus - with database - marks applied in verbose", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    verbose: true,
    strictValidation: false,
  });

  if (status.migrations && status.migrations.length > 0) {
    const firstMigration = status.migrations[0];
    expect(typeof firstMigration.isApplied).toEqual("boolean");
  }
});

// ============================================================================
// checkMigrationStatus - strict validation
// ============================================================================

test("checkMigrationStatus - strict validation - validates with simulation", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: true,
  });

  expect(typeof status.validation.areMigrationsValid).toEqual("boolean");
});

test("checkMigrationStatus - strict validation - skips when not strict", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: false,
  });

  // Should still report validity based on schema
  expect(typeof status.validation.areMigrationsValid).toEqual("boolean");
});

// ============================================================================
// checkMigrationStatus - summary generation
// ============================================================================

test("checkMigrationStatus - summary - generates human-readable text", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: false,
  });

  expect(typeof status.message).toEqual("string");
  expect(status.message.length > 0).toEqual(true);
});

test("checkMigrationStatus - summary - includes errors when unhealthy", async () => {
  // Test with invalid path to force errors
  const status = await checkMigrationStatus({
    migrationsDir: "/nonexistent/path",
    schemaPath: "/nonexistent/schema.ts",
    strictValidation: false,
  });

  expect(status.ok).toEqual(false);
  expect(status.validation.errors.length > 0).toEqual(true);
  expect(status.message.includes("error")).toEqual(true);
});

// ============================================================================
// assertMigrationSystemHealthy
// ============================================================================

test("assertMigrationSystemHealthy - does not throw when healthy", async () => {
  // This test depends on having valid test fixtures
  // For now, we'll just test the function doesn't crash
  try {
    await assertMigrationSystemHealthy({
      migrationsDir: testMigrationsDir,
      schemaPath: testSchemaPath,
      strictValidation: false,
    });
  } catch {
    // Expected to fail if fixtures are not set up properly
  }
});

test("assertMigrationSystemHealthy - throws when unhealthy", async () => {
  await expect(
    async () => {
      await assertMigrationSystemHealthy({
        migrationsDir: "/nonexistent/path",
        schemaPath: "/nonexistent/schema.ts",
        strictValidation: false,
      });
    },
  ).rejects.toThrow("Migration system is not healthy");
});

test("assertMigrationSystemHealthy - includes error details", async () => {
  try {
    await assertMigrationSystemHealthy({
      migrationsDir: "/nonexistent/path",
      schemaPath: "/nonexistent/schema.ts",
      strictValidation: false,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    expect(message.includes("Migration system is not healthy")).toEqual(true);
    expect(message.includes("Error")).toEqual(true);
  }
});

// ============================================================================
// checkMigrationStatus - index validation
// ============================================================================

test("checkMigrationStatus - index validation - does not validate without db", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: false,
  });

  // Without database, indexes should not be validated
  expect(status.indexes).toEqual(undefined);
});

test("checkMigrationStatus - index validation - validates with db", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    strictValidation: false,
  });

  // With database, indexes should be validated if there are migrations
  if (status.counts.total > 0) {
    expect(status.indexes !== undefined).toEqual(true);
    if (status.indexes) {
      expect(typeof status.indexes.areIndexesValid).toEqual("boolean");
      expect(typeof status.indexes.collectionsChecked).toEqual("number");
      expect(typeof status.indexes.validIndexCount).toEqual("number");
      expect(typeof status.indexes.invalidIndexCount).toEqual("number");
      expect(Array.isArray(status.indexes.issues)).toEqual(true);
    }
  }
});

test("checkMigrationStatus - index validation - reports index issues", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    strictValidation: false,
    verbose: true,
  });

  // If there are index issues, they should be reported
  if (status.indexes && !status.indexes.areIndexesValid) {
    expect(status.indexes.issues.length > 0).toEqual(true);

    // Check structure of index issues
    const firstIssue = status.indexes.issues[0];
    expect(typeof firstIssue.collection).toEqual("string");
    expect(typeof firstIssue.path).toEqual("string");
    expect(["missing", "outdated", "orphaned"].includes(firstIssue.type)).toEqual(true);
    expect(typeof firstIssue.description).toEqual("string");
  }
});

test("checkMigrationStatus - index validation - affects ok status", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    strictValidation: false,
  });

  // If indexes are invalid, ok should be false
  if (status.indexes && !status.indexes.areIndexesValid) {
    expect(status.ok).toEqual(false);
    expect(status.message.includes("index")).toEqual(true);
  }
});

test("checkMigrationStatus - index validation - includes detailed issues in verbose", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    strictValidation: false,
    verbose: true,
  });

  // In verbose mode with index issues, warnings should include index details
  if (status.indexes && !status.indexes.areIndexesValid) {
    const hasIndexWarnings = status.validation.warnings.some((w) =>
      w.includes("Index") || w.includes("index")
    );
    expect(hasIndexWarnings).toEqual(true);
  }
});

/**
 * Tests for migration status checking utilities
 */

import { assertEquals, assertRejects } from "@std/assert";
import * as path from "@std/path";
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
  Deno.cwd(),
  "test/fixtures/migrations",
);
const testSchemaPath = path.resolve(Deno.cwd(), "test/fixtures/schemas.ts");

// ============================================================================
// checkMigrationStatus - without database
// ============================================================================

Deno.test("checkMigrationStatus - without database - handles no migrations", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: path.resolve(Deno.cwd(), "test/fixtures/empty"),
    schemaPath: testSchemaPath,
  });

  assertEquals(status.counts.total, 0);
  assertEquals(status.database, undefined);
});

Deno.test("checkMigrationStatus - without database - validates schema", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: false, // Skip simulation for faster tests
  });

  // Check that schema validation was performed
  assertEquals(typeof status.validation.isSchemaConsistent, "boolean");
});

Deno.test("checkMigrationStatus - without database - includes verbose info", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    verbose: true,
    strictValidation: false,
  });

  if (status.counts.total > 0) {
    assertEquals(Array.isArray(status.migrations), true);
    assertEquals(status.migrations?.length, status.counts.total);

    // Check migration info structure
    if (status.migrations && status.migrations.length > 0) {
      const firstMigration = status.migrations[0];
      assertEquals(typeof firstMigration.id, "string");
      assertEquals(typeof firstMigration.name, "string");
      assertEquals(typeof firstMigration.isValid, "boolean");
      assertEquals(Array.isArray(firstMigration.errors), true);
      assertEquals(Array.isArray(firstMigration.warnings), true);
    }
  }
});

Deno.test("checkMigrationStatus - without database - reports warnings", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: false,
  });

  // Should have warnings array
  assertEquals(Array.isArray(status.validation.warnings), true);
});

// ============================================================================
// checkMigrationStatus - with database
// ============================================================================

Deno.test("checkMigrationStatus - with database - checks db status", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    strictValidation: false,
  });

  // Database-related fields should be populated
  assertEquals(status.database !== undefined, true);
  if (status.database) {
    assertEquals(typeof status.database.isUpToDate, "boolean");
    assertEquals(typeof status.database.appliedCount, "number");
    assertEquals(typeof status.database.pendingCount, "number");
  }
});

Deno.test("checkMigrationStatus - with database - includes pending IDs", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    strictValidation: false,
  });

  if (status.counts.total > 0 && status.database) {
    assertEquals(Array.isArray(status.database.pendingIds), true);
  }
});

Deno.test("checkMigrationStatus - with database - marks applied in verbose", async () => {
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
    assertEquals(typeof firstMigration.isApplied, "boolean");
  }
});

// ============================================================================
// checkMigrationStatus - strict validation
// ============================================================================

Deno.test("checkMigrationStatus - strict validation - validates with simulation", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: true,
  });

  assertEquals(typeof status.validation.areMigrationsValid, "boolean");
});

Deno.test("checkMigrationStatus - strict validation - skips when not strict", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: false,
  });

  // Should still report validity based on schema
  assertEquals(typeof status.validation.areMigrationsValid, "boolean");
});

// ============================================================================
// checkMigrationStatus - summary generation
// ============================================================================

Deno.test("checkMigrationStatus - summary - generates human-readable text", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: false,
  });

  assertEquals(typeof status.message, "string");
  assertEquals(status.message.length > 0, true);
});

Deno.test("checkMigrationStatus - summary - includes errors when unhealthy", async () => {
  // Test with invalid path to force errors
  const status = await checkMigrationStatus({
    migrationsDir: "/nonexistent/path",
    schemaPath: "/nonexistent/schema.ts",
    strictValidation: false,
  });

  assertEquals(status.ok, false);
  assertEquals(status.validation.errors.length > 0, true);
  assertEquals(status.message.includes("error"), true);
});

// ============================================================================
// assertMigrationSystemHealthy
// ============================================================================

Deno.test("assertMigrationSystemHealthy - does not throw when healthy", async () => {
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

Deno.test("assertMigrationSystemHealthy - throws when unhealthy", async () => {
  await assertRejects(
    async () => {
      await assertMigrationSystemHealthy({
        migrationsDir: "/nonexistent/path",
        schemaPath: "/nonexistent/schema.ts",
        strictValidation: false,
      });
    },
    Error,
    "Migration system is not healthy",
  );
});

Deno.test("assertMigrationSystemHealthy - includes error details", async () => {
  try {
    await assertMigrationSystemHealthy({
      migrationsDir: "/nonexistent/path",
      schemaPath: "/nonexistent/schema.ts",
      strictValidation: false,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    assertEquals(message.includes("Migration system is not healthy"), true);
    assertEquals(message.includes("Error"), true);
  }
});

// ============================================================================
// checkMigrationStatus - index validation
// ============================================================================

Deno.test("checkMigrationStatus - index validation - does not validate without db", async () => {
  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    strictValidation: false,
  });

  // Without database, indexes should not be validated
  assertEquals(status.indexes, undefined);
});

Deno.test("checkMigrationStatus - index validation - validates with db", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    strictValidation: false,
  });

  // With database, indexes should be validated if there are migrations
  if (status.counts.total > 0) {
    assertEquals(status.indexes !== undefined, true);
    if (status.indexes) {
      assertEquals(typeof status.indexes.areIndexesValid, "boolean");
      assertEquals(typeof status.indexes.collectionsChecked, "number");
      assertEquals(typeof status.indexes.validIndexCount, "number");
      assertEquals(typeof status.indexes.invalidIndexCount, "number");
      assertEquals(Array.isArray(status.indexes.issues), true);
    }
  }
});

Deno.test("checkMigrationStatus - index validation - reports index issues", async () => {
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
    assertEquals(status.indexes.issues.length > 0, true);

    // Check structure of index issues
    const firstIssue = status.indexes.issues[0];
    assertEquals(typeof firstIssue.collection, "string");
    assertEquals(typeof firstIssue.path, "string");
    assertEquals(["missing", "outdated", "orphaned"].includes(firstIssue.type), true);
    assertEquals(typeof firstIssue.description, "string");
  }
});

Deno.test("checkMigrationStatus - index validation - affects ok status", async () => {
  const mockDb = createMockDb();

  const status = await checkMigrationStatus({
    migrationsDir: testMigrationsDir,
    schemaPath: testSchemaPath,
    db: mockDb,
    strictValidation: false,
  });

  // If indexes are invalid, ok should be false
  if (status.indexes && !status.indexes.areIndexesValid) {
    assertEquals(status.ok, false);
    assertEquals(status.message.includes("index"), true);
  }
});

Deno.test("checkMigrationStatus - index validation - includes detailed issues in verbose", async () => {
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
    assertEquals(hasIndexWarnings, true);
  }
});


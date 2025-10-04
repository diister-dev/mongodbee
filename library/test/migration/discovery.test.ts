/**
 * Tests for Migration Discovery
 *
 * Tests discovery, loading, and validation of migration files
 */

import { assert, assertEquals, assertExists } from "@std/assert";
import * as path from "@std/path";
import {
  buildMigrationChain,
  discoverMigrationFiles,
  getPendingMigrations,
  loadAllMigrations,
  loadMigrationFile,
  validateMigrationChain,
} from "../../src/migration/discovery.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";

// Get absolute path to the definition module for dynamic imports in temp files
const DEFINITION_IMPORT_PATH =
  new URL("../../src/migration/definition.ts", import.meta.url).href;

// Helper to create temporary test directory with migrations
async function withTempMigrations(
  work: (tempDir: string) => Promise<void>,
) {
  const tempDir = await Deno.makeTempDir({ prefix: "mongodbee_migrations_" });

  try {
    await work(tempDir);
  } finally {
    await Deno.remove(tempDir, { recursive: true });
  }
}

// Helper to create a migration file
async function createMigrationFile(
  dir: string,
  fileName: string,
  content: string,
): Promise<void> {
  const filePath = path.join(dir, fileName);
  await Deno.writeTextFile(filePath, content);
}

// ============================================================================
// File Discovery Tests
// ============================================================================

Deno.test("discoverMigrationFiles - finds all .ts files", async () => {
  await withTempMigrations(async (tempDir) => {
    // Create test migration files
    await createMigrationFile(tempDir, "001_initial.ts", "export default {}");
    await createMigrationFile(tempDir, "002_add_users.ts", "export default {}");
    await createMigrationFile(tempDir, "003_add_posts.ts", "export default {}");

    // Create non-migration file (should be ignored)
    await createMigrationFile(tempDir, "README.md", "# Migrations");

    const files = await discoverMigrationFiles(tempDir);

    assertEquals(files.length, 3);
    assert(files.includes("001_initial.ts"));
    assert(files.includes("002_add_users.ts"));
    assert(files.includes("003_add_posts.ts"));
    assert(!files.includes("README.md"));
  });
});

Deno.test("discoverMigrationFiles - returns sorted files", async () => {
  await withTempMigrations(async (tempDir) => {
    // Create files in random order
    await createMigrationFile(tempDir, "003_third.ts", "export default {}");
    await createMigrationFile(tempDir, "001_first.ts", "export default {}");
    await createMigrationFile(tempDir, "002_second.ts", "export default {}");

    const files = await discoverMigrationFiles(tempDir);

    assertEquals(files, ["001_first.ts", "002_second.ts", "003_third.ts"]);
  });
});

Deno.test("discoverMigrationFiles - returns empty array for empty directory", async () => {
  await withTempMigrations(async (tempDir) => {
    const files = await discoverMigrationFiles(tempDir);

    assertEquals(files.length, 0);
  });
});

Deno.test("discoverMigrationFiles - throws on non-existent directory", async () => {
  try {
    await discoverMigrationFiles("/nonexistent/directory");
    throw new Error("Should have thrown");
  } catch (error) {
    assert(error instanceof Error);
    assert(error.message.includes("not found"));
  }
});

// ============================================================================
// Migration Loading Tests
// ============================================================================

Deno.test("loadMigrationFile - loads valid migration", async () => {
  await withTempMigrations(async (tempDir) => {
    // Get absolute path to the definition module
    const definitionPath =
      new URL("../../src/migration/definition.ts", import.meta.url).href;

    const migrationContent = `
      import { migrationDefinition } from "${definitionPath}";
      
      const id = "001";
      const name = "Initial";
      
      export default migrationDefinition(id, name, {
        parent: null,
        schemas: {
          collections: {
            users: {}
          }
        },
        migrate: (builder) => builder.compile()
      });
    `;

    await createMigrationFile(tempDir, "001_initial.ts", migrationContent);

    const migration = await loadMigrationFile(tempDir, "001_initial.ts");

    assertExists(migration);
    assertEquals(migration.id, "001");
    assertEquals(migration.name, "Initial");
    assertEquals(typeof migration.migrate, "function");
  });
});

Deno.test("loadMigrationFile - throws on missing default export", async () => {
  await withTempMigrations(async (tempDir) => {
    const migrationContent = `
      // No default export
      export const something = "value";
    `;

    await createMigrationFile(tempDir, "001_bad.ts", migrationContent);

    try {
      await loadMigrationFile(tempDir, "001_bad.ts");
      throw new Error("Should have thrown");
    } catch (error) {
      assert(error instanceof Error);
      assert(error.message.includes("default export"));
    }
  });
});

Deno.test("loadMigrationFile - throws on missing required properties", async () => {
  await withTempMigrations(async (tempDir) => {
    const migrationContent = `
      export default {
        // Missing id, name, and migrate
        schemas: { collections: {} }
      };
    `;

    await createMigrationFile(tempDir, "001_incomplete.ts", migrationContent);

    try {
      await loadMigrationFile(tempDir, "001_incomplete.ts");
      throw new Error("Should have thrown");
    } catch (error) {
      assert(error instanceof Error);
      assert(error.message.includes("missing required properties"));
    }
  });
});

Deno.test("loadMigrationFile - throws on syntax errors", async () => {
  await withTempMigrations(async (tempDir) => {
    const migrationContent = `
      export default {
        // Invalid JavaScript syntax
        this is not valid code
      };
    `;

    await createMigrationFile(tempDir, "001_syntax_error.ts", migrationContent);

    try {
      await loadMigrationFile(tempDir, "001_syntax_error.ts");
      throw new Error("Should have thrown");
    } catch (error) {
      assert(error instanceof Error);
      assert(error.message.includes("Failed to load"));
    }
  });
});

// ============================================================================
// Load All Migrations Tests
// ============================================================================

Deno.test("loadAllMigrations - loads multiple migrations", async () => {
  await withTempMigrations(async (tempDir) => {
    // Create first migration
    const definitionPath =
      new URL("../../src/migration/definition.ts", import.meta.url).href;
    const migration1 = `
      import { migrationDefinition } from "${definitionPath}";
      
      export default migrationDefinition("001", "First", {
        parent: null,
        schemas: { collections: {} },
        migrate: (builder) => builder.compile()
      });
    `;

    // Create second migration
    const migration2 = `
      import { migrationDefinition } from "${definitionPath}";
      
      export default migrationDefinition("002", "Second", {
        parent: null,
        schemas: { collections: {} },
        migrate: (builder) => builder.compile()
      });
    `;

    await createMigrationFile(tempDir, "001_first.ts", migration1);
    await createMigrationFile(tempDir, "002_second.ts", migration2);

    const migrations = await loadAllMigrations(tempDir);

    assertEquals(migrations.length, 2);
    assertEquals(migrations[0].fileName, "001_first.ts");
    assertEquals(migrations[0].migration.id, "001");
    assertEquals(migrations[1].fileName, "002_second.ts");
    assertEquals(migrations[1].migration.id, "002");
  });
});

Deno.test("loadAllMigrations - returns empty array for empty directory", async () => {
  await withTempMigrations(async (tempDir) => {
    const migrations = await loadAllMigrations(tempDir);

    assertEquals(migrations.length, 0);
  });
});

// ============================================================================
// Chain Validation Tests
// ============================================================================

Deno.test("validateMigrationChain - passes for valid chain", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m2 = migrationDefinition("002", "Second", {
    parent: m1,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const errors = validateMigrationChain([m1, m2]);

  assertEquals(errors.length, 0);
});

Deno.test("validateMigrationChain - detects first migration with parent", () => {
  const schemas = {
    collections: {},
  };

  const dummy = migrationDefinition("000", "Dummy", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m1 = migrationDefinition("001", "First", {
    parent: dummy,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const errors = validateMigrationChain([m1]);

  assert(errors.length > 0);
  assert(errors.some((e) => e.includes("should not have a parent")));
});

Deno.test("validateMigrationChain - detects incorrect parent", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m2 = migrationDefinition("002", "Second", {
    parent: null, // Wrong - should be m1
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const errors = validateMigrationChain([m1, m2]);

  assert(errors.length > 0);
  assert(errors.some((e) => e.includes("incorrect parent")));
});

Deno.test("validateMigrationChain - detects duplicate IDs", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m2 = migrationDefinition("001", "Second", {
    parent: m1,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const errors = validateMigrationChain([m1, m2]);

  assert(errors.length > 0);
  assert(errors.some((e) => e.includes("Duplicate")));
});

Deno.test("validateMigrationChain - returns empty for empty array", () => {
  const errors = validateMigrationChain([]);

  assertEquals(errors.length, 0);
});

// ============================================================================
// Build Migration Chain Tests
// ============================================================================

Deno.test("buildMigrationChain - builds valid chain from files", async () => {
  await withTempMigrations(async (tempDir) => {
    // Create migrations with proper parent-child relationship
    const definitionPath =
      new URL("../../src/migration/definition.ts", import.meta.url).href;
    const migration1 = `
      import { migrationDefinition } from "${definitionPath}";
      
      export default migrationDefinition("001", "First", {
        parent: null,
        schemas: { collections: {} },
        migrate: (builder) => builder.compile()
      });
    `;

    // Note: In real scenario, migration2 would import migration1 as parent
    // For this test, we'll create independent migrations
    const migration2 = `
      import { migrationDefinition } from "${definitionPath}";
      
      const parent = { id: "001", name: "First", schemas: { collections: {} }, parent: null, migrate: () => ({}) };
      
      export default migrationDefinition("002", "Second", {
        parent: parent,
        schemas: { collections: {} },
        migrate: (builder) => builder.compile()
      });
    `;

    await createMigrationFile(tempDir, "001_first.ts", migration1);
    await createMigrationFile(tempDir, "002_second.ts", migration2);

    const loaded = await loadAllMigrations(tempDir);
    const chain = buildMigrationChain(loaded);

    assertEquals(chain.length, 2);
    assertEquals(chain[0].id, "001");
    assertEquals(chain[1].id, "002");
  });
});

Deno.test("buildMigrationChain - sorts files correctly", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m2 = migrationDefinition("002", "Second", {
    parent: m1,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m3 = migrationDefinition("003", "Third", {
    parent: m2,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  // Load in random order
  const migrations = [
    { fileName: "003_third.ts", migration: m3 },
    { fileName: "001_first.ts", migration: m1 },
    { fileName: "002_second.ts", migration: m2 },
  ];

  const chain = buildMigrationChain(migrations);

  assertEquals(chain.length, 3);
  assertEquals(chain[0].id, "001");
  assertEquals(chain[1].id, "002");
  assertEquals(chain[2].id, "003");
});

Deno.test("buildMigrationChain - throws on invalid chain", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m2 = migrationDefinition("002", "Second", {
    parent: null, // Invalid - should reference m1
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const migrations = [
    { fileName: "001_first.ts", migration: m1 },
    { fileName: "002_second.ts", migration: m2 },
  ];

  try {
    buildMigrationChain(migrations);
    throw new Error("Should have thrown");
  } catch (error) {
    assert(error instanceof Error);
    assert(error.message.includes("validation failed"));
  }
});

// ============================================================================
// Pending Migrations Tests
// ============================================================================

Deno.test("getPendingMigrations - returns unapplied migrations", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m2 = migrationDefinition("002", "Second", {
    parent: m1,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m3 = migrationDefinition("003", "Third", {
    parent: m2,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const allMigrations = [m1, m2, m3];
  const appliedIds = ["001"]; // Only first migration applied

  const pending = getPendingMigrations(allMigrations, appliedIds);

  assertEquals(pending.length, 2);
  assertEquals(pending[0].id, "002");
  assertEquals(pending[1].id, "003");
});

Deno.test("getPendingMigrations - returns empty when all applied", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m2 = migrationDefinition("002", "Second", {
    parent: m1,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const allMigrations = [m1, m2];
  const appliedIds = ["001", "002"]; // All applied

  const pending = getPendingMigrations(allMigrations, appliedIds);

  assertEquals(pending.length, 0);
});

Deno.test("getPendingMigrations - returns all when none applied", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m2 = migrationDefinition("002", "Second", {
    parent: m1,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const allMigrations = [m1, m2];
  const appliedIds: string[] = []; // None applied

  const pending = getPendingMigrations(allMigrations, appliedIds);

  assertEquals(pending.length, 2);
  assertEquals(pending[0].id, "001");
  assertEquals(pending[1].id, "002");
});

// ============================================================================
// Integration Tests
// ============================================================================

Deno.test("Discovery - full workflow from files to chain", async () => {
  await withTempMigrations(async (tempDir) => {
    // Create a complete migration chain
    const migration1 = `
      import { migrationDefinition } from "${DEFINITION_IMPORT_PATH}";
      
      export default migrationDefinition("2025_01_01_0000_AAA@initial", "Initial", {
        parent: null,
        schemas: { collections: { users: {} } },
        migrate: (builder) => builder.createCollection("users").done().compile()
      });
    `;

    const migration2 = `
      import { migrationDefinition } from "${DEFINITION_IMPORT_PATH}";
      
      const parent = {
        id: "2025_01_01_0000_AAA@initial",
        name: "Initial",
        schemas: { collections: { users: {} } },
        parent: null,
        migrate: () => ({})
      };
      
      export default migrationDefinition("2025_01_02_0000_BBB@add_email", "Add Email", {
        parent: parent,
        schemas: { collections: { users: { email: {} } } },
        migrate: (builder) => builder.compile()
      });
    `;

    await createMigrationFile(
      tempDir,
      "2025_01_01_0000_AAA@initial.ts",
      migration1,
    );
    await createMigrationFile(
      tempDir,
      "2025_01_02_0000_BBB@add_email.ts",
      migration2,
    );

    // 1. Discover files
    const files = await discoverMigrationFiles(tempDir);
    assertEquals(files.length, 2);

    // 2. Load migrations
    const loaded = await loadAllMigrations(tempDir);
    assertEquals(loaded.length, 2);

    // 3. Build chain
    const chain = buildMigrationChain(loaded);
    assertEquals(chain.length, 2);

    // 4. Get pending migrations (none applied yet)
    const pending = getPendingMigrations(chain, []);
    assertEquals(pending.length, 2);

    // 5. Simulate applying first migration
    const pendingAfterOne = getPendingMigrations(chain, [chain[0].id]);
    assertEquals(pendingAfterOne.length, 1);
    assertEquals(pendingAfterOne[0].id, "2025_01_02_0000_BBB@add_email");
  });
});

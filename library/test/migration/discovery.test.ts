/**
 * Tests for Migration Discovery
 *
 * Tests discovery, loading, and validation of migration files
 */

import { test, expect } from "vitest";
import * as path from "node:path";
import * as fsp from "node:fs/promises";
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
  const os = await import("node:os");
  const tempDir = await fsp.mkdtemp(path.join(os.tmpdir(), "mongodbee_migrations_"));

  try {
    await work(tempDir);
  } finally {
    await fsp.rm(tempDir, { recursive: true });
  }
}

// Helper to create a migration file
async function createMigrationFile(
  dir: string,
  fileName: string,
  content: string,
): Promise<void> {
  const filePath = path.join(dir, fileName);
  await fsp.writeFile(filePath, content, "utf-8");
}

// ============================================================================
// File Discovery Tests
// ============================================================================

test("discoverMigrationFiles - finds all .ts files", async () => {
  await withTempMigrations(async (tempDir) => {
    // Create test migration files
    await createMigrationFile(tempDir, "001_initial.ts", "export default {}");
    await createMigrationFile(tempDir, "002_add_users.ts", "export default {}");
    await createMigrationFile(tempDir, "003_add_posts.ts", "export default {}");

    // Create non-migration file (should be ignored)
    await createMigrationFile(tempDir, "README.md", "# Migrations");

    const files = await discoverMigrationFiles(tempDir);

    expect(files.length).toEqual(3);
    expect(files.includes("001_initial.ts")).toBeTruthy();
    expect(files.includes("002_add_users.ts")).toBeTruthy();
    expect(files.includes("003_add_posts.ts")).toBeTruthy();
    expect(!files.includes("README.md")).toBeTruthy();
  });
});

test("discoverMigrationFiles - returns sorted files", async () => {
  await withTempMigrations(async (tempDir) => {
    // Create files in random order
    await createMigrationFile(tempDir, "003_third.ts", "export default {}");
    await createMigrationFile(tempDir, "001_first.ts", "export default {}");
    await createMigrationFile(tempDir, "002_second.ts", "export default {}");

    const files = await discoverMigrationFiles(tempDir);

    expect(files).toEqual(["001_first.ts", "002_second.ts", "003_third.ts"]);
  });
});

test("discoverMigrationFiles - returns empty array for empty directory", async () => {
  await withTempMigrations(async (tempDir) => {
    const files = await discoverMigrationFiles(tempDir);

    expect(files.length).toEqual(0);
  });
});

test("discoverMigrationFiles - throws on non-existent directory", async () => {
  try {
    await discoverMigrationFiles("/nonexistent/directory");
    throw new Error("Should have thrown");
  } catch (error) {
    expect(error instanceof Error).toBeTruthy();
    expect(error.message.includes("not found")).toBeTruthy();
  }
});

// ============================================================================
// Migration Loading Tests
// ============================================================================

test("loadMigrationFile - loads valid migration", async () => {
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

    expect(migration).toBeDefined();
    expect(migration.id).toEqual("001");
    expect(migration.name).toEqual("Initial");
    expect(typeof migration.migrate).toEqual("function");
  });
});

test("loadMigrationFile - throws on missing default export", async () => {
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
      expect(error instanceof Error).toBeTruthy();
      expect(error.message.includes("default export")).toBeTruthy();
    }
  });
});

test("loadMigrationFile - throws on missing required properties", async () => {
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
      expect(error instanceof Error).toBeTruthy();
      expect(error.message.includes("missing required properties")).toBeTruthy();
    }
  });
});

test("loadMigrationFile - throws on syntax errors", async () => {
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
      expect(error instanceof Error).toBeTruthy();
      expect(error.message.includes("Failed to load")).toBeTruthy();
    }
  });
});

// ============================================================================
// Load All Migrations Tests
// ============================================================================

test("loadAllMigrations - loads multiple migrations", async () => {
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

    expect(migrations.length).toEqual(2);
    expect(migrations[0].fileName).toEqual("001_first.ts");
    expect(migrations[0].migration.id).toEqual("001");
    expect(migrations[1].fileName).toEqual("002_second.ts");
    expect(migrations[1].migration.id).toEqual("002");
  });
});

test("loadAllMigrations - returns empty array for empty directory", async () => {
  await withTempMigrations(async (tempDir) => {
    const migrations = await loadAllMigrations(tempDir);

    expect(migrations.length).toEqual(0);
  });
});

// ============================================================================
// Chain Validation Tests
// ============================================================================

test("validateMigrationChain - passes for valid chain", () => {
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

  expect(errors.length).toEqual(0);
});

test("validateMigrationChain - detects first migration with parent", () => {
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

  expect(errors.length > 0).toBeTruthy();
  expect(errors.some((e) => e.includes("should not have a parent"))).toBeTruthy();
});

test("validateMigrationChain - detects incorrect parent", () => {
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

  expect(errors.length > 0).toBeTruthy();
  expect(errors.some((e) => e.includes("incorrect parent"))).toBeTruthy();
});

test("validateMigrationChain - detects duplicate IDs", () => {
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

  expect(errors.length > 0).toBeTruthy();
  expect(errors.some((e) => e.includes("Duplicate"))).toBeTruthy();
});

test("validateMigrationChain - returns empty for empty array", () => {
  const errors = validateMigrationChain([]);

  expect(errors.length).toEqual(0);
});

// ============================================================================
// Build Migration Chain Tests
// ============================================================================

test("buildMigrationChain - builds valid chain from files", async () => {
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

    expect(chain.length).toEqual(2);
    expect(chain[0].id).toEqual("001");
    expect(chain[1].id).toEqual("002");
  });
});

test("buildMigrationChain - sorts files correctly", () => {
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

  expect(chain.length).toEqual(3);
  expect(chain[0].id).toEqual("001");
  expect(chain[1].id).toEqual("002");
  expect(chain[2].id).toEqual("003");
});

test("buildMigrationChain - throws on invalid chain", () => {
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
    expect(error instanceof Error).toBeTruthy();
    expect(error.message.includes("validation failed")).toBeTruthy();
  }
});

// ============================================================================
// Pending Migrations Tests
// ============================================================================

test("getPendingMigrations - returns unapplied migrations", () => {
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

  expect(pending.length).toEqual(2);
  expect(pending[0].id).toEqual("002");
  expect(pending[1].id).toEqual("003");
});

test("getPendingMigrations - returns empty when all applied", () => {
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

  expect(pending.length).toEqual(0);
});

test("getPendingMigrations - returns all when none applied", () => {
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

  expect(pending.length).toEqual(2);
  expect(pending[0].id).toEqual("001");
  expect(pending[1].id).toEqual("002");
});

// ============================================================================
// Integration Tests
// ============================================================================

test("Discovery - full workflow from files to chain", async () => {
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
    expect(files.length).toEqual(2);

    // 2. Load migrations
    const loaded = await loadAllMigrations(tempDir);
    expect(loaded.length).toEqual(2);

    // 3. Build chain
    const chain = buildMigrationChain(loaded);
    expect(chain.length).toEqual(2);

    // 4. Get pending migrations (none applied yet)
    const pending = getPendingMigrations(chain, []);
    expect(pending.length).toEqual(2);

    // 5. Simulate applying first migration
    const pendingAfterOne = getPendingMigrations(chain, [chain[0].id]);
    expect(pendingAfterOne.length).toEqual(1);
    expect(pendingAfterOne[0].id).toEqual("2025_01_02_0000_BBB@add_email");
  });
});

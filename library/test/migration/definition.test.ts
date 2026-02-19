/*
 * Tests migration definition, parent-child relationships, and chain validation
 */

import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import {
  createMigrationSummary,
  findCommonAncestor,
  generateMigrationId,
  getMigrationAncestors,
  getMigrationPath,
  isMigrationAncestor,
  migrationDefinition,
  validateMigrationChain,
} from "../../src/migration/definition.ts";

// ============================================================================
// Migration Definition Tests
// ============================================================================

test("migrationDefinition - creates valid migration", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
    },
  };

  const migration = migrationDefinition("001", "Initial", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  expect(migration.id).toEqual("001");
  expect(migration.name).toEqual("Initial");
  expect(migration.parent).toEqual(null);
  expect(migration.schemas).toBeDefined();
  expect(typeof migration.migrate).toEqual("function");
});

test("migrationDefinition - throws on invalid ID", () => {
  const schemas = {
    collections: {},
  };

  try {
    migrationDefinition("", "Test", {
      parent: null,
      schemas,
      migrate: (builder) => builder.compile(),
    });
    throw new Error("Should have thrown");
  } catch (error) {
    expect(error instanceof Error).toBeTruthy();
    expect(error.message.includes("ID must be")).toBeTruthy();
  }
});

test("migrationDefinition - throws on missing schemas", () => {
  try {
    migrationDefinition("001", "Test", {
      parent: null,
      schemas: {} as never,
      migrate: (builder) => builder.compile(),
    });
    throw new Error("Should have thrown");
  } catch (error) {
    expect(error instanceof Error).toBeTruthy();
    expect(error.message.includes("collections schema")).toBeTruthy();
  }
});

// ============================================================================
// Parent-Child Relationship Tests
// ============================================================================

test("Migration chain - creates parent-child relationship", () => {
  const schemas1 = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
    },
  };

  const migration1 = migrationDefinition("001", "Create users", {
    parent: null,
    schemas: schemas1,
    migrate: (builder) => builder.createCollection("users").end().compile(),
  });

  const schemas2 = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
        email: v.string(),
      },
    },
  };

  const migration2 = migrationDefinition("002", "Add email", {
    parent: migration1,
    schemas: schemas2,
    migrate: (builder) => builder.compile(),
  });

  expect(migration2.parent).toEqual(migration1);
  expect(migration2.parent?.id).toEqual("001");
});

// ============================================================================
// Chain Validation Tests
// ============================================================================

test("validateMigrationChain - passes for valid chain", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
    },
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

  const result = validateMigrationChain([m1, m2, m3]);

  expect(result.valid).toBeTruthy();
  expect(result.errors.length).toEqual(0);
});

test("validateMigrationChain - fails when first migration has parent", () => {
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

  const result = validateMigrationChain([m1]);

  expect(!result.valid).toBeTruthy();
  expect(result.errors.some((e) => e.includes("should have no parent"))).toBeTruthy();
});

test("validateMigrationChain - fails on incorrect parent reference", () => {
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

  const result = validateMigrationChain([m1, m2]);

  expect(!result.valid).toBeTruthy();
  // m2 should have m1 as parent but has null, so validation should fail
  expect(result.errors.some((e) => e.includes("parent"))).toBeTruthy();
});

test("validateMigrationChain - fails on duplicate IDs", () => {
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

  const result = validateMigrationChain([m1, m2]);

  expect(!result.valid).toBeTruthy();
  expect(result.errors.some((e) => e.includes("Duplicate"))).toBeTruthy();
});

// ============================================================================
// Migration ID Generation Tests
// ============================================================================

test("generateMigrationId - creates unique IDs", () => {
  const id1 = generateMigrationId("test");
  const id2 = generateMigrationId("test");

  // IDs should be different
  expect(id1 !== id2).toBeTruthy();
});

test("generateMigrationId - includes name in ID", () => {
  const id = generateMigrationId("create_users");

  expect(id.includes("@create_users")).toBeTruthy();
});

test("generateMigrationId - has correct format", () => {
  const id = generateMigrationId("test");

  // Format: YYYY_MM_DD_HHMM_ULID@name
  expect(id.includes("@test")).toBeTruthy();

  const parts = id.split("@");
  expect(parts.length).toEqual(2);

  // Date part should have underscores
  const datePart = parts[0];
  expect(datePart.includes("_")).toBeTruthy();
});

test("generateMigrationId - generates sortable IDs", () => {
  const ids: string[] = [];

  for (let i = 0; i < 5; i++) {
    ids.push(generateMigrationId("test"));
    // Small delay to ensure different timestamps
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1);
  }

  const sorted = [...ids].sort();

  // IDs should sort in order of creation
  expect(ids).toEqual(sorted);
});

// ============================================================================
// Migration Ancestry Tests
// ============================================================================

test("getMigrationAncestors - returns empty for root migration", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const ancestors = getMigrationAncestors(m1);

  expect(ancestors.length).toEqual(0);
});

test("getMigrationAncestors - returns all ancestors in order", () => {
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

  const ancestors = getMigrationAncestors(m3);

  expect(ancestors.length).toEqual(2);
  expect(ancestors[0].id).toEqual("001");
  expect(ancestors[1].id).toEqual("002");
});

test("getMigrationPath - includes migration itself", () => {
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

  const path = getMigrationPath(m2);

  expect(path.length).toEqual(2);
  expect(path[0].id).toEqual("001");
  expect(path[1].id).toEqual("002");
});

test("findCommonAncestor - finds common ancestor", () => {
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

  const m4 = migrationDefinition("004", "Fourth", {
    parent: m2,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const common = findCommonAncestor(m3, m4);

  expect(common).toBeDefined();
  expect(common?.id).toEqual("002");
});

test("findCommonAncestor - returns null for unrelated migrations", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const m2 = migrationDefinition("002", "Second", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const common = findCommonAncestor(m1, m2);

  expect(common).toEqual(null);
});

test("isMigrationAncestor - returns true for ancestor", () => {
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

  expect(isMigrationAncestor(m1, m3)).toBeTruthy();
  expect(isMigrationAncestor(m2, m3)).toBeTruthy();
  expect(!isMigrationAncestor(m3, m1)).toBeTruthy();
});

// ============================================================================
// Migration Summary Tests
// ============================================================================

test("createMigrationSummary - returns correct metadata", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
    },
    multiModels: {
      catalog: {
        product: {
          _id: v.string(),
          name: v.string(),
        },
      },
    },
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

  const summary = createMigrationSummary(m2);

  expect(summary.id).toEqual("002");
  expect(summary.name).toEqual("Second");
  expect(summary.depth).toEqual(1);
  expect(summary.hasParent).toBeTruthy();
  expect(summary.parentId).toEqual("001");
  expect(summary.ancestorCount).toEqual(1);
  expect(summary.collectionCount).toEqual(1);
  expect(summary.multiCollectionCount).toEqual(1);
});

test("createMigrationSummary - handles root migration", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const summary = createMigrationSummary(m1);

  expect(summary.depth).toEqual(0);
  expect(!summary.hasParent).toBeTruthy();
  expect(summary.parentId).toEqual(null);
  expect(summary.ancestorCount).toEqual(0);
});

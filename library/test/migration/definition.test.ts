/*
 * Tests migration definition, parent-child relationships, and chain validation
 */

import * as v from "../../src/schema.ts";
import { assert, assertEquals, assertExists } from "@std/assert";
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

Deno.test("migrationDefinition - creates valid migration", () => {
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

  assertEquals(migration.id, "001");
  assertEquals(migration.name, "Initial");
  assertEquals(migration.parent, null);
  assertExists(migration.schemas);
  assertEquals(typeof migration.migrate, "function");
});

Deno.test("migrationDefinition - throws on invalid ID", () => {
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
    assert(error instanceof Error);
    assert(error.message.includes("ID must be"));
  }
});

Deno.test("migrationDefinition - throws on missing schemas", () => {
  try {
    migrationDefinition("001", "Test", {
      parent: null,
      schemas: {} as never,
      migrate: (builder) => builder.compile(),
    });
    throw new Error("Should have thrown");
  } catch (error) {
    assert(error instanceof Error);
    assert(error.message.includes("collections schema"));
  }
});

// ============================================================================
// Parent-Child Relationship Tests
// ============================================================================

Deno.test("Migration chain - creates parent-child relationship", () => {
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
    migrate: (builder) => builder.createCollection("users").done().compile(),
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

  assertEquals(migration2.parent, migration1);
  assertEquals(migration2.parent?.id, "001");
});

// ============================================================================
// Chain Validation Tests
// ============================================================================

Deno.test("validateMigrationChain - passes for valid chain", () => {
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

  assert(result.valid);
  assertEquals(result.errors.length, 0);
});

Deno.test("validateMigrationChain - fails when first migration has parent", () => {
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

  assert(!result.valid);
  assert(result.errors.some((e) => e.includes("should have no parent")));
});

Deno.test("validateMigrationChain - fails on incorrect parent reference", () => {
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

  assert(!result.valid);
  // m2 should have m1 as parent but has null, so validation should fail
  assert(result.errors.some((e) => e.includes("parent")));
});

Deno.test("validateMigrationChain - fails on duplicate IDs", () => {
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

  assert(!result.valid);
  assert(result.errors.some((e) => e.includes("Duplicate")));
});

// ============================================================================
// Migration ID Generation Tests
// ============================================================================

Deno.test("generateMigrationId - creates unique IDs", () => {
  const id1 = generateMigrationId("test");
  const id2 = generateMigrationId("test");

  // IDs should be different
  assert(id1 !== id2);
});

Deno.test("generateMigrationId - includes name in ID", () => {
  const id = generateMigrationId("create_users");

  assert(id.includes("@create_users"));
});

Deno.test("generateMigrationId - has correct format", () => {
  const id = generateMigrationId("test");

  // Format: YYYY_MM_DD_HHMM_ULID@name
  assert(id.includes("@test"));

  const parts = id.split("@");
  assertEquals(parts.length, 2);

  // Date part should have underscores
  const datePart = parts[0];
  assert(datePart.includes("_"));
});

Deno.test("generateMigrationId - generates sortable IDs", () => {
  const ids: string[] = [];

  for (let i = 0; i < 5; i++) {
    ids.push(generateMigrationId("test"));
    // Small delay to ensure different timestamps
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1);
  }

  const sorted = [...ids].sort();

  // IDs should sort in order of creation
  assertEquals(ids, sorted);
});

// ============================================================================
// Migration Ancestry Tests
// ============================================================================

Deno.test("getMigrationAncestors - returns empty for root migration", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const ancestors = getMigrationAncestors(m1);

  assertEquals(ancestors.length, 0);
});

Deno.test("getMigrationAncestors - returns all ancestors in order", () => {
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

  assertEquals(ancestors.length, 2);
  assertEquals(ancestors[0].id, "001");
  assertEquals(ancestors[1].id, "002");
});

Deno.test("getMigrationPath - includes migration itself", () => {
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

  assertEquals(path.length, 2);
  assertEquals(path[0].id, "001");
  assertEquals(path[1].id, "002");
});

Deno.test("findCommonAncestor - finds common ancestor", () => {
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

  assertExists(common);
  assertEquals(common?.id, "002");
});

Deno.test("findCommonAncestor - returns null for unrelated migrations", () => {
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

  assertEquals(common, null);
});

Deno.test("isMigrationAncestor - returns true for ancestor", () => {
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

  assert(isMigrationAncestor(m1, m3));
  assert(isMigrationAncestor(m2, m3));
  assert(!isMigrationAncestor(m3, m1));
});

// ============================================================================
// Migration Summary Tests
// ============================================================================

Deno.test("createMigrationSummary - returns correct metadata", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
    },
    multiCollections: {
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

  assertEquals(summary.id, "002");
  assertEquals(summary.name, "Second");
  assertEquals(summary.depth, 1);
  assert(summary.hasParent);
  assertEquals(summary.parentId, "001");
  assertEquals(summary.ancestorCount, 1);
  assertEquals(summary.collectionCount, 1);
  assertEquals(summary.multiCollectionCount, 1);
});

Deno.test("createMigrationSummary - handles root migration", () => {
  const schemas = {
    collections: {},
  };

  const m1 = migrationDefinition("001", "First", {
    parent: null,
    schemas,
    migrate: (builder) => builder.compile(),
  });

  const summary = createMigrationSummary(m1);

  assertEquals(summary.depth, 0);
  assert(!summary.hasParent);
  assertEquals(summary.parentId, null);
  assertEquals(summary.ancestorCount, 0);
});

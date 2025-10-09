/**
 * Tests for Migration Builder
 *
 * Tests the migration builder API for creating migrations
 */

import * as v from "../../src/schema.ts";
import { assert, assertEquals, assertExists } from "@std/assert";
import {
  getMigrationSummary,
  isCreateCollectionRule,
  isSeedCollectionRule,
  isTransformCollectionRule,
  migrationBuilder,
} from "../../src/migration/builder.ts";

// ============================================================================
// Basic Builder Tests
// ============================================================================

Deno.test("MigrationBuilder - compile returns migration state", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
    },
  };

  const state = migrationBuilder({ schemas }).compile();

  assertExists(state);
  assertExists(state.operations);
  assertExists(state.properties);
  assertEquals(typeof state.mark, "function");
  assertEquals(typeof state.hasProperty, "function");
});

Deno.test("MigrationBuilder - createCollection adds create operation", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .createCollection("users")
    .done()
    .compile();

  assertEquals(state.operations.length, 1);
  assert(isCreateCollectionRule(state.operations[0]));
  assertEquals(state.operations[0].collectionName, "users");
});

Deno.test("MigrationBuilder - createCollection marks as lossy", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .createCollection("users")
    .done()
    .compile();

  assert(state.hasProperty("lossy"));
});

Deno.test("MigrationBuilder - seed adds seed operation", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
    },
  };

  const documents = [
    { _id: "1", name: "Alice" },
    { _id: "2", name: "Bob" },
  ];

  const state = migrationBuilder({ schemas })
    .createCollection("users")
    .seed(documents)
    .done()
    .compile();

  assertEquals(state.operations.length, 2);
  assert(isSeedCollectionRule(state.operations[1]));
  assertEquals(state.operations[1].collectionName, "users");
  assertEquals(state.operations[1].documents.length, 2);
});

Deno.test("MigrationBuilder - transform adds transform operation", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
        age: v.optional(v.number()),
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .collection("users")
    .transform({
      up: (doc) => ({ ...doc, age: 25 }),
      down: (doc) => {
        const { age: _age, ...rest } = doc as Record<string, unknown>;
        return rest;
      },
    })
    .done()
    .compile();

  assertEquals(state.operations.length, 1);
  assert(isTransformCollectionRule(state.operations[0]));
  assertEquals(state.operations[0].collectionName, "users");
});

Deno.test("MigrationBuilder - chain multiple operations", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
      posts: {
        _id: v.string(),
        title: v.string(),
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .createCollection("users")
    .seed([{ _id: "1", name: "Alice" }])
    .done()
    .createCollection("posts")
    .done()
    .compile();

  assertEquals(state.operations.length, 3);
  assertEquals(state.operations[0].type, "create_collection");
  assertEquals(state.operations[1].type, "seed_collection");
  assertEquals(state.operations[2].type, "create_collection");
});

// ============================================================================
// Multi-Collection Builder Tests
// ============================================================================

Deno.test("MigrationBuilder - newMultiCollection creates instance", () => {
  const schemas = {
    collections: {},
    multiModels: {
      catalog: {
        product: {
          _id: v.string(),
          name: v.string(),
        },
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .newMultiCollection("catalog_main", "catalog")
    .end()
    .compile();

  assertEquals(state.operations.length, 1);
  assertEquals(state.operations[0].type, "create_multicollection_instance");
  const op0 = state.operations[0] as {
    collectionName?: string;
    collectionType?: string;
  };
  assertEquals(op0.collectionName, "catalog_main");
  assertEquals(op0.collectionType, "catalog");
});

Deno.test("MigrationBuilder - seedType adds seed for multi-collection", () => {
  const schemas = {
    collections: {},
    multiModels: {
      catalog: {
        product: {
          _id: v.string(),
          name: v.string(),
        },
      },
    },
  };

  const products = [
    { _id: "p1", name: "Product 1" },
    { _id: "p2", name: "Product 2" },
  ];

  const state = migrationBuilder({ schemas })
    .newMultiCollection("catalog_main", "catalog")
    .seedType("product", products)
    .end()
    .compile();

  assertEquals(state.operations.length, 2);
  assertEquals(state.operations[1].type, "seed_multicollection_instance");
  const op1 = state.operations[1] as {
    collectionName?: string;
    typeName?: string;
  };
  assertEquals(op1.collectionName, "catalog_main");
  assertEquals(op1.typeName, "product");
});

Deno.test("MigrationBuilder - multiCollection type transform", () => {
  const schemas = {
    collections: {},
    multiModels: {
      catalog: {
        product: {
          _id: v.string(),
          name: v.string(),
          price: v.optional(v.number()),
        },
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .multiCollection("catalog")
    .type("product")
    .transform({
      up: (doc) => ({ ...doc, price: 0 }),
      down: (doc) => {
        const { price: _price, ...rest } = doc as Record<string, unknown>;
        return rest;
      },
    })
    .end()
    .end()
    .compile();

  assertEquals(state.operations.length, 1);
  assertEquals(state.operations[0].type, "transform_multicollection_type");
  const op0 = state.operations[0] as {
    collectionType?: string;
    typeName?: string;
  };
  assertEquals(op0.collectionType, "catalog");
  assertEquals(op0.typeName, "product");
});

// ============================================================================
// Schema Validation Tests
// ============================================================================

Deno.test("MigrationBuilder - seed validates documents against schema", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
        email: v.pipe(v.string(), v.email()),
      },
    },
  };

  // Valid document
  const validDoc = { _id: "1", name: "Alice", email: "alice@example.com" };

  const state = migrationBuilder({ schemas })
    .createCollection("users")
    .seed([validDoc])
    .done()
    .compile();

  assertEquals(state.operations.length, 2);

  // Invalid document should throw
  const invalidDoc = { _id: "1", name: "Alice", email: "not-an-email" };

  try {
    migrationBuilder({ schemas })
      .createCollection("users")
      .seed([invalidDoc])
      .done()
      .compile();

    throw new Error("Should have thrown validation error");
  } catch (error) {
    assert(error instanceof Error);
    assert(error.message.includes("schema validation"));
  }
});

// ============================================================================
// Update Indexes Tests
// ============================================================================

Deno.test("MigrationBuilder - updateIndexes adds update operation", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        email: v.string(),
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .updateIndexes("users")
    .compile();

  assertEquals(state.operations.length, 1);
  assertEquals(state.operations[0].type, "update_indexes");
  const op0 = state.operations[0] as { collectionName?: string };
  assertEquals(op0.collectionName, "users");
});

Deno.test("MigrationBuilder - updateIndexes throws if collection schema not found", () => {
  const schemas = {
    collections: {},
  };

  try {
    migrationBuilder({ schemas })
      .updateIndexes("users")
      .compile();

    throw new Error("Should have thrown error");
  } catch (error) {
    assert(error instanceof Error);
    assert(error.message.includes("schema not found"));
  }
});

// ============================================================================
// Migration Summary Tests
// ============================================================================

Deno.test("getMigrationSummary - returns correct counts", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
      posts: {
        _id: v.string(),
        title: v.string(),
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .createCollection("users")
    .seed([{ _id: "1", name: "Alice" }])
    .done()
    .collection("posts")
    .transform({
      up: (doc) => doc,
      down: (doc) => doc,
    })
    .done()
    .compile();

  const summary = getMigrationSummary(state);

  assertEquals(summary.creates, 1);
  assertEquals(summary.seeds, 1);
  assertEquals(summary.transforms, 1);
  assertEquals(summary.totalOperations, 3);
  // createCollection now marks as lossy, not irreversible
  assert(!summary.isIrreversible);
  assert(summary.properties.includes("lossy"));
});

Deno.test("getMigrationSummary - shows reversible when no create operations", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .collection("users")
    .seed([{ _id: "1", name: "Alice" }])
    .done()
    .compile();

  const summary = getMigrationSummary(state);

  assertEquals(summary.creates, 0);
  assertEquals(summary.seeds, 1);
  assertEquals(summary.isIrreversible, false);
});

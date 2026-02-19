/**
 * Tests for Migration Builder
 *
 * Tests the migration builder API for creating migrations
 */

import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
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

test("MigrationBuilder - compile returns migration state", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
        name: v.string(),
      },
    },
  };

  const state = migrationBuilder({ schemas }).compile();

  expect(state).toBeDefined();
  expect(state.operations).toBeDefined();
  expect(state.properties).toBeDefined();
  expect(typeof state.mark).toEqual("function");
  expect(typeof state.hasProperty).toEqual("function");
});

test("MigrationBuilder - createCollection adds create operation", () => {
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
    .end()
    .compile();

  expect(state.operations.length).toEqual(1);
  expect(isCreateCollectionRule(state.operations[0])).toBeTruthy();
  expect(state.operations[0].collectionName).toEqual("users");
});

test("MigrationBuilder - createCollection marks as lossy", () => {
  const schemas = {
    collections: {
      users: {
        _id: v.string(),
      },
    },
  };

  const state = migrationBuilder({ schemas })
    .createCollection("users")
    .end()
    .compile();

  expect(state.hasProperty("lossy")).toBeTruthy();
});

test("MigrationBuilder - seed adds seed operation", () => {
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
    .end()
    .compile();

  expect(state.operations.length).toEqual(2);
  expect(isSeedCollectionRule(state.operations[1])).toBeTruthy();
  expect(state.operations[1].collectionName).toEqual("users");
  expect(state.operations[1].documents.length).toEqual(2);
});

test("MigrationBuilder - transform adds transform operation", () => {
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
    .end()
    .compile();

  expect(state.operations.length).toEqual(1);
  expect(isTransformCollectionRule(state.operations[0])).toBeTruthy();
  expect(state.operations[0].collectionName).toEqual("users");
});

test("MigrationBuilder - chain multiple operations", () => {
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
    .end()
    .createCollection("posts")
    .end()
    .compile();

  expect(state.operations.length).toEqual(3);
  expect(state.operations[0].type).toEqual("create_collection");
  expect(state.operations[1].type).toEqual("seed_collection");
  expect(state.operations[2].type).toEqual("create_collection");
});

// ============================================================================
// Multi-Collection Builder Tests
// ============================================================================

test("MigrationBuilder - newMultiCollection creates instance", () => {
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
    .createMultiModelInstance("catalog_main", "catalog")
    .end()
    .compile();

  expect(state.operations.length).toEqual(1);
  expect(state.operations[0].type).toEqual("create_multimodel_instance");
  const op0 = state.operations[0] as {
    collectionName?: string;
    modelType?: string;
  };
  expect(op0.collectionName).toEqual("catalog_main");
  expect(op0.modelType).toEqual("catalog");
});

test("MigrationBuilder - seedType adds seed for multi-collection", () => {
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
    .createMultiModelInstance("catalog_main", "catalog")
    .type("product").seed(products).end()
    .end()
    .compile();

  expect(state.operations.length).toEqual(2);
  expect(state.operations[1].type).toEqual("seed_multimodel_instance_type");
  const op1 = state.operations[1] as {
    collectionName?: string;
    modelType?: string;
    documentType?: string;
  };
  expect(op1.collectionName).toEqual("catalog_main");
  expect(op1.modelType).toEqual("catalog");
  expect(op1.documentType).toEqual("product");
});

test("MigrationBuilder - multiCollection type transform", () => {
  const schemas = {
    collections: {},
    multiCollections: {
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

  expect(state.operations.length).toEqual(1);
  expect(state.operations[0].type).toEqual("transform_multicollection_type");
  const op0 = state.operations[0] as {
    collectionName?: string;
    documentType?: string;
  };
  expect(op0.collectionName).toEqual("catalog");
  expect(op0.documentType).toEqual("product");
});

// ============================================================================
// Schema Validation Tests
// ============================================================================

test("MigrationBuilder - seed validates documents against schema", () => {
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
    .end()
    .compile();

  expect(state.operations.length).toEqual(2);

  // Invalid document should throw
  const invalidDoc = { _id: "1", name: "Alice", email: "not-an-email" };

  try {
    migrationBuilder({ schemas })
      .createCollection("users")
      .seed([invalidDoc])
      .end()
      .compile();

    throw new Error("Should have thrown validation error");
  } catch (error) {
    expect(error instanceof Error).toBeTruthy();
  }
});

// ============================================================================
// Update Indexes Tests
// ============================================================================

test("MigrationBuilder - updateIndexes adds update operation", () => {
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

  expect(state.operations.length).toEqual(1);
  expect(state.operations[0].type).toEqual("update_indexes");
  const op0 = state.operations[0] as { collectionName?: string };
  expect(op0.collectionName).toEqual("users");
});

test("MigrationBuilder - updateIndexes throws if collection schema not found", () => {
  const schemas = {
    collections: {},
  };

  try {
    migrationBuilder({ schemas })
      .updateIndexes("users")
      .compile();

    throw new Error("Should have thrown error");
  } catch (error) {
    expect(error instanceof Error).toBeTruthy();
    expect(error.message.includes("schema not found")).toBeTruthy();
  }
});

// ============================================================================
// Migration Summary Tests
// ============================================================================

test("getMigrationSummary - returns correct counts", () => {
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
    .end()
    .collection("posts")
    .transform({
      up: (doc) => doc,
      down: (doc) => doc,
    })
    .end()
    .compile();

  const summary = getMigrationSummary(state);

  expect(summary.creates).toEqual(1);
  expect(summary.seeds).toEqual(1);
  expect(summary.transforms).toEqual(1);
  expect(summary.totalOperations).toEqual(3);
  // createCollection now marks as lossy, not irreversible
  expect(!summary.isIrreversible).toBeTruthy();
  expect(summary.properties.includes("lossy")).toBeTruthy();
});

test("getMigrationSummary - shows reversible when no create operations", () => {
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
    .end()
    .compile();

  const summary = getMigrationSummary(state);

  expect(summary.creates).toEqual(0);
  expect(summary.seeds).toEqual(1);
  expect(summary.isIrreversible).toEqual(false);
});

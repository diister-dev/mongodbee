/**
 * Tests for Simulation Migration Applier
 *
 * Tests the simulation applier for in-memory migration validation
 */

import * as v from "../../src/schema.ts";
import { assert, assertEquals, assertExists } from "@std/assert";
import {
  compareDatabaseStates,
  createEmptyDatabaseState,
  SimulationApplier,
} from "../../src/migration/appliers/simulation.ts";
import type {
  CreateCollectionRule,
  CreateMultiCollectionInstanceRule,
  SeedCollectionRule,
  SeedMultiCollectionInstanceRule,
  TransformCollectionRule,
  TransformMultiCollectionTypeRule,
} from "../../src/migration/types.ts";

// ============================================================================
// Basic State Management Tests
// ============================================================================

Deno.test("SimulationApplier - createEmptyDatabaseState returns empty state", () => {
  const state = createEmptyDatabaseState();

  assertExists(state.collections);
  assertExists(state.multiModels);
  assertEquals(Object.keys(state.collections).length, 0);
  assertEquals(Object.keys(state.multiModels).length, 0);
});

Deno.test("SimulationApplier - compareDatabaseStates returns true for identical states", () => {
  const state1 = createEmptyDatabaseState();
  const state2 = createEmptyDatabaseState();

  assert(compareDatabaseStates(state1, state2));
});

Deno.test("SimulationApplier - compareDatabaseStates returns false for different states", () => {
  const state1 = createEmptyDatabaseState();
  const state2 = createEmptyDatabaseState();

  state2.collections.users = { content: [] };

  assert(!compareDatabaseStates(state1, state2));
});

// ============================================================================
// Create Collection Tests
// ============================================================================

Deno.test("SimulationApplier - creates collection in state", () => {
  const applier = new SimulationApplier();
  let state = createEmptyDatabaseState();

  const operation: CreateCollectionRule = {
    type: "create_collection",
    collectionName: "users",
  };

  state = applier.applyOperation(state, operation);

  assertExists(state.collections.users);
  assertEquals(state.collections.users.content.length, 0);
});

Deno.test("SimulationApplier - reverse drops collection from state", () => {
  const applier = new SimulationApplier();
  let state = createEmptyDatabaseState();

  // Create collection first
  state.collections.users = { content: [] };

  const operation: CreateCollectionRule = {
    type: "create_collection",
    collectionName: "users",
  };

  state = applier.applyReverseOperation(state, operation);

  assertEquals(state.collections.users, undefined);
});

Deno.test("SimulationApplier - does not mutate original state", () => {
  const applier = new SimulationApplier();
  const originalState = createEmptyDatabaseState();

  const operation: CreateCollectionRule = {
    type: "create_collection",
    collectionName: "users",
  };

  const newState = applier.applyOperation(originalState, operation);

  // Original state should be unchanged
  assertEquals(Object.keys(originalState.collections).length, 0);

  // New state should have the collection
  assertExists(newState.collections.users);
});

// ============================================================================
// Seed Collection Tests
// ============================================================================

Deno.test("SimulationApplier - seeds documents into collection", () => {
  const applier = new SimulationApplier();
  let state = createEmptyDatabaseState();
  state.collections.users = { content: [] };

  const documents = [
    { _id: "1", name: "Alice" },
    { _id: "2", name: "Bob" },
  ];

  const operation: SeedCollectionRule = {
    type: "seed_collection",
    collectionName: "users",
    documents,
  };

  state = applier.applyOperation(state, operation);

  assertEquals(state.collections.users.content.length, 2);
  assertEquals(state.collections.users.content[0]._id, "1");
  assertEquals(state.collections.users.content[1]._id, "2");
});

Deno.test("SimulationApplier - reverse removes seeded documents", () => {
  const applier = new SimulationApplier();
  let state = createEmptyDatabaseState();

  const documents = [
    { _id: "1", name: "Alice" },
    { _id: "2", name: "Bob" },
  ];

  state.collections.users = {
    content: [...documents],
  };

  const operation: SeedCollectionRule = {
    type: "seed_collection",
    collectionName: "users",
    documents,
  };

  state = applier.applyReverseOperation(state, operation);

  assertEquals(state.collections.users.content.length, 0);
});

// ============================================================================
// Transform Collection Tests
// ============================================================================

Deno.test("SimulationApplier - transforms all documents", () => {
  const applier = new SimulationApplier();
  let state = createEmptyDatabaseState();

  state.collections.users = {
    content: [
      { _id: "1", name: "Alice" },
      { _id: "2", name: "Bob" },
    ],
  };

  const operation: TransformCollectionRule = {
    type: "transform_collection",
    collectionName: "users",
    up: (doc: Record<string, unknown>) => ({
      ...doc,
      age: 25,
    }),
    down: (doc: Record<string, unknown>) => {
      const { age: _age, ...rest } = doc;
      return rest;
    },
  };

  state = applier.applyOperation(state, operation);

  assertEquals(state.collections.users.content.length, 2);
  assertEquals(
    (state.collections.users.content[0] as Record<string, unknown>).age,
    25,
  );
  assertEquals(
    (state.collections.users.content[1] as Record<string, unknown>).age,
    25,
  );
});

Deno.test("SimulationApplier - reverse transform restores original", () => {
  const applier = new SimulationApplier();
  let state = createEmptyDatabaseState();

  state.collections.users = {
    content: [
      { _id: "1", name: "Alice", age: 25 },
      { _id: "2", name: "Bob", age: 30 },
    ],
  };

  const operation: TransformCollectionRule = {
    type: "transform_collection",
    collectionName: "users",
    up: (doc: Record<string, unknown>) => ({
      ...doc,
      age: 25,
    }),
    down: (doc: Record<string, unknown>) => {
      const { age: _age, ...rest } = doc;
      return rest;
    },
  };

  state = applier.applyReverseOperation(state, operation);

  assertEquals(state.collections.users.content.length, 2);
  assertEquals(
    (state.collections.users.content[0] as Record<string, unknown>).age,
    undefined,
  );
  assertEquals(
    (state.collections.users.content[1] as Record<string, unknown>).age,
    undefined,
  );
});

// ============================================================================
// Multi-Collection Tests
// ============================================================================

Deno.test("SimulationApplier - creates multi-collection instance with metadata", () => {
  const applier = new SimulationApplier();
  let state = createEmptyDatabaseState();

  const operation: CreateMultiCollectionInstanceRule = {
    type: "create_multicollection_instance",
    collectionName: "catalog_main",
    collectionType: "catalog",
  };

  state = applier.applyOperation(state, operation);

  assertExists(state.multiModels);
  assertExists(state.multiModels.catalog_main);

  const content = state.multiModels.catalog_main.content;
  assertEquals(content.length, 2); // _information and _migrations

  const infoDoc = content.find((doc: Record<string, unknown>) =>
    doc._type === "_information"
  );
  const migrationsDoc = content.find((doc: Record<string, unknown>) =>
    doc._type === "_migrations"
  );

  assertExists(infoDoc);
  assertExists(migrationsDoc);
  assertEquals((infoDoc as Record<string, unknown>).collectionType, "catalog");
});

Deno.test("SimulationApplier - seeds multi-collection type with _type field", () => {
  const applier = new SimulationApplier();
  let state = createEmptyDatabaseState();

  state.multiModels = {
    catalog_main: { content: [] },
  };

  const products = [
    { name: "Product 1", price: 100 },
    { name: "Product 2", price: 200 },
  ];

  const operation: SeedMultiCollectionInstanceRule = {
    type: "seed_multicollection_instance",
    collectionName: "catalog_main",
    typeName: "product",
    documents: products,
  };

  state = applier.applyOperation(state, operation);

  assertExists(state.multiModels);
  const content = state.multiModels.catalog_main.content;
  assertEquals(content.length, 2);
  assertEquals((content[0] as Record<string, unknown>)._type, "product");
  assertEquals((content[1] as Record<string, unknown>)._type, "product");
  assert(
    ((content[0] as Record<string, unknown>)._id as string).startsWith(
      "product:",
    ),
  );
});

// ============================================================================
// Multi-Collection Transform Tests
// ============================================================================

Deno.test("SimulationApplier - transforms type across all instances", () => {
  const applier = new SimulationApplier();
  let state = createEmptyDatabaseState();

  state.multiModels = {
    catalog_store1: {
      content: [
        {
          _id: "_information",
          _type: "_information",
          collectionType: "catalog",
          createdAt: new Date(),
        },
        { _id: "p1", _type: "product", name: "Product 1" },
      ],
    },
    catalog_store2: {
      content: [
        {
          _id: "_information",
          _type: "_information",
          collectionType: "catalog",
          createdAt: new Date(),
        },
        { _id: "p2", _type: "product", name: "Product 2" },
      ],
    },
  };

  const operation: TransformMultiCollectionTypeRule = {
    type: "transform_multicollection_type",
    collectionType: "catalog",
    typeName: "product",
    up: (doc: Record<string, unknown>) => ({
      ...doc,
      price: 0,
    }),
    down: (doc: Record<string, unknown>) => {
      const { price: _price, ...rest } = doc;
      return rest;
    },
  };

  state = applier.applyOperation(state, operation);

  // Check both instances were transformed
  assertExists(state.multiModels);
  const product1 = state.multiModels.catalog_store1.content.find(
    (doc: Record<string, unknown>) => doc._type === "product",
  ) as Record<string, unknown>;
  const product2 = state.multiModels.catalog_store2.content.find(
    (doc: Record<string, unknown>) => doc._type === "product",
  ) as Record<string, unknown>;

  assertEquals(product1.price, 0);
  assertEquals(product2.price, 0);
});

Deno.test("SimulationApplier - generates mock data when no instances exist", () => {
  const applier = new SimulationApplier();
  let state = createEmptyDatabaseState();

  state.multiModels = {};

  const schema = v.object({
    _id: v.string(),
    name: v.string(),
    price: v.number(),
  });

  const operation: TransformMultiCollectionTypeRule = {
    type: "transform_multicollection_type",
    collectionType: "catalog",
    typeName: "product",
    schema,
    up: (doc: Record<string, unknown>) => ({
      ...doc,
      discount: 0,
    }),
    down: (doc: Record<string, unknown>) => {
      const { discount: _discount, ...rest } = doc;
      return rest;
    },
  };

  // Should not throw - should generate mock data and test transform
  state = applier.applyOperation(state, operation);

  // Test instance should be created
  assertExists(state.multiModels);
  assertExists(state.multiModels.catalog_test_simulation);
});

// ============================================================================
// Reversibility Tests
// ============================================================================

Deno.test("SimulationApplier - full forward and reverse cycle returns to original state", () => {
  const applier = new SimulationApplier();
  const initialState = createEmptyDatabaseState();

  const operations:
    (CreateCollectionRule | SeedCollectionRule | TransformCollectionRule)[] = [
      {
        type: "create_collection",
        collectionName: "users",
      },
      {
        type: "seed_collection",
        collectionName: "users",
        documents: [
          { _id: "1", name: "Alice" },
          { _id: "2", name: "Bob" },
        ],
      },
      {
        type: "transform_collection",
        collectionName: "users",
        up: (doc: Record<string, unknown>) => ({ ...doc, active: true }),
        down: (doc: Record<string, unknown>) => {
          const { active: _active, ...rest } = doc;
          return rest;
        },
      },
    ];

  // Apply all operations
  let state = initialState;
  for (const operation of operations) {
    state = applier.applyOperation(state, operation);
  }

  // Reverse all operations in reverse order
  for (let i = operations.length - 1; i >= 0; i--) {
    state = applier.applyReverseOperation(state, operations[i]);
  }

  // Should be back to initial state
  assert(compareDatabaseStates(state, initialState));
});

// ============================================================================
// Strict Validation Tests
// ============================================================================

Deno.test("SimulationApplier - strict mode throws on missing collection", () => {
  const applier = new SimulationApplier({ strictValidation: true });
  const state = createEmptyDatabaseState();

  const operation: SeedCollectionRule = {
    type: "seed_collection",
    collectionName: "nonexistent",
    documents: [{ _id: "1" }],
  };

  try {
    applier.applyOperation(state, operation);
    throw new Error("Should have thrown");
  } catch (error) {
    assert(error instanceof Error);
    assert(error.message.includes("does not exist"));
  }
});

Deno.test("SimulationApplier - non-strict mode creates collection if missing", () => {
  const applier = new SimulationApplier({ strictValidation: false });
  let state = createEmptyDatabaseState();

  const operation: SeedCollectionRule = {
    type: "seed_collection",
    collectionName: "users",
    documents: [{ _id: "1", name: "Alice" }],
  };

  // Should not throw
  state = applier.applyOperation(state, operation);

  assertExists(state.collections.users);
  assertEquals(state.collections.users.content.length, 1);
});

// ============================================================================
// History Tracking Tests
// ============================================================================

Deno.test("SimulationApplier - tracks operation history when enabled", () => {
  const applier = new SimulationApplier({ trackHistory: true });
  let state = createEmptyDatabaseState();

  const operation: CreateCollectionRule = {
    type: "create_collection",
    collectionName: "users",
  };

  state = applier.applyOperation(state, operation);

  assertExists(state.operationHistory);
  assertEquals(state.operationHistory.length, 1);
  assertEquals(state.operationHistory[0].operation.type, "create_collection");
  assertEquals(state.operationHistory[0].type, "apply");
});

Deno.test("SimulationApplier - does not track history when disabled", () => {
  const applier = new SimulationApplier({ trackHistory: false });
  let state = createEmptyDatabaseState();

  const operation: CreateCollectionRule = {
    type: "create_collection",
    collectionName: "users",
  };

  state = applier.applyOperation(state, operation);

  assertEquals(state.operationHistory, undefined);
});

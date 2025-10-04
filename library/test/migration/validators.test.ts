/**
 * @fileoverview Tests for migration validators
 *
 * Tests the three main validators:
 * - ChainValidator: Validates migration chain structure and ordering
 * - IntegrityValidator: Validates operation integrity and consistency
 * - SimulationValidator: Validates through in-memory simulation
 *
 * These tests focus on real-world scenarios rather than exhaustive edge cases.
 */

import { assert, assertEquals, assertExists } from "@std/assert";
import {
  createChainValidator,
  validateMigrationChain,
} from "../../src/migration/validators/chain.ts";
import {
  createIntegrityValidator,
  validateMigrationState,
} from "../../src/migration/validators/integrity.ts";
import {
  createSimulationValidator,
  validateMigrationWithSimulation,
} from "../../src/migration/validators/simulation.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import * as v from "valibot";

// ============================================================================
// ChainValidator - Tests for migration chain validation
// ============================================================================

Deno.test("ChainValidator - validates simple linear chain", () => {
  const m1 = migrationDefinition("2024_01_01_1200_A_first", "First", {
    parent: null,
    schemas: { collections: {}, multiCollections: {} },
    migrate(m) {
      return m.compile();
    },
  });

  const m2 = migrationDefinition("2024_01_01_1300_B_second", "Second", {
    parent: m1,
    schemas: { collections: {}, multiCollections: {} },
    migrate(m) {
      return m.compile();
    },
  });

  const m3 = migrationDefinition("2024_01_01_1400_C_third", "Third", {
    parent: m2,
    schemas: { collections: {}, multiCollections: {} },
    migrate(m) {
      return m.compile();
    },
  });

  const validator = createChainValidator();
  const result = validator.validateChain([m1, m2, m3]);

  assertEquals(result.isValid, true);
  assertEquals(result.errors.length, 0);
  assertEquals(result.metadata.totalMigrations, 3);
});

Deno.test("ChainValidator - detects duplicate migration IDs", () => {
  const m1 = migrationDefinition("2024_01_01_1200_DUP_test", "First", {
    parent: null,
    schemas: { collections: {}, multiCollections: {} },
    migrate(m) {
      return m.compile();
    },
  });

  const m2 = migrationDefinition("2024_01_01_1200_DUP_test", "Second", {
    parent: null,
    schemas: { collections: {}, multiCollections: {} },
    migrate(m) {
      return m.compile();
    },
  });

  const validator = createChainValidator();
  const result = validator.validateChain([m1, m2]);

  assertEquals(result.isValid, false);
  assert(result.errors.some((e) => e.includes("Duplicate")));
});

Deno.test("ChainValidator - detects broken parent references", () => {
  const fakeParent = {
    id: "nonexistent_fake",
    name: "Fake",
    parent: null,
    schemas: {},
    migrate: () => ({ operations: [], irreversible: false }),
  } as const;

  const child = migrationDefinition("2024_01_01_1300_CHILD_test", "Child", {
    parent: fakeParent as never,
    schemas: { collections: {}, multiCollections: {} },
    migrate(m) {
      return m.compile();
    },
  });

  const validator = createChainValidator();
  const result = validator.validateChain([child]);

  assertEquals(result.isValid, false);
  assert(result.errors.some((e) => e.includes("parent")));
});

Deno.test("ChainValidator - allows multiple roots when configured", () => {
  const root1 = migrationDefinition("2024_01_01_1200_R1_first", "Root 1", {
    parent: null,
    schemas: { collections: {}, multiCollections: {} },
    migrate(m) {
      return m.compile();
    },
  });

  const root2 = migrationDefinition("2024_01_01_1300_R2_second", "Root 2", {
    parent: null,
    schemas: { collections: {}, multiCollections: {} },
    migrate(m) {
      return m.compile();
    },
  });

  // Should fail with strict config
  const strictValidator = createChainValidator({ allowMultipleRoots: false });
  const strictResult = strictValidator.validateChain([root1, root2]);
  assertEquals(strictResult.isValid, false);

  // Should pass with lenient config
  const lenientValidator = createChainValidator({ allowMultipleRoots: true });
  const lenientResult = lenientValidator.validateChain([root1, root2]);
  assertEquals(lenientResult.isValid, true);
});

Deno.test("validateMigrationChain - convenience function", () => {
  const migration = migrationDefinition("2024_01_01_1200_TEST_test", "Test", {
    parent: null,
    schemas: { collections: {}, multiCollections: {} },
    migrate(m) {
      return m.compile();
    },
  });

  const result = validateMigrationChain([migration]);

  assertEquals(result.isValid, true);
  assertEquals(result.metadata.totalMigrations, 1);
});

// ============================================================================
// IntegrityValidator - Tests for operation integrity
// ============================================================================

Deno.test("IntegrityValidator - validates migration state with operations", () => {
  const schemas = {
    collections: {
      users: {
        name: v.string(),
        email: v.string(),
      },
    },
    multiCollections: {},
  };

  const migration = migrationDefinition(
    "2024_01_01_1200_TEST@users",
    "Create users",
    {
      parent: null,
      schemas,
      migrate(m) {
        m.createCollection("users");
        return m.compile();
      },
    },
  );

  const builder = migrationBuilder({ schemas });
  const state = migration.migrate(builder);

  const validator = createIntegrityValidator();
  const result = validator.validateMigrationState(state);

  assertEquals(result.isValid, true);
  assertEquals(result.metadata.totalOperations, 1);
  assertExists(result.metadata.operationTypes["create_collection"]);
  assert(result.metadata.affectedCollections.includes("users"));
});

Deno.test("IntegrityValidator - detects empty operations", () => {
  const state = {
    operations: [],
    irreversible: false,
    properties: [] as Array<{ type: "irreversible" }>,
    mark(props: { type: "irreversible" }) {
      this.properties.push(props);
    },
    hasProperty(type: "irreversible") {
      return this.properties.some((p) => p.type === type);
    },
  };

  const validator = createIntegrityValidator();
  const result = validator.validateMigrationState(state);

  assertEquals(result.isValid, false);
  assert(result.errors.some((e) => e.includes("empty")));
});

Deno.test("IntegrityValidator - enforces operation limits", () => {
  const operations = Array.from({ length: 20 }, (_, i) => ({
    type: "create_collection" as const,
    collectionName: `col_${i}`,
  }));

  const state = {
    operations,
    irreversible: false,
    properties: [] as Array<{ type: "irreversible" }>,
    mark(props: { type: "irreversible" }) {
      this.properties.push(props);
    },
    hasProperty(type: "irreversible") {
      return this.properties.some((p) => p.type === type);
    },
  };

  const validator = createIntegrityValidator({ maxOperations: 15 });
  const result = validator.validateMigrationState(state);

  assertEquals(result.isValid, false);
  assert(result.errors.some((e) => e.includes("exceeds")));
});

Deno.test("validateMigrationState - convenience function", () => {
  const state = {
    operations: [
      { type: "create_collection" as const, collectionName: "test" },
    ],
    irreversible: false,
    properties: [] as Array<{ type: "irreversible" }>,
    mark(props: { type: "irreversible" }) {
      this.properties.push(props);
    },
    hasProperty(type: "irreversible") {
      return this.properties.some((p) => p.type === type);
    },
  };

  const result = validateMigrationState(state);
  assertEquals(result.isValid, true);
});

// ============================================================================
// SimulationValidator - Tests for simulation-based validation
// ============================================================================

Deno.test("SimulationValidator - validates simple migration", async () => {
  const schemas = {
    collections: {
      users: {
        name: v.string(),
      },
    },
    multiCollections: {},
  };

  const migration = migrationDefinition("2024_01_01_1200_USR@users", "Users", {
    parent: null,
    schemas,
    migrate(m) {
      m.createCollection("users");
      return m.compile();
    },
  });

  const validator = createSimulationValidator();
  const result = await validator.validateMigration(migration);

  assertEquals(result.success, true);
  assertEquals(result.errors.length, 0);
  assertExists(result.data?.operationCount);
  assertEquals(result.data?.simulationCompleted, true);
});

Deno.test("SimulationValidator - validates migration with seed and transform", async () => {
  const schemas = {
    collections: {
      products: {
        name: v.string(),
        price: v.number(),
      },
    },
    multiCollections: {},
  };

  const m1 = migrationDefinition(
    "2024_01_01_1200_M1@create",
    "Create products",
    {
      parent: null,
      schemas,
      migrate(m) {
        m.createCollection("products")
          .seed([
            { name: "Item A", price: 100 },
            { name: "Item B", price: 200 },
          ]);
        return m.compile();
      },
    },
  );

  const m2 = migrationDefinition(
    "2024_01_01_1300_M2@discount",
    "Apply discount",
    {
      parent: m1,
      schemas,
      migrate(m) {
        m.collection("products")
          .transform({
            up: (doc: Record<string, unknown>) => {
              const product = doc as { name: string; price: number };
              return { ...product, price: product.price * 0.9 };
            },
            down: (doc: Record<string, unknown>) => {
              const product = doc as { name: string; price: number };
              return { ...product, price: product.price / 0.9 };
            },
          });
        return m.compile();
      },
    },
  );

  const validator = createSimulationValidator({ validateReversibility: true });
  const result = await validator.validateMigration(m2);

  assertEquals(result.success, true);
  assertExists(result.data?.simulationCompleted);
});

Deno.test("SimulationValidator - detects operations on non-existent collections", async () => {
  const schemas = {
    collections: {
      users: {
        name: v.string(),
      },
    },
    multiCollections: {},
  };

  const migration = migrationDefinition(
    "2024_01_01_1200_BAD@bad",
    "Bad migration",
    {
      parent: null,
      schemas,
      migrate(m) {
        // Try to seed without creating collection first
        m.collection("users")
          .seed([{ name: "Alice" }]);
        return m.compile();
      },
    },
  );

  const validator = createSimulationValidator();
  const result = await validator.validateMigration(migration);

  assertEquals(result.success, false);
  assert(result.errors.length > 0);
});

Deno.test("SimulationValidator - validates parent-child migrations", async () => {
  const schemas = {
    collections: {
      posts: {
        title: v.string(),
        content: v.string(),
      },
    },
    multiCollections: {},
  };

  const parent = migrationDefinition(
    "2024_01_01_1200_P@parent",
    "Create posts",
    {
      parent: null,
      schemas,
      migrate(m) {
        m.createCollection("posts");
        return m.compile();
      },
    },
  );

  const child = migrationDefinition("2024_01_01_1300_C@child", "Seed posts", {
    parent,
    schemas,
    migrate(m) {
      m.collection("posts")
        .seed([
          { title: "First Post", content: "Hello World" },
        ]);
      return m.compile();
    },
  });

  const validator = createSimulationValidator({ validateReversibility: false });
  const result = await validator.validateMigration(child);

  // Should simulate parent first, then child
  assertEquals(result.success, true);
});

Deno.test("SimulationValidator - warns about too many operations", async () => {
  const schemas = {
    collections: {
      items: {
        value: v.number(),
      },
    },
    multiCollections: {},
  };

  const migration = migrationDefinition(
    "2024_01_01_1200_MANY@many",
    "Many ops",
    {
      parent: null,
      schemas,
      migrate(m) {
        let builder = m.createCollection("items");

        // Add 20 seed operations
        for (let i = 0; i < 20; i++) {
          builder = builder.seed([{ value: i }]);
        }

        return builder.done().compile();
      },
    },
  );

  const validator = createSimulationValidator({ maxOperations: 10 });
  const result = await validator.validateMigration(migration);

  // Should succeed but with warnings
  assertEquals(result.success, true);
  assert(result.warnings.length > 0);
});

Deno.test("SimulationValidator - handles empty migrations", async () => {
  const migration = migrationDefinition(
    "2024_01_01_1200_EMPTY@empty",
    "Empty",
    {
      parent: null,
      schemas: { collections: {}, multiCollections: {} },
      migrate(m) {
        return m.compile();
      },
    },
  );

  const validator = createSimulationValidator();
  const result = await validator.validateMigration(migration);

  // Should pass but with warnings about no operations
  assertEquals(result.success, true);
  assert(result.warnings.some((w) => w.includes("no operations")));
});

Deno.test("SimulationValidator - detects schema without createCollection", async () => {
  const schemas = {
    collections: {
      users: {
        name: v.string(),
        email: v.string(),
      },
    },
    multiCollections: {},
  };

  const migration = migrationDefinition(
    "2024_01_01_1200_MISSING@missing",
    "Missing",
    {
      parent: null,
      schemas,
      migrate(m) {
        // Schema declares "users" but doesn't create it!
        return m.compile();
      },
    },
  );

  const validator = createSimulationValidator();
  const result = await validator.validateMigration(migration);

  // Should FAIL with error about missing collection
  assertEquals(result.success, false);
  assert(result.errors.some((e) => e.includes("users")));
  assert(result.errors.some((e) => e.includes("createCollection")));
});

Deno.test("SimulationValidator - detects schema without newMultiCollection", async () => {
  const schemas = {
    collections: {},
    multiCollections: {
      comments: {
        user_comment: {
          content: v.string(),
        },
      },
    },
  };

  const migration = migrationDefinition(
    "2024_01_01_1200_MISSINGMC@missingmc",
    "MissingMC",
    {
      parent: null,
      schemas,
      migrate(m) {
        // Schema declares "comments" multi-collection but doesn't create it!
        return m.compile();
      },
    },
  );

  const validator = createSimulationValidator();
  const result = await validator.validateMigration(migration);

  // Should PASS with warning (multi-collections are models, not required to be instantiated)
  assertEquals(result.success, true);
  assert(result.warnings.some((w) => w.includes("comments")));
  assert(result.warnings.some((w) => w.includes("model")));
});

Deno.test("validateMigrationWithSimulation - convenience function", async () => {
  const schemas = {
    collections: {
      comments: {
        text: v.string(),
        author: v.string(),
      },
    },
    multiCollections: {},
  };

  const migration = migrationDefinition(
    "2024_01_01_1200_COM@comments",
    "Comments",
    {
      parent: null,
      schemas,
      migrate(m) {
        m.createCollection("comments")
          .seed([
            { text: "Great post!", author: "Alice" },
          ]);
        return m.compile();
      },
    },
  );

  const result = await validateMigrationWithSimulation(migration, {
    validateReversibility: true,
  });

  assertEquals(result.success, true);
  assertExists(result.data?.simulationCompleted);
});

// ============================================================================
// Integration Tests - Validators working together
// ============================================================================

Deno.test("Validators - full validation pipeline", async () => {
  const schemas = {
    collections: {
      orders: {
        productName: v.string(),
        quantity: v.number(),
        price: v.number(),
      },
    },
    multiCollections: {},
  };

  const m1 = migrationDefinition("2024_01_01_1200_ORD1@init", "Init orders", {
    parent: null,
    schemas,
    migrate(m) {
      m.createCollection("orders")
        .seed([
          { productName: "Widget", quantity: 10, price: 99.99 },
          { productName: "Gadget", quantity: 5, price: 149.99 },
        ]);
      return m.compile();
    },
  });

  const m2 = migrationDefinition("2024_01_01_1300_ORD2@total", "Add total", {
    parent: m1,
    schemas: {
      collections: {
        orders: {
          productName: v.string(),
          quantity: v.number(),
          price: v.number(),
          total: v.number(),
        },
      },
      multiCollections: {},
    },
    migrate(m) {
      m.collection("orders")
        .transform({
          up: (doc: Record<string, unknown>) => {
            const order = doc as {
              productName: string;
              quantity: number;
              price: number;
            };
            return { ...order, total: order.quantity * order.price };
          },
          down: (doc: Record<string, unknown>) => {
            const { total: _total, ...rest } = doc as {
              total: number;
              [key: string]: unknown;
            };
            return rest;
          },
        });
      return m.compile();
    },
  });

  // 1. Chain validation
  const chainValidator = createChainValidator();
  const chainResult = chainValidator.validateChain([m1, m2]);
  assertEquals(chainResult.isValid, true);

  // 2. Integrity validation (without simulation since m2 needs parent state)
  const builder = migrationBuilder({ schemas: m2.schemas });
  const state = m2.migrate(builder);
  const integrityValidator = createIntegrityValidator({ runSimulation: false });
  const integrityResult = integrityValidator.validateMigrationState(state);

  assertEquals(integrityResult.isValid, true);

  // 3. Simulation validation
  const simValidator = createSimulationValidator({
    validateReversibility: false,
  });
  const simResult = await simValidator.validateMigration(m2);

  assertEquals(simResult.success, true);
});

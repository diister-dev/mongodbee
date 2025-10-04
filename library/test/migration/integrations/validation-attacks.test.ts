/**
 * Integration tests for validation attack scenarios
 * 
 * These tests verify that the migration system correctly blocks
 * invalid migrations that attempt to bypass validation rules.
 * 
 * Discovered during aggressive manual testing in playground/sandbox/13
 * 
 * @module
 */

import { assertEquals } from "@std/assert";
import * as v from "valibot";
import { migrationDefinition } from "../../../src/migration/definition.ts";
import { validateMigrationWithSimulation } from "../../../src/migration/validators/simulation.ts";

/**
 * TEST 1: Schema change without transformation
 * 
 * Scenario: Adding a required field without providing a transformation
 * to populate existing documents should be blocked.
 */
Deno.test("Validation Attack 1: Schema change without transformation should fail", async () => {
  // Root migration: creates users collection with seed data
  const rootMigration = migrationDefinition("2025_01_01_ROOT", "create_users", {
    parent: null,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          createdAt: v.date(),
        },
      },
      multiCollections: {},
    },
    migrate(migration) {
      migration.createCollection("users");
      
      // Add seed data so transformation validation is triggered
      migration.collection("users").seed([
        {
          _id: "user1",
          name: "Alice",
          email: "alice@example.com",
          createdAt: new Date("2025-01-01"),
        },
        {
          _id: "user2",
          name: "Bob",
          email: "bob@example.com",
          createdAt: new Date("2025-01-01"),
        },
      ]);
      
      return migration.compile();
    },
  });

  // Attack migration: adds required field WITHOUT transformation
  const attackMigration = migrationDefinition("2025_01_02_ATTACK1", "add_age_no_transform", {
    parent: rootMigration,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          age: v.number(), // ← NEW REQUIRED FIELD, but no transformation!
          createdAt: v.date(),
        },
      },
      multiCollections: {},
    },
    migrate(migration) {
      // 🚨 ATTACK: No transformation provided for new required field
      return migration.compile();
    },
  });

  const result = await validateMigrationWithSimulation(attackMigration);

  // Should detect missing transformation
  assertEquals(result.success, false, "Should fail validation");
  assertEquals(
    result.errors.some((e: string) => e.includes("users") && e.includes("transform")),
    true,
    "Error should mention missing transformation for collection",
  );
});

/**
 * TEST 2: Multi-collection type removal without transformation
 * 
 * Scenario: Removing a type from a multi-collection schema without
 * providing a transformation to migrate existing documents.
 */
Deno.test("Validation Attack 2: Multi-collection type removal without transformation should fail", async () => {
  // Root migration: creates posts multi-collection with article and video types
  const rootMigration = migrationDefinition("2025_01_01_ROOT", "create_posts", {
    parent: null,
    schemas: {
      collections: {},
      multiCollections: {
        posts: {
          article: {
            _id: v.string(),
            title: v.string(),
            content: v.string(),
          },
          video: {
            _id: v.string(),
            title: v.string(),
            url: v.string(),
          },
        },
      },
    },
    migrate(migration) {
      migration.newMultiCollection("posts", "main");
      return migration.compile();
    },
  });

  // Attack migration: removes video type WITHOUT transformation
  const attackMigration = migrationDefinition("2025_01_02_ATTACK2", "remove_video_type", {
    parent: rootMigration,
    schemas: {
      collections: {},
      multiCollections: {
        posts: {
          article: {
            _id: v.string(),
            title: v.string(),
            content: v.string(),
          },
          // 🚨 ATTACK: video type removed without transformation
        },
      },
    },
    migrate(migration) {
      // No transformation provided to handle existing video documents
      return migration.compile();
    },
  });

  const result = await validateMigrationWithSimulation(attackMigration);

  // Should detect missing transformation for type removal
  assertEquals(result.success, false, "Should fail validation");
  assertEquals(
    result.errors.some((e: string) => e.includes("video") && e.includes("removed")),
    true,
    "Error should mention removed 'video' type",
  );
});

/**
 * TEST 3: Transformation with invalid values
 * 
 * Scenario: Providing a transformation that returns values violating
 * the target schema (e.g., null instead of boolean).
 * 
 * ⚠️  CRITICAL TEST: This was discovered during manual testing!
 * Before the fix, this attack succeeded and corrupted the database.
 */
Deno.test("Validation Attack 3: Transformation returning invalid values should fail", async () => {
  // Root migration: creates users collection WITH SEED DATA
  const rootMigration = migrationDefinition("2025_01_01_ROOT", "create_users", {
    parent: null,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          createdAt: v.date(),
        },
      },
      multiCollections: {},
    },
    migrate(migration) {
      migration.createCollection("users").seed([
        {
          _id: "1",
          name: "Alice",
          email: "alice@example.com",
          createdAt: new Date("2025-01-01"),
        },
      ]);
      return migration.compile();
    },
  });

  // Attack migration: adds boolean field with transformation returning null
  const attackMigration = migrationDefinition("2025_01_02_ATTACK3", "add_verified_bad_transform", {
    parent: rootMigration,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          verified: v.boolean(), // ← NEW FIELD: expects boolean
          createdAt: v.date(),
        },
      },
      multiCollections: {},
    },
    migrate(migration) {
      // 🚨 ATTACK: Transformation returns null instead of boolean
      migration.collection("users").transform({
        up: (doc) => ({
          ...doc,
          verified: null as unknown as boolean, // ← INVALID: null is not boolean
        }),
        down: (doc) => {
          const { verified: _verified, ...rest } = doc;
          return rest;
        },
      });
      return migration.compile();
    },
  });

  const result = await validateMigrationWithSimulation(attackMigration);

  // Should detect invalid transformed values
  assertEquals(result.success, false, "Should fail validation");
  assertEquals(
    result.errors.some((e: string) => e.includes("verified") || e.includes("boolean") || e.includes("null")),
    true,
    "Error should mention validation issue with verified field",
  );
});

/**
 * TEST 4: Transformation with type mismatch
 * 
 * Scenario: Transformation returns wrong type (string when number expected)
 */
Deno.test("Validation Attack 4: Transformation with type mismatch should fail", async () => {
  const rootMigration = migrationDefinition("2025_01_01_ROOT", "create_users", {
    parent: null,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          createdAt: v.date(),
        },
      },
      multiCollections: {},
    },
    migrate(migration) {
      migration.createCollection("users").seed([
        {
          _id: "1",
          name: "Bob",
          email: "bob@example.com",
          createdAt: new Date("2025-01-01"),
        },
      ]);
      return migration.compile();
    },
  });

  const attackMigration = migrationDefinition("2025_01_02_ATTACK4", "add_age_wrong_type", {
    parent: rootMigration,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          age: v.number(), // ← Expects number
          createdAt: v.date(),
        },
      },
      multiCollections: {},
    },
    migrate(migration) {
      // 🚨 ATTACK: Returns string instead of number
      migration.collection("users").transform({
        up: (doc) => ({
          ...doc,
          age: "25" as unknown as number, // ← INVALID: string is not number
        }),
        down: (doc) => {
          const { age: _age, ...rest } = doc;
          return rest;
        },
      });
      return migration.compile();
    },
  });

  const result = await validateMigrationWithSimulation(attackMigration);

  assertEquals(result.success, false, "Should fail validation");
  assertEquals(
    result.errors.some((e: string) => e.includes("age") || e.includes("number") || e.includes("string")),
    true,
    "Error should mention validation issue with age field",
  );
});

/**
 * TEST 5: Transformation missing required field
 * 
 * Scenario: Transformation doesn't add a required field
 */
Deno.test("Validation Attack 5: Transformation missing required field should fail", async () => {
  const rootMigration = migrationDefinition("2025_01_01_ROOT", "create_users", {
    parent: null,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          createdAt: v.date(),
        },
      },
      multiCollections: {},
    },
    migrate(migration) {
      migration.createCollection("users").seed([
        {
          _id: "1",
          name: "Charlie",
          email: "charlie@example.com",
          createdAt: new Date("2025-01-01"),
        },
      ]);
      return migration.compile();
    },
  });

  const attackMigration = migrationDefinition("2025_01_02_ATTACK5", "add_status_incomplete", {
    parent: rootMigration,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          status: v.picklist(["active", "inactive"]), // ← Required field
          createdAt: v.date(),
        },
      },
      multiCollections: {},
    },
    migrate(migration) {
      // 🚨 ATTACK: Transformation doesn't add the 'status' field
      migration.collection("users").transform({
        up: (doc) => ({
          ...doc,
          // status field is missing!
        }),
        down: (doc) => {
          const { status: _status, ...rest } = doc;
          return rest;
        },
      });
      return migration.compile();
    },
  });

  const result = await validateMigrationWithSimulation(attackMigration);

  assertEquals(result.success, false, "Should fail validation");
  assertEquals(
    result.errors.some((e: string) => e.includes("status")),
    true,
    "Error should mention missing 'status' field",
  );
});

/**
 * TEST 6: Valid transformation should pass
 * 
 * Positive test: A properly written transformation should pass validation
 */
Deno.test("Valid transformation with correct values should pass", async () => {
  const rootMigration = migrationDefinition("2025_01_01_ROOT", "create_users", {
    parent: null,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          createdAt: v.date(),
        },
      },
      multiCollections: {},
    },
    migrate(migration) {
      migration.createCollection("users");
      return migration.compile();
    },
  });

  const validMigration = migrationDefinition("2025_01_02_VALID", "add_age_correct", {
    parent: rootMigration,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          age: v.number(),
          createdAt: v.date(),
        },
      },
      multiCollections: {},
    },
    migrate(migration) {
      // ✅ VALID: Proper transformation with correct type
      migration.collection("users").transform({
        up: (doc) => ({
          ...doc,
          age: 25, // ← VALID: number type
        }),
        down: (doc) => {
          const { age: _age, ...rest } = doc;
          return rest;
        },
      });
      return migration.compile();
    },
  });

  const result = await validateMigrationWithSimulation(validMigration);

  assertEquals(result.success, true, "Valid migration should pass without errors");
});

/**
 * TEST 7: Multi-collection transformation with invalid values
 * 
 * Scenario: Multi-collection type transformation returns invalid values
 */
Deno.test("Validation Attack 7: Multi-collection transformation with invalid values should fail", async () => {
  const rootMigration = migrationDefinition("2025_01_01_ROOT", "create_posts", {
    parent: null,
    schemas: {
      collections: {},
      multiCollections: {
        posts: {
          article: {
            _id: v.string(),
            title: v.string(),
            content: v.string(),
          },
        },
      },
    },
    migrate(migration) {
      migration.newMultiCollection("posts", "main");
      return migration.compile();
    },
  });

  const attackMigration = migrationDefinition("2025_01_02_ATTACK7", "add_published_bad", {
    parent: rootMigration,
    schemas: {
      collections: {},
      multiCollections: {
        posts: {
          article: {
            _id: v.string(),
            title: v.string(),
            content: v.string(),
            published: v.boolean(), // ← NEW FIELD: expects boolean
          },
        },
      },
    },
    migrate(migration) {
      // 🚨 ATTACK: Returns undefined instead of boolean
      migration.multiCollection("posts").type("article").transform({
        up: (doc) => ({
          ...doc,
          published: undefined as unknown as boolean,
        }),
        down: (doc) => {
          const { published: _published, ...rest } = doc;
          return rest;
        },
      });
      return migration.compile();
    },
  });

  const result = await validateMigrationWithSimulation(attackMigration);

  assertEquals(result.success, false, "Should fail validation");
  assertEquals(
    result.errors.some((e: string) => e.includes("published") && e.includes("boolean")),
    true,
    "Error should mention 'published' field and expected boolean type",
  );
});

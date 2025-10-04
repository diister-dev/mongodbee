/**
 * Integration test: Complete application lifecycle journey
 * 
 * This test simulates a real-world application evolution:
 * 1. Initial setup with users
 * 2. Adding posts system (multi-collection)
 * 3. Adding comments system (new multi-collection instance)
 * 4. Adding likes feature (schema evolution with transformation)
 * 
 * Tests validation at each step and rollback capabilities.
 * 
 * @module
 */

import { assertEquals } from "@std/assert";
import * as v from "valibot";
import { migrationDefinition } from "../../../src/migration/definition.ts";
import { validateMigrationWithSimulation } from "../../../src/migration/validators/simulation.ts";

Deno.test("Journey: Complete application lifecycle with validation", async () => {
  // ========================================
  // STEP 1: Initial application - Users
  // ========================================
  const migration1 = migrationDefinition("2025_01_01_1000_ROOT", "create_users", {
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

  const result1 = await validateMigrationWithSimulation(migration1);
  assertEquals(result1.success, true, "Step 1: Users creation should succeed");

  // ========================================
  // STEP 2: Add Posts System (Multi-collection)
  // ========================================
  const migration2 = migrationDefinition("2025_01_02_1000_POSTS", "add_posts_system", {
    parent: migration1,
    schemas: {
      collections: {
        ...migration1.schemas.collections,
      },
      multiCollections: {
        posts: {
          article: {
            _id: v.string(),
            authorId: v.string(),
            title: v.string(),
            content: v.string(),
            createdAt: v.date(),
          },
          video: {
            _id: v.string(),
            authorId: v.string(),
            title: v.string(),
            url: v.string(),
            duration: v.number(),
            createdAt: v.date(),
          },
        },
      },
    },
    migrate(migration) {
      migration
        .newMultiCollection("posts_main", "posts")
        .seedType("article", [
          {
            _id: "post1",
            authorId: "user1",
            title: "First Article",
            content: "Hello World",
            createdAt: new Date("2025-01-02"),
          },
        ])
        .seedType("video", [
          {
            _id: "post2",
            authorId: "user2",
            title: "Tutorial Video",
            url: "https://example.com/video1",
            duration: 300,
            createdAt: new Date("2025-01-02"),
          },
        ])
        .end();
      
      return migration.compile();
    },
  });

  const result2 = await validateMigrationWithSimulation(migration2);
  assertEquals(result2.success, true, "Step 2: Posts system should succeed");

  // ========================================
  // STEP 3: Add User Age Field (with transformation)
  // ========================================
  const migration3 = migrationDefinition("2025_01_03_1000_USER_AGE", "add_user_age", {
    parent: migration2,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.pipe(v.string(), v.email()),
          age: v.number(), // ← NEW FIELD
          createdAt: v.date(),
        },
      },
      multiCollections: {
        ...migration2.schemas.multiCollections,
      },
    },
    migrate(migration) {
      // Proper transformation with default value
      migration.collection("users").transform({
        up: (doc) => ({
          ...doc,
          age: 25, // Default age for existing users
        }),
        down: (doc) => {
          const { age: _age, ...rest } = doc;
          return rest;
        },
      });
      return migration.compile();
    },
  });

  const result3 = await validateMigrationWithSimulation(migration3);
  assertEquals(result3.success, true, "Step 3: User age field should succeed");

  // ========================================
  // STEP 4: Add Comments System (new multi-collection instance)
  // ========================================
  const migration4 = migrationDefinition("2025_01_04_1000_COMMENTS", "add_comments_system", {
    parent: migration3,
    schemas: {
      collections: {
        ...migration3.schemas.collections,
      },
      multiCollections: {
        ...migration3.schemas.multiCollections,
        comments: {
          comment: {
            _id: v.string(),
            postId: v.string(),
            authorId: v.string(),
            text: v.string(),
            createdAt: v.date(),
          },
        },
      },
    },
    migrate(migration) {
      // Create comments for posts multi-collection
      migration
        .newMultiCollection("comments_posts_main", "comments")
        .seedType("comment", [
          {
            _id: "comment1",
            postId: "post1",
            authorId: "user2",
            text: "Great article!",
            createdAt: new Date("2025-01-04"),
          },
        ])
        .end();
      
      return migration.compile();
    },
  });

  const result4 = await validateMigrationWithSimulation(migration4);
  assertEquals(result4.success, true, "Step 4: Comments system should succeed");

  // ========================================
  // STEP 5: Add Likes Feature (array field with transformation)
  // ========================================
  const migration5 = migrationDefinition("2025_01_05_1000_LIKES", "add_likes_feature", {
    parent: migration4,
    schemas: {
      collections: {
        ...migration4.schemas.collections,
      },
      multiCollections: {
        ...migration4.schemas.multiCollections,
        posts: {
          article: {
            ...migration4.schemas.multiCollections.posts.article,
            likes: v.array(v.string()), // ← NEW FIELD: array of user IDs
          },
          video: {
            ...migration4.schemas.multiCollections.posts.video,
            likes: v.array(v.string()), // ← NEW FIELD
          },
        },
      },
    },
    migrate(migration) {
      // Transform articles to add likes array
      migration.multiCollection("posts").type("article").transform({
        up: (doc) => ({
          ...doc,
          likes: [], // Empty array for existing articles
        }),
        down: (doc) => {
          const { likes: _likes, ...rest } = doc;
          return rest;
        },
      });
      
      // Transform videos to add likes array
      migration.multiCollection("posts").type("video").transform({
        up: (doc) => ({
          ...doc,
          likes: [], // Empty array for existing videos
        }),
        down: (doc) => {
          const { likes: _likes, ...rest } = doc;
          return rest;
        },
      });
      
      return migration.compile();
    },
  });

  const result5 = await validateMigrationWithSimulation(migration5);
  assertEquals(result5.success, true, "Step 5: Likes feature should succeed");

  // ========================================
  // VALIDATION: All migrations are reversible
  // ========================================
  const result5WithReversibility = await validateMigrationWithSimulation(migration5, {
    validateReversibility: true,
  });
  assertEquals(
    result5WithReversibility.success,
    true,
    "All migrations should be reversible",
  );
});

Deno.test("Journey: Rollback to different points", async () => {
  // Setup complete migration chain
  const m1 = migrationDefinition("2025_01_01_ROOT", "step1", {
    parent: null,
    schemas: {
      collections: {
        data: {
          _id: v.string(),
          value: v.number(),
        },
      },
      multiCollections: {},
    },
    migrate(m) {
      m.createCollection("data").seed([
        { _id: "1", value: 10 },
      ]);
      return m.compile();
    },
  });

  const m2 = migrationDefinition("2025_01_02_STEP2", "step2", {
    parent: m1,
    schemas: {
      collections: {
        data: {
          _id: v.string(),
          value: v.number(),
          doubled: v.number(), // NEW FIELD
        },
      },
      multiCollections: {},
    },
    migrate(m) {
      m.collection("data").transform({
        up: (doc) => ({
          ...doc,
          doubled: (doc.value as number) * 2,
        }),
        down: (doc) => {
          const { doubled: _doubled, ...rest } = doc;
          return rest;
        },
      });
      return m.compile();
    },
  });

  const m3 = migrationDefinition("2025_01_03_STEP3", "step3", {
    parent: m2,
    schemas: {
      collections: {
        data: {
          _id: v.string(),
          value: v.number(),
          doubled: v.number(),
          tripled: v.number(), // NEW FIELD
        },
      },
      multiCollections: {},
    },
    migrate(m) {
      m.collection("data").transform({
        up: (doc) => ({
          ...doc,
          tripled: (doc.value as number) * 3,
        }),
        down: (doc) => {
          const { tripled: _tripled, ...rest } = doc;
          return rest;
        },
      });
      return m.compile();
    },
  });

  // Validate each migration step
  const r1 = await validateMigrationWithSimulation(m1, { validateReversibility: true });
  assertEquals(r1.success, true, "Migration 1 should succeed");

  const r2 = await validateMigrationWithSimulation(m2, { validateReversibility: true });
  assertEquals(r2.success, true, "Migration 2 should succeed and be reversible");

  const r3 = await validateMigrationWithSimulation(m3, { validateReversibility: true });
  assertEquals(r3.success, true, "Migration 3 should succeed and be reversible");
});

Deno.test("Journey: State verification at each step", async () => {
  // This test validates that data evolves correctly through the migration chain
  
  const step1 = migrationDefinition("2025_01_01_INIT", "init", {
    parent: null,
    schemas: {
      collections: {
        counter: {
          _id: v.string(),
          count: v.number(),
        },
      },
      multiCollections: {},
    },
    migrate(m) {
      m.createCollection("counter").seed([
        { _id: "main", count: 0 },
      ]);
      return m.compile();
    },
  });

  // Step 2: Add increment field
  const step2 = migrationDefinition("2025_01_02_INCREMENT", "add_increment", {
    parent: step1,
    schemas: {
      collections: {
        counter: {
          _id: v.string(),
          count: v.number(),
          increment: v.number(), // How much to add each time
        },
      },
      multiCollections: {},
    },
    migrate(m) {
      m.collection("counter").transform({
        up: (doc) => ({
          ...doc,
          increment: 1, // Default increment
        }),
        down: (doc) => {
          const { increment: _increment, ...rest } = doc;
          return rest;
        },
      });
      return m.compile();
    },
  });

  // Step 3: Add label field
  const step3 = migrationDefinition("2025_01_03_LABEL", "add_label", {
    parent: step2,
    schemas: {
      collections: {
        counter: {
          _id: v.string(),
          count: v.number(),
          increment: v.number(),
          label: v.string(), // Human readable label
        },
      },
      multiCollections: {},
    },
    migrate(m) {
      m.collection("counter").transform({
        up: (doc) => ({
          ...doc,
          label: `Counter: ${doc.count}`,
        }),
        down: (doc) => {
          const { label: _label, ...rest } = doc;
          return rest;
        },
      });
      return m.compile();
    },
  });

  // Validate progression
  const r1 = await validateMigrationWithSimulation(step1);
  assertEquals(r1.success, true, "Initial state should be valid");

  const r2 = await validateMigrationWithSimulation(step2);
  assertEquals(r2.success, true, "State after adding increment should be valid");

  const r3 = await validateMigrationWithSimulation(step3);
  assertEquals(r3.success, true, "Final state with label should be valid");
});

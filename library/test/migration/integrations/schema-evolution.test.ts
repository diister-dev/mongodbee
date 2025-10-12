/**
 * Integration test: Schema evolution patterns
 * 
 * Tests common schema evolution scenarios:
 * 1. Adding optional fields (no transformation needed)
 * 2. Making optional fields required (transformation needed)
 * 3. Type migrations (string → enum, number → formatted string)
 * 4. Restructuring (flat → nested objects)
 * 
 * @module
 */

import { assertEquals } from "@std/assert";
import * as v from "valibot";
import { migrationDefinition } from "../../../src/migration/definition.ts";
import { validateMigrationWithSimulation } from "../../../src/migration/validators/simulation.ts";

Deno.test("Schema Evolution: Optional to required field progression", async () => {
  // Step 1: Initial schema
  const m1 = migrationDefinition("2025_01_01_INIT", "init_products", {
    parent: null,
    schemas: {
      collections: {
        products: {
          _id: v.string(),
          name: v.string(),
          price: v.number(),
        },
      },
      multiModels: {},
    },
    migrate(m) {
      m.createCollection("products").seed([
        { _id: "p1", name: "Widget", price: 9.99 },
        { _id: "p2", name: "Gadget", price: 19.99 },
      ]);
      return m.compile();
    },
  });

  // Step 2: Add optional description field
  const m2 = migrationDefinition("2025_01_02_DESC", "add_optional_description", {
    parent: m1,
    schemas: {
      collections: {
        products: {
          _id: v.string(),
          name: v.string(),
          price: v.number(),
          description: v.optional(v.string()), // ← OPTIONAL field
        },
      },
      multiModels: {},
    },
    migrate(m) {
      // Even for optional fields, we need transformation if schema changed
      m.collection("products").transform({
        up: (doc) => ({
          ...doc,
          description: undefined, // Explicitly set to undefined for optional field
        }),
        down: (doc) => {
          const { description: _description, ...rest } = doc;
          return rest;
        },
      });
      return m.compile();
    },
  });

  // Step 3: Make description required (needs transformation)
  const m3 = migrationDefinition("2025_01_03_DESC_REQ", "make_description_required", {
    parent: m2,
    schemas: {
      collections: {
        products: {
          _id: v.string(),
          name: v.string(),
          price: v.number(),
          description: v.string(), // ← NOW REQUIRED
        },
      },
      multiModels: {},
    },
    migrate(m) {
      // Transform to provide default description for products without one
      m.collection("products").transform({
        up: (doc) => ({
          ...doc,
          description: doc.description || "No description available",
        }),
        down: (doc) => ({
          ...doc,
          description: undefined, // Back to optional
        }),
      });
      return m.compile();
    },
  });

  const r1 = await validateMigrationWithSimulation(m1);
  assertEquals(r1.success, true, "Initial schema should be valid");

  const r2 = await validateMigrationWithSimulation(m2);
  assertEquals(r2.success, true, "Adding optional field should not require transformation");

  const r3 = await validateMigrationWithSimulation(m3);
  assertEquals(r3.success, true, "Making field required should succeed with transformation");
});

Deno.test("Schema Evolution: Type migration (string → enum)", async () => {
  // Step 1: Status as free-form string
  const m1 = migrationDefinition("2025_01_01_ORDERS", "create_orders", {
    parent: null,
    schemas: {
      collections: {
        orders: {
          _id: v.string(),
          status: v.string(), // Free-form string
          total: v.number(),
        },
      },
      multiModels: {},
    },
    migrate(m) {
      m.createCollection("orders").seed([
        { _id: "o1", status: "pending", total: 100 },
        { _id: "o2", status: "completed", total: 200 },
        { _id: "o3", status: "cancelled", total: 50 },
      ]);
      return m.compile();
    },
  });

  // Step 2: Migrate to enum (controlled values)
  const m2 = migrationDefinition("2025_01_02_STATUS_ENUM", "migrate_status_to_enum", {
    parent: m1,
    schemas: {
      collections: {
        orders: {
          _id: v.string(),
          status: v.picklist(["pending", "processing", "completed", "cancelled"]), // ← ENUM
          total: v.number(),
        },
      },
      multiModels: {},
    },
    migrate(m) {
      // Transform to normalize status values
      m.collection("orders").transform({
        up: (doc) => {
          const status = (doc.status as string).toLowerCase();
          // Map old values to new enum values
          const statusMap: Record<string, string> = {
            "pending": "pending",
            "in progress": "processing",
            "processing": "processing",
            "done": "completed",
            "completed": "completed",
            "canceled": "cancelled",
            "cancelled": "cancelled",
          };
          return {
            ...doc,
            status: statusMap[status] || "pending",
          };
        },
        down: (doc) => ({
          ...doc,
          status: doc.status, // Enum values are still valid strings
        }),
      });
      return m.compile();
    },
  });

  const r1 = await validateMigrationWithSimulation(m1);
  assertEquals(r1.success, true, "Free-form string status should be valid");

  const r2 = await validateMigrationWithSimulation(m2);
  assertEquals(r2.success, true, "Migrating to enum with normalization should succeed");
});

Deno.test("Schema Evolution: Restructuring (flat → nested)", async () => {
  // Step 1: Flat structure
  const m1 = migrationDefinition("2025_01_01_USERS_FLAT", "create_users_flat", {
    parent: null,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.string(),
          street: v.string(),
          city: v.string(),
          country: v.string(),
        },
      },
      multiModels: {},
    },
    migrate(m) {
      m.createCollection("users").seed([
        {
          _id: "u1",
          name: "Alice",
          email: "alice@example.com",
          street: "123 Main St",
          city: "Springfield",
          country: "USA",
        },
      ]);
      return m.compile();
    },
  });

  // Step 2: Nest address fields
  const m2 = migrationDefinition("2025_01_02_NEST_ADDRESS", "nest_address_fields", {
    parent: m1,
    schemas: {
      collections: {
        users: {
          _id: v.string(),
          name: v.string(),
          email: v.string(),
          address: v.object({ // ← NESTED structure
            street: v.string(),
            city: v.string(),
            country: v.string(),
          }),
        },
      },
      multiModels: {},
    },
    migrate(m) {
      // Transform to nest address fields
      m.collection("users").transform({
        up: (doc) => ({
          _id: doc._id,
          name: doc.name,
          email: doc.email,
          address: {
            street: doc.street as string,
            city: doc.city as string,
            country: doc.country as string,
          },
        }),
        down: (doc) => ({
          _id: doc._id,
          name: doc.name,
          email: doc.email,
          street: (doc.address as { street: string }).street,
          city: (doc.address as { city: string }).city,
          country: (doc.address as { country: string }).country,
        }),
      });
      return m.compile();
    },
  });

  const r1 = await validateMigrationWithSimulation(m1);
  assertEquals(r1.success, true, "Flat structure should be valid");

  const r2 = await validateMigrationWithSimulation(m2);
  assertEquals(r2.success, true, "Nesting fields should succeed with transformation");
});

Deno.test("Schema Evolution: Number to formatted string", async () => {
  // Step 1: Price as number
  const m1 = migrationDefinition("2025_01_01_PRICE_NUM", "price_as_number", {
    parent: null,
    schemas: {
      collections: {
        items: {
          _id: v.string(),
          name: v.string(),
          price: v.number(), // Number in cents
        },
      },
      multiModels: {},
    },
    migrate(m) {
      m.createCollection("items").seed([
        { _id: "i1", name: "Coffee", price: 350 }, // 3.50 USD in cents
        { _id: "i2", name: "Tea", price: 250 }, // 2.50 USD
      ]);
      return m.compile();
    },
  });

  // Step 2: Migrate to formatted string with currency
  const m2 = migrationDefinition("2025_01_02_PRICE_STR", "price_as_formatted_string", {
    parent: m1,
    schemas: {
      collections: {
        items: {
          _id: v.string(),
          name: v.string(),
          price: v.pipe(
            v.string(),
            v.regex(/^\$-?\d+\.\d{2}$/), // Format: $XX.XX
          ),
        },
      },
      multiModels: {},
    },
    migrate(m) {
      // Transform number to formatted currency string
      m.collection("items").transform({
        up: (doc) => ({
          ...doc,
          price: `$${((doc.price as number) / 100).toFixed(2)}`,
        }),
        down: (doc) => ({
          ...doc,
          price: Math.round(parseFloat((doc.price as string).slice(1)) * 100),
        }),
      });
      return m.compile();
    },
  });

  const r1 = await validateMigrationWithSimulation(m1);
  assertEquals(r1.success, true, "Numeric price should be valid");

  const r2 = await validateMigrationWithSimulation(m2);
  assertEquals(r2.success, true, "Formatted string price should succeed with transformation");
});

Deno.test("Schema Evolution: Adding validation constraints progressively", async () => {
  // Step 1: Basic email field
  const m1 = migrationDefinition("2025_01_01_EMAIL_BASIC", "email_no_validation", {
    parent: null,
    schemas: {
      collections: {
        contacts: {
          _id: v.string(),
          email: v.string(), // No validation
        },
      },
      multiModels: {},
    },
    migrate(m) {
      m.createCollection("contacts").seed([
        { _id: "c1", email: "alice@example.com" },
        { _id: "c2", email: "bob@test.org" },
      ]);
      return m.compile();
    },
  });

  // Step 2: Add email format validation
  const m2 = migrationDefinition("2025_01_02_EMAIL_VALID", "add_email_validation", {
    parent: m1,
    schemas: {
      collections: {
        contacts: {
          _id: v.string(),
          email: v.pipe(v.string(), v.email()), // ← Added validation
        },
      },
      multiModels: {},
    },
    migrate(m) {
      // Transformation to clean up invalid emails
      m.collection("contacts").transform({
        up: (doc) => {
          const email = doc.email as string;
          // Simple validation: contains @ and .
          if (!email.includes("@") || !email.includes(".")) {
            return {
              ...doc,
              email: "invalid@example.com", // Placeholder for invalid emails
            };
          }
          return doc;
        },
        down: (doc) => doc, // No change needed going back
      });
      return m.compile();
    },
  });

  const r1 = await validateMigrationWithSimulation(m1);
  assertEquals(r1.success, true, "Unvalidated email should be valid");

  const r2 = await validateMigrationWithSimulation(m2);
  assertEquals(r2.success, true, "Adding email validation should succeed with cleanup");
});

Deno.test("Schema Evolution: Array field addition and transformation", async () => {
  // Step 1: Single tag as string
  const m1 = migrationDefinition("2025_01_01_TAG_SINGLE", "single_tag", {
    parent: null,
    schemas: {
      collections: {
        posts: {
          _id: v.string(),
          title: v.string(),
          tag: v.string(), // Single tag
        },
      },
      multiModels: {},
    },
    migrate(m) {
      m.createCollection("posts").seed([
        { _id: "p1", title: "Post 1", tag: "tech" },
        { _id: "p2", title: "Post 2", tag: "news" },
      ]);
      return m.compile();
    },
  });

  // Step 2: Multiple tags as array
  const m2 = migrationDefinition("2025_01_02_TAGS_ARRAY", "tags_as_array", {
    parent: m1,
    schemas: {
      collections: {
        posts: {
          _id: v.string(),
          title: v.string(),
          tags: v.array(v.string()), // ← Array of tags
        },
      },
      multiModels: {},
    },
    migrate(m) {
      // Transform single tag to array
      m.collection("posts").transform({
        up: (doc) => ({
          _id: doc._id,
          title: doc.title,
          tags: [doc.tag as string], // Wrap in array
        }),
        down: (doc) => ({
          _id: doc._id,
          title: doc.title,
          tag: ((doc.tags as string[])[0]) || "untagged", // Take first tag
        }),
      });
      return m.compile();
    },
  });

  const r1 = await validateMigrationWithSimulation(m1);
  assertEquals(r1.success, true, "Single tag should be valid");

  const r2 = await validateMigrationWithSimulation(m2);
  assertEquals(r2.success, true, "Converting to array should succeed");
});

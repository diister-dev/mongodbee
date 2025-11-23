import * as v from "../src/schema.ts";
import { assertEquals, assertRejects } from "@std/assert";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { withIndex } from "../src/indexes.ts";
import { withDatabase } from "./+shared.ts";
import { defineModel } from "../src/multi-collection-model.ts";

/**
 * Tests for union schemas with indexes
 * Covers the case where withIndex is applied to union schemas
 */

Deno.test("withIndex - Union schemas with unique constraints", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Create union schema similar to SIRET/SIREN
    const NumberOrString = v.union([v.string(), v.number()]);

    const testSchema = {
      id: withIndex(v.string(), { unique: true }),
      value: withIndex(NumberOrString, { unique: true }),
      description: v.optional(v.string()),
    };

    const coll = await collection(db, "union_test", testSchema, { schemaManagement: "auto" });

    // First document with string value
    await coll.insertOne({
      id: "test1",
      value: "string_value",
      description: "String test",
    });

    // Second document with number value
    await coll.insertOne({
      id: "test2",
      value: 42,
      description: "Number test",
    });

    // Should prevent duplicate string value
    await assertRejects(
      async () => {
        await coll.insertOne({
          id: "test3",
          value: "string_value", // Same as first
        });
      },
      Error,
      "duplicate key",
    );

    // Should prevent duplicate number value
    await assertRejects(
      async () => {
        await coll.insertOne({
          id: "test4",
          value: 42, // Same as second
        });
      },
      Error,
      "duplicate key",
    );

    // Verify indexes were created
    const indexes = await coll.collection.listIndexes().toArray();
    const valueIndex = indexes.find((idx: Record<string, unknown>) =>
      idx.key && (idx.key as Record<string, unknown>).value
    );
    assertEquals(valueIndex?.unique, true);
  });
});

Deno.test("withIndex - Complex nested union schemas", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Complex validation schemas like SIRET/SIREN
    const SiretSchema = v.pipe(
      v.string(),
      v.regex(/^[0-9]+$/),
      v.minLength(14),
      v.maxLength(14),
    );

    const SirenSchema = v.pipe(
      v.string(),
      v.regex(/^[0-9]+$/),
      v.minLength(9),
      v.maxLength(9),
    );

    const SiretOrSirenSchema = v.union([SiretSchema, SirenSchema]);

    const companySchema = {
      id: withIndex(v.string(), { unique: true }),
      siretOrSiren: withIndex(SiretOrSirenSchema, { unique: true }),
      name: v.string(),
    };

    const coll = await collection(db, "complex_union_test", companySchema, { schemaManagement: "auto" });

    // Insert company with SIRET (14 digits)
    await coll.insertOne({
      id: "company1",
      siretOrSiren: "12345678901234",
      name: "Company with SIRET",
    });

    // Insert company with SIREN (9 digits)
    await coll.insertOne({
      id: "company2",
      siretOrSiren: "123456789",
      name: "Company with SIREN",
    });

    // Should prevent duplicate SIRET
    await assertRejects(
      async () => {
        await coll.insertOne({
          id: "company3",
          siretOrSiren: "12345678901234", // Same SIRET
          name: "Duplicate SIRET Company",
        });
      },
      Error,
      "duplicate key",
    );

    // Should prevent duplicate SIREN
    await assertRejects(
      async () => {
        await coll.insertOne({
          id: "company4",
          siretOrSiren: "123456789", // Same SIREN
          name: "Duplicate SIREN Company",
        });
      },
      Error,
      "duplicate key",
    );

    // Verify unique constraint works
    const count = await coll.countDocuments({});
    assertEquals(count, 2);
  });
});

Deno.test("withIndex - Multi-collection with union schemas", async (t) => {
  await withDatabase(t.name, async (db) => {
    const IdUnion = v.union([v.string(), v.number()]);

    const catalogSchema = {
      user: {
        userId: withIndex(IdUnion, { unique: true }),
        name: v.string(),
      },
      product: {
        productId: withIndex(IdUnion, { unique: true }),
        title: v.string(),
      },
    };

    const catalogModel = defineModel("multi_union_test", {
      schema: catalogSchema,
    });

    const multiColl = await multiCollection(
      db,
      "multi_union_test",
      catalogModel,
      { schemaManagement: "auto" },
    );

    // Insert user with string ID
    await multiColl.insertOne("user", {
      userId: "user123",
      name: "John Doe",
    });

    // Insert user with number ID
    await multiColl.insertOne("user", {
      userId: 456,
      name: "Jane Doe",
    });

    // Insert product with string ID
    await multiColl.insertOne("product", {
      productId: "prod789",
      title: "Product A",
    });

    // Should prevent duplicate user string ID
    await assertRejects(
      async () => {
        await multiColl.insertOne("user", {
          userId: "user123", // Same string ID
          name: "Duplicate User",
        });
      },
      Error,
      "duplicate key",
    );

    // Should prevent duplicate user number ID
    await assertRejects(
      async () => {
        await multiColl.insertOne("user", {
          userId: 456, // Same number ID
          name: "Another Duplicate",
        });
      },
      Error,
      "duplicate key",
    );

    // Should allow same ID across different types (scoped by type)
    await multiColl.insertOne("product", {
      productId: "user123", // Same as user string ID but different type
      title: "Product B",
    });

    const userCount = await multiColl.countDocuments("user");
    const productCount = await multiColl.countDocuments("product");

    assertEquals(userCount, 2);
    assertEquals(productCount, 2);
  });
});

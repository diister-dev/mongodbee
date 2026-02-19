import * as v from "../src/schema.ts";
import { test, expect } from "vitest";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { withIndex } from "../src/indexes.ts";
import { withDatabase } from "./+shared.ts";
import { MongoServerError } from "mongodb";
import { slug } from "@diister/mongodbee/schema";
import { defineModel } from "../src/multi-collection-model.ts";

test("withIndex - Basic index creation", async () => {
  await withDatabase("withIndex - Basic index creation", async (db) => {
    const userSchema = {
      username: withIndex(v.string()),
      email: v.string(),
      age: v.number(),
    };

    // Create collection with basic index on username field
    const users = await collection(db, "users", userSchema, { schemaManagement: "auto" });

    // Verify the collection was created
    expect(users).toBeDefined();

    // Insert test data
    await users.insertOne({
      username: "testuser",
      email: "test@example.com",
      age: 25,
    });

    // Verify index exists by checking collection indexes
    const indexes = await users.collection.listIndexes().toArray();
    const usernameIndex = indexes.find((idx) => idx.key?.username === 1);
    expect(usernameIndex).toBeDefined();
  });
});

test("withIndex - Unique index constraint", async () => {
  await withDatabase("withIndex - Unique index constraint", async (db) => {
    const userSchema = {
      username: v.string(),
      email: withIndex(v.string(), { unique: true }),
    };

    // Create collection with unique index on email field
    const users = await collection(db, "users", userSchema, { schemaManagement: "auto" });

    // Insert first user
    await users.insertOne({
      username: "user1",
      email: "test@example.com",
    });

    // Try to insert second user with same email - should fail
    await expect(
      async () => {
        await users.insertOne({
          username: "user2",
          email: "test@example.com",
        });
      },
    ).rejects.toThrow("duplicate key");
  });
});

test("withIndex - Case insensitive index", async () => {
  await withDatabase("withIndex - Case insensitive index", async (db) => {
    const userSchema = {
      username: v.string(),
      email: withIndex(v.string(), { unique: true, insensitive: true }),
    };

    // Create collection with case insensitive unique index on email field
    const users = await collection(db, "users", userSchema, { schemaManagement: "auto" });

    // Insert first user
    await users.insertOne({
      username: "user1",
      email: "Test@Example.com",
    });

    // Try to insert second user with different case email - should fail due to case insensitive index
    await expect(
      async () => {
        await users.insertOne({
          username: "user2",
          email: "test@EXAMPLE.com",
        });
      },
    ).rejects.toThrow("duplicate key");
  });
});

test("withIndex - Custom collation", async () => {
  await withDatabase("withIndex - Custom collation", async (db) => {
    const userSchema = {
      name: withIndex(v.string(), {
        unique: true,
        collation: { locale: "en", strength: 2 },
      }),
      email: v.string(),
    };

    // Create collection with custom collation on name field
    const users = await collection(db, "users", userSchema, { schemaManagement: "auto" });

    // Insert first user
    await users.insertOne({
      name: "jose",
      email: "jose@example.com",
    });

    // Try to insert user with accent differences - should fail due to collation
    await expect(
      async () => {
        await users.insertOne({
          name: "Jose", // Without accent
          email: "jose2@example.com",
        });
      },
    ).rejects.toThrow("duplicate key");
  });
});

test("withIndex - Multiple indexes on different fields", async () => {
  await withDatabase("withIndex - Multiple indexes on different fields", async (db) => {
    const userSchema = {
      username: withIndex(v.string(), { unique: true }),
      email: withIndex(v.string(), { unique: true }),
      age: withIndex(v.number()),
      status: v.string(),
    };

    // Create collection with multiple indexes on different fields
    const users = await collection(db, "users", userSchema, { schemaManagement: "auto" });

    // Verify all indexes were created
    const indexes = await users.collection.listIndexes().toArray();

    // Should have default _id index plus our 3 custom indexes
    expect(indexes.length).toEqual(4);

    const usernameIndex = indexes.find((idx) =>
      idx.key?.username === 1 && idx.unique === true
    );
    const emailIndex = indexes.find((idx) =>
      idx.key?.email === 1 && idx.unique === true
    );
    const ageIndex = indexes.find((idx) => idx.key?.age === 1 && !idx.unique);

    expect(usernameIndex).toBeDefined();
    expect(emailIndex).toBeDefined();
    expect(ageIndex).toBeDefined();
  });
});

test("withIndex - Multi-collection with type scoped indexes", async () => {
  await withDatabase("withIndex - Multi-collection with type scoped indexes", async (db) => {
    const catalogSchema = {
      product: {
        name: v.string(),
        sku: withIndex(v.string(), { unique: true }),
        price: v.number(),
      },
      category: {
        name: v.string(),
        slug: withIndex(v.string(), { unique: true }),
      },
    };

    // Create multi-collection with unique indexes on different fields per type
    const catalog = await multiCollection(
      db,
      "catalog",
      defineModel("catalog", {
        schema: catalogSchema,
      }),
      { schemaManagement: "auto" },
    );

    // Insert products with unique SKUs
    await catalog.insertOne("product", {
      name: "Laptop",
      sku: "LAP001",
      price: 999.99,
    });

    await catalog.insertOne("product", {
      name: "Mouse",
      sku: "MOU001",
      price: 29.99,
    });

    // Insert categories with unique slugs
    await catalog.insertOne("category", {
      name: "Electronics",
      slug: "electronics",
    });

    await catalog.insertOne("category", {
      name: "Computers",
      slug: "computers",
    });

    // Try to insert product with duplicate SKU - should fail
    await expect(
      async () => {
        await catalog.insertOne("product", {
          name: "Another Laptop",
          sku: "LAP001", // Duplicate SKU
          price: 1299.99,
        });
      },
    ).rejects.toThrow("duplicate key");

    // Try to insert category with duplicate slug - should fail
    await expect(
      async () => {
        await catalog.insertOne("category", {
          name: "Electronics 2",
          slug: "electronics", // Duplicate slug
        });
      },
    ).rejects.toThrow("duplicate key");

    // Verify that same values can exist across different types
    // This should succeed because indexes are type-scoped
    await catalog.insertOne("category", {
      name: "Laptops Category",
      slug: "LAP001", // Same as product SKU, but different type
    });
  });
});

test("withIndex - Multi-collection with scoped indexes by type", async () => {
  await withDatabase("withIndex - Multi-collection with scoped indexes by type", async (db) => {
    const catalogSchema = {
      products: {
        name: v.string(),
        price: v.number(),
        slug: withIndex(v.string(), { unique: true }),
      },
      cars: {
        name: v.string(),
        model: v.string(),
        slug: withIndex(v.string(), { unique: true }),
      },
    };

    const catalog = await multiCollection(
      db,
      "catalog",
      defineModel("catalog", { schema: catalogSchema }),
      { schemaManagement: "auto" },
    );

    // Insert product with unique slug
    await catalog.insertOne("products", {
      name: "Laptop",
      price: 999.99,
      slug: "laptop-2023",
    });

    // Insert car with unique slug
    await catalog.insertOne("cars", {
      name: "Tesla",
      model: "Model S",
      slug: "tesla-model-s",
    });

    // Insert a car with same slug as product - should work because indexes are scoped by type
    await catalog.insertOne("cars", {
      name: "Another Tesla",
      model: "Model 3",
      slug: "laptop-2023", // Same slug as product
    });

    // Insert another product with same slug - should fail
    await expect(
      async () => {
        await catalog.insertOne("products", {
          name: "Gaming Laptop",
          price: 1499.99,
          slug: "laptop-2023", // Duplicate slug for products
        });
      },
    ).rejects.toThrow("duplicate key");

    // Insert another car with same slug - should fail
    await expect(
      async () => {
        await catalog.insertOne("cars", {
          name: "Luxury Car",
          model: "Model X",
          slug: "tesla-model-s", // Duplicate slug for cars
        });
      },
    ).rejects.toThrow("duplicate key");

    // Verify products and cars can be queried correctly
    const products = await catalog.find("products").toArray();
    const cars = await catalog.find("cars").toArray();
    expect(products.length).toEqual(1);
    expect(cars.length).toEqual(2);
    expect(products[0].slug).toEqual("laptop-2023");
    expect(cars[0].slug).toEqual("tesla-model-s");
    expect(cars[1].slug).toEqual("laptop-2023"); // Car with same slug as product
    expect(cars[1]._type).toEqual("cars"); // Ensure type is preserved
    expect(products[0]._type).toEqual("products"); // Ensure type is preserved
  });
});

test("withIndex - Multi-collection with scoped deep indexes by type", async () => {
  await withDatabase("withIndex - Multi-collection with scoped deep indexes by type", async (db) => {
    const catalogSchema = {
      products: {
        name: v.string(),
        details: v.object({
          price: v.number(),
          slug: withIndex(v.string(), { unique: true }),
        }),
      },
      cars: {
        name: v.string(),
        details: v.object({
          model: v.string(),
          slug: withIndex(v.string(), { unique: true }),
        }),
      },
    };

    const catalog = await multiCollection(
      db,
      "catalog",
      defineModel("catalog", { schema: catalogSchema }),
      { schemaManagement: "auto" },
    );

    // Insert product with unique slug
    await catalog.insertOne("products", {
      name: "Laptop",
      details: {
        price: 999.99,
        slug: "laptop-2023",
      },
    });

    // Insert car with unique slug
    await catalog.insertOne("cars", {
      name: "Tesla",
      details: {
        model: "Model S",
        slug: "tesla-model-s",
      },
    });

    // Insert a car with same slug as product - should work because indexes are scoped by type
    await catalog.insertOne("cars", {
      name: "Another Tesla",
      details: {
        model: "Model 3",
        slug: "laptop-2023", // Same slug as product
      },
    });

    // Insert another product with same slug - should fail
    await expect(
      async () => {
        await catalog.insertOne("products", {
          name: "Gaming Laptop",
          details: {
            price: 1499.99,
            slug: "laptop-2023", // Duplicate slug for products
          },
        });
      },
    ).rejects.toThrow("duplicate key");

    // Insert another car with same slug - should fail
    await expect(
      async () => {
        await catalog.insertOne("cars", {
          name: "Luxury Car",
          details: {
            model: "Model X",
            slug: "tesla-model-s", // Duplicate slug for cars
          },
        });
      },
    ).rejects.toThrow("duplicate key");

    // Verify products and cars can be queried correctly
    const products = await catalog.find("products").toArray();
    const cars = await catalog.find("cars").toArray();
    expect(products.length).toEqual(1);
    expect(cars.length).toEqual(2);
    expect(products[0].details.slug).toEqual("laptop-2023");
    expect(cars[0].details.slug).toEqual("tesla-model-s");
    expect(cars[1].details.slug).toEqual("laptop-2023"); // Car with same slug as product
    expect(cars[1]._type).toEqual("cars"); // Ensure type is preserved
    expect(products[0]._type).toEqual("products"); // Ensure type is preserved
  });
});

test("withIndex - Automatic type field in multi-collection", async () => {
  await withDatabase("withIndex - Automatic type field in multi-collection", async (db) => {
    const catalogSchema = {
      product: {
        name: v.string(),
        price: v.number(),
      },
      category: {
        name: v.string(),
      },
    };

    const catalog = await multiCollection(
      db,
      "catalog",
      defineModel("catalog", { schema: catalogSchema }),
      { schemaManagement: "auto" },
    );

    // Insert documents
    await catalog.insertOne("product", {
      name: "Test Product",
      price: 100,
    });

    await catalog.insertOne("category", {
      name: "Test Category",
    });

    // Verify type field is automatically added
    const products = await catalog.find("product").toArray();
    const categories = await catalog.find("category").toArray();

    expect(products.length).toEqual(1);
    expect(categories.length).toEqual(1);
    expect(products[0]._type).toEqual("product");
    expect(categories[0]._type).toEqual("category");
  });
});

test("withIndex - Union schemas with unique constraints", async () => {
  await withDatabase("withIndex - Union schemas with unique constraints", async (db) => {
    // Test union schema like SIRET/SIREN
    const NumberOrString = v.union([v.string(), v.number()]);

    const testSchema = {
      id: withIndex(v.string(), { unique: true }),
      value: withIndex(NumberOrString, { unique: true }),
      description: v.optional(v.string()),
    };

    const coll = await collection(db, "union_test", testSchema, { schemaManagement: "auto" });

    // Insert documents with different union types
    await coll.insertOne({
      id: "test1",
      value: "string_value",
      description: "String test",
    });

    await coll.insertOne({
      id: "test2",
      value: 42,
      description: "Number test",
    });

    // Should prevent duplicate string value
    await expect(
      async () => {
        await coll.insertOne({
          id: "test3",
          value: "string_value", // Same as first
        });
      },
    ).rejects.toThrow("duplicate key");

    // Should prevent duplicate number value
    await expect(
      async () => {
        await coll.insertOne({
          id: "test4",
          value: 42, // Same as second
        });
      },
    ).rejects.toThrow("duplicate key");

    // Verify indexes were created correctly
    const indexes = await coll.collection.listIndexes().toArray();
    const valueIndex = indexes.find((idx) => idx.key?.value === 1);
    expect(valueIndex).toBeDefined();
    expect(valueIndex.unique).toEqual(true);
  });
});

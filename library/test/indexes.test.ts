import * as v from "../src/schema.ts";
import { assertEquals, assertExists, assertRejects } from "jsr:@std/assert";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { withIndex } from "../src/indexes.ts";
import { withDatabase } from "./+shared.ts";
import { MongoServerError } from "mongodb";

Deno.test("withIndex - Basic index creation", async (t) => {
  await withDatabase(t.name, async (db) => {
    const userSchema = {
      username: withIndex(v.string()),
      email: v.string(),
      age: v.number()
    };

    // Create collection with basic index on username field
    const users = await collection(db, "users", userSchema);

    // Verify the collection was created
    assertExists(users);

    // Insert test data
    await users.insertOne({
      username: "testuser",
      email: "test@example.com",
      age: 25
    });

    // Verify index exists by checking collection indexes
    const indexes = await users.collection.listIndexes().toArray();
    const usernameIndex = indexes.find(idx => idx.key?.username === 1);
    assertExists(usernameIndex);
  });
});

Deno.test("withIndex - Unique index constraint", async (t) => {
  await withDatabase(t.name, async (db) => {
    const userSchema = {
      username: v.string(),
      email: withIndex(v.string(), { unique: true })
    };

    // Create collection with unique index on email field
    const users = await collection(db, "users", userSchema);

    // Insert first user
    await users.insertOne({
      username: "user1",
      email: "test@example.com"
    });

    // Try to insert second user with same email - should fail
    await assertRejects(
      async () => {
        await users.insertOne({
          username: "user2",
          email: "test@example.com"
        });
      },
      MongoServerError,
      "duplicate key"
    );
  });
});

Deno.test("withIndex - Case insensitive index", async (t) => {
  await withDatabase(t.name, async (db) => {
    const userSchema = {
      username: v.string(),
      email: withIndex(v.string(), { unique: true, insensitive: true })
    };

    // Create collection with case insensitive unique index on email field
    const users = await collection(db, "users", userSchema);

    // Insert first user
    await users.insertOne({
      username: "user1",
      email: "Test@Example.com"
    });

    // Try to insert second user with different case email - should fail due to case insensitive index
    await assertRejects(
      async () => {
        await users.insertOne({
          username: "user2",
          email: "test@EXAMPLE.com"
        });
      },
      MongoServerError,
      "duplicate key"
    );
  });
});

Deno.test("withIndex - Custom collation", async (t) => {
  await withDatabase(t.name, async (db) => {
    const userSchema = {
      name: withIndex(v.string(), { 
        unique: true,
        collation: { locale: "en", strength: 2 }
      }),
      email: v.string()
    };

    // Create collection with custom collation on name field
    const users = await collection(db, "users", userSchema);

    // Insert first user
    await users.insertOne({
      name: "jose",
      email: "jose@example.com"
    });

    // Try to insert user with accent differences - should fail due to collation
    await assertRejects(
      async () => {
        await users.insertOne({
          name: "Jose", // Without accent
          email: "jose2@example.com"
        });
      },
      MongoServerError,
      "duplicate key"
    );
  });
});

Deno.test("withIndex - Multiple indexes on different fields", async (t) => {
  await withDatabase(t.name, async (db) => {
    const userSchema = {
      username: withIndex(v.string(), { unique: true }),
      email: withIndex(v.string(), { unique: true }),
      age: withIndex(v.number()),
      status: v.string()
    };

    // Create collection with multiple indexes on different fields
    const users = await collection(db, "users", userSchema);

    // Verify all indexes were created
    const indexes = await users.collection.listIndexes().toArray();
    
    // Should have default _id index plus our 3 custom indexes
    assertEquals(indexes.length, 4);
    
    const usernameIndex = indexes.find(idx => idx.key?.username === 1 && idx.unique === true);
    const emailIndex = indexes.find(idx => idx.key?.email === 1 && idx.unique === true);
    const ageIndex = indexes.find(idx => idx.key?.age === 1 && !idx.unique);
    
    assertExists(usernameIndex);
    assertExists(emailIndex);
    assertExists(ageIndex);
  });
});

Deno.test("withIndex - Multi-collection with type scoped indexes", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogSchema = {
      product: {
        name: v.string(),
        sku: withIndex(v.string(), { unique: true }),
        price: v.number()
      },
      category: {
        name: v.string(),
        slug: withIndex(v.string(), { unique: true })
      }
    };

    // Create multi-collection with unique indexes on different fields per type
    const catalog = await multiCollection(db, "catalog", catalogSchema);

    // Insert products with unique SKUs
    await catalog.insertOne("product", {
      name: "Laptop",
      sku: "LAP001",
      price: 999.99
    });

    await catalog.insertOne("product", {
      name: "Mouse",
      sku: "MOU001",
      price: 29.99
    });

    // Insert categories with unique slugs
    await catalog.insertOne("category", {
      name: "Electronics",
      slug: "electronics"
    });

    await catalog.insertOne("category", {
      name: "Computers",
      slug: "computers"
    });

    // Try to insert product with duplicate SKU - should fail
    await assertRejects(
      async () => {
        await catalog.insertOne("product", {
          name: "Another Laptop",
          sku: "LAP001", // Duplicate SKU
          price: 1299.99
        });
      },
      MongoServerError,
      "duplicate key"
    );

    // Try to insert category with duplicate slug - should fail
    await assertRejects(
      async () => {
        await catalog.insertOne("category", {
          name: "Electronics 2",
          slug: "electronics" // Duplicate slug
        });
      },
      MongoServerError,
      "duplicate key"
    );

    // Verify that same values can exist across different types
    // This should succeed because indexes are type-scoped
    await catalog.insertOne("category", {
      name: "Laptops Category",
      slug: "LAP001" // Same as product SKU, but different type
    });
  });
});

Deno.test("withIndex - Automatic type field in multi-collection", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogSchema = {
      product: {
        name: v.string(),
        price: v.number()
      },
      category: {
        name: v.string()
      }
    };

    const catalog = await multiCollection(db, "catalog", catalogSchema);

    // Insert documents
    await catalog.insertOne("product", {
      name: "Test Product",
      price: 100
    });

    await catalog.insertOne("category", {
      name: "Test Category"
    });

    // Verify type field is automatically added
    const products = await catalog.find("product");
    const categories = await catalog.find("category");

    assertEquals(products.length, 1);
    assertEquals(categories.length, 1);
    assertEquals(products[0].type, "product");
    assertEquals(categories[0].type, "category");
  });
});



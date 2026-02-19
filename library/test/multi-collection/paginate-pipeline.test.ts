import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import * as v from "../../src/schema.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

// Test schemas for multi-collection
const userSchema = {
  name: v.string(),
  age: v.number(),
  email: v.string(),
  isActive: v.boolean(),
} as const;

const productSchema = {
  name: v.string(),
  price: v.number(),
  category: v.string(),
  inStock: v.boolean(),
} as const;

const collectionSchema = {
  users: userSchema,
  products: productSchema,
} as const;

const collectionModel = defineModel("multi_test", {
  schema: collectionSchema,
});

test("Multi-Collection: Basic prepare → filter → format", async () => {
  await withDatabase("Multi-Collection: Basic prepare → filter → format", async (db) => {
    const mc = await multiCollection(db, "multi_test", collectionModel);

    // Insert test data
    await mc.insertOne("users", {
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });
    await mc.insertOne("users", {
      name: "Bob",
      age: 30,
      email: "bob@test.com",
      isActive: false,
    });
    await mc.insertOne("users", {
      name: "Charlie",
      age: 35,
      email: "charlie@test.com",
      isActive: true,
    });

    const results = await mc.paginate("users", {}, {
      // Step 1: Prepare (enrich with computed field)
      prepare: async (user) => ({
        ...user,
        ageGroup: user.age < 30 ? "young" : "adult",
        emailDomain: user.email.split("@")[1],
      }),

      // Step 2: Filter (only active users)
      filter: (enrichedUser) => enrichedUser.isActive,

      // Step 3: Format (return simplified format)
      format: async (enrichedUser) => ({
        displayName: enrichedUser.name,
        category: enrichedUser.ageGroup,
        domain: enrichedUser.emailDomain,
        type: enrichedUser._type,
      }),
    });

    expect(results.data.length === 2).toBeTruthy();
    expect(
      results.data[0].displayName === "Alice",
    ).toBeTruthy();
    expect(results.data[0].category === "young").toBeTruthy();
    expect(results.data[0].domain === "test.com").toBeTruthy();
    expect(results.data[0].type === "users").toBeTruthy();
    expect(
      results.data[1].displayName === "Charlie",
    ).toBeTruthy();
    expect(results.data[1].category === "adult").toBeTruthy();
  });
});

test("Multi-Collection: Products with pricing logic", async () => {
  await withDatabase("Multi-Collection: Products with pricing logic", async (db) => {
    const mc = await multiCollection(db, "multi_test", collectionModel);

    // Insert test products
    await mc.insertOne("products", {
      name: "Laptop",
      price: 999,
      category: "electronics",
      inStock: true,
    });
    await mc.insertOne("products", {
      name: "Mouse",
      price: 25,
      category: "electronics",
      inStock: false,
    });
    await mc.insertOne("products", {
      name: "Book",
      price: 15,
      category: "books",
      inStock: true,
    });

    const results = await mc.paginate("products", {}, {
      // Step 1: Prepare (enrich with pricing tiers)
      prepare: async (product) => ({
        ...product,
        priceRange: product.price < 50
          ? "budget"
          : product.price < 500
          ? "mid"
          : "premium",
        discountEligible: product.price > 100 && product.inStock,
      }),

      // Step 2: Filter (only in-stock products)
      filter: (enrichedProduct) => enrichedProduct.inStock,

      // Step 3: Format (create catalog format)
      format: async (enrichedProduct) => ({
        productName: enrichedProduct.name,
        displayPrice: `$${enrichedProduct.price}`,
        tier: enrichedProduct.priceRange,
        canDiscount: enrichedProduct.discountEligible,
        categoryTag: enrichedProduct.category.toUpperCase(),
      }),
    });

    expect(results.data.length === 2).toBeTruthy();
    expect(
      results.data[0].productName === "Laptop",
    ).toBeTruthy();
    expect(results.data[0].tier === "premium").toBeTruthy();
    expect(
      results.data[0].canDiscount === true,
    ).toBeTruthy();
    expect(
      results.data[1].productName === "Book",
    ).toBeTruthy();
    expect(results.data[1].tier === "budget").toBeTruthy();
    expect(
      results.data[1].canDiscount === false,
    ).toBeTruthy();
  });
});

test("Multi-Collection: Cross-type isolation", async () => {
  await withDatabase("Multi-Collection: Cross-type isolation", async (db) => {
    const mc = await multiCollection(db, "multi_test", collectionModel);

    // Insert mixed data
    await mc.insertOne("users", {
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });
    await mc.insertOne("products", {
      name: "Laptop",
      price: 999,
      category: "electronics",
      inStock: true,
    });
    await mc.insertOne("users", {
      name: "Bob",
      age: 30,
      email: "bob@test.com",
      isActive: false,
    });

    const userResults = await mc.paginate("users", {}, {
      prepare: async (user) => ({
        ...user,
        type: "user-record",
      }),
      format: async (enrichedUser) => ({
        name: enrichedUser.name,
        recordType: enrichedUser.type,
      }),
    });

    const productResults = await mc.paginate("products", {}, {
      prepare: async (product) => ({
        ...product,
        type: "product-record",
      }),
      format: async (enrichedProduct) => ({
        name: enrichedProduct.name,
        recordType: enrichedProduct.type,
      }),
    });

    expect(userResults.data.length === 2).toBeTruthy();
    expect(
      userResults.data.every((r) => r.recordType === "user-record"),
    ).toBeTruthy();
    expect(productResults.data.length === 1).toBeTruthy();
    expect(
      productResults.data.every((r) => r.recordType === "product-record"),
    ).toBeTruthy();
  });
});

test("Multi-Collection: External API enrichment", async () => {
  await withDatabase("Multi-Collection: External API enrichment", async (db) => {
    const mc = await multiCollection(db, "multi_test", collectionModel);

    await mc.insertOne("users", {
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });
    await mc.insertOne("products", {
      name: "Laptop",
      price: 999,
      category: "electronics",
      inStock: true,
    });

    // Mock external services
    const mockServices = {
      async getUserReputation(email: string) {
        await new Promise((resolve) => setTimeout(resolve, 5));
        return email.includes("alice") ? 95 : 50;
      },

      async getProductReviews(productName: string) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        return productName === "Laptop"
          ? { rating: 4.5, count: 142 }
          : { rating: 3.0, count: 5 };
      },
    };

    const userResults = await mc.paginate("users", {}, {
      prepare: async (user) => {
        const reputation = await mockServices.getUserReputation(user.email);
        return {
          ...user,
          reputation,
          trustLevel: reputation > 80 ? "high" : "medium",
        };
      },

      filter: (enrichedUser) => enrichedUser.isActive,

      format: async (enrichedUser) => ({
        userName: enrichedUser.name,
        trust: enrichedUser.trustLevel,
        score: enrichedUser.reputation,
      }),
    });

    const productResults = await mc.paginate("products", {}, {
      prepare: async (product) => {
        const reviews = await mockServices.getProductReviews(product.name);
        return {
          ...product,
          reviews,
          isPopular: reviews.count > 100,
        };
      },

      filter: (enrichedProduct) => enrichedProduct.inStock,

      format: async (enrichedProduct) => ({
        productName: enrichedProduct.name,
        rating: enrichedProduct.reviews.rating,
        popularity: enrichedProduct.isPopular ? "popular" : "niche",
      }),
    });

    expect(userResults.data.length === 1).toBeTruthy();
    expect(
      userResults.data[0].trust === "high",
    ).toBeTruthy();
    expect(userResults.data[0].score === 95).toBeTruthy();

    expect(
      productResults.data.length === 1,
    ).toBeTruthy();
    expect(
      productResults.data[0].rating === 4.5,
    ).toBeTruthy();
    expect(
      productResults.data[0].popularity === "popular",
    ).toBeTruthy();
  });
});

test("Multi-Collection: Type safety with generics", async () => {
  await withDatabase("Multi-Collection: Type safety with generics", async (db) => {
    const mc = await multiCollection(db, "multi_test", collectionModel);

    await mc.insertOne("users", {
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });

    // Test type transformations maintain type safety
    const results = await mc.paginate("users", {}, {
      prepare: async (user) => {
        // user should be User with _id and _type
        expect(typeof user.name === "string").toBeTruthy();
        expect(typeof user.age === "number").toBeTruthy();
        expect(typeof user._id === "string").toBeTruthy();
        expect(user._type === "users").toBeTruthy();

        return {
          ...user,
          enrichedField: "test-value",
        };
      },

      filter: (enrichedUser) => {
        // enrichedUser should have enrichedField
        expect(
          enrichedUser.enrichedField === "test-value",
        ).toBeTruthy();
        return true;
      },

      format: async (enrichedUser) => {
        // enrichedUser should still have all fields
        expect(enrichedUser.name === "Alice").toBeTruthy();
        expect(
          enrichedUser.enrichedField === "test-value",
        ).toBeTruthy();

        return {
          finalName: enrichedUser.name,
          finalValue: enrichedUser.enrichedField,
        };
      },
    });

    expect(results.data.length === 1).toBeTruthy();
    expect(results.data[0].finalName === "Alice").toBeTruthy();
    expect(
      results.data[0].finalValue === "test-value",
    ).toBeTruthy();
  });
});

test("Multi-Collection: Error handling", async () => {
  await withDatabase("Multi-Collection: Error handling", async (db) => {
    const mc = await multiCollection(db, "multi_test", collectionModel);

    await mc.insertOne("users", {
      name: "Alice",
      age: 25,
      email: "alice@test.com",
      isActive: true,
    });
    await mc.insertOne("users", {
      name: "Bob",
      age: 30,
      email: "bob@test.com",
      isActive: false,
    });

    // Test error in prepare
    try {
      await mc.paginate("users", {}, {
        prepare: async (user) => {
          if (user.name === "Bob") {
            throw new Error("Simulated prepare error");
          }
          return { ...user, processed: true };
        },
      });
      expect(false).toBeTruthy();
    } catch (error) {
      expect(
        (error as Error).message === "Simulated prepare error",
      ).toBeTruthy();
    }

    // Test error in filter
    try {
      await mc.paginate("users", {}, {
        filter: (user) => {
          if (user.name === "Bob") {
            throw new Error("Simulated filter error");
          }
          return true;
        },
      });
      expect(false).toBeTruthy();
    } catch (error) {
      expect(
        (error as Error).message === "Simulated filter error",
      ).toBeTruthy();
    }

    // Test error in format
    try {
      await mc.paginate("users", {}, {
        format: async (user) => {
          if (user.name === "Bob") {
            throw new Error("Simulated format error");
          }
          return { processed: user.name };
        },
      });
      expect(false).toBeTruthy();
    } catch (error) {
      expect(
        (error as Error).message === "Simulated format error",
      ).toBeTruthy();
    }
  });
});

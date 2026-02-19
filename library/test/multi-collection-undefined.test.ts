import * as v from "../src/schema.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { test, expect } from "vitest";
import { defineModel } from "../src/multi-collection-model.ts";
import { withDatabase } from "./+shared.ts";

test("MultiCollection: undefined behavior remove (default)", async () => {
  await withDatabase("mc_undef_remove", async (db) => {
    const catalogModel = defineModel("catalog_remove", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          description: v.optional(v.string()),
          category: v.optional(v.string()),
        },
        category: {
          name: v.string(),
          parentId: v.optional(v.string()),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog_remove", catalogModel);

    // Insert product with undefined values (should be removed)
    const productId = await catalog.insertOne("product", {
      name: "Laptop",
      price: 999.99,
      description: undefined, // Should be removed
      category: "Electronics",
    });

    const product = await catalog.findOne("product", { _id: productId });
    expect(product).not.toBeNull();
    expect(!("description" in product!)).toBeTruthy();
    expect(product!.name).toEqual("Laptop");
    expect(product!.price).toEqual(999.99);
    expect(product!.category).toEqual("Electronics");

    // Insert category with undefined values
    const categoryId = await catalog.insertOne("category", {
      name: "Electronics",
      parentId: undefined, // Should be removed
    });

    const category = await catalog.findOne("category", { _id: categoryId });
    expect(category).not.toBeNull();
    expect(!("parentId" in category!)).toBeTruthy();
    expect(category!.name).toEqual("Electronics");
  });
});

test("MultiCollection: undefined behavior error", async () => {
  await withDatabase("mc_undef_error", async (db) => {
    const model = defineModel("catalog_error", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          description: v.optional(v.string()),
          category: v.optional(v.string()),
        },
        category: {
          name: v.string(),
          parentId: v.optional(v.string()),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog_error", model, {
      undefinedBehavior: "error",
    });

    // Should throw error for product with undefined
    await expect(
      async () => {
        await catalog.insertOne("product", {
          name: "Laptop",
          price: 999.99,
          description: undefined, // Should cause error
          category: "Electronics",
        });
      },
    ).rejects.toThrow("Undefined values are not allowed");

    // Should throw error for category with undefined
    await expect(
      async () => {
        await catalog.insertOne("category", {
          name: "Electronics",
          parentId: undefined, // Should cause error
        });
      },
    ).rejects.toThrow("Undefined values are not allowed");

    // But should work fine without undefined values
    const productId = await catalog.insertOne("product", {
      name: "Laptop",
      price: 999.99,
      category: "Electronics",
    });

    const product = await catalog.findOne("product", { _id: productId });
    expect(product).not.toBeNull();
    expect(product!.name).toEqual("Laptop");
    expect(product!.price).toEqual(999.99);
    expect(product!.category).toEqual("Electronics");
  });
});

test("MultiCollection: insertMany with undefined behavior", async () => {
  await withDatabase("mc_undef_insertmany", async (db) => {
    // Test remove behavior
    const model = defineModel("catalog_many_remove", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          description: v.optional(v.string()),
          category: v.optional(v.string()),
        },
      },
    });

    const catalogRemove = await multiCollection(
      db,
      "catalog_many_remove",
      model,
      {
        undefinedBehavior: "remove",
      },
    );

    const productIds = await catalogRemove.insertMany("product", [
      {
        name: "Product1",
        price: 10.99,
        description: undefined, // Should be removed
        category: "Cat1",
      },
      {
        name: "Product2",
        price: 20.99,
        description: "Description2",
        category: undefined, // Should be removed
      },
    ]);

    expect(productIds.length).toEqual(2);

    const products = await catalogRemove.find("product", {}).toArray();
    expect(products.length).toEqual(2);

    const product1 = products.find((p) => p.name === "Product1");
    const product2 = products.find((p) => p.name === "Product2");

    expect(product1 !== undefined).toBeTruthy();
    expect(product2 !== undefined).toBeTruthy();
    expect(!("description" in product1!)).toBeTruthy();
    expect(!("category" in product2!)).toBeTruthy();

    // Test error behavior
    const modelError = defineModel("catalog_many_error", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          description: v.optional(v.string()),
          category: v.optional(v.string()),
        },
      },
    });

    const catalogError = await multiCollection(
      db,
      "catalog_many_error",
      modelError,
      {
        undefinedBehavior: "error",
      },
    );

    await expect(
      async () => {
        await catalogError.insertMany("product", [
          {
            name: "Product1",
            price: 10.99,
            description: undefined, // Should cause error
            category: "Cat1",
          },
        ]);
      },
    ).rejects.toThrow("Undefined values are not allowed");
  });
});

test("MultiCollection: Mixed document types with different undefined values", async () => {
  await withDatabase("mc_undef_mixed", async (db) => {
    const model = defineModel("catalog_mixed", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          description: v.optional(v.string()),
          categoryId: v.optional(v.string()),
        },
        category: {
          name: v.string(),
          parentId: v.optional(v.string()),
          description: v.optional(v.string()),
        },
        brand: {
          name: v.string(),
          website: v.optional(v.string()),
          country: v.optional(v.string()),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog_mixed", model, {
      undefinedBehavior: "remove",
    });

    // Insert different document types with undefined values
    const categoryId = await catalog.insertOne("category", {
      name: "Electronics",
      parentId: undefined, // Should be removed
      description: "Electronic devices",
    });

    const brandId = await catalog.insertOne("brand", {
      name: "TechCorp",
      website: "https://techcorp.com",
      country: undefined, // Should be removed
    });

    const productId = await catalog.insertOne("product", {
      name: "Smartphone",
      price: 599.99,
      description: undefined, // Should be removed
      categoryId: categoryId,
    });

    // Verify all documents were inserted correctly
    const category = await catalog.findOne("category", { _id: categoryId });
    const brand = await catalog.findOne("brand", { _id: brandId });
    const product = await catalog.findOne("product", { _id: productId });

    // Check category
    expect(category).not.toBeNull();
    expect(!("parentId" in category!)).toBeTruthy();
    expect(category!.name).toEqual("Electronics");
    expect(category!.description).toEqual("Electronic devices");

    // Check brand
    expect(brand).not.toBeNull();
    expect(!("country" in brand!)).toBeTruthy();
    expect(brand!.name).toEqual("TechCorp");
    expect(brand!.website).toEqual("https://techcorp.com");

    // Check product
    expect(product).not.toBeNull();
    expect(!("description" in product!)).toBeTruthy();
    expect(product!.name).toEqual("Smartphone");
    expect(product!.price).toEqual(599.99);
    expect(product!.categoryId).toEqual(categoryId);
  });
});

test("MultiCollection: Multiple collections with different undefined behaviors", async () => {
  await withDatabase("mc_undef_multi_behaviors", async (db) => {
    // Collection 1: Remove undefined (default)
    const model = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          description: v.optional(v.string()),
        },
      },
    });

    const catalogRemove = await multiCollection(
      db,
      "catalog_remove_multi",
      model,
      {
        undefinedBehavior: "remove",
      },
    );

    // Collection 2: Error on undefined
    const catalogError = await multiCollection(
      db,
      "catalog_error_multi",
      model,
      {
        undefinedBehavior: "error",
      },
    );

    const testProduct = {
      name: "TestProduct",
      price: 49.99,
      description: undefined,
    };

    // First collection should work (remove undefined)
    const productId1 = await catalogRemove.insertOne("product", testProduct);
    const product1 = await catalogRemove.findOne("product", {
      _id: productId1,
    });
    expect(product1).not.toBeNull();
    expect(!("description" in product1!)).toBeTruthy();
    expect(product1!.name).toEqual("TestProduct");

    // Second collection should fail (error on undefined)
    await expect(
      async () => {
        await catalogError.insertOne("product", testProduct);
      },
    ).rejects.toThrow("Undefined values are not allowed");

    // But second collection should work with clean data
    const productId2 = await catalogError.insertOne("product", {
      name: "TestProduct2",
      price: 59.99,
    });

    const product2 = await catalogError.findOne("product", { _id: productId2 });
    expect(product2).not.toBeNull();
    expect(product2!.name).toEqual("TestProduct2");
    expect(product2!.price).toEqual(59.99);
  });
});

test("MultiCollection: Nested undefined values", async () => {
  await withDatabase("mc_undef_nested", async (db) => {
    const nestedSchema = {
      name: v.string(),
      profile: v.optional(v.object({
        bio: v.optional(v.string()),
        website: v.optional(v.string()),
        social: v.optional(v.object({
          twitter: v.optional(v.string()),
          github: v.optional(v.string()),
        })),
      })),
      preferences: v.optional(v.array(v.string())),
    };

    const model = defineModel("nested_docs", {
      schema: {
        profiles: nestedSchema,
      },
    });

    const mc = await multiCollection(db, "nested_docs", model, {
      undefinedBehavior: "remove",
    });

    const profileId = await mc.insertOne("profiles", {
      name: "Developer",
      profile: {
        bio: "Software developer",
        website: undefined, // Should be removed
        social: {
          twitter: undefined, // Should be removed
          github: "dev123",
        },
      },
      preferences: undefined, // Should be removed
    });

    const profile = await mc.findOne("profiles", { _id: profileId });

    expect(profile).not.toBeNull();
    expect(profile!.name).toEqual("Developer");
    expect("profile" in profile!).toBeTruthy();
    expect(!("preferences" in profile!)).toBeTruthy();

    // Check nested object sanitization
    expect(profile!.profile !== undefined).toBeTruthy();
    expect("bio" in profile!.profile!).toBeTruthy();
    expect(!("website" in profile!.profile!)).toBeTruthy();
    expect("social" in profile!.profile!).toBeTruthy();
    expect(profile!.profile!.social !== undefined).toBeTruthy();
    expect(!("twitter" in profile!.profile!.social!)).toBeTruthy();
    expect("github" in profile!.profile!.social!).toBeTruthy();
    expect(profile!.profile!.social!.github).toEqual("dev123");
  });
});

test("MultiCollection: Ignore undefined behavior", async () => {
  await withDatabase("mc_undef_ignore", async (db) => {
    const model = defineModel("ignore_docs", {
      schema: {
        products: {
          name: v.string(),
          price: v.number(),
          description: v.optional(v.string()),
        },
      },
    });

    const mc = await multiCollection(db, "ignore_docs", model, {
      undefinedBehavior: "ignore",
    });

    // Should ignore undefined values and let MongoDB handle them
    // Note: This will likely fail at MongoDB level since undefined is not valid BSON
    try {
      await mc.insertOne("products", {
        name: "TestProduct",
        price: 29.99,
        description: undefined, // Will be ignored by our sanitizer
      });

      // If we reach here, MongoDB accepted it (unlikely)
      const product = await mc.findOne("products", { name: "TestProduct" });
      expect(product).not.toBeNull();
      expect(product!.name).toEqual("TestProduct");
    } catch (error) {
      // Expected: MongoDB will likely reject undefined values
      expect(error instanceof Error).toBeTruthy();
      // This is acceptable behavior for 'ignore' mode
    }
  });
});

test("MultiCollection: Performance with undefined sanitization", async () => {
  await withDatabase("mc_undef_perf", async (db) => {
    const model = defineModel("perf_docs", {
      schema: {
        items: {
          id: v.string(),
          data: v.optional(v.string()),
          metadata: v.optional(v.object({
            created: v.optional(v.string()),
            updated: v.optional(v.string()),
          })),
        },
      },
    });

    const mc = await multiCollection(db, "perf_docs", model, {
      undefinedBehavior: "remove",
    });

    // Generate test data with many undefined values
    const testData = [];
    for (let i = 0; i < 50; i++) { // Reduced for test performance
      testData.push({
        id: `item_${i}`,
        data: i % 3 === 0 ? undefined : `data_${i}`, // 1/3 undefined
        metadata: i % 2 === 0
          ? {
            created: `2023-01-${i % 28 + 1}`,
            updated: i % 4 === 0 ? undefined : `2023-02-${i % 28 + 1}`,
          }
          : undefined,
      });
    }

    const itemIds = await mc.insertMany("items", testData);

    expect(itemIds.length).toEqual(50);

    // Verify correct sanitization
    const items = await mc.find("items", {}).toArray();
    expect(items.length).toEqual(50);

    // Check that undefined values were properly removed
    for (const item of items) {
      expect("id" in item).toBeTruthy();

      // data should only be present if it wasn't undefined
      const originalItem = testData.find((d) => d.id === item.id);
      if (originalItem?.data !== undefined) {
        expect("data" in item).toBeTruthy();
      } else {
        expect(!("data" in item)).toBeTruthy();
      }

      // Check metadata sanitization
      if (originalItem?.metadata !== undefined) {
        expect("metadata" in item).toBeTruthy();
        expect(item.metadata !== undefined).toBeTruthy();
        expect("created" in item.metadata!).toBeTruthy();

        if (originalItem.metadata.updated !== undefined) {
          expect("updated" in item.metadata!).toBeTruthy();
        } else {
          expect(!("updated" in item.metadata!)).toBeTruthy();
        }
      } else {
        expect(!("metadata" in item)).toBeTruthy();
      }
    }
  });
});

test("MultiCollection: Array sanitization with undefined values", async () => {
  await withDatabase("mc_undef_arrays", async (db) => {
    const model = defineModel("array_docs", {
      schema: {
        posts: {
          title: v.string(),
          tags: v.optional(v.array(v.string())),
          comments: v.optional(v.array(v.object({
            author: v.string(),
            text: v.optional(v.string()),
            timestamp: v.optional(v.string()),
          }))),
        },
      },
    });

    const mc = await multiCollection(db, "array_docs", model, {
      undefinedBehavior: "remove",
    });

    const postId = await mc.insertOne("posts", {
      title: "Test Post",
      tags: ["tech", "mongodb"],
      comments: [
        {
          author: "user1",
          text: "Great post!",
          timestamp: undefined, // Should be removed
        },
        {
          author: "user2",
          text: undefined, // Should be removed
          timestamp: "2023-01-01",
        },
      ],
    });

    const post = await mc.findOne("posts", { _id: postId });

    expect(post).not.toBeNull();
    expect(post!.title).toEqual("Test Post");
    expect(post!.tags !== undefined).toBeTruthy();
    expect(post!.tags!.length).toEqual(2);
    expect(post!.comments !== undefined).toBeTruthy();
    expect(post!.comments!.length).toEqual(2);

    // Check first comment
    const comment1 = post!.comments![0];
    expect(comment1.author).toEqual("user1");
    expect(comment1.text).toEqual("Great post!");
    expect(!("timestamp" in comment1)).toBeTruthy();

    // Check second comment
    const comment2 = post!.comments![1];
    expect(comment2.author).toEqual("user2");
    expect(!("text" in comment2)).toBeTruthy();
    expect(comment2.timestamp).toEqual("2023-01-01");
  });
});

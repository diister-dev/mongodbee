import * as v from "../../src/schema.ts";
import { assertEquals, assertExists } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

Deno.test("Multi-collection paginate basic functionality", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          category: v.string(),
        },
        category: {
          name: v.string(),
          description: v.string(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert test categories
    const categoriesData = [];
    for (let i = 1; i <= 5; i++) {
      categoriesData.push({
        name: `Category ${i}`,
        description: `Description for category ${i}`,
      });
    }

    const categoryIds = await Promise.all(
      categoriesData.map((cat) => catalog.insertOne("category", cat)),
    );

    // Insert test products
    const productsData = [];
    for (let i = 1; i <= 20; i++) {
      productsData.push({
        name: `Product ${i}`,
        price: i * 10,
        category: categoryIds[i % 5], // Distribute products across categories
      });
    }

    await Promise.all(
      productsData.map((prod) => catalog.insertOne("product", prod)),
    );

    // Test basic pagination for products
    const firstPageProducts = await catalog.paginate("product", {}, {
      limit: 5,
    });
    assertEquals(firstPageProducts.data.length, 5);

    // Verify all results are products
    for (const product of firstPageProducts.data) {
      assertEquals(product._type, "product");
      assertExists(product.name);
      assertExists(product.price);
      assertExists(product.category);
    }

    // Test basic pagination for categories
    const firstPageCategories = await catalog.paginate("category", {}, {
      limit: 3,
    });
    assertEquals(firstPageCategories.data.length, 3);

    // Verify all results are categories
    for (const category of firstPageCategories.data) {
      assertEquals(category._type, "category");
      assertExists(category.name);
      assertExists(category.description);
    }
  });
});

Deno.test("Multi-collection paginate with afterId", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
        },
        user: {
          name: v.string(),
          email: v.string(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products
    const productIds = [];
    for (let i = 1; i <= 15; i++) {
      const id = await catalog.insertOne("product", {
        name: `Product ${i}`,
        price: i * 10,
      });
      productIds.push(id);
    }

    // Insert users
    const userIds = [];
    for (let i = 1; i <= 10; i++) {
      const id = await catalog.insertOne("user", {
        name: `User ${i}`,
        email: `user${i}@test.com`,
      });
      userIds.push(id);
    }

    // Test pagination with afterId for products
    const firstPageProducts = await catalog.paginate("product", {}, {
      limit: 5,
      sort: { _id: 1 },
    });
    assertEquals(firstPageProducts.data.length, 5);

    const secondPageProducts = await catalog.paginate("product", {}, {
      limit: 5,
      afterId: firstPageProducts.data[firstPageProducts.data.length - 1]._id,
      sort: { _id: 1 },
    });
    assertEquals(secondPageProducts.data.length, 5);

    // Verify no overlap between pages
    const firstPageIds = new Set(firstPageProducts.data.map((p) => p._id));
    const secondPageIds = new Set(secondPageProducts.data.map((p) => p._id));

    for (const id of secondPageIds) {
      assertEquals(firstPageIds.has(id), false);
    }

    // Test pagination with afterId for users
    const firstPageUsers = await catalog.paginate("user", {}, {
      limit: 3,
      sort: { _id: 1 },
    });
    assertEquals(firstPageUsers.data.length, 3);

    const secondPageUsers = await catalog.paginate("user", {}, {
      limit: 3,
      afterId: firstPageUsers.data[firstPageUsers.data.length - 1]._id,
      sort: { _id: 1 },
    });
    assertEquals(secondPageUsers.data.length, 3);

    // Verify all are users
    for (const user of secondPageUsers.data) {
      assertEquals(user._type, "user");
    }
  });
});

Deno.test("Multi-collection paginate with beforeId", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products
    for (let i = 1; i <= 10; i++) {
      await catalog.insertOne("product", {
        name: `Product ${i}`,
        price: i * 10,
      });
    }

    // Get all products to find a reference point
    const allProducts = await catalog.find("product", {}, { sort: { _id: 1 } });

    // Get products before the 7th product
    const beforePage = await catalog.paginate("product", {}, {
      limit: 3,
      beforeId: allProducts[6]._id,
      sort: { _id: -1 },
    });

    assertEquals(beforePage.data.length, 3);

    // Verify items come before the reference point in the sorted order
    // With sort: { _id: -1 }, "before" means items with higher _id
    const referenceId = allProducts[6]._id;
    for (const product of beforePage.data) {
      // Items before the anchor in descending order have _id > referenceId
      assertEquals(product._id > referenceId, true);
      assertEquals(product._type, "product");
    }
  });
});

Deno.test("Multi-collection paginate with filter", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          category: v.string(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products with different categories
    for (let i = 1; i <= 20; i++) {
      await catalog.insertOne("product", {
        name: `Product ${i}`,
        price: i * 10,
        category: i % 2 === 0 ? "electronics" : "clothing",
      });
    }

    // Paginate with MongoDB filter (only electronics)
    const electronicsProducts = await catalog.paginate("product", {
      category: "electronics",
    }, { limit: 10 });

    assertEquals(electronicsProducts.data.length, 10);

    // Verify all are electronics
    for (const product of electronicsProducts.data) {
      assertEquals(product.category, "electronics");
      assertEquals(product._type, "product");
    }

    // Paginate with custom filter (expensive electronics - price > 100)
    const expensiveElectronics = await catalog.paginate("product", {
      category: "electronics",
    }, {
      limit: 10,
      filter: (doc) => doc.price > 100,
    });

    // Should get products with price 110, 130, 150, 170, 190
    assertEquals(expensiveElectronics.data.length, 5);

    for (const product of expensiveElectronics.data) {
      assertEquals(product.category, "electronics");
      assertEquals(product.price > 100, true);
    }
  });
});

Deno.test("Multi-collection paginate with sorting", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products in random order
    const products = [
      { name: "Product C", price: 300 },
      { name: "Product A", price: 100 },
      { name: "Product E", price: 500 },
      { name: "Product B", price: 200 },
      { name: "Product D", price: 400 },
    ];

    await Promise.all(
      products.map((prod) => catalog.insertOne("product", prod)),
    );

    // Paginate with sorting by price ascending
    const sortedByPrice = await catalog.paginate("product", {}, {
      limit: 10,
      sort: { price: 1 },
    });

    assertEquals(sortedByPrice.data.length, 5);

    // Verify items are sorted by price
    for (let i = 0; i < sortedByPrice.data.length - 1; i++) {
      assertEquals(
        sortedByPrice.data[i].price <= sortedByPrice.data[i + 1].price,
        true,
      );
    }

    // Verify the actual order
    assertEquals(sortedByPrice.data[0].price, 100);
    assertEquals(sortedByPrice.data[1].price, 200);
    assertEquals(sortedByPrice.data[2].price, 300);
    assertEquals(sortedByPrice.data[3].price, 400);
    assertEquals(sortedByPrice.data[4].price, 500);
  });
});

Deno.test("Multi-collection paginate with invalid ID format", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
        },
        user: {
          name: v.string(),
          email: v.string(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert some data
    await catalog.insertOne("product", { name: "Product 1", price: 100 });
    await catalog.insertOne("user", {
      name: "User 1",
      email: "user1@test.com",
    });

    // Test with wrong ID format for afterId
    try {
      await catalog.paginate("product", {}, { afterId: "user:123" }); // User ID for product pagination
      assertEquals(true, false, "Should have thrown an error");
    } catch (error) {
      assertEquals(
        (error as Error).message.includes("Invalid afterId format"),
        true,
      );
    }

    // Test with wrong ID format for beforeId
    try {
      await catalog.paginate("user", {}, { beforeId: "product:456" }); // Product ID for user pagination
      assertEquals(true, false, "Should have thrown an error");
    } catch (error) {
      assertEquals(
        (error as Error).message.includes("Invalid beforeId format"),
        true,
      );
    }
  });
});

Deno.test("Multi-collection paginate with empty results", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
        },
        user: {
          name: v.string(),
          email: v.string(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Test empty collection
    const emptyProducts = await catalog.paginate("product", {}, { limit: 10 });
    assertEquals(emptyProducts.data.length, 0);

    // Insert some users but test products (should be empty)
    await catalog.insertOne("user", {
      name: "User 1",
      email: "user1@test.com",
    });
    await catalog.insertOne("user", {
      name: "User 2",
      email: "user2@test.com",
    });

    const stillEmptyProducts = await catalog.paginate("product", {}, {
      limit: 10,
    });
    assertEquals(stillEmptyProducts.data.length, 0);

    // But users should exist
    const existingUsers = await catalog.paginate("user", {}, { limit: 10 });
    assertEquals(existingUsers.data.length, 2);
  });
});

Deno.test("Multi-collection paginate with limit boundary conditions", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert 5 products
    for (let i = 1; i <= 5; i++) {
      await catalog.insertOne("product", {
        name: `Product ${i}`,
        price: i * 10,
      });
    }

    // Test with limit larger than available data
    const largeLimitPage = await catalog.paginate("product", {}, {
      limit: 100,
    });
    assertEquals(largeLimitPage.data.length, 5);

    // Test with limit of 1
    const singleItemPage = await catalog.paginate("product", {}, { limit: 1 });
    assertEquals(singleItemPage.data.length, 1);

    // Test with limit of 0
    const zeroLimitPage = await catalog.paginate("product", {}, { limit: 0 });
    assertEquals(zeroLimitPage.data.length, 0);
  });
});

Deno.test("Multi-collection paginate with custom sort and afterId", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          createdAt: v.number(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products with varying createdAt values (not in _id order)
    const products = [
      { name: "Product E", price: 500, createdAt: 500 },
      { name: "Product A", price: 100, createdAt: 100 },
      { name: "Product C", price: 300, createdAt: 300 },
      { name: "Product B", price: 200, createdAt: 200 },
      { name: "Product D", price: 400, createdAt: 400 },
      { name: "Product F", price: 600, createdAt: 600 },
    ];

    for (const prod of products) {
      await catalog.insertOne("product", prod);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Test 1: Paginate with custom sort (createdAt descending) - first page
    const firstPage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { createdAt: -1 },
    });

    assertEquals(firstPage.data.length, 3);
    assertEquals(firstPage.total, 6);

    // Verify first page is sorted by createdAt descending
    assertEquals(firstPage.data[0].createdAt, 600); // Product F
    assertEquals(firstPage.data[1].createdAt, 500); // Product E
    assertEquals(firstPage.data[2].createdAt, 400); // Product D

    // Test 2: Get second page using afterId with custom sort
    const secondPage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { createdAt: -1 },
      afterId: firstPage.data[firstPage.data.length - 1]._id,
    });

    assertEquals(secondPage.data.length, 3);

    // Verify second page continues the sort order
    assertEquals(secondPage.data[0].createdAt, 300); // Product C
    assertEquals(secondPage.data[1].createdAt, 200); // Product B
    assertEquals(secondPage.data[2].createdAt, 100); // Product A

    // Verify no overlap between pages
    const firstPageIds = new Set(firstPage.data.map((item) => item._id));
    for (const item of secondPage.data) {
      assertEquals(firstPageIds.has(item._id), false,
        "Second page should not contain items from first page");
    }
  });
});

Deno.test("Multi-collection paginate with custom sort and beforeId", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          score: v.number(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products with varying scores
    const products = [
      { name: "Low", score: 10 },
      { name: "High", score: 90 },
      { name: "Medium", score: 50 },
      { name: "VeryHigh", score: 100 },
      { name: "VeryLow", score: 5 },
      { name: "MediumHigh", score: 70 },
    ];

    for (const prod of products) {
      await catalog.insertOne("product", prod);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Get all items sorted by score descending
    const allItems = await catalog.paginate("product", {}, {
      limit: 6,
      sort: { score: -1 },
    });

    // allItems should be: VeryHigh(100), High(90), MediumHigh(70), Medium(50), Low(10), VeryLow(5)
    assertEquals(allItems.data[0].score, 100);
    assertEquals(allItems.data[1].score, 90);
    assertEquals(allItems.data[2].score, 70);

    // Use beforeId with the 4th item (Medium, score=50) as anchor
    const beforePage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { score: -1 },
      beforeId: allItems.data[3]._id,
    });

    assertEquals(beforePage.data.length, 3);

    // With beforeId, items are returned in reverse order (closest to anchor first)
    assertEquals(beforePage.data[0].score, 70);  // MediumHigh (closest to anchor)
    assertEquals(beforePage.data[1].score, 90);  // High
    assertEquals(beforePage.data[2].score, 100); // VeryHigh (furthest from anchor)
  });
});

Deno.test("Multi-collection paginate with multi-field custom sort and afterId", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          category: v.string(),
          name: v.string(),
          value: v.number(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products - sorted by category asc, then value desc
    const products = [
      { category: "A", name: "A-High", value: 100 },
      { category: "A", name: "A-Low", value: 10 },
      { category: "A", name: "A-Mid", value: 50 },
      { category: "B", name: "B-High", value: 90 },
      { category: "B", name: "B-Low", value: 20 },
      { category: "C", name: "C-Only", value: 60 },
    ];

    for (const prod of products) {
      await catalog.insertOne("product", prod);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Expected order with { category: 1, value: -1 }:
    // A-High(A,100), A-Mid(A,50), A-Low(A,10), B-High(B,90), B-Low(B,20), C-Only(C,60)

    const firstPage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { category: 1, value: -1 },
    });

    assertEquals(firstPage.data.length, 3);
    assertEquals(firstPage.data[0].name, "A-High");
    assertEquals(firstPage.data[1].name, "A-Mid");
    assertEquals(firstPage.data[2].name, "A-Low");

    // Get second page
    const secondPage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { category: 1, value: -1 },
      afterId: firstPage.data[firstPage.data.length - 1]._id,
    });

    assertEquals(secondPage.data.length, 3);
    assertEquals(secondPage.data[0].name, "B-High");
    assertEquals(secondPage.data[1].name, "B-Low");
    assertEquals(secondPage.data[2].name, "C-Only");
  });
});

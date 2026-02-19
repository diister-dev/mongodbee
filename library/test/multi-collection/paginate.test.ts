import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

test("Multi-collection paginate basic functionality", async () => {
  await withDatabase("Multi-collection paginate basic functionality", async (db) => {
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
    expect(firstPageProducts.data.length).toEqual(5);

    // Verify all results are products
    for (const product of firstPageProducts.data) {
      expect(product._type).toEqual("product");
      expect(product.name).toBeDefined();
      expect(product.price).toBeDefined();
      expect(product.category).toBeDefined();
    }

    // Test basic pagination for categories
    const firstPageCategories = await catalog.paginate("category", {}, {
      limit: 3,
    });
    expect(firstPageCategories.data.length).toEqual(3);

    // Verify all results are categories
    for (const category of firstPageCategories.data) {
      expect(category._type).toEqual("category");
      expect(category.name).toBeDefined();
      expect(category.description).toBeDefined();
    }
  });
});

test("Multi-collection paginate with afterId", async () => {
  await withDatabase("Multi-collection paginate with afterId", async (db) => {
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
    expect(firstPageProducts.data.length).toEqual(5);

    const secondPageProducts = await catalog.paginate("product", {}, {
      limit: 5,
      afterId: firstPageProducts.data[firstPageProducts.data.length - 1]._id,
      sort: { _id: 1 },
    });
    expect(secondPageProducts.data.length).toEqual(5);

    // Verify no overlap between pages
    const firstPageIds = new Set(firstPageProducts.data.map((p) => p._id));
    const secondPageIds = new Set(secondPageProducts.data.map((p) => p._id));

    for (const id of secondPageIds) {
      expect(firstPageIds.has(id)).toEqual(false);
    }

    // Test pagination with afterId for users
    const firstPageUsers = await catalog.paginate("user", {}, {
      limit: 3,
      sort: { _id: 1 },
    });
    expect(firstPageUsers.data.length).toEqual(3);

    const secondPageUsers = await catalog.paginate("user", {}, {
      limit: 3,
      afterId: firstPageUsers.data[firstPageUsers.data.length - 1]._id,
      sort: { _id: 1 },
    });
    expect(secondPageUsers.data.length).toEqual(3);

    // Verify all are users
    for (const user of secondPageUsers.data) {
      expect(user._type).toEqual("user");
    }
  });
});

test("Multi-collection paginate with beforeId", async () => {
  await withDatabase("Multi-collection paginate with beforeId", async (db) => {
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
    const allProducts = await catalog.find("product", {}, { sort: { _id: 1 } }).toArray();

    // Get products before the 7th product
    const beforePage = await catalog.paginate("product", {}, {
      limit: 3,
      beforeId: allProducts[6]._id,
      sort: { _id: -1 },
    });

    expect(beforePage.data.length).toEqual(3);

    // Verify items come before the reference point in the sorted order
    // With sort: { _id: -1 }, "before" means items with higher _id
    const referenceId = allProducts[6]._id;
    for (const product of beforePage.data) {
      // Items before the anchor in descending order have _id > referenceId
      expect(product._id > referenceId).toEqual(true);
      expect(product._type).toEqual("product");
    }
  });
});

test("Multi-collection paginate with filter", async () => {
  await withDatabase("Multi-collection paginate with filter", async (db) => {
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

    expect(electronicsProducts.data.length).toEqual(10);

    // Verify all are electronics
    for (const product of electronicsProducts.data) {
      expect(product.category).toEqual("electronics");
      expect(product._type).toEqual("product");
    }

    // Paginate with custom filter (expensive electronics - price > 100)
    const expensiveElectronics = await catalog.paginate("product", {
      category: "electronics",
    }, {
      limit: 10,
      filter: (doc) => doc.price > 100,
    });

    // Should get products with price 110, 130, 150, 170, 190
    expect(expensiveElectronics.data.length).toEqual(5);

    for (const product of expensiveElectronics.data) {
      expect(product.category).toEqual("electronics");
      expect(product.price > 100).toEqual(true);
    }
  });
});

test("Multi-collection paginate with sorting", async () => {
  await withDatabase("Multi-collection paginate with sorting", async (db) => {
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

    expect(sortedByPrice.data.length).toEqual(5);

    // Verify items are sorted by price
    for (let i = 0; i < sortedByPrice.data.length - 1; i++) {
      expect(
        sortedByPrice.data[i].price <= sortedByPrice.data[i + 1].price,
      ).toEqual(true);
    }

    // Verify the actual order
    expect(sortedByPrice.data[0].price).toEqual(100);
    expect(sortedByPrice.data[1].price).toEqual(200);
    expect(sortedByPrice.data[2].price).toEqual(300);
    expect(sortedByPrice.data[3].price).toEqual(400);
    expect(sortedByPrice.data[4].price).toEqual(500);
  });
});

test("Multi-collection paginate with invalid ID format", async () => {
  await withDatabase("Multi-collection paginate with invalid ID format", async (db) => {
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
      expect(true).toEqual(false);
    } catch (error) {
      expect(
        (error as Error).message.includes("Invalid afterId format"),
      ).toEqual(true);
    }

    // Test with wrong ID format for beforeId
    try {
      await catalog.paginate("user", {}, { beforeId: "product:456" }); // Product ID for user pagination
      expect(true).toEqual(false);
    } catch (error) {
      expect(
        (error as Error).message.includes("Invalid beforeId format"),
      ).toEqual(true);
    }
  });
});

test("Multi-collection paginate with empty results", async () => {
  await withDatabase("Multi-collection paginate with empty results", async (db) => {
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
    expect(emptyProducts.data.length).toEqual(0);

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
    expect(stillEmptyProducts.data.length).toEqual(0);

    // But users should exist
    const existingUsers = await catalog.paginate("user", {}, { limit: 10 });
    expect(existingUsers.data.length).toEqual(2);
  });
});

test("Multi-collection paginate with limit boundary conditions", async () => {
  await withDatabase("Multi-collection paginate with limit boundary conditions", async (db) => {
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
    expect(largeLimitPage.data.length).toEqual(5);

    // Test with limit of 1
    const singleItemPage = await catalog.paginate("product", {}, { limit: 1 });
    expect(singleItemPage.data.length).toEqual(1);

    // Test with limit of 0
    const zeroLimitPage = await catalog.paginate("product", {}, { limit: 0 });
    expect(zeroLimitPage.data.length).toEqual(0);
  });
});

test("Multi-collection paginate with custom sort and afterId", async () => {
  await withDatabase("Multi-collection paginate with custom sort and afterId", async (db) => {
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

    expect(firstPage.data.length).toEqual(3);
    expect(firstPage.total).toEqual(6);

    // Verify first page is sorted by createdAt descending
    expect(firstPage.data[0].createdAt).toEqual(600); // Product F
    expect(firstPage.data[1].createdAt).toEqual(500); // Product E
    expect(firstPage.data[2].createdAt).toEqual(400); // Product D

    // Test 2: Get second page using afterId with custom sort
    const secondPage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { createdAt: -1 },
      afterId: firstPage.data[firstPage.data.length - 1]._id,
    });

    expect(secondPage.data.length).toEqual(3);

    // Verify second page continues the sort order
    expect(secondPage.data[0].createdAt).toEqual(300); // Product C
    expect(secondPage.data[1].createdAt).toEqual(200); // Product B
    expect(secondPage.data[2].createdAt).toEqual(100); // Product A

    // Verify no overlap between pages
    const firstPageIds = new Set(firstPage.data.map((item) => item._id));
    for (const item of secondPage.data) {
      expect(firstPageIds.has(item._id)).toEqual(false);
    }
  });
});

test("Multi-collection paginate with custom sort and beforeId", async () => {
  await withDatabase("Multi-collection paginate with custom sort and beforeId", async (db) => {
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
    expect(allItems.data[0].score).toEqual(100);
    expect(allItems.data[1].score).toEqual(90);
    expect(allItems.data[2].score).toEqual(70);

    // Use beforeId with the 4th item (Medium, score=50) as anchor
    const beforePage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { score: -1 },
      beforeId: allItems.data[3]._id,
    });

    expect(beforePage.data.length).toEqual(3);

    // With beforeId, items are returned in the SAME order as forward pagination
    expect(beforePage.data[0].score).toEqual(100); // VeryHigh
    expect(beforePage.data[1].score).toEqual(90);  // High
    expect(beforePage.data[2].score).toEqual(70);  // MediumHigh (closest to anchor)
  });
});

test("Multi-collection paginate with multi-field custom sort and afterId", async () => {
  await withDatabase("Multi-collection paginate with multi-field custom sort and afterId", async (db) => {
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

    expect(firstPage.data.length).toEqual(3);
    expect(firstPage.data[0].name).toEqual("A-High");
    expect(firstPage.data[1].name).toEqual("A-Mid");
    expect(firstPage.data[2].name).toEqual("A-Low");

    // Get second page
    const secondPage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { category: 1, value: -1 },
      afterId: firstPage.data[firstPage.data.length - 1]._id,
    });

    expect(secondPage.data.length).toEqual(3);
    expect(secondPage.data[0].name).toEqual("B-High");
    expect(secondPage.data[1].name).toEqual("B-Low");
    expect(secondPage.data[2].name).toEqual("C-Only");
  });
});

test("Multi-collection paginate with _id descending sort", async () => {
  await withDatabase("Multi-collection paginate with _id descending sort", async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          value: v.number(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert 10 products
    for (let i = 1; i <= 10; i++) {
      await catalog.insertOne("product", { name: `Product ${i}`, value: i * 10 });
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // First page with _id descending (newest first)
    const firstPage = await catalog.paginate("product", {}, {
      limit: 4,
      sort: { _id: -1 },
    });

    expect(firstPage.data.length).toEqual(4);
    expect(firstPage.total).toEqual(10);

    // Should be Product 10, 9, 8, 7 (newest to oldest)
    expect(firstPage.data[0].name).toEqual("Product 10");
    expect(firstPage.data[1].name).toEqual("Product 9");
    expect(firstPage.data[2].name).toEqual("Product 8");
    expect(firstPage.data[3].name).toEqual("Product 7");

    // Collect first page IDs
    const firstPageIds = new Set(firstPage.data.map((item) => item._id));

    // Second page
    const secondPage = await catalog.paginate("product", {}, {
      limit: 4,
      sort: { _id: -1 },
      afterId: firstPage.data[firstPage.data.length - 1]._id,
    });

    expect(secondPage.data.length).toEqual(4);

    // Should be Product 6, 5, 4, 3
    expect(secondPage.data[0].name).toEqual("Product 6");
    expect(secondPage.data[1].name).toEqual("Product 5");
    expect(secondPage.data[2].name).toEqual("Product 4");
    expect(secondPage.data[3].name).toEqual("Product 3");

    // Verify no duplicates
    for (const item of secondPage.data) {
      expect(firstPageIds.has(item._id)).toEqual(false);
    }

    // Third page
    const thirdPage = await catalog.paginate("product", {}, {
      limit: 4,
      sort: { _id: -1 },
      afterId: secondPage.data[secondPage.data.length - 1]._id,
    });

    expect(thirdPage.data.length).toEqual(2); // Only 2 remaining

    // Should be Product 2, 1
    expect(thirdPage.data[0].name).toEqual("Product 2");
    expect(thirdPage.data[1].name).toEqual("Product 1");

    // Verify no duplicates with previous pages
    const secondPageIds = new Set(secondPage.data.map((item) => item._id));
    for (const item of thirdPage.data) {
      expect(firstPageIds.has(item._id)).toEqual(false);
      expect(secondPageIds.has(item._id)).toEqual(false);
    }
  });
});

test("Multi-collection paginate with duplicate sort values", async () => {
  await withDatabase("Multi-collection paginate with duplicate sort values", async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          category: v.string(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products where all have the same category (duplicate sort values)
    for (let i = 1; i <= 6; i++) {
      await catalog.insertOne("product", { name: `Product ${i}`, category: "same" });
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // First page with sort on duplicate field
    const firstPage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { category: 1 },
    });

    expect(firstPage.data.length).toEqual(3);
    expect(firstPage.total).toEqual(6);

    // Collect first page names
    const firstPageNames = firstPage.data.map((item) => item.name);

    // Second page should get remaining items, no duplicates
    const secondPage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { category: 1 },
      afterId: firstPage.data[firstPage.data.length - 1]._id,
    });

    expect(secondPage.data.length).toEqual(3);

    // Verify no overlap between pages
    const secondPageNames = secondPage.data.map((item) => item.name);
    for (const name of secondPageNames) {
      expect(firstPageNames.includes(name)).toEqual(false);
    }

    // Verify all 6 items are covered
    const allNames = [...firstPageNames, ...secondPageNames];
    expect(allNames.length).toEqual(6);
    for (let i = 1; i <= 6; i++) {
      expect(allNames.includes(`Product ${i}`)).toEqual(true);
    }
  });
});

test("Multi-collection paginate with duplicate sort values and beforeId", async () => {
  await withDatabase("Multi-collection paginate with duplicate sort values and beforeId", async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          status: v.string(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products where all have the same status
    const testData = [
      { name: "A", status: "active" },
      { name: "B", status: "active" },
      { name: "C", status: "active" },
      { name: "D", status: "active" },
      { name: "E", status: "active" },
      { name: "F", status: "active" },
    ];

    for (const item of testData) {
      await catalog.insertOne("product", item);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Get all items to find anchor
    const allItems = await catalog.paginate("product", {}, {
      limit: 6,
      sort: { status: 1 },
    });

    expect(allItems.data.length).toEqual(6);

    // Use beforeId with the 4th item as anchor
    const beforePage = await catalog.paginate("product", {}, {
      limit: 3,
      sort: { status: 1 },
      beforeId: allItems.data[3]._id,
    });

    expect(beforePage.data.length).toEqual(3);

    // Should return first 3 items in original order
    const beforeNames = beforePage.data.map((item) => item.name);
    const expectedNames = allItems.data.slice(0, 3).map((item) => item.name);

    for (let i = 0; i < 3; i++) {
      expect(beforeNames[i]).toEqual(expectedNames[i]);
    }
  });
});

test("Multi-collection paginate with _id descending sort and beforeId", async () => {
  await withDatabase("Multi-collection paginate with _id descending sort and beforeId", async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          value: v.number(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert 10 products
    for (let i = 1; i <= 10; i++) {
      await catalog.insertOne("product", { name: `Product ${i}`, value: i * 10 });
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Get all items with _id descending to find anchor
    const allItems = await catalog.paginate("product", {}, {
      limit: 10,
      sort: { _id: -1 },
    });

    // Order: Product 10, 9, 8, 7, 6, 5, 4, 3, 2, 1

    // Use beforeId with Product 5 (index 5) as anchor
    // Should return items BEFORE it in the sorted order: Product 10, 9, 8, 7, 6
    const beforePage = await catalog.paginate("product", {}, {
      limit: 5,
      sort: { _id: -1 },
      beforeId: allItems.data[5]._id, // Product 5
    });

    expect(beforePage.data.length).toEqual(5);

    // Should return in original sort order
    expect(beforePage.data[0].name).toEqual("Product 10");
    expect(beforePage.data[1].name).toEqual("Product 9");
    expect(beforePage.data[2].name).toEqual("Product 8");
    expect(beforePage.data[3].name).toEqual("Product 7");
    expect(beforePage.data[4].name).toEqual("Product 6");
  });
});

test("Multi-collection paginate accumulation with _id descending - no duplicates across 5+ pages", async () => {
  await withDatabase("Multi-collection paginate accumulation with _id descending - no duplicates across 5+ pages", async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          value: v.number(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert 25 products
    for (let i = 1; i <= 25; i++) {
      await catalog.insertOne("product", { name: `Product ${i}`, value: i * 10 });
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    const allCollectedIds: string[] = [];
    const allCollectedNames: string[] = [];

    // Page 1
    const page1 = await catalog.paginate("product", {}, {
      limit: 5,
      sort: { _id: -1 },
    });
    for (const item of page1.data) {
      allCollectedIds.push(item._id);
      allCollectedNames.push(item.name);
    }

    // Page 2
    const page2 = await catalog.paginate("product", {}, {
      limit: 5,
      sort: { _id: -1 },
      afterId: page1.data[page1.data.length - 1]._id,
    });
    for (const item of page2.data) {
      if (allCollectedIds.includes(item._id)) {
        throw new Error(`DUPLICATE on page 2: ${item.name} (${item._id})`);
      }
      allCollectedIds.push(item._id);
      allCollectedNames.push(item.name);
    }

    // Page 3
    const page3 = await catalog.paginate("product", {}, {
      limit: 5,
      sort: { _id: -1 },
      afterId: page2.data[page2.data.length - 1]._id,
    });
    for (const item of page3.data) {
      if (allCollectedIds.includes(item._id)) {
        throw new Error(`DUPLICATE on page 3: ${item.name} (${item._id})`);
      }
      allCollectedIds.push(item._id);
      allCollectedNames.push(item.name);
    }

    // Page 4
    const page4 = await catalog.paginate("product", {}, {
      limit: 5,
      sort: { _id: -1 },
      afterId: page3.data[page3.data.length - 1]._id,
    });
    for (const item of page4.data) {
      if (allCollectedIds.includes(item._id)) {
        throw new Error(`DUPLICATE on page 4: ${item.name} (${item._id})`);
      }
      allCollectedIds.push(item._id);
      allCollectedNames.push(item.name);
    }

    // Page 5
    const page5 = await catalog.paginate("product", {}, {
      limit: 5,
      sort: { _id: -1 },
      afterId: page4.data[page4.data.length - 1]._id,
    });
    for (const item of page5.data) {
      if (allCollectedIds.includes(item._id)) {
        throw new Error(`DUPLICATE on page 5: ${item.name} (${item._id})`);
      }
      allCollectedIds.push(item._id);
      allCollectedNames.push(item.name);
    }

    // Verify we got all 25 items with no duplicates
    expect(allCollectedIds.length).toEqual(25);

    // Verify order is correct (descending)
    expect(allCollectedNames[0]).toEqual("Product 25");
    expect(allCollectedNames[24]).toEqual("Product 1");

    // Verify no duplicates using Set
    const uniqueIds = new Set(allCollectedIds);
    expect(uniqueIds.size).toEqual(25);
  });
});

test("Multi-collection paginate with nested field sort", async () => {
  await withDatabase("Multi-collection paginate with nested field sort", async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        exhibitor: {
          data: v.object({
            company: v.string(),
            email: v.string(),
          }),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert exhibitors with nested data
    const exhibitors = [
      { data: { company: "Alpha Corp", email: "alpha@test.com" } },
      { data: { company: "Beta Inc", email: "beta@test.com" } },
      { data: { company: "Charlie LLC", email: "charlie@test.com" } },
      { data: { company: "Delta Ltd", email: "delta@test.com" } },
      { data: { company: "Echo Co", email: "echo@test.com" } },
      { data: { company: "Foxtrot SA", email: "foxtrot@test.com" } },
    ];

    for (const exhibitor of exhibitors) {
      await catalog.insertOne("exhibitor", exhibitor);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // First page sorted by nested field data.email ascending
    const page1 = await catalog.paginate("exhibitor", {}, {
      limit: 3,
      sort: { "data.email": 1 },
    });

    expect(page1.data.length).toEqual(3);
    expect(page1.total).toEqual(6);

    // Should be alpha, beta, charlie (alphabetical by email)
    expect(page1.data[0].data.email).toEqual("alpha@test.com");
    expect(page1.data[1].data.email).toEqual("beta@test.com");
    expect(page1.data[2].data.email).toEqual("charlie@test.com");

    // Second page with afterId
    const page2 = await catalog.paginate("exhibitor", {}, {
      limit: 3,
      sort: { "data.email": 1 },
      afterId: page1.data[page1.data.length - 1]._id,
    });

    expect(page2.data.length).toEqual(3);

    // Should be delta, echo, foxtrot
    expect(page2.data[0].data.email).toEqual("delta@test.com");
    expect(page2.data[1].data.email).toEqual("echo@test.com");
    expect(page2.data[2].data.email).toEqual("foxtrot@test.com");

    // Verify no duplicates between pages
    const page1Ids = new Set(page1.data.map((item) => item._id));
    for (const item of page2.data) {
      expect(page1Ids.has(item._id)).toEqual(false);
    }
  });
});

test("Multi-collection paginate with nested field sort descending", async () => {
  await withDatabase("Multi-collection paginate with nested field sort descending", async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        exhibitor: {
          data: v.object({
            company: v.string(),
            email: v.string(),
          }),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert exhibitors with nested data
    const exhibitors = [
      { data: { company: "Alpha Corp", email: "alpha@test.com" } },
      { data: { company: "Beta Inc", email: "beta@test.com" } },
      { data: { company: "Charlie LLC", email: "charlie@test.com" } },
      { data: { company: "Delta Ltd", email: "delta@test.com" } },
      { data: { company: "Echo Co", email: "echo@test.com" } },
      { data: { company: "Foxtrot SA", email: "foxtrot@test.com" } },
    ];

    for (const exhibitor of exhibitors) {
      await catalog.insertOne("exhibitor", exhibitor);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // First page sorted by nested field data.email descending
    const page1 = await catalog.paginate("exhibitor", {}, {
      limit: 3,
      sort: { "data.email": -1 },
    });

    expect(page1.data.length).toEqual(3);
    expect(page1.total).toEqual(6);

    // Should be foxtrot, echo, delta (reverse alphabetical by email)
    expect(page1.data[0].data.email).toEqual("foxtrot@test.com");
    expect(page1.data[1].data.email).toEqual("echo@test.com");
    expect(page1.data[2].data.email).toEqual("delta@test.com");

    // Second page with afterId
    const page2 = await catalog.paginate("exhibitor", {}, {
      limit: 3,
      sort: { "data.email": -1 },
      afterId: page1.data[page1.data.length - 1]._id,
    });

    expect(page2.data.length).toEqual(3);

    // Should be charlie, beta, alpha
    expect(page2.data[0].data.email).toEqual("charlie@test.com");
    expect(page2.data[1].data.email).toEqual("beta@test.com");
    expect(page2.data[2].data.email).toEqual("alpha@test.com");

    // Verify no duplicates between pages
    const page1Ids = new Set(page1.data.map((item) => item._id));
    for (const item of page2.data) {
      expect(page1Ids.has(item._id)).toEqual(false);
    }
  });
});

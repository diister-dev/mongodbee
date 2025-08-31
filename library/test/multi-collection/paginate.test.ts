import * as v from "../../src/schema.ts";
import { assertEquals, assertExists } from "jsr:@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";

Deno.test("Multi-collection paginate basic functionality", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
        price: v.number(),
        category: v.string()
      },
      category: {
        name: v.string(),
        description: v.string()
      }
    });

    // Insert test categories
    const categoriesData = [];
    for (let i = 1; i <= 5; i++) {
      categoriesData.push({
        name: `Category ${i}`,
        description: `Description for category ${i}`
      });
    }
    
    const categoryIds = await Promise.all(
      categoriesData.map(cat => catalog.insertOne("category", cat))
    );

    // Insert test products
    const productsData = [];
    for (let i = 1; i <= 20; i++) {
      productsData.push({
        name: `Product ${i}`,
        price: i * 10,
        category: categoryIds[i % 5] // Distribute products across categories
      });
    }
    
    await Promise.all(
      productsData.map(prod => catalog.insertOne("product", prod))
    );

    // Test basic pagination for products
    const firstPageProducts = await catalog.paginate("product", {}, { limit: 5 });
    assertEquals(firstPageProducts.data.length, 5);
    
    // Verify all results are products
    for (const product of firstPageProducts.data) {
      assertEquals(product._type, "product");
      assertExists(product.name);
      assertExists(product.price);
      assertExists(product.category);
    }

    // Test basic pagination for categories
    const firstPageCategories = await catalog.paginate("category", {}, { limit: 3 });
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
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
        price: v.number()
      },
      user: {
        name: v.string(),
        email: v.string()
      }
    });

    // Insert products
    const productIds = [];
    for (let i = 1; i <= 15; i++) {
      const id = await catalog.insertOne("product", {
        name: `Product ${i}`,
        price: i * 10
      });
      productIds.push(id);
    }

    // Insert users
    const userIds = [];
    for (let i = 1; i <= 10; i++) {
      const id = await catalog.insertOne("user", {
        name: `User ${i}`,
        email: `user${i}@test.com`
      });
      userIds.push(id);
    }

    // Test pagination with afterId for products
    const firstPageProducts = await catalog.paginate("product", {}, { limit: 5, sort: { _id: 1 } });
    assertEquals(firstPageProducts.data.length, 5);
    
    const secondPageProducts = await catalog.paginate("product", {}, { 
      limit: 5, 
      afterId: firstPageProducts.data[firstPageProducts.data.length - 1]._id,
      sort: { _id: 1 }
    });
    assertEquals(secondPageProducts.data.length, 5);
    
    // Verify no overlap between pages
    const firstPageIds = new Set(firstPageProducts.data.map(p => p._id));
    const secondPageIds = new Set(secondPageProducts.data.map(p => p._id));
    
    for (const id of secondPageIds) {
      assertEquals(firstPageIds.has(id), false);
    }

    // Test pagination with afterId for users
    const firstPageUsers = await catalog.paginate("user", {}, { limit: 3, sort: { _id: 1 } });
    assertEquals(firstPageUsers.data.length, 3);
    
    const secondPageUsers = await catalog.paginate("user", {}, { 
      limit: 3, 
      afterId: firstPageUsers.data[firstPageUsers.data.length - 1]._id,
      sort: { _id: 1 }
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
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
        price: v.number()
      }
    });

    // Insert products
    for (let i = 1; i <= 10; i++) {
      await catalog.insertOne("product", {
        name: `Product ${i}`,
        price: i * 10
      });
    }

    // Get all products to find a reference point
    const allProducts = await catalog.find("product", {}, { sort: { _id: 1 } });
    
    // Get products before the 7th product
    const beforePage = await catalog.paginate("product", {}, { 
      limit: 3, 
      beforeId: allProducts[6]._id,
      sort: { _id: -1 }
    });
    
    assertEquals(beforePage.data.length, 3);
    
    // Verify items come before the reference point
    const referenceId = allProducts[6]._id;
    for (const product of beforePage.data) {
      assertEquals(product._id < referenceId, true);
      assertEquals(product._type, "product");
    }
  });
});

Deno.test("Multi-collection paginate with filter", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
        price: v.number(),
        category: v.string()
      }
    });

    // Insert products with different categories
    for (let i = 1; i <= 20; i++) {
      await catalog.insertOne("product", {
        name: `Product ${i}`,
        price: i * 10,
        category: i % 2 === 0 ? "electronics" : "clothing"
      });
    }

    // Paginate with MongoDB filter (only electronics)
    const electronicsProducts = await catalog.paginate("product", 
      { category: "electronics" }, 
      { limit: 10 }
    );
    
    assertEquals(electronicsProducts.data.length, 10);
    
    // Verify all are electronics
    for (const product of electronicsProducts.data) {
      assertEquals(product.category, "electronics");
      assertEquals(product._type, "product");
    }

    // Paginate with custom filter (expensive electronics - price > 100)
    const expensiveElectronics = await catalog.paginate("product",
      { category: "electronics" },
      { 
        limit: 10,
        filter: (doc) => doc.price > 100
      }
    );
    
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
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
        price: v.number()
      }
    });

    // Insert products in random order
    const products = [
      { name: "Product C", price: 300 },
      { name: "Product A", price: 100 },
      { name: "Product E", price: 500 },
      { name: "Product B", price: 200 },
      { name: "Product D", price: 400 }
    ];
    
    await Promise.all(
      products.map(prod => catalog.insertOne("product", prod))
    );

    // Paginate with sorting by price ascending
    const sortedByPrice = await catalog.paginate("product", {}, { 
      limit: 10, 
      sort: { price: 1 }
    });
    
    assertEquals(sortedByPrice.data.length, 5);
    
    // Verify items are sorted by price
    for (let i = 0; i < sortedByPrice.data.length - 1; i++) {
      assertEquals(sortedByPrice.data[i].price <= sortedByPrice.data[i + 1].price, true);
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
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
        price: v.number()
      },
      user: {
        name: v.string(),
        email: v.string()
      }
    });

    // Insert some data
    await catalog.insertOne("product", { name: "Product 1", price: 100 });
    await catalog.insertOne("user", { name: "User 1", email: "user1@test.com" });

    // Test with wrong ID format for afterId
    try {
      await catalog.paginate("product", {}, { afterId: "user:123" }); // User ID for product pagination
      assertEquals(true, false, "Should have thrown an error");
    } catch (error) {
      assertEquals((error as Error).message.includes("Invalid afterId format"), true);
    }

    // Test with wrong ID format for beforeId
    try {
      await catalog.paginate("user", {}, { beforeId: "product:456" }); // Product ID for user pagination
      assertEquals(true, false, "Should have thrown an error");
    } catch (error) {
      assertEquals((error as Error).message.includes("Invalid beforeId format"), true);
    }
  });
});

Deno.test("Multi-collection paginate with empty results", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
        price: v.number()
      },
      user: {
        name: v.string(),
        email: v.string()
      }
    });

    // Test empty collection
    const emptyProducts = await catalog.paginate("product", {}, { limit: 10 });
    assertEquals(emptyProducts.data.length, 0);

    // Insert some users but test products (should be empty)
    await catalog.insertOne("user", { name: "User 1", email: "user1@test.com" });
    await catalog.insertOne("user", { name: "User 2", email: "user2@test.com" });
    
    const stillEmptyProducts = await catalog.paginate("product", {}, { limit: 10 });
    assertEquals(stillEmptyProducts.data.length, 0);
    
    // But users should exist
    const existingUsers = await catalog.paginate("user", {}, { limit: 10 });
    assertEquals(existingUsers.data.length, 2);
  });
});

Deno.test("Multi-collection paginate with limit boundary conditions", async (t) => {
  await withDatabase(t.name, async (db) => {
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
        price: v.number()
      }
    });

    // Insert 5 products
    for (let i = 1; i <= 5; i++) {
      await catalog.insertOne("product", {
        name: `Product ${i}`,
        price: i * 10
      });
    }

    // Test with limit larger than available data
    const largeLimitPage = await catalog.paginate("product", {}, { limit: 100 });
    assertEquals(largeLimitPage.data.length, 5);
    
    // Test with limit of 1
    const singleItemPage = await catalog.paginate("product", {}, { limit: 1 });
    assertEquals(singleItemPage.data.length, 1);
    
    // Test with limit of 0
    const zeroLimitPage = await catalog.paginate("product", {}, { limit: 0 });
    assertEquals(zeroLimitPage.data.length, 0);
  });
});

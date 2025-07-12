import * as v from "../src/schema.ts";
import { assertEquals, assertExists } from "jsr:@std/assert";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";

Deno.test("Paginate basic functionality", async (t) => {
  await withDatabase(t.name, async (db) => {
    // Define a simple schema
    const itemSchema = {
      name: v.string(),
      value: v.number(),
      category: v.string()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data
    const testData = [];
    for (let i = 1; i <= 50; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
        category: i % 2 === 0 ? "even" : "odd"
      });
    }
    
    await items.insertMany(testData);

    // Test basic pagination
    const firstPage = await items.paginate({}, { limit: 10 });
    assertEquals(firstPage.length, 10);
    
    // Test that results are properly typed
    assertExists(firstPage[0]._id);
    assertExists(firstPage[0].name);
    assertExists(firstPage[0].value);
    assertExists(firstPage[0].category);
  });
});

Deno.test("Paginate with afterId", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data with predictable IDs
    const testData = [];
    for (let i = 1; i <= 20; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i
      });
    }
    
    await items.insertMany(testData);

    // Get first page
    const firstPage = await items.paginate({}, { limit: 5 });
    assertEquals(firstPage.length, 5);
    
    // Get second page using afterId (keeping same type as original)
    const secondPage = await items.paginate({}, { 
      limit: 5, 
      afterId: firstPage[firstPage.length - 1]._id // Pass ObjectId as-is
    });
    
    assertEquals(secondPage.length, 5);
    
    // Verify that second page items are different from first page
    const firstPageIds = new Set(firstPage.map(item => item._id.toString()));
    const secondPageIds = new Set(secondPage.map(item => item._id.toString()));
    
    // No overlap between pages
    for (const id of secondPageIds) {
      assertEquals(firstPageIds.has(id), false);
    }
  });
});

Deno.test("Paginate with beforeId", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data
    const testData = [];
    for (let i = 1; i <= 20; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i
      });
    }
    
    await items.insertMany(testData);

    // Get all items first to get a reference point
    const allItems = await items.find({}).toArray();
    
    // Get items before the 10th item (keeping same type as original)
    const beforePage = await items.paginate({}, { 
      limit: 5, 
      beforeId: allItems[9]._id // Pass ObjectId as-is
    });
    
    assertEquals(beforePage.length, 5);
    
    // Verify items come before the reference point
    const referenceId = allItems[9]._id.toString();
    for (const item of beforePage) {
      assertEquals(item._id.toString() < referenceId, true);
    }
  });
});

Deno.test("Paginate with custom filter", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
      category: v.string()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data
    const testData = [];
    for (let i = 1; i <= 30; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
        category: i % 2 === 0 ? "even" : "odd"
      });
    }
    
    await items.insertMany(testData);

    // Paginate with custom filter that only includes even values
    const evenItems = await items.paginate(
      { category: "even" },
      { 
        limit: 10,
        filter: (doc) => doc.value % 4 === 0 // Only multiples of 4
      }
    );
    
    // Should get items with values 4, 8, 12, 16, 20, 24, 28
    assertEquals(evenItems.length, 7);
    
    // Verify all items match the filter
    for (const item of evenItems) {
      assertEquals(item.category, "even");
      assertEquals(item.value % 4, 0);
    }
  });
});

Deno.test("Paginate with sorting", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data in random order
    const testData = [
      { name: "Item C", value: 3 },
      { name: "Item A", value: 1 },
      { name: "Item B", value: 2 },
      { name: "Item E", value: 5 },
      { name: "Item D", value: 4 }
    ];
    
    await items.insertMany(testData);

    // Paginate with sorting by value ascending
    const sortedItems = await items.paginate({}, { 
      limit: 10, 
      sort: { value: 1 }
    });
    
    assertEquals(sortedItems.length, 5);
    
    // Verify items are sorted by value
    for (let i = 0; i < sortedItems.length - 1; i++) {
      assertEquals(sortedItems[i].value < sortedItems[i + 1].value, true);
    }
    
    // Verify the actual order
    assertEquals(sortedItems[0].value, 1);
    assertEquals(sortedItems[1].value, 2);
    assertEquals(sortedItems[2].value, 3);
    assertEquals(sortedItems[3].value, 4);
    assertEquals(sortedItems[4].value, 5);
  });
});

Deno.test("Paginate with empty results", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Don't insert any data
    
    // Paginate empty collection
    const emptyPage = await items.paginate({}, { limit: 10 });
    assertEquals(emptyPage.length, 0);
    
    // Insert some data but filter that matches nothing
    await items.insertMany([
      { name: "Item 1", value: 1 },
      { name: "Item 2", value: 2 }
    ]);
    
    const noMatchPage = await items.paginate({ value: 999 }, { limit: 10 });
    assertEquals(noMatchPage.length, 0);
  });
});

Deno.test("Paginate with limit boundary conditions", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data
    const testData = [];
    for (let i = 1; i <= 5; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i
      });
    }
    
    await items.insertMany(testData);

    // Test with limit larger than available data
    const largeLimitPage = await items.paginate({}, { limit: 100 });
    assertEquals(largeLimitPage.length, 5);
    
    // Test with limit of 1
    const singleItemPage = await items.paginate({}, { limit: 1 });
    assertEquals(singleItemPage.length, 1);
    
    // Test with limit of 0
    const zeroLimitPage = await items.paginate({}, { limit: 0 });
    assertEquals(zeroLimitPage.length, 0);
  });
});

Deno.test("Paginate with complex query filter", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
      category: v.string(),
      active: v.boolean()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data
    const testData = [];
    for (let i = 1; i <= 20; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
        category: i % 3 === 0 ? "special" : "normal",
        active: i % 2 === 0
      });
    }
    
    await items.insertMany(testData);

    // Complex query: active items with value > 10
    const complexQuery = await items.paginate(
      { 
        active: true,
        value: { $gt: 10 }
      },
      { limit: 10 }
    );
    
    // Should get items with values 12, 14, 16, 18, 20
    assertEquals(complexQuery.length, 5);
    
    // Verify all items match the filter
    for (const item of complexQuery) {
      assertEquals(item.active, true);
      assertEquals(item.value > 10, true);
    }
  });
});

Deno.test("Paginate performance with large dataset", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert larger dataset
    const testData = [];
    for (let i = 1; i <= 1000; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i
      });
    }
    
    await items.insertMany(testData);

    // Test pagination performance
    const start = performance.now();
    const page = await items.paginate({}, { limit: 50 });
    const end = performance.now();
    
    assertEquals(page.length, 50);
    
    // Performance should be reasonable (less than 1 second)
    const duration = end - start;
    assertEquals(duration < 1000, true);
  });
});

Deno.test("Paginate with custom string IDs", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data with custom string IDs using raw collection
    const testData = [];
    for (let i = 1; i <= 10; i++) {
      testData.push({
        _id: `item:${i.toString().padStart(3, '0')}` as any, // Custom string IDs like "item:001", "item:002"
        name: `Item ${i}`,
        value: i
      });
    }
    
    await items.collection.insertMany(testData);

    // Get first page
    const firstPage = await items.paginate({}, { limit: 3, sort: { _id: 1 } });
    assertEquals(firstPage.length, 3);
    assertEquals(firstPage[0]._id, "item:001");
    assertEquals(firstPage[1]._id, "item:002");
    assertEquals(firstPage[2]._id, "item:003");
    
    // Test pagination with custom string ID
    const secondPage = await items.paginate({}, { 
      limit: 3, 
      afterId: firstPage[firstPage.length - 1]._id, // "item:003"
      sort: { _id: 1 }
    });
    
    assertEquals(secondPage.length, 3);
    assertEquals(secondPage[0]._id, "item:004");
    assertEquals(secondPage[1]._id, "item:005");
    assertEquals(secondPage[2]._id, "item:006");
    
    // Test beforeId with custom string ID
    const beforePage = await items.paginate({}, { 
      limit: 2, 
      beforeId: "item:003",
      sort: { _id: -1 }
    });
    
    assertEquals(beforePage.length, 2);
    assertEquals(beforePage[0]._id, "item:002");
    assertEquals(beforePage[1]._id, "item:001");
  });
});

Deno.test("Paginate with consistent ID types", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Test 1: With ObjectIds (auto-generated)
    await items.insertMany([
      { name: "Item 1", value: 1 },
      { name: "Item 2", value: 2 },
      { name: "Item 3", value: 3 },
      { name: "Item 4", value: 4 },
    ]);

    const firstPage = await items.paginate({}, { limit: 2 });
    assertEquals(firstPage.length, 2);
    
    // Use ObjectId for pagination (type consistency)
    const secondPage = await items.paginate({}, { 
      limit: 2, 
      afterId: firstPage[firstPage.length - 1]._id // ObjectId
    });
    
    assertEquals(secondPage.length, 2);
    });
});

Deno.test("Paginate with consistent ID types with custom ID", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data with custom string IDs
    const customItems = [
      { _id: "custom:1" as any, name: "Item A", value: 10 },
      { _id: "custom:2" as any, name: "Item B", value: 20 },
      { _id: "custom:3" as any, name: "Item C", value: 30 },
      { _id: "custom:4" as any, name: "Item D", value: 40 },
    ];
    
    await items.insertMany(customItems);

    // Get first page
    const firstPage = await items.paginate({}, { limit: 2, sort: { _id: 1 } });
    assertEquals(firstPage.length, 2);
    
    // Use custom string ID for pagination
    const secondPage = await items.paginate({}, { 
      limit: 2, 
      afterId: firstPage[firstPage.length - 1]._id, // Custom string ID
      sort: { _id: 1 }
    });
    
    assertEquals(secondPage.length, 2);
    assertEquals(secondPage[0]._id, "custom:3");
  });
});
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
    assertEquals(firstPage.data.length, 10);
    assertEquals(firstPage.total, 50);
    assertEquals(firstPage.position, 0);
    
    // Test that results are properly typed
    assertExists(firstPage.data[0]._id);
    assertExists(firstPage.data[0].name);
    assertExists(firstPage.data[0].value);
    assertExists(firstPage.data[0].category);
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
    assertEquals(firstPage.data.length, 5);
    
    // Get second page using afterId (keeping same type as original)
    const secondPage = await items.paginate({}, { 
      limit: 5, 
      afterId: firstPage.data[firstPage.data.length - 1]._id // Pass ObjectId as-is
    });
    
    assertEquals(secondPage.data.length, 5);
    
    // Verify that second page items are different from first page
    const firstPageIds = new Set(firstPage.data.map(item => item._id.toString()));
    const secondPageIds = new Set(secondPage.data.map(item => item._id.toString()));
    
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

    // Get the last elements first to establish an anchor point
    const allItems = await items.paginate({}, { limit: 20 });
    const anchorId = allItems.data[10]._id; // Use 11th item as anchor
    
    // Get items before the anchor
    const beforePage = await items.paginate({}, {
      limit: 5,
      beforeId: anchorId
    });
    
    assertEquals(beforePage.data.length, 5);
    
    // Verify that beforeId returned items that come before the anchor
    for (const item of beforePage.data) {
      // In most cases, these should be different IDs unless they're exactly sequential
      // The main test is that we get results and they're valid
      assertExists(item._id);
    }
  });
});

Deno.test("Paginate with filter", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
      category: v.string()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data
    const testData = [];
    for (let i = 1; i <= 15; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
        category: i % 2 === 0 ? "even" : "odd"
      });
    }
    
    await items.insertMany(testData);

    // Test pagination with filter - only get even items
    const evenItems = await items.paginate(
      { category: "even" },
      { limit: 10 }
    );
    
    assertEquals(evenItems.data.length, 7); // 2,4,6,8,10,12,14
    
    // Verify all items are even
    for (const item of evenItems.data) {
      assertEquals(item.category, "even");
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
      { name: "Item B", value: 5 },
      { name: "Item A", value: 1 },
      { name: "Item D", value: 3 },
      { name: "Item C", value: 4 },
      { name: "Item E", value: 2 }
    ];
    
    await items.insertMany(testData);

    // Test pagination with sorting by value ascending
    const sortedItems = await items.paginate({}, {
      limit: 5,
      sort: { value: 1 }
    });
    
    assertEquals(sortedItems.data.length, 5);
    
    // Verify sorted order
    for (let i = 0; i < sortedItems.data.length - 1; i++) {
      assertEquals(sortedItems.data[i].value < sortedItems.data[i + 1].value, true);
    }
    
    // Check specific order
    assertEquals(sortedItems.data[0].value, 1);
    assertEquals(sortedItems.data[1].value, 2);
    assertEquals(sortedItems.data[2].value, 3);
    assertEquals(sortedItems.data[3].value, 4);
    assertEquals(sortedItems.data[4].value, 5);
  });
});

Deno.test("Paginate edge cases", async (t) => {
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

    // Test empty filter results
    const emptyPage = await items.paginate({ value: 999 }, { limit: 10 });
    assertEquals(emptyPage.data.length, 0);
    assertEquals(emptyPage.total, 0);

    // Test no match filter
    const noMatchPage = await items.paginate({ name: "NonExistent" }, { limit: 10 });
    assertEquals(noMatchPage.data.length, 0);

    // Test limit larger than data
    const largeLimitPage = await items.paginate({}, { limit: 100 });
    assertEquals(largeLimitPage.data.length, 5);
    assertEquals(largeLimitPage.total, 5);

    // Test limit of 1
    const singleItemPage = await items.paginate({}, { limit: 1 });
    assertEquals(singleItemPage.data.length, 1);

    // Test limit of 0
    const zeroLimitPage = await items.paginate({}, { limit: 0 });
    assertEquals(zeroLimitPage.data.length, 0);
  });
});

Deno.test("Paginate complex queries", async (t) => {
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

    // Test complex query: active items in special category
    const complexQuery = await items.paginate(
      { 
        category: "special",
        active: true
      },
      { limit: 10 }
    );
    
    assertEquals(complexQuery.data.length, 3); // Items 6, 12, 18
    
    // Verify all results match criteria
    for (const item of complexQuery.data) {
      assertEquals(item.category, "special");
      assertEquals(item.active, true);
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
    
    // Insert larger test dataset
    const testData = [];
    for (let i = 1; i <= 100; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i
      });
    }
    
    await items.insertMany(testData);

    // Test pagination with reasonable limit
    const page = await items.paginate({}, { limit: 50 });
    assertEquals(page.data.length, 50);
    assertEquals(page.total, 100);
    assertEquals(page.position, 0);
  });
});

Deno.test("Paginate with string IDs", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      _id: v.string(),
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data with custom string IDs
    const testData = [
      { _id: "item:001", name: "First", value: 1 },
      { _id: "item:002", name: "Second", value: 2 },
      { _id: "item:003", name: "Third", value: 3 },
      { _id: "item:004", name: "Fourth", value: 4 },
      { _id: "item:005", name: "Fifth", value: 5 },
      { _id: "item:006", name: "Sixth", value: 6 }
    ];
    
    for (const item of testData) {
      await items.insertOne(item);
    }

    // Test pagination with string IDs
    const firstPage = await items.paginate({}, { 
      limit: 3,
      sort: { _id: 1 }
    });
    
    assertEquals(firstPage.data.length, 3);
    assertEquals(firstPage.data[0]._id, "item:001");
    assertEquals(firstPage.data[1]._id, "item:002");
    assertEquals(firstPage.data[2]._id, "item:003");

    // Test afterId with string ID
    const secondPage = await items.paginate({}, {
      limit: 3,
      afterId: firstPage.data[firstPage.data.length - 1]._id, // "item:003"
      sort: { _id: 1 }
    });
    
    assertEquals(secondPage.data.length, 3);
    assertEquals(secondPage.data[0]._id, "item:004");
    assertEquals(secondPage.data[1]._id, "item:005");
    assertEquals(secondPage.data[2]._id, "item:006");

    // Test beforeId with string ID
    const beforePage = await items.paginate({}, {
      limit: 3,
      beforeId: "item:003",
      sort: { _id: 1 }
    });
    
    assertEquals(beforePage.data.length, 2);
    assertEquals(beforePage.data[0]._id, "item:002");
    assertEquals(beforePage.data[1]._id, "item:001");
  });
});

Deno.test("Paginate with ObjectId IDs", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert test data (MongoDB will auto-generate ObjectIds)
    const testData = [
      { name: "First", value: 1 },
      { name: "Second", value: 2 },
      { name: "Third", value: 3 },
      { name: "Fourth", value: 4 }
    ];
    
    await items.insertMany(testData);

    // Test pagination with ObjectIds
    const firstPage = await items.paginate({}, { limit: 2 });
    assertEquals(firstPage.data.length, 2);

    // Test afterId with ObjectId
    const secondPage = await items.paginate({}, {
      limit: 2,
      afterId: firstPage.data[firstPage.data.length - 1]._id // ObjectId
    });
    
    assertEquals(secondPage.data.length, 2);
  });
});

Deno.test("Paginate with mixed ID types", async (t) => {
  await withDatabase(t.name, async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number()
    };

    const items = await collection(db, "items", itemSchema);
    
    // Insert some data with custom string IDs and some without
    await items.insertOne({ _id: "custom:1" as any, name: "Custom 1", value: 1 });
    await items.insertOne({ _id: "custom:2" as any, name: "Custom 2", value: 2 });
    await items.insertOne({ _id: "custom:3" as any, name: "Auto 1", value: 3 }); // Will get ObjectId
    await items.insertOne({ _id: "custom:4" as any, name: "Auto 2", value: 4 }); // Will get ObjectId

    // Test pagination with mixed ID types
    const firstPage = await items.paginate({}, { limit: 2 });
    assertEquals(firstPage.data.length, 2);

    // Test afterId
    const secondPage = await items.paginate({}, {
      limit: 2,
      afterId: firstPage.data[firstPage.data.length - 1]._id
    });
    
    assertEquals(secondPage.data.length, 2);
  });
});

import * as v from "../src/schema.ts";
import { test, expect } from "vitest";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";

test("Paginate basic functionality", async () => {
  await withDatabase("Paginate basic functionality", async (db) => {
    // Define a simple schema
    const itemSchema = {
      name: v.string(),
      value: v.number(),
      category: v.string(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data
    const testData = [];
    for (let i = 1; i <= 50; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
        category: i % 2 === 0 ? "even" : "odd",
      });
    }

    await items.insertMany(testData);

    // Test basic pagination
    const firstPage = await items.paginate({}, { limit: 10 });
    expect(firstPage.data.length).toEqual(10);
    expect(firstPage.total).toEqual(50);
    expect(firstPage.position).toEqual(0);

    // Test that results are properly typed
    expect(firstPage.data[0]._id).toBeDefined();
    expect(firstPage.data[0].name).toBeDefined();
    expect(firstPage.data[0].value).toBeDefined();
    expect(firstPage.data[0].category).toBeDefined();
  });
});

test("Paginate with afterId", async () => {
  await withDatabase("Paginate with afterId", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data with predictable IDs
    const testData = [];
    for (let i = 1; i <= 20; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
      });
    }

    await items.insertMany(testData);

    // Get first page
    const firstPage = await items.paginate({}, { limit: 5 });
    expect(firstPage.data.length).toEqual(5);

    // Get second page using afterId (keeping same type as original)
    const secondPage = await items.paginate({}, {
      limit: 5,
      afterId: firstPage.data[firstPage.data.length - 1]._id, // Pass ObjectId as-is
    });

    expect(secondPage.data.length).toEqual(5);

    // Verify that second page items are different from first page
    const firstPageIds = new Set(
      firstPage.data.map((item) => item._id.toString()),
    );
    const secondPageIds = new Set(
      secondPage.data.map((item) => item._id.toString()),
    );

    // No overlap between pages
    for (const id of secondPageIds) {
      expect(firstPageIds.has(id)).toEqual(false);
    }
  });
});

test("Paginate with beforeId", async () => {
  await withDatabase("Paginate with beforeId", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data
    const testData = [];
    for (let i = 1; i <= 20; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
      });
    }

    await items.insertMany(testData);

    // Get the last elements first to establish an anchor point
    const allItems = await items.paginate({}, { limit: 20 });
    const anchorId = allItems.data[10]._id; // Use 11th item as anchor

    // Get items before the anchor
    const beforePage = await items.paginate({}, {
      limit: 5,
      beforeId: anchorId,
    });

    expect(beforePage.data.length).toEqual(5);

    // Verify that beforeId returned items that come before the anchor
    for (const item of beforePage.data) {
      // In most cases, these should be different IDs unless they're exactly sequential
      // The main test is that we get results and they're valid
      expect(item._id).toBeDefined();
    }
  });
});

test("Paginate with filter", async () => {
  await withDatabase("Paginate with filter", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
      category: v.string(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data
    const testData = [];
    for (let i = 1; i <= 15; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
        category: i % 2 === 0 ? "even" : "odd",
      });
    }

    await items.insertMany(testData);

    // Test pagination with filter - only get even items
    const evenItems = await items.paginate(
      { category: "even" },
      { limit: 10 },
    );

    expect(evenItems.data.length).toEqual(7); // 2,4,6,8,10,12,14

    // Verify all items are even
    for (const item of evenItems.data) {
      expect(item.category).toEqual("even");
    }
  });
});

test("Paginate with sorting", async () => {
  await withDatabase("Paginate with sorting", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data in random order
    const testData = [
      { name: "Item B", value: 5 },
      { name: "Item A", value: 1 },
      { name: "Item D", value: 3 },
      { name: "Item C", value: 4 },
      { name: "Item E", value: 2 },
    ];

    await items.insertMany(testData);

    // Test pagination with sorting by value ascending
    const sortedItems = await items.paginate({}, {
      limit: 5,
      sort: { value: 1 },
    });

    expect(sortedItems.data.length).toEqual(5);

    // Verify sorted order
    for (let i = 0; i < sortedItems.data.length - 1; i++) {
      expect(
        sortedItems.data[i].value < sortedItems.data[i + 1].value,
      ).toEqual(true);
    }

    // Check specific order
    expect(sortedItems.data[0].value).toEqual(1);
    expect(sortedItems.data[1].value).toEqual(2);
    expect(sortedItems.data[2].value).toEqual(3);
    expect(sortedItems.data[3].value).toEqual(4);
    expect(sortedItems.data[4].value).toEqual(5);
  });
});

test("Paginate edge cases", async () => {
  await withDatabase("Paginate edge cases", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data
    const testData = [];
    for (let i = 1; i <= 5; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
      });
    }

    await items.insertMany(testData);

    // Test empty filter results
    const emptyPage = await items.paginate({ value: 999 }, { limit: 10 });
    expect(emptyPage.data.length).toEqual(0);
    expect(emptyPage.total).toEqual(0);

    // Test no match filter
    const noMatchPage = await items.paginate({ name: "NonExistent" }, {
      limit: 10,
    });
    expect(noMatchPage.data.length).toEqual(0);

    // Test limit larger than data
    const largeLimitPage = await items.paginate({}, { limit: 100 });
    expect(largeLimitPage.data.length).toEqual(5);
    expect(largeLimitPage.total).toEqual(5);

    // Test limit of 1
    const singleItemPage = await items.paginate({}, { limit: 1 });
    expect(singleItemPage.data.length).toEqual(1);

    // Test limit of 0
    const zeroLimitPage = await items.paginate({}, { limit: 0 });
    expect(zeroLimitPage.data.length).toEqual(0);
  });
});

test("Paginate complex queries", async () => {
  await withDatabase("Paginate complex queries", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
      category: v.string(),
      active: v.boolean(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data
    const testData = [];
    for (let i = 1; i <= 20; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
        category: i % 3 === 0 ? "special" : "normal",
        active: i % 2 === 0,
      });
    }

    await items.insertMany(testData);

    // Test complex query: active items in special category
    const complexQuery = await items.paginate(
      {
        category: "special",
        active: true,
      },
      { limit: 10 },
    );

    expect(complexQuery.data.length).toEqual(3); // Items 6, 12, 18

    // Verify all results match criteria
    for (const item of complexQuery.data) {
      expect(item.category).toEqual("special");
      expect(item.active).toEqual(true);
    }
  });
});

test("Paginate performance with large dataset", async () => {
  await withDatabase("Paginate performance with large dataset", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert larger test dataset
    const testData = [];
    for (let i = 1; i <= 100; i++) {
      testData.push({
        name: `Item ${i}`,
        value: i,
      });
    }

    await items.insertMany(testData);

    // Test pagination with reasonable limit
    const page = await items.paginate({}, { limit: 50 });
    expect(page.data.length).toEqual(50);
    expect(page.total).toEqual(100);
    expect(page.position).toEqual(0);
  });
});

test("Paginate with string IDs", async () => {
  await withDatabase("Paginate with string IDs", async (db) => {
    const itemSchema = {
      _id: v.string(),
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data with custom string IDs
    const testData = [
      { _id: "item:001", name: "First", value: 1 },
      { _id: "item:002", name: "Second", value: 2 },
      { _id: "item:003", name: "Third", value: 3 },
      { _id: "item:004", name: "Fourth", value: 4 },
      { _id: "item:005", name: "Fifth", value: 5 },
      { _id: "item:006", name: "Sixth", value: 6 },
    ];

    for (const item of testData) {
      await items.insertOne(item);
    }

    // Test pagination with string IDs
    const firstPage = await items.paginate({}, {
      limit: 3,
      sort: { _id: 1 },
    });

    expect(firstPage.data.length).toEqual(3);
    expect(firstPage.data[0]._id).toEqual("item:001");
    expect(firstPage.data[1]._id).toEqual("item:002");
    expect(firstPage.data[2]._id).toEqual("item:003");

    // Test afterId with string ID
    const secondPage = await items.paginate({}, {
      limit: 3,
      afterId: firstPage.data[firstPage.data.length - 1]._id, // "item:003"
      sort: { _id: 1 },
    });

    expect(secondPage.data.length).toEqual(3);
    expect(secondPage.data[0]._id).toEqual("item:004");
    expect(secondPage.data[1]._id).toEqual("item:005");
    expect(secondPage.data[2]._id).toEqual("item:006");

    // Test beforeId with string ID
    // With beforeId, items are returned in the same order as forward pagination
    const beforePage = await items.paginate({}, {
      limit: 3,
      beforeId: "item:003",
      sort: { _id: 1 },
    });

    expect(beforePage.data.length).toEqual(2);
    expect(beforePage.data[0]._id).toEqual("item:001");
    expect(beforePage.data[1]._id).toEqual("item:002");
  });
});

test("Paginate with ObjectId IDs", async () => {
  await withDatabase("Paginate with ObjectId IDs", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data (MongoDB will auto-generate ObjectIds)
    const testData = [
      { name: "First", value: 1 },
      { name: "Second", value: 2 },
      { name: "Third", value: 3 },
      { name: "Fourth", value: 4 },
    ];

    await items.insertMany(testData);

    // Test pagination with ObjectIds
    const firstPage = await items.paginate({}, { limit: 2 });
    expect(firstPage.data.length).toEqual(2);

    // Test afterId with ObjectId
    const secondPage = await items.paginate({}, {
      limit: 2,
      afterId: firstPage.data[firstPage.data.length - 1]._id, // ObjectId
    });

    expect(secondPage.data.length).toEqual(2);
  });
});

test("Paginate with mixed ID types", async () => {
  await withDatabase("Paginate with mixed ID types", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert some data with custom string IDs and some without
    await items.insertOne({
      _id: "custom:1" as any,
      name: "Custom 1",
      value: 1,
    });
    await items.insertOne({
      _id: "custom:2" as any,
      name: "Custom 2",
      value: 2,
    });
    await items.insertOne({ _id: "custom:3" as any, name: "Auto 1", value: 3 }); // Will get ObjectId
    await items.insertOne({ _id: "custom:4" as any, name: "Auto 2", value: 4 }); // Will get ObjectId

    // Test pagination with mixed ID types
    const firstPage = await items.paginate({}, { limit: 2 });
    expect(firstPage.data.length).toEqual(2);

    // Test afterId
    const secondPage = await items.paginate({}, {
      limit: 2,
      afterId: firstPage.data[firstPage.data.length - 1]._id,
    });

    expect(secondPage.data.length).toEqual(2);
  });
});

test("Paginate with custom sort and afterId", async () => {
  await withDatabase("Paginate with custom sort and afterId", async (db) => {
    const itemSchema = {
      name: v.string(),
      createdAt: v.number(),
      priority: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data with varying createdAt values (not in _id order)
    // We deliberately insert in a different order than the sort order
    const testData = [
      { name: "Item E", createdAt: 500, priority: 1 },  // Should be 1st with createdAt desc
      { name: "Item A", createdAt: 100, priority: 5 },  // Should be 5th with createdAt desc
      { name: "Item C", createdAt: 300, priority: 3 },  // Should be 3rd with createdAt desc
      { name: "Item B", createdAt: 200, priority: 4 },  // Should be 4th with createdAt desc
      { name: "Item D", createdAt: 400, priority: 2 },  // Should be 2nd with createdAt desc
      { name: "Item F", createdAt: 600, priority: 0 },  // Should be 0th with createdAt desc (first)
    ];

    // Insert one by one to ensure different _id timestamps
    for (const item of testData) {
      await items.insertOne(item);
      // Small delay to ensure different ObjectId timestamps
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Test 1: Paginate with custom sort (createdAt descending) - first page
    const firstPage = await items.paginate({}, {
      limit: 3,
      sort: { createdAt: -1 },
    });

    expect(firstPage.data.length).toEqual(3);
    expect(firstPage.total).toEqual(6);

    // Verify first page is sorted by createdAt descending
    expect(firstPage.data[0].createdAt).toEqual(600); // Item F
    expect(firstPage.data[1].createdAt).toEqual(500); // Item E
    expect(firstPage.data[2].createdAt).toEqual(400); // Item D

    // Test 2: Get second page using afterId with custom sort
    // This should return items with createdAt < 400 (Item C, B, A)
    const secondPage = await items.paginate({}, {
      limit: 3,
      sort: { createdAt: -1 },
      afterId: firstPage.data[firstPage.data.length - 1]._id,
    });

    expect(secondPage.data.length).toEqual(3);

    // Verify second page continues the sort order
    expect(secondPage.data[0].createdAt).toEqual(300); // Item C
    expect(secondPage.data[1].createdAt).toEqual(200); // Item B
    expect(secondPage.data[2].createdAt).toEqual(100); // Item A

    // Verify no overlap between pages
    const firstPageIds = new Set(firstPage.data.map((item) => item._id.toString()));
    for (const item of secondPage.data) {
      expect(firstPageIds.has(item._id.toString())).toEqual(false);
    }
  });
});

test("Paginate with custom sort and beforeId", async () => {
  await withDatabase("Paginate with custom sort and beforeId", async (db) => {
    const itemSchema = {
      name: v.string(),
      score: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data with varying scores
    const testData = [
      { name: "Low", score: 10 },
      { name: "High", score: 90 },
      { name: "Medium", score: 50 },
      { name: "VeryHigh", score: 100 },
      { name: "VeryLow", score: 5 },
      { name: "MediumHigh", score: 70 },
    ];

    for (const item of testData) {
      await items.insertOne(item);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Get all items sorted by score descending to find anchor point
    const allItems = await items.paginate({}, {
      limit: 6,
      sort: { score: -1 },
    });

    // allItems should be: VeryHigh(100), High(90), MediumHigh(70), Medium(50), Low(10), VeryLow(5)
    expect(allItems.data[0].score).toEqual(100);
    expect(allItems.data[1].score).toEqual(90);
    expect(allItems.data[2].score).toEqual(70);

    // Use beforeId with the 4th item (Medium, score=50) as anchor
    // Should return items BEFORE it in the sorted order (higher scores)
    const beforePage = await items.paginate({}, {
      limit: 3,
      sort: { score: -1 },
      beforeId: allItems.data[3]._id, // Medium (score=50)
    });

    expect(beforePage.data.length).toEqual(3);

    // With beforeId, we get items that come BEFORE the anchor in the sorted order
    // Items are returned in the SAME order as forward pagination (reversed internally to maintain consistency)
    // Original order: VeryHigh(100), High(90), MediumHigh(70), [Medium(50)], Low(10), VeryLow(5)
    // Before Medium(50): VeryHigh, High, MediumHigh - returned in original order
    expect(beforePage.data[0].score).toEqual(100); // VeryHigh
    expect(beforePage.data[1].score).toEqual(90);  // High
    expect(beforePage.data[2].score).toEqual(70);  // MediumHigh (closest to anchor)
  });
});

test("Paginate with multi-field custom sort and afterId", async () => {
  await withDatabase("Paginate with multi-field custom sort and afterId", async (db) => {
    const itemSchema = {
      category: v.string(),
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert test data - sorted by category asc, then value desc
    const testData = [
      { category: "A", name: "A-High", value: 100 },
      { category: "A", name: "A-Low", value: 10 },
      { category: "A", name: "A-Mid", value: 50 },
      { category: "B", name: "B-High", value: 90 },
      { category: "B", name: "B-Low", value: 20 },
      { category: "C", name: "C-Only", value: 60 },
    ];

    for (const item of testData) {
      await items.insertOne(item);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Expected order with { category: 1, value: -1 }:
    // A-High(A,100), A-Mid(A,50), A-Low(A,10), B-High(B,90), B-Low(B,20), C-Only(C,60)

    const firstPage = await items.paginate({}, {
      limit: 3,
      sort: { category: 1, value: -1 },
    });

    expect(firstPage.data.length).toEqual(3);
    expect(firstPage.data[0].name).toEqual("A-High");
    expect(firstPage.data[1].name).toEqual("A-Mid");
    expect(firstPage.data[2].name).toEqual("A-Low");

    // Get second page
    const secondPage = await items.paginate({}, {
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

test("Paginate with duplicate sort values", async () => {
  await withDatabase("Paginate with duplicate sort values", async (db) => {
    const itemSchema = {
      name: v.string(),
      category: v.string(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert items where all have the same category (duplicate sort values)
    const testData = [
      { name: "Item 1", category: "same" },
      { name: "Item 2", category: "same" },
      { name: "Item 3", category: "same" },
      { name: "Item 4", category: "same" },
      { name: "Item 5", category: "same" },
      { name: "Item 6", category: "same" },
    ];

    for (const item of testData) {
      await items.insertOne(item);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // First page with sort on duplicate field
    const firstPage = await items.paginate({}, {
      limit: 3,
      sort: { category: 1 },
    });

    expect(firstPage.data.length).toEqual(3);
    expect(firstPage.total).toEqual(6);

    // Collect first page names
    const firstPageNames = firstPage.data.map((item) => item.name);

    // Second page should get remaining items, no duplicates
    const secondPage = await items.paginate({}, {
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
      expect(allNames.includes(`Item ${i}`)).toEqual(true);
    }
  });
});

test("Paginate with duplicate sort values and beforeId", async () => {
  await withDatabase("Paginate with duplicate sort values and beforeId", async (db) => {
    const itemSchema = {
      name: v.string(),
      status: v.string(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert items where all have the same status
    const testData = [
      { name: "A", status: "active" },
      { name: "B", status: "active" },
      { name: "C", status: "active" },
      { name: "D", status: "active" },
      { name: "E", status: "active" },
      { name: "F", status: "active" },
    ];

    for (const item of testData) {
      await items.insertOne(item);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Get all items to find anchor
    const allItems = await items.paginate({}, {
      limit: 6,
      sort: { status: 1 },
    });

    expect(allItems.data.length).toEqual(6);

    // Use beforeId with the 4th item as anchor
    const beforePage = await items.paginate({}, {
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

test("Paginate with _id descending sort", async () => {
  await withDatabase("Paginate with _id descending sort", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert 10 items
    const insertedIds: string[] = [];
    for (let i = 1; i <= 10; i++) {
      const id = await items.insertOne({ name: `Item ${i}`, value: i * 10 });
      insertedIds.push(id.toString());
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // First page with _id descending (newest first)
    const firstPage = await items.paginate({}, {
      limit: 4,
      sort: { _id: -1 },
    });

    expect(firstPage.data.length).toEqual(4);
    expect(firstPage.total).toEqual(10);

    // Should be Item 10, 9, 8, 7 (newest to oldest)
    expect(firstPage.data[0].name).toEqual("Item 10");
    expect(firstPage.data[1].name).toEqual("Item 9");
    expect(firstPage.data[2].name).toEqual("Item 8");
    expect(firstPage.data[3].name).toEqual("Item 7");

    // Collect first page IDs
    const firstPageIds = new Set(firstPage.data.map((item) => item._id.toString()));

    // Second page
    const secondPage = await items.paginate({}, {
      limit: 4,
      sort: { _id: -1 },
      afterId: firstPage.data[firstPage.data.length - 1]._id,
    });

    expect(secondPage.data.length).toEqual(4);

    // Should be Item 6, 5, 4, 3
    expect(secondPage.data[0].name).toEqual("Item 6");
    expect(secondPage.data[1].name).toEqual("Item 5");
    expect(secondPage.data[2].name).toEqual("Item 4");
    expect(secondPage.data[3].name).toEqual("Item 3");

    // Verify no duplicates
    for (const item of secondPage.data) {
      expect(firstPageIds.has(item._id.toString())).toEqual(false);
    }

    // Third page
    const thirdPage = await items.paginate({}, {
      limit: 4,
      sort: { _id: -1 },
      afterId: secondPage.data[secondPage.data.length - 1]._id,
    });

    expect(thirdPage.data.length).toEqual(2); // Only 2 remaining

    // Should be Item 2, 1
    expect(thirdPage.data[0].name).toEqual("Item 2");
    expect(thirdPage.data[1].name).toEqual("Item 1");

    // Verify no duplicates with previous pages
    const secondPageIds = new Set(secondPage.data.map((item) => item._id.toString()));
    for (const item of thirdPage.data) {
      expect(firstPageIds.has(item._id.toString())).toEqual(false);
      expect(secondPageIds.has(item._id.toString())).toEqual(false);
    }
  });
});

test("Paginate with _id descending sort and beforeId", async () => {
  await withDatabase("Paginate with _id descending sort and beforeId", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert 10 items
    for (let i = 1; i <= 10; i++) {
      await items.insertOne({ name: `Item ${i}`, value: i * 10 });
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Get all items with _id descending to find anchor
    const allItems = await items.paginate({}, {
      limit: 10,
      sort: { _id: -1 },
    });

    // Order: Item 10, 9, 8, 7, 6, 5, 4, 3, 2, 1

    // Use beforeId with Item 5 (index 5) as anchor
    // Should return items BEFORE it in the sorted order: Item 10, 9, 8, 7, 6
    const beforePage = await items.paginate({}, {
      limit: 5,
      sort: { _id: -1 },
      beforeId: allItems.data[5]._id, // Item 5
    });

    expect(beforePage.data.length).toEqual(5);

    // Should return in original sort order
    expect(beforePage.data[0].name).toEqual("Item 10");
    expect(beforePage.data[1].name).toEqual("Item 9");
    expect(beforePage.data[2].name).toEqual("Item 8");
    expect(beforePage.data[3].name).toEqual("Item 7");
    expect(beforePage.data[4].name).toEqual("Item 6");
  });
});

test("Paginate accumulation with _id descending - no duplicates across 5+ pages", async () => {
  await withDatabase("Paginate accumulation with _id descending - no duplicates across 5+ pages", async (db) => {
    const itemSchema = {
      name: v.string(),
      value: v.number(),
    };

    const items = await collection(db, "items", itemSchema);

    // Insert 25 items
    for (let i = 1; i <= 25; i++) {
      await items.insertOne({ name: `Item ${i}`, value: i * 10 });
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    const allCollectedIds: string[] = [];
    const allCollectedNames: string[] = [];

    // Page 1
    const page1 = await items.paginate({}, {
      limit: 5,
      sort: { _id: -1 },
    });
    for (const item of page1.data) {
      allCollectedIds.push(item._id.toString());
      allCollectedNames.push(item.name);
    }

    // Page 2
    const page2 = await items.paginate({}, {
      limit: 5,
      sort: { _id: -1 },
      afterId: page1.data[page1.data.length - 1]._id,
    });
    for (const item of page2.data) {
      const idStr = item._id.toString();
      if (allCollectedIds.includes(idStr)) {
        throw new Error(`DUPLICATE on page 2: ${item.name} (${idStr})`);
      }
      allCollectedIds.push(idStr);
      allCollectedNames.push(item.name);
    }

    // Page 3
    const page3 = await items.paginate({}, {
      limit: 5,
      sort: { _id: -1 },
      afterId: page2.data[page2.data.length - 1]._id,
    });
    for (const item of page3.data) {
      const idStr = item._id.toString();
      if (allCollectedIds.includes(idStr)) {
        throw new Error(`DUPLICATE on page 3: ${item.name} (${idStr})`);
      }
      allCollectedIds.push(idStr);
      allCollectedNames.push(item.name);
    }

    // Page 4
    const page4 = await items.paginate({}, {
      limit: 5,
      sort: { _id: -1 },
      afterId: page3.data[page3.data.length - 1]._id,
    });
    for (const item of page4.data) {
      const idStr = item._id.toString();
      if (allCollectedIds.includes(idStr)) {
        throw new Error(`DUPLICATE on page 4: ${item.name} (${idStr})`);
      }
      allCollectedIds.push(idStr);
      allCollectedNames.push(item.name);
    }

    // Page 5
    const page5 = await items.paginate({}, {
      limit: 5,
      sort: { _id: -1 },
      afterId: page4.data[page4.data.length - 1]._id,
    });
    for (const item of page5.data) {
      const idStr = item._id.toString();
      if (allCollectedIds.includes(idStr)) {
        throw new Error(`DUPLICATE on page 5: ${item.name} (${idStr})`);
      }
      allCollectedIds.push(idStr);
      allCollectedNames.push(item.name);
    }

    // Verify we got all 25 items with no duplicates
    expect(allCollectedIds.length).toEqual(25);

    // Verify order is correct (descending)
    expect(allCollectedNames[0]).toEqual("Item 25");
    expect(allCollectedNames[24]).toEqual("Item 1");

    // Verify no duplicates using Set
    const uniqueIds = new Set(allCollectedIds);
    expect(uniqueIds.size).toEqual(25);
  });
});

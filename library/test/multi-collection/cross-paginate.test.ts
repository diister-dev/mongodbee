import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

test("Cross-pagination: paginate across two types", async () => {
  await withDatabase("Cross-pagination: paginate across two types", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
          email: v.string(),
          department: v.string(),
        },
        visitor: {
          name: v.string(),
          email: v.string(),
          company: v.string(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert collaborators
    for (let i = 1; i <= 5; i++) {
      await people.insertOne("collaborator", {
        name: `Collaborator ${i}`,
        email: `collab${i}@company.com`,
        department: `Dept ${i % 3}`,
      });
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Insert visitors
    for (let i = 1; i <= 5; i++) {
      await people.insertOne("visitor", {
        name: `Visitor ${i}`,
        email: `visitor${i}@external.com`,
        company: `Company ${i % 2}`,
      });
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    // Cross-paginate both types
    const result = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 10,
      sort: { _id: 1 },
    });

    // Should get all 10 documents
    expect(result.total).toEqual(10);
    expect(result.data.length).toEqual(10);

    // Verify we have both types in the result
    const types = new Set(result.data.map((doc) => doc._type));
    expect(types.has("collaborator")).toEqual(true);
    expect(types.has("visitor")).toEqual(true);

    // Verify all documents have required common fields
    for (const doc of result.data) {
      expect(doc._id).toBeDefined();
      expect(doc._type).toBeDefined();
      expect(doc.name).toBeDefined();
      expect(doc.email).toBeDefined();
    }
  });
});

test("Cross-pagination: paginate with limit smaller than total", async () => {
  await withDatabase("Cross-pagination: paginate with limit smaller than total", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
          createdAt: v.number(),
        },
        visitor: {
          name: v.string(),
          createdAt: v.number(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert interleaved data with explicit timestamps
    const now = Date.now();
    await people.insertOne("collaborator", { name: "C1", createdAt: now + 1 });
    await people.insertOne("visitor", { name: "V1", createdAt: now + 2 });
    await people.insertOne("collaborator", { name: "C2", createdAt: now + 3 });
    await people.insertOne("visitor", { name: "V2", createdAt: now + 4 });
    await people.insertOne("collaborator", { name: "C3", createdAt: now + 5 });
    await people.insertOne("visitor", { name: "V3", createdAt: now + 6 });

    // Cross-paginate with limit
    const page1 = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 3,
      sort: { createdAt: 1 },
    });

    expect(page1.total).toEqual(6);
    expect(page1.data.length).toEqual(3);
    expect(page1.position).toEqual(0);

    // Verify order by createdAt
    expect(page1.data[0].name).toEqual("C1");
    expect(page1.data[1].name).toEqual("V1");
    expect(page1.data[2].name).toEqual("C2");
  });
});

test("Cross-pagination: afterId with multiple types", async () => {
  await withDatabase("Cross-pagination: afterId with multiple types", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
          createdAt: v.number(),
        },
        visitor: {
          name: v.string(),
          createdAt: v.number(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert interleaved data
    const now = Date.now();
    await people.insertOne("collaborator", { name: "C1", createdAt: now + 1 });
    await people.insertOne("visitor", { name: "V1", createdAt: now + 2 });
    await people.insertOne("collaborator", { name: "C2", createdAt: now + 3 });
    await people.insertOne("visitor", { name: "V2", createdAt: now + 4 });
    await people.insertOne("collaborator", { name: "C3", createdAt: now + 5 });
    await people.insertOne("visitor", { name: "V3", createdAt: now + 6 });

    // Get first page
    const page1 = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 3,
      sort: { createdAt: 1 },
    });

    expect(page1.data.length).toEqual(3);
    const lastId = page1.data[page1.data.length - 1]._id;

    // Get second page using afterId
    const page2 = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 3,
      sort: { createdAt: 1 },
      afterId: lastId,
    });

    expect(page2.data.length).toEqual(3);

    // Verify continuation
    expect(page2.data[0].name).toEqual("V2");
    expect(page2.data[1].name).toEqual("C3");
    expect(page2.data[2].name).toEqual("V3");

    // Verify no overlap
    const page1Ids = new Set(page1.data.map((d) => d._id));
    for (const doc of page2.data) {
      expect(page1Ids.has(doc._id)).toEqual(false);
    }
  });
});

test("Cross-pagination: afterId can be from any of the allowed types", async () => {
  await withDatabase("Cross-pagination: afterId can be from any of the allowed types", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
        },
        visitor: {
          name: v.string(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert data with delays to ensure ULID ordering
    await people.insertOne("collaborator", { name: "C1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    const visitorId = await people.insertOne("visitor", { name: "V1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("visitor", { name: "V2" });

    // First get all items to understand the order
    const all = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 10,
      sort: { _id: 1 },
    });

    // Find the position of V1 in the sorted list
    const v1Index = all.data.findIndex((d) => d._id === visitorId);

    // Use V1's ID as afterId when paginating both types
    const result = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 10,
      sort: { _id: 1 },
      afterId: visitorId, // This is a visitor ID
    });

    // Should work - visitor ID is valid for cross-pagination
    // Items after V1 should be returned
    const expectedCount = all.data.length - v1Index - 1;
    expect(result.data.length).toEqual(expectedCount);

    // Verify V1 is not in the result (we're paginating AFTER it)
    const hasV1 = result.data.some((d) => d._id === visitorId);
    expect(hasV1).toEqual(false);
  });
});

test("Cross-pagination: invalid afterId format throws error", async () => {
  await withDatabase("Cross-pagination: invalid afterId format throws error", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
        },
        visitor: {
          name: v.string(),
        },
        admin: {
          name: v.string(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert some data
    await people.insertOne("collaborator", { name: "C1" });
    const adminId = await people.insertOne("admin", { name: "A1" });

    // Try to use admin ID when paginating only collaborator and visitor
    try {
      await people.paginate(["collaborator", "visitor"], {}, {
        afterId: adminId, // admin is NOT in the allowed types
      });
      expect(true).toEqual(false);
    } catch (error) {
      expect(
        (error as Error).message.includes("Invalid afterId format"),
      ).toEqual(true);
      expect(
        (error as Error).message.includes("collaborator"),
      ).toEqual(true);
      expect(
        (error as Error).message.includes("visitor"),
      ).toEqual(true);
    }
  });
});

test("Cross-pagination: beforeId with multiple types", async () => {
  await withDatabase("Cross-pagination: beforeId with multiple types", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
          createdAt: v.number(),
        },
        visitor: {
          name: v.string(),
          createdAt: v.number(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert interleaved data
    const now = Date.now();
    await people.insertOne("collaborator", { name: "C1", createdAt: now + 1 });
    await people.insertOne("visitor", { name: "V1", createdAt: now + 2 });
    await people.insertOne("collaborator", { name: "C2", createdAt: now + 3 });
    await people.insertOne("visitor", { name: "V2", createdAt: now + 4 });
    await people.insertOne("collaborator", { name: "C3", createdAt: now + 5 });
    await people.insertOne("visitor", { name: "V3", createdAt: now + 6 });

    // Get all to find anchor
    const all = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 10,
      sort: { createdAt: 1 },
    });

    // Use beforeId with the 4th item as anchor (V2)
    const beforePage = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 3,
      sort: { createdAt: 1 },
      beforeId: all.data[3]._id,
    });

    expect(beforePage.data.length).toEqual(3);

    // Should return first 3 items in original order
    expect(beforePage.data[0].name).toEqual("C1");
    expect(beforePage.data[1].name).toEqual("V1");
    expect(beforePage.data[2].name).toEqual("C2");
  });
});

test("Cross-pagination: with filter applied to all types", async () => {
  await withDatabase("Cross-pagination: with filter applied to all types", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
          active: v.boolean(),
        },
        visitor: {
          name: v.string(),
          active: v.boolean(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert data with active status
    await people.insertOne("collaborator", { name: "C1", active: true });
    await people.insertOne("collaborator", { name: "C2", active: false });
    await people.insertOne("visitor", { name: "V1", active: true });
    await people.insertOne("visitor", { name: "V2", active: false });
    await people.insertOne("collaborator", { name: "C3", active: true });
    await people.insertOne("visitor", { name: "V3", active: true });

    // Cross-paginate with filter
    const result = await people.paginate(
      ["collaborator", "visitor"],
      { active: true },
      { limit: 10 },
    );

    // Should only get active ones
    expect(result.total).toEqual(4);
    expect(result.data.length).toEqual(4);

    for (const doc of result.data) {
      expect(doc.active).toEqual(true);
    }
  });
});

test("Cross-pagination: with custom sort descending", async () => {
  await withDatabase("Cross-pagination: with custom sort descending", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
          score: v.number(),
        },
        visitor: {
          name: v.string(),
          score: v.number(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert with scores
    await people.insertOne("collaborator", { name: "C1", score: 10 });
    await people.insertOne("visitor", { name: "V1", score: 50 });
    await people.insertOne("collaborator", { name: "C2", score: 30 });
    await people.insertOne("visitor", { name: "V2", score: 20 });
    await people.insertOne("collaborator", { name: "C3", score: 40 });

    // Cross-paginate with descending sort
    const result = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 10,
      sort: { score: -1 },
    });

    expect(result.data.length).toEqual(5);

    // Verify descending order
    expect(result.data[0].score).toEqual(50); // V1
    expect(result.data[1].score).toEqual(40); // C3
    expect(result.data[2].score).toEqual(30); // C2
    expect(result.data[3].score).toEqual(20); // V2
    expect(result.data[4].score).toEqual(10); // C1
  });
});

test("Cross-pagination: three types simultaneously", async () => {
  await withDatabase("Cross-pagination: three types simultaneously", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        employee: {
          name: v.string(),
          createdAt: v.number(),
        },
        contractor: {
          name: v.string(),
          createdAt: v.number(),
        },
        intern: {
          name: v.string(),
          createdAt: v.number(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    const now = Date.now();
    await people.insertOne("employee", { name: "E1", createdAt: now + 1 });
    await people.insertOne("contractor", { name: "C1", createdAt: now + 2 });
    await people.insertOne("intern", { name: "I1", createdAt: now + 3 });
    await people.insertOne("employee", { name: "E2", createdAt: now + 4 });
    await people.insertOne("contractor", { name: "C2", createdAt: now + 5 });
    await people.insertOne("intern", { name: "I2", createdAt: now + 6 });

    // Paginate all three types
    const result = await people.paginate(
      ["employee", "contractor", "intern"],
      {},
      { limit: 10, sort: { createdAt: 1 } },
    );

    expect(result.total).toEqual(6);
    expect(result.data.length).toEqual(6);

    // Verify all types present
    const types = new Set(result.data.map((d) => d._type));
    expect(types.size).toEqual(3);
    expect(types.has("employee")).toEqual(true);
    expect(types.has("contractor")).toEqual(true);
    expect(types.has("intern")).toEqual(true);

    // Verify order
    expect(result.data[0].name).toEqual("E1");
    expect(result.data[1].name).toEqual("C1");
    expect(result.data[2].name).toEqual("I1");
    expect(result.data[3].name).toEqual("E2");
    expect(result.data[4].name).toEqual("C2");
    expect(result.data[5].name).toEqual("I2");
  });
});

test("Cross-pagination: single type array behaves like single key", async () => {
  await withDatabase("Cross-pagination: single type array behaves like single key", async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
        },
        category: {
          name: v.string(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products
    for (let i = 1; i <= 5; i++) {
      await catalog.insertOne("product", { name: `Product ${i}`, price: i * 10 });
    }

    // Insert categories
    for (let i = 1; i <= 3; i++) {
      await catalog.insertOne("category", { name: `Category ${i}` });
    }

    // Paginate with single-element array
    const resultArray = await catalog.paginate(["product"], {}, { limit: 10 });

    // Paginate with single key (traditional)
    const resultSingle = await catalog.paginate("product", {}, { limit: 10 });

    // Should have same results
    expect(resultArray.total).toEqual(resultSingle.total);
    expect(resultArray.data.length).toEqual(resultSingle.data.length);
    expect(resultArray.total).toEqual(5);

    // All should be products
    for (const doc of resultArray.data) {
      expect(doc._type).toEqual("product");
    }
  });
});

test("Cross-pagination: accumulation across 5+ pages with no duplicates", async () => {
  await withDatabase("Cross-pagination: accumulation across 5+ pages with no duplicates", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
        },
        visitor: {
          name: v.string(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert 15 of each type = 30 total
    for (let i = 1; i <= 15; i++) {
      await people.insertOne("collaborator", { name: `C${i}` });
      await new Promise((resolve) => setTimeout(resolve, 5));
      await people.insertOne("visitor", { name: `V${i}` });
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    const allCollectedIds: string[] = [];
    let lastId: string | undefined;

    // Paginate through all pages
    for (let page = 1; page <= 6; page++) {
      const result = await people.paginate(["collaborator", "visitor"], {}, {
        limit: 5,
        sort: { _id: 1 },
        afterId: lastId,
      });

      for (const item of result.data) {
        if (allCollectedIds.includes(item._id)) {
          throw new Error(`DUPLICATE on page ${page}: ${item.name} (${item._id})`);
        }
        allCollectedIds.push(item._id);
      }

      if (result.data.length === 0) break;
      lastId = result.data[result.data.length - 1]._id;
    }

    // Should have all 30 items
    expect(allCollectedIds.length).toEqual(30);

    // Verify no duplicates using Set
    const uniqueIds = new Set(allCollectedIds);
    expect(uniqueIds.size).toEqual(30);
  });
});

test("Cross-pagination: with prepare, filter, and format", async () => {
  await withDatabase("Cross-pagination: with prepare, filter, and format", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
          salary: v.number(),
        },
        visitor: {
          name: v.string(),
          salary: v.number(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    await people.insertOne("collaborator", { name: "C1", salary: 50000 });
    await people.insertOne("visitor", { name: "V1", salary: 30000 });
    await people.insertOne("collaborator", { name: "C2", salary: 70000 });
    await people.insertOne("visitor", { name: "V2", salary: 40000 });

    const result = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 10,
      prepare: (doc) => ({ ...doc, bonusEligible: doc.salary > 45000 }),
      filter: (doc) => doc.bonusEligible === true,
      format: (doc) => ({ name: doc.name, bonus: doc.bonusEligible }),
    });

    // Only C1 (50000) and C2 (70000) have salary > 45000
    expect(result.data.length).toEqual(2);

    for (const doc of result.data) {
      expect(doc.bonus).toEqual(true);
      expect(doc.name).toBeDefined();
    }
  });
});

test("Cross-pagination: empty result when no matching types", async () => {
  await withDatabase("Cross-pagination: empty result when no matching types", async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
        },
        category: {
          name: v.string(),
        },
        tag: {
          name: v.string(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Only insert tags
    await catalog.insertOne("tag", { name: "Tag 1" });
    await catalog.insertOne("tag", { name: "Tag 2" });

    // Paginate products and categories (which are empty)
    const result = await catalog.paginate(["product", "category"], {}, {
      limit: 10,
    });

    expect(result.total).toEqual(0);
    expect(result.data.length).toEqual(0);
  });
});

test("Cross-pagination: backward compatibility - single key still works", async () => {
  await withDatabase("Cross-pagination: backward compatibility - single key still works", async (db) => {
    const catalogModel = defineModel("catalog", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
        },
        category: {
          name: v.string(),
        },
      },
    });

    const catalog = await multiCollection(db, "catalog", catalogModel);

    // Insert products
    for (let i = 1; i <= 5; i++) {
      await catalog.insertOne("product", { name: `Product ${i}`, price: i * 10 });
    }

    // Insert categories
    await catalog.insertOne("category", { name: "Cat 1" });

    // Traditional single-key pagination should still work
    const products = await catalog.paginate("product", {}, { limit: 10 });

    expect(products.total).toEqual(5);
    expect(products.data.length).toEqual(5);

    for (const doc of products.data) {
      expect(doc._type).toEqual("product");
      expect(doc.price).toBeDefined();
    }
  });
});

// ============================================
// naturalIdSort tests
// ============================================

test("Cross-pagination: naturalIdSort sorts by ULID (creation time) across types", async () => {
  await withDatabase("Cross-pagination: naturalIdSort sorts by ULID (creation time) across types", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
        },
        visitor: {
          name: v.string(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert interleaved data with delays to ensure ULID ordering
    await people.insertOne("collaborator", { name: "C1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("visitor", { name: "V1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("collaborator", { name: "C2" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("visitor", { name: "V2" });

    // Without naturalIdSort: sorted by full _id (type prefix first)
    // collaborator:xxx < visitor:xxx alphabetically
    const withoutNatural = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 10,
      sort: { _id: 1 },
    });

    // With naturalIdSort: sorted by ULID part only (chronological order)
    const withNatural = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 10,
      naturalIdSort: true,
    });

    // Both should return all 4 items
    expect(withoutNatural.data.length).toEqual(4);
    expect(withNatural.data.length).toEqual(4);

    // With naturalIdSort, order should be chronological: C1, V1, C2, V2
    expect(withNatural.data[0].name).toEqual("C1");
    expect(withNatural.data[1].name).toEqual("V1");
    expect(withNatural.data[2].name).toEqual("C2");
    expect(withNatural.data[3].name).toEqual("V2");

    // Without naturalIdSort, collaborators come before visitors (alphabetical by type prefix)
    // The exact order depends on type prefix comparison
    const typesWithout = withoutNatural.data.map((d) => d._type);
    // All collaborators should come before all visitors
    const firstVisitorIdx = typesWithout.indexOf("visitor");
    const lastCollabIdx = typesWithout.lastIndexOf("collaborator");
    expect(lastCollabIdx < firstVisitorIdx).toEqual(true);
  });
});

test("Cross-pagination: naturalIdSort with afterId", async () => {
  await withDatabase("Cross-pagination: naturalIdSort with afterId", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
        },
        visitor: {
          name: v.string(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert interleaved data
    await people.insertOne("collaborator", { name: "C1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    const v1Id = await people.insertOne("visitor", { name: "V1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("collaborator", { name: "C2" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("visitor", { name: "V2" });

    // Get page after V1 with naturalIdSort
    const result = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 10,
      naturalIdSort: true,
      afterId: v1Id,
    });

    // Should get C2 and V2 (the items after V1 chronologically)
    expect(result.data.length).toEqual(2);
    expect(result.data[0].name).toEqual("C2");
    expect(result.data[1].name).toEqual("V2");
  });
});

test("Cross-pagination: naturalIdSort descending order", async () => {
  await withDatabase("Cross-pagination: naturalIdSort descending order", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
        },
        visitor: {
          name: v.string(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert interleaved data
    await people.insertOne("collaborator", { name: "C1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("visitor", { name: "V1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("collaborator", { name: "C2" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("visitor", { name: "V2" });

    // With naturalIdSort and descending order
    const result = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 10,
      sort: { _id: -1 },
      naturalIdSort: true,
    });

    // Should be reverse chronological: V2, C2, V1, C1
    expect(result.data.length).toEqual(4);
    expect(result.data[0].name).toEqual("V2");
    expect(result.data[1].name).toEqual("C2");
    expect(result.data[2].name).toEqual("V1");
    expect(result.data[3].name).toEqual("C1");
  });
});

test("Cross-pagination: naturalIdSort pagination across multiple pages", async () => {
  await withDatabase("Cross-pagination: naturalIdSort pagination across multiple pages", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        collaborator: {
          name: v.string(),
        },
        visitor: {
          name: v.string(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert 10 items interleaved
    for (let i = 1; i <= 5; i++) {
      await people.insertOne("collaborator", { name: `C${i}` });
      await new Promise((resolve) => setTimeout(resolve, 10));
      await people.insertOne("visitor", { name: `V${i}` });
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    const allCollected: string[] = [];
    let lastId: string | undefined;

    // Paginate through all items with naturalIdSort
    for (let page = 1; page <= 5; page++) {
      const result = await people.paginate(["collaborator", "visitor"], {}, {
        limit: 3,
        naturalIdSort: true,
        afterId: lastId,
      });

      for (const item of result.data) {
        if (allCollected.includes(item.name)) {
          throw new Error(`DUPLICATE: ${item.name}`);
        }
        allCollected.push(item.name);
      }

      if (result.data.length === 0) break;
      lastId = result.data[result.data.length - 1]._id;
    }

    // Should have all 10 items in chronological order
    expect(allCollected.length).toEqual(10);
    expect(allCollected[0]).toEqual("C1");
    expect(allCollected[1]).toEqual("V1");
    expect(allCollected[2]).toEqual("C2");
    expect(allCollected[3]).toEqual("V2");
    // ... alternating pattern continues
    expect(allCollected[8]).toEqual("C5");
    expect(allCollected[9]).toEqual("V5");
  });
});

test("Cross-pagination: naturalIdSort with three types", async () => {
  await withDatabase("Cross-pagination: naturalIdSort with three types", async (db) => {
    const peopleModel = defineModel("people", {
      schema: {
        admin: {
          name: v.string(),
        },
        collaborator: {
          name: v.string(),
        },
        visitor: {
          name: v.string(),
        },
      },
    });

    const people = await multiCollection(db, "people", peopleModel);

    // Insert in order: A1, C1, V1, A2, C2, V2
    await people.insertOne("admin", { name: "A1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("collaborator", { name: "C1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("visitor", { name: "V1" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("admin", { name: "A2" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("collaborator", { name: "C2" });
    await new Promise((resolve) => setTimeout(resolve, 10));
    await people.insertOne("visitor", { name: "V2" });

    // With naturalIdSort
    const result = await people.paginate(
      ["admin", "collaborator", "visitor"],
      {},
      { limit: 10, naturalIdSort: true }
    );

    // Should be chronological: A1, C1, V1, A2, C2, V2
    expect(result.data.length).toEqual(6);
    expect(result.data[0].name).toEqual("A1");
    expect(result.data[1].name).toEqual("C1");
    expect(result.data[2].name).toEqual("V1");
    expect(result.data[3].name).toEqual("A2");
    expect(result.data[4].name).toEqual("C2");
    expect(result.data[5].name).toEqual("V2");
  });
});

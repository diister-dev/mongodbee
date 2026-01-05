import * as v from "../../src/schema.ts";
import { assertEquals, assertExists } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

Deno.test("Cross-pagination: paginate across two types", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertEquals(result.total, 10);
    assertEquals(result.data.length, 10);

    // Verify we have both types in the result
    const types = new Set(result.data.map((doc) => doc._type));
    assertEquals(types.has("collaborator"), true);
    assertEquals(types.has("visitor"), true);

    // Verify all documents have required common fields
    for (const doc of result.data) {
      assertExists(doc._id);
      assertExists(doc._type);
      assertExists(doc.name);
      assertExists(doc.email);
    }
  });
});

Deno.test("Cross-pagination: paginate with limit smaller than total", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    assertEquals(page1.total, 6);
    assertEquals(page1.data.length, 3);
    assertEquals(page1.position, 0);

    // Verify order by createdAt
    assertEquals(page1.data[0].name, "C1");
    assertEquals(page1.data[1].name, "V1");
    assertEquals(page1.data[2].name, "C2");
  });
});

Deno.test("Cross-pagination: afterId with multiple types", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    assertEquals(page1.data.length, 3);
    const lastId = page1.data[page1.data.length - 1]._id;

    // Get second page using afterId
    const page2 = await people.paginate(["collaborator", "visitor"], {}, {
      limit: 3,
      sort: { createdAt: 1 },
      afterId: lastId,
    });

    assertEquals(page2.data.length, 3);

    // Verify continuation
    assertEquals(page2.data[0].name, "V2");
    assertEquals(page2.data[1].name, "C3");
    assertEquals(page2.data[2].name, "V3");

    // Verify no overlap
    const page1Ids = new Set(page1.data.map((d) => d._id));
    for (const doc of page2.data) {
      assertEquals(page1Ids.has(doc._id), false, "Should not have duplicates");
    }
  });
});

Deno.test("Cross-pagination: afterId can be from any of the allowed types", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertEquals(result.data.length, expectedCount);

    // Verify V1 is not in the result (we're paginating AFTER it)
    const hasV1 = result.data.some((d) => d._id === visitorId);
    assertEquals(hasV1, false, "V1 should not be in the result");
  });
});

Deno.test("Cross-pagination: invalid afterId format throws error", async (t) => {
  await withDatabase(t.name, async (db) => {
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
      assertEquals(true, false, "Should have thrown an error");
    } catch (error) {
      assertEquals(
        (error as Error).message.includes("Invalid afterId format"),
        true,
      );
      assertEquals(
        (error as Error).message.includes("collaborator"),
        true,
      );
      assertEquals(
        (error as Error).message.includes("visitor"),
        true,
      );
    }
  });
});

Deno.test("Cross-pagination: beforeId with multiple types", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    assertEquals(beforePage.data.length, 3);

    // Should return first 3 items in original order
    assertEquals(beforePage.data[0].name, "C1");
    assertEquals(beforePage.data[1].name, "V1");
    assertEquals(beforePage.data[2].name, "C2");
  });
});

Deno.test("Cross-pagination: with filter applied to all types", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertEquals(result.total, 4);
    assertEquals(result.data.length, 4);

    for (const doc of result.data) {
      assertEquals(doc.active, true);
    }
  });
});

Deno.test("Cross-pagination: with custom sort descending", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    assertEquals(result.data.length, 5);

    // Verify descending order
    assertEquals(result.data[0].score, 50); // V1
    assertEquals(result.data[1].score, 40); // C3
    assertEquals(result.data[2].score, 30); // C2
    assertEquals(result.data[3].score, 20); // V2
    assertEquals(result.data[4].score, 10); // C1
  });
});

Deno.test("Cross-pagination: three types simultaneously", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    assertEquals(result.total, 6);
    assertEquals(result.data.length, 6);

    // Verify all types present
    const types = new Set(result.data.map((d) => d._type));
    assertEquals(types.size, 3);
    assertEquals(types.has("employee"), true);
    assertEquals(types.has("contractor"), true);
    assertEquals(types.has("intern"), true);

    // Verify order
    assertEquals(result.data[0].name, "E1");
    assertEquals(result.data[1].name, "C1");
    assertEquals(result.data[2].name, "I1");
    assertEquals(result.data[3].name, "E2");
    assertEquals(result.data[4].name, "C2");
    assertEquals(result.data[5].name, "I2");
  });
});

Deno.test("Cross-pagination: single type array behaves like single key", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertEquals(resultArray.total, resultSingle.total);
    assertEquals(resultArray.data.length, resultSingle.data.length);
    assertEquals(resultArray.total, 5);

    // All should be products
    for (const doc of resultArray.data) {
      assertEquals(doc._type, "product");
    }
  });
});

Deno.test("Cross-pagination: accumulation across 5+ pages with no duplicates", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertEquals(allCollectedIds.length, 30);

    // Verify no duplicates using Set
    const uniqueIds = new Set(allCollectedIds);
    assertEquals(uniqueIds.size, 30, "There are duplicate IDs");
  });
});

Deno.test("Cross-pagination: with prepare, filter, and format", async (t) => {
  await withDatabase(t.name, async (db) => {
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
    assertEquals(result.data.length, 2);

    for (const doc of result.data) {
      assertEquals(doc.bonus, true);
      assertExists(doc.name);
    }
  });
});

Deno.test("Cross-pagination: empty result when no matching types", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    assertEquals(result.total, 0);
    assertEquals(result.data.length, 0);
  });
});

Deno.test("Cross-pagination: backward compatibility - single key still works", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    assertEquals(products.total, 5);
    assertEquals(products.data.length, 5);

    for (const doc of products.data) {
      assertEquals(doc._type, "product");
      assertExists(doc.price);
    }
  });
});

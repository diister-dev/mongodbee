// Coverage for collection.paginate({ sortPipeline }) — sorting on a field
// COMPUTED by aggregation stages (an externalLookup'ed doc's field). Page-2
// cases are the point: a page-1-only test passes even when the cursor is
// wrong; DESC with missing joined docs catches the `$expr` vs query-operator
// cursor semantics.

import { assertEquals, assertRejects } from "@std/assert";
import * as v from "../src/schema.ts";
import { collection } from "../src/collection.ts";
import { withDatabase } from "./+shared.ts";

const OrderSchema = {
  ref: v.string(),
  customerId: v.optional(v.string()),
};

// deno-lint-ignore no-explicit-any
const customerSortPipeline = (s: any) => [
  s.externalLookup("customers", "customerId", "_id", { as: "customerDocs" }),
  s.addFields({ customer: { $first: "$customerDocs" } }),
];

/** 15 orders; `i % 3 !== 0` reference a customer (5 orphans), tiers repeat. */
async function seed(db: Parameters<Parameters<typeof withDatabase>[1]>[0]) {
  const orders = await collection(db, "orders", OrderSchema);
  const customers = db.collection("customers");
  for (let i = 0; i < 15; i++) {
    let customerId: string | undefined;
    if (i % 3 !== 0) {
      customerId = `cust${i}`;
      await customers.insertOne({
        // deno-lint-ignore no-explicit-any
        _id: customerId as any,
        tier: (i % 4) * 10,
      });
    }
    await orders.insertOne({
      ref: `O${String(i).padStart(2, "0")}`,
      customerId,
    });
  }
  return orders;
}

// deno-lint-ignore no-explicit-any
async function groundTruth(db: any, dir: 1 | -1): Promise<string[]> {
  const rows = await db.collection("orders").aggregate([
    {
      $lookup: {
        from: "customers",
        localField: "customerId",
        foreignField: "_id",
        as: "customerDocs",
      },
    },
    { $addFields: { customer: { $first: "$customerDocs" } } },
    // `_id` tie-break follows the field's direction (normalizePaginateSort).
    { $sort: { "customer.tier": dir, _id: dir } },
  ]).toArray();
  return (rows as { _id: string }[]).map((r) => String(r._id));
}

Deno.test("collection sortPipeline DESC with missing joined docs: full coverage, positions consistent", async (t) => {
  await withDatabase(t.name, async (db) => {
    const orders = await seed(db);
    const truth = await groundTruth(db, -1);

    // deno-lint-ignore no-explicit-any
    const all: any[] = [];
    let afterId: string | undefined = undefined;
    let offset = 0;
    for (let guard = 0; guard < 100; guard++) {
      // deno-lint-ignore no-explicit-any
      const page: any = await orders.paginate({}, {
        limit: 4,
        sort: { "customer.tier": -1 },
        afterId,
        sortPipeline: customerSortPipeline,
      });
      assertEquals(page.total, 15);
      assertEquals(page.position, offset);
      all.push(...page.data);
      offset += page.data.length;
      if (page.data.length < 4) break;
      afterId = page.data[page.data.length - 1]._id;
    }

    const walked = all.map((d) => String(d._id));
    assertEquals(walked.length, 15, "orphan orders must not vanish");
    assertEquals(new Set(walked).size, 15);
    assertEquals(walked, truth);
  });
});

Deno.test("collection sortPipeline ASC: walk == raw $sort order; joined field survives", async (t) => {
  await withDatabase(t.name, async (db) => {
    const orders = await seed(db);
    const truth = await groundTruth(db, 1);

    // deno-lint-ignore no-explicit-any
    const all: any[] = [];
    let afterId: string | undefined = undefined;
    for (let guard = 0; guard < 100; guard++) {
      // deno-lint-ignore no-explicit-any
      const page: any = await orders.paginate({}, {
        limit: 4,
        sort: { "customer.tier": 1 },
        afterId,
        sortPipeline: customerSortPipeline,
      });
      all.push(...page.data);
      if (page.data.length < 4) break;
      afterId = page.data[page.data.length - 1]._id;
    }

    assertEquals(all.map((d) => String(d._id)), truth);
    const withCustomer = all.find((d) => d.customerId);
    assertEquals(typeof withCustomer?.customer?.tier, "number");
  });
});

Deno.test("collection sortPipeline: sort key produced by `pipeline` throws, pointing at sortPipeline", async (t) => {
  await withDatabase(t.name, async (db) => {
    const orders = await seed(db);

    await assertRejects(
      () =>
        orders.paginate({}, {
          limit: 4,
          sort: { "customer.tier": 1 },
          pipeline: customerSortPipeline,
        }),
      Error,
      "sortPipeline",
    );
  });
});

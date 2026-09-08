// Verrou — collection.paginate backward (`beforeId`) position must be
// PIPELINE-AWARE, like its own `total` already is.
//
// Regression it guards: with a filtering `pipeline` (a JOIN that drops
// non-survivors), `total` is computed through the pipeline but the backward
// position count used a bare countDocuments — it counted dropped documents
// too, so `position` overshot and `position + data.length` could exceed
// `total` on a backward page: believable numbers, wrong pager. multi and
// scoped already counted through the pipeline; collection was the diverging
// copy.

import { test } from "./+harness.ts";
import { assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import * as v from "../src/schema.ts";

test("paginate (collection): beforeId position is pipeline-aware", async (t) => {
  await withDatabase(t.name, async (db) => {
    const orders = await collection(db, "orders", {
      _id: v.string(),
      ref: v.string(),
      customerId: v.optional(v.string()),
    });
    const customers = db.collection("customers");

    // Deterministic ids → fixed _id-asc order. Only EVEN orders have a
    // customer → 10 JOIN survivors out of 20.
    const oid = (i: number) => `order:o${String(i).padStart(2, "0")}`;
    for (let i = 0; i < 20; i++) {
      const customerId = i % 2 === 0 ? `cust${i}` : undefined;
      if (customerId) {
        await customers.insertOne({ _id: customerId as never, tier: i });
      }
      await orders.insertOne({
        _id: oid(i),
        ref: `R${i}`,
        customerId,
      } as never);
    }

    const pipeline = (stage: any) => [
      stage.externalLookup("customers", "customerId", "_id", "c"),
      { $match: { "c.0": { $exists: true } } },
    ];

    // Survivors in _id order: o00, o02, …, o18. Anchor on o10 (survivor
    // index 5) and page backward by 3: expect survivors 2..4 with
    // position 2 and total 10.
    const back: any = await orders.paginate(
      {},
      {
        limit: 3,
        beforeId: oid(10),
        pipeline,
      },
    );
    assertEquals(back.total, 10, "total counts JOIN survivors only");
    assertEquals(
      back.data.map((d: { _id: string }) => d._id),
      [oid(4), oid(6), oid(8)],
    );
    assertEquals(
      back.position,
      2,
      "backward position must be counted THROUGH the pipeline",
    );
  });
});

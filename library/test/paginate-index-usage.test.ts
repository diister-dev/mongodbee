// Verrou — every index-strategy cursor page must stay a bounded index read
// when a pagination-friendly index exists ({_scope, _type, <field>, _id}).
//
// Regression it guards (measured on MongoDB 8.0, 2k-doc scope): the cursor
// used to be emitted as `{$and: [base, {$or: rungs}]}` with nested rungs.
// Only a TOP-LEVEL $or goes through the subplanner; the $and'ed form lost its
// union bounds on descending walks and fell back to a full-range index scan
// `[MaxKey, MinKey]` with the whole $or as a residual filter — every
// descending page from a value anchor examined about half the scope
// (totalKeysExamined ≈ N/2 for a page of 25). The rooted `$or`-of-`$and`
// composition (see composeCursorQuery) keeps every boundary case at
// totalKeysExamined ≈ limit. This also locks that the `{f: {$ne: null}}`
// null-block rung is planned as two tight index intervals, not a scan.
//
// Proven through the REAL paginate path via the database profiler: each
// `find` the walk issues must ride an IXSCAN with bounded keysExamined.

import { assert } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { newId, refId } from "../src/ids.ts";

const EXPO = "exposition:expoaaaaa01";
const WITHOUT_VALUE = 1_000;
const WITH_VALUE = 1_000;
const LIMIT = 25;
// Measured ≈ limit + a couple of boundary keys; leave margin for duplicate
// groups. The broken plan examined ≈ 1000 keys — an order of magnitude away.
const MAX_KEYS_PER_PAGE = 150;

Deno.test("paginate (scoped): every cursor page is a bounded index read", async () => {
  await withDatabase("paginate-index-usage", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: {
        participant: { name: v.string(), generatedAt: v.optional(v.date()) },
      },
    });
    const view = catalog.scope(EXPO);

    // Seed through the raw collection (2k inserts through the ODM would
    // dominate the test's runtime); the shape matches the storage schema.
    const raw = db.collection("catalog");
    const docs: Record<string, unknown>[] = [];
    for (let i = 0; i < WITHOUT_VALUE + WITH_VALUE; i++) {
      docs.push({
        _id: `participant:${newId()}`,
        _scope: EXPO,
        _type: "participant",
        name: `p-${i}`,
        ...(i >= WITHOUT_VALUE
          ? { generatedAt: new Date(Date.UTC(2026, 0, 1, 0, 0, 0, i)) }
          : {}),
      });
    }
    await raw.insertMany(docs as never[]);

    // The pagination-friendly shape withIndex creates: field + trailing _id
    // so the (field, _id) sort AND the cursor branches resolve in the index.
    await raw.createIndex(
      { _scope: 1, _type: 1, generatedAt: 1, _id: 1 },
      {
        name: "pagination_idx",
        partialFilterExpression: { _type: { $eq: "participant" } },
      },
    );

    await db.command({ profile: 2 });

    // Walk both directions across both boundaries: descending from a value
    // anchor exercises the `$lt ∪ null` branches (the case that used to scan
    // half the scope); ascending from inside the null block exercises the
    // `$ne: null` rung. The `_id` tie-break is left implicit so it follows
    // the field's direction (an explicit `{generatedAt: -1, _id: 1}` matches
    // no index order and is legitimately a blocking sort).
    for (
      const sort of [
        { generatedAt: -1 } as const,
        { generatedAt: 1 } as const,
      ]
    ) {
      let afterId: string | undefined = undefined;
      for (let page = 0; page < 6; page++) {
        const p: { data: { _id: string }[] } = await view.paginate(
          "participant",
          undefined,
          {
            sort,
            limit: LIMIT,
            skipTotal: true,
            ...(afterId ? { afterId } : {}),
            // deno-lint-ignore no-explicit-any
          } as any,
          // deno-lint-ignore no-explicit-any
        ) as any;
        if (p.data.length === 0) break;
        afterId = p.data[p.data.length - 1]._id;
      }
    }

    await db.command({ profile: 0 });

    // Every data `find` of the walk (anchor lookups examine ≤ 1 key and pass
    // the same bound) must be a bounded IXSCAN — no COLLSCAN, no half-scope
    // residual scan.
    const profile = await db
      .collection("system.profile")
      .find({ op: "query", ns: `${db.databaseName}.catalog` })
      .toArray();
    const pageFinds = profile.filter((p) =>
      (p.command as { limit?: number } | undefined)?.limit === LIMIT
    );
    assert(
      pageFinds.length >= 10,
      `expected the walk's page finds in the profile, got ${pageFinds.length}`,
    );
    for (const op of pageFinds) {
      assert(
        String(op.planSummary ?? "").includes("IXSCAN"),
        `page find did not ride an index: ${op.planSummary}`,
      );
      assert(
        (op.keysExamined as number) <= MAX_KEYS_PER_PAGE,
        `page find examined ${op.keysExamined} keys (> ${MAX_KEYS_PER_PAGE}) ` +
          `— the cursor lost its tight index bounds (filter: ${
            JSON.stringify(op.command?.filter).slice(0, 200)
          })`,
      );
    }
  });
});

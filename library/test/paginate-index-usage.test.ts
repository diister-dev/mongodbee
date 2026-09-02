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
import { withIndex } from "../src/indexes.ts";
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
    // The index is the one `withIndex` itself creates —
    // {_scope, _type, generatedAt, _id} partial on _type (see
    // paginationKeySuffix): this locks the whole chain, applier shape
    // included, not just the cursor emission.
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        participant: {
          name: v.string(),
          generatedAt: v.optional(withIndex(v.date())),
        },
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

    // Walk both directions across both boundaries: descending from a value
    // anchor exercises the `$lt ∪ null ∪ $type` branches (the case that used
    // to scan half the scope); ascending from inside the null block exercises
    // the `$ne: null` rung. The `_id` tie-break is left implicit so it
    // follows the field's direction (an explicit `{generatedAt: -1, _id: 1}`
    // matches no index order and is legitimately a blocking sort).
    const runWalks = async () => {
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
    };

    // The page finds of one profiled walk: every one must be a bounded
    // IXSCAN — no COLLSCAN, no half-scope residual scan. Returns the first
    // violation instead of throwing so the caller can retry once.
    const profiledWalkViolation = async (): Promise<string | null> => {
      await db.collection("system.profile").drop().catch(() => {});
      await db.command({ profile: 2 });
      await runWalks();
      await db.command({ profile: 0 });
      const profile = await db
        .collection("system.profile")
        .find({ op: "query", ns: `${db.databaseName}.catalog` })
        .toArray();
      const pageFinds = profile.filter((p) =>
        (p.command as { limit?: number } | undefined)?.limit === LIMIT
      );
      if (pageFinds.length < 10) {
        return `expected the walk's page finds in the profile, got ${pageFinds.length}`;
      }
      for (const op of pageFinds) {
        if (!String(op.planSummary ?? "").includes("IXSCAN")) {
          return `page find did not ride an index: ${op.planSummary}`;
        }
        if ((op.keysExamined as number) > MAX_KEYS_PER_PAGE) {
          return `page find examined ${op.keysExamined} keys ` +
            `(> ${MAX_KEYS_PER_PAGE}) — the cursor lost its tight index ` +
            `bounds (filter: ${
              JSON.stringify(op.command?.filter).slice(0, 200)
            })`;
        }
      }
      return null;
    };

    // A broken EMISSION fails both attempts — the parent commit's shapes
    // lose their bounds deterministically across plan-cache-cleared runs.
    //
    // Why the retry exists — the plan IS ambiguous at the branch level, by
    // measurement: in a no-sort trial the null-block branches
    // ({generatedAt: null} and {generatedAt: null, _id: {$lt}}) tie EXACTLY
    // (score 3.0002, identical keys/docs/nReturned) between this pagination
    // index, the {_scope,_type,_id} base index and _type_1 — the rivals are
    // legitimate indexes and branch-level planning cannot see that only one
    // candidate feeds SORT_MERGE. Nothing in the emission can break that
    // tie. With the sort attached the pagination index wins decisively
    // (3.0002 vs 1.0001 — blocking-sort rivals return 0 during the trial).
    //
    // Measured resolution (2026-08): the whole-$or SUBPLAN chose SORT_MERGE
    // on the pagination index in every observed run — 6 index-catalog
    // creation orders × {6.0.28, 7.0.34, 8.0.28}, 30 rounds of cache
    // poisoning (a same-shape no-sort query cached first), 230 dedicated
    // cold-cache walks (with and without parallel load), and 50+
    // instrumented full-suite runs across the three versions with ZERO
    // first-attempt violations. One first-attempt violation was seen
    // historically (~1 in 5 suite runs during this verrou's development,
    // never since); the tie is real and the planner is server code we do
    // not control, so the second, cache-cleared attempt stays as the
    // defensive gate.
    let violation = await profiledWalkViolation();
    if (violation !== null) {
      await db.command({ planCacheClear: "catalog" });
      violation = await profiledWalkViolation();
    }
    assert(violation === null, violation ?? undefined);
  });
});

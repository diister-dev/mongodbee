/**
 * Lookup sub-pipeline / index visibility regression tests.
 *
 * The stage builders must emit the lookup sub-pipeline's constants (`_type`,
 * `_scope`) as query operators and keep ONLY the correlated join key in
 * `$expr`. The planner does not accept an `$expr` equality as subsuming a
 * `partialFilterExpression`, so an `$expr`-only match makes the partial
 * indexes created by `withIndex` invisible and every lookup degrades to
 * scanning the whole scope (measured: 59 s vs 0.35 s on a 10k-doc scope).
 *
 * These tests lock:
 * - the emitted `$match` shape (constants outside `$expr`),
 * - planner behaviour via `explain()` (partial index used, keys bounded),
 * - cross-scope isolation of every lookup form after the change.
 */
import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import {
  type AggregationStage,
  scopedMultiCollection,
} from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

async function makeCatalog(
  db: Parameters<Parameters<typeof withDatabase>[1]>[0],
) {
  return await scopedMultiCollection(db, "catalog", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: {
      participant: {
        name: v.string(),
      },
      badge: {
        participantId: withIndex(v.string()),
        label: v.string(),
      },
    },
  });
}

/** Extract the `$lookup` stage stats from an executionStats explain. */
function lookupStats(explain: any): {
  indexesUsed: string[];
  collectionScans: number;
  totalKeysExamined: number;
} {
  const stage = explain.stages?.find((s: any) => s.$lookup);
  assert(
    stage,
    `expected a $lookup stage in explain output, got ${JSON.stringify(
      Object.keys(explain),
    )}`,
  );
  return {
    indexesUsed: stage.indexesUsed ?? [],
    collectionScans: stage.collectionScans ?? 0,
    totalKeysExamined: stage.totalKeysExamined ?? 0,
  };
}

test("lookup: sub-pipeline match keeps constants OUT of $expr", async () => {
  await withDatabase("smc-lookup-shape", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);

    let captured: AggregationStage[] = [];
    await expoA.aggregate((stage) => {
      captured = [
        stage.match("participant", {}),
        stage.lookup("badge", "_id", "participantId", "badges"),
        stage.anyLookup("_id", "participantId", "anyBadges"),
      ];
      return captured;
    });

    const subMatch = (stage: AggregationStage): Record<string, unknown> => {
      const lookup = stage.$lookup as { pipeline: AggregationStage[] };
      return lookup.pipeline[0].$match as Record<string, unknown>;
    };
    // _scope (and _type for the typed lookup) must be plain query-operator
    // constraints (index-visible); $expr must contain ONLY the correlated
    // join comparison.
    const typed = subMatch(captured[1]);
    assertEquals(typed._type, "badge");
    assertEquals(typed._scope, EXPO_A);
    assertEquals(typed.$expr, { $eq: ["$participantId", "$$localValue"] });
    const any = subMatch(captured[2]);
    assertEquals(any._scope, EXPO_A);
    assertEquals(any.$expr, { $eq: ["$participantId", "$$localValue"] });
  });
});

test("lookup: planner uses the withIndex-created partial index", async () => {
  await withDatabase("smc-lookup-partial-idx", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    const N = 50;
    for (let i = 0; i < N; i++) {
      const pid = await expoA.insertOne("participant", { name: `p-${i}` });
      await expoA.insertOne("badge", { participantId: pid, label: `b-${i}` });
    }
    // Same volume in another scope: an un-scoped or $expr-only lookup would
    // have to wade through these keys too.
    for (let i = 0; i < N; i++) {
      const pid = await expoB.insertOne("participant", { name: `q-${i}` });
      await expoB.insertOne("badge", { participantId: pid, label: `c-${i}` });
    }

    // Capture the pipeline the library actually emits, then explain it.
    let captured: AggregationStage[] = [];
    await expoA.aggregate((stage) => {
      captured = [
        stage.match("participant", {}),
        stage.lookup("badge", "_id", "participantId", "badges"),
      ];
      return captured;
    });

    const explain = await db
      .collection("catalog")
      .aggregate([
        // Mirrors the scope guard the ScopedView prepends in aggregate().
        { $match: { _scope: EXPO_A } },
        ...captured,
      ])
      .explain("executionStats");

    const stats = lookupStats(explain);
    assert(
      stats.indexesUsed.includes("_scope__type_badge_participantId"),
      `expected the withIndex partial index to be used, got ${JSON.stringify(
        stats.indexesUsed,
      )}`,
    );
    assertEquals(stats.collectionScans, 0);
    // Each of the N sub-plans should examine ~1 key. The $expr-only shape
    // examined every (scope, type) key per input row: ≥ N * 2N = 5000 here.
    assert(
      stats.totalKeysExamined <= N * 4,
      `lookup examined ${stats.totalKeysExamined} keys — partial index not ` +
        `driving the sub-pipeline (expected ≤ ${N * 4})`,
    );
  });
});

test("lookup: every form stays scope-isolated after the mixed-match change", async () => {
  await withDatabase("smc-lookup-isolation", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);

    const pidA = await expoA.insertOne("participant", { name: "Alice" });
    await expoA.insertOne("badge", { participantId: pidA, label: "in-A" });
    // Adversarial: a badge in ANOTHER scope pointing at A's participant id.
    await expoB.insertOne("badge", { participantId: pidA, label: "in-B" });

    // String form
    const viaString = await expoA.aggregate((stage) => [
      stage.match("participant", {}),
      stage.lookup("badge", "_id", "participantId", "badges"),
    ]);
    assertEquals(viaString.length, 1);
    assertEquals(viaString[0].badges.length, 1);
    assertEquals(viaString[0].badges[0].label, "in-A");
    assertEquals(viaString[0].badges[0]._scope, EXPO_A);

    // Options form with a user sub-pipeline (appended AFTER the scope guard)
    const viaOptions = await expoA.aggregate((stage) => [
      stage.match("participant", {}),
      stage.lookup("badge", "_id", "participantId", {
        as: "badges",
        pipeline: (s) => [s.sort({ label: 1 })],
      }),
    ]);
    assertEquals(viaOptions[0].badges.length, 1);
    assertEquals(viaOptions[0].badges[0].label, "in-A");

    // anyLookup (no _type pin — _scope pin must still hold)
    const viaAny = await expoA.aggregate((stage) => [
      stage.match("participant", {}),
      stage.anyLookup("_id", "participantId", "badges"),
    ]);
    assertEquals(viaAny[0].badges.length, 1);
    assertEquals(viaAny[0].badges[0]._scope, EXPO_A);
  });
});

test("lookup: multi-scope view sees its scopes, nothing beyond", async () => {
  await withDatabase("smc-lookup-multiscope", async (db) => {
    const catalog = await makeCatalog(db);
    const expoA = catalog.scope(EXPO_A);
    const expoB = catalog.scope(EXPO_B);
    const expoC = catalog.scope("exposition:expoccccc03");

    const pidA = await expoA.insertOne("participant", { name: "Alice" });
    await expoA.insertOne("badge", { participantId: pidA, label: "in-A" });
    await expoB.insertOne("badge", { participantId: pidA, label: "in-B" });
    await expoC.insertOne("badge", { participantId: pidA, label: "in-C" });

    const rows = await catalog.scopes([EXPO_A, EXPO_B]).aggregate((stage) => [
      stage.match("participant", {}),
      stage.lookup("badge", "_id", "participantId", {
        as: "badges",
        pipeline: (s) => [s.sort({ label: 1 })],
      }),
    ]);
    assertEquals(rows.length, 1);
    // A and B badges are reachable; C must never be.
    assertEquals(
      rows[0].badges.map((b: { label: string }) => b.label),
      ["in-A", "in-B"],
    );
  });
});

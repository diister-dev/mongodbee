/**
 * Lookup sub-pipeline / index visibility regression tests (multiCollection).
 *
 * Both stage builders (aggregate + paginate) must emit the `_type` constant
 * as a query operator and keep ONLY the correlated join key in `$expr`: the
 * planner does not accept an `$expr` equality as subsuming a
 * `partialFilterExpression`, so an `$expr`-only match makes the partial
 * indexes created by `withIndex` invisible and the lookup scans every doc of
 * the type on every input row.
 */
import { assert, assertEquals } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { defineModel } from "../../src/multi-collection-model.ts";
import { withDatabase } from "../+shared.ts";
import * as v from "../../src/schema.ts";
import { withIndex } from "../../src/indexes.ts";

type Stage = Record<string, unknown>;

const badgeModel = defineModel("registry", {
  schema: {
    participant: {
      name: v.string(),
    },
    badge: {
      participantId: withIndex(v.string()),
      label: v.string(),
    },
  },
});

function subMatch(stage: Stage): Record<string, unknown> {
  const lookup = stage.$lookup as { pipeline: Stage[] };
  return lookup.pipeline[0].$match as Record<string, unknown>;
}

Deno.test("lookup: aggregate + paginate builders keep _type OUT of $expr", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registry", badgeModel, {
      schemaManagement: "auto",
    });
    const pid = await mc.insertOne("participant", { name: "Alice" });
    await mc.insertOne("badge", { participantId: pid, label: "b" });

    let viaAggregate: Stage[] = [];
    await mc.aggregate((stage) => {
      viaAggregate = [
        stage.match("participant", {}),
        stage.lookup("badge", "_id", "participantId", "badges"),
        stage.lookup("badge", "_id", "participantId", {
          as: "badgesOpts",
          pipeline: (s) => [s.sort({ label: 1 })],
        }),
      ];
      return viaAggregate;
    });

    let viaPaginate: Stage[] = [];
    await mc.paginate("participant", {}, {
      limit: 10,
      pipeline: (stage) => {
        viaPaginate = [
          stage.lookup("badge", "_id", "participantId", "badges"),
        ];
        return viaPaginate;
      },
    });

    for (
      const match of [
        subMatch(viaAggregate[1]),
        subMatch(viaAggregate[2]),
        subMatch(viaPaginate[0]),
      ]
    ) {
      assertEquals(match._type, "badge");
      assertEquals(match.$expr, { $eq: ["$participantId", "$$localValue"] });
    }
  });
});

Deno.test("lookup: planner uses the withIndex-created partial index", async (t) => {
  await withDatabase(t.name, async (db) => {
    const mc = await multiCollection(db, "registry", badgeModel, {
      schemaManagement: "auto",
    });

    const N = 50;
    for (let i = 0; i < N; i++) {
      const pid = await mc.insertOne("participant", { name: `p-${i}` });
      await mc.insertOne("badge", { participantId: pid, label: `b-${i}` });
    }

    let captured: Stage[] = [];
    await mc.aggregate((stage) => {
      captured = [
        stage.match("participant", {}),
        stage.lookup("badge", "_id", "participantId", "badges"),
      ];
      return captured;
    });

    const explain = await db.collection("registry").aggregate(captured)
      .explain("executionStats");
    // deno-lint-ignore no-explicit-any
    const stage = (explain as any).stages?.find((s: any) => s.$lookup);
    assert(stage, "expected a $lookup stage in explain output");
    assert(
      (stage.indexesUsed ?? []).includes("badge_participantId"),
      `expected the withIndex partial index to be used, got ${
        JSON.stringify(stage.indexesUsed)
      }`,
    );
    assertEquals(stage.collectionScans ?? 0, 0);
    // Each of the N sub-plans should examine ~1 key; the $expr-only shape
    // examined every badge per input row (≥ N * N = 2500 here).
    assert(
      (stage.totalKeysExamined ?? 0) <= N * 4,
      `lookup examined ${stage.totalKeysExamined} keys — partial index not ` +
        `driving the sub-pipeline (expected ≤ ${N * 4})`,
    );
  });
});

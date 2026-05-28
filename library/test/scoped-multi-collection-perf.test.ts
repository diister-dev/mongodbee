/**
 * Performance comparison: N multiCollections (one per scope) vs.
 * 1 scopedMultiCollection holding the same data partitioned by scope.
 *
 * Not a strict benchmark — there is no warmup, no statistical aggregation,
 * just side-by-side timings on a single fresh MongoDB instance. The point
 * is to validate the design hypothesis that scopedMultiCollection scales
 * better than per-scope collections at high cardinality of scopes.
 */
import { withDatabase } from "./+shared.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";

const SCOPES = 30;       // distinct expositions
const DOCS_PER_SCOPE = 50;
const QUERY_ITERATIONS = 5;

function fmt(ms: number) {
  return `${ms.toFixed(1).padStart(7)} ms`;
}

function makeScopeId(i: number): string {
  // 0-padded so ids sort lexically the same way they sort numerically
  return `exposition:e${String(i).padStart(8, "0")}`;
}

async function measure<T>(
  label: string,
  fn: () => Promise<T>,
): Promise<{ label: string; ms: number; result: T }> {
  const t0 = performance.now();
  const result = await fn();
  const ms = performance.now() - t0;
  return { label, ms, result };
}

Deno.test({
  name: "PERF — N multiCollections vs 1 scopedMultiCollection",
  // The perf test is informative ; it must not break CI on flaky runs.
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    await withDatabase("smc-perf-multi", async (db) => {
      // -------- Setup data --------
      const typesShape = {
        artwork: {
          title: withIndex(v.string(), { unique: true }),
          year: v.number(),
        },
        artist: {
          name: v.string(),
        },
      };

      // -------- Scenario A: N multiCollections (one per scope) --------
      const setupA = await measure(
        `setup ${SCOPES} multiCollections`,
        async () => {
          const collections: Array<Awaited<ReturnType<typeof multiCollection>>> = [];
          for (let i = 0; i < SCOPES; i++) {
            collections.push(
              // deno-lint-ignore no-explicit-any
              await multiCollection<any>(db, `legacy_${i}`, typesShape),
            );
          }
          return collections;
        },
      );

      const insertA = await measure(
        `insert ${SCOPES * DOCS_PER_SCOPE} docs into N multiCollections`,
        async () => {
          for (let i = 0; i < SCOPES; i++) {
            const docs = Array.from({ length: DOCS_PER_SCOPE }, (_, j) => ({
              title: `t-${i}-${j}`,
              year: j,
            }));
            await setupA.result[i].insertMany("artwork", docs);
          }
        },
      );

      const queryA = await measure(
        `find docs of one scope × ${QUERY_ITERATIONS} (multiCollections)`,
        async () => {
          for (let it = 0; it < QUERY_ITERATIONS; it++) {
            const i = it % SCOPES;
            await setupA.result[i].find("artwork", {});
          }
        },
      );

      // Count indexes for scenario A
      let totalIndexesA = 0;
      for (let i = 0; i < SCOPES; i++) {
        const idx = await db.collection(`legacy_${i}`).indexes();
        totalIndexesA += idx.length;
      }

      // -------- Scenario B: 1 scopedMultiCollection --------
      const setupB = await measure(
        "setup 1 scopedMultiCollection",
        () =>
          scopedMultiCollection(db, "catalog", {
            scope: refId("exposition"),
            types: typesShape,
          }),
      );

      const insertB = await measure(
        `insert ${SCOPES * DOCS_PER_SCOPE} docs into scopedMultiCollection`,
        async () => {
          for (let i = 0; i < SCOPES; i++) {
            const view = setupB.result.scope(makeScopeId(i));
            const docs = Array.from({ length: DOCS_PER_SCOPE }, (_, j) => ({
              title: `t-${i}-${j}`,
              year: j,
            }));
            await view.insertMany("artwork", docs);
          }
        },
      );

      const queryB = await measure(
        `find docs of one scope × ${QUERY_ITERATIONS} (scopedMultiCollection)`,
        async () => {
          for (let it = 0; it < QUERY_ITERATIONS; it++) {
            const i = it % SCOPES;
            await setupB.result.scope(makeScopeId(i)).find("artwork", {});
          }
        },
      );

      const indexesB = await db.collection("catalog").indexes();

      // -------- Report --------
      const totalDocs = SCOPES * DOCS_PER_SCOPE;
      console.log("");
      console.log("=".repeat(72));
      console.log(`PERF: ${SCOPES} scopes × ${DOCS_PER_SCOPE} docs = ${totalDocs} docs total`);
      console.log("=".repeat(72));
      console.log("                                            multiColl  | scopedColl");
      console.log("-".repeat(72));
      console.log(
        `setup                                       ${fmt(setupA.ms)}  | ${fmt(setupB.ms)}`,
      );
      console.log(
        `insert ${totalDocs} docs                            ${fmt(insertA.ms)}  | ${fmt(insertB.ms)}`,
      );
      console.log(
        `query 1 scope × ${QUERY_ITERATIONS} iterations              ${fmt(queryA.ms)}  | ${fmt(queryB.ms)}`,
      );
      console.log(
        `indexes total                              ${String(totalIndexesA).padStart(8)}   |   ${String(indexesB.length).padStart(6)}`,
      );
      console.log("=".repeat(72));
      console.log(
        `setup ratio:  scopedColl is ${(setupA.ms / setupB.ms).toFixed(1)}× faster`,
      );
      console.log(
        `index count : scopedColl uses ${(totalIndexesA / indexesB.length).toFixed(1)}× fewer indexes`,
      );
      console.log("");
    });
  },
});

/**
 * Hardcore perf comparison: 5000 scopes.
 *
 * SAFETY: this test creates databases / collections under deterministic
 * naming patterns so they can be inspected / dropped manually if the run
 * hangs or crashes.
 *
 *   Database prefix: "@TEST_perf_hardcore_5k@<random8>"
 *   Legacy collections: "perf5k_legacy_<i>" where i in [0, 4999]
 *   Scoped collection:  "perf5k_scoped"
 *
 * If a run is interrupted, find leftover DBs with:
 *   mongosh --eval 'db.adminCommand({listDatabases:1}).databases.filter(d=>d.name.startsWith("@TEST_perf_hardcore_5k@")).forEach(d=>print(d.name))'
 * and drop them:
 *   mongosh --eval 'db.adminCommand({listDatabases:1}).databases.filter(d=>d.name.startsWith("@TEST_perf_hardcore_5k@")).forEach(d=>db.getSiblingDB(d.name).dropDatabase())'
 *
 * The test is OPT-IN via the env var `RUN_PERF_HARDCORE=1` because it
 * takes several minutes and pounds the local MongoDB.
 */
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { closeAllWatchers } from "../src/change-stream.ts";
import { MongoClient } from "../src/mongodb.ts";

const SCOPES = 5000;
const DOCS_PER_SCOPE = 5; // 25_000 docs total
const QUERY_SAMPLE = 20;

const DB_PREFIX = "@TEST_perf_hardcore_5k@";
const LEGACY_COLLECTION_PREFIX = "perf5k_legacy_";
const SCOPED_COLLECTION_NAME = "perf5k_scoped";

function fmt(ms: number) {
  return `${ms.toFixed(0).padStart(8)} ms`;
}

function scopeId(i: number): string {
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
  console.log(`  ✓ ${label}: ${fmt(ms)}`);
  return { label, ms, result };
}

Deno.test({
  name: `PERF HARDCORE — ${SCOPES} scopes × ${DOCS_PER_SCOPE} docs`,
  ignore: !Deno.env.get("RUN_PERF_HARDCORE"),
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const dbName = `${DB_PREFIX}${
      crypto.randomUUID().replace(/-/g, "").substring(0, 8)
    }`;
    console.log("");
    console.log("=".repeat(78));
    console.log(`HARDCORE PERF RUN`);
    console.log(`Database name (for manual cleanup if interrupted):`);
    console.log(`  ${dbName}`);
    console.log(
      `Collections: ${LEGACY_COLLECTION_PREFIX}{0..${SCOPES - 1}} | ${SCOPED_COLLECTION_NAME}`,
    );
    console.log("=".repeat(78));
    console.log("");

    const client = new MongoClient("mongodb://localhost:27017");
    const db = client.db(dbName);

    const typesShape = {
      artwork: {
        title: withIndex(v.string(), { unique: true }),
        year: v.number(),
      },
    };

    type Report = {
      scenario: string;
      setupMs?: number;
      insertMs?: number;
      queryMs?: number;
      indexes?: number;
      failed?: string;
    };
    const reportA: Report = { scenario: `multiCollection ×${SCOPES}` };
    const reportB: Report = { scenario: `scopedMultiCollection ×1` };

    try {
      // -------- Scenario A: N multiCollections --------
      console.log(`>>> Scenario A: ${SCOPES} multiCollections (one per scope)`);

      try {
        const collections: Awaited<ReturnType<typeof multiCollection>>[] = [];
        const t0 = performance.now();

        for (let i = 0; i < SCOPES; i++) {
          collections.push(
            // deno-lint-ignore no-explicit-any
            await multiCollection<any>(
              db,
              `${LEGACY_COLLECTION_PREFIX}${i}`,
              typesShape,
            ),
          );
          if ((i + 1) % 500 === 0) {
            const elapsed = ((performance.now() - t0) / 1000).toFixed(1);
            console.log(
              `    progress: created ${i + 1}/${SCOPES} collections (${elapsed}s elapsed)`,
            );
          }
        }
        reportA.setupMs = performance.now() - t0;
        console.log(`  ✓ setup: ${fmt(reportA.setupMs)}`);

        // Insert
        const tIns = performance.now();
        for (let i = 0; i < SCOPES; i++) {
          const docs = Array.from({ length: DOCS_PER_SCOPE }, (_, j) => ({
            title: `legacy-${i}-${j}`,
            year: j,
          }));
          await collections[i].insertMany("artwork", docs);
          if ((i + 1) % 500 === 0) {
            const elapsed = ((performance.now() - tIns) / 1000).toFixed(1);
            console.log(
              `    progress: inserted ${i + 1}/${SCOPES} scopes (${elapsed}s elapsed)`,
            );
          }
        }
        reportA.insertMs = performance.now() - tIns;
        console.log(
          `  ✓ insert ${SCOPES * DOCS_PER_SCOPE} docs: ${fmt(reportA.insertMs)}`,
        );

        // Query sample
        const tQ = performance.now();
        for (let q = 0; q < QUERY_SAMPLE; q++) {
          const i = Math.floor(Math.random() * SCOPES);
          await collections[i].find("artwork", {});
        }
        reportA.queryMs = performance.now() - tQ;
        console.log(
          `  ✓ random query ×${QUERY_SAMPLE}: ${fmt(reportA.queryMs)}`,
        );

        // Sample index count : list indexes on the first collection × scope
        // count to extrapolate. listing 5000 indexes is itself slow, so we
        // sample one and multiply.
        const sampleIdx = await db.collection(`${LEGACY_COLLECTION_PREFIX}0`)
          .indexes();
        reportA.indexes = sampleIdx.length * SCOPES;
        console.log(
          `  ✓ indexes (sampled × ${SCOPES}): ${reportA.indexes}`,
        );
      } catch (err) {
        reportA.failed = err instanceof Error ? err.message : String(err);
        console.log(`  ✗ scenario A failed: ${reportA.failed}`);
      }

      console.log("");

      // -------- Scenario B: 1 scopedMultiCollection --------
      console.log(`>>> Scenario B: 1 scopedMultiCollection`);

      try {
        const setup = await measure("setup 1 scopedMultiCollection", () =>
          scopedMultiCollection(db, SCOPED_COLLECTION_NAME, {
            scope: refId("exposition"),
            types: typesShape,
          })
        );
        reportB.setupMs = setup.ms;

        const tIns = performance.now();
        for (let i = 0; i < SCOPES; i++) {
          const view = setup.result.scope(scopeId(i));
          const docs = Array.from({ length: DOCS_PER_SCOPE }, (_, j) => ({
            title: `scoped-${i}-${j}`,
            year: j,
          }));
          await view.insertMany("artwork", docs);
          if ((i + 1) % 500 === 0) {
            const elapsed = ((performance.now() - tIns) / 1000).toFixed(1);
            console.log(
              `    progress: inserted ${i + 1}/${SCOPES} scopes (${elapsed}s elapsed)`,
            );
          }
        }
        reportB.insertMs = performance.now() - tIns;
        console.log(
          `  ✓ insert ${SCOPES * DOCS_PER_SCOPE} docs: ${fmt(reportB.insertMs)}`,
        );

        const tQ = performance.now();
        for (let q = 0; q < QUERY_SAMPLE; q++) {
          const i = Math.floor(Math.random() * SCOPES);
          await setup.result.scope(scopeId(i)).find("artwork", {});
        }
        reportB.queryMs = performance.now() - tQ;
        console.log(
          `  ✓ random query ×${QUERY_SAMPLE}: ${fmt(reportB.queryMs)}`,
        );

        const idx = await db.collection(SCOPED_COLLECTION_NAME).indexes();
        reportB.indexes = idx.length;
        console.log(`  ✓ indexes total: ${reportB.indexes}`);
      } catch (err) {
        reportB.failed = err instanceof Error ? err.message : String(err);
        console.log(`  ✗ scenario B failed: ${reportB.failed}`);
      }

      // -------- Report --------
      console.log("");
      console.log("=".repeat(78));
      console.log(`HARDCORE REPORT — ${SCOPES} scopes × ${DOCS_PER_SCOPE} docs`);
      console.log("=".repeat(78));
      console.log(
        `                                  multiColl ×${SCOPES}  |  scopedColl ×1`,
      );
      console.log("-".repeat(78));
      const row = (label: string, a: string, b: string) => {
        console.log(`  ${label.padEnd(28)} ${a.padStart(17)}  |  ${b.padStart(15)}`);
      };
      row(
        "setup",
        reportA.setupMs !== undefined ? fmt(reportA.setupMs) : "FAILED",
        reportB.setupMs !== undefined ? fmt(reportB.setupMs) : "FAILED",
      );
      row(
        `insert ${SCOPES * DOCS_PER_SCOPE} docs`,
        reportA.insertMs !== undefined ? fmt(reportA.insertMs) : "FAILED",
        reportB.insertMs !== undefined ? fmt(reportB.insertMs) : "FAILED",
      );
      row(
        `random query ×${QUERY_SAMPLE}`,
        reportA.queryMs !== undefined ? fmt(reportA.queryMs) : "FAILED",
        reportB.queryMs !== undefined ? fmt(reportB.queryMs) : "FAILED",
      );
      row(
        "indexes total",
        reportA.indexes !== undefined ? String(reportA.indexes) : "FAILED",
        reportB.indexes !== undefined ? String(reportB.indexes) : "FAILED",
      );
      console.log("=".repeat(78));

      if (reportA.failed) {
        console.log(`note: scenario A (multiCollection) failed: ${reportA.failed}`);
      }
      if (reportB.failed) {
        console.log(`note: scenario B (scopedColl) failed: ${reportB.failed}`);
      }
      console.log("");
    } finally {
      console.log(`Cleaning up: dropping ${dbName}`);
      try {
        await closeAllWatchers(db);
      } catch { /* ignore */ }
      await db.dropDatabase();
      await client.close();
    }
  },
});

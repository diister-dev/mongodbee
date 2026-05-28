/**
 * MEGA perf comparison: 5000 scopes × 5000 docs = 25M documents total.
 *
 * SAFETY: deterministic naming so leftover state can be cleaned manually.
 *
 *   Database prefix:           "@TEST_perf_mega_25m@<random8>"
 *   Legacy collections:        "perfmega_legacy_<i>" where i in [0, 4999]
 *   Scoped collection:         "perfmega_scoped"
 *
 * Cleanup leftover DBs (if a run was interrupted):
 *   mongosh --eval 'db.adminCommand({listDatabases:1}).databases.filter(d=>d.name.startsWith("@TEST_perf_mega_25m@")).forEach(d=>db.getSiblingDB(d.name).dropDatabase())'
 *
 * Opt-in via env var `RUN_PERF_MEGA=1`. Expected runtime: 30-90 minutes.
 *
 * What this measures (relative to perf-hardcore, which is 5000 × 5 = 25k docs):
 *   - Insert at realistic per-scope volume (5000 docs/scope)
 *   - listScopes / equivalent collection enumeration
 *   - Search across N scopes (10 and 100) — multi-scope reads
 */
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { closeAllWatchers } from "../src/change-stream.ts";
import { MongoClient } from "../src/mongodb.ts";

const SCOPES = 5000;
const DOCS_PER_SCOPE = 5000;
const TOTAL_DOCS = SCOPES * DOCS_PER_SCOPE;

const QUERY_ITERATIONS = 20;
const SEARCH_N_SMALL = 10;
const SEARCH_N_LARGE = 100;

const DB_PREFIX = "@TEST_perf_mega_25m@";
const LEGACY_COLLECTION_PREFIX = "perfmega_legacy_";
const SCOPED_COLLECTION_NAME = "perfmega_scoped";

// Soft circuit-breaker: abort a scenario whose insert phase exceeds this.
const INSERT_TIMEOUT_MS = 60 * 60 * 1000; // 1 h

function fmt(ms: number) {
  return `${(ms / 1000).toFixed(1).padStart(9)} s`;
}

function scopeId(i: number): string {
  return `exposition:e${String(i).padStart(8, "0")}`;
}

function pickRandomScopes(n: number): number[] {
  const set = new Set<number>();
  while (set.size < n) set.add(Math.floor(Math.random() * SCOPES));
  return [...set];
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

type Report = {
  scenario: string;
  setupMs?: number;
  insertMs?: number;
  insertedDocs?: number;
  queryMs?: number;
  listMs?: number;
  searchSmallMs?: number;
  searchLargeMs?: number;
  indexes?: number;
  failed?: string;
};

Deno.test({
  name: `PERF MEGA — ${SCOPES} scopes × ${DOCS_PER_SCOPE} docs (${TOTAL_DOCS.toLocaleString()} total)`,
  ignore: !Deno.env.get("RUN_PERF_MEGA"),
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const dbName = `${DB_PREFIX}${
      crypto.randomUUID().replace(/-/g, "").substring(0, 8)
    }`;
    const runStart = performance.now();
    console.log("");
    console.log("=".repeat(78));
    console.log(`MEGA PERF RUN`);
    console.log(`Database name (for manual cleanup if interrupted):`);
    console.log(`  ${dbName}`);
    console.log(`Total documents to insert: ${TOTAL_DOCS.toLocaleString()}`);
    console.log("=".repeat(78));
    console.log("");

    const client = new MongoClient("mongodb://localhost:27017");
    const db = client.db(dbName);

    // Keep the schema minimal — no withIndex on user fields. We want to
    // measure scope-discriminator overhead, not unique-index maintenance
    // (a 25M-entry unique index would dominate the timings).
    const typesShape = {
      artwork: {
        title: v.string(),
        year: v.number(),
      },
    };

    const reportA: Report = { scenario: `multiCollection ×${SCOPES}` };
    const reportB: Report = { scenario: `scopedMultiCollection ×1` };

    // Random scope samples reused for read benches (same in both scenarios
    // for fairness).
    const sampleScopes = Array.from(
      { length: QUERY_ITERATIONS },
      () => Math.floor(Math.random() * SCOPES),
    );
    const searchSmallScopes = pickRandomScopes(SEARCH_N_SMALL);
    const searchLargeScopes = pickRandomScopes(SEARCH_N_LARGE);

    try {
      // ============== Scenario A: N multiCollections ==============
      console.log(`>>> Scenario A: ${SCOPES} multiCollections (one per scope)`);

      try {
        // -- setup
        const collections: Awaited<ReturnType<typeof multiCollection>>[] = [];
        const tSetup = performance.now();
        for (let i = 0; i < SCOPES; i++) {
          collections.push(
            // deno-lint-ignore no-explicit-any
            await multiCollection<any>(
              db,
              `${LEGACY_COLLECTION_PREFIX}${i}`,
              typesShape,
            ),
          );
          if ((i + 1) % 1000 === 0) {
            const elapsed = ((performance.now() - tSetup) / 1000).toFixed(1);
            console.log(
              `    setup: ${i + 1}/${SCOPES} collections (${elapsed}s)`,
            );
          }
        }
        reportA.setupMs = performance.now() - tSetup;
        console.log(`  ✓ setup: ${fmt(reportA.setupMs)}`);

        // -- insert (with timeout)
        const tIns = performance.now();
        let abortedAt: number | null = null;
        for (let i = 0; i < SCOPES; i++) {
          const docs = Array.from({ length: DOCS_PER_SCOPE }, (_, j) => ({
            title: `legacy-${i}-${j}`,
            year: j,
          }));
          await collections[i].insertMany("artwork", docs);
          if ((i + 1) % 100 === 0) {
            const elapsed = performance.now() - tIns;
            const rate = ((i + 1) * DOCS_PER_SCOPE) / (elapsed / 1000);
            console.log(
              `    inserted ${(i + 1) * DOCS_PER_SCOPE}/${TOTAL_DOCS} docs ` +
                `(${(elapsed / 1000).toFixed(0)}s elapsed, ${rate.toFixed(0)} docs/s)`,
            );
            if (elapsed > INSERT_TIMEOUT_MS) {
              abortedAt = i + 1;
              console.log(
                `    ⚠ INSERT_TIMEOUT_MS exceeded at scope ${i + 1} — aborting scenario A inserts`,
              );
              break;
            }
          }
        }
        reportA.insertMs = performance.now() - tIns;
        reportA.insertedDocs = (abortedAt ?? SCOPES) * DOCS_PER_SCOPE;
        console.log(
          `  ✓ insert ${reportA.insertedDocs.toLocaleString()} docs: ${fmt(reportA.insertMs)}`,
        );

        // -- random per-scope query
        const tQ = performance.now();
        for (const i of sampleScopes) {
          if (abortedAt !== null && i >= abortedAt) continue;
          await collections[i].find("artwork", {});
        }
        reportA.queryMs = performance.now() - tQ;
        console.log(`  ✓ random query ×${QUERY_ITERATIONS}: ${fmt(reportA.queryMs)}`);

        // -- "list scopes" equivalent: enumerate collection names
        const tL = performance.now();
        const names = await db.listCollections({}, { nameOnly: true }).toArray();
        reportA.listMs = performance.now() - tL;
        const legacyCount = names.filter((c) =>
          c.name.startsWith(LEGACY_COLLECTION_PREFIX)
        ).length;
        console.log(
          `  ✓ listCollections (${legacyCount} legacy): ${fmt(reportA.listMs)}`,
        );

        // -- search across N random scopes
        const tS1 = performance.now();
        const seenSmall: unknown[] = [];
        for (const i of searchSmallScopes) {
          if (abortedAt !== null && i >= abortedAt) continue;
          const docs = await collections[i].find("artwork", { year: 0 });
          seenSmall.push(...docs);
        }
        reportA.searchSmallMs = performance.now() - tS1;
        console.log(
          `  ✓ search in ${SEARCH_N_SMALL} scopes (year=0, ${seenSmall.length} docs): ${fmt(reportA.searchSmallMs)}`,
        );

        const tS2 = performance.now();
        const seenLarge: unknown[] = [];
        for (const i of searchLargeScopes) {
          if (abortedAt !== null && i >= abortedAt) continue;
          const docs = await collections[i].find("artwork", { year: 0 });
          seenLarge.push(...docs);
        }
        reportA.searchLargeMs = performance.now() - tS2;
        console.log(
          `  ✓ search in ${SEARCH_N_LARGE} scopes (year=0, ${seenLarge.length} docs): ${fmt(reportA.searchLargeMs)}`,
        );

        // Sample indexes from the first collection × scope count.
        const sampleIdx = await db.collection(`${LEGACY_COLLECTION_PREFIX}0`)
          .indexes();
        reportA.indexes = sampleIdx.length * SCOPES;
      } catch (err) {
        reportA.failed = err instanceof Error ? err.message : String(err);
        console.log(`  ✗ scenario A failed: ${reportA.failed}`);
      }

      console.log("");

      // ============== Scenario B: 1 scopedMultiCollection ==============
      console.log(`>>> Scenario B: 1 scopedMultiCollection`);

      try {
        const setup = await measure("setup 1 scopedMultiCollection", () =>
          scopedMultiCollection(db, SCOPED_COLLECTION_NAME, {
            scope: refId("exposition"),
            types: typesShape,
            allowUnscoped: true, // needed for the unscoped read bench
          })
        );
        reportB.setupMs = setup.ms;
        const catalog = setup.result;

        const tIns = performance.now();
        let abortedAt: number | null = null;
        for (let i = 0; i < SCOPES; i++) {
          const view = catalog.scope(scopeId(i));
          const docs = Array.from({ length: DOCS_PER_SCOPE }, (_, j) => ({
            title: `scoped-${i}-${j}`,
            year: j,
          }));
          await view.insertMany("artwork", docs);
          if ((i + 1) % 100 === 0) {
            const elapsed = performance.now() - tIns;
            const rate = ((i + 1) * DOCS_PER_SCOPE) / (elapsed / 1000);
            console.log(
              `    inserted ${(i + 1) * DOCS_PER_SCOPE}/${TOTAL_DOCS} docs ` +
                `(${(elapsed / 1000).toFixed(0)}s elapsed, ${rate.toFixed(0)} docs/s)`,
            );
            if (elapsed > INSERT_TIMEOUT_MS) {
              abortedAt = i + 1;
              console.log(
                `    ⚠ INSERT_TIMEOUT_MS exceeded at scope ${i + 1} — aborting scenario B inserts`,
              );
              break;
            }
          }
        }
        reportB.insertMs = performance.now() - tIns;
        reportB.insertedDocs = (abortedAt ?? SCOPES) * DOCS_PER_SCOPE;
        console.log(
          `  ✓ insert ${reportB.insertedDocs.toLocaleString()} docs: ${fmt(reportB.insertMs)}`,
        );

        const tQ = performance.now();
        for (const i of sampleScopes) {
          if (abortedAt !== null && i >= abortedAt) continue;
          await catalog.scope(scopeId(i)).find("artwork", {});
        }
        reportB.queryMs = performance.now() - tQ;
        console.log(`  ✓ random query ×${QUERY_ITERATIONS}: ${fmt(reportB.queryMs)}`);

        // listScopes — native API
        const tL = performance.now();
        const scopes = await catalog.listScopes();
        reportB.listMs = performance.now() - tL;
        console.log(
          `  ✓ listScopes (${scopes.length} scopes): ${fmt(reportB.listMs)}`,
        );

        // search in N scopes via .scopes([...]).find()
        const tS1 = performance.now();
        const smallIds = searchSmallScopes
          .filter((i) => abortedAt === null || i < abortedAt)
          .map(scopeId);
        const smallDocs = await catalog.scopes(smallIds).find("artwork", { year: 0 });
        reportB.searchSmallMs = performance.now() - tS1;
        console.log(
          `  ✓ search in ${SEARCH_N_SMALL} scopes via .scopes() (${smallDocs.length} docs): ${fmt(reportB.searchSmallMs)}`,
        );

        const tS2 = performance.now();
        const largeIds = searchLargeScopes
          .filter((i) => abortedAt === null || i < abortedAt)
          .map(scopeId);
        const largeDocs = await catalog.scopes(largeIds).find("artwork", { year: 0 });
        reportB.searchLargeMs = performance.now() - tS2;
        console.log(
          `  ✓ search in ${SEARCH_N_LARGE} scopes via .scopes() (${largeDocs.length} docs): ${fmt(reportB.searchLargeMs)}`,
        );

        const idx = await db.collection(SCOPED_COLLECTION_NAME).indexes();
        reportB.indexes = idx.length;
      } catch (err) {
        reportB.failed = err instanceof Error ? err.message : String(err);
        console.log(`  ✗ scenario B failed: ${reportB.failed}`);
      }

      // ============== Report ==============
      const totalElapsed = ((performance.now() - runStart) / 1000 / 60).toFixed(1);
      console.log("");
      console.log("=".repeat(78));
      console.log(
        `MEGA REPORT — ${SCOPES} scopes × ${DOCS_PER_SCOPE} docs (run took ${totalElapsed} min)`,
      );
      console.log("=".repeat(78));
      console.log(`                                multiColl ×${SCOPES}  |   scopedColl ×1`);
      console.log("-".repeat(78));
      const row = (label: string, a: string, b: string) => {
        console.log(`  ${label.padEnd(28)} ${a.padStart(17)}  |  ${b.padStart(17)}`);
      };
      const F = (v: number | undefined) => v === undefined ? "FAILED/SKIP" : fmt(v);
      row("setup", F(reportA.setupMs), F(reportB.setupMs));
      row(`insert (docs reached)`, F(reportA.insertMs), F(reportB.insertMs));
      row(`  → docs inserted`,
        (reportA.insertedDocs ?? 0).toLocaleString(),
        (reportB.insertedDocs ?? 0).toLocaleString());
      row(`random query ×${QUERY_ITERATIONS}`, F(reportA.queryMs), F(reportB.queryMs));
      row(`listScopes / listColls`, F(reportA.listMs), F(reportB.listMs));
      row(`search in ${SEARCH_N_SMALL} scopes`, F(reportA.searchSmallMs), F(reportB.searchSmallMs));
      row(`search in ${SEARCH_N_LARGE} scopes`, F(reportA.searchLargeMs), F(reportB.searchLargeMs));
      row("indexes total",
        reportA.indexes !== undefined ? String(reportA.indexes) : "FAILED",
        reportB.indexes !== undefined ? String(reportB.indexes) : "FAILED");
      console.log("=".repeat(78));

      if (reportA.failed) console.log(`note: scenario A failed: ${reportA.failed}`);
      if (reportB.failed) console.log(`note: scenario B failed: ${reportB.failed}`);
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

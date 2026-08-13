/**
 * PERF baseline for `flow_to_scope` — the consolidation primitive behind the
 * diivento migration (N legacy collections → ONE scopedMultiCollection).
 *
 * This is the "measure" half of the measure → optimize → re-measure loop. It
 * drives a real `flow_to_scope` over a large single source collection and uses
 * the applier's `onProgress` hook as the throughput probe (docs/s) — no
 * external profiler. Re-run it after batching `flow_to_scope` to quantify the
 * gain.
 *
 * SAFETY: deterministic DB naming so leftover state can be cleaned manually.
 *   Database prefix:  "@TEST_perf_migration@<random8>"
 *
 * Cleanup leftover DBs (if a run was interrupted):
 *   mongosh --eval 'db.adminCommand({listDatabases:1}).databases.filter(d=>d.name.startsWith("@TEST_perf_migration@")).forEach(d=>db.getSiblingDB(d.name).dropDatabase())'
 *
 * Opt-in via env var `RUN_PERF_MIGRATION=1`. Scale with `DOCS` / `SCOPES`:
 *   RUN_PERF_MIGRATION=1 DOCS=50000 SCOPES=500 deno test -A test/migration/flow-to-scope-perf.test.ts
 */
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import {
  createMongodbApplier,
  type MigrationProgressEvent,
} from "../../src/migration/appliers/mongodb.ts";
import { MongoClient } from "../../src/mongodb.ts";
import * as v from "../../src/schema.ts";
import { TEST_URI } from "../+shared.ts";

const DOCS = Number(Deno.env.get("DOCS") ?? "20000");
const SCOPES = Number(Deno.env.get("SCOPES") ?? "200");
const BATCH_SIZE = Number(Deno.env.get("BATCH_SIZE") ?? "1000");

const DB_PREFIX = "@TEST_perf_migration@";
const SRC = "bench_src";
const DST = "bench_scoped";

Deno.test({
  name:
    `PERF MIGRATION — flow_to_scope ${DOCS.toLocaleString()} docs → ${SCOPES} scopes`,
  ignore: !Deno.env.get("RUN_PERF_MIGRATION"),
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const dbName = `${DB_PREFIX}${
      crypto.randomUUID().replace(/-/g, "").substring(0, 8)
    }`;
    const client = new MongoClient(TEST_URI);
    const db = client.db(dbName);

    console.log("");
    console.log("=".repeat(72));
    console.log(`MIGRATION PERF — flow_to_scope`);
    console.log(
      `  docs=${DOCS.toLocaleString()} scopes=${SCOPES} batch=${BATCH_SIZE}`,
    );
    console.log(`  db (manual cleanup if interrupted): ${dbName}`);
    console.log("=".repeat(72));

    try {
      // -- seed the source collection (bulk, fast) ------------------------
      const tSeed = performance.now();
      const src = db.collection(SRC);
      for (let i = 0; i < DOCS; i += BATCH_SIZE) {
        const batch = Array.from(
          { length: Math.min(BATCH_SIZE, DOCS - i) },
          (_, j) => {
            const n = i + j;
            return {
              _id: `art:${String(n).padStart(9, "0")}`,
              title: `artwork-${n}`,
              year: n % 2000,
              bucket: n % SCOPES, // → derives the scope
            };
          },
        );
        // deno-lint-ignore no-explicit-any
        await src.insertMany(batch as any);
      }
      const seedMs = performance.now() - tSeed;
      console.log(
        `  seeded ${DOCS.toLocaleString()} docs in ${
          (seedMs / 1000).toFixed(1)
        }s`,
      );

      // -- build the consolidation migration ------------------------------
      const S = { collections: { [SRC]: { _id: v.string() } } };
      const m = migrationDefinition("001", "perf-consolidate", {
        parent: null,
        schemas: S,
        migrate: (b) =>
          b.flowToScope({
            from: { kind: "collection", name: SRC },
            into: { collection: DST },
            toType: () => "artwork",
            scope: (d) => `exposition:e${(d as { bucket: number }).bucket}`,
            map: (d) => {
              const { bucket: _bucket, ...rest } = d as Record<string, unknown>;
              return rest;
            },
            source: "consume",
          }).compile(),
      });
      const ops = m.migrate(migrationBuilder({ schemas: S })).operations;

      // -- run it, using onProgress as the throughput probe ---------------
      let lastLogged = 0;
      const onProgress = (e: MigrationProgressEvent) => {
        if (e.phase === "progress" && e.processed - lastLogged >= 5000) {
          lastLogged = e.processed;
          const rate = e.processed / (e.elapsedMs / 1000 || 1);
          console.log(
            `    ${e.operationType}: ${e.processed.toLocaleString()} docs ` +
              `(${rate.toFixed(0)} docs/s)`,
          );
        }
      };

      const tRun = performance.now();
      await createMongodbApplier(db, m, {
        currentMigrationId: m.id,
        batchSize: BATCH_SIZE,
        onProgress,
      }).applyMigration(ops, "up");
      const runMs = performance.now() - tRun;

      // -- verify + report ------------------------------------------------
      const remaining = await src.countDocuments();
      const moved = await db.collection(DST).countDocuments();
      const rate = DOCS / (runMs / 1000);

      console.log("-".repeat(72));
      console.log(
        `  flow_to_scope: ${DOCS.toLocaleString()} docs in ` +
          `${(runMs / 1000).toFixed(1)}s → ${rate.toFixed(0)} docs/s`,
      );
      console.log(`  source remaining: ${remaining} (expect 0, consumed)`);
      console.log(`  scoped total:     ${moved.toLocaleString()}`);
      console.log("=".repeat(72));
      console.log("");

      if (remaining !== 0) {
        throw new Error(`source not fully consumed: ${remaining}`);
      }
      if (moved !== DOCS) {
        throw new Error(`expected ${DOCS} moved, got ${moved}`);
      }
    } finally {
      console.log(`Cleaning up: dropping ${dbName}`);
      await db.dropDatabase();
      await client.close();
    }
  },
});

/**
 * PERF for the scoped READ path: how much does the per-document `v.safeParse`
 * in `ScopedView.find` cost, relative to the raw driver read (the floor)?
 *
 * This is the runtime hot path that grows with DB size. The bench reads the
 * same scope repeatedly:
 *   A. validated   — `view.find(type, {})` (current default)
 *   B. raw floor   — `collection.find({_scope,_type}).toArray()` (no parse)
 *   C. opt-out     — `view.find(type, {}, { validate: false })` (this PR)
 *
 * Opt-in via env var `RUN_PERF_READ=1`. Scale with `DOCS` / `ITERS`.
 */
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { MongoClient } from "../src/mongodb.ts";
import { TEST_URI } from "./+shared.ts";

const DOCS = Number(Deno.env.get("DOCS") ?? "50000");
const ITERS = Number(Deno.env.get("ITERS") ?? "10");
const DB_PREFIX = "@TEST_perf_read@";
const SCOPE = "exposition:read-bench";

function fmt(ms: number, docs: number) {
  const rate = docs / (ms / 1000);
  return `${(ms / 1000).toFixed(2)}s → ${rate.toFixed(0)} docs/s`;
}

Deno.test({
  name: `PERF READ — scoped find over ${DOCS} docs × ${ITERS} iters`,
  ignore: !Deno.env.get("RUN_PERF_READ"),
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
    console.log(`READ PERF — ${DOCS} docs, ${ITERS} iterations`);
    console.log(`  db (manual cleanup if interrupted): ${dbName}`);
    console.log("=".repeat(72));

    try {
      // Moderately rich schema so per-doc parse cost is realistic.
      const catalog = await scopedMultiCollection(db, "readbench", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          artwork: {
            title: v.string(),
            year: v.number(),
            description: v.string(),
            tags: v.array(v.string()),
            meta: v.object({ author: v.string(), rating: v.number() }),
          },
        },
        allowUnscoped: true,
      });
      const view = catalog.scope(SCOPE);

      const docs = Array.from({ length: DOCS }, (_, i) => ({
        title: `artwork-${i}`,
        year: i % 2000,
        description: `A description for artwork number ${i} with some text.`,
        tags: ["a", "b", `tag-${i % 50}`],
        meta: { author: `author-${i % 100}`, rating: i % 5 },
      }));
      const tSeed = performance.now();
      for (let i = 0; i < DOCS; i += 1000) {
        await view.insertMany("artwork", docs.slice(i, i + 1000));
      }
      console.log(
        `  seeded ${DOCS} docs in ${
          ((performance.now() - tSeed) / 1000).toFixed(1)
        }s`,
      );

      const rawColl = db.collection("readbench");

      // Warm up the cache so we measure CPU, not cold reads.
      await view.find("artwork", {});

      // A — validated (current default)
      const tA = performance.now();
      let lastLenA = 0;
      for (let k = 0; k < ITERS; k++) {
        lastLenA = (await view.find("artwork", {})).length;
      }
      const msA = performance.now() - tA;

      // B — raw driver read (no parse) — the floor
      const tB = performance.now();
      let lastLenB = 0;
      for (let k = 0; k < ITERS; k++) {
        lastLenB = (await rawColl.find(
          { _scope: SCOPE, _type: "artwork" } as Record<string, unknown>,
        ).toArray()).length;
      }
      const msB = performance.now() - tB;

      // C — validate:false opt-out (this PR)
      const tC = performance.now();
      let lastLenC = 0;
      for (let k = 0; k < ITERS; k++) {
        lastLenC =
          // deno-lint-ignore no-explicit-any
          (await view.find("artwork", {}, { validate: false } as any)).length;
      }
      const msC = performance.now() - tC;

      // D — findProject to 2 of 6 fields (the typed API; drops the heavy
      // description / tags / meta) — the real lever on the deserialization floor
      const tD = performance.now();
      let lastLenD = 0;
      for (let k = 0; k < ITERS; k++) {
        lastLenD =
          (await view.findProject("artwork", ["title", "year"])).length;
      }
      const msD = performance.now() - tD;

      const totalA = lastLenA * ITERS;
      console.log("-".repeat(72));
      console.log(
        `  A validated        : ${fmt(msA, totalA)}  (${lastLenA} docs/read)`,
      );
      console.log(
        `  B raw floor        : ${
          fmt(msB, lastLenB * ITERS)
        }  (${lastLenB} docs/read)`,
      );
      console.log(
        `  C validate:false   : ${
          fmt(msC, lastLenC * ITERS)
        }  (${lastLenC} docs/read)`,
      );
      console.log(
        `  D projected(2/6)   : ${
          fmt(msD, lastLenD * ITERS)
        }  (${lastLenD} docs/read)`,
      );
      console.log(`  validation overhead (A vs B): ${(msA / msB).toFixed(2)}×`);
      console.log(`  projection speedup  (A vs D): ${(msA / msD).toFixed(2)}×`);
      console.log("=".repeat(72));
      console.log("");
    } finally {
      console.log(`Cleaning up: dropping ${dbName}`);
      await db.dropDatabase();
      await client.close();
    }
  },
});

/**
 * PERF baseline for the per-INSTANCE overhead of a multi-model migration.
 *
 * `transform_multimodel_instances_type` (and the unconditional
 * `recordMigrationOnAllMultiModelInstances` tail) walk every instance of a
 * model: one `shouldInstanceReceiveMigrationFromChain` read + one
 * `recordMultiCollectionMigration` write PER instance, sequentially. This bench
 * measures that wall-clock so we can quantify the gain from bounded-parallel
 * execution. Throughput is reported via the applier's `onProgress` hook.
 *
 * Instances are named `thing:<i>` so `discoverMultiCollectionInstances` uses
 * its name-convention fast path (one listCollections, no per-collection read) —
 * keeping the measurement focused on the shouldReceive/record N+1.
 *
 * SAFETY: deterministic DB naming.
 *   Database prefix:  "@TEST_perf_instances@<random8>"
 * Cleanup leftover DBs (if interrupted):
 *   mongosh --eval 'db.adminCommand({listDatabases:1}).databases.filter(d=>d.name.startsWith("@TEST_perf_instances@")).forEach(d=>db.getSiblingDB(d.name).dropDatabase())'
 *
 * Opt-in via env var `RUN_PERF_INSTANCES=1`. Scale with `INSTANCES`.
 */
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createMultiCollectionInfo } from "../../src/migration/multicollection-registry.ts";
import { MongoClient } from "../../src/mongodb.ts";
import * as v from "../../src/schema.ts";
import { TEST_URI } from "../+shared.ts";

const INSTANCES = Number(Deno.env.get("INSTANCES") ?? "1000");
const DB_PREFIX = "@TEST_perf_instances@";

Deno.test({
  name: `PERF INSTANCES — multi-model migration over ${INSTANCES} instances`,
  ignore: !Deno.env.get("RUN_PERF_INSTANCES"),
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
    console.log(`INSTANCES PERF — ${INSTANCES} multi-model instances`);
    console.log(`  db (manual cleanup if interrupted): ${dbName}`);
    console.log("=".repeat(72));

    try {
      // -- setup: register N instances of model "thing", 2 item docs each ---
      const tSetup = performance.now();
      const CHUNK = 50;
      for (let i = 0; i < INSTANCES; i += CHUNK) {
        await Promise.all(
          Array.from(
            { length: Math.min(CHUNK, INSTANCES - i) },
            async (_, j) => {
              const coll = `thing:${i + j}`;
              await createMultiCollectionInfo(db, coll, "thing", "000");
              // deno-lint-ignore no-explicit-any
              await db.collection(coll).insertMany([
                { _id: "item:0", _type: "item", val: 0 },
                { _id: "item:1", _type: "item", val: 1 },
              ] as any);
            },
          ),
        );
      }
      console.log(
        `  setup ${INSTANCES} instances in ${
          ((performance.now() - tSetup) / 1000).toFixed(1)
        }s`,
      );

      // -- migration: bump every instance's `item.val` ----------------------
      const S = {
        multiModels: { thing: { item: { _id: v.string(), val: v.number() } } },
      };
      const parent = migrationDefinition("000", "base", {
        parent: null,
        schemas: S,
        migrate: (b) => b.compile(),
      });
      const m = migrationDefinition("001", "bump", {
        parent,
        schemas: S,
        migrate: (b) => {
          b.multiModelInstances("thing").type("item").transform({
            up: (d) => ({ ...d, val: ((d.val as number) ?? 0) + 1 }),
            down: (d) => ({ ...d, val: ((d.val as number) ?? 0) - 1 }),
          });
          return b.compile();
        },
      });
      const ops = m.migrate(
        migrationBuilder({ schemas: S, parentSchemas: parent.schemas }),
      ).operations;

      // -- measure ----------------------------------------------------------
      const tRun = performance.now();
      await createMongodbApplier(db, m, {
        currentMigrationId: m.id,
      }).applyMigration(ops, "up");
      const runMs = performance.now() - tRun;

      const rate = INSTANCES / (runMs / 1000);
      console.log("-".repeat(72));
      console.log(
        `  migration over ${INSTANCES} instances: ${
          (runMs / 1000).toFixed(2)
        }s → ${rate.toFixed(0)} instances/s`,
      );
      console.log("=".repeat(72));
      console.log("");
    } finally {
      console.log(`Cleaning up: dropping ${dbName}`);
      await db.dropDatabase();
      await client.close();
    }
  },
});

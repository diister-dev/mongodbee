import { spawn } from "node:child_process";
import process from "node:process";
import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import { TEST_URI, withDatabase } from "./+shared.ts";
import { checkComputed } from "../src/computed-apply.ts";
import {
  COMPUTED_PENDING_COLLECTION,
  drainComputedPending,
  pendingComputed,
} from "../src/computed-marks.ts";
import { CLUSTER_SCOPES, openCluster } from "./fixtures/computed-cluster.ts";

const NODE_SCRIPT = new URL(
  "./fixtures/computed-cluster-node.ts",
  import.meta.url,
).pathname;

type Node = {
  name: string;
  kill: () => void;
  exited: Promise<number>;
  output: Promise<string>;
};

const RUN_ARGS = "Deno" in globalThis ? ["run", "-A"] : [];

function spawnNode(
  name: string,
  env: Record<string, string>,
  onLine?: (line: string) => void,
): Node {
  const child = spawn(process.execPath, [...RUN_ARGS, NODE_SCRIPT], {
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  let partial = "";
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    stdout += chunk;
    const lines = (partial + chunk).split("\n");
    partial = lines.pop() ?? "";
    for (const line of lines) if (line) onLine?.(line);
  });
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  const exited = new Promise<number>((resolve) =>
    child.on("close", (code, signal) =>
      resolve(code ?? (signal === null ? 1 : 128)),
    ),
  );
  return {
    name,
    kill: () => child.kill("SIGKILL"),
    exited,
    output: exited.then(() => stdout + stderr),
  };
}

test({
  name: "computed multi-node: separate processes writing and draining, one drainer killed mid-drain, converge to the truth",
  timeout: 90_000,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const { topology, expositions } = await openCluster(db, 3);
      for (const scope of CLUSTER_SCOPES) {
        const view = expositions.scope(scope);
        await view.insertMany("expo_organization", [
          { name: "O1", status: "validated" },
          { name: "O2", status: "pending" },
          { name: "O3", status: "validated" },
        ]);
        await view.insertMany(
          "participant",
          Array.from({ length: 6 }, (_, index) => ({ name: `P${index}` })),
        );
      }

      const base = {
        CLUSTER_URI: TEST_URI,
        CLUSTER_DB: db.databaseName,
        CLUSTER_LEASE_MS: "1500",
      };
      let killed = false;
      let victim: Node | undefined;
      victim = spawnNode(
        "drainer-victim",
        { ...base, CLUSTER_ROLE: "drainer", CLUSTER_DURATION_MS: "12000" },
        (line) => {
          if (line === "IN_DRAIN" && !killed && victim) {
            killed = true;
            victim.kill();
          }
        },
      );
      const drainers = [1, 2].map((index) =>
        spawnNode(`drainer-${index}`, {
          ...base,
          CLUSTER_ROLE: "drainer",
          CLUSTER_DURATION_MS: "6000",
        }),
      );
      const writers = [1, 2, 3, 4].map((index) =>
        spawnNode(`writer-${index}`, {
          ...base,
          CLUSTER_ROLE: "writer",
          CLUSTER_SEED: String(index * 101),
          CLUSTER_ITERATIONS: "60",
        }),
      );

      const writerResults = await Promise.all(
        writers.map(async (node) => ({
          name: node.name,
          code: await node.exited,
          output: await node.output,
        })),
      );
      for (const result of writerResults)
        assertEquals(
          result.code,
          0,
          `${result.name} failed:\n${result.output}`,
        );
      await Promise.all(drainers.map((node) => node.exited));
      const victimCode = await victim.exited;

      assert(
        killed,
        "the victim drainer was killed while it held a claimed mark",
      );
      assert(victimCode !== 0, "the victim died by signal, not cleanly");

      const deadline = Date.now() + 15_000;
      let remaining = (await pendingComputed(db)).count;
      while (remaining > 0 && Date.now() < deadline) {
        remaining = (
          await drainComputedPending(db, { topology, leaseMs: 1500 })
        ).remaining;
        if (remaining > 0)
          await new Promise((resolve) => setTimeout(resolve, 200));
      }
      assertEquals(
        remaining,
        0,
        "every mark, including the one the dead drainer held, is eventually drained once its lease expires",
      );
      assertEquals(
        await db.collection(COMPUTED_PENDING_COLLECTION).countDocuments({}),
        0,
      );
      assertEquals((await checkComputed(db, topology)).drifts, []);
      const memberships = await db
        .collection("+expositions")
        .countDocuments({ _type: "org_membership" });
      assert(
        memberships > 0,
        `the writers produced a real workload (${memberships} memberships)`,
      );
    });
  },
});

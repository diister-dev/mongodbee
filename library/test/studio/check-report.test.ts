import { test } from "../+harness.ts";
import { assert, assertEquals, assertRejects } from "../+assert.ts";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { installLibrary, runCli } from "../migration/cli/shared.ts";
import { runCheck } from "../../src/migration/cli/commands/check.ts";
import {
  type CheckEvent,
  runCheckInWorker,
  runStudioCheck,
} from "../../src/studio/api/check.ts";
import { getMockGenerationConfig } from "../../src/migration/validators/mock/config.ts";
import {
  type CheckFixtureVariant,
  normalizeCliOutput,
  writeCheckFixture,
} from "./check-fixture.ts";

async function withFixture(
  variant: CheckFixtureVariant,
  work: (dir: string) => Promise<void>,
): Promise<void> {
  const dir = await mkdtemp(path.join(tmpdir(), "mongodbee_check_"));
  try {
    await installLibrary(dir);
    await writeCheckFixture(dir, variant);
    await work(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

const GOLDEN: Array<[CheckFixtureVariant, string[], string]> = [
  ["valid", ["check", "--mode", "quick"], "check-valid"],
  [
    "failing-simulation",
    ["check", "--mode", "quick"],
    "check-failing-simulation",
  ],
  ["schema-drift", ["check", "--mode", "quick"], "check-schema-drift"],
  ["valid", ["check", "--mode", "normal", "--last", "1"], "check-valid-last1"],
];

for (const [variant, args, golden] of GOLDEN) {
  test({
    name: `check CLI output is byte-identical to the pre-refactor capture: ${golden}`,
    timeout: 60_000,
    fn: async () => {
      await withFixture(variant, async (dir) => {
        const result = await runCli(dir, args);
        const output = `exit ${result.code}\n--- stdout\n${normalizeCliOutput(
          result.stdout,
          dir,
        )}--- stderr\n${normalizeCliOutput(result.stderr, dir)}`;
        const expected = await readFile(
          new URL(`./fixtures/${golden}.golden.txt`, import.meta.url),
          "utf8",
        );
        assertEquals(output, expected);
      });
    },
  });
}

test({
  name: "runCheck returns a structured report for a valid chain",
  timeout: 60_000,
  fn: async () => {
    await withFixture("valid", async (dir) => {
      const lines: string[] = [];
      const report = await runCheck(
        { cwd: dir, mode: "quick" },
        { log: (line) => lines.push(line), write: () => {}, tty: false },
      );
      assertEquals(report.stage, "simulation");
      assertEquals(report.valid, true);
      assertEquals(report.failure, undefined);
      assertEquals(report.schema?.valid, true);
      assertEquals(report.migrationCount, 2);
      assertEquals(
        report.migrations.map((m) => [
          m.name,
          m.valid,
          m.operationCount,
          m.reversible,
        ]),
        [
          ["create users", true, 2, true],
          ["add role", true, 1, true],
        ],
      );
      assert(lines.some((line) => line.includes("Found 2 migration(s)")));
    });
  },
});

test({
  name: "runCheck reports a failing simulation with its error and does not throw",
  timeout: 60_000,
  fn: async () => {
    await withFixture("failing-simulation", async (dir) => {
      const report = await runCheck(
        { cwd: dir, mode: "quick" },
        { log: () => {}, write: () => {}, tty: false },
      );
      assertEquals(report.valid, false);
      assertEquals(report.failure?.message, "Migration validation failed");
      const failing = report.migrations.find((m) => !m.valid);
      assertEquals(failing?.name, "add role");
      assert(
        failing?.errors.some((error) => error.includes('received "owner"')),
      );
    });
  },
});

test({
  name: "runCheck stops at the schema stage when schemas.ts drifted",
  timeout: 60_000,
  fn: async () => {
    await withFixture("schema-drift", async (dir) => {
      const report = await runCheck(
        { cwd: dir, mode: "quick" },
        { log: () => {}, write: () => {}, tty: false },
      );
      assertEquals(report.stage, "schema");
      assertEquals(report.schema?.valid, false);
      assertEquals(report.migrations, []);
      assertEquals(report.failure?.message, "Schema validation failed");
    });
  },
});

test({
  name: "studio check streams schema, per-migration and done events in order",
  timeout: 60_000,
  fn: async () => {
    await withFixture("failing-simulation", async (dir) => {
      const events: CheckEvent[] = [];
      const report = await runStudioCheck(
        { project: { cwd: dir } } as never,
        { mode: "quick" },
        (event) => events.push(event),
      );
      assertEquals(
        events.map((event) => event.type),
        [
          "start",
          "schema",
          "migration-start",
          "migration-result",
          "migration-start",
          "migration-result",
          "done",
        ],
      );
      const start = events[0] as Extract<CheckEvent, { type: "start" }>;
      assertEquals(start.inMemory, true);
      const second = events[5] as Extract<
        CheckEvent,
        { type: "migration-result" }
      >;
      assertEquals(second.valid, false);
      assertEquals(report?.valid, false);
      assertEquals(report?.failure, "Migration validation failed");
    });
  },
});

test({
  name: "studio check stops between migrations when its signal aborts",
  timeout: 60_000,
  fn: async () => {
    await withFixture("failing-simulation", async (dir) => {
      const events: CheckEvent[] = [];
      const abort = new AbortController();
      const report = await runStudioCheck(
        { project: { cwd: dir } } as never,
        { mode: "quick", signal: abort.signal },
        (event) => {
          events.push(event);
          if (event.type === "migration-result") abort.abort();
        },
      );
      assertEquals(report, undefined);
      assertEquals(
        events.map((event) => event.type),
        ["start", "schema", "migration-start", "migration-result"],
      );
    });
  },
});

test({
  name: "runCheck takes a precise mock volume and retention ratio",
  timeout: 60_000,
  fn: async () => {
    await withFixture("valid", async (dir) => {
      const chunks: string[] = [];
      const lines: string[] = [];
      const report = await runCheck(
        { cwd: dir, mode: "quick", docs: 7, retention: 0.25 },
        {
          log: (line) => lines.push(line),
          write: (chunk) => chunks.push(chunk),
          tty: false,
        },
      );
      assertEquals(report.valid, true);
      assertEquals(report.docsPerCollection, 7);
      assertEquals(report.stateRetentionRatio, 0.25);
      assert(lines.some((line) => line.includes("docs: 7, retention: 0.25")));
      assert(chunks.join("").includes("[quick, 7 docs, 25% kept]"));
    });
  },
});

test("runCheck refuses a mock volume or retention outside its range", async () => {
  for (const options of [
    { docs: 0 },
    { docs: 2.5 },
    { docs: 99999 },
    { retention: 1.5 },
    { retention: -0.1 },
  ]) {
    await assertRejects(() =>
      runCheck(options, { log: () => {}, write: () => {}, tty: false }),
    );
  }
});

test("getMockGenerationConfig lets a volume override the preset", () => {
  assertEquals(getMockGenerationConfig("quick").DOCS_PER_COLLECTION_MAX, 10);
  assertEquals(
    getMockGenerationConfig("quick", 42).DOCS_PER_COLLECTION_MIN,
    42,
  );
  assertEquals(getMockGenerationConfig("hard", 42).DOCS_PER_COLLECTION_MAX, 42);
  assertEquals(
    getMockGenerationConfig("normal", 0).DOCS_PER_COLLECTION_MAX,
    100,
  );
});

test({
  name: "studio check runs in a worker and leaves the event loop free",
  timeout: 60_000,
  fn: async () => {
    await withFixture("failing-simulation", async (dir) => {
      const events: CheckEvent[] = [];
      let ticks = 0;
      const timer = setInterval(() => ticks++, 5);
      try {
        await runCheckInWorker(
          { project: { cwd: dir } } as never,
          { mode: "quick" },
          (event) => events.push(event),
        );
      } finally {
        clearInterval(timer);
      }
      assertEquals(
        events.map((event) => event.type),
        [
          "start",
          "schema",
          "migration-start",
          "migration-result",
          "migration-start",
          "migration-result",
          "done",
        ],
      );
      assert(ticks > 0);
    });
  },
});

test({
  name: "studio check in a worker stops at once when its signal aborts",
  timeout: 60_000,
  fn: async () => {
    await withFixture("failing-simulation", async (dir) => {
      const events: CheckEvent[] = [];
      const abort = new AbortController();
      await runCheckInWorker(
        { project: { cwd: dir } } as never,
        { mode: "quick", signal: abort.signal },
        (event) => {
          events.push(event);
          if (event.type === "migration-start") abort.abort();
        },
      );
      assert(!events.some((event) => event.type === "done"));
    });
  },
});

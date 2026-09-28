import { test } from "../../library/test/+harness.ts";
import { assert, assertEquals } from "../../library/test/+assert.ts";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { installLibrary } from "../../library/test/migration/cli/shared.ts";
import {
  type CheckEvent,
  runCheckInWorker,
  runStudioCheck,
} from "../src/api/check.ts";
import {
  type CheckFixtureVariant,
  writeCheckFixture,
} from "../../library/test/migration/cli/check-fixture.ts";

async function withFixture(
  variant: CheckFixtureVariant,
  work: (dir: string) => Promise<void>,
): Promise<void> {
  const dir = await mkdtemp(path.join(tmpdir(), "mongodbee_studio_check_"));
  try {
    await installLibrary(dir);
    await writeCheckFixture(dir, variant);
    await work(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test({
  name: "studio check streams schema, per-migration and done events in order",
  timeout: 60_000,
  fn: async () => {
    await withFixture("failing-simulation", async (dir) => {
      const events: CheckEvent[] = [];
      const report = await runStudioCheck(
        { project: { cwd: dir } },
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
        { project: { cwd: dir } },
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
  name: "studio check runs in a worker and leaves the event loop free",
  timeout: 60_000,
  fn: async () => {
    await withFixture("failing-simulation", async (dir) => {
      const events: CheckEvent[] = [];
      let ticks = 0;
      const timer = setInterval(() => ticks++, 5);
      try {
        await runCheckInWorker(
          { project: { cwd: dir } },
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
        { project: { cwd: dir } },
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

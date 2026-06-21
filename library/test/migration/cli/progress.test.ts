import { assert, assertEquals, assertStringIncludes } from "@std/assert";
import { createProgressReporter } from "../../../src/migration/cli/utils/progress.ts";
import type { MigrationProgressEvent } from "../../../src/migration/appliers/mongodb.ts";

function ev(p: Partial<MigrationProgressEvent>): MigrationProgressEvent {
  return {
    operationType: "flow_to_scope",
    phase: "progress",
    processed: 0,
    elapsedMs: 0,
    ...p,
  } as MigrationProgressEvent;
}

// Strip ANSI color/cursor codes so assertions read plainly.
function plain(s: string): string {
  // deno-lint-ignore no-control-regex
  return s.replace(/\x1b\[[0-9;]*[A-Za-z]/g, "").replace(/\r/g, "");
}

Deno.test("progress: disabled writes nothing", () => {
  let out = "";
  const r = createProgressReporter({ enabled: false, write: (c) => out += c });
  r.onProgress(ev({ phase: "start" }));
  r.onProgress(ev({ phase: "progress", processed: 100, elapsedMs: 100 }));
  r.onProgress(ev({ phase: "done", processed: 200, elapsedMs: 200 }));
  r.finish();
  assertEquals(out, "");
});

Deno.test("progress: with total renders bar, %, throughput; closes line on done", () => {
  let out = "";
  const r = createProgressReporter({ enabled: true, write: (c) => out += c });

  r.onProgress(ev({
    operationType: "flow_to_scope",
    collection: "scoped",
    phase: "start",
    processed: 0,
    total: 1000,
    elapsedMs: 0,
  }));
  r.onProgress(ev({
    phase: "done",
    collection: "scoped",
    processed: 1000,
    total: 1000,
    elapsedMs: 2000, // 1000 docs / 2s = 500 docs/s
  }));

  const text = plain(out);
  assertStringIncludes(text, "flow_to_scope");
  assertStringIncludes(text, "scoped");
  assertStringIncludes(text, "1,000/1,000");
  assertStringIncludes(text, "100%");
  assertStringIncludes(text, "█"); // a filled bar
  assertStringIncludes(text, "500 docs/s");
  assert(out.endsWith("\n"), "the line is closed with a newline on done");
});

Deno.test("progress: without total falls back to a doc counter", () => {
  let out = "";
  const r = createProgressReporter({ enabled: true, write: (c) => out += c });
  r.onProgress(
    ev({ operationType: "transform_collection", phase: "start", processed: 0 }),
  );
  r.onProgress(ev({ phase: "done", processed: 8000, elapsedMs: 1000 }));
  const text = plain(out);
  assertStringIncludes(text, "transform_collection");
  assertStringIncludes(text, "8,000 docs");
  assertStringIncludes(text, "8,000 docs/s");
});

Deno.test("progress: finish() closes a still-open line (e.g. on error mid-operation)", () => {
  let out = "";
  const r = createProgressReporter({ enabled: true, write: (c) => out += c });
  r.onProgress(ev({ phase: "start", processed: 0, total: 100 }));
  // no `done` (operation threw) → finish must emit the closing newline
  r.finish();
  assert(out.endsWith("\n"));
});

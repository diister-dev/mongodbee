import { test } from "../../+harness.ts";
import { assert, assertEquals, assertStringIncludes } from "../../+assert.ts";
import process from "node:process";
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
  // biome-ignore lint/suspicious/noControlCharactersInRegex: matching ANSI escapes is the point
  return s.replace(/\x1b\[[0-9;]*[A-Za-z]/g, "").replace(/\r/g, "");
}

test("progress: disabled writes nothing", () => {
  let out = "";
  const r = createProgressReporter({
    enabled: false,
    write: (c) => (out += c),
  });
  r.onProgress(ev({ phase: "start" }));
  r.onProgress(ev({ phase: "progress", processed: 100, elapsedMs: 100 }));
  r.onProgress(ev({ phase: "done", processed: 200, elapsedMs: 200 }));
  r.finish();
  assertEquals(out, "");
});

test("progress: with total renders bar, %, throughput; closes line on done", () => {
  let out = "";
  const r = createProgressReporter({ enabled: true, write: (c) => (out += c) });

  r.onProgress(
    ev({
      operationType: "flow_to_scope",
      collection: "scoped",
      phase: "start",
      processed: 0,
      total: 1000,
      elapsedMs: 0,
    }),
  );
  r.onProgress(
    ev({
      phase: "done",
      collection: "scoped",
      processed: 1000,
      total: 1000,
      elapsedMs: 2000, // 1000 docs / 2s = 500 docs/s
    }),
  );

  const text = plain(out);
  assertStringIncludes(text, "flow_to_scope");
  assertStringIncludes(text, "scoped");
  assertStringIncludes(text, "1,000/1,000");
  assertStringIncludes(text, "100%");
  assertStringIncludes(text, "█"); // a filled bar
  assertStringIncludes(text, "500 docs/s");
  assert(out.endsWith("\n"), "the line is closed with a newline on done");
});

test("progress: without total falls back to a doc counter", () => {
  let out = "";
  const r = createProgressReporter({ enabled: true, write: (c) => (out += c) });
  r.onProgress(
    ev({ operationType: "transform_collection", phase: "start", processed: 0 }),
  );
  r.onProgress(ev({ phase: "done", processed: 8000, elapsedMs: 1000 }));
  const text = plain(out);
  assertStringIncludes(text, "transform_collection");
  assertStringIncludes(text, "8,000 docs");
  assertStringIncludes(text, "8,000 docs/s");
});

test("progress: finish() closes a still-open line (e.g. on error mid-operation)", () => {
  let out = "";
  const r = createProgressReporter({ enabled: true, write: (c) => (out += c) });
  r.onProgress(ev({ phase: "start", processed: 0, total: 100 }));
  // no `done` (operation threw) → finish must emit the closing newline
  r.finish();
  assert(out.endsWith("\n"));
});

// Regression for C6: the default sink must go through `process.stdout` (works
// on Node/Bun/Deno) rather than a runtime-specific stdout handle. Spy on
// `process.stdout.write` and confirm the un-injected reporter reaches it.
test("progress: default writer routes through process.stdout (cross-runtime)", () => {
  const stdout = process.stdout as any;
  const originalWrite = stdout.write;
  let captured = "";
  stdout.write = (chunk: unknown): boolean => {
    captured +=
      typeof chunk === "string"
        ? chunk
        : new TextDecoder().decode(chunk as Uint8Array);
    return true;
  };
  try {
    // No `write` injected → exercises the default (process.stdout) sink.
    const r = createProgressReporter({ enabled: true });
    r.onProgress(ev({ phase: "done", processed: 5, elapsedMs: 1000 }));
    r.finish();
  } finally {
    stdout.write = originalWrite;
  }
  assertStringIncludes(plain(captured), "5 docs");
});

/**
 * Verrou — step progress must never leak cursor escapes into a non-TTY sink.
 *
 * The simulation phase runs for tens of seconds with nothing on screen, so
 * `check` looks hung; the fix draws a transient "in flight" line per
 * migration. That line only works on a terminal — in CI logs, in a pipe, and
 * in the in-process test harness that consumes `checkCommand` as a library
 * function, a stray `\r`/`\x1b[K` is garbage that also breaks log matching.
 */
import { assert, assertEquals } from "@std/assert";
import { createStepReporter } from "../../../src/migration/cli/utils/step-reporter.ts";

function capture(tty: boolean) {
  const chunks: string[] = [];
  const steps = createStepReporter({ tty, write: (c) => chunks.push(c) });
  return { chunks, steps, output: () => chunks.join("") };
}

Deno.test("step reporter: non-TTY emits plain lines only", () => {
  const { steps, output } = capture(false);

  steps.start("  … [1/2] alpha");
  steps.done("  ✓ [1/2] alpha");
  steps.start("  … [2/2] beta");
  steps.done("  ✗ [2/2] beta");
  steps.log("done");
  steps.finish();

  assertEquals(output(), "  ✓ [1/2] alpha\n  ✗ [2/2] beta\ndone\n");
  assertEquals(output().includes("\r"), false, "no carriage returns");
  assertEquals(output().includes("\x1b["), false, "no cursor escapes");
});

Deno.test("step reporter: TTY overwrites the transient line with the verdict", () => {
  const { chunks, steps } = capture(true);

  steps.start("  [1/1] alpha", { spinner: false });
  steps.done("  ✓ [1/1] alpha  1 operation");
  steps.finish();

  assertEquals(chunks[0], "\r\x1b[K  [1/1] alpha");
  // The verdict first erases the in-flight line, then commits its own.
  assertEquals(chunks[1], "\r\x1b[K");
  assertEquals(chunks[2], "  ✓ [1/1] alpha  1 operation\n");
});

Deno.test("step reporter: an abandoned transient line is erased by finish()", () => {
  const { chunks, steps } = capture(true);

  steps.start("  [1/1] alpha", { spinner: false });
  steps.finish();
  steps.finish(); // idempotent — a second finish must not emit anything

  assertEquals(chunks, ["\r\x1b[K  [1/1] alpha", "\r\x1b[K"]);
});

// Verrou — the in-flight line must MOVE.
//
// Regression it guards: the first version drew a static line and left it for
// the ~10s a migration takes to simulate. That reads as a hang exactly like the
// blank screen it replaced — the operator cannot tell "still working" from
// "wedged", which is the complaint that prompted the whole reporter.
Deno.test("step reporter: the in-flight line animates and counts elapsed time", async () => {
  const { chunks, steps } = capture(true);

  steps.start("[1/2] alpha");
  const framesAtStart = chunks.length;
  await new Promise((r) => setTimeout(r, 400));
  steps.finish();

  assert(
    chunks.length > framesAtStart + 1,
    `the line never re-rendered (${chunks.length} chunks) — a static line reads as a hang`,
  );
  // Distinct spinner frames, not the same glyph redrawn.
  const glyphs = new Set(
    chunks.slice(0, -1).map((c) => c.replace("\r\x1b[K", "").charAt(0)),
  );
  assert(glyphs.size > 1, `spinner did not advance: saw ${[...glyphs]}`);
});

// Verrou — the animation must never hold the process open.
//
// A bare setInterval keeps Deno's event loop alive: the CLI would hang after
// its last line, and this very test file would fail the runner's timer
// sanitizer. `start()` unrefs the tick; leaving the timer running past the test
// is the failure being guarded, so this case deliberately does NOT call
// finish() and relies on the sanitizer to catch a leak.
Deno.test("step reporter: a still-open animated line does not leak a timer", () => {
  const { steps } = capture(true);
  steps.start("[1/1] alpha");
  steps.finish();
});

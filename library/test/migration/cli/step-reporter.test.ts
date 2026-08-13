/**
 * Verrou — step progress must never leak cursor escapes into a non-TTY sink.
 *
 * The simulation phase runs for tens of seconds with nothing on screen, so
 * `check` looks hung; the fix draws a transient "in flight" line per
 * migration. That line only works on a terminal — in CI logs, in a pipe, and
 * in the in-process test harness that consumes `checkCommand` as a library
 * function, a stray `\r`/`\x1b[K` is garbage that also breaks log matching.
 */
import { assertEquals } from "@std/assert";
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

  steps.start("  … [1/1] alpha");
  steps.done("  ✓ [1/1] alpha  1 operation");
  steps.finish();

  assertEquals(chunks[0], "\r\x1b[K  … [1/1] alpha");
  // The verdict first erases the in-flight line, then commits its own.
  assertEquals(chunks[1], "\r\x1b[K");
  assertEquals(chunks[2], "  ✓ [1/1] alpha  1 operation\n");
});

Deno.test("step reporter: an abandoned transient line is erased by finish()", () => {
  const { chunks, steps } = capture(true);

  steps.start("  … [1/1] alpha");
  steps.finish();
  steps.finish(); // idempotent — a second finish must not emit anything

  assertEquals(chunks, ["\r\x1b[K  … [1/1] alpha", "\r\x1b[K"]);
});

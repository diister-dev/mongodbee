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
import { bold, dim, stripAnsiCode } from "@std/fmt/colors";
import { createStepReporter } from "../../../src/migration/cli/utils/step-reporter.ts";

function capture(tty: boolean, extra: { minRedrawMs?: number } = {}) {
  const chunks: string[] = [];
  const steps = createStepReporter({
    tty,
    write: (c) => chunks.push(c),
    // Observe every note unless a case is specifically about the floor.
    minRedrawMs: extra.minRedrawMs ?? 0,
  });
  return { chunks, steps, output: () => chunks.join("") };
}

Deno.test("step reporter: non-TTY emits plain lines only", () => {
  const { steps, output } = capture(false);

  steps.start("  … [1/2] alpha");
  steps.update("mocking collections/a 1/100");
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

  steps.start("  [1/1] alpha", { live: false });
  steps.done("  ✓ [1/1] alpha  1 operation");
  steps.finish();

  assertEquals(chunks[0], "\r\x1b[K  [1/1] alpha");
  // The verdict first erases the in-flight line, then commits its own.
  assertEquals(chunks[1], "\r\x1b[K");
  assertEquals(chunks[2], "  ✓ [1/1] alpha  1 operation\n");
});

Deno.test("step reporter: an abandoned transient line is erased by finish()", () => {
  const { chunks, steps } = capture(true);

  steps.start("  [1/1] alpha", { live: false });
  steps.finish();
  steps.finish(); // idempotent — a second finish must not emit anything

  assertEquals(chunks, ["\r\x1b[K  [1/1] alpha", "\r\x1b[K"]);
});

// Verrou — the in-flight line must MOVE, and it must move because the WORK
// moved.
//
// Regression it guards: two shipped versions animated this line on a
// `setInterval`. The loop it decorates — `SimulationValidator` — is
// `await`-heavy but every promise resolves synchronously, so the event loop
// never reaches the timer phase: the callback fired ZERO times across a
// 17-second silence on the owner's chain, and both fixes had been "verified"
// against a demo that awaited a real setTimeout. Progress is now pushed in by
// the work, so the only thing that can stop the line is the work stopping.
Deno.test("step reporter: the in-flight line re-renders from progress notes", () => {
  const { chunks, steps } = capture(true);

  steps.start("[1/2] alpha");
  const atStart = chunks.length;
  steps.update("applying operation 3/19");
  steps.update("mocking collections/+users 40/100");
  steps.finish();

  assert(
    chunks.length > atStart + 1,
    `the line never re-rendered (${chunks.length} chunks) — a static line reads as a hang`,
  );
  const rendered = chunks.slice(atStart, -1).map(stripAnsiCode);
  assert(
    rendered.some((c) => c.includes("applying operation 3/19")),
    `the note never reached the screen: ${JSON.stringify(rendered)}`,
  );
  assert(
    rendered.some((c) => c.includes("mocking collections/+users 40/100")),
    `the line does not follow the work: ${JSON.stringify(rendered)}`,
  );
  // Every redraw still erases first, or the notes pile up on one row.
  for (const chunk of chunks.slice(atStart, -1)) {
    assert(chunk.startsWith("\r\x1b[K"), `un-erased redraw: ${chunk}`);
  }
});

// Verrou — the line must never be driven by a timer again.
//
// The previous design's failure was structural, not a tuning mistake: no
// interval, however short, fires inside a loop that never yields. Scheduling
// one here is therefore a design regression, and the parent commit fails this
// case on its `setInterval` in `start()`.
Deno.test("step reporter: rendering schedules no timers", () => {
  const scheduled: string[] = [];
  const realInterval = globalThis.setInterval;
  const realTimeout = globalThis.setTimeout;
  globalThis.setInterval = ((...args: unknown[]) => {
    scheduled.push("setInterval");
    return (realInterval as (...a: unknown[]) => number)(...args);
  }) as typeof globalThis.setInterval;
  globalThis.setTimeout = ((...args: unknown[]) => {
    scheduled.push("setTimeout");
    return (realTimeout as (...a: unknown[]) => number)(...args);
  }) as typeof globalThis.setTimeout;

  try {
    const { steps } = capture(true);
    steps.start("[1/1] alpha");
    steps.update("applying operation 1/1");
    steps.done("  ✓ [1/1] alpha");
    steps.finish();
  } finally {
    globalThis.setInterval = realInterval;
    globalThis.setTimeout = realTimeout;
  }

  assertEquals(
    scheduled,
    [],
    "the in-flight line must be driven by the work, never by a clock",
  );
});

// Verrou — the elapsed counter survived the removal of the animation.
//
// It is what names the slow migration: without it a ten-second step and a
// one-second step look the same once they land.
Deno.test("step reporter: the in-flight line carries an elapsed counter", () => {
  const chunks: string[] = [];
  let now = 1_000_000;
  const realNow = Date.now;
  Date.now = () => now;
  try {
    const steps = createStepReporter({
      tty: true,
      write: (c) => chunks.push(c),
      minRedrawMs: 0,
    });
    steps.start("[1/1] alpha");
    now += 7_000;
    steps.update("mocking collections/a 1/100");
    steps.finish();
  } finally {
    Date.now = realNow;
  }

  assert(
    chunks.some((c) => stripAnsiCode(c).includes("7s")),
    `no elapsed counter: ${JSON.stringify(chunks.map(stripAnsiCode))}`,
  );
});

// Verrou — a note must not turn the transient line into an unerasable one.
//
// `\r\x1b[K` clears only the row the cursor sits on. The step labels are
// already ~130 columns on the owner's chain; appending a progress note pushed
// them past any terminal width, and a wrapped line leaves its first rows on
// screen underneath the verdict.
Deno.test("step reporter: the in-flight line is clamped to the terminal width", () => {
  const chunks: string[] = [];
  const steps = createStepReporter({
    tty: true,
    write: (c) => chunks.push(c),
    minRedrawMs: 0,
    columns: 40,
  });

  steps.start(`  ${bold("a".repeat(60))} ${dim("(2026_01_01_0000_XXXX)")}`);
  steps.update("mocking collections/+something_long 40/100");
  steps.finish();

  for (const chunk of chunks) {
    const visible = stripAnsiCode(chunk).replace("\r", "");
    assert(
      visible.length <= 40,
      `line of ${visible.length} columns would wrap a 40-column terminal`,
    );
  }
  // Colour must be closed, or the truncation bleeds into the next write.
  assert(
    chunks.some((c) => c.endsWith("\x1b[0m")),
    "a truncated line must reset its SGR state",
  );
});

// Verrou — the redraw floor throttles writes without hiding the phase change.
//
// The mock engine reports per generated document — six figures of them on a
// real chain — and every redraw is a write syscall. The floor caps that, but
// the FIRST note after a step opens must always paint: for a step that
// finishes quickly it is the only note there will ever be.
Deno.test("step reporter: the redraw floor keeps the first note of a step", () => {
  const chunks: string[] = [];
  const steps = createStepReporter({
    tty: true,
    write: (c) => chunks.push(c),
    minRedrawMs: 60_000,
  });

  steps.start("[1/1] alpha");
  steps.update("first note");
  steps.update("second note");
  steps.finish();

  const text = stripAnsiCode(chunks.join(""));
  assert(text.includes("first note"), "the opening note must always paint");
  assertEquals(
    text.includes("second note"),
    false,
    "a redraw inside the floor must be dropped",
  );
});

// Verrou — `live: false` stays a silent, static line.
//
// It is what a step that lands immediately uses; decorating it or letting
// notes redraw it would turn a one-shot label into a flicker.
Deno.test("step reporter: a live:false step ignores progress notes", () => {
  const { chunks, steps } = capture(true);

  steps.start("  ⏭  fast-forward [1/3] alpha", { live: false });
  steps.update("mocking collections/a 1/100");
  steps.finish();

  assertEquals(chunks, [
    "\r\x1b[K  ⏭  fast-forward [1/3] alpha",
    "\r\x1b[K",
  ]);
});

Deno.test("step reporter: a note outside any step writes nothing", () => {
  const { chunks, steps } = capture(true);

  steps.update("mocking collections/a 1/100");
  steps.start("[1/1] alpha", { live: false });
  steps.done("  ✓ [1/1] alpha");
  steps.update("mocking collections/a 2/100");

  assertEquals(
    chunks.filter((c) => stripAnsiCode(c).includes("mocking")),
    [],
  );
});

// Verrou — the in-flight marker must MOVE between renders.
//
// Regression it guards: the marker was a fixed "⋯". The note underneath it
// changes, but a reader scanning the screen without reading the text had no
// motion to lock onto — and a phase that reports the SAME note twice looked
// frozen even though work was landing. The frame advances per render, never on
// a clock: the validator blocks the thread, so a timer-driven frame would sit
// still exactly when motion matters.
Deno.test("step reporter: the in-flight marker advances frame on each render", () => {
  const { chunks, steps } = capture(true, { minRedrawMs: 0 });

  steps.start("[1/1] alpha");
  for (const note of ["mocking a", "mocking b", "mocking c", "mocking d"]) {
    steps.update(note);
  }
  steps.finish();

  const markers = chunks
    .filter((c) => c.startsWith("\r\x1b[K") && c.length > 4)
    .map((c) => c.replace("\r\x1b[K", "").charAt(0));
  assert(markers.length >= 5, `expected renders, saw ${markers.length}`);
  assert(
    new Set(markers).size > 1,
    `the marker never changed (${
      markers.join("")
    }) — a static glyph reads as frozen`,
  );
});

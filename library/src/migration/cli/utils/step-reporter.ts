/**
 * One-line-per-step progress for the migration validation loop.
 *
 * The simulation phase runs for tens of seconds with nothing on screen, so a
 * `check` looks hung. This draws a transient "in flight" line on a TTY —
 * re-rendered from the work as it advances, then overwritten in place by the
 * step's verdict when it lands — and stays line-oriented everywhere else, so
 * CI logs, pipes and the in-process test harness never receive `\r` or ANSI
 * cursor escapes.
 *
 * Driven by {@link StepReporter.update}, never by a timer:
 * `SimulationValidator.validateMigration` is `await`-heavy but its promises
 * resolve synchronously, so draining microtasks never reaches the timer phase
 * and a `setInterval` callback fires zero times across a 17s step. A render is
 * a synchronous write, so it paints even while the loop holds the thread.
 *
 * @module
 */
import process from "node:process";

/**
 * Floor between two redraws of the same line. A note changes far faster than
 * an eye can read it and every redraw is a write syscall — the mock engine
 * reports per generated document, six figures of them on a real chain.
 */
const MIN_REDRAW_MS = 80;

/**
 * In-flight marker, advanced one frame per render — never on a timer (see the
 * module doc). Motion the eye catches without reading, so a step reporting the
 * same note twice still looks alive.
 */
const FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"] as const;

/** A step-oriented console writer. See {@link createStepReporter}. */
export interface StepReporter {
  /**
   * Announce a step that is starting (transient on a TTY, silent otherwise).
   *
   * @param options.live - Decorate the label with an in-flight marker and an
   *   elapsed-seconds counter, and accept {@link StepReporter.update} notes
   *   for it. Default `true`. Pass `false` for a step that lands immediately
   *   (a skip), where the decoration would only flicker — the label is then
   *   drawn once, exactly as given.
   */
  start(label: string, options?: { live?: boolean }): void;
  /**
   * Re-render the open step's line with a note describing what the work is
   * doing right now. No-op off a TTY, without an open step, or for a step
   * started with `live: false`.
   */
  update(note: string): void;
  /** Commit the step's final line, replacing the transient one. */
  done(line: string): void;
  /** Print a line outside any step, closing a transient line first. */
  log(line: string): void;
  /** Drop a still-open transient line (call in a `finally`). */
  finish(): void;
}

/** Visible width, ignoring the SGR sequences the labels are coloured with. */
// deno-lint-ignore no-control-regex
const ANSI = /\x1b\[[0-9;]*m/g;

/**
 * Truncates to `max` visible columns, keeping SGR sequences intact.
 *
 * A transient line that wraps cannot be erased: `\r\x1b[K` only clears the
 * row the cursor sits on, so the earlier rows stay on screen under the
 * verdict. The step labels are already ~130 columns on this chain, and a
 * progress note only makes them longer.
 */
function clamp(line: string, max: number): string {
  if (max <= 0 || line.replace(ANSI, "").length <= max) return line;
  let visible = 0;
  let out = "";
  for (let i = 0; i < line.length;) {
    if (line[i] === "\x1b" && line[i + 1] === "[") {
      // A CSI sequence costs no columns: copy it whole, parameter bytes then
      // the final byte, and keep counting from after it.
      let end = i + 2;
      while (end < line.length && /[0-9;]/.test(line[end])) end++;
      out += line.slice(i, end + 1);
      i = end + 1;
      continue;
    }
    if (visible === max) break;
    out += line[i++];
    visible++;
  }
  // Reset, or the truncated line bleeds its colour into the next write.
  return `${out}\x1b[0m`;
}

/** Terminal width, or 0 when nothing can tell us (no clamping then). */
function terminalColumns(): number {
  try {
    if (typeof Deno !== "undefined" && typeof Deno.consoleSize === "function") {
      return Deno.consoleSize().columns;
    }
  } catch {
    // Not a terminal — the tty flag below already decides whether we render.
  }
  return process.stdout.columns ?? 0;
}

/**
 * Builds a {@link StepReporter}.
 *
 * @param options.tty - Render transient lines. Defaults to
 *   `Deno.stdout.isTerminal()`, falling back to `process.stdout.isTTY` so the
 *   CLI behaves the same when the published package runs under Node.
 * @param options.write - Sink for rendered chunks; defaults to stdout.
 *   Injectable so both branches are testable without a real terminal.
 * @param options.minRedrawMs - Floor between two redraws of the same line.
 *   Defaults to {@link MIN_REDRAW_MS}; tests pass `0` to observe every note.
 * @param options.columns - Width to clamp transient lines to. Defaults to the
 *   terminal's when writing to stdout, `0` (no clamping) behind an injected
 *   sink.
 */
export function createStepReporter(
  options: {
    tty?: boolean;
    write?: (chunk: string) => void;
    minRedrawMs?: number;
    columns?: number;
  } = {},
): StepReporter {
  const tty = options.tty ??
    (typeof Deno !== "undefined"
      ? Deno.stdout?.isTerminal?.() === true
      : process.stdout.isTTY === true);
  const write = options.write ??
    ((chunk: string) => {
      process.stdout.write(chunk);
    });
  const minRedrawMs = options.minRedrawMs ?? MIN_REDRAW_MS;
  // Clamping exists because a real terminal WRAPS. An injected sink is a
  // string buffer with no width, so measuring one there would only truncate
  // what the caller wanted to read back.
  const columns = options.columns ??
    (tty && !options.write ? terminalColumns() : 0);

  let transientOpen = false;
  let live = false;
  let startedAt = 0;
  let lastRenderAt = 0;
  let label = "";
  let note = "";
  let frame = 0;

  const clearTransient = () => {
    if (!transientOpen) return;
    // \r to column 0, \x1b[K to erase the rest — the next write overwrites it.
    write("\r\x1b[K");
    transientOpen = false;
    live = false;
  };

  // The line is evidence of life AND of what the work is doing; the elapsed
  // count also exposes WHICH migration is the slow one.
  const render = () => {
    const secs = Math.floor((Date.now() - startedAt) / 1000);
    lastRenderAt = Date.now();
    // Clamp the payload only: `\x1b[K` is not an SGR sequence, so folding it
    // into the measured string would spend three columns on the erase itself.
    const body = `${FRAMES[frame++ % FRAMES.length]} ${label}${
      note ? ` · ${note}` : ""
    }${secs > 0 ? ` ${secs}s` : ""}`;
    write(`\r\x1b[K${clamp(body, columns)}`);
  };

  return {
    start(stepLabel, stepOptions) {
      if (!tty) return;
      clearTransient();
      transientOpen = true;
      if (stepOptions?.live === false) {
        write(`\r\x1b[K${stepLabel}`);
        return;
      }
      live = true;
      label = stepLabel;
      note = "";
      frame = 0;
      startedAt = Date.now();
      render();
      // Disarm the floor: the first note names the phase that just began, and
      // for a step that finishes fast it is the only note there will ever be.
      lastRenderAt = 0;
    },
    update(stepNote) {
      if (!live) return;
      if (Date.now() - lastRenderAt < minRedrawMs) return;
      note = stepNote;
      render();
    },
    done(line) {
      clearTransient();
      write(`${line}\n`);
    },
    log(line) {
      clearTransient();
      write(`${line}\n`);
    },
    finish() {
      clearTransient();
    },
  };
}

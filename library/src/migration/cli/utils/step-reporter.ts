/**
 * One-line-per-step progress for the migration validation loop.
 *
 * The simulation phase runs for tens of seconds with nothing on screen, so a
 * `check` looks hung. This draws a transient "in flight" line on a TTY —
 * overwritten in place by the step's verdict when it lands — and stays
 * line-oriented everywhere else, so CI logs, pipes and the in-process test
 * harness never receive `\r` or ANSI cursor escapes.
 *
 * @module
 */
import process from "node:process";

const SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"] as const;
const TICK_MS = 120;

/**
 * Keep the animation from being the reason a process — or a test case — stays
 * alive. The timer only decorates work that is already holding the event loop.
 */
function unrefTimer(id: unknown): void {
  if (typeof Deno !== "undefined" && typeof Deno.unrefTimer === "function") {
    Deno.unrefTimer(id as number);
    return;
  }
  (id as { unref?: () => void })?.unref?.();
}

/** A step-oriented console writer. See {@link createStepReporter}. */
export interface StepReporter {
  /**
   * Announce a step that is starting (transient on a TTY, silent otherwise).
   *
   * @param options.spinner - Prefix an animated frame and an elapsed-seconds
   *   counter, re-rendered on a timer. Default `true`. Pass `false` for a step
   *   that lands immediately (a skip), where motion would only flicker — the
   *   label is then drawn once, exactly as given.
   */
  start(label: string, options?: { spinner?: boolean }): void;
  /** Commit the step's final line, replacing the transient one. */
  done(line: string): void;
  /** Print a line outside any step, closing a transient line first. */
  log(line: string): void;
  /** Drop a still-open transient line (call in a `finally`). */
  finish(): void;
}

/**
 * Builds a {@link StepReporter}.
 *
 * @param options.tty - Render transient lines. Defaults to
 *   `Deno.stdout.isTerminal()`, falling back to `process.stdout.isTTY` so the
 *   CLI behaves the same when the published package runs under Node.
 * @param options.write - Sink for rendered chunks; defaults to stdout.
 *   Injectable so both branches are testable without a real terminal.
 */
export function createStepReporter(
  options: { tty?: boolean; write?: (chunk: string) => void } = {},
): StepReporter {
  const tty = options.tty ??
    (typeof Deno !== "undefined"
      ? Deno.stdout?.isTerminal?.() === true
      : process.stdout.isTTY === true);
  const write = options.write ??
    ((chunk: string) => {
      process.stdout.write(chunk);
    });

  let transientOpen = false;
  let tick: unknown;
  let frame = 0;
  let startedAt = 0;
  let current = "";

  const clearTransient = () => {
    if (tick !== undefined) {
      clearInterval(tick as number);
      tick = undefined;
    }
    if (!transientOpen) return;
    // \r to column 0, \x1b[K to erase the rest — the next write overwrites it.
    write("\r\x1b[K");
    transientOpen = false;
  };

  // A migration takes ~10s to simulate, and a STATIC line for that long reads
  // as a hang exactly like the empty screen it replaced — the operator cannot
  // tell "still working" from "wedged". The frame moves and the elapsed count
  // rises, so the line is evidence of life; the seconds also expose WHICH
  // migration is the slow one.
  const render = () => {
    const secs = Math.floor((Date.now() - startedAt) / 1000);
    write(
      `\r\x1b[K${SPINNER[frame % SPINNER.length]} ${current}${
        secs > 0 ? ` ${secs}s` : ""
      }`,
    );
    frame++;
  };

  return {
    start(label, options) {
      if (!tty) return;
      clearTransient();
      transientOpen = true;
      if (options?.spinner === false) {
        write(`\r\x1b[K${label}`);
        return;
      }
      current = label;
      startedAt = Date.now();
      frame = 0;
      render();
      tick = setInterval(render, TICK_MS);
      unrefTimer(tick);
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

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

/** A step-oriented console writer. See {@link createStepReporter}. */
export interface StepReporter {
  /** Announce a step that is starting (transient on a TTY, silent otherwise). */
  start(label: string): void;
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
 *   `Deno.stdout.isTerminal()`, falling back to `false` outside Deno.
 * @param options.write - Sink for rendered chunks; defaults to stdout.
 *   Injectable so both branches are testable without a real terminal.
 */
export function createStepReporter(
  options: { tty?: boolean; write?: (chunk: string) => void } = {},
): StepReporter {
  const tty = options.tty ??
    (typeof Deno !== "undefined" && Deno.stdout?.isTerminal?.() === true);
  const write = options.write ??
    ((chunk: string) => {
      process.stdout.write(chunk);
    });

  let transientOpen = false;

  const clearTransient = () => {
    if (!transientOpen) return;
    // \r to column 0, \x1b[K to erase the rest — the next write overwrites it.
    write("\r\x1b[K");
    transientOpen = false;
  };

  return {
    start(label) {
      if (!tty) return;
      clearTransient();
      write(`\r\x1b[K${label}`);
      transientOpen = true;
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

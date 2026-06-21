/**
 * Live progress renderer for migration CLI commands.
 *
 * Consumes the applier's `onProgress` events and draws a single in-place
 * updating line per long-running operation — spinner, processed/total, a bar +
 * percentage when the total is known, throughput (docs/s) and elapsed time.
 * Redraws are throttled to ~11 fps so a fast operation doesn't flood the
 * terminal. When not attached to a TTY it stays silent (no `\r` garbage in
 * piped/CI logs).
 *
 * @module
 */
import type { MigrationProgressEvent } from "../../appliers/mongodb.ts";
import { dim } from "@std/fmt/colors";

const FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const encoder = new TextEncoder();

/** A wired `onProgress` callback plus a `finish` to close any open line. */
export interface ProgressReporter {
  onProgress: (event: MigrationProgressEvent) => void;
  /** Close an open progress line (call after the migration completes/fails). */
  finish: () => void;
}

/**
 * Build a {@link ProgressReporter}. Pass its `onProgress` to
 * {@link createMongodbApplier} and call `finish()` in a `finally`.
 *
 * @param options.enabled - Render live progress. Defaults to `false`; callers
 *   typically pass `Deno.stdout.isTerminal()`.
 * @param options.write - Sink for rendered chunks. Defaults to stdout;
 *   injectable so the renderer can be unit-tested without a TTY.
 */
export function createProgressReporter(
  options: {
    enabled?: boolean;
    write?: (chunk: string) => void;
  } = {},
): ProgressReporter {
  const enabled = options.enabled ?? false;
  const write = options.write ??
    ((chunk: string) => {
      Deno.stdout.writeSync(encoder.encode(chunk));
    });

  let frame = 0;
  let lastDraw = 0;
  let lineOpen = false;

  const rate = (event: MigrationProgressEvent): string => {
    const seconds = event.elapsedMs / 1000;
    return seconds > 0
      ? `${
        Math.round(event.processed / seconds).toLocaleString("en-US")
      } docs/s`
      : "—";
  };

  const render = (event: MigrationProgressEvent): string => {
    frame = (frame + 1) % FRAMES.length;
    const where = event.collection ? ` ${dim("→")} ${event.collection}` : "";
    let body: string;
    if (event.total && event.total > 0) {
      const pct = Math.min(
        100,
        Math.floor((event.processed / event.total) * 100),
      );
      const width = 18;
      const filled = Math.round((pct / 100) * width);
      body =
        `${event.processed.toLocaleString("en-US")}/${
          event.total.toLocaleString("en-US")
        } ` +
        `${"█".repeat(filled)}${"░".repeat(width - filled)} ${pct}%`;
    } else {
      body = `${event.processed.toLocaleString("en-US")} docs`;
    }
    return `  ${FRAMES[frame]} ${event.operationType}${where}  ${body}  ` +
      `${dim(rate(event))}  ${dim(`${(event.elapsedMs / 1000).toFixed(1)}s`)}`;
  };

  const drawInPlace = (event: MigrationProgressEvent) => {
    // \r returns to column 0, \x1b[K clears to end of line — overwrite in place.
    write(`\r\x1b[K${render(event)}`);
    lineOpen = true;
  };

  return {
    onProgress(event) {
      if (!enabled) return;
      if (event.phase === "done") {
        drawInPlace(event); // draw the final numbers, then close the line
        write("\n");
        lineOpen = false;
      } else if (event.phase === "start") {
        lastDraw = performance.now();
        drawInPlace(event);
      } else {
        const now = performance.now();
        if (now - lastDraw < 90) return; // ~11 fps throttle
        lastDraw = now;
        drawInPlace(event);
      }
    },
    finish() {
      if (enabled && lineOpen) {
        write("\n");
        lineOpen = false;
      }
    },
  };
}

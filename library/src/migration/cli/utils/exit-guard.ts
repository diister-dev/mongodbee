/**
 * Guarantees that a finished CLI command ends its process.
 *
 * @module
 */

import process from "node:process";
import { setTimeout } from "node:timers";

/**
 * How long a finished command may keep the process alive before it is ended.
 *
 * Every command closes its MongoDB client in a `finally`, and nothing else
 * should hold the event loop. But a stray handle (a socket a driver keeps, a
 * listener left on stdin) kept the process waiting with the work long done,
 * and callers without a terminal (agents, CI) could not tell a hang from a
 * slow run.
 */
export const EXIT_GRACE_MS = 5_000;

/** Commands meant to keep running after `main()` resolves. */
const LONG_RUNNING = new Set(["studio"]);

/**
 * Arms an `unref`'d timer that ends the process once `graceMs` have passed.
 *
 * When the event loop drains on its own, the process exits as it always did
 * and the timer never fires: it only acts when something leaked. Disabled
 * when any argument names a long-running command (checked on every argument:
 * with a flag value before it, `--port 4000 studio`, the first positional is
 * not the command).
 */
export function armExitGuard(
  args: readonly string[],
  graceMs: number = EXIT_GRACE_MS,
): void {
  if (args.some((arg) => LONG_RUNNING.has(arg))) return;
  const command = args.find((arg) => !arg.startsWith("-")) ?? "help";
  setTimeout(() => {
    process.stderr.write(
      `mongodbee: "${command}" finished but a handle kept the process alive; exiting.\n`,
    );
    process.exit(process.exitCode ?? 0);
  }, graceMs).unref();
}

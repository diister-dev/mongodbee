/**
 * User confirmation utility for CLI commands
 *
 * @module
 */

import * as readline from "node:readline";
import process from "node:process";
import { dim, yellow } from "../../../utils/colors.ts";

/**
 * Streams to prompt on. Defaults to the process' stdin/stdout; tests inject
 * their own.
 */
export interface ConfirmOptions {
  input?: readline.ReadLineOptions["input"];
  output?: readline.ReadLineOptions["output"];
}

/**
 * Prompts user for confirmation
 *
 * Resolves `true` only for an answer of `yes` (any case, surrounding
 * whitespace ignored). Any other line, or the input closing without a line
 * (Ctrl-D, an empty pipe), resolves `false`.
 *
 * @param message - The confirmation message to display
 * @param options - Streams to read the answer from / echo to
 * @returns Promise resolving to true if user confirmed, false otherwise
 */
export async function confirm(
  message: string,
  options: ConfirmOptions = {},
): Promise<boolean> {
  console.log(yellow(message));
  console.log(dim("Type 'yes' to confirm: "));

  const rl = readline.createInterface({
    input: options.input ?? process.stdin,
    output: options.output ?? process.stdout,
  });

  return new Promise((resolve) => {
    // `rl.close()` emits `close` SYNCHRONOUSLY. Resolving from the `line`
    // handler AFTER closing let the `close` handler win the race, so every
    // answer — "yes" included — came back as a refusal. Settle once, from
    // whichever event fires first, and only then close.
    let settled = false;
    const settle = (value: boolean) => {
      if (settled) return;
      settled = true;
      resolve(value);
    };

    rl.once("line", (line) => {
      settle(line.trim().toLowerCase() === "yes");
      rl.close();
    });
    rl.once("close", () => settle(false));
  });
}

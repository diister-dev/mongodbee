/**
 * User confirmation utility for CLI commands
 *
 * @module
 */

import * as readline from "node:readline";
import { dim, yellow } from "@std/fmt/colors";

/**
 * Prompts user for confirmation
 *
 * @param message - The confirmation message to display
 * @returns Promise resolving to true if user confirmed, false otherwise
 */
export async function confirm(message: string): Promise<boolean> {
  console.log(yellow(message));
  console.log(dim("Type 'yes' to confirm: "));

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  return new Promise((resolve) => {
    rl.once("line", (line) => {
      rl.close();
      resolve(line.trim().toLowerCase() === "yes");
    });
    rl.once("close", () => {
      resolve(false);
    });
  });
}

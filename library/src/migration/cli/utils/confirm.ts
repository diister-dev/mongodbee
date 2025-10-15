/**
 * User confirmation utility for CLI commands
 *
 * @module
 */

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

  const buf = new Uint8Array(1024);
  const n = await Deno.stdin.read(buf);

  if (n === null) {
    return false;
  }

  const answer = new TextDecoder().decode(buf.subarray(0, n)).trim()
    .toLowerCase();
  return answer === "yes";
}

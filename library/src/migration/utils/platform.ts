/**
 * Platform utilities for cross-runtime compatibility
 *
 * @module
 */

import process from "node:process";

/**
 * Convert a file path to a file:// URL for dynamic imports
 * Works on Windows, macOS, and Linux across Deno, Node.js, and Bun
 */
export function pathToFileUrl(filePath: string): string {
  if (process.platform === "win32") {
    return `file:///${filePath.replace(/\\/g, "/")}`;
  }
  return `file://${filePath}`;
}

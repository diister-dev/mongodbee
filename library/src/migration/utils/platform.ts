/**
 * Platform utilities for cross-runtime compatibility
 *
 * @module
 */

import { fileURLToPath, pathToFileURL } from "node:url";
import { realpathSync } from "node:fs";
import process from "node:process";

/**
 * Convert a file path to a file:// URL for dynamic imports
 * Works on Windows, macOS, and Linux across Deno, Node.js, and Bun
 *
 * Delegates to `node:url`, which every target runtime implements, rather than
 * building the URL by hand: a migrations directory containing a space, `#` or
 * `?` produced an href that the dynamic `import()` then resolved to the wrong
 * path (or failed outright), because those characters need percent-encoding.
 */
export function pathToFileUrl(filePath: string): string {
  return pathToFileURL(filePath).href;
}

/**
 * Whether the given module is the one the runtime was asked to execute.
 *
 * Deliberately does NOT consult `import.meta.main`: read here it would describe
 * *this* module, which is never the entry point. The caller's `import.meta.url`
 * against `process.argv[1]` is the check that holds in Node, Bun and Deno
 * alike.
 *
 * The comparison has to go through `realpath`: npm installs a `bin` as a
 * symlink, so the CLI runs with `argv[1]` pointing at
 * `node_modules/.bin/mongodbee` while `import.meta.url` names the real file
 * inside the package — comparing them raw makes the entry point silently do
 * nothing.
 *
 * @param moduleUrl The caller's own `import.meta.url`.
 */
export function isMainModule(moduleUrl: string): boolean {
  const entry = process.argv[1];
  if (!entry) return false;

  const resolve = (p: string): string => {
    try {
      return realpathSync(p);
    } catch {
      return p;
    }
  };

  try {
    return resolve(fileURLToPath(moduleUrl)) === resolve(entry);
  } catch {
    return false;
  }
}

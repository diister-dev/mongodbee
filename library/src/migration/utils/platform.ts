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
 * Whether the calling module is the one the runtime was asked to execute.
 *
 * Takes the caller's own `import.meta` (ours would describe this module,
 * which is never the entry point). Deno and Bun answer directly through
 * `import.meta.main`, and that is the only answer that holds when the entry
 * is a `jsr:` or `npm:` specifier: its URL is not a file path, so the
 * argv comparison below can only say "no" — and
 * `deno run jsr:@diister/mongodbee/migration/cli/bin migrate` used to exit 0
 * having done nothing at all. Node has no such flag, so it falls back to
 * `import.meta.url` against `process.argv[1]`.
 *
 * That comparison has to go through `realpath`: npm installs a `bin` as a
 * symlink, so the CLI runs with `argv[1]` pointing at
 * `node_modules/.bin/mongodbee` while `import.meta.url` names the real file
 * inside the package — comparing them raw makes the entry point silently do
 * nothing.
 *
 * @param meta The caller's own `import.meta`.
 */
export function isMainModule(meta: { url: string; main?: boolean }): boolean {
  if (typeof meta.main === "boolean") return meta.main;

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
    return resolve(fileURLToPath(meta.url)) === resolve(entry);
  } catch {
    return false;
  }
}

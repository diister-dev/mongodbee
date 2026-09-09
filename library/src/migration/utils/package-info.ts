/**
 * @fileoverview Package information utilities
 *
 * Provides access to MongoDBee package metadata such as version number.
 *
 * @module
 */

import { existsSync } from "node:fs";
import path from "node:path";
import { NAME, VERSION } from "../../version.ts";

/** The package's name on JSR, which is scoped unlike the npm one. */
const JSR_NAME = "@diister/mongodbee";

/**
 * Gets the current version of MongoDBee
 *
 * @returns The version string (e.g., "0.13.0")
 *
 * @example
 * ```typescript
 * const version = getCurrentVersion();
 * console.log(`MongoDBee version: ${version}`);
 * // Output: MongoDBee version: 0.13.0
 * ```
 */
export function getCurrentVersion(): string {
  return VERSION;
}

/**
 * Gets package information including name and version
 *
 * @returns Object with package name and version
 *
 * @example
 * ```typescript
 * const info = getPackageInfo();
 * console.log(`${info.name} v${info.version}`);
 * // Output: mongodbee v0.13.0
 * ```
 */
export function getPackageInfo(): { name: string; version: string } {
  return { name: NAME, version: VERSION };
}

/**
 * The specifier generated files should import MongoDBee under.
 *
 * The package carries two names — `mongodbee` on npm, `@diister/mongodbee` on
 * JSR — so a template cannot hardcode one without breaking every user of the
 * other. The project's own manifest says which ecosystem it lives in.
 *
 * @param projectDir Directory of the project the files are generated into.
 * @returns `"mongodbee"` for an npm/Bun project, `"@diister/mongodbee"` for a
 *   Deno one.
 *
 * @example
 * ```typescript
 * const spec = resolveImportSpecifier(cwd);
 * const template = `import { migrationDefinition } from "${spec}/migration";`;
 * ```
 */
export function resolveImportSpecifier(projectDir: string): string {
  const has = (file: string) => existsSync(path.join(projectDir, file));

  // A package.json settles it: the project installs from npm, where the name is
  // unscoped. Checked first because a project can carry both manifests.
  if (has("package.json")) return NAME;
  if (has("deno.json") || has("deno.jsonc") || has("jsr.json")) {
    return JSR_NAME;
  }
  return NAME;
}

/**
 * @fileoverview Package information utilities
 *
 * Provides access to MongoDBee package metadata such as version number.
 *
 * @module
 */

import { NAME, VERSION } from "../../version.ts";

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
 * One name serves both registries, so nothing has to be inferred — kept as a
 * function because the generated templates read better calling it than
 * interpolating a constant, and because npm refused the unscoped `mongodbee`
 * (too close to `mongodb`), which is the kind of decision worth leaving a
 * handle on.
 *
 * @example
 * ```typescript
 * const template = `import { migrationDefinition } from "${importSpecifier()}/migration";`;
 * ```
 */
export function importSpecifier(): string {
  return NAME;
}

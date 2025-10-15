/**
 * @fileoverview Package information utilities
 *
 * Provides access to MongoDBee package metadata such as version number.
 *
 * @module
 */

import packageInfo from "../../../deno.json" with { type: "json" };

/**
 * Gets the current version of MongoDBee from deno.json
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
  return packageInfo.version;
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
 * // Output: @diister/mongodbee v0.13.0
 * ```
 */
export function getPackageInfo(): { name: string; version: string } {
  return {
    name: packageInfo.name,
    version: packageInfo.version,
  };
}

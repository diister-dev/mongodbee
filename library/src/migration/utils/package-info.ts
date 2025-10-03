/**
 * @fileoverview Package information utilities
 * 
 * Provides access to MongoDBee package metadata such as version number.
 * 
 * @module
 */

// Cache the version at module load time
let cachedVersion: string | null = null;
let cachedPackageInfo: { name: string; version: string } | null = null;

/**
 * Reads and caches package info from deno.json
 */
function loadPackageInfo(): { name: string; version: string } {
  if (cachedPackageInfo) {
    return cachedPackageInfo;
  }

  try {
    // Read deno.json from the library root
    const denoJsonPath = new URL('../../../deno.json', import.meta.url);
    const content = Deno.readTextFileSync(denoJsonPath);
    const denoJson = JSON.parse(content);
    
    cachedPackageInfo = {
      name: denoJson.name || '@diister/mongodbee',
      version: denoJson.version || 'unknown',
    };
    
    cachedVersion = cachedPackageInfo.version;
    
    return cachedPackageInfo;
  } catch (error) {
    console.warn('Unable to read package info from deno.json:', error);
    
    cachedPackageInfo = {
      name: '@diister/mongodbee',
      version: 'unknown',
    };
    cachedVersion = 'unknown';
    
    return cachedPackageInfo;
  }
}

/**
 * Gets the current version of MongoDBee from deno.json
 * 
 * The version is cached after the first read for optimal performance.
 * 
 * @returns The version string (e.g., "0.13.0") or "unknown" if unable to read
 * 
 * @example
 * ```typescript
 * const version = getCurrentVersion();
 * console.log(`MongoDBee version: ${version}`);
 * // Output: MongoDBee version: 0.13.0
 * ```
 */
export function getCurrentVersion(): string {
  if (cachedVersion !== null) {
    return cachedVersion;
  }
  
  const info = loadPackageInfo();
  return info.version;
}

/**
 * Gets package information including name and version
 * 
 * The package info is cached after the first read for optimal performance.
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
  return loadPackageInfo();
}

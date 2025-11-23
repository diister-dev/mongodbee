/**
 * @fileoverview Runtime configuration management for MongoDBee
 *
 * This module provides a singleton pattern for accessing MongoDBee configuration
 * at runtime. It automatically loads the mongodbee.config.ts file from the
 * current working directory if it exists.
 *
 * @module
 */

import { DEFAULT_MONGODBEE_CONFIG, type MongodbeeConfig } from "./config.ts";
import { loadConfig } from "./migration/config/loader.ts";

/**
 * Current runtime configuration
 */
let currentConfig: MongodbeeConfig = { ...DEFAULT_MONGODBEE_CONFIG };

/**
 * Flag to track if config has been loaded
 */
let configLoaded = false;

/**
 * Discovers and loads configuration file from common locations
 */
async function discoverAndLoadConfig(): Promise<void> {
  if (configLoaded) return;

  try {
    const loadedConfig = await loadConfig();
    if (loadedConfig) {
      setRuntimeConfig(loadedConfig);
    }
  } catch {
    // No config file found, use defaults
  }

  configLoaded = true;
}

// Auto-load configuration at module initialization (top-level await)
await discoverAndLoadConfig();

/**
 * Sets the runtime configuration for MongoDBee
 *
 * Call this at application startup to configure MongoDBee behavior.
 * The configuration is merged with defaults, so you only need to specify
 * the values you want to override.
 *
 * @param config - The configuration to set
 *
 * @example
 * ```typescript
 * import { setRuntimeConfig } from "@diister/mongodbee";
 *
 * // At application startup
 * setRuntimeConfig({
 *   runtime: {
 *     schemaManagement: "managed"
 *   }
 * });
 * ```
 */
export function setRuntimeConfig(config: MongodbeeConfig): void {
  currentConfig = {
    ...DEFAULT_MONGODBEE_CONFIG,
    ...config,
    database: {
      ...DEFAULT_MONGODBEE_CONFIG.database,
      ...config.database,
      connection: {
        ...DEFAULT_MONGODBEE_CONFIG.database?.connection,
        ...config.database?.connection,
      },
    },
    paths: {
      ...DEFAULT_MONGODBEE_CONFIG.paths,
      ...config.paths,
    },
    runtime: {
      ...DEFAULT_MONGODBEE_CONFIG.runtime,
      ...config.runtime,
    },
  };
}

/**
 * Gets the current runtime configuration
 *
 * @returns The current configuration merged with defaults
 *
 * @example
 * ```typescript
 * import { getRuntimeConfig } from "@diister/mongodbee";
 *
 * const config = getRuntimeConfig();
 * console.log(config.runtime?.schemaManagement); // "auto" or "managed"
 * ```
 */
export function getRuntimeConfig(): MongodbeeConfig {
  return currentConfig;
}

/**
 * Checks if schema management is in "managed" mode
 *
 * In managed mode, collections do not automatically apply validators
 * and indexes - this is handled by migrations instead.
 *
 * @returns True if schema management is "managed", false if "auto"
 *
 * @example
 * ```typescript
 * import { isSchemaManaged } from "@diister/mongodbee";
 *
 * if (isSchemaManaged()) {
 *   // Skip auto-apply, migrations handle this
 * } else {
 *   // Apply validators and indexes automatically
 * }
 * ```
 */
export function isSchemaManaged(): boolean {
  return currentConfig.runtime?.schemaManagement === "managed";
}

/**
 * Resets the runtime configuration to defaults
 *
 * Useful for testing purposes.
 */
export function resetRuntimeConfig(): void {
  currentConfig = { ...DEFAULT_MONGODBEE_CONFIG };
  configLoaded = false;
}

/**
 * Reloads the configuration from the config file
 *
 * Call this if the config file has changed and you want to reload it.
 */
export async function reloadRuntimeConfig(): Promise<void> {
  configLoaded = false;
  currentConfig = { ...DEFAULT_MONGODBEE_CONFIG };
  await discoverAndLoadConfig();
}

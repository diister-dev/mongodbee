/**
 * @fileoverview Configuration system exports for MongoDBee migrations
 * 
 * This module provides the main exports for the configuration system,
 * including types, loading utilities, and validation functions.
 * 
 * @example
 * ```typescript
 * import { loadConfig, createConfig, validateConfigPaths } from "@diister/mongodbee/migration/config";
 * 
 * // Load configuration with auto-discovery
 * const { config, warnings } = await loadConfig();
 * 
 * // Create configuration programmatically
 * const config = createConfig({
 *   database: { uri: "mongodb://localhost:27017", database: "myapp" },
 *   paths: { migrations: "./migrations", schemas: "./schemas" }
 * });
 * 
 * // Validate paths exist
 * const errors = await validateConfigPaths(config);
 * if (errors.length > 0) {
 *   console.error("Configuration validation failed:", errors);
 * }
 * ```
 * 
 * @module
 */

// Re-export all types
export type {
  DatabaseConfig,
  PathsConfig,
  MigrationConfig,
  CliConfig,
  MigrationSystemConfig,
  EnvironmentConfig,
  ConfigResult,
  ConfigLoadOptions,
} from './types.ts';

// Re-export schemas for external validation
export {
  DatabaseConfigSchema,
  PathsConfigSchema,
  MigrationConfigSchema,
  CliConfigSchema,
  MigrationSystemConfigSchema,
  DEFAULT_CONFIG,
} from './types.ts';

import { createConfig } from './loader.ts';

// Re-export all utility functions
export {
  createConfig,
  loadConfig,
  resolveConfigPaths,
  validateConfigPaths,
  ensureConfigDirectories,
} from './loader.ts';

/**
 * Quick configuration builder for common use cases
 * 
 * @param database - Database connection details
 * @param paths - File system paths
 * @returns A basic validated configuration
 * 
 * @example
 * ```typescript
 * import { quickConfig } from "@diister/mongodbee/migration/config";
 * 
 * const config = quickConfig(
 *   { uri: "mongodb://localhost:27017", database: "myapp" },
 *   { migrations: "./migrations", schemas: "./schemas" }
 * );
 * ```
 */
export function quickConfig(
  database: { uri: string; database: string },
  paths: { migrations: string; schemas: string }
) {
  return createConfig({ database, paths });
}
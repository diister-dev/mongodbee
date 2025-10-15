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
  CliConfig,
  ConfigLoadOptions,
  ConfigResult,
  DatabaseConfig,
  EnvironmentConfig,
  MigrationConfig,
  MigrationSystemConfig,
  PathsConfig,
} from "./types.ts";

// Re-export schemas for external validation
export {
  CliConfigSchema,
  DatabaseConfigSchema,
  DEFAULT_CONFIG,
  MigrationConfigSchema,
  MigrationSystemConfigSchema,
  PathsConfigSchema,
} from "./types.ts";

// Re-export all utility functions
export {
  createConfig,
  ensureConfigDirectories,
  loadConfig,
  resolveConfigPaths,
  validateConfigPaths,
} from "./loader.ts";

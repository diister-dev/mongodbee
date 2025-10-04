/**
 * @fileoverview Migration runners system exports
 *
 * This module provides the main exports for the migration runners system,
 * including execution runners, validators, loggers, and utility functions.
 *
 * @example
 * ```typescript
 * import {
 *   createMigrationRunner,
 *   createConsoleLogger,
 *   DEFAULT_RUNNER_CONFIG
 * } from "@diister/mongodbee/migration/runners";
 *
 * const logger = createConsoleLogger('info');
 * const runner = createMigrationRunner({
 *   config: migrationConfig,
 *   applier: mongodbApplier,
 *   logger
 * }, {
 *   ...DEFAULT_RUNNER_CONFIG,
 *   dryRun: true
 * });
 *
 * const result = await runner.executeMigration(migration);
 * ```
 *
 * @module
 */

// Re-export all types
export type {
  MigrationExecutionContext,
  MigrationExecutionResult,
  MigrationLogger,
  MigrationOperationCallback,
  MigrationProgress,
  MigrationProgressCallback,
  MigrationRunner,
  MigrationRunnerConfig,
  MigrationValidator,
  ValidationResult,
} from "./execution.ts";

// Re-export all utility functions and constants
export {
  createConsoleLogger,
  createMigrationRunner,
  createNoOpLogger,
  DEFAULT_RUNNER_CONFIG,
} from "./execution.ts";

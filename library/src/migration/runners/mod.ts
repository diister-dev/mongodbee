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
  MigrationValidator,
  MigrationLogger,
  MigrationProgressCallback,
  MigrationOperationCallback,
  MigrationProgress,
  ValidationResult,
  MigrationExecutionResult,
  MigrationRunnerConfig,
  MigrationRunner,
} from './execution.ts';

// Re-export all utility functions and constants
export {
  createMigrationRunner,
  createConsoleLogger,
  createNoOpLogger,
  DEFAULT_RUNNER_CONFIG,
} from './execution.ts';

import { createMigrationRunner, createConsoleLogger, DEFAULT_RUNNER_CONFIG } from './execution.ts';
import type { MigrationExecutionContext, MigrationRunnerConfig } from './execution.ts';

/**
 * Convenience function to create a basic migration runner
 * 
 * @param applier - The migration applier to use
 * @param config - System configuration
 * @param options - Optional runner configuration overrides
 * @returns A configured migration runner
 * 
 * @example
 * ```typescript
 * import { createBasicRunner } from "@diister/mongodbee/migration/runners";
 * 
 * const runner = createBasicRunner(mongoApplier, systemConfig, {
 *   validateBeforeExecution: true,
 *   operationTimeout: 60000
 * });
 * 
 * const result = await runner.executeMigration(migration);
 * ```
 */
export function createBasicRunner(
  applier: MigrationExecutionContext['applier'],
  config: MigrationExecutionContext['config'],
  options?: Partial<MigrationRunnerConfig>
) {
  return createMigrationRunner({
    config,
    applier,
    logger: createConsoleLogger('info'),
  }, {
    ...DEFAULT_RUNNER_CONFIG,
    ...options,
  });
}
/**
 * @fileoverview Migration execution runners for coordinating migrations
 *
 * This module provides high-level runners that coordinate migration execution,
 * validation, logging, and error handling. Runners use a functional approach
 * and integrate all migration system components.
 *
 * @example
 * ```typescript
 * import { createMigrationRunner, runMigration } from "@diister/mongodbee/migration/runners";
 *
 * const runner = createMigrationRunner({
 *   config: migrationConfig,
 *   applier: mongodbApplier,
 *   validator: schemaValidator
 * });
 *
 * const result = await runMigration(runner, migrationDefinition);
 * console.log(`Applied ${result.appliedOperations} operations`);
 * ```
 *
 * @module
 */

import type {
  MigrationApplier,
  MigrationDefinition,
  MigrationRule,
  MigrationState,
} from "../types.ts";
import type { MigrationSystemConfig } from "../config/types.ts";
import { migrationBuilder } from "../builder.ts";

/**
 * Context for migration execution
 *
 * Contains all the information needed to execute a migration,
 * including configuration, appliers, validators, and state.
 */
export type MigrationExecutionContext = {
  /** Migration system configuration */
  config: MigrationSystemConfig;

  /** The applier to use for executing operations */
  applier: MigrationApplier;

  /** Optional validator for pre-execution validation */
  validator?: MigrationValidator;

  /** Optional logger for execution events */
  logger?: MigrationLogger;

  /** Optional progress callback */
  onProgress?: MigrationProgressCallback;

  /** Optional operation callback for custom handling */
  onOperation?: MigrationOperationCallback;
};

/**
 * Validator function type for migration validation
 */
export type MigrationValidator = {
  /** Validates a complete migration definition */
  validateMigration: (
    definition: MigrationDefinition,
  ) => Promise<ValidationResult>;

  /** Validates individual operations before execution */
  validateOperation: (
    operation: MigrationRule,
    context: MigrationExecutionContext,
  ) => Promise<ValidationResult>;

  /** Validates the final state after migration */
  validateState: (
    state: MigrationState,
    context: MigrationExecutionContext,
  ) => Promise<ValidationResult>;
};

/**
 * Logger interface for migration events
 */
export type MigrationLogger = {
  /** Log informational messages */
  info: (message: string, data?: Record<string, unknown>) => void;

  /** Log warning messages */
  warn: (message: string, data?: Record<string, unknown>) => void;

  /** Log error messages */
  error: (
    message: string,
    error?: Error,
    data?: Record<string, unknown>,
  ) => void;

  /** Log debug messages */
  debug: (message: string, data?: Record<string, unknown>) => void;
};

/**
 * Progress callback for migration execution
 */
export type MigrationProgressCallback = (progress: MigrationProgress) => void;

/**
 * Operation callback for custom operation handling
 */
export type MigrationOperationCallback = (
  operation: MigrationRule,
  phase: "before" | "after" | "error",
  context: MigrationExecutionContext,
  error?: Error,
) => Promise<void> | void;

/**
 * Progress information during migration execution
 */
export type MigrationProgress = {
  /** Current migration being executed */
  migration: MigrationDefinition;

  /** Total number of operations to execute */
  totalOperations: number;

  /** Number of operations completed */
  completedOperations: number;

  /** Current operation being executed */
  currentOperation?: MigrationRule;

  /** Current phase of execution */
  phase: "validation" | "execution" | "cleanup" | "completed" | "error";

  /** Optional message about current progress */
  message?: string;

  /** Execution start time */
  startTime: Date;

  /** Estimated completion time (if available) */
  estimatedCompletion?: Date;
};

/**
 * Validation result from validators
 */
export type ValidationResult = {
  /** Whether validation passed */
  success: boolean;

  /** Validation errors (blocking) */
  errors: string[];

  /** Validation warnings (non-blocking) */
  warnings: string[];

  /** Additional validation data */
  data?: Record<string, unknown>;
};

/**
 * Result of migration execution
 */
export type MigrationExecutionResult = {
  /** Whether the migration executed successfully */
  success: boolean;

  /** The migration that was executed */
  migration: MigrationDefinition;

  /** Final state after migration */
  finalState: MigrationState;

  /** Number of operations that were applied */
  appliedOperations: number;

  /** Total execution time in milliseconds */
  executionTime: number;

  /** Validation results */
  validation: ValidationResult;

  /** Any execution errors */
  errors: string[];

  /** Any execution warnings */
  warnings: string[];

  /** Additional execution data */
  metadata?: Record<string, unknown>;
};

/**
 * Configuration for migration runner
 */
export type MigrationRunnerConfig = {
  /** Whether to validate before execution */
  validateBeforeExecution?: boolean;

  /** Whether to validate after execution */
  validateAfterExecution?: boolean;

  /** Whether to continue on validation warnings */
  continueOnWarnings?: boolean;

  /** Whether to continue on operation errors */
  continueOnErrors?: boolean;

  /** Timeout for individual operations in milliseconds */
  operationTimeout?: number;

  /** Maximum number of retry attempts for failed operations */
  maxRetries?: number;

  /** Delay between retry attempts in milliseconds */
  retryDelay?: number;

  /** Whether to create backups before destructive operations */
  createBackups?: boolean;

  /** Whether to run in dry-run mode (simulation only) */
  dryRun?: boolean;
};

/**
 * Migration runner instance
 */
export type MigrationRunner = {
  /** The execution context */
  context: MigrationExecutionContext;

  /** Runner configuration */
  config: MigrationRunnerConfig;

  /** Execute a single migration */
  executeMigration: (
    definition: MigrationDefinition,
  ) => Promise<MigrationExecutionResult>;

  /** Execute multiple migrations in sequence */
  executeMigrations: (
    definitions: MigrationDefinition[],
  ) => Promise<MigrationExecutionResult[]>;

  /** Rollback a migration */
  rollbackMigration: (
    definition: MigrationDefinition,
  ) => Promise<MigrationExecutionResult>;

  /** Validate a migration without executing */
  validateMigration: (
    definition: MigrationDefinition,
  ) => Promise<ValidationResult>;
};

/**
 * Default runner configuration
 */
export const DEFAULT_RUNNER_CONFIG: MigrationRunnerConfig = {
  validateBeforeExecution: true,
  validateAfterExecution: true,
  continueOnWarnings: true,
  continueOnErrors: false,
  operationTimeout: 30000,
  maxRetries: 3,
  retryDelay: 1000,
  createBackups: true,
  dryRun: false,
};

/**
 * Creates a default console logger
 */
export function createConsoleLogger(
  level: "debug" | "info" | "warn" | "error" = "info",
): MigrationLogger {
  const levels = { debug: 0, info: 1, warn: 2, error: 3 };
  const currentLevel = levels[level];

  return {
    debug: (message: string, data?: Record<string, unknown>) => {
      if (currentLevel <= levels.debug) {
        console.debug(`[DEBUG] ${message}`, data || "");
      }
    },
    info: (message: string, data?: Record<string, unknown>) => {
      if (currentLevel <= levels.info) {
        console.info(`[INFO] ${message}`, data || "");
      }
    },
    warn: (message: string, data?: Record<string, unknown>) => {
      if (currentLevel <= levels.warn) {
        console.warn(`[WARN] ${message}`, data || "");
      }
    },
    error: (message: string, error?: Error, data?: Record<string, unknown>) => {
      if (currentLevel <= levels.error) {
        console.error(`[ERROR] ${message}`, error?.message || "", data || "");
      }
    },
  };
}

/**
 * Creates a no-op logger that discards all messages
 */
export function createNoOpLogger(): MigrationLogger {
  return {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
  };
}

/**
 * Executes a single operation with retry logic and error handling
 */
async function executeOperationWithRetry(
  operation: MigrationRule,
  applier: MigrationApplier,
  config: MigrationRunnerConfig,
  logger?: MigrationLogger,
): Promise<{ success: boolean; error?: Error; attempts: number }> {
  const maxAttempts = (config.maxRetries || 0) + 1;
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      logger?.debug(`Executing operation ${operation.type}`, {
        attempt,
        maxAttempts,
      });

      if (config.operationTimeout) {
        // Create a timeout promise with cleanup
        let timeoutId: number | undefined;
        const timeoutPromise = new Promise<never>((_, reject) => {
          timeoutId = setTimeout(
            () =>
              reject(
                new Error(
                  `Operation timeout after ${config.operationTimeout}ms`,
                ),
              ),
            config.operationTimeout,
          );
        });

        try {
          // Race between operation and timeout
          await Promise.race([
            applier.applyOperation(operation),
            timeoutPromise,
          ]);
        } finally {
          // Always clear the timeout to prevent leaks
          if (timeoutId !== undefined) {
            clearTimeout(timeoutId);
          }
        }
      } else {
        await applier.applyOperation(operation);
      }

      logger?.debug(`Operation ${operation.type} completed successfully`, {
        attempt,
      });
      return { success: true, attempts: attempt };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      logger?.warn(`Operation ${operation.type} failed on attempt ${attempt}`, {
        error: lastError.message,
      });

      // If this isn't the last attempt, wait before retrying
      if (attempt < maxAttempts && config.retryDelay) {
        await new Promise((resolve) => setTimeout(resolve, config.retryDelay));
      }
    }
  }

  return { success: false, error: lastError, attempts: maxAttempts };
}

/**
 * Creates a migration runner with the specified context and configuration
 *
 * @param context - Execution context with appliers, validators, etc.
 * @param config - Runner configuration options
 * @returns A configured migration runner
 *
 * @example
 * ```typescript
 * const runner = createMigrationRunner({
 *   config: systemConfig,
 *   applier: mongoApplier,
 *   validator: chainValidator,
 *   logger: createConsoleLogger('info')
 * }, {
 *   validateBeforeExecution: true,
 *   continueOnWarnings: true,
 *   operationTimeout: 30000
 * });
 * ```
 */
export function createMigrationRunner(
  context: MigrationExecutionContext,
  config: MigrationRunnerConfig = DEFAULT_RUNNER_CONFIG,
): MigrationRunner {
  const mergedConfig = { ...DEFAULT_RUNNER_CONFIG, ...config };
  const logger = context.logger || createNoOpLogger();

  return {
    context,
    config: mergedConfig,

    async executeMigration(
      definition: MigrationDefinition,
    ): Promise<MigrationExecutionResult> {
      const startTime = Date.now();
      const errors: string[] = [];
      const warnings: string[] = [];
      let appliedOperations = 0;

      logger.info(`Starting migration: ${definition.name}`, {
        id: definition.id,
      });

      try {
        // Build migration state by calling migrate with a real builder
        const builder = migrationBuilder({ schemas: definition.schemas });
        const state = definition.migrate(builder);
        const operations = state.operations;

        // Create progress tracker
        const progress: MigrationProgress = {
          migration: definition,
          totalOperations: operations.length,
          completedOperations: 0,
          phase: "validation",
          startTime: new Date(startTime),
        };

        // Validation phase
        if (mergedConfig.validateBeforeExecution && context.validator) {
          logger.info("Validating migration before execution");
          progress.phase = "validation";
          context.onProgress?.({ ...progress });

          const validation = await context.validator.validateMigration(
            definition,
          );

          if (!validation.success) {
            errors.push(...validation.errors);
            if (!mergedConfig.continueOnErrors) {
              return {
                success: false,
                migration: definition,
                finalState: state,
                appliedOperations: 0,
                executionTime: Date.now() - startTime,
                validation,
                errors,
                warnings: [...warnings, ...validation.warnings],
              };
            }
          }

          warnings.push(...validation.warnings);
        }

        // Execution phase
        progress.phase = "execution";
        context.onProgress?.({ ...progress });

        if (!mergedConfig.dryRun) {
          logger.info(`Executing ${operations.length} operations`);

          for (let i = 0; i < operations.length; i++) {
            const operation = operations[i];
            progress.currentOperation = operation;
            progress.completedOperations = i;
            context.onProgress?.({ ...progress });

            // Pre-operation callback
            await context.onOperation?.(operation, "before", context);

            // Validate individual operation if validator is available
            if (context.validator) {
              const opValidation = await context.validator.validateOperation(
                operation,
                context,
              );
              if (!opValidation.success) {
                const opErrors = opValidation.errors.map((err) =>
                  `Operation ${i + 1} (${operation.type}): ${err}`
                );
                errors.push(...opErrors);

                if (!mergedConfig.continueOnErrors) {
                  await context.onOperation?.(
                    operation,
                    "error",
                    context,
                    new Error(opErrors.join(", ")),
                  );
                  break;
                }
              }
              warnings.push(...opValidation.warnings);
            }

            // Execute operation with retry logic
            const result = await executeOperationWithRetry(
              operation,
              context.applier,
              mergedConfig,
              logger,
            );

            if (result.success) {
              appliedOperations++;
              logger.debug(
                `Operation ${i + 1}/${operations.length} completed`,
                { type: operation.type },
              );
              await context.onOperation?.(operation, "after", context);
            } else {
              const error = result.error ||
                new Error("Unknown operation error");
              errors.push(
                `Operation ${
                  i + 1
                } (${operation.type}) failed after ${result.attempts} attempts: ${error.message}`,
              );
              await context.onOperation?.(operation, "error", context, error);

              if (!mergedConfig.continueOnErrors) {
                break;
              }
            }
          }
        } else {
          logger.info(
            "Dry-run mode: simulating operations without applying changes",
          );
          appliedOperations = operations.length; // In dry-run, we "simulate" all operations
        }

        // Post-execution validation
        let finalValidation: ValidationResult = {
          success: true,
          errors: [],
          warnings: [],
        };
        if (mergedConfig.validateAfterExecution && context.validator) {
          logger.info("Validating migration after execution");
          finalValidation = await context.validator.validateState(
            state,
            context,
          );
          errors.push(...finalValidation.errors);
          warnings.push(...finalValidation.warnings);
        }

        progress.phase = errors.length > 0 ? "error" : "completed";
        progress.completedOperations = appliedOperations;
        context.onProgress?.({ ...progress });

        const success = errors.length === 0;
        const executionTime = Date.now() - startTime;

        logger.info(`Migration ${success ? "completed" : "failed"}`, {
          appliedOperations,
          executionTime,
          errors: errors.length,
          warnings: warnings.length,
        });

        return {
          success,
          migration: definition,
          finalState: state,
          appliedOperations,
          executionTime,
          validation: finalValidation,
          errors,
          warnings,
          metadata: {
            dryRun: mergedConfig.dryRun,
            totalOperations: operations.length,
          },
        };
      } catch (error) {
        const executionError = error instanceof Error
          ? error
          : new Error(String(error));
        errors.push(`Migration execution failed: ${executionError.message}`);
        logger.error("Migration execution failed", executionError);

        const builder = migrationBuilder({ schemas: definition.schemas });

        return {
          success: false,
          migration: definition,
          finalState: definition.migrate(builder),
          appliedOperations,
          executionTime: Date.now() - startTime,
          validation: { success: false, errors, warnings },
          errors,
          warnings,
        };
      }
    },

    async executeMigrations(
      definitions: MigrationDefinition[],
    ): Promise<MigrationExecutionResult[]> {
      const results: MigrationExecutionResult[] = [];

      logger.info(
        `Starting batch migration of ${definitions.length} migrations`,
      );

      for (let i = 0; i < definitions.length; i++) {
        const definition = definitions[i];
        logger.info(
          `Executing migration ${
            i + 1
          }/${definitions.length}: ${definition.name}`,
        );

        const result = await this.executeMigration(definition);
        results.push(result);

        // Stop on first failure if not continuing on errors
        if (!result.success && !mergedConfig.continueOnErrors) {
          logger.error(
            `Stopping batch execution due to failure in migration: ${definition.name}`,
          );
          break;
        }
      }

      const successful = results.filter((r) => r.success).length;
      logger.info(
        `Batch migration completed: ${successful}/${definitions.length} successful`,
      );

      return results;
    },

    async rollbackMigration(
      definition: MigrationDefinition,
    ): Promise<MigrationExecutionResult> {
      logger.info(`Starting rollback of migration: ${definition.name}`);

      // Build the migration state to get operations
      const builder = migrationBuilder({ schemas: definition.schemas });
      const state = definition.migrate(builder);
      const operations = state.operations.slice().reverse(); // Reverse order for rollback

      // Create a temporary definition for rollback
      const rollbackDefinition: MigrationDefinition = {
        ...definition,
        id: `${definition.id}_rollback`,
        name: `Rollback: ${definition.name}`,
        migrate: () => ({
          ...state,
          operations: operations,
        }),
      };

      // Use reverse operations for rollback
      const originalApplier = context.applier;
      const rollbackApplier: MigrationApplier = {
        applyOperation: (operation) =>
          originalApplier.applyReverseOperation(operation),
        applyReverseOperation: (operation) =>
          originalApplier.applyOperation(operation),
      };

      const rollbackContext = {
        ...context,
        applier: rollbackApplier,
      };

      const rollbackRunner = createMigrationRunner(
        rollbackContext,
        mergedConfig,
      );
      return await rollbackRunner.executeMigration(rollbackDefinition);
    },

    async validateMigration(
      definition: MigrationDefinition,
    ): Promise<ValidationResult> {
      if (!context.validator) {
        return {
          success: true,
          errors: [],
          warnings: ["No validator configured - skipping validation"],
        };
      }

      logger.info(`Validating migration: ${definition.name}`);
      return await context.validator.validateMigration(definition);
    },
  };
}

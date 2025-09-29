/**
 * @fileoverview Simulation-based migration validator
 * 
 * This module provides a validator that uses the SimulationApplier to validate
 * migrations before applying them to real databases. It ensures migrations
 * can be executed and reversed properly in a safe, in-memory environment.
 * 
 * @example
 * ```typescript
 * import { createSimulationValidator } from "@diister/mongodbee/migration/validators";
 * 
 * const validator = createSimulationValidator();
 * const result = await validator.validateMigration(migrationDefinition);
 * 
 * if (!result.success) {
 *   console.error("Migration validation failed:", result.errors);
 * }
 * ```
 * 
 * @module
 */

import type {
  MigrationDefinition,
  MigrationState,
  MigrationRule,
} from '../types.ts';
import type {
  MigrationValidator,
  MigrationExecutionContext,
  ValidationResult,
} from '../runners/execution.ts';
import { 
  type SimulationApplier, 
  createSimulationApplier, 
  createEmptyDatabaseState,
  compareDatabaseStates,
  type SimulationDatabaseState 
} from '../appliers/simulation.ts';
import { migrationBuilder } from '../builder.ts';

/**
 * Configuration options for the simulation validator
 */
export interface SimulationValidatorOptions {
  /** Whether to validate that migrations are reversible */
  validateReversibility?: boolean;
  
  /** Whether to use strict validation in the simulation applier */
  strictValidation?: boolean;
  
  /** Whether to track operation history during simulation */
  trackHistory?: boolean;
  
  /** Maximum number of operations to validate (for performance) */
  maxOperations?: number;
  
  /** Whether to validate individual operations */
  validateOperations?: boolean;
}

/**
 * Default validator configuration
 */
export const DEFAULT_SIMULATION_VALIDATOR_OPTIONS: SimulationValidatorOptions = {
  validateReversibility: true,
  strictValidation: true,
  trackHistory: true,
  maxOperations: 1000,
  validateOperations: true,
};

/**
 * Simulation-based migration validator
 * 
 * This validator uses the SimulationApplier to validate migrations in an
 * in-memory environment before they are applied to real databases.
 */
export class SimulationValidator implements MigrationValidator {
  private readonly applier: SimulationApplier;
  private readonly options: SimulationValidatorOptions;

  constructor(options: SimulationValidatorOptions = {}) {
    this.options = { ...DEFAULT_SIMULATION_VALIDATOR_OPTIONS, ...options };
    this.applier = createSimulationApplier({
      strictValidation: this.options.strictValidation,
      trackHistory: this.options.trackHistory,
    });
  }

  /**
   * Validates a complete migration definition
   * 
   * @param definition - The migration definition to validate
   * @returns Validation result with success status, errors, and warnings
   */
  validateMigration(definition: MigrationDefinition): Promise<ValidationResult> {
    const errors: string[] = [];
    const warnings: string[] = [];

    try {
      // Build migration state
      const builder = migrationBuilder({ schemas: definition.schemas });
      const state = definition.migrate(builder);
      const operations = state.operations;

      // Check operation count limit
      if (this.options.maxOperations && operations.length > this.options.maxOperations) {
        warnings.push(`Migration has ${operations.length} operations, which exceeds the recommended limit of ${this.options.maxOperations}`);
      }

      // Validate that migration has operations
      if (operations.length === 0) {
        warnings.push('Migration has no operations');
        return Promise.resolve({
          success: true,
          errors,
          warnings,
          data: { operationCount: 0 }
        });
      }

      // Test forward execution
      let currentState = createEmptyDatabaseState();
      const forwardErrors: string[] = [];

      for (let i = 0; i < operations.length; i++) {
        const operation = operations[i];
        try {
          currentState = this.applier.applyOperation(currentState, operation);
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          forwardErrors.push(`Operation ${i + 1} (${operation.type}): ${errorMessage}`);
        }
      }

      if (forwardErrors.length > 0) {
        errors.push('Forward migration simulation failed:');
        errors.push(...forwardErrors.map(err => `  ${err}`));
      }

      // Test reversibility if enabled and forward execution succeeded
      if (this.options.validateReversibility && forwardErrors.length === 0) {
        const reverseErrors = this.validateReversibility(state, currentState);
        if (reverseErrors.length > 0) {
          errors.push('Migration reversibility validation failed:');
          errors.push(...reverseErrors.map(err => `  ${err}`));
        }
      }

      // Check for irreversible properties
      if (state.hasProperty('irreversible')) {
        if (this.options.validateReversibility) {
          warnings.push('Migration is marked as irreversible but reversibility validation is enabled');
        } else {
          warnings.push('Migration is marked as irreversible');
        }
      }

      return Promise.resolve({
        success: errors.length === 0,
        errors,
        warnings,
        data: {
          operationCount: operations.length,
          hasIrreversibleProperty: state.hasProperty('irreversible'),
          simulationCompleted: true
        }
      });

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      errors.push(`Migration validation failed: ${errorMessage}`);
      
      return Promise.resolve({
        success: false,
        errors,
        warnings,
        data: { simulationCompleted: false }
      });
    }
  }

  /**
   * Validates individual operations before execution
   * 
   * @param operation - The operation to validate
   * @param _context - Execution context (not used in simulation validation)
   * @returns Validation result
   */
  validateOperation(operation: MigrationRule, _context: MigrationExecutionContext): Promise<ValidationResult> {
    if (!this.options.validateOperations) {
      return Promise.resolve({
        success: true,
        errors: [],
        warnings: [],
        data: { operationValidated: false }
      });
    }

    const errors: string[] = [];
    const warnings: string[] = [];

    try {
      // Test operation in isolation
      const testState = createEmptyDatabaseState();
      this.applier.applyOperation(testState, operation);

      return Promise.resolve({
        success: true,
        errors,
        warnings,
        data: { operationValidated: true }
      });

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      errors.push(`Operation validation failed: ${errorMessage}`);
      
      return Promise.resolve({
        success: false,
        errors,
        warnings,
        data: { operationValidated: false }
      });
    }
  }

  /**
   * Validates the final state after migration
   * 
   * @param state - The migration state to validate
   * @param _context - Execution context (not used in simulation validation)
   * @returns Validation result
   */
  validateState(state: MigrationState, _context: MigrationExecutionContext): Promise<ValidationResult> {
    const errors: string[] = [];
    const warnings: string[] = [];

    try {
      // Basic state validation
      if (state.operations.length === 0) {
        warnings.push('Migration state has no operations');
      }

      // Check for conflicting properties
      const properties = state.properties.map(p => p.type);
      if (properties.includes('irreversible') && this.options.validateReversibility) {
        warnings.push('Migration is marked as irreversible but reversibility validation was requested');
      }

      return Promise.resolve({
        success: errors.length === 0,
        errors,
        warnings,
        data: {
          operationCount: state.operations.length,
          properties: properties
        }
      });

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      errors.push(`State validation failed: ${errorMessage}`);
      
      return Promise.resolve({
        success: false,
        errors,
        warnings,
        data: { stateValidated: false }
      });
    }
  }

  /**
   * Validates that a migration can be reversed properly
   * 
   * @private
   * @param state - The migration state with operations
   * @param forwardState - The database state after forward execution
   * @returns Array of error messages (empty if validation passes)
   */
  private validateReversibility(state: MigrationState, forwardState: SimulationDatabaseState): string[] {
    const errors: string[] = [];
    const operations = state.operations;
    
    try {
      // Apply reverse operations in reverse order
      let reverseState = JSON.parse(JSON.stringify(forwardState)) as SimulationDatabaseState;
      
      for (let i = operations.length - 1; i >= 0; i--) {
        const operation = operations[i];
        try {
          reverseState = this.applier.applyReverseOperation(reverseState, operation);
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          errors.push(`Reverse operation ${i + 1} (${operation.type}): ${errorMessage}`);
          return errors; // Stop on first reverse error
        }
      }

      // Compare final state with initial state
      const initialState = createEmptyDatabaseState();
      const statesMatch = compareDatabaseStates(reverseState, initialState);
      
      if (!statesMatch) {
        errors.push('Migration is not fully reversible - final state does not match initial state');
        
        // Provide more detailed comparison if possible
        const initialCollections = Object.keys(initialState.collections || {});
        const finalCollections = Object.keys(reverseState.collections || {});
        
        if (finalCollections.length !== initialCollections.length) {
          errors.push(`Collection count mismatch: initial=${initialCollections.length}, final=${finalCollections.length}`);
        }

        const extraCollections = finalCollections.filter(name => !initialCollections.includes(name));
        if (extraCollections.length > 0) {
          errors.push(`Extra collections after reversal: ${extraCollections.join(', ')}`);
        }

        const missingCollections = initialCollections.filter(name => !finalCollections.includes(name));
        if (missingCollections.length > 0) {
          errors.push(`Missing collections after reversal: ${missingCollections.join(', ')}`);
        }
      }

      return errors;

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      errors.push(`Reversibility validation failed: ${errorMessage}`);
      return errors;
    }
  }
}

/**
 * Factory function to create a simulation validator
 * 
 * @param options - Configuration options for the validator
 * @returns A new simulation validator instance
 * 
 * @example
 * ```typescript
 * import { createSimulationValidator } from "@diister/mongodbee/migration/validators";
 * 
 * const validator = createSimulationValidator({
 *   validateReversibility: true,
 *   strictValidation: true,
 *   maxOperations: 500
 * });
 * 
 * const result = await validator.validateMigration(migration);
 * ```
 */
export function createSimulationValidator(options?: SimulationValidatorOptions): SimulationValidator {
  return new SimulationValidator(options);
}

/**
 * Utility function to validate a single migration definition quickly
 * 
 * @param definition - Migration definition to validate
 * @param options - Validator options
 * @returns Validation result
 * 
 * @example
 * ```typescript
 * import { validateMigrationWithSimulation } from "@diister/mongodbee/migration/validators";
 * 
 * const result = await validateMigrationWithSimulation(migration, {
 *   validateReversibility: false,
 *   strictValidation: true
 * });
 * 
 * if (result.success) {
 *   console.log("Migration is valid!");
 * }
 * ```
 */
export async function validateMigrationWithSimulation(
  definition: MigrationDefinition, 
  options?: SimulationValidatorOptions
): Promise<ValidationResult> {
  const validator = createSimulationValidator(options);
  return await validator.validateMigration(definition);
}
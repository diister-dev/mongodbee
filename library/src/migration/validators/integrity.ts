/**
 * @fileoverview Integrity validation for migration operations and states
 * 
 * This module provides validation for migration operations to ensure they:
 * - Follow correct operation patterns and structure
 * - Can be safely applied and reversed
 * - Don't conflict with existing database state
 * - Meet operational integrity requirements
 * 
 * @example
 * ```typescript
 * import { createIntegrityValidator, validateMigrationState } from "@diister/mongodbee/migration/validators";
 * 
 * const validator = createIntegrityValidator({ strictMode: true });
 * const migrationState = { operations: [...] };
 * 
 * const result = await validator.validateMigrationState(migrationState);
 * if (!result.isValid) {
 *   console.error('Integrity validation failed:', result.errors);
 * }
 * ```
 * 
 * @module
 */

import type { MigrationState, MigrationRule } from '../types.ts';
import type { SimulationDatabaseState, SimulationApplier } from '../appliers/simulation.ts';
import { createSimulationApplier, createEmptyDatabaseState } from '../appliers/simulation.ts';

/**
 * Result of integrity validation
 */
export interface IntegrityValidationResult {
  /** Whether the migration state passes integrity validation */
  isValid: boolean;
  
  /** List of validation errors found */
  errors: string[];
  
  /** List of validation warnings */
  warnings: string[];
  
  /** Additional metadata about the validation */
  metadata: {
    /** Total number of operations validated */
    totalOperations: number;
    
    /** Number of operations by type */
    operationTypes: Record<string, number>;
    
    /** Collections affected by operations */
    affectedCollections: string[];
    
    /** Whether operations are reversible */
    isReversible: boolean;
    
    /** Validation performance metrics */
    performance: {
      /** Time taken for validation in milliseconds */
      duration: number;
      
      /** Time taken for simulation in milliseconds */
      simulationDuration?: number;
    };
  };
}

/**
 * Configuration options for integrity validation
 */
export interface IntegrityValidatorOptions {
  /** Whether to perform strict validation (default: true) */
  strictMode?: boolean;
  
  /** Whether to simulate operations for deeper validation (default: true) */
  runSimulation?: boolean;
  
  /** Whether to validate reversibility by testing reverse operations (default: true) */
  validateReversibility?: boolean;
  
  /** Maximum number of operations to process (default: unlimited) */
  maxOperations?: number;
  
  /** Timeout for validation in milliseconds (default: 30000) */
  timeout?: number;
}

/**
 * Integrity validator for migration operations and states
 */
export class IntegrityValidator {
  private simulationApplier: SimulationApplier;

  constructor(private options: IntegrityValidatorOptions = {}) {
    // Set defaults
    this.options = {
      strictMode: true,
      runSimulation: true,
      validateReversibility: true,
      timeout: 30000,
      ...options,
    };

    // Create simulation applier for testing
    this.simulationApplier = createSimulationApplier({
      strictValidation: this.options.strictMode,
      trackHistory: true,
    });
  }

  /**
   * Validates a migration state for operational integrity
   * 
   * @param migrationState - The migration state to validate
   * @param initialState - Optional initial database state for simulation
   * @returns Validation result with errors, warnings, and metadata
   * 
   * @example
   * ```typescript
   * const validator = new IntegrityValidator({ strictMode: true });
   * const result = validator.validateMigrationState(migrationState);
   * 
   * if (result.isValid) {
   *   console.log('Migration state is valid');
   * } else {
   *   console.error('Validation errors:', result.errors);
   * }
   * ```
   */
  validateMigrationState(
    migrationState: MigrationState,
    initialState?: SimulationDatabaseState
  ): IntegrityValidationResult {
    const startTime = Date.now();
    const errors: string[] = [];
    const warnings: string[] = [];
    const operationTypes: Record<string, number> = {};
    const affectedCollections = new Set<string>();

    // Basic validations
    this.validateBasicStructure(migrationState, errors);
    
    // Analyze operations
    for (const operation of migrationState.operations) {
      this.validateOperation(operation, errors, warnings);
      
      // Count operation types
      operationTypes[operation.type] = (operationTypes[operation.type] || 0) + 1;
      
      // Track affected collections
      if ('collectionName' in operation && operation.collectionName) {
        affectedCollections.add(operation.collectionName);
      }
    }

    // Check operation limits
    if (this.options.maxOperations && migrationState.operations.length > this.options.maxOperations) {
      errors.push(`Migration exceeds maximum operations limit: ${migrationState.operations.length} > ${this.options.maxOperations}`);
    }

    // Simulation-based validation
    let simulationDuration = 0;
    let isReversible = false;
    
    if (this.options.runSimulation && errors.length === 0) {
      const simulationStart = Date.now();
      
      try {
        const simulationResult = this.runSimulation(
          migrationState,
          initialState || createEmptyDatabaseState(),
          errors,
          warnings
        );
        
        isReversible = simulationResult.isReversible;
        simulationDuration = Date.now() - simulationStart;
      } catch (error) {
        errors.push(`Simulation failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
      }
    }

    const totalDuration = Date.now() - startTime;

    return {
      isValid: errors.length === 0,
      errors,
      warnings,
      metadata: {
        totalOperations: migrationState.operations.length,
        operationTypes,
        affectedCollections: Array.from(affectedCollections),
        isReversible,
        performance: {
          duration: totalDuration,
          simulationDuration: simulationDuration > 0 ? simulationDuration : undefined,
        },
      }
    };
  }

  /**
   * Validates basic structure of migration state
   * 
   * @private
   */
  private validateBasicStructure(migrationState: MigrationState, errors: string[]): void {
    if (!migrationState || typeof migrationState !== 'object') {
      errors.push('Migration state must be an object');
      return;
    }

    if (!Array.isArray(migrationState.operations)) {
      errors.push('Migration state must have operations array');
      return;
    }

    if (migrationState.operations.length === 0) {
      errors.push('Migration state cannot have empty operations array');
    }
  }

  /**
   * Validates a single migration operation
   * 
   * @private
   */
  private validateOperation(operation: MigrationRule, errors: string[], warnings: string[]): void {
    if (!operation || typeof operation !== 'object') {
      errors.push('Operation must be an object');
      return;
    }

    if (!operation.type || typeof operation.type !== 'string') {
      errors.push('Operation must have a valid type');
      return;
    }

    // Type-specific validations
    switch (operation.type) {
      case 'create_collection':
        this.validateCreateCollectionOperation(operation, errors, warnings);
        break;
        
      case 'seed_collection':
        this.validateSeedCollectionOperation(operation, errors, warnings);
        break;
        
      case 'transform_collection':
        this.validateTransformCollectionOperation(operation, errors, warnings);
        break;
        
      default:
        errors.push(`Unknown operation type: ${(operation as { type?: string }).type || 'unknown'}`);
    }
  }

  /**
   * Validates create collection operation
   * 
   * @private
   */
  private validateCreateCollectionOperation(
    operation: MigrationRule,
    errors: string[],
    warnings: string[]
  ): void {
    if (!('collectionName' in operation) || !operation.collectionName) {
      errors.push('create_collection operation must have collectionName');
      return;
    }

    if (typeof operation.collectionName !== 'string') {
      errors.push('create_collection collectionName must be a string');
    }

    if (operation.collectionName.length === 0) {
      errors.push('create_collection collectionName cannot be empty');
    }

    // Collection name pattern validation
    if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(operation.collectionName)) {
      warnings.push(`Collection name "${operation.collectionName}" should start with letter and contain only letters, numbers, and underscores`);
    }
  }

  /**
   * Validates seed collection operation
   * 
   * @private
   */
  private validateSeedCollectionOperation(
    operation: MigrationRule,
    errors: string[],
    warnings: string[]
  ): void {
    if (!('collectionName' in operation) || !operation.collectionName) {
      errors.push('seed_collection operation must have collectionName');
      return;
    }

    if (!('documents' in operation) || !Array.isArray(operation.documents)) {
      errors.push('seed_collection operation must have documents array');
      return;
    }

    if (operation.documents.length === 0) {
      warnings.push('seed_collection operation has no documents to insert');
    }

    // Validate documents structure
    for (let i = 0; i < operation.documents.length; i++) {
      const doc = operation.documents[i];
      
      if (doc === null || doc === undefined) {
        errors.push(`seed_collection document ${i} is null or undefined`);
      } else if (typeof doc === 'object' && '_id' in doc && doc._id === null) {
        errors.push(`seed_collection document ${i} has null _id`);
      }
    }
  }

  /**
   * Validates transform collection operation
   * 
   * @private
   */
  private validateTransformCollectionOperation(
    operation: MigrationRule,
    errors: string[],
    warnings: string[]
  ): void {
    if (!('collectionName' in operation) || !operation.collectionName) {
      errors.push('transform_collection operation must have collectionName');
      return;
    }

    if (!('up' in operation) || typeof operation.up !== 'function') {
      errors.push('transform_collection operation must have up function');
    }

    if (!('down' in operation) || typeof operation.down !== 'function') {
      errors.push('transform_collection operation must have down function');
    } else if (!this.options.validateReversibility) {
      warnings.push('transform_collection reversibility not validated (validateReversibility disabled)');
    }
  }

  /**
   * Runs simulation to validate operations can be applied and reversed
   * 
   * @private
   */
  private runSimulation(
    migrationState: MigrationState,
    initialState: SimulationDatabaseState,
    errors: string[],
    warnings: string[]
  ): { isReversible: boolean } {
    try {
      // Apply all operations
      let currentState = { ...initialState };
      
      for (const operation of migrationState.operations) {
        currentState = this.simulationApplier.applyOperation(currentState, operation);
      }

      // Test reversibility if enabled
      let isReversible = true;
      
      if (this.options.validateReversibility) {
        try {
          // Reverse all operations in reverse order
          let reverseState = { ...currentState };
          
          for (let i = migrationState.operations.length - 1; i >= 0; i--) {
            const operation = migrationState.operations[i];
            reverseState = this.simulationApplier.applyReverseOperation(reverseState, operation);
          }

          // Compare with initial state (excluding operation history)
          const cleanInitial = { ...initialState };
          const cleanReverse = { ...reverseState };
          delete cleanInitial.operationHistory;
          delete cleanReverse.operationHistory;
          
          if (JSON.stringify(cleanInitial) !== JSON.stringify(cleanReverse)) {
            isReversible = false;
            warnings.push('Migration operations are not fully reversible');
          }
        } catch (error) {
          isReversible = false;
          warnings.push(`Reversibility test failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
      }

      return { isReversible };
    } catch (error) {
      errors.push(`Simulation failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
      return { isReversible: false };
    }
  }
}

/**
 * Factory function to create an integrity validator
 * 
 * @param options - Configuration options for the validator
 * @returns A new integrity validator instance
 * 
 * @example
 * ```typescript
 * import { createIntegrityValidator } from "@diister/mongodbee/migration/validators";
 * 
 * const validator = createIntegrityValidator({
 *   strictMode: true,
 *   runSimulation: true,
 *   validateReversibility: true
 * });
 * ```
 */
export function createIntegrityValidator(options?: IntegrityValidatorOptions): IntegrityValidator {
  return new IntegrityValidator(options);
}

/**
 * Convenience function to validate a migration state
 * 
 * @param migrationState - The migration state to validate
 * @param options - Optional validator configuration
 * @param initialState - Optional initial database state for simulation
 * @returns Validation result
 * 
 * @example
 * ```typescript
 * import { validateMigrationState } from "@diister/mongodbee/migration/validators";
 * 
 * const result = validateMigrationState(migrationState, {
 *   strictMode: true
 * });
 * 
 * if (!result.isValid) {
 *   throw new Error(`State validation failed: ${result.errors.join(', ')}`);
 * }
 * ```
 */
export function validateMigrationState(
  migrationState: MigrationState,
  options?: IntegrityValidatorOptions,
  initialState?: SimulationDatabaseState
): IntegrityValidationResult {
  const validator = createIntegrityValidator(options);
  return validator.validateMigrationState(migrationState, initialState);
}
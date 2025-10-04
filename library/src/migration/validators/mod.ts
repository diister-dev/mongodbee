/**
 * @fileoverview Migration validators for MongoDBee migration system
 *
 * This module provides comprehensive validation tools for ensuring migration integrity:
 * - **Chain Validation**: Validates migration chain structure and relationships
 * - **Integrity Validation**: Validates operation integrity and reversibility
 *
 * @example
 * ```typescript
 * // Chain validation
 * import { validateMigrationChain } from "@diister/mongodbee/migration/validators";
 *
 * const migrations = [
 *   { id: '001', parent: null, name: 'Initial', schemas: {}, migrate: () => ({}) },
 *   { id: '002', parent: migrations[0], name: 'Add users', schemas: {}, migrate: () => ({}) }
 * ];
 *
 * const chainResult = validateMigrationChain(migrations);
 * if (!chainResult.isValid) {
 *   console.error('Chain validation failed:', chainResult.errors);
 * }
 * ```
 *
 * @example
 * ```typescript
 * // Integrity validation
 * import { validateMigrationState } from "@diister/mongodbee/migration/validators";
 *
 * const migrationState = {
 *   operations: [
 *     { type: 'create_collection', collectionName: 'users' },
 *     { type: 'seed_collection', collectionName: 'users', documents: [] }
 *   ]
 * };
 *
 * const integrityResult = validateMigrationState(migrationState);
 * if (!integrityResult.isValid) {
 *   console.error('Integrity validation failed:', integrityResult.errors);
 * }
 * ```
 *
 * @module
 */

// Export chain validation functionality
export {
  type ChainValidationResult,
  ChainValidator,
  type ChainValidatorOptions,
  createChainValidator,
  validateMigrationChain,
} from "./chain.ts";

// Export integrity validation functionality
export {
  createIntegrityValidator,
  type IntegrityValidationResult,
  IntegrityValidator,
  type IntegrityValidatorOptions,
  validateMigrationState,
} from "./integrity.ts";

// Export simulation validation functionality
export {
  createSimulationValidator,
  DEFAULT_SIMULATION_VALIDATOR_OPTIONS,
  SimulationValidator,
  type SimulationValidatorOptions,
  validateMigrationWithSimulation,
} from "./simulation.ts";

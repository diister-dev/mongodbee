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
 * import { createChainValidator } from "@diister/mongodbee/migration";
 *
 * const migrations = [
 *   { id: '001', parent: null, name: 'Initial', schemas: {}, migrate: () => ({}) },
 *   { id: '002', parent: migrations[0], name: 'Add users', schemas: {}, migrate: () => ({}) }
 * ];
 *
 * const chainResult = createChainValidator().validateChain(migrations);
 * if (!chainResult.isValid) {
 *   console.error('Chain validation failed:', chainResult.errors);
 * }
 * ```
 *
 * @example
 * ```typescript
 * // Simulation validation: replays the migration against a mock database,
 * // which is what `mongodbee check` does before touching a real one.
 * import { validateMigrationWithSimulation } from "@diister/mongodbee/migration";
 *
 * const result = await validateMigrationWithSimulation(migrationDefinition);
 * if (!result.isValid) {
 *   console.error('Simulation failed:', result.errors);
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

// Export simulation validation functionality
export {
  createSimulationValidator,
  DEFAULT_SIMULATION_VALIDATOR_OPTIONS,
  getMockGenerationConfig,
  type MigrationValidator,
  type SimulationPowerLevel,
  SimulationValidator,
  type SimulationValidatorOptions,
  validateMigrationWithSimulation,
  type ValidationResult,
} from "./simulation.ts";

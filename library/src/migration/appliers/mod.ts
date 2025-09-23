/**
 * @fileoverview Migration appliers for MongoDBee migration system
 * 
 * This module provides different appliers for executing migration operations:
 * - **SimulationApplier**: In-memory simulation for testing and validation
 * - **MongodbApplier**: Real MongoDB operations for production use
 * 
 * @example
 * ```typescript
 * // For testing and validation
 * import { createSimulationApplier, createEmptyDatabaseState } from "@diister/mongodbee/migration/appliers";
 * 
 * const simulationApplier = createSimulationApplier({ strictValidation: true });
 * let state = createEmptyDatabaseState();
 * 
 * state = simulationApplier.applyOperation(state, {
 *   type: 'create_collection',
 *   collectionName: 'users'
 * });
 * ```
 * 
 * @example
 * ```typescript
 * // For production use
 * import { createMongodbApplier } from "@diister/mongodbee/migration/appliers";
 * import { MongoClient } from "@diister/mongodbee/mongodb";
 * 
 * const client = new MongoClient("mongodb://localhost:27017");
 * const db = client.db("myapp");
 * const applier = createMongodbApplier(db);
 * 
 * await applier.applyOperation({
 *   type: 'create_collection',
 *   collectionName: 'users'
 * });
 * ```
 * 
 * @module
 */

// Export all simulation applier functionality
export {
  SimulationApplier,
  type SimulationMigrationApplier,
  type SimulationApplierOptions,
  type SimulationDatabaseState,
  createSimulationApplier,
  createEmptyDatabaseState,
  compareDatabaseStates,
} from './simulation.ts';

// Export all MongoDB applier functionality  
export {
  MongodbApplier,
  type MongodbApplierOptions,
  createMongodbApplier,
  validateMongodbConnection,
} from './mongodb.ts';
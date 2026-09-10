/**
 * @fileoverview MongoDBee Migration System - Main Module
 *
 * This is the main entry point for the MongoDBee migration system. It provides
 * a comprehensive, type-safe, and functional approach to MongoDB migrations
 * with schema validation, simulation capabilities, and robust error handling.
 *
 * ## Features
 *
 * - **Type-Safe Migrations**: Full TypeScript support with Valibot schema validation
 * - **Functional Design**: Pure functions and immutable data structures
 * - **Simulation Support**: Test migrations without touching your database
 * - **Schema Validation**: Validate data integrity throughout the migration process
 * - **Flexible Configuration**: Support for multiple environments and configurations
 * - **Template System**: Generate migrations from built-in or custom templates
 * - **JSR Compatible**: Designed for the JavaScript Registry with proper documentation
 *
 * ## Basic Usage
 *
 * ### Creating a Migration
 *
 * ```typescript
 * import { migrationBuilder } from "@diister/mongodbee/migration";
 * import * as v from "@diister/mongodbee/schema";
 *
 * const userSchema = v.object({
 *   _id: v.string(),
 *   name: v.string(),
 *   email: v.pipe(v.string(), v.email()),
 *   createdAt: v.date(),
 * });
 *
 * const migration = migrationBuilder({
 *   schemas: {
 *     collections: { users: userSchema }
 *   }
 * })
 *   .createCollection("users")
 *     .seed([
 *       { name: "Admin", email: "admin@example.com", createdAt: new Date() }
 *     ])
 *     .done()
 *   .compile();
 * ```
 *
 * @module
 */

import { VERSION } from "../version.ts";

// Core types and interfaces
export type {
  CollectionBuilder as MigrationCollectionBuilder,
  CreateCollectionRule,
  // Database state
  DatabaseState,
  // Applier interfaces
  MigrationApplier,
  MigrationBuilder,
  // Migration definitions
  MigrationDefinition,
  MigrationProperty,
  // Operation types
  MigrationRule,
  MigrationState,
  // Schema types
  SchemasDefinition,
  SeedCollectionRule,
  TransformCollectionRule,
  TransformRule,
} from "./types.ts";

// Builder system
export {
  getMigrationSummary,
  isCreateCollectionRule,
  isSeedCollectionRule,
  isTransformCollectionRule,
  migrationBuilder,
} from "./builder.ts";
export type { MigrationBuilderOptions } from "./builder.ts";

// Import for default export
import { migrationBuilder } from "./builder.ts";

// Definition management
export {
  createMigrationSummary,
  findCommonAncestor,
  generateMigrationId,
  getMigrationAncestors,
  getMigrationPath,
  isMigrationAncestor,
  migrationDefinition,
  validateMigrationChain,
} from "./definition.ts";
export type { MigrationDefinitionOptions } from "./definition.ts";

// Appliers
export { createMongodbApplier } from "./appliers/mongodb.ts";
export type {
  MigrationProgressEvent,
  MongodbApplierOptions,
} from "./appliers/mongodb.ts";

// Configuration
export * from "./config/mod.ts";

// Validators. This used to name only four symbols, which left the simulation
// validators — the ones `mongodbee check` itself runs on — documented but
// unreachable.
//
// Deliberately not `export *`: `validators/chain.ts` also exports a
// `validateMigrationChain`, and `definition.ts` already exports that name from
// here. A star export would resolve to `definition.ts`'s, silently handing
// callers a `{ valid, errors }` where chain.ts's docs promise
// `{ isValid, errors, warnings, metadata }`. Chain validation is reachable
// through `createChainValidator().validateChain()` instead, which is
// unambiguous.
export {
  type ChainValidationResult,
  ChainValidator,
  type ChainValidatorOptions,
  createChainValidator,
  createSimulationValidator,
  DEFAULT_SIMULATION_VALIDATOR_OPTIONS,
  getMockGenerationConfig,
  type MigrationValidator,
  SimulationValidator,
  type SimulationValidatorOptions,
  type SimulationPowerLevel,
  validateMigrationWithSimulation,
  type ValidationResult,
} from "./validators/mod.ts";

// Multi-collection registry
export {
  createMetadataSchemas,
  createMultiCollectionInfo,
  discoverMultiCollectionInstances,
  getMultiCollectionInfo,
  getMultiCollectionMigrations,
  getMultiModelAppliedMigrationIds,
  getMultiModelCurrentState,
  getMultiModelMigrationHistory,
  isInstanceCreatedAfterMigration,
  markAsMultiCollection,
  markMultiModelMigrationAsFailed,
  markMultiModelMigrationAsReverted,
  MULTI_COLLECTION_INFO_TYPE,
  MULTI_COLLECTION_MIGRATIONS_TYPE,
  multiCollectionInstanceExists,
  recordMultiCollectionMigration,
  shouldInstanceReceiveMigration,
  shouldInstanceReceiveMigrationByChain,
  shouldInstanceReceiveMigrationFromChain,
} from "./multicollection-registry.ts";
export type {
  MultiCollectionInfo,
  MultiCollectionMigrations,
  MultiModelMigrationOperation,
  MultiModelMigrationOperationType,
  MultiModelOperationStatus,
} from "./multicollection-registry.ts";

// Catch-up system for orphaned multi-model instances
export {
  detectInstancesNeedingCatchUp,
  filterOperationsForModelType,
  getMigrationsForCatchUp,
} from "./catch-up.ts";
export type { CatchUpSummary, InstanceCatchUpInfo } from "./catch-up.ts";

// Generic event sourcing utilities
export {
  calculateMigrationStateFromHistory,
  getAppliedMigrationIdsFromHistory,
  groupOperationsByMigrationId,
} from "./migration-history.ts";
export type { BaseMigrationOperation } from "./migration-history.ts";

// Migration ID utilities
export {
  compareMigrationTimestamps,
  extractMigrationTimestamp,
  isMigrationAfter,
  isMigrationBefore,
} from "./utils/migration-id.ts";

// Status checking utilities
export {
  assertMigrationSystemHealthy,
  checkMigrationStatus,
} from "./check-status.ts";
export type {
  CheckMigrationStatusOptions,
  DatabaseStatusDetails,
  IndexIssue,
  IndexValidationDetails,
  MigrationCounts,
  MigrationInfo,
  MigrationStatusResult,
  MigrationValidationDetails,
} from "./check-status.ts";

// Pre-flight privilege check (what the connected account may do vs. what a run needs)
export {
  checkMigrationPrivileges,
  DB_ADMIN_ONLY_ACTIONS,
  evaluatePrivileges,
  MIGRATION_PRIVILEGE_ACTIONS,
} from "./privileges.ts";
export type {
  CheckMigrationPrivilegesOptions,
  ConnectionAuthInfo,
  MigrationPrivilegeCheck,
  PrivilegeEvaluation,
  ServerPrivilege,
} from "./privileges.ts";

/**
 * The version of MongoDBee this migration system ships with.
 *
 * Re-exported from the single source of truth in `src/version.ts`, which
 * `scripts/check-package.ts` keeps in step with package.json and jsr.json. It
 * used to be a hardcoded "1.0.0" that no release ever updated.
 */
export { VERSION };

/**
 * Default export providing the most commonly used functions
 *
 * @example
 * ```typescript
 * import mongodbee from "@diister/mongodbee/migration";
 *
 * // Build a migration
 * const migration = mongodbee.builder({ schemas: mySchemas })
 *   .createCollection("users")
 *   .done()
 *   .compile();
 * ```
 */
export default {
  // Core functions
  builder: migrationBuilder,

  // Version
  VERSION,
};

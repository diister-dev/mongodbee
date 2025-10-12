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
 * - **Execution Runners**: Coordinate migration execution with logging and error handling
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
 * ### Running Migrations
 *
 * ```typescript
 * import { createMigrationRunner, MongodbApplier } from "@diister/mongodbee/migration";
 *
 * // Create applier and runner
 * const applier = new MongodbApplier(mongoClient.db("myapp"));
 * const runner = createMigrationRunner({
 *   config: systemConfig,
 *   applier
 * });
 *
 * // Execute migration
 * const result = await runner.executeMigration(migrationDefinition);
 * console.log(`Migration completed: ${result.success}`);
 * ```
 *
 * @module
 */

// Core types and interfaces
export type {
  CreateCollectionRule,
  // Database state
  DatabaseState,
  // Applier interfaces
  MigrationApplier,
  MigrationBuilder,
  CollectionBuilder as MigrationCollectionBuilder,
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
export { SimulationApplier } from "./appliers/simulation.ts";
export { createMongodbApplier } from "./appliers/mongodb.ts";
export type { MongodbApplierOptions } from "./appliers/mongodb.ts";

// Configuration
export * from "./config/mod.ts";

// Runners
export * from "./runners/mod.ts";

// Validators (partial export of working functions)
export {
  createChainValidator,
  createIntegrityValidator,
} from "./validators/mod.ts";

// Multi-collection registry
export {
  createMultiCollectionInfo,
  discoverMultiCollectionInstances,
  getMultiCollectionInfo,
  getMultiCollectionMigrations,
  isInstanceCreatedAfterMigration,
  markAsMultiCollection,
  MULTI_COLLECTION_INFO_TYPE,
  MULTI_COLLECTION_MIGRATIONS_TYPE,
  multiCollectionInstanceExists,
  recordMultiCollectionMigration,
  shouldInstanceReceiveMigration,
} from "./multicollection-registry.ts";

// Status checking utilities
export {
  assertMigrationSystemHealthy,
  checkMigrationStatus,
} from "./check-status.ts";
export type {
  CheckMigrationStatusOptions,
  DatabaseStatusDetails,
  MigrationCounts,
  MigrationInfo,
  MigrationStatusResult,
  MigrationValidationDetails,
} from "./check-status.ts";

/**
 * Version information for the migration system
 */
export const VERSION = "1.0.0";

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

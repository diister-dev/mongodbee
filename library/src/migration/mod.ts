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
  // Migration definitions
  MigrationDefinition,
  MigrationState,
  MigrationProperty,
  MigrationBuilder,
  MigrationCollectionBuilder,
  
  // Operation types
  MigrationRule,
  CreateCollectionRule,
  SeedCollectionRule,
  TransformCollectionRule,
  TransformRule,
  
  // Applier interfaces
  MigrationApplier,
  
  // Schema types
  SchemasDefinition,
  
  // Database state
  DatabaseState,
} from './types.ts';

// Builder system
export {
  migrationBuilder,
  isCreateCollectionRule,
  isSeedCollectionRule,
  isTransformCollectionRule,
  getMigrationSummary,
} from './builder.ts';
export type {
  MigrationBuilderOptions,
} from './builder.ts';

// Import for default export
import { migrationBuilder } from './builder.ts';

// Definition management
export {
  migrationDefinition,
  validateMigrationChain,
  generateMigrationId,
  getMigrationAncestors,
  getMigrationPath,
  findCommonAncestor,
  isMigrationAncestor,
  createMigrationSummary,
} from './definition.ts';
export type {
  MigrationDefinitionOptions,
} from './definition.ts';

// Appliers
export { SimulationApplier } from './appliers/simulation.ts';
export { MongodbApplier } from './appliers/mongodb.ts';

// Configuration
export * from './config/mod.ts';

// Runners  
export * from './runners/mod.ts';

// Generators
export * from './generators/mod.ts';

// Validators (partial export of working functions)
export {
  createIntegrityValidator,
  createChainValidator,
  validateMigrationState,
} from './validators/mod.ts';

/**
 * Version information for the migration system
 */
export const VERSION = '1.0.0';

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
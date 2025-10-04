/**
 * @fileoverview Configuration type definitions for MongoDBee migration system
 *
 * This module defines all configuration-related types including database connection
 * settings, file paths, migration configuration, and CLI options. All configurations
 * use Valibot schemas for validation.
 *
 * @example
 * ```typescript
 * import { createMigrationConfig } from "@diister/mongodbee/migration/config";
 *
 * const config = createMigrationConfig({
 *   database: {
 *     uri: "mongodb://localhost:27017",
 *     database: "myapp"
 *   },
 *   paths: {
 *     migrations: "./migrations",
 *     schemas: "./schemas"
 *   }
 * });
 * ```
 *
 * @module
 */

import * as v from "../../schema.ts";

/**
 * Database connection configuration
 *
 * Defines how to connect to MongoDB for applying migrations.
 * Supports both URI-based and component-based configuration.
 */
export const DatabaseConfigSchema = v.object({
  /** Connection configuration */
  connection: v.object({
    /** MongoDB connection URI */
    uri: v.string(),

    /** Optional connection options */
    options: v.optional(v.object({
      /** Connection timeout in milliseconds */
      connectTimeoutMS: v.optional(v.number()),

      /** Server selection timeout in milliseconds */
      serverSelectionTimeoutMS: v.optional(v.number()),

      /** Maximum pool size */
      maxPoolSize: v.optional(v.number()),

      /** Minimum pool size */
      minPoolSize: v.optional(v.number()),

      /** Maximum idle time in milliseconds */
      maxIdleTimeMS: v.optional(v.number()),

      /** SSL/TLS configuration */
      ssl: v.optional(v.boolean()),

      /** Authentication source database */
      authSource: v.optional(v.string()),

      /** Read preference */
      readPreference: v.optional(
        v.picklist([
          "primary",
          "primaryPreferred",
          "secondary",
          "secondaryPreferred",
          "nearest",
        ]),
      ),

      /** Write concern */
      writeConcern: v.optional(v.object({
        w: v.optional(v.union([v.number(), v.string()])),
        j: v.optional(v.boolean()),
        wtimeout: v.optional(v.number()),
      })),
    })),
  }),

  /** Target database name */
  name: v.string(),
});

/**
 * File system paths configuration
 *
 * Defines where migration files, schemas, and other resources are located.
 * All paths can be absolute or relative to the project root.
 */
export const PathsConfigSchema = v.object({
  /** Directory containing migration files */
  migrations: v.string(),

  /** Directory containing schema definitions */
  schemas: v.string(),

  /** Optional directory for temporary files during migration */
  temp: v.optional(v.string()),

  /** Optional directory for backup files */
  backup: v.optional(v.string()),

  /** Optional directory for migration logs */
  logs: v.optional(v.string()),
});

/**
 * Migration execution configuration
 *
 * Controls how migrations are executed, validated, and logged.
 */
export const MigrationConfigSchema = v.object({
  /** Whether to run in dry-run mode (simulation only) */
  dryRun: v.optional(v.boolean()),

  /** Whether to create backup before applying migrations */
  backup: v.optional(v.boolean()),

  /** Maximum number of migrations to apply in one batch */
  batchSize: v.optional(v.number()),

  /** Timeout for individual migration operations in milliseconds */
  operationTimeout: v.optional(v.number()),

  /** Whether to continue on validation errors */
  continueOnError: v.optional(v.boolean()),

  /** Logging configuration */
  logging: v.optional(v.object({
    /** Log level */
    level: v.optional(v.picklist(["debug", "info", "warn", "error"])),

    /** Whether to log to console */
    console: v.optional(v.boolean()),

    /** Whether to log to file */
    file: v.optional(v.boolean()),

    /** Custom log format */
    format: v.optional(v.picklist(["json", "text", "structured"])),
  })),

  /** Validation configuration */
  validation: v.optional(v.object({
    /** Whether to validate schemas before applying */
    schemas: v.optional(v.boolean()),

    /** Whether to validate migration chain integrity */
    chain: v.optional(v.boolean()),

    /** Whether to validate data integrity after operations */
    data: v.optional(v.boolean()),
  })),
});

/**
 * CLI-specific configuration
 *
 * Controls CLI behavior and output formatting.
 */
export const CliConfigSchema = v.object({
  /** Whether to use colored output */
  colors: v.optional(v.boolean()),

  /** Output format for CLI commands */
  format: v.optional(v.picklist(["table", "json", "yaml", "text"])),

  /** Whether to show verbose output */
  verbose: v.optional(v.boolean()),

  /** Whether to show progress indicators */
  progress: v.optional(v.boolean()),

  /** Whether to prompt for confirmation on destructive operations */
  confirmDestructive: v.optional(v.boolean()),
});

/**
 * Complete migration system configuration
 *
 * Combines all configuration aspects into a single, validated structure.
 */
export const MigrationSystemConfigSchema = v.object({
  /** Database connection settings */
  database: DatabaseConfigSchema,

  /** File system paths */
  paths: PathsConfigSchema,

  /** Migration execution settings */
  migration: v.optional(MigrationConfigSchema),

  /** CLI-specific settings */
  cli: v.optional(CliConfigSchema),

  /** Custom environment-specific overrides */
  environments: v.optional(v.record(
    v.string(),
    v.object({
      database: v.optional(v.partial(DatabaseConfigSchema)),
      paths: v.optional(v.partial(PathsConfigSchema)),
      migration: v.optional(v.partial(MigrationConfigSchema)),
      cli: v.optional(v.partial(CliConfigSchema)),
    }),
  )),
});

/**
 * Inferred TypeScript types from Valibot schemas
 */
export type DatabaseConfig = v.InferInput<typeof DatabaseConfigSchema>;
export type PathsConfig = v.InferInput<typeof PathsConfigSchema>;
export type MigrationConfig = v.InferInput<typeof MigrationConfigSchema>;
export type CliConfig = v.InferInput<typeof CliConfigSchema>;
export type MigrationSystemConfig = v.InferInput<
  typeof MigrationSystemConfigSchema
>;

/**
 * Environment-specific configuration override
 */
export type EnvironmentConfig = {
  database?: Partial<DatabaseConfig>;
  paths?: Partial<PathsConfig>;
  migration?: Partial<MigrationConfig>;
  cli?: Partial<CliConfig>;
};

/**
 * Configuration loading result with validation information
 */
export type ConfigResult = {
  /** The loaded and validated configuration */
  config: MigrationSystemConfig;

  /** Source of the configuration (file path, environment, etc.) */
  source: string;

  /** Any validation warnings (non-fatal issues) */
  warnings: string[];

  /** Applied environment overrides */
  environment?: string;
};

/**
 * Configuration loading options
 */
export type ConfigLoadOptions = {
  /** Specific environment to load */
  environment?: string;

  /** Custom config file path */
  configPath?: string;

  /** Whether to merge with environment variables */
  useEnvVars?: boolean;

  /** Custom environment variable prefix */
  envPrefix?: string;

  /** Whether to validate the configuration strictly */
  strict?: boolean;
};

/**
 * Default configuration values
 *
 * These defaults provide sensible fallbacks for all optional configuration.
 * Users only need to specify database connection and paths.
 */
export const DEFAULT_CONFIG: Partial<MigrationSystemConfig> = {
  migration: {
    dryRun: false,
    backup: true,
    batchSize: 10,
    operationTimeout: 30000,
    continueOnError: false,
    logging: {
      level: "info",
      console: true,
      file: true,
      format: "structured",
    },
    validation: {
      schemas: true,
      chain: true,
      data: true,
    },
  },
  cli: {
    colors: true,
    format: "table",
    verbose: false,
    progress: true,
    confirmDestructive: true,
  },
};

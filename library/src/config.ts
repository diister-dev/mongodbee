/**
 * Simple configuration type for MongoDBee migrations
 * Inspired by Drizzle's configuration style
 */
export type MongodbeeConfig = {
  /** Database connection and credentials */
  database?: {
    /** Connection settings */
    connection?: {
      /** MongoDB connection URI */
      uri?: string;
      /** Connection options */
      options?: Record<string, unknown>;
    };
    /** Database name */
    name?: string;
  };

  /** Paths configuration */
  paths?: {
    /** Directory containing migration files (default: "./migrations") */
    migrations?: string;
    /** Path to schemas file (default: "./schemas.ts") */
    schemas?: string;
  };
};

/**
 * Defines a MongoDBee configuration with type safety
 *
 * @param config - The configuration object
 * @returns The same configuration with type checking
 *
 * @example
 * ```typescript
 * import { defineConfig } from "@diister/mongodbee";
 *
 * export default defineConfig({
 *   database: {
 *     connection: {
 *       uri: "mongodb://localhost:27017"
 *     },
 *     name: "myapp"
 *   },
 *   paths: {
 *     migrations: "./migrations",
 *     schemas: "./schemas.ts"
 *   }
 * });
 * ```
 */
export function defineConfig(config: MongodbeeConfig): MongodbeeConfig {
  return config;
}

/**
 * Default configuration values
 */
export const DEFAULT_MONGODBEE_CONFIG: MongodbeeConfig = {
  database: {
    connection: {
      uri: "mongodb://localhost:27017"
    },
    name: "myapp"
  },
  paths: {
    migrations: "./migrations",
    schemas: "./schemas.ts"
  },
};
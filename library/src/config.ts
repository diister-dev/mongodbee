/**
 * Simple configuration type for MongoDBee migrations
 * Inspired by Drizzle's configuration style
 */
export type MongodbeeConfig = {
  /** Path to schema file (optional) */
  schema?: string;

  /** Database connection and credentials */
  db?: {
    /** MongoDB connection URI */
    uri?: string;
    /** Database name */
    name?: string;
    /** Username for authentication */
    username?: string;
    /** Password for authentication */
    password?: string;
  };

  /** Paths configuration */
  paths?: {
    /** Directory containing migration files (default: "./migrations") */
    migrationsDir?: string;
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
 *   schema: "./schemas.ts",
 *   db: {
 *     uri: "mongodb://localhost:27017",
 *     name: "myapp",
 *     username: process.env.MONGODBEE_USERNAME,
 *     password: process.env.MONGODBEE_PASSWORD,
 *   },
 *   paths: {
 *     migrationsDir: "./migrations",
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
export const DEFAULT_MONGODBEE_CONFIG: Required<MongodbeeConfig> = {
  schema: "./schemas.ts",
  db: {
    uri: "mongodb://localhost:27017",
    name: "myapp",
    username: undefined,
    password: undefined,
  },
  paths: {
    migrationsDir: "./migrations",
  },
};
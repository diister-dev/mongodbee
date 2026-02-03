/**
 * @fileoverview Configuration loading and management utilities
 *
 * This module provides functional utilities for loading, validating, and managing
 * migration system configuration from various sources including files, environment
 * variables, and programmatic inputs.
 *
 * @example
 * ```typescript
 * import { loadConfig, createConfig } from "@diister/mongodbee/migration/config";
 *
 * // Load from config file
 * const { config } = await loadConfig({
 *   configPath: "./mongodbee.config.json",
 *   environment: "development"
 * });
 *
 * // Create programmatically
 * const config = createConfig({
 *   database: { uri: "mongodb://localhost:27017", database: "myapp" },
 *   paths: { migrations: "./migrations", schemas: "./schemas" }
 * });
 * ```
 *
 * @module
 */

import process from "node:process";
import * as fs from "node:fs/promises";
import * as v from "../../schema.ts";
import * as path from "@std/path";
import {
  type ConfigLoadOptions,
  type ConfigResult,
  DEFAULT_CONFIG,
  type MigrationSystemConfig,
  MigrationSystemConfigSchema,
} from "./types.ts";
import { red } from "@std/fmt/colors";
import { pathToFileUrl } from "../utils/platform.ts";

/**
 * Deeply merges two configuration objects
 *
 * @param base - The base configuration object
 * @param override - The override configuration object
 * @returns The merged configuration
 */
function deepMergeConfig(
  base: Record<string, unknown>,
  override: Record<string, unknown>,
): Record<string, unknown> {
  const result = { ...base };

  for (const [key, value] of Object.entries(override)) {
    if (value === undefined) continue;

    if (
      typeof value === "object" &&
      value !== null &&
      !Array.isArray(value) &&
      key in result &&
      typeof result[key] === "object" &&
      result[key] !== null &&
      !Array.isArray(result[key])
    ) {
      result[key] = deepMergeConfig(
        result[key] as Record<string, unknown>,
        value as Record<string, unknown>,
      );
    } else {
      result[key] = value;
    }
  }

  return result;
}

/**
 * Validates a configuration object using the schema
 *
 * @param config - The configuration object to validate
 * @returns Validation result with parsed config and any issues
 */
function validateConfig(config: unknown): {
  success: boolean;
  config?: MigrationSystemConfig;
  errors: string[];
  warnings: string[];
} {
  const parseResult = v.safeParse(MigrationSystemConfigSchema, config);

  if (!parseResult.success) {
    return {
      success: false,
      errors: parseResult.issues.map((issue) =>
        `${
          issue.path?.map((p) => String(p)).join(".") || "root"
        }: ${issue.message}`
      ),
      warnings: [],
    };
  }

  // Check for potential warnings
  const warnings: string[] = [];
  const validConfig = parseResult.output;

  // Warn if no backup is configured in production-like environments
  if (!validConfig.migration?.backup) {
    warnings.push(
      "Backup is disabled - this may be risky in production environments",
    );
  }

  // Warn if validation is disabled
  if (validConfig.migration?.validation?.schemas === false) {
    warnings.push(
      "Schema validation is disabled - this may lead to data integrity issues",
    );
  }

  // Warn if dry-run is enabled (might be accidental)
  if (validConfig.migration?.dryRun) {
    warnings.push(
      "Dry-run mode is enabled - no actual changes will be applied",
    );
  }

  return {
    success: true,
    config: validConfig,
    errors: [],
    warnings,
  };
}

/**
 * Loads configuration from environment variables
 *
 * @param prefix - Environment variable prefix (default: 'MONGODBEE_')
 * @returns Partial configuration from environment variables
 */
function loadFromEnvironment(
  prefix = "MONGODBEE_",
): Partial<MigrationSystemConfig> {
  const config: Partial<MigrationSystemConfig> = {};

  // Database configuration
  const dbUri = process.env[`${prefix}DB_URI`];
  const dbName = process.env[`${prefix}DB_NAME`];

  if (dbUri && dbName) {
    config.database = {
      connection: {
        uri: dbUri,
      },
      name: dbName,
    };

    // Optional database options
    const connectTimeout = process.env[`${prefix}DB_CONNECT_TIMEOUT`];
    const maxPoolSize = process.env[`${prefix}DB_MAX_POOL_SIZE`];

    if (connectTimeout || maxPoolSize) {
      config.database.connection.options = {
        ...(connectTimeout &&
          { connectTimeoutMS: parseInt(connectTimeout, 10) }),
        ...(maxPoolSize && { maxPoolSize: parseInt(maxPoolSize, 10) }),
      };
    }
  }

  // Paths configuration
  const migrationsPath = process.env[`${prefix}MIGRATIONS_PATH`];
  const schemasPath = process.env[`${prefix}SCHEMAS_PATH`];

  if (migrationsPath && schemasPath) {
    config.paths = {
      migrations: migrationsPath,
      schemas: schemasPath,
    };

    // Optional paths
    const tempPath = process.env[`${prefix}TEMP_PATH`];
    const backupPath = process.env[`${prefix}BACKUP_PATH`];
    const logsPath = process.env[`${prefix}LOGS_PATH`];

    if (tempPath) config.paths.temp = tempPath;
    if (backupPath) config.paths.backup = backupPath;
    if (logsPath) config.paths.logs = logsPath;
  }

  // Migration configuration
  const dryRun = process.env[`${prefix}DRY_RUN`];
  const backup = process.env[`${prefix}BACKUP`];
  const logLevel = process.env[`${prefix}LOG_LEVEL`];

  if (dryRun || backup || logLevel) {
    config.migration = {
      ...(dryRun && { dryRun: dryRun.toLowerCase() === "true" }),
      ...(backup && { backup: backup.toLowerCase() === "true" }),
    };

    if (logLevel) {
      config.migration.logging = {
        level: logLevel as "debug" | "info" | "warn" | "error",
      };
    }
  }

  // CLI configuration
  const noColors = process.env[`${prefix}NO_COLORS`];
  const verbose = process.env[`${prefix}VERBOSE`];

  if (noColors || verbose) {
    config.cli = {
      ...(noColors && { colors: noColors.toLowerCase() !== "true" }),
      ...(verbose && { verbose: verbose.toLowerCase() === "true" }),
    };
  }

  return config;
}

/**
 * Loads configuration from a JSON or YAML file
 *
 * @param filePath - Path to the configuration file
 * @param cwd - Current working directory for resolving relative paths
 * @returns The loaded configuration object
 */
async function loadFromFile(
  filePath: string,
  cwd: string = process.cwd(),
): Promise<Partial<MigrationSystemConfig>> {
  try {
    if (filePath.endsWith(".json")) {
      const fullPath = path.isAbsolute(filePath)
        ? filePath
        : path.resolve(cwd, filePath);
      const content = await fs.readFile(fullPath, "utf-8");
      return JSON.parse(content);
    }
    if (filePath.endsWith(".ts") || filePath.endsWith(".js")) {
      const fullPath = path.isAbsolute(filePath)
        ? filePath
        : path.resolve(cwd, filePath);
      // Convert path to file:// URL for dynamic import
      const importPath = pathToFileUrl(fullPath);
      const mod = await import(importPath);
      if (mod.default) return mod.default;
      if (mod.config) return mod.config;
      throw new Error(
        `No default or named 'config' export found in ${filePath}`,
      );
    }
    throw new Error(
      `Unsupported config file format: ${filePath}. Only .json, .ts, .js are supported.`,
    );
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Configuration file not found: ${filePath}`);
    }
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Failed to load configuration from ${filePath}: ${message}`,
    );
  }
}

/**
 * Discovers configuration files in common locations
 *
 * @param cwd - Current working directory for resolving relative paths
 * @returns Array of potential configuration file paths
 */
function discoverConfigFiles(cwd: string = process.cwd()): string[] {
  return [
    path.resolve(cwd, "./mongodbee.config.ts"),
    path.resolve(cwd, "./mongodbee.config.js"),
    path.resolve(cwd, "./mongodbee.config.json"),
    path.resolve(cwd, "./mongodbee.json"),
    path.resolve(cwd, "./.mongodbee.json"),
    path.resolve(cwd, "./config/mongodbee.json"),
    path.resolve(cwd, "./config/migrations.json"),
  ];
}

/**
 * Creates a validated configuration object
 *
 * This is a pure function that takes a partial configuration and returns
 * a fully validated configuration with defaults applied.
 *
 * @param input - Partial configuration input
 * @returns Validated configuration with defaults applied
 *
 * @example
 * ```typescript
 * const config = createConfig({
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
 */
export function createConfig(
  input: Partial<MigrationSystemConfig>,
): MigrationSystemConfig {
  // Merge with defaults
  const mergedConfig = deepMergeConfig(
    DEFAULT_CONFIG as Record<string, unknown>,
    input as Record<string, unknown>,
  ) as MigrationSystemConfig;

  // Validate the configuration
  const validation = validateConfig(mergedConfig);

  if (!validation.success) {
    throw new Error(
      `Configuration validation failed:\n${validation.errors.join("\n")}`,
    );
  }

  return validation.config!;
}

/**
 * Loads MongoDBee configuration from file or uses defaults
 *
 * @param options - Optional configuration path and working directory
 * @returns The loaded configuration
 */
export async function loadConfig(
  options: { configPath?: string; cwd?: string } = {},
): Promise<Partial<MigrationSystemConfig>> {
  const cwd = options.cwd || process.cwd();
  let config: Partial<MigrationSystemConfig>;

  // If explicit config path provided, try to load it
  if (options.configPath) {
    try {
      config = await loadFromFile(options.configPath, cwd);
      return config;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `${
          red("Failed to load config from")
        } ${options.configPath}: ${message}`,
      );
      throw error;
    }
  }

  // Otherwise, try to discover config files
  const toCheck = discoverConfigFiles(cwd);
  for (const configPath of toCheck) {
    try {
      config = await loadFromFile(configPath, cwd);
      return config;
    } catch {
      // Continue to next file
      continue;
    }
  }

  // If no config found, throw error
  throw new Error(
    'No configuration file found. Run "mongodbee init" to create one.',
  );
}

/**
 * Loads configuration from multiple sources with priority
 *
 * Priority order:
 * 1. Explicit configuration file (if specified)
 * 2. Environment-specific overrides
 * 3. Environment variables
 * 4. Default discovered config files
 * 5. Default configuration
 *
 * @param options - Configuration loading options
 * @returns Promise resolving to configuration result
 *
 * @example
 * ```typescript
 * // Load with auto-discovery
 * const { config } = await loadConfig();
 *
 * // Load specific environment
 * const { config } = await loadConfig({
 *   environment: "production",
 *   configPath: "./config/prod.json"
 * });
 *
 * // Load with environment variables
 * const { config } = await loadConfig({
 *   useEnvVars: true,
 *   envPrefix: "MYAPP_"
 * });
 * ```
 */
export async function loadConfigOld(
  options: ConfigLoadOptions = {},
): Promise<ConfigResult> {
  const {
    environment,
    configPath,
    useEnvVars = true,
    envPrefix = "MONGODBEE_",
    strict = true,
  } = options;

  let baseConfig = { ...DEFAULT_CONFIG } as Record<string, unknown>;
  let configSource = "defaults";
  const warnings: string[] = [];

  // 1. Try to load from specified or discovered config files
  let fileConfig: Record<string, unknown> = {};

  if (configPath) {
    try {
      fileConfig = await loadFromFile(configPath) as Record<string, unknown>;
      configSource = configPath;
    } catch (error) {
      if (strict) {
        throw error;
      }
      const message = error instanceof Error ? error.message : String(error);
      warnings.push(`Failed to load config from ${configPath}: ${message}`);
    }
  } else {
    // Try to discover config files
    for (const configFilePath of discoverConfigFiles()) {
      try {
        await fs.stat(configFilePath);
        fileConfig = await loadFromFile(configFilePath) as Record<string, unknown>;
        configSource = configFilePath;
        break;
      } catch {
        // Continue to next file
      }
    }
  }

  // 2. Merge base config with file config
  baseConfig = deepMergeConfig(baseConfig, fileConfig);

  // 3. Apply environment variables if enabled
  if (useEnvVars) {
    const envConfig = loadFromEnvironment(envPrefix) as Record<string, unknown>;
    baseConfig = deepMergeConfig(baseConfig, envConfig);
    if (Object.keys(envConfig).length > 0) {
      configSource += " + environment";
    }
  }

  // 4. Apply environment-specific overrides
  if (
    environment &&
    (baseConfig as MigrationSystemConfig).environments?.[environment]
  ) {
    const envOverride = (baseConfig as MigrationSystemConfig)
      .environments![environment] as Record<string, unknown>;
    baseConfig = deepMergeConfig(baseConfig, envOverride);
    configSource += ` + ${environment} environment`;
  }

  // 5. Validate the final configuration
  const validation = validateConfig(baseConfig as MigrationSystemConfig);

  if (!validation.success) {
    throw new Error(
      `Configuration validation failed:\n${validation.errors.join("\n")}`,
    );
  }

  return {
    config: validation.config!,
    source: configSource,
    warnings: [...warnings, ...validation.warnings],
    environment,
  };
}

/**
 * Resolves relative paths in the configuration to absolute paths
 *
 * @param config - Configuration with potentially relative paths
 * @param basePath - Base path to resolve relative paths against
 * @returns Configuration with absolute paths
 */
export function resolveConfigPaths(
  config: MigrationSystemConfig,
  basePath: string = process.cwd(),
): MigrationSystemConfig {
  const resolved = { ...config };

  // Resolve paths configuration
  if (resolved.paths) {
    resolved.paths = {
      ...resolved.paths,
      migrations:
        new URL(resolved.paths.migrations, `file://${basePath}/`).pathname,
      schemas: new URL(resolved.paths.schemas, `file://${basePath}/`).pathname,
    };

    if (resolved.paths.temp) {
      resolved.paths.temp =
        new URL(resolved.paths.temp, `file://${basePath}/`).pathname;
    }

    if (resolved.paths.backup) {
      resolved.paths.backup =
        new URL(resolved.paths.backup, `file://${basePath}/`).pathname;
    }

    if (resolved.paths.logs) {
      resolved.paths.logs =
        new URL(resolved.paths.logs, `file://${basePath}/`).pathname;
    }
  }

  return resolved;
}

/**
 * Validates that required directories exist and are accessible
 *
 * @param config - Configuration to validate
 * @returns Array of validation errors
 */
export async function validateConfigPaths(
  config: MigrationSystemConfig,
): Promise<string[]> {
  const errors: string[] = [];

  if (!config.paths) {
    return ["No paths configuration provided"];
  }

  // Check migrations directory
  try {
    const migrationsStat = await fs.stat(config.paths.migrations);
    if (!migrationsStat.isDirectory()) {
      errors.push(
        `Migrations path is not a directory: ${config.paths.migrations}`,
      );
    }
  } catch {
    errors.push(
      `Migrations directory does not exist: ${config.paths.migrations}`,
    );
  }

  // Check schemas directory
  try {
    const schemasStat = await fs.stat(config.paths.schemas);
    if (!schemasStat.isDirectory()) {
      errors.push(`Schemas path is not a directory: ${config.paths.schemas}`);
    }
  } catch {
    errors.push(`Schemas directory does not exist: ${config.paths.schemas}`);
  }

  // Check optional directories
  const optionalPaths = [
    ["temp", config.paths.temp],
    ["backup", config.paths.backup],
    ["logs", config.paths.logs],
  ] as const;

  for (const [name, optPath] of optionalPaths) {
    if (optPath) {
      try {
        const stat = await fs.stat(optPath);
        if (!stat.isDirectory()) {
          errors.push(`${name} path is not a directory: ${optPath}`);
        }
      } catch {
        // Optional directories can be created if they don't exist
        // This is just a warning, not an error
      }
    }
  }

  return errors;
}

/**
 * Creates necessary directories based on configuration
 *
 * @param config - Configuration containing paths to create
 * @returns Array of created directory paths
 */
export async function ensureConfigDirectories(
  config: MigrationSystemConfig,
): Promise<string[]> {
  const created: string[] = [];

  if (!config.paths) {
    return created;
  }

  const pathsToCreate = [
    config.paths.migrations,
    config.paths.schemas,
    config.paths.temp,
    config.paths.backup,
    config.paths.logs,
  ].filter(Boolean) as string[];

  for (const dirPath of pathsToCreate) {
    try {
      await fs.stat(dirPath);
    } catch {
      try {
        await fs.mkdir(dirPath, { recursive: true });
        created.push(dirPath);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(`Failed to create directory ${dirPath}: ${message}`);
      }
    }
  }

  return created;
}

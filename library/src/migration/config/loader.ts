/**
 * @fileoverview Configuration loading and management utilities
 *
 * This module provides functional utilities for loading, validating, and managing
 * migration system configuration from various sources including files, environment
 * variables, and programmatic inputs.
 *
 * @example
 * ```typescript
 * import { loadConfig, createConfig } from "@diister/mongodbee/migration";
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
import { existsSync } from "node:fs";
import * as v from "../../schema.ts";
import * as path from "node:path";
import {
  DEFAULT_CONFIG,
  type MigrationSystemConfig,
  MigrationSystemConfigSchema,
} from "./types.ts";
import { red } from "../../utils/colors.ts";
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
      // `issue.path` holds valibot PathItem objects, not strings: stringifying
      // them produced "[object Object].[object Object]: Invalid key" for every
      // malformed config. The readable segment is `key`.
      errors: parseResult.issues.map(
        (issue) =>
          `${
            issue.path?.map((item) => String(item.key)).join(".") || "root"
          }: ${issue.message}`,
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
 *     connection: { uri: "mongodb://localhost:27017" },
 *     name: "myapp"
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
        `${red(
          "Failed to load config from",
        )} ${options.configPath}: ${message}`,
      );
      throw error;
    }
  }

  // Otherwise, try to discover config files
  const toCheck = discoverConfigFiles(cwd);
  // A configuration file that EXISTS but fails to load — a bad import
  // specifier, a syntax error, a missing dependency — used to be swallowed
  // here and reported as "no configuration file found", sending people to
  // `init` to recreate a file that was already there. Keep why each candidate
  // was rejected so the real cause survives.
  const rejected: string[] = [];
  for (const configPath of toCheck) {
    try {
      config = await loadFromFile(configPath, cwd);
      return config;
    } catch (error) {
      if (existsSync(configPath)) {
        const message = error instanceof Error ? error.message : String(error);
        rejected.push(`  ${configPath}: ${message}`);
      }
      continue;
    }
  }

  if (rejected.length > 0) {
    throw new Error(
      `Found a configuration file, but it could not be loaded:\n${rejected.join(
        "\n",
      )}`,
    );
  }

  throw new Error(
    'No configuration file found. Run "mongodbee init" to create one.',
  );
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
      migrations: new URL(resolved.paths.migrations, `file://${basePath}/`)
        .pathname,
      schemas: new URL(resolved.paths.schemas, `file://${basePath}/`).pathname,
    };

    if (resolved.paths.temp) {
      resolved.paths.temp = new URL(
        resolved.paths.temp,
        `file://${basePath}/`,
      ).pathname;
    }

    if (resolved.paths.backup) {
      resolved.paths.backup = new URL(
        resolved.paths.backup,
        `file://${basePath}/`,
      ).pathname;
    }

    if (resolved.paths.logs) {
      resolved.paths.logs = new URL(
        resolved.paths.logs,
        `file://${basePath}/`,
      ).pathname;
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

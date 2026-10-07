/**
 * Check command for MongoDBee Migration CLI
 *
 * Validates migrations and schema consistency without applying them
 *
 * @module
 */

import process from "node:process";
import { blue, bold, dim, green, red, yellow } from "../../../utils/colors.ts";
import * as path from "node:path";

import { loadConfig } from "../../config/loader.ts";
import { buildMigrationChain, loadAllMigrations } from "../../discovery.ts";
import { validateMigrationChainWithProjectSchema } from "../../schema-validation.ts";
import {
  MigrationValidationFailedError,
  type MigrationProgressHook,
  type MigrationResultHook,
  type MigrationValidationResult,
  validateMigrationsWithSimulation,
} from "../utils/validate-migrations.ts";
import type { SimulationPowerLevel } from "../../validators/simulation.ts";

export interface CheckCommandOptions {
  configPath?: string;
  cwd?: string;
  verbose?: boolean;
  /**
   * Simulation mode controlling validation complexity
   * - `quick`: Fast validation with minimal mock data
   * - `normal`: Balanced validation (default)
   * - `hard`: Comprehensive validation with extensive mock data
   */
  mode?: string;
  /**
   * Only validate the last N migrations
   */
  last?: number;
  docs?: number;
  retention?: number;
}

export const MAX_DOCS_PER_COLLECTION = 5000;

export function parseDocsPerCollection(raw: unknown): number | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  const value = Number(raw);
  if (
    !Number.isInteger(value) ||
    value < 1 ||
    value > MAX_DOCS_PER_COLLECTION
  ) {
    throw new Error(
      `--docs expects a whole number from 1 to ${MAX_DOCS_PER_COLLECTION}, got "${raw}"`,
    );
  }
  return value;
}

export function parseRetention(raw: unknown): number | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new Error(`--retention expects a ratio from 0 to 1, got "${raw}"`);
  }
  return value;
}

/**
 * Parse and validate the simulation mode option
 */
function parseSimulationMode(
  mode: string | undefined,
  log: (line: string) => void,
): SimulationPowerLevel {
  if (!mode) return "normal";
  const normalized = mode.toLowerCase();
  if (
    normalized === "quick" ||
    normalized === "normal" ||
    normalized === "hard"
  ) {
    return normalized;
  }
  log(yellow(`⚠ Unknown mode "${mode}", using "normal" instead`));
  return "normal";
}

/**
 * Check migrations and schema consistency
 */
export async function checkCommand(
  options: CheckCommandOptions = {},
): Promise<void> {
  const report = await runCheck(options);
  if (report.failure) throw report.failure;
}

export type CheckStage = "no-migrations" | "schema" | "simulation";

export interface CheckMigrationReport {
  id: string;
  name: string;
  valid: boolean;
  errors: string[];
  warnings: string[];
  operationCount?: number;
  reversible?: boolean;
}

export interface CheckReport {
  migrationsDir: string;
  schemaPath: string;
  powerLevel: SimulationPowerLevel;
  lastN?: number;
  docsPerCollection?: number;
  stateRetentionRatio?: number;
  migrationCount: number;
  stage: CheckStage;
  valid: boolean;
  schema?: { valid: boolean; errors: string[]; warnings: string[] };
  migrations: CheckMigrationReport[];
  notValidated: string[];
  failure?: Error;
}

export interface CheckHooks {
  log?: (line: string) => void;
  write?: (chunk: string) => void;
  tty?: boolean;
  onMigrationStart?: MigrationProgressHook;
  onMigrationResult?: MigrationResultHook;
  onSchemaResult?: (
    schema: NonNullable<CheckReport["schema"]>,
  ) => void | Promise<void>;
}

export async function runCheck(
  options: CheckCommandOptions = {},
  hooks: CheckHooks = {},
): Promise<CheckReport> {
  const log = hooks.log ?? ((line: string) => console.log(line));
  const powerLevel = parseSimulationMode(options.mode, log);
  const lastN = options.last && options.last > 0 ? options.last : undefined;
  const docsPerCollection = parseDocsPerCollection(options.docs);
  const stateRetentionRatio = parseRetention(options.retention);

  log(bold(blue("🐝 Checking migrations...")));
  if (
    powerLevel !== "normal" ||
    lastN ||
    docsPerCollection !== undefined ||
    stateRetentionRatio !== undefined
  ) {
    const info = [
      powerLevel !== "normal" ? `mode: ${powerLevel}` : "",
      lastN ? `last: ${lastN}` : "",
      docsPerCollection !== undefined ? `docs: ${docsPerCollection}` : "",
      stateRetentionRatio !== undefined
        ? `retention: ${stateRetentionRatio}`
        : "",
    ]
      .filter(Boolean)
      .join(", ");
    log(dim(`  Options: ${info}`));
  }
  log("");

  // Load configuration
  const cwd = options.cwd || process.cwd();
  const config = await loadConfig({ configPath: options.configPath, cwd });

  const migrationsDir = path.resolve(
    cwd,
    config.paths?.migrations || "./migrations",
  );
  const schemaPath = path.resolve(cwd, config.paths?.schemas || "./schemas.ts");

  log(dim(`Migrations directory: ${migrationsDir}`));
  log(dim(`Schemas file: ${schemaPath}`));
  log("");

  const report: CheckReport = {
    migrationsDir,
    schemaPath,
    powerLevel,
    lastN,
    ...(docsPerCollection !== undefined ? { docsPerCollection } : {}),
    ...(stateRetentionRatio !== undefined ? { stateRetentionRatio } : {}),
    migrationCount: 0,
    stage: "no-migrations",
    valid: true,
    migrations: [],
    notValidated: [],
  };

  // Discover and load migrations
  const migrationsWithFiles = await loadAllMigrations(migrationsDir);

  if (migrationsWithFiles.length === 0) {
    log(yellow("⚠ No migrations found"));
    return report;
  }

  const allMigrations = buildMigrationChain(migrationsWithFiles);
  report.migrationCount = allMigrations.length;

  log(dim(`Found ${allMigrations.length} migration(s)`));
  log("");

  // Validate schema consistency
  report.stage = "schema";
  log(bold("📋 Validating schema consistency..."));
  const schemaValidation = await validateMigrationChainWithProjectSchema(
    allMigrations,
    schemaPath,
  );
  report.schema = schemaValidation;
  await hooks.onSchemaResult?.(schemaValidation);

  if (schemaValidation.warnings.length > 0) {
    log(yellow("\n  Warnings:"));
    for (const warning of schemaValidation.warnings) {
      log(yellow(`    ⚠ ${warning}`));
    }
  }

  if (!schemaValidation.valid) {
    log(red("\n  ✗ Schema validation failed"));
    for (const error of schemaValidation.errors) {
      log(red(`    ${error}`));
    }
    log("");
    report.valid = false;
    report.failure = new Error("Schema validation failed");
    return report;
  }

  log(green("  ✓ Schema consistency validated"));
  log("");

  // Validate each migration with simulation
  report.stage = "simulation";
  const windowed = Boolean(lastN && lastN < allMigrations.length);
  report.notValidated = windowed
    ? allMigrations.slice(0, -lastN!).map((m) => m.id)
    : [];
  const collect = (results: MigrationValidationResult[]) =>
    results.map((result) => ({
      id: result.migration.id,
      name: result.migration.name,
      valid: result.valid,
      errors: result.errors,
      warnings: result.warnings,
      operationCount: result.operationCount,
      reversible: result.reversible,
    }));
  try {
    const results = await validateMigrationsWithSimulation(allMigrations, {
      verbose: options.verbose,
      powerLevel,
      lastN,
      docsPerCollection,
      stateRetentionRatio,
      write: hooks.write,
      tty: hooks.tty,
      onMigrationStart: hooks.onMigrationStart,
      onMigrationResult: hooks.onMigrationResult,
    });
    report.migrations = collect(results);
  } catch (error) {
    if (!(error instanceof MigrationValidationFailedError)) throw error;
    report.migrations = collect(error.results);
    report.valid = false;
    report.failure = error;
  }
  return report;
}

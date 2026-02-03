/**
 * Check command for MongoDBee Migration CLI
 *
 * Validates migrations and schema consistency without applying them
 *
 * @module
 */

import process from "node:process";
import { blue, bold, dim, green, red, yellow } from "@std/fmt/colors";
import * as path from "@std/path";

import { loadConfig } from "../../config/loader.ts";
import {
  buildMigrationChain,
  loadAllMigrations,
} from "../../discovery.ts";
import { validateMigrationChainWithProjectSchema } from "../../schema-validation.ts";
import { validateMigrationsWithSimulation } from "../utils/validate-migrations.ts";
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
}

/**
 * Parse and validate the simulation mode option
 */
function parseSimulationMode(mode?: string): SimulationPowerLevel {
  if (!mode) return "normal";
  const normalized = mode.toLowerCase();
  if (normalized === "quick" || normalized === "normal" || normalized === "hard") {
    return normalized;
  }
  console.log(yellow(`⚠ Unknown mode "${mode}", using "normal" instead`));
  return "normal";
}

/**
 * Check migrations and schema consistency
 */
export async function checkCommand(
  options: CheckCommandOptions = {},
): Promise<void> {
  const powerLevel = parseSimulationMode(options.mode);
  const lastN = options.last && options.last > 0 ? options.last : undefined;

  console.log(bold(blue("🐝 Checking migrations...")));
  if (powerLevel !== "normal" || lastN) {
    const modeInfo = powerLevel !== "normal" ? `mode: ${powerLevel}` : "";
    const lastInfo = lastN ? `last: ${lastN}` : "";
    const info = [modeInfo, lastInfo].filter(Boolean).join(", ");
    console.log(dim(`  Options: ${info}`));
  }
  console.log();

  // Load configuration
  const cwd = options.cwd || process.cwd();
  const config = await loadConfig({ configPath: options.configPath, cwd });

  const migrationsDir = path.resolve(
    cwd,
    config.paths?.migrations || "./migrations",
  );
  const schemaPath = path.resolve(
    cwd,
    config.paths?.schemas || "./schemas.ts",
  );

  console.log(dim(`Migrations directory: ${migrationsDir}`));
  console.log(dim(`Schemas file: ${schemaPath}`));
  console.log();

  // Discover and load migrations
  const migrationsWithFiles = await loadAllMigrations(migrationsDir);
  
  if (migrationsWithFiles.length === 0) {
    console.log(yellow("⚠ No migrations found"));
    return;
  }

  const allMigrations = buildMigrationChain(migrationsWithFiles);

  console.log(dim(`Found ${allMigrations.length} migration(s)`));
  console.log();

  // Validate schema consistency
  console.log(bold("📋 Validating schema consistency..."));
  const schemaValidation = await validateMigrationChainWithProjectSchema(
    allMigrations,
    schemaPath,
  );

  if (schemaValidation.warnings.length > 0) {
    console.log(yellow("\n  Warnings:"));
    for (const warning of schemaValidation.warnings) {
      console.log(yellow(`    ⚠ ${warning}`));
    }
  }

  if (!schemaValidation.valid) {
    console.log(red("\n  ✗ Schema validation failed"));
    for (const error of schemaValidation.errors) {
      console.log(red(`    ${error}`));
    }
    console.log();
    throw new Error("Schema validation failed");
  }

  console.log(green("  ✓ Schema consistency validated"));
  console.log();

  // Validate each migration with simulation
  await validateMigrationsWithSimulation(allMigrations, {
    verbose: options.verbose,
    powerLevel,
    lastN,
  });
}

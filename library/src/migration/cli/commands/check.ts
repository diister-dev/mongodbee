/**
 * Check command for MongoDBee Migration CLI
 *
 * Validates migrations and schema consistency without applying them
 *
 * @module
 */

import { blue, bold, dim, green, red, yellow } from "@std/fmt/colors";
import * as path from "@std/path";

import { loadConfig } from "../../config/loader.ts";
import {
  buildMigrationChain,
  loadAllMigrations,
} from "../../discovery.ts";
import { validateMigrationChainWithProjectSchema } from "../../schema-validation.ts";
import { validateMigrationsWithSimulation } from "../utils/validate-migrations.ts";

export interface CheckCommandOptions {
  configPath?: string;
  cwd?: string;
  verbose?: boolean;
}

/**
 * Check migrations and schema consistency
 */
export async function checkCommand(
  options: CheckCommandOptions = {},
): Promise<void> {
  console.log(bold(blue("🐝 Checking migrations...")));
  console.log();

  // Load configuration
  const cwd = options.cwd || Deno.cwd();
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
  });
}

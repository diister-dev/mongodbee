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
import { createSimulationValidator } from "../../validators/simulation.ts";
import { validateMigrationChainWithProjectSchema } from "../../schema-validation.ts";

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

  try {
    // Load configuration
    console.log(dim("Loading configuration..."));
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
    console.log(dim("Discovering migrations..."));
    const migrationsWithFiles = await loadAllMigrations(migrationsDir);
    
    if (migrationsWithFiles.length === 0) {
      console.log(yellow("⚠ No migrations found"));
      return;
    }

    const allMigrations = buildMigrationChain(migrationsWithFiles);

    console.log(green(`✓ Found ${allMigrations.length} migration(s)`));
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
    console.log(bold("🧪 Validating migrations with simulation..."));
    console.log();

    const simulationValidator = createSimulationValidator({
      validateReversibility: true,
      strictValidation: true,
      maxOperations: 1000,
    });

    let allValid = true;
    const results: Array<{
      migration: typeof allMigrations[0];
      valid: boolean;
      errors: string[];
      warnings: string[];
    }> = [];

    for (const migration of allMigrations) {
      console.log(
        `  ${blue("→")} ${bold(migration.name)} ${dim(`(${migration.id})`)}`,
      );

      try {
        const validationResult = await simulationValidator.validateMigration(
          migration,
        );

        results.push({
          migration,
          valid: validationResult.success,
          errors: validationResult.errors,
          warnings: validationResult.warnings,
        });

        if (validationResult.success) {
          const operationCount = validationResult.data?.operationCount || 0;
          const isReversible = !validationResult.data?.hasIrreversibleProperty;
          
          console.log(
            green(
              `    ✓ Valid (${operationCount} operation${operationCount !== 1 ? "s" : ""}, ${
                isReversible ? "reversible" : "irreversible"
              })`,
            ),
          );

          if (validationResult.warnings.length > 0) {
            for (const warning of validationResult.warnings) {
              console.log(yellow(`      ⚠ ${warning}`));
            }
          }
        } else {
          allValid = false;
          console.log(red(`    ✗ Invalid`));
          for (const error of validationResult.errors) {
            console.log(red(`      ${error}`));
          }
        }

        if (options.verbose && validationResult.warnings.length > 0) {
          for (const warning of validationResult.warnings) {
            console.log(yellow(`      ⚠ ${warning}`));
          }
        }
      } catch (error) {
        allValid = false;
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        console.log(red(`    ✗ Validation error: ${errorMessage}`));

        results.push({
          migration,
          valid: false,
          errors: [errorMessage],
          warnings: [],
        });
      }

      console.log();
    }

    // Summary
    console.log(bold("📊 Summary:"));
    console.log();

    const validCount = results.filter((r) => r.valid).length;
    const invalidCount = results.filter((r) => !r.valid).length;

    console.log(`  Total migrations: ${bold(String(results.length))}`);
    console.log(`  Valid: ${green(bold(String(validCount)))}`);
    if (invalidCount > 0) {
      console.log(`  Invalid: ${red(bold(String(invalidCount)))}`);
    }

    console.log();

    if (allValid) {
      console.log(
        green(bold("✓ All migrations are valid and ready to apply!")),
      );
    } else {
      console.log(
        red(bold("✗ Some migrations have errors. Please fix them before applying.")),
      );
      throw new Error("Migration validation failed");
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(red(bold("Error:")), message);
    throw error;
  }
}

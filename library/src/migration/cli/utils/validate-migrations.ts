/**
 * Shared migration validation utilities
 *
 * @module
 */

import { blue, bold, dim, green, red, yellow } from "@std/fmt/colors";
import type { MigrationDefinition } from "../../types.ts";
import { createSimulationValidator } from "../../validators/simulation.ts";

export interface MigrationValidationResult {
  migration: MigrationDefinition;
  valid: boolean;
  errors: string[];
  warnings: string[];
}

export interface ValidateMigrationsOptions {
  verbose?: boolean;
}

/**
 * Validates all migrations with simulation
 *
 * @param migrations - Migrations to validate
 * @param options - Validation options
 * @returns Array of validation results
 * @throws Error if any migration is invalid
 */
export async function validateMigrationsWithSimulation(
  migrations: MigrationDefinition[],
  options: ValidateMigrationsOptions = {},
): Promise<MigrationValidationResult[]> {
  console.log(bold("🧪 Validating migrations with simulation..."));
  console.log();

  const simulationValidator = createSimulationValidator({
    validateReversibility: true,
    strictValidation: true,
    maxOperations: 1000,
  });

  let allValid = true;
  const results: MigrationValidationResult[] = [];

  for (const migration of migrations) {
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

  if (!allValid) {
    console.log(
      red(bold("✗ Some migrations have errors. Please fix them before applying.")),
    );
    throw new Error("Migration validation failed");
  }

  console.log(
    green(bold("✓ All migrations are valid and ready to apply!")),
  );

  return results;
}

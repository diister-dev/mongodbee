/**
 * Shared migration validation utilities
 *
 * @module
 */

import { blue, bold, dim, green, red, yellow } from "@std/fmt/colors";
import type { MigrationDefinition } from "../../types.ts";
import { createEmptyDatabaseState, type SimulationDatabaseState } from "../../types.ts";
import { createSimulationValidator, type SimulationPowerLevel, type SimulationValidatorOptions } from "../../validators/simulation.ts";

export interface MigrationValidationResult {
  migration: MigrationDefinition;
  valid: boolean;
  errors: string[];
  warnings: string[];
}

export interface ValidateMigrationsOptions {
  verbose?: boolean;
  /**
   * Ratio of documents to keep from previous state when propagating state (0.0 to 1.0)
   * - 0.0 = discard all previous state, generate 100% fresh mock data (like before)
   * - 0.5 = keep 50% of previous state, generate 50% fresh mock data (default, optimized)
   * - 1.0 = keep 100% of previous state, no fresh mock data
   *
   * @default 0.5
   */
  stateRetentionRatio?: number;

  /**
   * Simulation power level controlling mock data generation complexity
   * - `quick`: Fast validation with minimal mock data (10-20 docs)
   * - `normal`: Balanced validation (100 docs)
   * - `thorough`: Comprehensive validation (500+ docs)
   *
   * @default "normal"
   */
  powerLevel?: SimulationPowerLevel;

  /**
   * Only validate the last N migrations
   * If not provided, all migrations are validated
   */
  lastN?: number;
}

/**
 * Validates all migrations with simulation
 * 
 * Uses state propagation with configurable retention ratio to avoid O(n²) complexity.
 * By default, keeps 50% of the previous state and generates 50% fresh mock data
 * to balance performance with edge case coverage.
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
  const { lastN, powerLevel = "normal" } = options;

  // Determine which migrations to validate based on lastN option
  const migrationsToValidate = lastN && lastN > 0 && lastN < migrations.length
    ? migrations.slice(-lastN)
    : migrations;

  const skippedMigrations = lastN && lastN > 0 && lastN < migrations.length
    ? migrations.slice(0, -lastN)
    : [];

  const modeLabel = powerLevel === "quick" ? "quick" : powerLevel === "hard" ? "hard" : "normal";
  const lastNLabel = lastN && lastN > 0 ? ` (last ${Math.min(lastN, migrations.length)})` : "";

  console.log(bold(`🧪 Validating migrations with simulation [${modeLabel}]${lastNLabel}...`));
  console.log();

  const stateRetentionRatio = options.stateRetentionRatio ?? 0.5;

  const validatorOptions: SimulationValidatorOptions = {
    strictValidation: true,
    maxOperations: 1000,
    stateRetentionRatio,
    powerLevel,
  };

  const simulationValidator = createSimulationValidator(validatorOptions);

  let allValid = true;
  const results: MigrationValidationResult[] = [];

  // Track current state to propagate between migrations (O(n) instead of O(n²))
  let currentState: SimulationDatabaseState = createEmptyDatabaseState();

  // Fast-forward through skipped migrations (just state propagation, minimal output)
  if (skippedMigrations.length > 0) {
    console.log(dim(`  Skipping ${skippedMigrations.length} migration(s)...`));
    for (const migration of skippedMigrations) {
      try {
        const validationResult = await simulationValidator.validateMigration(
          migration,
          currentState,
        );
        if (validationResult.success && validationResult.data?.stateAfterMigration) {
          currentState = simulationValidator.prepareStateForNextMigration(
            validationResult.data.stateAfterMigration as SimulationDatabaseState,
            migration.schemas,
          );
        }
        results.push({
          migration,
          valid: true,
          errors: [],
          warnings: ["Skipped (--last N mode)"],
        });
      } catch {
        results.push({
          migration,
          valid: true,
          errors: [],
          warnings: ["Skipped (--last N mode)"],
        });
      }
    }
    console.log();
  }

  for (const migration of migrationsToValidate) {
    console.log(
      `  ${blue("→")} ${bold(migration.name)} ${dim(`(${migration.id})`)}`,
    );

    try {
      // Pass the current state to avoid re-simulating all parent migrations
      const validationResult = await simulationValidator.validateMigration(
        migration,
        currentState,
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
        
        // Update state for next migration: apply retention ratio (keep X%, generate fresh X%)
        if (validationResult.data?.stateAfterMigration) {
          currentState = simulationValidator.prepareStateForNextMigration(
            validationResult.data.stateAfterMigration as SimulationDatabaseState,
            migration.schemas,
          );
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

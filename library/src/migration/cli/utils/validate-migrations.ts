/**
 * Shared migration validation utilities
 *
 * @module
 */

import { bold, dim, green, red, yellow } from "@std/fmt/colors";
import type { MigrationDefinition } from "../../types.ts";
import {
  createEmptyDatabaseState,
  type SimulationDatabaseState,
} from "../../types.ts";
import {
  createSimulationValidator,
  type SimulationPowerLevel,
  type SimulationValidatorOptions,
} from "../../validators/simulation.ts";
import {
  digestWarnings,
  formatWarningDigest,
  type WarningSource,
} from "./warning-digest.ts";
import { createStepReporter, type StepReporter } from "./step-reporter.ts";

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

  /**
   * Render transient "in flight" step lines. Defaults to
   * `Deno.stdout.isTerminal()` — off in CI, pipes and the in-process test
   * harness, where `\r` would be garbage.
   */
  tty?: boolean;

  /**
   * Sink for every rendered chunk. Defaults to stdout; injectable so the
   * reporting can be asserted without a terminal.
   */
  write?: (chunk: string) => void;
}

/** Formats a step counter as a right-aligned `[ 3/12]`. */
function counter(index: number, total: number): string {
  const width = String(total).length;
  return `[${String(index).padStart(width)}/${total}]`;
}

/**
 * Display-only annotation for a migration that failed outside the `--last N`
 * window. It never enters `MigrationValidationResult.errors` — that array is
 * part of the consumed return contract.
 */
const SKIPPED_RANGE_NOTE = "outside the --last N window";

/** A failing migration plus an optional display-only annotation. */
interface ReportedFailure {
  result: MigrationValidationResult;
  note?: string;
}

/**
 * Prints the failing migrations and their errors.
 *
 * Errors are deferred to the very end of the run on purpose: with a
 * deduplicated warning digest the tail is short, so the last thing on screen
 * is the verdict — the complaint that started this was an `✗ Invalid` buried
 * under ~150 repeated warning lines.
 */
function reportFailures(
  steps: StepReporter,
  failures: ReportedFailure[],
  total: number,
): void {
  steps.log(
    red(
      bold(
        `✗ Validation FAILED — ${failures.length} of ${total} migration(s) have errors`,
      ),
    ),
  );
  steps.log("");
  for (const { result, note } of failures) {
    const suffix = note ? ` ${dim(`— ${note}`)}` : "";
    steps.log(
      red(
        `  ✗ ${result.migration.name} ${
          dim(`(${result.migration.id})`)
        }${suffix}`,
      ),
    );
    for (const error of result.errors) {
      steps.log(red(`      ${error}`));
    }
  }
  steps.log("");
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

  const modeLabel = powerLevel === "quick"
    ? "quick"
    : powerLevel === "hard"
    ? "hard"
    : "normal";
  const lastNLabel = lastN && lastN > 0
    ? ` (last ${Math.min(lastN, migrations.length)})`
    : "";

  const steps = createStepReporter({ tty: options.tty, write: options.write });

  steps.log(
    bold(
      `🧪 Validating migrations with simulation [${modeLabel}]${lastNLabel}...`,
    ),
  );
  steps.log("");

  const stateRetentionRatio = options.stateRetentionRatio ?? 0.5;

  const validatorOptions: SimulationValidatorOptions = {
    maxOperations: 1000,
    stateRetentionRatio,
    powerLevel,
  };

  const simulationValidator = createSimulationValidator(validatorOptions);

  let allValid = true;
  const results: MigrationValidationResult[] = [];

  // Reporting accumulators. Warnings are folded chain-wide and errors are held
  // back to the verdict block, so neither can bury the other.
  const warningSources: WarningSource[] = [];
  const failures: ReportedFailure[] = [];

  // Track current state to propagate between migrations (O(n) instead of O(n²))
  let currentState: SimulationDatabaseState = createEmptyDatabaseState();

  // Fast-forward through skipped migrations. We still run the full simulation
  // (it is needed to propagate state), so its verdict is authoritative: a
  // broken migration outside the --last N window is still broken and must NOT
  // be hidden behind a green "all valid" banner. Only the *reporting detail*
  // is reduced for skipped migrations, never the correctness gate.
  if (skippedMigrations.length > 0) {
    let skippedIndex = 0;
    for (const migration of skippedMigrations) {
      skippedIndex++;
      steps.start(
        dim(
          `  ⏭  fast-forward ${
            counter(skippedIndex, skippedMigrations.length)
          } ${migration.name}`,
        ),
      );
      try {
        const validationResult = await simulationValidator.validateMigration(
          migration,
          currentState,
        );
        if (validationResult.success) {
          if (validationResult.data?.stateAfterMigration) {
            currentState = simulationValidator.prepareStateForNextMigration(
              validationResult.data
                .stateAfterMigration as SimulationDatabaseState,
              migration.schemas,
            );
          }
          results.push({
            migration,
            valid: true,
            errors: [],
            warnings: ["Skipped (--last N mode)"],
          });
        } else {
          allValid = false;
          const result: MigrationValidationResult = {
            migration,
            valid: false,
            errors: validationResult.errors,
            warnings: validationResult.warnings,
          };
          results.push(result);
          failures.push({ result, note: SKIPPED_RANGE_NOTE });
          warningSources.push({
            migrationId: migration.id,
            warnings: validationResult.warnings,
          });
        }
      } catch (error) {
        allValid = false;
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        const result: MigrationValidationResult = {
          migration,
          valid: false,
          errors: [errorMessage],
          warnings: [],
        };
        results.push(result);
        failures.push({ result, note: SKIPPED_RANGE_NOTE });
      }
    }
    steps.log(
      dim(`  ⏭  ${skippedMigrations.length} migration(s) fast-forwarded`),
    );
  }

  let index = 0;
  for (const migration of migrationsToValidate) {
    index++;
    const step = `${counter(index, migrationsToValidate.length)} ${
      bold(migration.name)
    } ${dim(`(${migration.id})`)}`;
    steps.start(`  ${dim("…")} ${step}`);

    try {
      // Pass the current state to avoid re-simulating all parent migrations
      const validationResult = await simulationValidator.validateMigration(
        migration,
        currentState,
      );

      const result: MigrationValidationResult = {
        migration,
        valid: validationResult.success,
        errors: validationResult.errors,
        warnings: validationResult.warnings,
      };
      results.push(result);
      warningSources.push({
        migrationId: migration.id,
        warnings: validationResult.warnings,
      });

      const warned = validationResult.warnings.length > 0
        ? ` ${yellow(`⚠ ${validationResult.warnings.length}`)}`
        : "";

      if (validationResult.success) {
        const operationCount = validationResult.data?.operationCount || 0;
        const isReversible = !validationResult.data?.hasIrreversibleProperty;

        steps.done(
          `  ${green("✓")} ${step} ${
            dim(
              `${operationCount} operation${operationCount !== 1 ? "s" : ""}, ${
                isReversible ? "reversible" : "irreversible"
              }`,
            )
          }${warned}`,
        );

        // Update state for next migration: apply retention ratio (keep X%, generate fresh X%)
        if (validationResult.data?.stateAfterMigration) {
          currentState = simulationValidator.prepareStateForNextMigration(
            validationResult.data
              .stateAfterMigration as SimulationDatabaseState,
            migration.schemas,
          );
        }
      } else {
        allValid = false;
        failures.push({ result });
        steps.done(
          `  ${red("✗")} ${step} ${
            red(
              `${validationResult.errors.length} error${
                validationResult.errors.length !== 1 ? "s" : ""
              }`,
            )
          }${warned}`,
        );
      }

      // `--verbose` restores the pre-digest firehose: every warning under the
      // migration that produced it, uncorrelated and uncapped.
      if (options.verbose) {
        for (const warning of validationResult.warnings) {
          steps.log(yellow(`      ⚠ ${warning}`));
        }
      }
    } catch (error) {
      allValid = false;
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      const result: MigrationValidationResult = {
        migration,
        valid: false,
        errors: [errorMessage],
        warnings: [],
      };
      results.push(result);
      failures.push({ result });
      steps.done(`  ${red("✗")} ${step} ${red("validation error")}`);
    }
  }

  steps.finish();
  steps.log("");

  const validCount = results.filter((r) => r.valid).length;
  const invalidCount = results.filter((r) => !r.valid).length;

  steps.log(bold("📊 Summary:"));
  steps.log("");
  steps.log(`  Total migrations: ${bold(String(results.length))}`);
  steps.log(`  Valid: ${green(bold(String(validCount)))}`);
  if (invalidCount > 0) {
    steps.log(`  Invalid: ${red(bold(String(invalidCount)))}`);
  }
  steps.log("");

  // Chain-invariant findings (an ambiguous identifier space, a reference
  // nobody mints) are properties of the MODEL, so the simulation re-reports
  // them under every migration. Fold them once here instead.
  const digest = digestWarnings(warningSources);
  const digestLines = formatWarningDigest(digest, {
    totalMigrations: migrationsToValidate.length,
    verbose: options.verbose,
  });
  if (digestLines.length > 0) {
    for (const line of digestLines) steps.log(line ? yellow(line) : "");
    if (!options.verbose && digest.occurrences > digest.groups.length) {
      steps.log("");
      steps.log(
        dim(
          "  Run with --verbose to see every warning under the migration that raised it.",
        ),
      );
    }
    steps.log("");
  }

  if (!allValid) {
    reportFailures(steps, failures, results.length);
    throw new Error("Migration validation failed");
  }

  steps.log(
    green(bold("✓ All migrations are valid and ready to apply!")),
  );

  return results;
}

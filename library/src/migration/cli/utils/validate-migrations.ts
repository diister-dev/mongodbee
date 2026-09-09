/**
 * Shared migration validation utilities
 *
 * @module
 */

import { bold, dim, green, red, yellow } from "../../../utils/colors.ts";
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

/**
 * Result of simulating ONE migration.
 *
 * Only simulated migrations get one: a migration outside the `--last N`
 * window is absent from the returned array rather than carried as a green
 * entry, so "nothing was found" can never be read off something where
 * nothing was looked for.
 */
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
   * Only validate the last N migrations. If not provided, all migrations are
   * validated.
   *
   * Literally only those N: the window is seeded from its first migration's
   * PARENT SCHEMAS, and the earlier migrations are neither simulated nor
   * reported on. A caller that then ACTS on the chain (applying it, say)
   * owns the gap — see `migrateCommand`, which widens the window to cover
   * every migration it is about to apply.
   */
  lastN?: number;

  /**
   * Render transient "in flight" step lines. Defaults to
   * `process.stdout.isTTY` — off in CI, pipes and the in-process test
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

/** A failing migration plus an optional display-only annotation. */
interface ReportedFailure {
  result: MigrationValidationResult;
  note?: string;
}

/**
 * Thrown when a validated migration is invalid.
 *
 * Carries the per-migration results, which the run has already computed: a
 * caller that catches the throw would otherwise be left with the message and
 * no way to see WHICH migration failed on WHAT without re-running everything.
 */
export class MigrationValidationFailedError extends Error {
  /** The per-migration results, so callers can report WHICH one failed. */
  readonly results: MigrationValidationResult[];

  constructor(results: MigrationValidationResult[]) {
    super("Migration validation failed");
    this.name = "MigrationValidationFailedError";
    this.results = results;
  }
}

/**
 * Prints the failing migrations and their errors.
 *
 * Errors are deferred to the very end on purpose: with the warning digest
 * deduplicated the tail is short, so the verdict is the last thing on screen
 * rather than buried under repeated warning lines.
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
        `  ✗ ${result.migration.name} ${dim(
          `(${result.migration.id})`,
        )}${suffix}`,
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
 * @returns Validation results for the migrations that were simulated
 * @throws Error if any validated migration is invalid
 */
export async function validateMigrationsWithSimulation(
  migrations: MigrationDefinition[],
  options: ValidateMigrationsOptions = {},
): Promise<MigrationValidationResult[]> {
  const { lastN, powerLevel = "normal" } = options;

  // Determine which migrations to validate based on lastN option
  const windowed = Boolean(lastN && lastN > 0 && lastN < migrations.length);
  const migrationsToValidate = windowed
    ? migrations.slice(-lastN!)
    : migrations;

  const notValidated = windowed ? migrations.slice(0, -lastN!) : [];

  const modeLabel =
    powerLevel === "quick"
      ? "quick"
      : powerLevel === "hard"
        ? "hard"
        : "normal";
  const lastNLabel =
    lastN && lastN > 0 ? ` (last ${Math.min(lastN, migrations.length)})` : "";

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
    // The in-flight line is driven by the work, not by a clock: the validator
    // never yields to the event loop, so a timer-based animation cannot fire
    // (it did not, for 17 seconds at a stretch). The reporter throttles the
    // redraws; this callback stays a plain function call.
    onProgress: (note) => steps.update(note),
  };

  const simulationValidator = createSimulationValidator(validatorOptions);

  let allValid = true;
  const results: MigrationValidationResult[] = [];

  // Reporting accumulators. Warnings are folded chain-wide and errors are held
  // back to the verdict block, so neither can bury the other.
  const warningSources: WarningSource[] = [];
  const failures: ReportedFailure[] = [];

  // The state the first validated migration starts from.
  //
  // Full chain: the empty database the root migration builds on.
  //
  // `--last N`: `undefined`, which sends `validateMigration` down
  // SimulationValidator's standalone path — it reaches the window's entry
  // state from the parent's DECLARED SCHEMAS (ancestor operations replayed on
  // an empty database for the real seeds, then mock-populated) instead of
  // running a full simulation per migration just to hand a state forward.
  // Those simulations were the entire cost of the flag: `--last 1` used to be
  // SLOWER than the full check it was meant to shortcut.
  //
  // The trade is the one the flag advertises: migrations outside the window
  // are not validated at all, so they can no longer be reported either way —
  // see `notValidated` below, and `migrate`, which never narrows the window
  // below the set of migrations it is about to apply.
  let currentState: SimulationDatabaseState | undefined = windowed
    ? undefined
    : createEmptyDatabaseState();

  let index = 0;
  for (const migration of migrationsToValidate) {
    index++;
    const step = `${counter(index, migrationsToValidate.length)} ${bold(
      migration.name,
    )} ${dim(`(${migration.id})`)}`;
    // The reporter adds its own in-flight marker; a static `…` here reads as two.
    steps.start(`  ${step}`);

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

      const warned =
        validationResult.warnings.length > 0
          ? ` ${yellow(`⚠ ${validationResult.warnings.length}`)}`
          : "";

      if (validationResult.success) {
        const operationCount = validationResult.data?.operationCount || 0;
        const isReversible = !validationResult.data?.hasIrreversibleProperty;

        steps.done(
          `  ${green("✓")} ${step} ${dim(
            `${operationCount} operation${operationCount !== 1 ? "s" : ""}, ${
              isReversible ? "reversible" : "irreversible"
            }`,
          )}${warned}`,
        );

        // Update state for next migration: apply retention ratio (keep X%, generate fresh X%)
        if (validationResult.data?.stateAfterMigration) {
          // This phase runs AFTER the verdict landed, so it owned no line —
          // and it is the slowest of the whole loop (retention + mock refill
          // measured at 5-16s per migration). That silence was the longest
          // window on screen; it now gets a transient line of its own,
          // dropped again before the next migration opens its.
          steps.start(dim(`  ⤷ propagating state to the next migration`));
          currentState = simulationValidator.prepareStateForNextMigration(
            validationResult.data
              .stateAfterMigration as SimulationDatabaseState,
            migration.schemas,
          );
          steps.finish();
        }
      } else {
        allValid = false;
        failures.push({ result });
        steps.done(
          `  ${red("✗")} ${step} ${red(
            `${validationResult.errors.length} error${
              validationResult.errors.length !== 1 ? "s" : ""
            }`,
          )}${warned}`,
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
      const errorMessage =
        error instanceof Error ? error.message : String(error);
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
  if (notValidated.length > 0) {
    steps.log(`  Not validated: ${yellow(bold(String(notValidated.length)))}`);
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
    throw new MigrationValidationFailedError(results);
  }

  // A windowed run has no opinion on the migrations it never simulated, so it
  // must not borrow the full check's all-clear. Saying which ones were left
  // out is the whole difference between a shortcut and a blind spot.
  if (notValidated.length > 0) {
    steps.log(
      green(
        bold(
          `✓ The last ${results.length} migration(s) are valid and ready to apply!`,
        ),
      ),
    );
    steps.log(
      yellow(
        `  ${notValidated.length} earlier migration(s) were NOT validated — run without --last for the full chain.`,
      ),
    );
  } else {
    steps.log(green(bold("✓ All migrations are valid and ready to apply!")));
  }

  return results;
}

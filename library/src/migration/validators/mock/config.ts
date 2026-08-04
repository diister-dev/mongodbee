/**
 * @fileoverview Mock generation configuration for the simulation validator
 *
 * Groups the knobs that actually drive mock data generation. Every field
 * declared here is read by the engine in `populate.ts` — configuration that
 * promises behavior nothing implements is worse than no configuration,
 * because it lets a reader believe a lever exists when it does not.
 *
 * @module
 */

/**
 * Simulation power levels for controlling mock data generation complexity
 *
 * - `quick`: Fast validation with minimal mock data (10 docs per collection)
 *   Best for: Quick checks, CI pipelines, development iterations
 *
 * - `normal`: Balanced validation with moderate mock data (100 docs per collection)
 *   Best for: Regular validation, pre-commit checks
 *
 * - `hard`: Comprehensive validation with extensive mock data (500 docs per collection)
 *   Best for: Pre-release validation, catching edge cases
 */
export type SimulationPowerLevel = "quick" | "normal" | "hard";

/**
 * Resolved mock generation configuration for a power level.
 *
 * Historical note: earlier revisions also exposed `DOCS_PER_TYPE_MIN/MAX`
 * and `MIN_SPARSE_THRESHOLD`. No code path ever read them — the per-type
 * volume is derived from the per-collection count (see `populate.ts`), and
 * sparseness is measured against `DOCS_PER_COLLECTION_MIN`. They were
 * removed rather than kept as decoys.
 */
export interface MockGenerationConfig {
  DOCS_PER_COLLECTION_MIN: number;
  DOCS_PER_COLLECTION_MAX: number;
  DEFAULT_STATE_RETENTION_RATIO: number;
}

/**
 * Default ratio of documents to keep from the previous state (0.0 to 1.0)
 * when propagating state between migrations.
 */
export const DEFAULT_STATE_RETENTION_RATIO = 0.5;

/**
 * Number of synthetic instances generated per multi-model.
 *
 * One instance is enough to exercise every type schema of a model — extra
 * instances would only duplicate the same assertions. A comment used to
 * claim this was "configurable via constants"; it never was, so the constant
 * now states the real behavior instead of promising a knob that does not
 * exist. The planned correlated-data generation may make this configurable
 * for real.
 */
export const INSTANCES_PER_MODEL = 1;

/**
 * Configuration presets for each power level
 */
const POWER_LEVEL_PRESETS: Record<SimulationPowerLevel, {
  docsPerCollectionMin: number;
  docsPerCollectionMax: number;
}> = {
  quick: {
    docsPerCollectionMin: 10,
    docsPerCollectionMax: 10,
  },
  normal: {
    docsPerCollectionMin: 100,
    docsPerCollectionMax: 100,
  },
  hard: {
    docsPerCollectionMin: 500,
    docsPerCollectionMax: 500,
  },
};

/**
 * Gets the mock generation constants for a given power level
 *
 * @param powerLevel - The simulation power level
 * @returns Mock generation configuration for the specified level
 */
export function getMockGenerationConfig(
  powerLevel: SimulationPowerLevel = "normal",
): MockGenerationConfig {
  const preset = POWER_LEVEL_PRESETS[powerLevel];
  return {
    DOCS_PER_COLLECTION_MIN: preset.docsPerCollectionMin,
    DOCS_PER_COLLECTION_MAX: preset.docsPerCollectionMax,
    DEFAULT_STATE_RETENTION_RATIO,
  };
}

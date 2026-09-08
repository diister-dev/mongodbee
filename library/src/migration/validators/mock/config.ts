/**
 * @fileoverview Mock generation configuration for the simulation validator
 *
 * Volume knobs read by the population engine in `populate.ts`.
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
 * Number of synthetic instances generated for a multi-model whose identifier
 * space holds no pooled entity — nothing to be one-per-entity with, so the
 * model still gets one instance to exercise its type schemas.
 */
export const INSTANCES_PER_MODEL = 1;

/**
 * Configuration presets for each power level.
 *
 * Multi-model volume stays linear without a cap: instances follow the entity
 * pool (N), and the per-model batch budget divides the document count across
 * them (see `drawInstanceBatchCount` in `populate.ts`).
 */
const POWER_LEVEL_PRESETS: Record<
  SimulationPowerLevel,
  {
    docsPerCollectionMin: number;
    docsPerCollectionMax: number;
  }
> = {
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

/**
 * @fileoverview Mock data generation for the simulation validator
 *
 * Everything the simulation validator needs to fabricate database state:
 * volume configuration per power level (`config.ts`), single-document
 * generation on top of valibot-mock (`generator.ts`), the shared population
 * engine both state-building paths call (`populate.ts`), and the folding of
 * generation failures into validation output (`failures.ts`).
 *
 * @module
 */

export {
  DEFAULT_STATE_RETENTION_RATIO,
  getMockGenerationConfig,
  INSTANCES_PER_MODEL,
  type MockGenerationConfig,
  type SimulationPowerLevel,
} from "./config.ts";

export {
  generateMockDocument,
  generateMockScopeValue,
  type MockDocumentOptions,
} from "./generator.ts";

export {
  type CorrelationSession,
  type CorrelationSessionOptions,
  createCorrelationSession,
  type DocTarget,
  type MintRequest,
  schemasFingerprint,
} from "./correlation.ts";

export {
  type MockPopulateContext,
  populateCollections,
  populateDeclaredBuckets,
  populateExistingMultiModelInstances,
  populateMultiCollections,
  type PopulatePolicy,
  populateScopedMultiCollections,
  populateSyntheticMultiModelInstances,
  retainAndRefreshBuckets,
} from "./populate.ts";

export { foldMockGenerationFailures } from "./failures.ts";

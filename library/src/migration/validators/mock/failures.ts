/**
 * @fileoverview Turning mock-generation failures into validation output
 *
 * The generation engine (`populate.ts`) only RECORDS failures — deciding what
 * a failure means for the validation verdict lives here, in one place, so the
 * standalone path (`buildMockStateFromSchemas`) and the propagation path
 * (`prepareStateForNextMigration` → next `validateMigration`) cannot drift
 * on severity semantics.
 *
 * @module
 */

import type {
  DatabaseState,
  MockGenerationFailure,
  SchemasDefinition,
} from "../../types.ts";

type MockGenerationBucket = keyof DatabaseState;

/** Human label per bucket, reused by every failure message. */
const BUCKET_LABELS: Record<MockGenerationBucket, string> = {
  collections: "collection",
  multiCollections: "multi-collection",
  multiModels: "multi-model instance",
  scopedMultiCollections: "scoped multi-collection",
};

/**
 * Per-bucket probes answering the two questions severity depends on:
 * does the current schema still declare the failed collection, and does the
 * validated state hold any document for it? multiModels is the odd one out —
 * declaration is per MODEL while the state is keyed per INSTANCE, so its
 * probes go through `modelType`.
 */
const BUCKET_PROBES: {
  [K in MockGenerationBucket]: {
    isDeclared: (
      schemas: SchemasDefinition,
      failure: MockGenerationFailure,
    ) => boolean;
    hasDocuments: (
      state: DatabaseState,
      failure: MockGenerationFailure,
    ) => boolean;
  };
} = {
  collections: {
    isDeclared: (schemas, failure) =>
      schemas.collections?.[failure.collection] !== undefined,
    hasDocuments: (state, failure) =>
      (state.collections[failure.collection]?.content.length ?? 0) > 0,
  },
  multiCollections: {
    isDeclared: (schemas, failure) =>
      schemas.multiCollections?.[failure.collection] !== undefined,
    hasDocuments: (state, failure) =>
      (state.multiCollections[failure.collection]?.content.length ?? 0) > 0,
  },
  multiModels: {
    isDeclared: (schemas, failure) =>
      failure.modelType !== undefined &&
      schemas.multiModels?.[failure.modelType] !== undefined,
    hasDocuments: (state, failure) =>
      Object.values(state.multiModels).some(
        (instance) =>
          instance.modelType === failure.modelType &&
          instance.content.length > 0,
      ),
  },
  scopedMultiCollections: {
    isDeclared: (schemas, failure) =>
      schemas.scopedMultiCollections?.[failure.collection] !== undefined,
    hasDocuments: (state, failure) =>
      (state.scopedMultiCollections[failure.collection]?.content.length ?? 0) >
        0,
  },
};

/**
 * Folds recorded generation failures into validation errors and warnings.
 *
 * Severity rule: a failure is a BLOCKING error when the schema still declares
 * the collection and the validated state holds zero documents for it — the
 * downstream validation loops iterate `content`, so zero documents means zero
 * assertions, which is precisely the false-green the old `catch { break }`
 * produced. Any other failure (documents survived from seeds or retention)
 * degrades coverage but not the verdict, so it is reported as a warning.
 *
 * @param failures - Failures recorded by the generation engine
 * @param schemas - The schemas of the migration being validated
 * @param state - The state the validation actually checks (after migration)
 */
export function foldMockGenerationFailures(
  failures: MockGenerationFailure[],
  schemas: SchemasDefinition,
  state: DatabaseState,
): { errors: string[]; warnings: string[] } {
  const errors: string[] = [];
  const warnings: string[] = [];

  for (const failure of failures) {
    const probe = BUCKET_PROBES[failure.bucket];
    const label = BUCKET_LABELS[failure.bucket];

    // Correlation findings describe degraded FIDELITY, never absent
    // coverage: the documents exist, only their identities failed to
    // coincide. They are never blocking — unlike a generation failure,
    // where an empty-but-declared target stays an error.
    if (failure.kind === "correlation") {
      warnings.push(`Mock identity correlation: ${failure.message}`);
      continue;
    }

    const message =
      `Mock data generation failed for ${label} "${failure.collection}": ${failure.message}`;

    if (
      probe.isDeclared(schemas, failure) &&
      !probe.hasDocuments(state, failure)
    ) {
      errors.push(
        `${message} — the ${label} has no documents while its schema declares some, ` +
          `so schema validation would silently check nothing`,
      );
    } else {
      warnings.push(message);
    }
  }

  return { errors, warnings };
}

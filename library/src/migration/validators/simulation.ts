/**
 * @fileoverview Simulation-based migration validator
 *
 * This module provides a validator that uses the SimulationApplier to validate
 * migrations before applying them to real databases. It ensures migrations
 * can be executed and reversed properly in a safe, in-memory environment.
 *
 * @example
 * ```typescript
 * import { createSimulationValidator } from "@diister/mongodbee/migration/validators";
 *
 * const validator = createSimulationValidator();
 * const result = await validator.validateMigration(migrationDefinition);
 *
 * if (!result.success) {
 *   console.error("Migration validation failed:", result.errors);
 * }
 * ```
 *
 * @module
 */

import type {
  MigrationDefinition,
  MigrationRule,
  MockGenerationFailure,
  SchemasDefinition,
} from "../types.ts";
import {
  createEmptyDatabaseState,
  type SimulationDatabaseState,
} from "../types.ts";
import { getIrreversibleOperations, migrationBuilder } from "../builder.ts";
import * as v from "valibot";
import { dirtyEquivalent } from "../../utils/object.ts";
import { createMemoryApplier } from "../appliers/memory.ts";
import {
  type CorrelationSession,
  createCorrelationSession,
  DEFAULT_STATE_RETENTION_RATIO,
  foldMockGenerationFailures,
  getMockGenerationConfig,
  type MockGenerationConfig,
  type MockPopulateContext,
  populateDeclaredBuckets,
  populateExistingMultiModelInstances,
  retainAndRefreshBuckets,
  schemasFingerprint,
  type SimulationPowerLevel,
} from "./mock/mod.ts";
import { fnv1a32 } from "../utils/seed-id.ts";
import {
  deleteWarnings,
  isDocumentDelete,
  snapshotIds,
} from "./delete-checks.ts";

// Mock generation lives in ./mock/ — these stay re-exported here because
// this file is their historical import path.
export { getMockGenerationConfig } from "./mock/mod.ts";
export type { SimulationPowerLevel } from "./mock/mod.ts";

/**
 * Validation result from validators
 */
export type ValidationResult = {
  /** Whether validation passed */
  success: boolean;

  /** Validation errors (blocking) */
  errors: string[];

  /** Validation warnings (non-blocking) */
  warnings: string[];

  /** Additional validation data */
  data?: Record<string, unknown>;
};

/**
 * Validator interface for migration validation
 */
export type MigrationValidator = {
  /** Validates a complete migration definition */
  validateMigration: (
    definition: MigrationDefinition,
    initialState?: SimulationDatabaseState,
  ) => Promise<ValidationResult>;
};

/**
 * Configuration options for the simulation validator
 */
export interface SimulationValidatorOptions {
  /** Maximum number of operations to validate (for performance) */
  maxOperations?: number;

  /**
   * Ratio of documents to keep from previous state when propagating state (0.0 to 1.0)
   * - 0.0 = discard all previous state, generate 100% fresh mock data
   * - 0.5 = keep 50% of previous state, generate 50% fresh mock data (default)
   * - 1.0 = keep 100% of previous state, no fresh mock data
   *
   * This allows testing both:
   * - Existing data transformations (retained portion)
   * - Edge cases with fresh data (new mock portion)
   *
   * @default 0.5
   */
  stateRetentionRatio?: number;

  /**
   * Simulation power level controlling mock data generation complexity
   * - `quick`: Fast validation with minimal mock data (10-20 docs)
   * - `normal`: Balanced validation (100 docs) - default
   * - `hard`: Comprehensive validation (500+ docs)
   *
   * @default "normal"
   */
  powerLevel?: SimulationPowerLevel;

  /**
   * Called as the simulation advances, with a short note naming what it is
   * doing right now ("applying operation 7/19", "mocking +expositions
   * 40/100"). Purely observational: the validator never reads it back, and a
   * run with and without it reaches byte-identical verdicts.
   *
   * It exists because this class is where a `check` spends its minutes and
   * none of that time yields to the event loop — every `await` here resolves
   * synchronously, so a caller cannot animate anything on a timer. Reporting
   * from the work is the only channel that reaches the screen.
   *
   * The callback runs inside the hot loops (per document, per operation), so
   * it must be cheap and must not throw; throttling is the caller's job.
   *
   * @default undefined - nothing is reported
   */
  onProgress?: (note: string) => void;
}

/**
 * Default validator configuration
 */
export const DEFAULT_SIMULATION_VALIDATOR_OPTIONS: SimulationValidatorOptions =
  {
    maxOperations: 1000,
    stateRetentionRatio: DEFAULT_STATE_RETENTION_RATIO,
    powerLevel: "normal",
  };

/**
 * Simulation-based migration validator
 *
 * This validator uses the memory applier to validate migrations in an
 * in-memory environment before they are applied to real databases.
 */
export class SimulationValidator implements MigrationValidator {
  private readonly options: SimulationValidatorOptions;
  private readonly mockConfig: MockGenerationConfig;
  /** {@link SimulationValidatorOptions.onProgress}, defaulted to a no-op. */
  private readonly report: (note: string) => void;

  constructor(options: SimulationValidatorOptions = {}) {
    this.options = { ...DEFAULT_SIMULATION_VALIDATOR_OPTIONS, ...options };
    this.mockConfig = getMockGenerationConfig(this.options.powerLevel);
    this.report = options.onProgress ?? (() => {});
  }

  /** Population context carrying the progress channel into the mock engine. */
  private mockContext(
    failures: MockGenerationFailure[],
    session: CorrelationSession,
  ): MockPopulateContext {
    return {
      config: this.mockConfig,
      failures,
      session,
      onProgress: this.options.onProgress,
    };
  }

  /**
   * Validates a complete migration definition
   *
   * @param definition - The migration definition to validate
   * @param initialState - Optional initial database state (from parent migration)
   *                       If not provided, will generate mock state from parent schemas
   * @returns Validation result with success status, errors, and warnings
   */
  async validateMigration(
    definition: MigrationDefinition,
    initialState?: SimulationDatabaseState,
  ): Promise<ValidationResult> {
    const errors: string[] = [];
    const warnings: string[] = [];

    try {
      // Create applier for this migration
      const applier = createMemoryApplier(definition);

      // Build migration state for current migration
      const builder = migrationBuilder({
        schemas: definition.schemas,
        parentSchemas: definition.parent?.schemas,
      });
      const state = definition.migrate(builder);
      const operations = state.operations;

      // Check operation count limit
      if (
        this.options.maxOperations &&
        operations.length > this.options.maxOperations
      ) {
        warnings.push(
          `Migration has ${operations.length} operations, which exceeds the recommended limit of ${this.options.maxOperations}`,
        );
      }

      // Validate that migration has operations
      if (operations.length === 0) {
        warnings.push(
          "Migration has no operations",
        );
      }

      // Collects mock-generation failures from the three paths that feed
      // this validation: state preparation, initial mock state, and the
      // multi-model top-up below.
      const generationFailures: MockGenerationFailure[] = [];

      // Determine initial state
      let currentState = await this.determineInitialState(
        definition,
        initialState,
        generationFailures,
      );

      // Test forward execution of current migration
      const forwardErrors: string[] = [];
      const stateBeforeMigration = currentState; // Capture state before applying this migration

      // Apply operations in sequence
      let appliedOperations = 0;
      for (let i = 0; i < operations.length; i++) {
        this.report(`applying operation ${i + 1}/${operations.length}`);
        const operation = operations[i];
        const idsBefore = isDocumentDelete(operation)
          ? snapshotIds(currentState)
          : undefined;
        try {
          currentState = await applier.applyOperation(currentState, operation);
          appliedOperations++;
          if (idsBefore) {
            warnings.push(
              ...deleteWarnings(i, operation, idsBefore, currentState),
            );
          }
        } catch (error) {
          const errorMessage = error instanceof Error
            ? error.message
            : String(error);
          forwardErrors.push(
            `Operation ${i + 1} (${operation.type}): ${errorMessage}`,
          );
        }
      }

      if (appliedOperations !== operations.length) {
        errors.push(
          `Only ${appliedOperations} out of ${operations.length} operations were applied successfully.`,
        );
      }

      const stateAfterMigration = currentState; // Capture state after applying this migration

      if (forwardErrors.length > 0) {
        errors.push("Forward migration simulation failed:");
        errors.push(...forwardErrors.map((err) => `  ${err}`));
      }

      // Check for collections creations and multi-collection creations
      {
        // Validate that NEW collections (not from parent) are actually created
        if (definition.schemas.collections) {
          const declaredCollections = Object.keys(
            definition.schemas.collections ?? {},
          );
          const parentCollections = Object.keys(
            definition.parent?.schemas.collections ?? {},
          );
          const createdCollections = Object.keys(
            stateAfterMigration.collections ?? {},
          );

          const declaredCollectionsName = new Set(declaredCollections);
          const parentCollectionsName = new Set(parentCollections);
          const createdCollectionsName = new Set(createdCollections);
          // New collections are those declared in this migration but not present in parent
          const newCollectionsFromParent = declaredCollectionsName.difference(
            parentCollectionsName,
          );
          // Check that all NEW collections are created in migrate()
          const missingCollections = newCollectionsFromParent.difference(
            createdCollectionsName,
          );

          if (missingCollections.size > 0) {
            for (const collName of missingCollections) {
              errors.push(
                `Collection "${collName}" is declared in schema but not created in migrate()`,
              );
            }
            errors.push(
              " 💡 Tip: Did you forget to call .createCollection() in your migration?",
            );
          }
        }

        if (definition.schemas.multiCollections) {
          const declaredMultiCollections = Object.keys(
            definition.schemas.multiCollections ?? {},
          );
          const parentMultiCollections = Object.keys(
            definition.parent?.schemas.multiCollections ?? {},
          );
          const createdMultiCollections = Object.keys(
            stateAfterMigration.multiCollections ?? {},
          );

          const declaredMultiCollectionsName = new Set(
            declaredMultiCollections,
          );
          const parentMultiCollectionsName = new Set(parentMultiCollections);
          const createdMultiCollectionsName = new Set(createdMultiCollections);
          // New multi-collections are those declared in this migration but not present in parent
          const newMultiCollectionsFromParent = declaredMultiCollectionsName
            .difference(parentMultiCollectionsName);
          // Check that all NEW multi-collections are created in migrate()
          const missingMultiCollections = newMultiCollectionsFromParent
            .difference(createdMultiCollectionsName);

          if (missingMultiCollections.size > 0) {
            for (const collName of missingMultiCollections) {
              errors.push(
                `Multi-collection "${collName}" is declared in schema but not created in migrate()`,
              );
            }
            errors.push(
              " 💡 Tip: Did you forget to call .createMultiCollection() in your migration?",
            );
          }
        }

        // Validate that NEW scoped multi-collections (not from parent) are actually created
        if (definition.schemas.scopedMultiCollections) {
          const declaredScoped = Object.keys(
            definition.schemas.scopedMultiCollections ?? {},
          );
          const parentScoped = Object.keys(
            definition.parent?.schemas.scopedMultiCollections ?? {},
          );
          const createdScoped = Object.keys(
            stateAfterMigration.scopedMultiCollections ?? {},
          );

          const declaredScopedName = new Set(declaredScoped);
          const parentScopedName = new Set(parentScoped);
          const createdScopedName = new Set(createdScoped);
          // New scoped multi-collections are those declared in this migration but not present in parent
          const newScopedFromParent = declaredScopedName.difference(
            parentScopedName,
          );
          // Check that all NEW scoped multi-collections are created in migrate()
          const missingScoped = newScopedFromParent.difference(
            createdScopedName,
          );

          if (missingScoped.size > 0) {
            for (const collName of missingScoped) {
              errors.push(
                `Scoped multi-collection "${collName}" is declared in schema but not created in migrate()`,
              );
            }
            errors.push(
              " 💡 Tip: Did you forget to call .createScopedMultiCollection() in your migration?",
            );
          }
        }

        // Warn about NEW declared multi-collections (they are models, not required to be instantiated)
        if (definition.schemas.multiModels) {
          const declaredMultiModels = Object.keys(
            definition.schemas.multiModels,
          );
          const parentMultiModels = definition.parent?.schemas.multiModels
            ? Object.keys(definition.parent.schemas.multiModels)
            : [];
          const createdMultiModels = Object.keys(
            stateAfterMigration.multiModels || {},
          );

          // Only check multi-collections that are NEW in this migration (not inherited from parent)
          const newMultiModels = declaredMultiModels.filter(
            (name) => !parentMultiModels.includes(name),
          );

          const missingModels = newMultiModels.filter(
            (name) => !createdMultiModels.includes(name),
          );

          if (missingModels.length > 0) {
            warnings.push(
              `Schema declares ${missingModels.length} NEW multi-collection model(s) that are not instantiated in migrate(): ${
                missingModels.join(", ")
              }`,
            );
            warnings.push(
              "  💡 Note: Multi-collections are models and don't require instantiation in the migration.",
            );
          }
        }
      }

      // Populate existing multi-model instances with mock data for validation
      // This ensures that instances created by the migration have data to validate against
      if (definition.schemas.multiModels) {
        populateExistingMultiModelInstances(
          stateAfterMigration,
          definition.schemas.multiModels,
          "ifSparse",
          this.mockContext(
            generationFailures,
            // Seeded on the migration id like the initial-state session:
            // stable per migration, different between migrations.
            createCorrelationSession({
              schemas: definition.schemas,
              seed: fnv1a32(definition.id),
            }),
          ),
        );
      }

      // A mock-generation failure must never silently reduce validation
      // coverage — see foldMockGenerationFailures for the severity rule
      // (warning, or blocking error when the target ended up empty).
      const generationIssues = foldMockGenerationFailures(
        generationFailures,
        definition.schemas,
        stateAfterMigration,
      );
      errors.push(...generationIssues.errors);
      warnings.push(...generationIssues.warnings);

      // Validate schema changes require transformations for existing data
      const changeStateResult = await this.validateSchemaChanges(
        definition,
        applier,
        stateBeforeMigration,
        stateAfterMigration,
        operations,
      );

      errors.push(...changeStateResult);

      return Promise.resolve({
        success: errors.length === 0,
        errors,
        warnings,
        data: {
          operationCount: operations.length,
          hasIrreversibleProperty: state.hasProperty("irreversible"),
          simulationCompleted: true,
          // Include final state for state propagation optimization
          stateAfterMigration,
        },
      });
    } catch (error) {
      console.error(error);
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      errors.push(`Migration validation failed: ${errorMessage}`);

      return Promise.resolve({
        success: false,
        errors,
        warnings,
        data: { simulationCompleted: false },
      });
    }
  }

  /**
   * Determines the initial database state for validation
   *
   * @private
   */
  private async determineInitialState(
    definition: MigrationDefinition,
    providedState: SimulationDatabaseState | undefined,
    failures: MockGenerationFailure[],
  ): Promise<SimulationDatabaseState> {
    if (providedState) {
      // Use provided state (from migrate.ts incremental validation)
      // This avoids O(n²) complexity when validating batches
      //
      // Failures recorded while PREPARING this state ride on it — the only
      // channel the preparation API has — and are consumed here so this
      // validation reports them exactly once.
      if (providedState.mockGenerationFailures) {
        failures.push(...providedState.mockGenerationFailures);
        delete providedState.mockGenerationFailures;
      }
      return providedState;
    }

    if (definition.parent) {
      // Standalone validation of child migration
      // Build hybrid state: real seeds from parent migrations + mock supplements
      return await this.buildMockStateFromSchemas(definition.parent, failures);
    }

    // Root migration with no parent - start with empty state
    return createEmptyDatabaseState();
  }

  /**
   * Validates collection schema changes and ensures transformations exist for incompatible changes
   *
   * @private
   */
  private async validateCollectionSchemaChanges(
    definition: MigrationDefinition,
    applier: ReturnType<typeof createMemoryApplier>,
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ): Promise<{
    errors: string[];
    issues: { type: string; message: string }[];
  }> {
    const errors: string[] = [];
    const issues: { type: string; message: string }[] = [];
    const currentSchema = definition.schemas.collections || {};
    const parentSchema = definition.parent?.schemas.collections || {};

    // Validate current state collections against their schemas
    for (
      const [collectionName, currentCollSchema] of Object.entries(currentSchema)
    ) {
      const content = (stateAfter.collections || {})[collectionName]?.content ||
        [];
      for (const [docIndex, doc] of content.entries()) {
        this.report(
          `checking collections/${collectionName} ${
            docIndex + 1
          }/${content.length}`,
        );
        const valid = v.safeParse(v.object(currentCollSchema), doc);
        if (!valid.success) {
          errors.push(
            `Document in collection "${collectionName}" does not match schema:\n-> ${
              valid.issues.map((issue) => {
                return `(${v.getDotPath(issue)}) ${issue.message}`;
              }).join("\n-> ")
            }`,
          );
        }
      }
    }

    // The real rollback path refuses a migration containing any irreversible
    // operation (applier pre-scan), so no rollback state exists to validate.
    if (getIrreversibleOperations(operations).length > 0) {
      return { errors, issues };
    }

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
      this.report(
        `rolling back collections operation ${
          operations.length - i
        }/${operations.length}`,
      );
      const operation = operations[i];
      try {
        stateBeforeRollback = await applier.reverseOperation(
          stateBeforeRollback,
          operation,
        );
      } catch {
        // Ignore errors during reverse application
      }
    }
    const stateAfterRollback = stateBeforeRollback;

    // Check each collection for schema changes
    for (
      const [collectionName, parentCollSchema] of Object.entries(parentSchema)
    ) {
      // New collection, no validation needed
      if (!parentCollSchema) continue;
      // Check if schema has changed
      const rolledBack =
        (stateAfterRollback.collections || {})[collectionName]?.content || [];
      for (const [docIndex, doc] of rolledBack.entries()) {
        this.report(
          `checking rolled-back collections/${collectionName} ${
            docIndex + 1
          }/${rolledBack.length}`,
        );
        const valid = v.safeParse(v.object(parentCollSchema), doc);
        if (!valid.success) {
          errors.push(
            `The collection "${collectionName}" not valid after rollback.\n-> ${
              valid.issues.map((issue) => {
                return `(${v.getDotPath(issue)}) ${issue.message}`;
              }).join("\n-> ")
            }"`,
          );
        }

        const docBefore = (stateBefore.collections || {})[collectionName]
          ?.content?.[docIndex];

        const equal = dirtyEquivalent(docBefore, doc);
        if (!equal) {
          issues.push({
            type: "rollback_document_mismatch",
            message:
              `Document in collection "${collectionName}" different after rollback.`,
          });
        }
      }
      const currentCollSchema = currentSchema[collectionName];
      if (!currentCollSchema) continue; // Collection was removed, skip
    }

    return {
      errors,
      issues,
    };
  }

  private async validateMultiCollectionSchemaChanges(
    definition: MigrationDefinition,
    applier: ReturnType<typeof createMemoryApplier>,
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ): Promise<{
    errors: string[];
    issues: { type: string; message: string }[];
  }> {
    const errors: string[] = [];
    const issues: { type: string; message: string }[] = [];
    const currentSchema = definition.schemas.multiCollections || {};
    const parentSchema = definition.parent?.schemas.multiCollections || {};

    // Validate current state multi-collections against their schemas
    for (
      const [multiCollectionName, currentMultiCollSchema] of Object.entries(
        currentSchema,
      )
    ) {
      const currentCollState = stateAfter.multiCollections[multiCollectionName];
      // Never created in state: the creation check already reports it.
      if (!currentCollState) continue;
      const allCollTypes = Object.keys(currentMultiCollSchema);
      for (const [docIndex, element] of currentCollState.content.entries()) {
        this.report(
          `checking multiCollections/${multiCollectionName} ${
            docIndex + 1
          }/${currentCollState.content.length}`,
        );
        const elementType = element._type as string;
        if (!allCollTypes.includes(elementType)) {
          errors.push(
            `Document in multi-collection "${multiCollectionName}" has unknown type "${elementType}"`,
          );
          continue;
        }

        const schema = currentMultiCollSchema[elementType];
        const valid = v.safeParse(
          v.object({
            ...schema,
            _type: v.literal(elementType),
          }),
          element,
        );

        if (!valid.success) {
          errors.push(
            `Document in multi-collection "${multiCollectionName}" type "${elementType}" does not match schema:\n-> ${
              valid.issues.map((issue) => {
                return `(${v.getDotPath(issue)}) ${issue.message}`;
              }).join("\n-> ")
            }`,
          );
        }
      }
    }

    // No rollback state exists to validate for an irreversible migration.
    if (getIrreversibleOperations(operations).length > 0) {
      return { errors, issues };
    }

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
      this.report(
        `rolling back multi-collections operation ${
          operations.length - i
        }/${operations.length}`,
      );
      const operation = operations[i];
      try {
        stateBeforeRollback = await applier.reverseOperation(
          stateBeforeRollback,
          operation,
        );
      } catch {
        // Ignore errors during reverse application
      }
    }
    const stateAfterRollback = stateBeforeRollback;

    // Check each multi-collection for schema changes
    for (
      const [multiCollectionName, parentMultiCollSchema] of Object.entries(
        parentSchema,
      )
    ) {
      // New multi-collection, no validation needed
      if (!parentMultiCollSchema) continue;
      // Check if schema has changed
      const rolledBack =
        (stateAfterRollback.multiCollections || {})[multiCollectionName]
          ?.content || [];
      for (const [docIndex, doc] of rolledBack.entries()) {
        this.report(
          `checking rolled-back multiCollections/${multiCollectionName} ${
            docIndex + 1
          }/${rolledBack.length}`,
        );
        const docType = doc._type as string;
        const parentTypeSchema = parentMultiCollSchema[docType];
        if (!parentTypeSchema) continue; // Type was added, no validation needed
        const valid = v.safeParse(
          v.object({
            ...parentTypeSchema,
            _type: v.literal(docType),
          }),
          doc,
        );
        if (!valid.success) {
          errors.push(
            `The multi-collection "${multiCollectionName}" type "${docType}" not valid after rollback.\n-> ${
              valid.issues.map((issue) => {
                return `(${v.getDotPath(issue)}) ${issue.message}`;
              }).join("\n-> ")
            }`,
          );
        }
        const docBefore =
          (stateBefore.multiCollections || {})[multiCollectionName]?.content
            ?.[docIndex];
        const equal = dirtyEquivalent(docBefore, doc);
        if (!equal) {
          issues.push({
            type: "rollback_document_mismatch",
            message:
              `Document in multi-collection "${multiCollectionName}" type "${docType}" different after rollback.`,
          });
        }
      }
    }

    return {
      errors,
      issues,
    };
  }

  private async validateMultiModelSchemaChanges(
    definition: MigrationDefinition,
    applier: ReturnType<typeof createMemoryApplier>,
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ): Promise<{
    errors: string[];
    issues: { type: string; message: string }[];
  }> {
    const errors: string[] = [];
    const issues: { type: string; message: string }[] = [];
    const currentSchema = definition.schemas.multiModels || {};
    const parentSchema = definition.parent?.schemas.multiModels || {};

    const allModelType = Object.keys(currentSchema);

    // Validate current state multi-collection models against their schemas
    for (
      const [collectionName, instance] of Object.entries(
        stateAfter.multiModels || {},
      )
    ) {
      this.report(`checking multiModels/${collectionName}`);
      const { modelType, content } = instance;
      if (!allModelType.includes(modelType)) {
        errors.push(
          `Multi-collection model "${collectionName}" instance exists but model is not declared in schema`,
        );
        continue;
      }
      const modelSchema = currentSchema[modelType];
      if (!modelSchema) {
        errors.push(
          `Multi-collection model "${collectionName}" instance exists but model is not declared in schema`,
        );
        continue;
      }

      for (const [docIndex, element] of content.entries()) {
        this.report(
          `checking multiModels/${collectionName} ${
            docIndex + 1
          }/${content.length}`,
        );
        const elementType = element._type as string;
        const allTypes = Object.keys(modelSchema);
        if (!allTypes.includes(elementType)) {
          errors.push(
            `Document in multi-collection model "${collectionName}" has unknown type "${elementType}"`,
          );
          continue;
        }
        const schema = modelSchema[elementType];
        const valid = v.safeParse(
          v.object({
            ...schema,
            _type: v.literal(elementType),
          }),
          element,
        );
        if (!valid.success) {
          errors.push(
            `Document in multi-collection model "${collectionName}" type "${elementType}" does not match schema:\n-> ${
              valid.issues.map((issue) => {
                return `(${v.getDotPath(issue)}) ${issue.message}`;
              }).join("\n-> ")
            }`,
          );
        }
      }
    }

    // No rollback state exists to validate for an irreversible migration.
    if (getIrreversibleOperations(operations).length > 0) {
      return { errors, issues };
    }

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
      this.report(
        `rolling back models operation ${
          operations.length - i
        }/${operations.length}`,
      );
      const operation = operations[i];
      try {
        stateBeforeRollback = await applier.reverseOperation(
          stateBeforeRollback,
          operation,
        );
      } catch {
        // Ignore errors during reverse application
      }
    }
    const stateAfterRollback = stateBeforeRollback;
    // Check each multi-collection model for schema changes
    for (const [modelType, parentModelSchema] of Object.entries(parentSchema)) {
      // New multi-collection model, no validation needed
      if (!parentModelSchema) continue;
      // Check if schema has changed
      for (
        const [collectionName, instance] of Object.entries(
          stateAfterRollback.multiModels || {},
        )
      ) {
        if (instance.modelType !== modelType) continue;
        for (const [docIndex, doc] of instance.content.entries()) {
          this.report(
            `checking rolled-back multiModels/${collectionName} ${
              docIndex + 1
            }/${instance.content.length}`,
          );
          const docType = doc._type as string;
          const parentTypeSchema = parentModelSchema[docType];
          if (!parentTypeSchema) continue; // Type was added, no validation needed
          const valid = v.safeParse(
            v.object({
              ...parentTypeSchema,
              _type: v.literal(docType),
            }),
            doc,
          );
          if (!valid.success) {
            errors.push(
              `The multi-collection model "${collectionName}" type "${docType}" not valid after rollback.\n-> ${
                valid.issues.map((issue) => {
                  return `(${v.getDotPath(issue)}) ${issue.message}`;
                }).join("\n-> ")
              }`,
            );
          }
          const docBefore = (stateBefore.multiModels || {})[collectionName]
            ?.content?.[docIndex];
          const equal = dirtyEquivalent(docBefore, doc);
          if (!equal) {
            issues.push({
              type: "rollback_document_mismatch",
              message:
                `Document in multi-collection model "${collectionName}" type "${docType}" different after rollback.`,
            });
          }
        }
      }
    }

    return {
      errors,
      issues,
    };
  }

  /**
   * Validates scoped multi-collection documents against their schemas
   *
   * Mirrors {@link validateMultiCollectionSchemaChanges} for the
   * `scopedMultiCollections` bucket: every simulated document must match its
   * declared type schema, carry a known `_type`, and carry a `_scope` value
   * that validates against the scoped collection's `scope` schema. The
   * rollback pass revalidates documents against the parent schemas.
   *
   * @private
   */
  private async validateScopedMultiCollectionSchemaChanges(
    definition: MigrationDefinition,
    applier: ReturnType<typeof createMemoryApplier>,
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ): Promise<{
    errors: string[];
    issues: { type: string; message: string }[];
  }> {
    const errors: string[] = [];
    const issues: { type: string; message: string }[] = [];
    const currentSchema = definition.schemas.scopedMultiCollections || {};
    const parentSchema = definition.parent?.schemas.scopedMultiCollections ||
      {};

    // Validate current state scoped multi-collections against their schemas
    for (
      const [scopedName, currentScopedSchema] of Object.entries(currentSchema)
    ) {
      const currentCollState = stateAfter.scopedMultiCollections?.[scopedName];
      // Never created in state: the creation check already reports it.
      if (!currentCollState) continue;
      const allCollTypes = Object.keys(currentScopedSchema.types);
      for (const [docIndex, element] of currentCollState.content.entries()) {
        this.report(
          `checking scopedMultiCollections/${scopedName} ${
            docIndex + 1
          }/${currentCollState.content.length}`,
        );
        const elementType = element._type as string;
        if (!allCollTypes.includes(elementType)) {
          errors.push(
            `Document in scoped multi-collection "${scopedName}" has unknown type "${elementType}"`,
          );
          continue;
        }

        const scopeValid = v.safeParse(
          currentScopedSchema.scope,
          element._scope,
        );
        if (!scopeValid.success) {
          errors.push(
            `Document in scoped multi-collection "${scopedName}" type "${elementType}" has invalid _scope:\n-> ${
              scopeValid.issues.map((issue) => issue.message).join("\n-> ")
            }`,
          );
        }

        const schema = currentScopedSchema.types[elementType];
        const valid = v.safeParse(
          v.object({
            ...schema,
            _type: v.literal(elementType),
          }),
          element,
        );

        if (!valid.success) {
          errors.push(
            `Document in scoped multi-collection "${scopedName}" type "${elementType}" does not match schema:\n-> ${
              valid.issues.map((issue) => {
                return `(${v.getDotPath(issue)}) ${issue.message}`;
              }).join("\n-> ")
            }`,
          );
        }
      }
    }

    // No rollback state exists to validate for an irreversible migration.
    if (getIrreversibleOperations(operations).length > 0) {
      return { errors, issues };
    }

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
      this.report(
        `rolling back scoped multi-collections operation ${
          operations.length - i
        }/${operations.length}`,
      );
      const operation = operations[i];
      try {
        stateBeforeRollback = await applier.reverseOperation(
          stateBeforeRollback,
          operation,
        );
      } catch {
        // Ignore errors during reverse application
      }
    }
    const stateAfterRollback = stateBeforeRollback;

    // Check each scoped multi-collection for schema changes
    for (
      const [scopedName, parentScopedSchema] of Object.entries(parentSchema)
    ) {
      // New scoped multi-collection, no validation needed
      if (!parentScopedSchema) continue;
      const rolledBackScoped =
        (stateAfterRollback.scopedMultiCollections || {})[scopedName]
          ?.content || [];
      for (const [docIndex, doc] of rolledBackScoped.entries()) {
        this.report(
          `checking rolled-back scopedMultiCollections/${scopedName} ${
            docIndex + 1
          }/${rolledBackScoped.length}`,
        );
        const docType = doc._type as string;
        const parentTypeSchema = parentScopedSchema.types[docType];
        if (!parentTypeSchema) continue; // Type was added, no validation needed
        const valid = v.safeParse(
          v.object({
            ...parentTypeSchema,
            _type: v.literal(docType),
          }),
          doc,
        );
        if (!valid.success) {
          errors.push(
            `The scoped multi-collection "${scopedName}" type "${docType}" not valid after rollback.\n-> ${
              valid.issues.map((issue) => {
                return `(${v.getDotPath(issue)}) ${issue.message}`;
              }).join("\n-> ")
            }`,
          );
        }
        const docBefore = (stateBefore.scopedMultiCollections || {})[scopedName]
          ?.content
          ?.[docIndex];
        const equal = dirtyEquivalent(docBefore, doc);
        if (!equal) {
          issues.push({
            type: "rollback_document_mismatch",
            message:
              `Document in scoped multi-collection "${scopedName}" type "${docType}" different after rollback.`,
          });
        }
      }
    }

    return {
      errors,
      issues,
    };
  }

  /**
   * Validates that schema changes for multi-collections have corresponding transformations
   *
   * @private
   * @param definition - The migration definition
   * @param applier - The memory applier for this migration
   * @param stateBefore - Database state before this migration
   * @param stateAfter - Database state after this migration
   * @param operations - Operations in this migration
   * @returns Array of error messages (empty if validation passes)
   */
  private async validateSchemaChanges(
    definition: MigrationDefinition,
    applier: ReturnType<typeof createMemoryApplier>,
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ): Promise<string[]> {
    const errors: string[] = [];

    // Each sub-validator gets its OWN clones because it rolls the state back
    // in place. A clone of a chain-sized state is a third of a second of
    // blocked thread, hence a note before each pair.
    const clones = (phase: string): [
      SimulationDatabaseState,
      SimulationDatabaseState,
    ] => {
      this.report(`cloning state for ${phase}`);
      return [structuredClone(stateBefore), structuredClone(stateAfter)];
    };

    // Validate collection schema changes
    const collectionClones = clones("collections");
    const collectionChangeResult = await this.validateCollectionSchemaChanges(
      definition,
      applier,
      collectionClones[0],
      collectionClones[1],
      operations,
    );

    // Validate multi-collection schema changes
    const multiCollectionClones = clones("multi-collections");
    const multiCollectionChangeResult = await this
      .validateMultiCollectionSchemaChanges(
        definition,
        applier,
        multiCollectionClones[0],
        multiCollectionClones[1],
        operations,
      );

    const multiModelClones = clones("models");
    const multiModelsChangeResult = await this.validateMultiModelSchemaChanges(
      definition,
      applier,
      multiModelClones[0],
      multiModelClones[1],
      operations,
    );

    // Validate scoped multi-collection schema changes
    const scopedClones = clones("scoped multi-collections");
    const scopedChangeResult = await this
      .validateScopedMultiCollectionSchemaChanges(
        definition,
        applier,
        scopedClones[0],
        scopedClones[1],
        operations,
      );

    errors.push(
      ...[...new Set(collectionChangeResult.errors)],
      ...[...new Set(multiCollectionChangeResult.errors)],
      ...[...new Set(multiModelsChangeResult.errors)],
      ...[...new Set(scopedChangeResult.errors)],
    );

    return errors;
  }

  /**
   * Simulates all parent migrations to get real seed data
   *
   * @private
   */
  private async simulateParentMigrations(
    parent: MigrationDefinition,
  ): Promise<SimulationDatabaseState> {
    let currentState = createEmptyDatabaseState();

    // Collect all ancestors (from root to immediate parent)
    const ancestors: MigrationDefinition[] = [];
    let current: MigrationDefinition | null = parent;

    while (current !== null) {
      ancestors.unshift(current);
      current = current.parent;
    }

    // Apply each ancestor migration in order
    for (const ancestor of ancestors) {
      // Create applier for this ancestor migration
      const applier = createMemoryApplier(ancestor);

      const builder = migrationBuilder({
        schemas: ancestor.schemas,
        parentSchemas: ancestor.parent?.schemas,
      });
      const state = ancestor.migrate(builder);

      for (const operation of state.operations) {
        currentState = await applier.applyOperation(currentState, operation);
      }
    }

    return currentState;
  }

  /**
   * Builds a hybrid database state from parent migrations
   * Used for standalone validation when no initial state is provided
   *
   * Hybrid approach:
   * 1. Simulates all parent migrations (preserves real seeds)
   * 2. Adds mock data to empty collections (tests edge cases)
   *
   * This ensures we catch issues with both:
   * - Real seed data (tests expect specific values)
   * - Empty/sparse collections (transformations on collections without seeds)
   *
   * @private
   * @param parent - The parent migration definition
   * @param failures - Collector receiving mock-generation failures
   * @returns Database state with real seeds + mock data supplements
   */
  private async buildMockStateFromSchemas(
    parent: MigrationDefinition,
    failures: MockGenerationFailure[],
  ): Promise<SimulationDatabaseState> {
    const currentState = await this.simulateParentMigrations(parent);

    const ctx = this.mockContext(
      failures,
      createCorrelationSession({
        schemas: parent.schemas,
        seed: fnv1a32(parent.id),
      }),
    );

    // "always": keeps real parent seeds AND adds generated documents, so
    // both seeded and edge-case data get exercised. Session seed derives
    // from the migration id, so a simulation replays identically per run.
    populateDeclaredBuckets(currentState, parent.schemas, "always", ctx);

    // Instances MINTED BY the replayed ancestor operations are already
    // "covered" for synthetic population — which only decides whether an
    // instance should exist, never fills one. Without this top-up they reach
    // the migration under validation empty, and every transform over an
    // instance type validates zero documents. The propagated path gets this
    // for free (validateMigration tops instances up after each ancestor);
    // the standalone path has to ask.
    if (parent.schemas.multiModels) {
      populateExistingMultiModelInstances(
        currentState,
        parent.schemas.multiModels,
        "ifSparse",
        ctx,
      );
    }

    return currentState;
  }

  /**
   * Prepares state for next migration by applying retention ratio
   *
   * This method:
   * 1. Keeps a percentage of existing documents (based on stateRetentionRatio)
   *    and refreshes each entry back to its pre-retention size — never
   *    beyond it, so propagation cannot compound volume across a chain
   * 2. Mock-populates every declared-but-empty entry, so the next
   *    migration's validation has data to test against even when no
   *    documents were created in migrations
   *
   * This ensures we test both:
   * - Existing data that went through previous migrations (retained)
   * - Fresh edge cases with new mock data (generated)
   *
   * Mock-generation failures from preparation ride on the returned state's
   * `mockGenerationFailures` field (the only channel this locked signature
   * allows) and are consumed by the next `validateMigration` call.
   *
   * @param currentState - The current database state after migration
   * @param schemas - The schemas to use for generating new mock data
   * @returns New state with retained + fresh data
   */
  prepareStateForNextMigration(
    currentState: SimulationDatabaseState,
    schemas: SchemasDefinition,
  ): SimulationDatabaseState {
    const ratio = this.options.stateRetentionRatio ??
      this.mockConfig.DEFAULT_STATE_RETENTION_RATIO;

    // Clone the state to avoid mutations
    this.report("cloning state");
    const newState: SimulationDatabaseState = structuredClone(currentState);

    // Failures riding on the incoming state were already folded into the
    // previous validation's result — a fresh preparation reports fresh ones.
    delete newState.mockGenerationFailures;

    const ctx: MockPopulateContext = this.mockContext(
      [],
      // The locked `(state, schemas)` signature carries no migration id, so
      // the seed derives from a stable fingerprint of the schemas — which is
      // exactly what schemasFingerprint exists for.
      createCorrelationSession({
        schemas,
        seed: fnv1a32(schemasFingerprint(schemas)),
      }),
    );

    retainAndRefreshBuckets(newState, schemas, ratio, ctx);
    populateDeclaredBuckets(newState, schemas, "ifEmpty", ctx);

    if (ctx.failures.length > 0) {
      newState.mockGenerationFailures = ctx.failures;
    }

    return newState;
  }
}

/**
 * Factory function to create a simulation validator
 *
 * @param options - Configuration options for the validator
 * @returns A new simulation validator instance
 *
 * @example
 * ```typescript
 * import { createSimulationValidator } from "@diister/mongodbee/migration/validators";
 *
 * const validator = createSimulationValidator({
 *   maxOperations: 500,
 *   powerLevel: "quick",
 *   stateRetentionRatio: 0.5 // Keep 50% of previous state
 * });
 *
 * const result = await validator.validateMigration(migration);
 * ```
 */
export function createSimulationValidator(
  options?: SimulationValidatorOptions,
): SimulationValidator {
  return new SimulationValidator(options);
}

/**
 * Utility function to validate a single migration definition quickly
 *
 * @param definition - Migration definition to validate
 * @param options - Validator options
 * @returns Validation result
 *
 * @example
 * ```typescript
 * import { validateMigrationWithSimulation } from "@diister/mongodbee/migration/validators";
 *
 * const result = await validateMigrationWithSimulation(migration, {
 *   powerLevel: "quick"
 * });
 *
 * if (result.success) {
 *   console.log("Migration is valid!");
 * }
 * ```
 */
export async function validateMigrationWithSimulation(
  definition: MigrationDefinition,
  options?: SimulationValidatorOptions,
): Promise<ValidationResult> {
  const validator = createSimulationValidator(options);
  return await validator.validateMigration(definition);
}

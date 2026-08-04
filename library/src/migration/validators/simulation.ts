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
import { migrationBuilder } from "../builder.ts";
import * as v from "valibot";
import { dirtyEquivalent } from "../../utils/object.ts";
import { createMemoryApplier } from "../appliers/memory.ts";
import {
  DEFAULT_STATE_RETENTION_RATIO,
  foldMockGenerationFailures,
  getMockGenerationConfig,
  type MockGenerationConfig,
  type MockPopulateContext,
  populateDeclaredBuckets,
  populateExistingMultiModelInstances,
  retainAndRefreshBuckets,
  type SimulationPowerLevel,
} from "./mock/mod.ts";

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
  /** Whether to use strict validation in the simulation applier */
  strictValidation?: boolean;

  /** Whether to track operation history during simulation */
  trackHistory?: boolean;

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
}

/**
 * Default validator configuration
 */
export const DEFAULT_SIMULATION_VALIDATOR_OPTIONS: SimulationValidatorOptions =
  {
    strictValidation: true,
    trackHistory: true,
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

  constructor(options: SimulationValidatorOptions = {}) {
    this.options = { ...DEFAULT_SIMULATION_VALIDATOR_OPTIONS, ...options };
    this.mockConfig = getMockGenerationConfig(this.options.powerLevel);
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

      // Collects every mock-generation failure that concerns THIS
      // validation: failures inherited from the state preparation
      // (propagation path), failures from building the initial mock state
      // (standalone path), and failures from topping up multi-model
      // instances below. Folded into errors/warnings after the migration ran.
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
        const operation = operations[i];
        try {
          currentState = await applier.applyOperation(currentState, operation);
          appliedOperations++;
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
          { config: this.mockConfig, failures: generationFailures },
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
      // Failures recorded while PREPARING this state ride on it because the
      // preparation API has no other channel (see SimulationDatabaseState).
      // Consume them here so this validation reports them exactly once.
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
      for (
        const [_docIndex, doc]
          of ((stateAfter.collections || {})[collectionName]?.content || [])
            .entries()
      ) {
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

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
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
      for (
        const [docIndex, doc]
          of ((stateAfterRollback.collections || {})[collectionName]?.content ||
            []).entries()
      ) {
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
      for (const element of currentCollState.content) {
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

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
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
      for (
        const [docIndex, doc]
          of ((stateAfterRollback.multiCollections || {})[multiCollectionName]
            ?.content || []).entries()
      ) {
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

      for (const element of content) {
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

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
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
      for (const element of currentCollState.content) {
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

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
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
      for (
        const [docIndex, doc]
          of ((stateAfterRollback.scopedMultiCollections || {})[scopedName]
            ?.content || []).entries()
      ) {
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

    // Validate collection schema changes
    const collectionChangeResult = await this.validateCollectionSchemaChanges(
      definition,
      applier,
      structuredClone(stateBefore),
      structuredClone(stateAfter),
      operations,
    );

    // Validate multi-collection schema changes
    const multiCollectionChangeResult = await this
      .validateMultiCollectionSchemaChanges(
        definition,
        applier,
        structuredClone(stateBefore),
        structuredClone(stateAfter),
        operations,
      );

    const multiModelsChangeResult = await this.validateMultiModelSchemaChanges(
      definition,
      applier,
      structuredClone(stateBefore),
      structuredClone(stateAfter),
      operations,
    );

    // Validate scoped multi-collection schema changes
    const scopedChangeResult = await this
      .validateScopedMultiCollectionSchemaChanges(
        definition,
        applier,
        structuredClone(stateBefore),
        structuredClone(stateAfter),
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

    // "always": the state keeps its real parent seeds AND gains generated
    // documents on top, so both seeded values and edge cases are exercised.
    populateDeclaredBuckets(currentState, parent.schemas, "always", {
      config: this.mockConfig,
      failures,
    });

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
   * Mock-generation failures encountered during preparation are recorded on
   * the returned state (`mockGenerationFailures`) — this signature is the
   * public propagation API, so the state is the only channel — and are
   * consumed by the next `validateMigration` call, which folds them into its
   * result.
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
    const newState: SimulationDatabaseState = structuredClone(currentState);

    // Failures riding on the incoming state were already folded into the
    // previous validation's result — a fresh preparation reports fresh ones.
    delete newState.mockGenerationFailures;

    const ctx: MockPopulateContext = {
      config: this.mockConfig,
      failures: [],
    };

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
 *   validateReversibility: true,
 *   strictValidation: true,
 *   maxOperations: 500,
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
 *   validateReversibility: false,
 *   strictValidation: true
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

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
  MigrationState,
  SchemasDefinition,
} from "../types.ts";
import type {
  MigrationExecutionContext,
  MigrationValidator,
  ValidationResult,
} from "../runners/execution.ts";
import {
  compareDatabaseStates,
  createEmptyDatabaseState,
  createSimulationApplier,
  type SimulationApplier,
  type SimulationDatabaseState,
} from "../appliers/simulation.ts";
import { migrationBuilder } from "../builder.ts";
import * as v from "valibot";
import { createMockGenerator } from "@diister/valibot-mock";
import { dirtyEquivalent } from "../../utils/object.ts";
import { createMemoryApplier } from "../appliers/memory.ts";

/**
 * Constants for mock data generation
 */
const MOCK_GENERATION = {
  DOCS_PER_COLLECTION_MIN: 1000,
  DOCS_PER_COLLECTION_MAX: 1000,
  DOCS_PER_TYPE_MIN: 1,
  DOCS_PER_TYPE_MAX: 2,
  MIN_SPARSE_THRESHOLD: 2,
} as const;

/**
 * Configuration options for the simulation validator
 */
export interface SimulationValidatorOptions {
  /** Whether to validate that migrations are reversible */
  validateReversibility?: boolean;

  /** Whether to use strict validation in the simulation applier */
  strictValidation?: boolean;

  /** Whether to track operation history during simulation */
  trackHistory?: boolean;

  /** Maximum number of operations to validate (for performance) */
  maxOperations?: number;

  /** Whether to validate individual operations */
  validateOperations?: boolean;
}

/**
 * Default validator configuration
 */
export const DEFAULT_SIMULATION_VALIDATOR_OPTIONS: SimulationValidatorOptions =
  {
    validateReversibility: true,
    strictValidation: true,
    trackHistory: true,
    maxOperations: 1000,
    validateOperations: true,
  };

/**
 * Simulation-based migration validator
 *
 * This validator uses the SimulationApplier to validate migrations in an
 * in-memory environment before they are applied to real databases.
 */
export class SimulationValidator implements MigrationValidator {
  private readonly applier: ReturnType<typeof createMemoryApplier>;
  private readonly options: SimulationValidatorOptions;

  constructor(options: SimulationValidatorOptions = {}) {
    this.options = { ...DEFAULT_SIMULATION_VALIDATOR_OPTIONS, ...options };
    // this.applier = createSimulationApplier({
    //   strictValidation: this.options.strictValidation,
    //   trackHistory: this.options.trackHistory,
    // });
    this.applier = createMemoryApplier();
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

      // Determine initial state
      let currentState = await this.determineInitialState(definition, initialState);

      // Test forward execution of current migration
      const forwardErrors: string[] = [];
      const stateBeforeMigration = currentState; // Capture state before applying this migration

      // Apply operations in sequence
      let appliedOperations = 0;
      for (let i = 0; i < operations.length; i++) {
        const operation = operations[i];
        try {
          currentState = await this.applier.applyOperation(currentState, operation);
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

      if(appliedOperations !== operations.length) {
        errors.push(`Only ${appliedOperations} out of ${operations.length} operations were applied successfully.`);
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
          const declaredCollections = Object.keys(definition.schemas.collections ?? {});
          const parentCollections = Object.keys(definition.parent?.schemas.collections ?? {});
          const createdCollections = Object.keys(stateAfterMigration.collections ?? {});
  
          const declaredCollectionsName = new Set(declaredCollections);
          const parentCollectionsName = new Set(parentCollections);
          const createdCollectionsName = new Set(createdCollections);
          // New collections are those declared in this migration but not present in parent
          const newCollectionsFromParent = declaredCollectionsName.difference(parentCollectionsName);
          // Check that all NEW collections are created in migrate()
          const missingCollections = newCollectionsFromParent.difference(createdCollectionsName);
  
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
          const declaredMultiCollections = Object.keys(definition.schemas.multiCollections ?? {});
          const parentMultiCollections = Object.keys(definition.parent?.schemas.multiCollections ?? {});
          const createdMultiCollections = Object.keys(stateAfterMigration.multiCollections ?? {});
  
          const declaredMultiCollectionsName = new Set(declaredMultiCollections);
          const parentMultiCollectionsName = new Set(parentMultiCollections);
          const createdMultiCollectionsName = new Set(createdMultiCollections);
          // New multi-collections are those declared in this migration but not present in parent
          const newMultiCollectionsFromParent = declaredMultiCollectionsName.difference(parentMultiCollectionsName);
          // Check that all NEW multi-collections are created in migrate()
          const missingMultiCollections = newMultiCollectionsFromParent.difference(createdMultiCollectionsName);
  
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

      // Validate schema changes require transformations for existing data
      const changeStateResult = await this.validateSchemaChanges(
        definition,
        stateBeforeMigration,
        stateAfterMigration,
        operations,
      )

      errors.push(...changeStateResult);

      // if (definition.parent && definition.schemas.multiModels) {
      //   const schemaChangeErrors = this.validateSchemaChanges(
      //     definition,
      //     stateBeforeMigration,
      //     operations,
      //   );
      //   if (schemaChangeErrors.length > 0) {
      //     errors.push(...schemaChangeErrors);
      //   }
      // }

      /*
      // Validate that transformed documents match their target schemas
      // This catches bad transformation logic (e.g., returning null instead of proper values)
      if (forwardErrors.length === 0) {
        const transformValidationErrors = this.validateTransformedDocuments(
          definition,
          stateAfterMigration,
        );
        if (transformValidationErrors.length > 0) {
          errors.push(...transformValidationErrors);
        }
      }

      // Test reversibility if enabled and forward execution succeeded
      if (this.options.validateReversibility && forwardErrors.length === 0) {
        const reverseErrors = this.validateReversibility(
          state,
          stateBeforeMigration,
          stateAfterMigration,
        );
        if (reverseErrors.length > 0) {
          errors.push("Migration reversibility validation failed:");
          errors.push(...reverseErrors.map((err) => `  ${err}`));
        }
      }

      // Check for irreversible properties
      if (state.hasProperty("irreversible")) {
        if (this.options.validateReversibility) {
          warnings.push(
            "Migration is marked as irreversible but reversibility validation is enabled",
          );
        } else {
          warnings.push("Migration is marked as irreversible");
        }
      }

      // Check for lossy transformations
      if (state.hasProperty("lossy")) {
        const lossyTransforms = operations.filter((op) =>
          (op.type === "transform_collection" || op.type === "transform_multicollection_type") &&
          op.lossy
        );
        warnings.push(
          `Migration has lossy transformations - rollback will result in data loss`,
        );
        for (const op of lossyTransforms) {
          if (op.type === "transform_collection") {
            warnings.push(`  ⚠ Collection: ${op.collectionName}`);
          } else if (op.type === "transform_multicollection_type") {
            warnings.push(`  ⚠ Multi-collection: ${op.collectionType}.${op.typeName}`);
          }
        }
      }
      */

      return Promise.resolve({
        success: errors.length === 0,
        errors,
        warnings,
        data: {
          operationCount: operations.length,
          hasIrreversibleProperty: state.hasProperty("irreversible"),
          simulationCompleted: true,
        },
      });
    } catch (error) {
      console.error(error)
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
   * Validates individual operations before execution
   *
   * @param operation - The operation to validate
   * @param _context - Execution context (not used in simulation validation)
   * @returns Validation result
   */
  validateOperation(
    operation: MigrationRule,
    _context: MigrationExecutionContext,
  ): Promise<ValidationResult> {
    if (!this.options.validateOperations) {
      return Promise.resolve({
        success: true,
        errors: [],
        warnings: [],
        data: { operationValidated: false },
      });
    }

    const errors: string[] = [];
    const warnings: string[] = [];

    try {
      // Test operation in isolation
      const testState = createEmptyDatabaseState();
      this.applier.applyOperation(testState, operation);

      return Promise.resolve({
        success: true,
        errors,
        warnings,
        data: { operationValidated: true },
      });
    } catch (error) {
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      errors.push(`Operation validation failed: ${errorMessage}`);

      return Promise.resolve({
        success: false,
        errors,
        warnings,
        data: { operationValidated: false },
      });
    }
  }

  /**
   * Validates the final state after migration
   *
   * @param state - The migration state to validate
   * @param _context - Execution context (not used in simulation validation)
   * @returns Validation result
   */
  validateState(
    state: MigrationState,
    _context: MigrationExecutionContext,
  ): Promise<ValidationResult> {
    const errors: string[] = [];
    const warnings: string[] = [];

    try {
      // Basic state validation
      if (state.operations.length === 0) {
        warnings.push("Migration state has no operations");
      }

      // Check for conflicting properties
      const properties = state.properties.map((p) => p.type);
      if (
        properties.includes("irreversible") &&
        this.options.validateReversibility
      ) {
        warnings.push(
          "Migration is marked as irreversible but reversibility validation was requested",
        );
      }

      return Promise.resolve({
        success: errors.length === 0,
        errors,
        warnings,
        data: {
          operationCount: state.operations.length,
          properties: properties,
        },
      });
    } catch (error) {
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      errors.push(`State validation failed: ${errorMessage}`);

      return Promise.resolve({
        success: false,
        errors,
        warnings,
        data: { stateValidated: false },
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
    providedState?: SimulationDatabaseState,
  ): Promise<SimulationDatabaseState> {
    if (providedState) {
      // Use provided state (from migrate.ts incremental validation)
      // This avoids O(n²) complexity when validating batches
      return providedState;
    }

    if (definition.parent) {
      // Standalone validation of child migration
      // Build hybrid state: real seeds from parent migrations + mock supplements
      return await this.buildMockStateFromSchemas(definition.parent);
    }

    // Root migration with no parent - start with empty state
    return createEmptyDatabaseState();
  }

  /**
   * Formats validation errors from valibot into readable messages
   *
   * @private
   */
  private formatValidationError(
    // deno-lint-ignore no-explicit-any
    validation: v.SafeParseResult<any>,
    context: {
      type: "collection" | "multicollection";
      name: string;
      index: number;
      typeName?: string;
    },
  ): string[] {
    const errors: string[] = [];

    const location = context.type === "collection"
      ? `collection "${context.name}" (index ${context.index})`
      : `multi-collection "${context.name}" type "${context.typeName}" (index ${context.index})`;

    errors.push(`Transformed document in ${location} does not match schema:`);

    if (validation.issues && validation.issues.length > 0) {
      const issue = validation.issues[0];
      errors.push(
        `  Field: ${issue.path?.map((p: { key: string }) => p.key).join(".") || "(root)"}`,
      );
      errors.push(`  Expected: ${issue.expected || "valid value"}`);
      errors.push(
        `  Received: ${issue.received || JSON.stringify(issue.input)}`,
      );
      errors.push(`  Issue: ${issue.message}`);
    }

    const tip = context.type === "collection"
      ? "Check your transformation function - it may be returning invalid values."
      : `Check your transformation function for type "${context.typeName}" - it may be returning invalid values.`;

    errors.push(`  💡 Tip: ${tip}`);

    return errors;
  }

  /**
   * Validates collection schema changes and ensures transformations exist for incompatible changes
   *
   * @private
   */
  private async validateCollectionSchemaChanges(
    definition: MigrationDefinition,
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ): Promise<{
    errors: string[];
    issues: { type: string, message: string }[];
  }> {
    const errors: string[] = [];
    const issues: { type: string, message: string }[] = [];
    const currentSchema = definition.schemas.collections || {};
    const parentSchema = definition.parent?.schemas.collections || {};

    // Validate current state collections against their schemas
    for (const [collectionName, currentCollSchema] of Object.entries(currentSchema)) {
      for (const [docIndex, doc] of ((stateAfter.collections || {})[collectionName]?.content || []).entries()) {
        const valid = v.safeParse(v.object(currentCollSchema), doc);
        if (!valid.success) {
          errors.push(`Document in collection "${collectionName}" does not match schema:\n-> ${valid.issues.map((issue) => {
            return `(${v.getDotPath(issue)}) ${issue.message}`;
          }).join("\n-> ")}`);
        }
      }
    }

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
      const operation = operations[i];
      try {
        stateBeforeRollback = await this.applier.reverseOperation(stateBeforeRollback, operation);
      } catch {
        // Ignore errors during reverse application
      }
    }
    const stateAfterRollback = stateBeforeRollback;

    // Check each collection for schema changes
    for (const [collectionName, parentCollSchema] of Object.entries(parentSchema)) {
      // New collection, no validation needed
      if (!parentCollSchema) continue;
      // Check if schema has changed
      for (const [docIndex, doc] of ((stateAfterRollback.collections || {})[collectionName]?.content || []).entries()) {
        const valid = v.safeParse(v.object(parentCollSchema), doc);
        if (!valid.success) {
          errors.push(`The collection "${collectionName}" not valid after rollback.\n-> ${valid.issues.map((issue) => {            
            return `(${v.getDotPath(issue)}) ${issue.message}`;
          }).join("\n-> ")}"`);
        }

        const docBefore = (stateBefore.collections || {})[collectionName]?.content?.[docIndex];

        const equal = dirtyEquivalent(docBefore, doc);
        if (!equal) {
          issues.push({
            type: "rollback_document_mismatch",
            message: `Document in collection "${collectionName}" different after rollback.`
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
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ) : Promise<{
    errors: string[];
    issues: { type: string, message: string }[];
  }> {
    const errors: string[] = [];
    const issues: { type: string, message: string }[] = [];
    const currentSchema = definition.schemas.multiCollections || {};
    const parentSchema = definition.parent?.schemas.multiCollections || {};

    // Validate current state multi-collections against their schemas
    for (const [multiCollectionName, currentMultiCollSchema] of Object.entries(currentSchema)) {
      const currentCollState = stateAfter.multiCollections[multiCollectionName];
      const allCollTypes = Object.keys(currentMultiCollSchema);
      for(const element of currentCollState.content) {
        const elementType = element._type as string;
        if(!allCollTypes.includes(elementType)) {
          errors.push(`Document in multi-collection "${multiCollectionName}" has unknown type "${elementType}"`);
          continue;
        }

        const schema = currentMultiCollSchema[elementType];
        const valid = v.safeParse(v.object(schema), element);

        if (!valid.success) {
          errors.push(`Document in multi-collection "${multiCollectionName}" type "${elementType}" does not match schema:\n-> ${valid.issues.map((issue) => {
            return `(${v.getDotPath(issue)}) ${issue.message}`;
          }).join("\n-> ")}`);
        }
      }
    }

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
      const operation = operations[i];
      try {
        stateBeforeRollback = await this.applier.reverseOperation(stateBeforeRollback, operation);
      } catch {
        // Ignore errors during reverse application
      }
    }
    const stateAfterRollback = stateBeforeRollback;

    // Check each multi-collection for schema changes
    for (const [multiCollectionName, parentMultiCollSchema] of Object.entries(parentSchema)) {
      // New multi-collection, no validation needed
      if (!parentMultiCollSchema) continue;
      // Check if schema has changed
      for (const [docIndex, doc] of ((stateAfterRollback.multiCollections || {})[multiCollectionName]?.content || []).entries()) {
        const docType = doc._type as string;
        const parentTypeSchema = parentMultiCollSchema[docType];
        if (!parentTypeSchema) continue; // Type was added, no validation needed
        const valid = v.safeParse(v.object(parentTypeSchema), doc);
        if (!valid.success) {
          errors.push(`The multi-collection "${multiCollectionName}" type "${docType}" not valid after rollback.\n-> ${valid.issues.map((issue) => {
            return `(${v.getDotPath(issue)}) ${issue.message}`;
          }).join("\n-> ")}`);
        }
        const docBefore = (stateBefore.multiCollections || {})[multiCollectionName]?.content?.[docIndex];
        const equal = dirtyEquivalent(docBefore, doc);
        if (!equal) {
          issues.push({
            type: "rollback_document_mismatch",
            message: `Document in multi-collection "${multiCollectionName}" type "${docType}" different after rollback.`
          });
        }
      }
    }

    return {
      errors,
      issues,
    }
  }

  private async validateMultiModelSchemaChanges(
    definition: MigrationDefinition,
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ) : Promise<{
    errors: string[];
    issues: { type: string, message: string }[];
  }> {
    const errors: string[] = [];
    const issues: { type: string, message: string }[] = [];
    const currentSchema = definition.schemas.multiModels || {};
    const parentSchema = definition.parent?.schemas.multiModels || {};
    
    const allModelType = Object.keys(currentSchema);
    
    // Validate current state multi-collection models against their schemas
    for (const [collectionName, instance] of Object.entries(stateAfter.multiModels || {})) {
      const { modelType, content } = instance;
      if (!allModelType.includes(modelType)) {
        errors.push(`Multi-collection model "${collectionName}" instance exists but model is not declared in schema`);
        continue;
      }
      const modelSchema = currentSchema[modelType];
      if (!modelSchema) {
        errors.push(`Multi-collection model "${collectionName}" instance exists but model is not declared in schema`);
        continue;
      }

      for(const element of content) {
        const elementType = element._type as string;
        const allTypes = Object.keys(modelSchema);
        if(!allTypes.includes(elementType)) {
          errors.push(`Document in multi-collection model "${collectionName}" has unknown type "${elementType}"`);
          continue;
        }
        const schema = modelSchema[elementType];
        const valid = v.safeParse(v.object(schema), element);
        if (!valid.success) {
          errors.push(`Document in multi-collection model "${collectionName}" type "${elementType}" does not match schema:\n-> ${valid.issues.map((issue) => {
            return `(${v.getDotPath(issue)}) ${issue.message}`;
          }).join("\n-> ")}`);
        }
      }
    }

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
      const operation = operations[i];
      try {
        stateBeforeRollback = await this.applier.reverseOperation(stateBeforeRollback, operation);
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
      for (const [collectionName, instance] of Object.entries(stateAfterRollback.multiModels || {})) {
        if (instance.modelType !== modelType) continue;
        for (const [docIndex, doc] of instance.content.entries()) {
          const docType = doc._type as string;
          const parentTypeSchema = parentModelSchema[docType];
          if (!parentTypeSchema) continue; // Type was added, no validation needed
          const valid = v.safeParse(v.object(parentTypeSchema), doc);
          if (!valid.success) {
            errors.push(`The multi-collection model "${collectionName}" type "${docType}" not valid after rollback.\n-> ${valid.issues.map((issue) => {
              return `(${v.getDotPath(issue)}) ${issue.message}`;
            }).join("\n-> ")}`);
          }
          const docBefore = (stateBefore.multiModels || {})[collectionName]?.content?.[docIndex];
          const equal = dirtyEquivalent(docBefore, doc);
          if (!equal) {
            issues.push({
              type: "rollback_document_mismatch",
              message: `Document in multi-collection model "${collectionName}" type "${docType}" different after rollback.`
            });
          }
        }
      }
    }

    return {
      errors,
      issues,
    }
  }

  /**
   * Validates that schema changes for multi-collections have corresponding transformations
   *
   * @private
   * @param definition - The migration definition
   * @param stateBefore - Database state before this migration
   * @param operations - Operations in this migration
   * @returns Array of error messages (empty if validation passes)
   */
  private async validateSchemaChanges(
    definition: MigrationDefinition,
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ): Promise<string[]> {
    const errors: string[] = [];
    
    // Validate collection schema changes
    const collectionChangeResult = await this.validateCollectionSchemaChanges(
      definition,
      structuredClone(stateBefore),
      structuredClone(stateAfter),
      operations,
    );
    
    // Validate multi-collection schema changes
    const multiCollectionChangeResult = await this.validateMultiCollectionSchemaChanges(
      definition,
      structuredClone(stateBefore),
      structuredClone(stateAfter),
      operations,
    );

    const multiModelsChangeResult = await this.validateMultiModelSchemaChanges(
      definition,
      structuredClone(stateBefore),
      structuredClone(stateAfter),
      operations,
    );

    errors.push(
      ...[...new Set(collectionChangeResult.errors)],
      ...[...new Set(multiCollectionChangeResult.errors)],
      ...[...new Set(multiModelsChangeResult.errors)],
    );

    return errors;
  }

  /**
   * Validates collection documents against their schemas
   *
   * @private
   */
  private validateCollectionDocuments(
    collections: Record<string, Record<string, unknown>>,
    stateAfter: SimulationDatabaseState,
  ): string[] {
    const errors: string[] = [];

    for (const [collectionName, schema] of Object.entries(collections)) {
      const collectionData = stateAfter.collections?.[collectionName];
      if (!collectionData || collectionData.content.length === 0) continue;

      // Find first invalid document
      for (let i = 0; i < collectionData.content.length; i++) {
        const doc = collectionData.content[i];
        // deno-lint-ignore no-explicit-any
        const validation = v.safeParse(v.object(schema as any), doc);

        if (!validation.success) {
          errors.push(
            ...this.formatValidationError(validation, {
              type: "collection",
              name: collectionName,
              index: i,
            }),
          );
          // Only show first invalid document to avoid spam
          break;
        }
      }
    }

    return errors;
  }

  /**
   * Validates multi-collection documents against their schemas
   *
   * @private
   */
  private validateMultiCollectionDocuments(
    multiCollections: Record<string, Record<string, Record<string, unknown>>>,
    stateAfter: SimulationDatabaseState,
  ): string[] {
    const errors: string[] = [];

    for (
      const [multiCollectionName, types] of Object.entries(multiCollections)
    ) {
      for (
        const [instanceName, instanceData] of Object.entries(
          stateAfter.multiCollections || {},
        )
      ) {
        // Match instance to multi-collection type
        if (!instanceName.startsWith(`${multiCollectionName}@`)) continue;

        // Validate each document
        for (let i = 0; i < instanceData.content.length; i++) {
          const doc = instanceData.content[i];
          const docType = doc._type as string;

          if (!docType || !types[docType]) {
            continue; // Type might be removed, handled by schema change validation
          }

          const schema = types[docType];
          // deno-lint-ignore no-explicit-any
          const validation = v.safeParse(v.object(schema as any), doc);

          if (!validation.success) {
            errors.push(
              ...this.formatValidationError(validation, {
                type: "multicollection",
                name: instanceName,
                index: i,
                typeName: docType,
              }),
            );
            // Only show first invalid document per type to avoid spam
            break;
          }
        }
      }
    }

    return errors;
  }

  /**
   * Generates a mock document from a Valibot schema for testing purposes
   * Uses valibot-mock to generate realistic test data
   *
   * @private
   * @param schema - Valibot schema representing document structure
   * @returns Mock document matching the schema
   */
  private generateMockDocument(
    schema: Record<string, unknown>,
  ): Record<string, unknown> {
    // Wrap the schema in v.object() for valibot-mock
    // deno-lint-ignore no-explicit-any
    const schemaObject = v.object(schema as Record<string, v.BaseSchema<any, any, any>>);
    
    // Use valibot-mock to generate realistic test data from schema
    // deno-lint-ignore no-explicit-any
    const generator = createMockGenerator(schemaObject as any);
    const mockData = generator.generate();
    
    // Validate the generated data matches the schema
    const validation = v.safeParse(schemaObject, mockData);
    if (validation.success) {
      return validation.output;
    }
    // If validation fails (shouldn't happen), fallback to simple mock
    console.warn("/!\\ Generated mock data did not validate against schema, using simple mock instead");
    return mockData;
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
      const builder = migrationBuilder({
        schemas: ancestor.schemas,
        parentSchemas: ancestor.parent?.schemas,
      });
      const state = ancestor.migrate(builder);

      for (const operation of state.operations) {
        currentState = await this.applier.applyOperation(currentState, operation);
      }
    }

    return currentState;
  }

  /**
   * Generates mock documents for empty collections
   *
   * @private
   */
  private populateCollectionsMock(
    state: SimulationDatabaseState,
    collections: Record<string, Record<string, unknown>>,
  ): SimulationDatabaseState {
    const currentState = state;

    for (const [collectionName, schema] of Object.entries(collections)) {
      if(!currentState.collections?.[collectionName]) {
        currentState.collections[collectionName] = { content: [] };
      }
      const collection = currentState.collections?.[collectionName];
      if (!collection) continue;

      const mockDocs: Record<string, unknown>[] = [];
      const docCount = Math.floor(
        Math.random() *
          (MOCK_GENERATION.DOCS_PER_COLLECTION_MAX -
            MOCK_GENERATION.DOCS_PER_COLLECTION_MIN + 1),
      ) + MOCK_GENERATION.DOCS_PER_COLLECTION_MIN;

      for (let i = 0; i < docCount; i++) {
        try {
          const mockDoc = this.generateMockDocument(
            schema as Record<string, unknown>,
          );
          mockDocs.push(mockDoc);
        } catch (_error) {
          break;
        }
      }
      collection.content.push(...mockDocs);
    }

    return currentState;
  }

  /**
   * Supplements multi-collections with mock data
   *
   * @private
   */
  private populateMultiCollectionsMock(
    state: SimulationDatabaseState,
    multiCollections: NonNullable<SchemasDefinition['multiCollections']>,
  ): SimulationDatabaseState {
    const currentState = state;

    for(const [collectionName, schema] of Object.entries(multiCollections)) {
      if(!currentState.multiCollections?.[collectionName]) {
        currentState.multiCollections[collectionName] = { content: [] };
      }
      const collection = currentState.multiCollections?.[collectionName];

      // If collection exists but is empty, add some mock data
      if (!collection) continue;

      const docCount = Math.floor(
        Math.random() *
          (MOCK_GENERATION.DOCS_PER_COLLECTION_MAX -
            MOCK_GENERATION.DOCS_PER_COLLECTION_MIN + 1),
      ) + MOCK_GENERATION.DOCS_PER_COLLECTION_MIN;

      for (let i = 0; i < docCount; i++) {
        try {
          for(const typeName of Object.keys(schema)) {
            const mockDoc = this.generateMockDocument(
              schema[typeName] as Record<string, unknown>,
            );
            collection.content.push({ ...mockDoc, _type: typeName });
          }
        } catch (_error) {
          break;
        }
      }
    }

    return currentState;
  }

  private populateMultiCollectionsModelMock(
    state: SimulationDatabaseState,
    multiModels: NonNullable<SchemasDefinition['multiModels']>,
  ): SimulationDatabaseState {
    const currentState = state;

    for(const [modelType, schema] of Object.entries(multiModels)) {      
      const collectCount = 5;
      for (let i = 0; i < collectCount; i++) {
        const collectionName = `${modelType}@instance${i+1}`;
        currentState.multiModels[collectionName] = {
          modelType,
          content: []
        };
        const modelInstances = currentState.multiModels?.[collectionName];
        
        if(!currentState.multiModels?.[modelType]) {
          currentState.multiModels[modelType] = {
            modelType,
            content: []
          };
        }

        const docCount = Math.floor(
          Math.random() *
            (MOCK_GENERATION.DOCS_PER_COLLECTION_MAX -
              MOCK_GENERATION.DOCS_PER_COLLECTION_MIN + 1),
        ) + MOCK_GENERATION.DOCS_PER_COLLECTION_MIN;

        for (let j = 0; j < docCount; j++) {
          try {
            for(const typeName of Object.keys(schema)) {
              const mockDoc = this.generateMockDocument(
                schema[typeName] as Record<string, unknown>,
              );
              modelInstances.content.push({ ...mockDoc, _type: typeName });
            }
          } catch (_error) {
            break;
          }
        }
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
   * @returns Database state with real seeds + mock data supplements
   */
  private async buildMockStateFromSchemas(
    parent: MigrationDefinition,
  ): Promise<SimulationDatabaseState> {
    let currentState = await this.simulateParentMigrations(parent);
    
    if (parent.schemas.collections) {
      currentState = this.populateCollectionsMock(
        currentState,
        parent.schemas.collections,
      );
    }

    if (parent.schemas.multiCollections) {
      currentState = this.populateMultiCollectionsMock(
        currentState,
        parent.schemas.multiCollections,
      );
    }
    
    if (parent.schemas.multiModels) {
      currentState = this.populateMultiCollectionsModelMock(
        currentState,
        parent.schemas.multiModels,
      );
    }

    return currentState;
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
 *   maxOperations: 500
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

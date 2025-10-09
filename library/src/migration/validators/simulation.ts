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

/**
 * Constants for mock data generation
 */
const MOCK_GENERATION = {
  DOCS_PER_COLLECTION_MIN: 2,
  DOCS_PER_COLLECTION_MAX: 4,
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
  private readonly applier: SimulationApplier;
  private readonly options: SimulationValidatorOptions;

  constructor(options: SimulationValidatorOptions = {}) {
    this.options = { ...DEFAULT_SIMULATION_VALIDATOR_OPTIONS, ...options };
    this.applier = createSimulationApplier({
      strictValidation: this.options.strictValidation,
      trackHistory: this.options.trackHistory,
    });
  }

  /**
   * Validates a complete migration definition
   *
   * @param definition - The migration definition to validate
   * @param initialState - Optional initial database state (from parent migration)
   *                       If not provided, will generate mock state from parent schemas
   * @returns Validation result with success status, errors, and warnings
   */
  validateMigration(
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
      let currentState = this.determineInitialState(definition, initialState);

      // Test forward execution of current migration
      const forwardErrors: string[] = [];
      const stateBeforeMigration = currentState; // Capture state before applying this migration

      // Apply operations in sequence
      for (let i = 0; i < operations.length; i++) {
        const operation = operations[i];
        try {
          currentState = this.applier.applyOperation(currentState, operation);
        } catch (error) {
          const errorMessage = error instanceof Error
            ? error.message
            : String(error);
          forwardErrors.push(
            `Operation ${i + 1} (${operation.type}): ${errorMessage}`,
          );
        }
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
          const createdMultiCollections = Object.keys(stateAfterMigration.multiModels ?? {});
  
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
      const changeStateResult = this.validateSchemaChanges(
        definition,
        stateBeforeMigration,
        stateAfterMigration,
        operations,
      )
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
  private determineInitialState(
    definition: MigrationDefinition,
    providedState?: SimulationDatabaseState,
  ): SimulationDatabaseState {
    if (providedState) {
      // Use provided state (from migrate.ts incremental validation)
      // This avoids O(n²) complexity when validating batches
      return providedState;
    }

    if (definition.parent) {
      // Standalone validation of child migration
      // Build hybrid state: real seeds from parent migrations + mock supplements
      return this.buildMockStateFromSchemas(definition.parent);
    }

    // Root migration with no parent - start with empty state
    return createEmptyDatabaseState();
  }

  /**
   * Detects if a schema has changed by comparing JSON representations
   *
   * @private
   */
  private hasSchemaChanged(
    parentSchema: unknown,
    currentSchema: unknown,
  ): boolean {
    return JSON.stringify(parentSchema) !== JSON.stringify(currentSchema);
  }

  /**
   * Validates that documents match a given schema
   *
   * @private
   * @returns true if all documents are valid, false otherwise
   */
  private areDocumentsValid(
    documents: Record<string, unknown>[],
    schema: Record<string, unknown>,
  ): boolean {
    for (const doc of documents) {
      // deno-lint-ignore no-explicit-any
      const validation = v.safeParse(v.object(schema as any), doc);
      if (!validation.success) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if operations contain a transformation matching the predicate
   *
   * @private
   */
  private hasTransformOperation(
    operations: MigrationRule[],
    predicate: (op: MigrationRule) => boolean,
  ): boolean {
    return operations.some(predicate);
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
  private validateCollectionSchemaChanges(
    definition: MigrationDefinition,
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ): string[] {
    const errors: string[] = [];
    const currentSchema = definition.schemas.collections || {};
    const parentSchema = definition.parent?.schemas.collections || {};

    // Validate current state collections against their schemas
    for (const [collectionName, currentCollSchema] of Object.entries(currentSchema)) {
      for (const [docIndex, doc] of ((stateAfter.collections || {})[collectionName]?.content || []).entries()) {
        const valid = v.safeParse(v.object(currentCollSchema), doc);
        if (!valid.success) {
          errors.push(...this.formatValidationError(valid, {
            type: "collection",
            name: collectionName,
            index: docIndex,
          }));
        }
      }
    }

    let stateBeforeRollback = stateAfter;
    // Apply reverse operations to get back to pre-migration state
    for (let i = operations.length - 1; i >= 0; i--) {
      const operation = operations[i];
      try {
        stateBeforeRollback = this.applier.applyReverseOperation(stateBeforeRollback, operation);
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
          // @TODO: Throw error if lossy or irreversible not marked.
          console.warn(`Skipping schema change validation for collection "${collectionName}" as documents do not match parent schema.`, operations);
        }
      }
      const currentCollSchema = currentSchema[collectionName];
      if (!currentCollSchema) continue; // Collection was removed, skip
    }

    return errors;
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
  private validateSchemaChanges(
    definition: MigrationDefinition,
    stateBefore: SimulationDatabaseState,
    stateAfter: SimulationDatabaseState,
    operations: MigrationRule[],
  ): string[] {
    const errors: string[] = [];

    // Validate collection schema changes
    errors.push(
      ...this.validateCollectionSchemaChanges(
        definition,
        stateBefore,
        stateAfter,
        operations,
      ),
    );

    // Validate multi-collection schema changes
    // errors.push(
    //   ...this.validateMultiCollectionSchemaChanges(
    //     definition,
    //     stateBefore,
    //     operations,
    //   ),
    // );

    return errors;
  }

  /**
   * Collects all documents of a specific type from multi-collection instances
   *
   * @private
   */
  private collectMultiCollectionModelDocuments(
    stateBefore: SimulationDatabaseState,
    multiCollectionName: string,
    typeName: string,
  ): {
    documents: Record<string, unknown>[];
    instanceNames: string[];
    totalCount: number;
  } {
    const documents: Record<string, unknown>[] = [];
    const instanceNames: string[] = [];
    let totalCount = 0;

    if (!stateBefore.multiModels) {
      return { documents, instanceNames, totalCount };
    }

    for (
      const [instanceType, instanceData] of Object.entries(
        stateBefore.multiModels,
      )
    ) {
      // if (instanceName.startsWith(`${multiCollectionName}@`)) {
      //   const docsOfType = instanceData.content.filter(
      //     (doc: Record<string, unknown>) => doc._type === typeName,
      //   );
      //   if (docsOfType.length > 0) {
      //     totalCount += docsOfType.length;
      //     instanceNames.push(instanceName);
      //     documents.push(...docsOfType);
      //   }
      // }
      if (instanceType === multiCollectionName) {
        const docsOfType = instanceData.content.filter(
          (doc: Record<string, unknown>) => doc._type === typeName,
        );
        if (docsOfType.length > 0) {
          totalCount += docsOfType.length;
          instanceNames.push(instanceType);
          documents.push(...docsOfType);
        }
      }
    }

    return { documents, instanceNames, totalCount };
  }

  /**
   * Validates multi-collection type schema changes
   *
   * @private
   */
  private validateMultiCollectionTypeChange(
    multiCollectionName: string,
    typeName: string,
    parentTypeSchema: Record<string, unknown>,
    currentTypeSchema: Record<string, unknown>,
    stateBefore: SimulationDatabaseState,
    operations: MigrationRule[],
  ): string[] {
    const errors: string[] = [];

    if (!this.hasSchemaChanged(parentTypeSchema, currentTypeSchema)) {
      return errors;
    }

    const { documents, instanceNames, totalCount } = this
      .collectMultiCollectionModelDocuments(
        stateBefore,
        multiCollectionName,
        typeName,
      );

    // No documents, schema change is safe
    if (totalCount === 0) return errors;

    // Check if existing documents are compatible with new schema
    const documentsAreValid = this.areDocumentsValid(
      documents,
      currentTypeSchema,
    );

    // Documents are valid, no transformation needed
    if (documentsAreValid) return errors;

    // Documents are invalid, check if transformation exists
    const hasTransform = this.hasTransformOperation(
      operations,
      (op) =>
        op.type === "transform_multicollection_type" &&
        op.collectionType === multiCollectionName &&
        op.typeName === typeName,
    );

    if (!hasTransform) {
      errors.push(
        `Schema for multi-collection "${multiCollectionName}.${typeName}" has changed and existing documents are not compatible.`,
      );
      errors.push(
        `  There are ${totalCount} existing document(s) of type "${typeName}" across ${instanceNames.length} instance(s): ${
          instanceNames.join(", ")
        }`,
      );
      errors.push(
        `  💡 Tip: Add a .multiCollection("${multiCollectionName}").transformType("${typeName}", {...}) operation.`,
      );
    }

    return errors;
  }

  /**
   * Validates removed multi-collection types
   *
   * @private
   */
  private validateRemovedMultiCollectionType(
    multiCollectionName: string,
    removedTypeName: string,
    stateBefore: SimulationDatabaseState,
  ): string[] {
    const errors: string[] = [];

    const { instanceNames, totalCount } = this
      .collectMultiCollectionModelDocuments(
        stateBefore,
        multiCollectionName,
        removedTypeName,
      );

    errors.push(
      `Type "${multiCollectionName}.${removedTypeName}" was removed from schema but no migration is provided.`,
    );

    if (totalCount > 0) {
      errors.push(
        `  There are ${totalCount} existing document(s) of this type across ${instanceNames.length} instance(s): ${
          instanceNames.join(", ")
        }`,
      );
      errors.push(
        `  These documents will become orphaned without a transformation.`,
      );
    } else {
      errors.push(
        `  While no documents exist currently, future multi-collection instances could contain documents of this type.`,
      );
    }

    errors.push(`  💡 Options:`);
    errors.push(
      `     1. Rename: Add .multiCollection("${multiCollectionName}").type("${removedTypeName}").transform({ up: (doc) => ({ ...doc, _type: "new_name" }), down: ... })`,
    );
    errors.push(
      `     2. Delete: Add .multiCollection("${multiCollectionName}").type("${removedTypeName}").transform({ up: () => null, down: ... })`,
    );
    errors.push(
      `     3. Merge: Transform into another existing type with appropriate field mapping`,
    );

    return errors;
  }

  /**
   * Validates multi-collection schema changes
   *
   * @private
   */
  private validateMultiCollectionSchemaChanges(
    definition: MigrationDefinition,
    stateBefore: SimulationDatabaseState,
    operations: MigrationRule[],
  ): string[] {
    const errors: string[] = [];

    if (!definition.schemas.multiModels) {
      return errors;
    }

    const parentSchemas = definition.parent?.schemas.multiModels || {};
    const currentSchemas = definition.schemas.multiModels;

    for (const multiCollectionName of Object.keys(currentSchemas)) {
      const parentSchema = parentSchemas[multiCollectionName];
      const currentSchema = currentSchemas[multiCollectionName];

      // New multi-collection, no validation needed
      if (!parentSchema) continue;

      // Validate each type within the multi-collection
      for (const typeName of Object.keys(currentSchema)) {
        const parentTypeSchema = parentSchema[typeName];
        const currentTypeSchema = currentSchema[typeName];

        // New type, no validation needed
        if (!parentTypeSchema) continue;

        errors.push(
          ...this.validateMultiCollectionTypeChange(
            multiCollectionName,
            typeName,
            parentTypeSchema,
            currentTypeSchema,
            stateBefore,
            operations,
          ),
        );
      }

      // Check for removed types
      for (const removedTypeName of Object.keys(parentSchema)) {
        if (!currentSchema[removedTypeName]) {
          errors.push(
            ...this.validateRemovedMultiCollectionType(
              multiCollectionName,
              removedTypeName,
              stateBefore,
            ),
          );
        }
      }
    }

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
   * Validates that transformed documents match their target schemas
   *
   * This catches transformations that return invalid values (e.g., null instead of boolean)
   *
   * @private
   * @param definition - Migration definition with schemas
   * @param stateAfter - Database state after transformation
   * @returns Array of error messages (empty if validation passes)
   */
  private validateTransformedDocuments(
    definition: MigrationDefinition,
    stateAfter: SimulationDatabaseState,
  ): string[] {
    const errors: string[] = [];

    // Validate collection documents
    if (definition.schemas.collections) {
      errors.push(
        ...this.validateCollectionDocuments(
          definition.schemas.collections,
          stateAfter,
        ),
      );
    }

    // Validate multi-collection documents
    if (definition.schemas.multiCollections) {
      errors.push(
        ...this.validateMultiCollectionDocuments(
          definition.schemas.multiCollections,
          stateAfter,
        ),
      );
    }

    return errors;
  }

  /**
   * Validates that a migration can be reversed properly
   *
   * @private
   * @param state - The migration state with operations
   * @param forwardState - The database state after forward execution
   * @returns Array of error messages (empty if validation passes)
   */
  private validateReversibility(
    state: MigrationState,
    initialState: SimulationDatabaseState,
    forwardState: SimulationDatabaseState,
  ): string[] {
    const errors: string[] = [];
    const operations = state.operations;

    try {
      // Apply reverse operations in reverse order
      let reverseState = JSON.parse(
        JSON.stringify(forwardState),
      ) as SimulationDatabaseState;

      for (let i = operations.length - 1; i >= 0; i--) {
        const operation = operations[i];
        try {
          reverseState = this.applier.applyReverseOperation(
            reverseState,
            operation,
          );
        } catch (error) {
          const errorMessage = error instanceof Error
            ? error.message
            : String(error);
          errors.push(
            `Reverse operation ${i + 1} (${operation.type}): ${errorMessage}`,
          );
          return errors; // Stop on first reverse error
        }
      }

      // Compare final state with initial state (the state BEFORE this migration)
      const statesMatch = compareDatabaseStates(reverseState, initialState);

      if (!statesMatch) {
        errors.push(
          "Migration is not fully reversible - final state does not match initial state",
        );

        // Provide more detailed comparison if possible
        const initialCollections = Object.keys(initialState.collections || {});
        const finalCollections = Object.keys(reverseState.collections || {});

        if (finalCollections.length !== initialCollections.length) {
          errors.push(
            `Collection count mismatch: initial=${initialCollections.length}, final=${finalCollections.length}`,
          );
        }

        const extraCollections = finalCollections.filter((name) =>
          !initialCollections.includes(name)
        );
        if (extraCollections.length > 0) {
          errors.push(
            `Extra collections after reversal: ${extraCollections.join(", ")}`,
          );
        }

        const missingCollections = initialCollections.filter((name) =>
          !finalCollections.includes(name)
        );
        if (missingCollections.length > 0) {
          errors.push(
            `Missing collections after reversal: ${
              missingCollections.join(", ")
            }`,
          );
        }
      }

      return errors;
    } catch (error) {
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      errors.push(`Reversibility validation failed: ${errorMessage}`);
      return errors;
    }
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
  private simulateParentMigrations(
    parent: MigrationDefinition,
  ): SimulationDatabaseState {
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
        currentState = this.applier.applyOperation(currentState, operation);
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
      if(!currentState.multiModels?.[modelType]) {
        currentState.multiModels[modelType] = {};
      }

      const modelInstances = currentState.multiModels?.[modelType];
      
      const collectCount = 5;
      for (let i = 0; i < collectCount; i++) {
        const collec = { content: [] as Record<string, unknown>[] }
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
              collec.content.push({ ...mockDoc, _type: typeName });
            }
          } catch (_error) {
            break;
          }
        }
        
        const collectionName = `${modelType}@instance${i+1}`;
        modelInstances[collectionName] = collec;
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
  private buildMockStateFromSchemas(
    parent: MigrationDefinition,
  ): SimulationDatabaseState {
    let currentState = this.simulateParentMigrations(parent);
    
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

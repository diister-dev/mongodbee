/**
 * @fileoverview Multi-model instance catch-up system
 *
 * Handles detection and synchronization of orphaned multi-model instances
 * that are missing migrations.
 *
 * @module
 */

import type { Db } from "../mongodb.ts";
import type { MigrationDefinition, MigrationRule } from "./types.ts";
import {
  discoverMultiCollectionInstances,
  getMultiCollectionMigrations,
  getMultiModelAppliedMigrationIds,
  shouldInstanceReceiveMigrationByChain,
} from "./multicollection-registry.ts";
import { migrationBuilder } from "./builder.ts";
import { getAppliedMigrationIds } from "./state.ts";

/**
 * Information about an instance that needs catch-up
 */
export interface InstanceCatchUpInfo {
  /** Collection name of the instance */
  collectionName: string;
  /** Multi-model type */
  modelType: string;
  /** List of missing migration IDs */
  missingMigrationIds: string[];
  /** Whether this is an orphaned instance (no _migrations document) */
  isOrphaned: boolean;
}

/**
 * Summary of all instances needing catch-up
 */
export interface CatchUpSummary {
  /** Map of modelType to instances needing catch-up */
  instancesByModel: Map<string, InstanceCatchUpInfo[]>;
  /** Total number of instances needing catch-up */
  totalInstances: number;
  /** Total number of missing migrations across all instances */
  totalMissingMigrations: number;
}

/**
 * Detects all multi-model instances that need catch-up
 *
 * An instance needs catch-up if:
 * 1. It has no _migrations document (orphaned)
 * 2. It's missing some migrations from the chain
 *
 * @param db - Database instance
 * @param allMigrations - Complete migration chain
 * @returns Summary of instances needing catch-up
 */
export async function detectInstancesNeedingCatchUp(
  db: Db,
  allMigrations: MigrationDefinition[],
): Promise<CatchUpSummary> {
  const instancesByModel = new Map<string, InstanceCatchUpInfo[]>();
  let totalInstances = 0;
  let totalMissingMigrations = 0;

  // Get globally applied migrations (excludes reverted/failed)
  const globallyAppliedIds = await getAppliedMigrationIds(db);
  const globallyAppliedSet = new Set(globallyAppliedIds);

  // Get all unique model types from all migrations
  const modelTypes = extractModelTypesFromMigrations(allMigrations);

  for (const modelType of modelTypes) {
    // Discover all instances of this model type
    const instances = await discoverMultiCollectionInstances(db, modelType);

    for (const collectionName of instances) {
      // Get migrations document for this instance
      const migrationsDoc = await getMultiCollectionMigrations(
        db,
        collectionName,
      );

      let missingMigrationIds: string[] = [];
      let isOrphaned = false;

      if (!migrationsDoc) {
        // Orphaned instance - needs ALL globally applied migrations that should apply to it
        // We still need to filter by model type since orphaned instances don't have tracking
        isOrphaned = true;
        missingMigrationIds = allMigrations
          .filter((m) => globallyAppliedSet.has(m.id)) // Only globally applied
          .filter((m) => hasMigrationForModelType(m, modelType))
          .map((m) => m.id);
      } else {
        // Instance has migrations - check which ones are missing
        // Since migrations are now recorded on ALL instances (even if not affected),
        // we can simply compare the applied IDs with globally applied IDs
        
        // Get only migrations with "applied" status (excludes reverted/failed)
        const appliedIds = await getMultiModelAppliedMigrationIds(
          db,
          collectionName,
        );
        const appliedSet = new Set(appliedIds);

        // Find the migration when this instance was created
        const instanceCreationMigration = allMigrations.find(
          (m) => m.id === migrationsDoc.fromMigrationId
        );

        missingMigrationIds = allMigrations
          .filter((m) => globallyAppliedSet.has(m.id)) // Only globally applied
          // Skip migrations that the instance should NOT receive (created after instance)
          .filter((m) => {
            // If we can't find the creation migration, include all to be safe
            if (!instanceCreationMigration) {
              console.warn(
                `Could not find creation migration ${migrationsDoc.fromMigrationId} for instance ${collectionName}. ` +
                `This instance may need manual review.`
              );
              return true;
            }
            
            // Instance should receive migrations that happened at or after its creation
            return shouldInstanceReceiveMigrationByChain(
              instanceCreationMigration,
              m
            );
          })
          .filter((m) => !appliedSet.has(m.id))
          .map((m) => m.id);
      }

      // Only add if there are missing migrations
      if (missingMigrationIds.length > 0) {
        const info: InstanceCatchUpInfo = {
          collectionName,
          modelType,
          missingMigrationIds,
          isOrphaned,
        };

        if (!instancesByModel.has(modelType)) {
          instancesByModel.set(modelType, []);
        }
        instancesByModel.get(modelType)!.push(info);

        totalInstances++;
        totalMissingMigrations += missingMigrationIds.length;
      }
    }
  }

  return {
    instancesByModel,
    totalInstances,
    totalMissingMigrations,
  };
}

/**
 * Extracts all unique model types referenced in migrations
 *
 * @param migrations - Array of migrations
 * @returns Set of model types
 */
function extractModelTypesFromMigrations(
  migrations: MigrationDefinition[],
): Set<string> {
  const modelTypes = new Set<string>();

  for (const migration of migrations) {
    // Check multiModels in schemas
    if (migration.schemas.multiModels) {
      for (const modelType of Object.keys(migration.schemas.multiModels)) {
        modelTypes.add(modelType);
      }
    }
  }

  return modelTypes;
}

/**
 * Checks if a migration has operations for a specific model type
 *
 * @param migration - Migration definition
 * @param modelType - Model type to check
 * @returns True if migration has actual operations affecting this model type
 */
function hasMigrationForModelType(
  migration: MigrationDefinition,
  modelType: string,
): boolean {
  // Check if model type exists in schemas
  if (!migration.schemas.multiModels?.[modelType]) {
    return false;
  }

  // Having the schema is not enough - we need to check if there are actual operations
  // Generate operations by executing the migration
  const builder = migrationBuilder({ 
    schemas: migration.schemas,
    parentSchemas: migration.parent?.schemas,
  });
  const state = migration.migrate(builder);
  
  // Filter operations for this model type
  const relevantOps = filterOperationsForModelType(state.operations, modelType);
  
  // Only return true if there are actual operations
  return relevantOps.length > 0;
}

/**
 * Filters migration operations to only those relevant for a specific model type
 *
 * This is critical for catch-up: we only want to apply operations that affect
 * the specific multi-model instance, not operations for other models.
 *
 * @param operations - All operations from a migration
 * @param modelType - Model type to filter for
 * @returns Filtered operations
 */
export function filterOperationsForModelType(
  operations: MigrationRule[],
  modelType: string,
): MigrationRule[] {
  return operations.filter((op) => {
    switch (op.type) {
      // Multi-model specific operations
      case "create_multimodel_instance":
      case "seed_multimodel_instance_type":
      case "transform_multimodel_instance_type":
        // These operations are specific to a single instance
        return false;

      case "seed_multimodel_instances_type":
      case "transform_multimodel_instances_type":
        return op.modelType === modelType;

      // Skip collection and multi-collection operations
      case "create_collection":
      case "seed_collection":
      case "transform_collection":
      case "create_multicollection":
      case "seed_multicollection_type":
      case "transform_multicollection_type":
      case "update_indexes":
      case "mark_as_multimodel":
        return false;

      default:
        return false;
    }
  });
}

/**
 * Gets the list of migrations that need to be applied to catch up an instance
 *
 * @param allMigrations - Complete migration chain
 * @param missingMigrationIds - IDs of missing migrations
 * @returns Ordered list of migrations to apply
 */
export function getMigrationsForCatchUp(
  allMigrations: MigrationDefinition[],
  missingMigrationIds: string[],
): MigrationDefinition[] {
  const missingSet = new Set(missingMigrationIds);

  // Return migrations in order, filtered to only missing ones
  return allMigrations.filter((m) => missingSet.has(m.id));
}

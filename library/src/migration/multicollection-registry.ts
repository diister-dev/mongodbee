/**
 * @fileoverview Multi-collection instance registry and discovery
 *
 * This module handles tracking and discovering instances of multi-collections.
 * Each multi-collection instance stores metadata in special documents with _type="_information".
 *
 * @module
 */

import type { Db } from '../mongodb.ts';

/**
 * Special document types reserved for multi-collection metadata
 */
export const MULTI_COLLECTION_INFO_TYPE = '_information';
export const MULTI_COLLECTION_MIGRATIONS_TYPE = '_migrations';

/**
 * Information document stored in each multi-collection instance
 */
export type MultiCollectionInfo = {
  _type: typeof MULTI_COLLECTION_INFO_TYPE;
  multiCollectionType: string;
  instanceName: string;
  createdAt: Date;
  createdByMigration: string;
  schemas: Record<string, unknown>;
};

/**
 * Migrations document stored in each multi-collection instance
 */
export type MultiCollectionMigrations = {
  _type: typeof MULTI_COLLECTION_MIGRATIONS_TYPE;
  appliedMigrations: Array<{
    id: string;
    appliedAt: Date;
  }>;
};

/**
 * Discovers all instances of a specific multi-collection type
 *
 * @param db - Database instance
 * @param multiCollectionType - The type of multi-collection to discover
 * @returns Array of instance names
 */
export async function discoverMultiCollectionInstances(
  db: Db,
  multiCollectionType: string
): Promise<string[]> {
  // List all collections in the database
  const collections = await db.listCollections().toArray();
  const instances: string[] = [];

  // Check each collection for multi-collection metadata
  for (const collInfo of collections) {
    const collName = collInfo.name;

    // Skip system collections
    if (collName.startsWith('system.') || collName.startsWith('mongodbee_')) {
      continue;
    }

    try {
      const collection = db.collection(collName);

      // Check if this collection has multi-collection info
      const info = await collection.findOne({
        _type: MULTI_COLLECTION_INFO_TYPE,
      }) as MultiCollectionInfo | null;

      if (info && info.multiCollectionType === multiCollectionType) {
        instances.push(info.instanceName);
      }
    } catch (_error) {
      // Silently skip collections that can't be read
      continue;
    }
  }

  return instances.sort();
}

/**
 * Gets information about a multi-collection instance
 *
 * @param db - Database instance
 * @param multiCollectionName - Name of the multi-collection template
 * @param instanceName - Name of the specific instance
 * @returns The information document or null if not found
 */
export async function getMultiCollectionInfo(
  db: Db,
  multiCollectionName: string,
  instanceName: string
): Promise<MultiCollectionInfo | null> {
  const collectionName = `${multiCollectionName}_${instanceName}`;
  const collection = db.collection(collectionName);

  return await collection.findOne({
    _type: MULTI_COLLECTION_INFO_TYPE,
  }) as MultiCollectionInfo | null;
}

/**
 * Creates information document for a new multi-collection instance
 *
 * @param db - Database instance
 * @param multiCollectionName - Name of the multi-collection template
 * @param instanceName - Name of the specific instance
 * @param migrationId - ID of the migration creating this instance (optional, defaults to 'unknown')
 * @param schemas - Schema snapshot
 */
export async function createMultiCollectionInfo(
  db: Db,
  multiCollectionName: string,
  instanceName: string,
  migrationId: string = 'unknown',
  schemas: Record<string, unknown> = {}
): Promise<void> {
  // Build collection name from multi-collection name and instance name
  const collectionName = `${multiCollectionName}_${instanceName}`;
  const collection = db.collection(collectionName);

  const info: MultiCollectionInfo = {
    _type: MULTI_COLLECTION_INFO_TYPE,
    multiCollectionType: multiCollectionName,
    instanceName,
    createdAt: new Date(),
    createdByMigration: migrationId,
    schemas,
  };

  await collection.insertOne(info as Record<string, unknown>);

  // Also create the migrations tracking document
  const migrations: MultiCollectionMigrations = {
    _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
    appliedMigrations: [
      {
        id: migrationId,
        appliedAt: new Date(),
      }
    ],
  };

  await collection.insertOne(migrations as Record<string, unknown>);
}

/**
 * Records a migration as applied to a multi-collection instance
 *
 * @param db - Database instance
 * @param multiCollectionName - Name of the multi-collection template
 * @param instanceName - Name of the specific instance
 * @param migrationId - ID of the migration
 */
export async function recordMultiCollectionMigration(
  db: Db,
  multiCollectionName: string,
  instanceName: string,
  migrationId: string
): Promise<void> {
  // Build collection name from multi-collection name and instance name
  const collectionName = `${multiCollectionName}_${instanceName}`;
  const collection = db.collection(collectionName);

  await collection.updateOne(
    { _type: MULTI_COLLECTION_MIGRATIONS_TYPE } as Record<string, unknown>,
    {
      $push: {
        appliedMigrations: {
          id: migrationId,
          appliedAt: new Date(),
        }
      }
    } as Record<string, unknown>
  );
}

/**
 * Gets all migrations applied to a multi-collection instance
 *
 * @param db - Database instance
 * @param multiCollectionName - Name of the multi-collection template
 * @param instanceName - Name of the specific instance
 * @returns The migrations document or null
 */
export async function getMultiCollectionMigrations(
  db: Db,
  multiCollectionName: string,
  instanceName: string
): Promise<MultiCollectionMigrations | null> {
  const collectionName = `${multiCollectionName}_${instanceName}`;
  const collection = db.collection(collectionName);

  return await collection.findOne({
    _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
  }) as MultiCollectionMigrations | null;
}

/**
 * Checks if a multi-collection instance exists
 *
 * @param db - Database instance
 * @param multiCollectionName - Name of the multi-collection template
 * @param instanceName - Name of the specific instance
 * @returns True if the instance exists
 */
export async function multiCollectionInstanceExists(
  db: Db,
  multiCollectionName: string,
  instanceName: string
): Promise<boolean> {
  const info = await getMultiCollectionInfo(db, multiCollectionName, instanceName);
  return info !== null;
}

/**
 * Checks if an instance was created after a specific migration
 *
 * Compares migration IDs using timestamp prefix (YYYY_MM_DD_HHMM_ULID@name format)
 * Returns true if the instance was created AFTER the specified migration
 *
 * @param instanceCreatedAtMigrationId - Migration ID when instance was created
 * @param currentMigrationId - Migration ID to compare against
 * @returns True if instance was created after the current migration
 *
 * @example
 * ```typescript
 * const info = await getMultiCollectionInfo(db, "catalog", "louvre");
 * const skipInstance = isInstanceCreatedAfterMigration(
 *   info.createdByMigration,
 *   "2025_09_29_2136_BFMP698V60@initial"
 * );
 * // Returns true if instance was created after the "initial" migration
 * ```
 */
export function isInstanceCreatedAfterMigration(
  instanceCreatedAtMigrationId: string,
  currentMigrationId: string
): boolean {
  // Handle special cases
  if (instanceCreatedAtMigrationId === 'unknown' || instanceCreatedAtMigrationId === 'current') {
    // Unknown creation = assume old, needs all migrations
    return false;
  }

  if (currentMigrationId === 'unknown') {
    return false;
  }

  // Extract timestamp parts (format: YYYY_MM_DD_HHMM_ULID@name)
  const extractTimestamp = (migrationId: string): string => {
    const parts = migrationId.split('@')[0];
    return parts || migrationId;
  };

  const instanceTimestamp = extractTimestamp(instanceCreatedAtMigrationId);
  const currentTimestamp = extractTimestamp(currentMigrationId);

  // Lexicographic comparison works because of the date format (YYYY_MM_DD_HHMM)
  return instanceTimestamp > currentTimestamp;
}

/**
 * Checks if an instance should receive a specific migration
 *
 * An instance should receive a migration if it was created BEFORE or AT that migration.
 * Instances created AFTER a migration don't need it (they already have that schema).
 *
 * @param db - Database instance
 * @param multiCollectionName - Name of the multi-collection template
 * @param instanceName - Name of the specific instance
 * @param migrationId - Migration ID to check
 * @returns True if the instance should receive this migration
 */
export async function shouldInstanceReceiveMigration(
  db: Db,
  multiCollectionName: string,
  instanceName: string,
  migrationId: string
): Promise<boolean> {
  const info = await getMultiCollectionInfo(db, multiCollectionName, instanceName);

  if (!info) {
    // Instance doesn't exist, can't receive migration
    return false;
  }

  // Instance should receive migration if it was created before or at this migration
  return !isInstanceCreatedAfterMigration(info.createdByMigration, migrationId);
}
/**
 * Marks an existing collection as a multi-collection instance
 * 
 * Use this to retroactively register a collection that was created manually
 * or to "adopt" an existing collection into the multi-collection system.
 * 
 * ⚠️ WARNING: This does NOT validate that the collection has the correct structure!
 * Make sure the collection already contains documents with `_type` fields.
 * 
 * @param db - Database instance
 * @param collectionName - Full name of the existing collection (e.g., "library_central")
 * @param multiCollectionName - Name of the multi-collection template (e.g., "library")
 * @param instanceName - Instance name (e.g., "central")
 * @param createdByMigrationId - The migration ID to mark as creation point (defaults to last applied migration)
 * @param schemas - Optional schemas to store in metadata
 * 
 * @example
 * ```typescript
 * // Adopt an existing collection that was created manually
 * await markAsMultiCollection(
 *   db,
 *   "library_central",
 *   "library",
 *   "central",
 *   "2025_10_02_0201_H3KFNKY03S@initial"  // Mark as created at M1
 * );
 * ```
 */
export async function markAsMultiCollection(
  db: Db,
  collectionName: string,
  multiCollectionName: string,
  instanceName: string,
  createdByMigrationId?: string,
  schemas: Record<string, unknown> = {}
): Promise<void> {
  const collection = db.collection(collectionName);

  // Check if already marked
  const existing = await collection.findOne({
    _type: MULTI_COLLECTION_INFO_TYPE
  });

  if (existing) {
    throw new Error(
      `Collection ${collectionName} is already marked as a multi-collection instance. ` +
      `Use updateMultiCollectionInfo() to update metadata.`
    );
  }

  // Get migration ID if not provided
  let migrationId = createdByMigrationId;
  if (!migrationId) {
    const { getLastAppliedMigration } = await import('./state.ts');
    const lastMigration = await getLastAppliedMigration(db);
    migrationId = lastMigration?.id || 'current';
  }

  // Create the metadata
  await createMultiCollectionInfo(
    db,
    multiCollectionName,
    instanceName,
    migrationId,
    schemas
  );
}

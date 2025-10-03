/**
 * @fileoverview Multi-collection instance registry and discovery
 *
 * This module handles tracking and discovering instances of multi-collections.
 * Each multi-collection instance stores metadata in special documents with _type="_information".
 *
 * @module
 */

import type { Db } from '../mongodb.ts';
import { getCurrentVersion } from './utils/package-info.ts';

/**
 * Special document types reserved for multi-collection metadata
 */
export const MULTI_COLLECTION_INFO_TYPE = '_information';
export const MULTI_COLLECTION_MIGRATIONS_TYPE = '_migrations';

/**
 * Information document stored in each multi-collection instance
 */
export type MultiCollectionInfo = {
  _id: typeof MULTI_COLLECTION_INFO_TYPE;
  _type: typeof MULTI_COLLECTION_INFO_TYPE;
  collectionType: string;
  createdAt: Date;
};

/**
 * Migrations document stored in each multi-collection instance
 */
export type MultiCollectionMigrations = {
  _id: typeof MULTI_COLLECTION_MIGRATIONS_TYPE;
  _type: typeof MULTI_COLLECTION_MIGRATIONS_TYPE;
  fromMigrationId: string;
  mongodbeeVersion: string;
  appliedMigrations: Array<{
    id: string;
    appliedAt: Date;
    mongodbeeVersion: string;
  }>;
};

/**
 * Discovers all instances of a specific multi-collection type
 *
 * @param db - Database instance
 * @param collectionType - The type/model of multi-collection to discover
 * @returns Array of collection names
 */
export async function discoverMultiCollectionInstances(
  db: Db,
  collectionType: string
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

      if (info && info.collectionType === collectionType) {
        instances.push(collName); // Return the full collection name
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
 * @param collectionName - Full name of the collection
 * @returns The information document or null if not found
 */
export async function getMultiCollectionInfo(
  db: Db,
  collectionName: string
): Promise<MultiCollectionInfo | null> {
  const collection = db.collection(collectionName);

  return await collection.findOne({
    _type: MULTI_COLLECTION_INFO_TYPE,
  }) as MultiCollectionInfo | null;
}

/**
 * Creates information document for a new multi-collection instance
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @param collectionType - Type/model name of the multi-collection
 * @param migrationId - ID of the migration creating this instance (optional, defaults to 'unknown')
 */
export async function createMultiCollectionInfo(
  db: Db,
  collectionName: string,
  collectionType: string,
  migrationId: string = 'unknown'
): Promise<void> {
  const collection = db.collection(collectionName);
  const mongodbeeVersion = getCurrentVersion();

  const info: MultiCollectionInfo = {
    _id: MULTI_COLLECTION_INFO_TYPE,
    _type: MULTI_COLLECTION_INFO_TYPE,
    collectionType,
    createdAt: new Date(),
  };

  await collection.insertOne(info as Record<string, unknown>);

  // Also create the migrations tracking document
  const migrations: MultiCollectionMigrations = {
    _id: MULTI_COLLECTION_MIGRATIONS_TYPE,
    _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
    fromMigrationId: migrationId,
    mongodbeeVersion,
    appliedMigrations: [
      {
        id: migrationId,
        appliedAt: new Date(),
        mongodbeeVersion,
      }
    ],
  };

  await collection.insertOne(migrations as Record<string, unknown>);
}

/**
 * Records a migration as applied to a multi-collection instance
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @param migrationId - ID of the migration
 */
export async function recordMultiCollectionMigration(
  db: Db,
  collectionName: string,
  migrationId: string
): Promise<void> {
  const collection = db.collection(collectionName);
  const mongodbeeVersion = getCurrentVersion();

  await collection.updateOne(
    { _type: MULTI_COLLECTION_MIGRATIONS_TYPE } as Record<string, unknown>,
    {
      $push: {
        appliedMigrations: {
          id: migrationId,
          appliedAt: new Date(),
          mongodbeeVersion,
        }
      }
    } as Record<string, unknown>
  );
}

/**
 * Gets all migrations applied to a multi-collection instance
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @returns The migrations document or null
 */
export async function getMultiCollectionMigrations(
  db: Db,
  collectionName: string
): Promise<MultiCollectionMigrations | null> {
  const collection = db.collection(collectionName);

  return await collection.findOne({
    _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
  }) as MultiCollectionMigrations | null;
}

/**
 * Checks if a multi-collection instance exists
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @returns True if the instance exists
 */
export async function multiCollectionInstanceExists(
  db: Db,
  collectionName: string
): Promise<boolean> {
  try {
    const collection = db.collection(collectionName);
    const info = await collection.findOne({
      _type: MULTI_COLLECTION_INFO_TYPE,
    }) as MultiCollectionInfo | null;
    return info !== null;
  } catch (_error) {
    return false;
  }
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
 * @param collectionName - Full name of the collection
 * @param migrationId - Migration ID to check
 * @returns True if the instance should receive this migration
 */
export async function shouldInstanceReceiveMigration(
  db: Db,
  collectionName: string,
  migrationId: string
): Promise<boolean> {
  try {
    const collection = db.collection(collectionName);
    const migrations = await collection.findOne({
      _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
    }) as MultiCollectionMigrations | null;

    if (!migrations) {
      // No migrations document, can't receive migration
      return false;
    }

    // Instance should receive migration if it was created before or at this migration
    return !isInstanceCreatedAfterMigration(migrations.fromMigrationId, migrationId);
  } catch (_error) {
    return false;
  }
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
 * @param collectionName - Full name of the existing collection
 * @param collectionType - Type/model name of the multi-collection
 * @param fromMigrationId - The migration ID to mark as creation point (defaults to last applied migration)
 *
 * @example
 * ```typescript
 * // Adopt an existing collection that was created manually
 * await markAsMultiCollection(
 *   db,
 *   "library_central",
 *   "library",
 *   "2025_10_02_0201_H3KFNKY03S@initial"
 * );
 * ```
 */
export async function markAsMultiCollection(
  db: Db,
  collectionName: string,
  collectionType: string,
  fromMigrationId?: string
): Promise<void> {
  const collection = db.collection(collectionName);

  // Check if already marked
  const existing = await collection.findOne({
    _type: MULTI_COLLECTION_INFO_TYPE
  });

  if (existing) {
    throw new Error(
      `Collection ${collectionName} is already marked as a multi-collection instance.`
    );
  }

  // Get migration ID if not provided
  let migrationId = fromMigrationId;
  if (!migrationId) {
    const { getLastAppliedMigration } = await import('./state.ts');
    const lastMigration = await getLastAppliedMigration(db);
    migrationId = lastMigration?.id || 'current';
  }

  // Create the metadata
  await createMultiCollectionInfo(
    db,
    collectionName,
    collectionType,
    migrationId
  );
}

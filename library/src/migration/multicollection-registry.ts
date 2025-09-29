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
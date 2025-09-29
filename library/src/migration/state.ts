/**
 * @fileoverview Migration state tracking system
 *
 * This module handles tracking which migrations have been applied to the database.
 * State is stored in a dedicated MongoDB collection.
 *
 * @module
 */

import type { Db, Collection } from '../mongodb.ts';

/**
 * State of a migration in the database
 */
export type MigrationStateRecord = {
  /** Unique migration ID */
  id: string;

  /** Human-readable migration name */
  name: string;

  /** Current status of the migration */
  status: 'pending' | 'applied' | 'failed' | 'reverted';

  /** When the migration was applied */
  appliedAt?: Date;

  /** When the migration was reverted (if applicable) */
  revertedAt?: Date;

  /** Error message if migration failed */
  error?: string;

  /** Checksum of migration file to detect changes */
  checksum?: string;

  /** Duration of migration execution in milliseconds */
  duration?: number;
};

/**
 * Name of the collection used to track migration state
 */
export const MIGRATION_STATE_COLLECTION = 'mongodbee_state';

/**
 * Gets the migration state collection
 */
export function getMigrationStateCollection(db: Db): Collection<MigrationStateRecord> {
  return db.collection(MIGRATION_STATE_COLLECTION);
}

/**
 * Gets the current state of all migrations
 */
export async function getAllMigrationStates(db: Db): Promise<MigrationStateRecord[]> {
  const collection = getMigrationStateCollection(db);
  return await collection.find({}).sort({ appliedAt: 1 }).toArray();
}

/**
 * Gets the state of a specific migration
 */
export async function getMigrationState(db: Db, migrationId: string): Promise<MigrationStateRecord | null> {
  const collection = getMigrationStateCollection(db);
  return await collection.findOne({ id: migrationId });
}

/**
 * Checks if a migration has been applied
 */
export async function isMigrationApplied(db: Db, migrationId: string): Promise<boolean> {
  const state = await getMigrationState(db, migrationId);
  return state?.status === 'applied';
}

/**
 * Marks a migration as applied
 */
export async function markMigrationAsApplied(
  db: Db,
  migrationId: string,
  name: string,
  duration?: number,
  checksum?: string
): Promise<void> {
  const collection = getMigrationStateCollection(db);

  await collection.updateOne(
    { id: migrationId },
    {
      $set: {
        id: migrationId,
        name,
        status: 'applied' as const,
        appliedAt: new Date(),
        duration,
        checksum,
        error: undefined,
      }
    },
    { upsert: true }
  );
}

/**
 * Marks a migration as failed
 */
export async function markMigrationAsFailed(
  db: Db,
  migrationId: string,
  name: string,
  error: string
): Promise<void> {
  const collection = getMigrationStateCollection(db);

  await collection.updateOne(
    { id: migrationId },
    {
      $set: {
        id: migrationId,
        name,
        status: 'failed' as const,
        error,
        appliedAt: new Date(),
      }
    },
    { upsert: true }
  );
}

/**
 * Marks a migration as reverted
 */
export async function markMigrationAsReverted(
  db: Db,
  migrationId: string
): Promise<void> {
  const collection = getMigrationStateCollection(db);

  await collection.updateOne(
    { id: migrationId },
    {
      $set: {
        status: 'reverted' as const,
        revertedAt: new Date(),
      }
    }
  );
}

/**
 * Gets a list of applied migration IDs in order
 */
export async function getAppliedMigrationIds(db: Db): Promise<string[]> {
  const states = await getAllMigrationStates(db);
  return states
    .filter(s => s.status === 'applied')
    .map(s => s.id);
}

/**
 * Gets the last applied migration
 */
export async function getLastAppliedMigration(db: Db): Promise<MigrationStateRecord | null> {
  const collection = getMigrationStateCollection(db);
  const results = await collection
    .find({ status: 'applied' })
    .sort({ appliedAt: -1 })
    .limit(1)
    .toArray();

  return results[0] ?? null;
}

/**
 * Clears all migration state (dangerous, use with caution)
 */
export async function clearMigrationState(db: Db): Promise<void> {
  const collection = getMigrationStateCollection(db);
  await collection.deleteMany({});
}
/**
 * @fileoverview Migration history tracking system (Event Sourcing)
 *
 * This module implements an event sourcing approach for tracking migration operations.
 * Instead of updating a single state record, we append operation records to get a complete
 * audit trail of all migration operations.
 *
 * @module
 */

import { Collection } from "mongodb";
import type { Db } from '../mongodb.ts';
import { getCurrentVersion } from "./utils/package-info.ts";

/**
 * Type of migration operation
 */
export type MigrationOperationType = 'applied' | 'reverted' | 'failed';

/**
 * Status of operation execution
 */
export type OperationStatus = 'success' | 'failure';

/**
 * Record of a migration operation (event sourcing)
 */
export type MigrationOperation = {
  /** Unique ID for this operation record */
  _id?: unknown;

  /** ID of the migration */
  migrationId: string;

  /** Name of the migration */
  migrationName: string;

  /** Type of operation performed */
  operation: MigrationOperationType;

  /** When the operation was executed */
  executedAt: Date;

  /** Duration of operation in milliseconds */
  duration?: number;

  /** Error message if operation failed */
  error?: string;

  /** Status of the operation */
  status: OperationStatus;

  /** Version of MongoDBee that executed this operation */
  mongodbeeVersion: string;
};

/**
 * Name of the collection used to track migration operations
 */
export const MIGRATION_OPERATIONS_COLLECTION = '__dbee_migration__';

/**
 * Gets the migration operations collection
 */
export function getMigrationOperationsCollection(db: Db): Collection<MigrationOperation> {
  return db.collection(MIGRATION_OPERATIONS_COLLECTION);
}

/**
 * Records a migration operation in the history
 *
 * @param db - Database instance
 * @param migrationId - ID of the migration
 * @param migrationName - Name of the migration
 * @param operation - Type of operation (applied, reverted, failed)
 * @param duration - Duration in milliseconds
 * @param error - Error message if operation failed
 */
export async function recordOperation(
  db: Db,
  migrationId: string,
  migrationName: string,
  operation: MigrationOperationType,
  duration?: number,
  error?: string
): Promise<void> {
  const collection = getMigrationOperationsCollection(db);
  const mongodbeeVersion = getCurrentVersion();

  const record: MigrationOperation = {
    migrationId,
    migrationName,
    operation,
    executedAt: new Date(),
    duration,
    error,
    status: error ? 'failure' : 'success',
    mongodbeeVersion,
  };

  await collection.insertOne(record as any);
}

/**
 * Gets the complete history of operations for a specific migration
 *
 * @param db - Database instance
 * @param migrationId - ID of the migration
 * @returns Array of operations ordered by execution time
 */
export async function getMigrationHistory(
  db: Db,
  migrationId: string
): Promise<MigrationOperation[]> {
  const collection = getMigrationOperationsCollection(db);

  return await collection
    .find({ migrationId })
    .sort({ executedAt: 1 })
    .toArray();
}

/**
 * Gets the last operation for a specific migration
 *
 * @param db - Database instance
 * @param migrationId - ID of the migration
 * @returns The last operation or null if none exists
 */
export async function getLastOperation(
  db: Db,
  migrationId: string
): Promise<MigrationOperation | null> {
  const collection = getMigrationOperationsCollection(db);

  const results = await collection
    .find({ migrationId })
    .sort({ executedAt: -1 })
    .limit(1)
    .toArray();

  return results[0] ?? null;
}

/**
 * Gets the complete history of all migration operations
 *
 * @param db - Database instance
 * @returns Array of all operations ordered by execution time
 */
export async function getAllOperations(db: Db): Promise<MigrationOperation[]> {
  const collection = getMigrationOperationsCollection(db);

  return await collection
    .find({})
    .sort({ executedAt: 1 })
    .toArray();
}

/**
 * Calculates the current state of a migration from its history
 *
 * @param operations - Array of operations for a migration
 * @returns Current status ('pending', 'applied', 'failed', 'reverted')
 */
export function calculateMigrationState(
  operations: MigrationOperation[]
): 'pending' | 'applied' | 'failed' | 'reverted' {
  if (operations.length === 0) {
    return 'pending';
  }

  // Get last successful operation
  const lastSuccessful = operations
    .filter(op => op.status === 'success')
    .pop();

  if (!lastSuccessful) {
    // All operations failed
    return 'failed';
  }

  // Return state based on last successful operation
  switch (lastSuccessful.operation) {
    case 'applied':
      return 'applied';
    case 'reverted':
      return 'reverted';
    case 'failed':
      return 'failed';
    default:
      return 'pending';
  }
}

/**
 * Gets the current state of all migrations
 *
 * @param db - Database instance
 * @returns Map of migration ID to current state
 */
export async function getCurrentState(db: Db): Promise<Map<string, {
  status: 'pending' | 'applied' | 'failed' | 'reverted';
  lastOperation?: MigrationOperation;
}>> {
  const allOperations = await getAllOperations(db);

  // Group operations by migration ID
  const byMigration = new Map<string, MigrationOperation[]>();

  for (const op of allOperations) {
    const existing = byMigration.get(op.migrationId) || [];
    existing.push(op);
    byMigration.set(op.migrationId, existing);
  }

  // Calculate state for each migration
  const states = new Map();

  for (const [migrationId, operations] of byMigration.entries()) {
    const status = calculateMigrationState(operations);
    const lastOp = operations[operations.length - 1];

    states.set(migrationId, {
      status,
      lastOperation: lastOp,
    });
  }

  return states;
}

/**
 * Gets IDs of migrations that are currently applied
 *
 * @param db - Database instance
 * @returns Array of migration IDs that are in 'applied' state
 */
export async function getAppliedMigrationIds(db: Db): Promise<string[]> {
  const states = await getCurrentState(db);
  const applied: string[] = [];

  for (const [migrationId, state] of states.entries()) {
    if (state.status === 'applied') {
      applied.push(migrationId);
    }
  }

  return applied;
}

/**
 * Gets the last successfully applied migration
 *
 * @param db - Database instance
 * @returns The last applied migration operation or null
 */
export async function getLastAppliedMigration(db: Db): Promise<MigrationOperation | null> {
  const collection = getMigrationOperationsCollection(db);

  const results = await collection
    .find({
      operation: 'applied',
      status: 'success',
    })
    .sort({ executedAt: -1 })
    .limit(1)
    .toArray();

  return results[0] ?? null;
}

/**
 * Clears all migration operations (dangerous, use with caution)
 *
 * @param db - Database instance
 */
export async function clearAllOperations(db: Db): Promise<void> {
  const collection = getMigrationOperationsCollection(db);
  await collection.deleteMany({});
}
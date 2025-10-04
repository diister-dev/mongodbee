/**
 * @fileoverview Migration state tracking system (Compatibility Layer)
 *
 * This module provides a compatibility layer over the new history-based system.
 * It maintains the same API but uses the event sourcing history internally.
 *
 * @module
 */

import type { Db } from "../mongodb.ts";
import {
  calculateMigrationState,
  clearAllOperations,
  getAppliedMigrationIds as getAppliedIdsFromHistory,
  getCurrentState,
  getLastAppliedMigration as getLastAppliedFromHistory,
  getLastOperation,
  getMigrationHistory,
  type MigrationOperation,
  recordOperation,
} from "./history.ts";

/**
 * State of a migration in the database (computed from history)
 */
export type MigrationStateRecord = {
  /** Unique migration ID */
  id: string;

  /** Human-readable migration name */
  name: string;

  /** Current status of the migration */
  status: "pending" | "applied" | "failed" | "reverted";

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
 * Name of the collection used to track migration state (deprecated, use history)
 * @deprecated Use history.ts instead
 */
export const MIGRATION_STATE_COLLECTION = "mongodbee_state";

/**
 * Converts a migration operation to a state record
 */
function operationToStateRecord(
  migrationId: string,
  migrationName: string,
  operations: MigrationOperation[],
): MigrationStateRecord {
  const status = calculateMigrationState(operations);
  const lastOp = operations[operations.length - 1];

  // Find last applied operation
  const lastApplied = operations
    .filter((op) => op.operation === "applied" && op.status === "success")
    .pop();

  // Find last reverted operation
  const lastReverted = operations
    .filter((op) => op.operation === "reverted" && op.status === "success")
    .pop();

  // Find last failed operation
  const lastFailed = operations
    .filter((op) => op.operation === "failed")
    .pop();

  return {
    id: migrationId,
    name: migrationName,
    status,
    appliedAt: lastApplied?.executedAt,
    revertedAt: lastReverted?.executedAt,
    duration: lastOp?.duration,
    error: lastFailed?.error,
  };
}

/**
 * Gets the current state of all migrations
 */
export async function getAllMigrationStates(
  db: Db,
): Promise<MigrationStateRecord[]> {
  const states = await getCurrentState(db);
  const records: MigrationStateRecord[] = [];

  for (const [migrationId, state] of states.entries()) {
    const history = await getMigrationHistory(db, migrationId);
    const record = operationToStateRecord(
      migrationId,
      state.lastOperation?.migrationName || migrationId,
      history,
    );
    records.push(record);
  }

  // Sort by last operation date
  return records.sort((a, b) => {
    const dateA = a.appliedAt || a.revertedAt || new Date(0);
    const dateB = b.appliedAt || b.revertedAt || new Date(0);
    return dateA.getTime() - dateB.getTime();
  });
}

/**
 * Gets the state of a specific migration
 */
export async function getMigrationState(
  db: Db,
  migrationId: string,
): Promise<MigrationStateRecord | null> {
  const history = await getMigrationHistory(db, migrationId);

  if (history.length === 0) {
    return null;
  }

  const migrationName = history[0].migrationName;
  return operationToStateRecord(migrationId, migrationName, history);
}

/**
 * Checks if a migration has been applied
 */
export async function isMigrationApplied(
  db: Db,
  migrationId: string,
): Promise<boolean> {
  const state = await getMigrationState(db, migrationId);
  return state?.status === "applied";
}

/**
 * Marks a migration as applied
 */
export async function markMigrationAsApplied(
  db: Db,
  migrationId: string,
  name: string,
  duration?: number,
): Promise<void> {
  await recordOperation(db, migrationId, name, "applied", duration);
}

/**
 * Marks a migration as failed
 */
export async function markMigrationAsFailed(
  db: Db,
  migrationId: string,
  name: string,
  error: string,
): Promise<void> {
  await recordOperation(db, migrationId, name, "failed", undefined, error);
}

/**
 * Marks a migration as reverted
 */
export async function markMigrationAsReverted(
  db: Db,
  migrationId: string,
  name?: string,
  duration?: number,
): Promise<void> {
  // Get name from last operation if not provided
  if (!name) {
    const lastOp = await getLastOperation(db, migrationId);
    name = lastOp?.migrationName || migrationId;
  }

  await recordOperation(db, migrationId, name, "reverted", duration);
}

/**
 * Gets a list of applied migration IDs in order
 */
export async function getAppliedMigrationIds(db: Db): Promise<string[]> {
  return await getAppliedIdsFromHistory(db);
}

/**
 * Gets the last applied migration
 */
export async function getLastAppliedMigration(
  db: Db,
): Promise<MigrationStateRecord | null> {
  const lastOp = await getLastAppliedFromHistory(db);

  if (!lastOp) {
    return null;
  }

  const history = await getMigrationHistory(db, lastOp.migrationId);
  return operationToStateRecord(
    lastOp.migrationId,
    lastOp.migrationName,
    history,
  );
}

/**
 * Clears all migration state (dangerous, use with caution)
 */
export async function clearMigrationState(db: Db): Promise<void> {
  await clearAllOperations(db);
}

// Re-export multi-collection functions for convenience
export { createMultiCollectionInfo } from "./multicollection-registry.ts";

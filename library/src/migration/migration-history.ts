/**
 * @fileoverview Generic event sourcing utilities for migration history
 *
 * This module provides reusable functions for calculating migration states
 * from event histories. Works for both global migrations and multi-model instances.
 *
 * @module
 */

/**
 * Base operation interface that all migration operations must implement
 */
export interface BaseMigrationOperation {
  /** ID of the migration */
  id: string;
  /** Type of operation performed */
  operation: "applied" | "reverted" | "failed";
  /** Status of the operation */
  status: "success" | "failure";
  /** When the operation was executed */
  appliedAt: Date;
}

/**
 * Calculates the current state of a migration from its operation history
 *
 * This implements the core event sourcing logic:
 * - Look at all operations
 * - Find the last successful one
 * - Return its operation type as the current state
 *
 * @param operations - Array of operations for a migration (ordered by time)
 * @returns Current status of the migration
 *
 * @example
 * ```typescript
 * const ops = [
 *   { operation: "applied", status: "success", ... },
 *   { operation: "reverted", status: "success", ... },
 * ];
 * const state = calculateMigrationStateFromHistory(ops); // "reverted"
 * ```
 */
export function calculateMigrationStateFromHistory<
  T extends BaseMigrationOperation,
>(
  operations: T[],
): "pending" | "applied" | "failed" | "reverted" {
  if (operations.length === 0) {
    return "pending";
  }

  // Get last successful operation
  const lastSuccessful = operations
    .filter((op) => op.status === "success")
    .pop();

  if (!lastSuccessful) {
    // All operations failed
    return "failed";
  }

  // Return state based on last successful operation
  switch (lastSuccessful.operation) {
    case "applied":
      return "applied";
    case "reverted":
      return "reverted";
    case "failed":
      return "failed";
    default:
      return "pending";
  }
}

/**
 * Groups operations by migration ID and calculates current state for each
 *
 * This is useful when you have a flat list of operations and want to
 * know the current state of each unique migration.
 *
 * @param operations - Flat array of all operations
 * @returns Map of migration ID to its current state and last operation
 *
 * @example
 * ```typescript
 * const ops = [
 *   { id: "migration-1", operation: "applied", status: "success", ... },
 *   { id: "migration-1", operation: "reverted", status: "success", ... },
 *   { id: "migration-2", operation: "applied", status: "success", ... },
 * ];
 * 
 * const states = groupOperationsByMigrationId(ops);
 * // Map {
 * //   "migration-1" => { status: "reverted", lastOperation: {...} },
 * //   "migration-2" => { status: "applied", lastOperation: {...} }
 * // }
 * ```
 */
export function groupOperationsByMigrationId<T extends BaseMigrationOperation>(
  operations: T[],
): Map<
  string,
  {
    status: "pending" | "applied" | "failed" | "reverted";
    lastOperation: T;
  }
> {
  // Group operations by migration ID
  const byMigration = new Map<string, T[]>();

  for (const op of operations) {
    const existing = byMigration.get(op.id) || [];
    existing.push(op);
    byMigration.set(op.id, existing);
  }

  // Calculate state for each migration
  const states = new Map<
    string,
    {
      status: "pending" | "applied" | "failed" | "reverted";
      lastOperation: T;
    }
  >();

  for (const [migrationId, migrationOps] of byMigration.entries()) {
    const status = calculateMigrationStateFromHistory(migrationOps);
    const lastOp = migrationOps[migrationOps.length - 1];

    states.set(migrationId, {
      status,
      lastOperation: lastOp,
    });
  }

  return states;
}

/**
 * Gets IDs of migrations that are currently in "applied" state
 *
 * This filters out migrations that are pending, failed, or reverted.
 *
 * @param operations - Array of all operations
 * @returns Array of migration IDs with "applied" status
 *
 * @example
 * ```typescript
 * const ops = [
 *   { id: "migration-1", operation: "applied", status: "success", ... },
 *   { id: "migration-2", operation: "applied", status: "success", ... },
 *   { id: "migration-2", operation: "reverted", status: "success", ... },
 * ];
 * 
 * const applied = getAppliedMigrationIdsFromHistory(ops);
 * // ["migration-1"]  // migration-2 is excluded because it was reverted
 * ```
 */
export function getAppliedMigrationIdsFromHistory<
  T extends BaseMigrationOperation,
>(
  operations: T[],
): string[] {
  const states = groupOperationsByMigrationId(operations);
  const applied: string[] = [];

  for (const [migrationId, state] of states.entries()) {
    if (state.status === "applied") {
      applied.push(migrationId);
    }
  }

  return applied;
}

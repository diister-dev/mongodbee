/**
 * @fileoverview Utilities for working with migration IDs
 *
 * Migration IDs follow the format: YYYY_MM_DD_HHMM_ULID@name
 * This module provides utilities to extract and compare migration timestamps.
 *
 * @module
 */

/**
 * Extracts the timestamp prefix from a migration ID
 *
 * Migration IDs have the format: YYYY_MM_DD_HHMM_ULID@name
 * This function extracts the YYYY_MM_DD_HHMM_ULID part (before the @)
 *
 * @param migrationId - The migration ID to extract timestamp from
 * @returns The timestamp prefix (YYYY_MM_DD_HHMM_ULID)
 *
 * @example
 * ```typescript
 * const timestamp = extractMigrationTimestamp("2025_10_09_1445_4G3198R0CE@init");
 * console.log(timestamp); // "2025_10_09_1445_4G3198R0CE"
 * ```
 */
export function extractMigrationTimestamp(migrationId: string): string {
  const parts = migrationId.split("@")[0];
  return parts || migrationId;
}

/**
 * Compares two migration IDs by their timestamps
 *
 * @param migrationIdA - First migration ID
 * @param migrationIdB - Second migration ID
 * @returns -1 if A is before B, 0 if equal, 1 if A is after B
 *
 * @example
 * ```typescript
 * const result = compareMigrationTimestamps(
 *   "2025_10_09_1445_4G3198R0CE@init",
 *   "2025_10_09_1548_4KNGS9Z0X5@migration-2"
 * );
 * console.log(result); // -1 (init is before migration-2)
 * ```
 */
export function compareMigrationTimestamps(
  migrationIdA: string,
  migrationIdB: string,
): -1 | 0 | 1 {
  const timestampA = extractMigrationTimestamp(migrationIdA);
  const timestampB = extractMigrationTimestamp(migrationIdB);

  if (timestampA < timestampB) return -1;
  if (timestampA > timestampB) return 1;
  return 0;
}

/**
 * Checks if migration A was created before migration B
 *
 * @param migrationIdA - First migration ID
 * @param migrationIdB - Second migration ID
 * @returns True if A is before B
 *
 * @example
 * ```typescript
 * const isBefore = isMigrationBefore(
 *   "2025_10_09_1445_4G3198R0CE@init",
 *   "2025_10_09_1548_4KNGS9Z0X5@migration-2"
 * );
 * console.log(isBefore); // true
 * ```
 */
export function isMigrationBefore(
  migrationIdA: string,
  migrationIdB: string,
): boolean {
  return compareMigrationTimestamps(migrationIdA, migrationIdB) === -1;
}

/**
 * Checks if migration A was created after migration B
 *
 * @param migrationIdA - First migration ID
 * @param migrationIdB - Second migration ID
 * @returns True if A is after B
 *
 * @example
 * ```typescript
 * const isAfter = isMigrationAfter(
 *   "2025_10_09_1548_4KNGS9Z0X5@migration-2",
 *   "2025_10_09_1445_4G3198R0CE@init"
 * );
 * console.log(isAfter); // true
 * ```
 */
export function isMigrationAfter(
  migrationIdA: string,
  migrationIdB: string,
): boolean {
  return compareMigrationTimestamps(migrationIdA, migrationIdB) === 1;
}

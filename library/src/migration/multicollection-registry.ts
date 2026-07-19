/**
 * @fileoverview Multi-collection instance registry and discovery
 *
 * This module handles tracking and discovering instances of multi-collections.
 * Each multi-collection instance stores metadata in special documents with _type="_information".
 *
 * @module
 */

import type { Db, MongoClient } from "../mongodb.ts";
import { getCurrentVersion } from "./utils/package-info.ts";
import * as v from "valibot";
import {
  calculateMigrationStateFromHistory,
  getAppliedMigrationIdsFromHistory,
  groupOperationsByMigrationId,
} from "./migration-history.ts";
import { isMigrationAncestor } from "./definition.ts";
import type { MigrationDefinition } from "./types.ts";
import { getSessionContext } from "../session.ts";
import { createLogger } from "../utils/logger.ts";

const log = createLogger("registry");

/**
 * Helper to get session from database client
 * @internal
 */
function getSessionFromDb(db: Db) {
  const client = (db as unknown as { client: MongoClient }).client;
  if (!client) return undefined;
  const { getSession } = getSessionContext(client);
  return getSession();
}

/**
 * Special document types reserved for multi-collection metadata
 */
export const MULTI_COLLECTION_INFO_TYPE = "_information";
export const MULTI_COLLECTION_MIGRATIONS_TYPE = "_migrations";

const metadataSchema: readonly [
  v.ObjectSchema<{
    readonly _id: v.LiteralSchema<"_information", undefined>;
    readonly _type: v.LiteralSchema<"_information", undefined>;
    readonly collectionType: v.StringSchema<undefined>;
    readonly createdAt: v.DateSchema<undefined>;
  }, undefined>,
  v.ObjectSchema<{
    readonly _id: v.LiteralSchema<"_migrations", undefined>;
    readonly _type: v.LiteralSchema<"_migrations", undefined>;
    readonly fromMigrationId: v.StringSchema<undefined>;
    readonly mongodbeeVersion: v.StringSchema<undefined>;
    readonly appliedMigrations: v.ArraySchema<
      v.ObjectSchema<{
        readonly id: v.StringSchema<undefined>;
        readonly operation: v.UnionSchema<[
          v.LiteralSchema<"applied", undefined>,
          v.LiteralSchema<"reverted", undefined>,
          v.LiteralSchema<"failed", undefined>,
        ], undefined>;
        readonly appliedAt: v.DateSchema<undefined>;
        readonly duration: v.OptionalSchema<
          v.NumberSchema<undefined>,
          undefined
        >;
        readonly error: v.OptionalSchema<v.StringSchema<undefined>, undefined>;
        readonly status: v.UnionSchema<[
          v.LiteralSchema<"success", undefined>,
          v.LiteralSchema<"failure", undefined>,
        ], undefined>;
        readonly mongodbeeVersion: v.StringSchema<undefined>;
      }, undefined>,
      undefined
    >;
  }, undefined>,
] = [
  v.object({
    _id: v.literal(MULTI_COLLECTION_INFO_TYPE),
    _type: v.literal(MULTI_COLLECTION_INFO_TYPE),
    collectionType: v.string(),
    createdAt: v.date(),
  }),
  v.object({
    _id: v.literal(MULTI_COLLECTION_MIGRATIONS_TYPE),
    _type: v.literal(MULTI_COLLECTION_MIGRATIONS_TYPE),
    fromMigrationId: v.string(),
    mongodbeeVersion: v.string(),
    appliedMigrations: v.array(v.object({
      id: v.string(),
      operation: v.union([
        v.literal("applied"),
        v.literal("reverted"),
        v.literal("failed"),
      ]),
      appliedAt: v.date(),
      duration: v.optional(v.number()),
      error: v.optional(v.string()),
      status: v.union([
        v.literal("success"),
        v.literal("failure"),
      ]),
      mongodbeeVersion: v.string(),
    })),
  }),
] as const;

/**
 * Creates valibot schemas for multi-collection metadata documents
 * These schemas are used for validator creation in MongoDB
 *
 * @returns Array of valibot object schemas for metadata documents
 */
export function createMetadataSchemas(): typeof metadataSchema {
  return [
    v.object({
      _id: v.literal(MULTI_COLLECTION_INFO_TYPE),
      _type: v.literal(MULTI_COLLECTION_INFO_TYPE),
      collectionType: v.string(),
      createdAt: v.date(),
    }),
    v.object({
      _id: v.literal(MULTI_COLLECTION_MIGRATIONS_TYPE),
      _type: v.literal(MULTI_COLLECTION_MIGRATIONS_TYPE),
      fromMigrationId: v.string(),
      mongodbeeVersion: v.string(),
      appliedMigrations: v.array(v.object({
        id: v.string(),
        operation: v.union([
          v.literal("applied"),
          v.literal("reverted"),
          v.literal("failed"),
        ]),
        appliedAt: v.date(),
        duration: v.optional(v.number()),
        error: v.optional(v.string()),
        status: v.union([
          v.literal("success"),
          v.literal("failure"),
        ]),
        mongodbeeVersion: v.string(),
      })),
    }),
  ];
}

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
 * Type of migration operation for multi-collection instances
 */
export type MultiModelMigrationOperationType =
  | "applied"
  | "reverted"
  | "failed";

/**
 * Status of operation execution for multi-collection instances
 */
export type MultiModelOperationStatus = "success" | "failure";

/**
 * Record of a migration operation on a multi-collection instance
 */
export type MultiModelMigrationOperation = {
  /** ID of the migration */
  id: string;

  /** Type of operation performed */
  operation: MultiModelMigrationOperationType;

  /** When the operation was executed */
  appliedAt: Date;

  /** Duration of operation in milliseconds */
  duration?: number;

  /** Error message if operation failed */
  error?: string;

  /** Status of the operation */
  status: MultiModelOperationStatus;

  /** Version of MongoDBee that executed this operation */
  mongodbeeVersion: string;
};

/**
 * Migrations document stored in each multi-collection instance
 */
export type MultiCollectionMigrations = {
  _id: typeof MULTI_COLLECTION_MIGRATIONS_TYPE;
  _type: typeof MULTI_COLLECTION_MIGRATIONS_TYPE;
  fromMigrationId: string;
  mongodbeeVersion: string;
  appliedMigrations: MultiModelMigrationOperation[];
};

/**
 * How {@link discoverMultiCollectionInstances} treats a collection whose NAME
 * matches the `<model>:` instance convention but which carries NO valid
 * `_information` marker while still holding real documents — i.e. a collection
 * we cannot positively identify as an instance of this model.
 *
 * - `"throw"` (default): fail LOUD. Collect every such collection and throw a
 *   single error listing them. Destructive consumers (flow / drop / validator
 *   `collMod`) must never silently act on a collection we can't verify — this
 *   halts them before any data is flowed or dropped and tells the operator how
 *   to proceed.
 * - `"skip"`: exclude them from the result without throwing. For read-only
 *   listing / detection paths that must not crash — they run no destructive op
 *   on the result, so an unidentifiable collection is simply left unlisted.
 * - `"include"`: legacy name-only behaviour — treat them as instances. Unsafe
 *   to feed into a destructive op; kept only for callers that explicitly want
 *   name-convention discovery regardless of metadata.
 */
export type UnverifiedPrefixMatchMode = "throw" | "skip" | "include";

/**
 * Options for {@link discoverMultiCollectionInstances}.
 */
export interface DiscoverInstancesOptions {
  /**
   * What to do with a `<model>:` prefix-named collection that has no valid
   * `_information` marker but does contain data. Defaults to `"throw"`.
   */
  onUnverifiedPrefixMatch?: UnverifiedPrefixMatchMode;
}

/**
 * Discovers all instances of a specific multi-collection type
 *
 * An instance is recognised by its `_information` marker document
 * (`_type === "_information"`, `collectionType === <model>`). Collections whose
 * NAME follows the `<model>:<id>` convention but carry no such marker are
 * ambiguous: they may be a real instance with corrupt/missing metadata, OR an
 * unrelated collection that merely matches the naming convention. Because
 * callers feed this list into destructive operations (flow-to-scope `consume`
 * drops each instance; validator sync runs `collMod` over each), treating a
 * name-only match as an instance would widen the blast radius to unrelated
 * data. Rather than silently skip such a collection (hiding data) OR silently
 * treat it as an instance (risking a destructive op on unrelated data), the
 * default is to fail LOUD via {@link DiscoverInstancesOptions.onUnverifiedPrefixMatch}.
 *
 * Empty prefix-named collections carry no data at risk (a freshly-created or
 * about-to-be-adopted instance, or a stray empty collection) and are always
 * skipped silently so legitimate adoption / validator-sync paths keep working.
 *
 * @param db - Database instance
 * @param collectionType - The type/model of multi-collection to discover
 * @param options - Discovery options (see {@link DiscoverInstancesOptions})
 * @returns Array of collection names
 * @throws If `onUnverifiedPrefixMatch` is `"throw"` (the default) and one or
 *   more non-empty prefix-named collections lack a valid `_information` marker.
 */
export async function discoverMultiCollectionInstances(
  db: Db,
  collectionType: string,
  options: DiscoverInstancesOptions = {},
): Promise<string[]> {
  const onUnverified = options.onUnverifiedPrefixMatch ?? "throw";
  const session = getSessionFromDb(db);

  // List all collections in the database
  // Note: listCollections cannot run in a transaction, so we don't pass session here
  const collections = await db.listCollections().toArray();
  const instances = new Set<string>();

  // Prefix-named collections that hold data but expose no valid `_information`
  // marker — collected so we can report ALL of them in one loud error instead
  // of blindly (and destructively) treating them as instances.
  const unverified: string[] = [];

  // Instances are named `<model>:<id>` by convention, but the NAME alone is not
  // proof — the authoritative signal is the `_information` marker document.
  const namePrefix = `${collectionType}:`;

  for (const collInfo of collections) {
    const collName = collInfo.name;

    // Skip system collections
    if (collName.startsWith("system.") || collName.startsWith("mongodbee_")) {
      continue;
    }

    const isPrefixMatch = collName.startsWith(namePrefix);

    // Read the `_information` marker. This is authoritative: a matching marker
    // makes the collection an instance regardless of its name; a marker for a
    // different type rules it out.
    let info: MultiCollectionInfo | null = null;
    try {
      info = await db.collection(collName).findOne({
        _type: MULTI_COLLECTION_INFO_TYPE,
      }, { session }) as MultiCollectionInfo | null;
    } catch (_error) {
      // Unreadable collection. For a non-prefix collection it is simply not one
      // of ours; for a prefix match we can't prove it safe, so fall through to
      // the suspicious-handling below.
      if (!isPrefixMatch) continue;
    }

    // Verified instance: marker names this exact model type.
    if (info && info.collectionType === collectionType) {
      instances.add(collName);
      continue;
    }

    // Marker present but for a DIFFERENT model — belongs to another type, never
    // ours (whether or not the name matches our prefix).
    if (
      info && typeof info.collectionType === "string" &&
      info.collectionType.length > 0
    ) {
      continue;
    }

    // No matching marker and no prefix match → not an instance of this model.
    if (!isPrefixMatch) continue;

    // Prefix match but NO valid marker. An EMPTY collection carries no data at
    // risk (freshly-created / soon-to-be-adopted instance, or a stray empty
    // collection), so skip it silently to keep adoption / validator-sync paths
    // working. Only a NON-EMPTY unidentifiable collection is suspicious — a
    // destructive consumer would otherwise flow or drop its real data.
    let hasData = false;
    try {
      const anyDoc = await db.collection(collName).findOne({}, {
        projection: { _id: 1 },
        session,
      });
      hasData = anyDoc !== null;
    } catch (_error) {
      // Can't prove it empty → treat as data-bearing (suspicious).
      hasData = true;
    }
    if (!hasData) continue;

    if (onUnverified === "include") {
      instances.add(collName);
    } else if (onUnverified === "throw") {
      unverified.push(collName);
    }
    // "skip": excluded from `instances`, no throw.
  }

  if (onUnverified === "throw" && unverified.length > 0) {
    unverified.sort((a, b) => a.localeCompare(b));
    throw new Error(
      `discoverMultiCollectionInstances("${collectionType}"): ` +
        `${unverified.length} collection(s) match the "${namePrefix}*" ` +
        `instance naming convention but have no valid ` +
        `"${MULTI_COLLECTION_INFO_TYPE}" marker and contain data: ` +
        `${unverified.join(", ")}. Refusing to treat them as instances — a ` +
        `destructive migration step (flow-to-scope consume, drop, or ` +
        `validator sync) could otherwise flow or drop unrelated data. To ` +
        `proceed, either register/repair each collection as an instance (see ` +
        `markAsMultiCollection) or rename/remove it so it no longer matches ` +
        `the "${namePrefix}*" convention.`,
    );
  }

  return [...instances].sort((a, b) => a.localeCompare(b));
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
  collectionName: string,
): Promise<MultiCollectionInfo | null> {
  const session = getSessionFromDb(db);
  const collection = db.collection(collectionName);

  return await collection.findOne({
    _type: MULTI_COLLECTION_INFO_TYPE,
  }, { session }) as MultiCollectionInfo | null;
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
  migrationId: string = "unknown",
): Promise<void> {
  log.debug(
    `createMultiCollectionInfo(${collectionName}, type=${collectionType}, migration=${migrationId})`,
  );
  const session = getSessionFromDb(db);
  const collection = db.collection(collectionName);
  const mongodbeeVersion = getCurrentVersion();

  const info: MultiCollectionInfo = {
    _id: MULTI_COLLECTION_INFO_TYPE,
    _type: MULTI_COLLECTION_INFO_TYPE,
    collectionType,
    createdAt: new Date(),
  };

  log.debug(
    `createMultiCollectionInfo(${collectionName}): insertOne _information`,
  );
  await collection.insertOne(info as Record<string, unknown>, { session });

  // Also create the migrations tracking document with initial migration
  const initialOperation: MultiModelMigrationOperation = {
    id: migrationId,
    operation: "applied",
    appliedAt: new Date(),
    status: "success",
    mongodbeeVersion,
  };

  const migrations: MultiCollectionMigrations = {
    _id: MULTI_COLLECTION_MIGRATIONS_TYPE,
    _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
    fromMigrationId: migrationId,
    mongodbeeVersion,
    appliedMigrations: [initialOperation],
  };

  log.debug(
    `createMultiCollectionInfo(${collectionName}): insertOne _migrations`,
  );
  await collection.insertOne(migrations as Record<string, unknown>, {
    session,
  });
  log.debug(`createMultiCollectionInfo(${collectionName}): done`);
}

/**
 * Records a migration operation for a multi-collection instance
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @param migrationId - ID of the migration
 * @param operation - Type of operation (applied, reverted, failed)
 * @param duration - Duration in milliseconds
 * @param error - Error message if operation failed
 */
export async function recordMultiCollectionMigration(
  db: Db,
  collectionName: string,
  migrationId: string,
  operation: MultiModelMigrationOperationType = "applied",
  duration?: number,
  error?: string,
): Promise<void> {
  const session = getSessionFromDb(db);
  const collection = db.collection(collectionName);
  const mongodbeeVersion = getCurrentVersion();

  // Build record with only defined fields to avoid null values in MongoDB
  const record: Record<string, unknown> = {
    id: migrationId,
    operation,
    appliedAt: new Date(),
    status: error ? "failure" : "success",
    mongodbeeVersion,
  };

  // Only add optional fields if they have values
  if (duration !== undefined) {
    record.duration = duration;
  }
  if (error !== undefined) {
    record.error = error;
  }

  await collection.updateOne(
    { _type: MULTI_COLLECTION_MIGRATIONS_TYPE } as Record<string, unknown>,
    {
      $push: {
        appliedMigrations: record,
      },
    } as Record<string, unknown>,
    { session },
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
  collectionName: string,
): Promise<MultiCollectionMigrations | null> {
  const session = getSessionFromDb(db);
  const collection = db.collection(collectionName);

  return await collection.findOne({
    _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
  }, { session }) as MultiCollectionMigrations | null;
}

/**
 * Calculates the current state of a migration for a multi-collection instance
 *
 * Uses the generic event sourcing logic from migration-history.ts
 *
 * @param operations - Array of operations for a migration
 * @returns Current status ('pending', 'applied', 'failed', 'reverted')
 */
export function calculateMultiModelMigrationState(
  operations: MultiModelMigrationOperation[],
): "pending" | "applied" | "failed" | "reverted" {
  // MultiModelMigrationOperation already matches BaseMigrationOperation interface
  return calculateMigrationStateFromHistory(operations);
}

/**
 * Gets the history of operations for a specific migration on a multi-collection instance
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @param migrationId - ID of the migration
 * @returns Array of operations for that migration
 */
export async function getMultiModelMigrationHistory(
  db: Db,
  collectionName: string,
  migrationId: string,
): Promise<MultiModelMigrationOperation[]> {
  const migrations = await getMultiCollectionMigrations(db, collectionName);

  if (!migrations) {
    return [];
  }

  return migrations.appliedMigrations.filter((op) => op.id === migrationId);
}

/**
 * Gets the current state of all migrations for a multi-collection instance
 *
 * Uses the generic event sourcing logic from migration-history.ts
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @returns Map of migration ID to current state
 */
export async function getMultiModelCurrentState(
  db: Db,
  collectionName: string,
): Promise<
  Map<string, {
    status: "pending" | "applied" | "failed" | "reverted";
    lastOperation?: MultiModelMigrationOperation;
  }>
> {
  const migrations = await getMultiCollectionMigrations(db, collectionName);

  if (!migrations) {
    return new Map();
  }

  // Use generic grouping function
  return groupOperationsByMigrationId(migrations.appliedMigrations);
}

/**
 * Gets IDs of migrations that are currently applied to a multi-collection instance
 *
 * Uses the generic filtering logic from migration-history.ts
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @returns Array of migration IDs that are in 'applied' state
 */
export async function getMultiModelAppliedMigrationIds(
  db: Db,
  collectionName: string,
): Promise<string[]> {
  const migrations = await getMultiCollectionMigrations(db, collectionName);

  if (!migrations) {
    return [];
  }

  // Use generic function to get applied IDs
  return getAppliedMigrationIdsFromHistory(migrations.appliedMigrations);
}

/**
 * Marks a migration as reverted for a multi-collection instance
 * This is used during rollbacks to record that a migration was undone
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @param migrationId - ID of the migration to mark as reverted
 * @param duration - Duration in milliseconds
 */
export async function markMultiModelMigrationAsReverted(
  db: Db,
  collectionName: string,
  migrationId: string,
  duration?: number,
): Promise<void> {
  await recordMultiCollectionMigration(
    db,
    collectionName,
    migrationId,
    "reverted",
    duration,
  );
}

/**
 * Marks a migration as failed for a multi-collection instance
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @param migrationId - ID of the migration
 * @param error - Error message
 */
export async function markMultiModelMigrationAsFailed(
  db: Db,
  collectionName: string,
  migrationId: string,
  error: string,
): Promise<void> {
  await recordMultiCollectionMigration(
    db,
    collectionName,
    migrationId,
    "failed",
    undefined,
    error,
  );
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
  collectionName: string,
): Promise<boolean> {
  log.debug(`multiCollectionInstanceExists(${collectionName})`);
  try {
    const session = getSessionFromDb(db);
    const collection = db.collection(collectionName);
    const info = await collection.findOne({
      _type: MULTI_COLLECTION_INFO_TYPE,
    }, { session }) as MultiCollectionInfo | null;
    log.debug(
      `multiCollectionInstanceExists(${collectionName}): ${info !== null}`,
    );
    return info !== null;
  } catch (error) {
    log.warn(
      `multiCollectionInstanceExists(${collectionName}) threw, treating as false:`,
      error,
    );
    return false;
  }
}

/**
 * Checks if an instance should receive a specific migration based on the migration chain
 *
 * An instance created at migration A should receive migration B if:
 * - B is A itself, OR
 * - A is an ancestor of B (A comes before B in the chain)
 *
 * In other words: instances receive migrations that are the same or come AFTER their creation.
 *
 * This uses the actual parent-child relationships in the migration chain,
 * not just timestamp comparison.
 *
 * @param instanceCreationMigration - The migration when the instance was created
 * @param candidateMigration - The migration to check
 * @returns True if the instance should receive this migration
 *
 * @example
 * ```typescript
 * const shouldReceive = shouldInstanceReceiveMigrationByChain(
 *   migration2, // instance created at migration-2
 *   migration4  // checking migration-4
 * );
 * // Returns true because migration-2 is an ancestor of migration-4
 * ```
 */
export function shouldInstanceReceiveMigrationByChain(
  instanceCreationMigration: MigrationDefinition,
  candidateMigration: MigrationDefinition,
): boolean {
  // Instance should receive the migration if:
  // 1. It's the same migration (created AT this migration)
  // 2. The instance was created BEFORE the candidate (instance creation is ancestor of candidate)
  return (
    instanceCreationMigration.id === candidateMigration.id ||
    isMigrationAncestor(instanceCreationMigration, candidateMigration)
  );
}

/**
 * Checks if an instance was created after a specific migration
 *
 * @deprecated Use shouldInstanceReceiveMigrationByChain instead for proper chain-based comparison
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
  currentMigrationId: string,
): boolean {
  // Handle special cases
  if (
    instanceCreatedAtMigrationId === "unknown" ||
    instanceCreatedAtMigrationId === "current"
  ) {
    // Unknown creation = assume old, needs all migrations
    return false;
  }

  if (currentMigrationId === "unknown") {
    return false;
  }

  // Extract timestamp parts (format: YYYY_MM_DD_HHMM_ULID@name)
  const extractTimestamp = (migrationId: string): string => {
    const parts = migrationId.split("@")[0];
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
 * @deprecated Use {@link shouldInstanceReceiveMigrationFromChain} instead. This
 * relies on {@link isInstanceCreatedAfterMigration}, which compares migration
 * IDs lexicographically — unsound when IDs from different generators (legacy
 * padded vs. timestamp+ULID) coexist. The chain-based variant walks the actual
 * parent links and is correct in the general case.
 *
 * @param db - Database instance
 * @param collectionName - Full name of the collection
 * @param migrationId - Migration ID to check
 * @returns True if the instance should receive this migration
 */
export async function shouldInstanceReceiveMigration(
  db: Db,
  collectionName: string,
  migrationId: string,
): Promise<boolean> {
  try {
    const session = getSessionFromDb(db);
    const collection = db.collection(collectionName);
    const migrations = await collection.findOne({
      _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
    }, { session }) as MultiCollectionMigrations | null;

    if (!migrations) {
      // No migrations document, can't receive migration
      return false;
    }

    // Instance should receive migration if it was created before or at this migration
    return !isInstanceCreatedAfterMigration(
      migrations.fromMigrationId,
      migrationId,
    );
  } catch (_error) {
    return false;
  }
}

/**
 * Chain-based variant of {@link shouldInstanceReceiveMigration}.
 *
 * Looks up the instance's `fromMigrationId` in the registry, then walks
 * `currentMigration`'s parent chain. Returns `true` iff the instance was
 * created at or before `currentMigration` (i.e. the id is in current's
 * ancestry).
 *
 * Prefer this over {@link shouldInstanceReceiveMigration} — the latter
 * uses lexicographic ID comparison which is unsound when IDs from
 * different generators (legacy padded vs. timestamp+ULID) coexist.
 *
 * Special cases :
 * - `fromMigrationId === "unknown"` or `"current"` → assume legacy
 *   instance, apply the migration (returns `true`).
 *
 * @param db - Database
 * @param collectionName - Instance collection name
 * @param currentMigration - The migration being applied
 * @returns `true` if the instance should receive the migration
 */
export async function shouldInstanceReceiveMigrationFromChain(
  db: Db,
  collectionName: string,
  currentMigration: MigrationDefinition,
): Promise<boolean> {
  try {
    const session = getSessionFromDb(db);
    const collection = db.collection(collectionName);
    const migrations = await collection.findOne({
      _type: MULTI_COLLECTION_MIGRATIONS_TYPE,
    }, { session }) as MultiCollectionMigrations | null;

    if (!migrations) {
      // No migrations-metadata document on this instance — we can't establish
      // its creation point, so we don't target it. (Matches the historical
      // behaviour of shouldInstanceReceiveMigration.)
      return false;
    }

    const fromId = migrations.fromMigrationId;
    if (fromId === "unknown" || fromId === "current") {
      // Legacy / unmarked instance — apply migration to be safe.
      return true;
    }

    // Walk current migration's chain (current → parent → grand-parent → …).
    // If we find `fromId` anywhere, the instance was created at or before
    // current → should receive it. Otherwise the instance is on a different
    // branch or in the future → skip.
    let m: MigrationDefinition | null = currentMigration;
    while (m !== null) {
      if (m.id === fromId) return true;
      m = m.parent;
    }
    return false;
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
  fromMigrationId?: string,
): Promise<void> {
  const session = getSessionFromDb(db);
  const collection = db.collection(collectionName);

  // Check if already marked
  const existing = await collection.findOne({
    _type: MULTI_COLLECTION_INFO_TYPE,
  }, { session });

  if (existing) {
    throw new Error(
      `Collection ${collectionName} is already marked as a multi-collection instance.`,
    );
  }

  // Get migration ID if not provided
  let migrationId = fromMigrationId;
  if (!migrationId) {
    const { getLastAppliedMigration } = await import("./state.ts");
    const lastMigration = await getLastAppliedMigration(db);
    migrationId = lastMigration?.id || "current";
  }

  // Create the metadata
  await createMultiCollectionInfo(
    db,
    collectionName,
    collectionType,
    migrationId,
  );
}

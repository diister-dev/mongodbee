/**
 * Pre-flight privilege check for migration runs
 *
 * A migration run is not just reads and writes. Around every migration the
 * applier issues DDL: `collMod` to disable and restore validators, `create`
 * for new collections, `createIndexes` / `dropIndexes` to sync indexes,
 * `drop` and `renameCollection` for the operations that need them.
 *
 * The built-in `readWrite` role grants NONE of `collMod` — that action lives
 * in `dbAdmin`. An account that can read and write every collection therefore
 * fails HALF-WAY through a migration: validators already disabled, documents
 * already rewritten, then `not authorized on <db> to execute command
 * { collMod: ... }`.
 *
 * This module asks the server what the current account may do
 * (`connectionStatus` with `showPrivileges: true`) so a run can be refused
 * BEFORE anything is touched. It never needs a privilege of its own: any
 * authenticated connection may run `connectionStatus`.
 *
 * @module
 */

import type { Db } from "../mongodb.ts";

/**
 * Every privilege action a migration run (`migrate`, `rollback`, `sync`) can
 * need on the target database.
 *
 * Kept as a flat list rather than derived per operation: the validator and
 * index synchronization that wraps EVERY migration already needs the DDL
 * actions, and the built-in `readWrite` role already includes every other
 * entry, so deriving a smaller set per migration would never unblock a real
 * account — it would only make the message harder to trust.
 *
 * | Action                   | Used for                                              |
 * |--------------------------|-------------------------------------------------------|
 * | `find`                   | history, registries, documents to transform           |
 * | `insert`                 | history records, seeds, transformed documents         |
 * | `update`                 | bulk rewrites, multi-collection registry updates      |
 * | `remove`                 | consumed sources, deleted types, deleted documents    |
 * | `listCollections`        | collection existence, current validator               |
 * | `listIndexes`            | index diff before creating / dropping                 |
 * | `createCollection`       | `create*` operations (collection created WITH validator)|
 * | `dropCollection`         | drops, deleted instances, rollback of a create        |
 * | `renameCollectionSameDB` | `rename_collection`                                   |
 * | `collMod`                | validators off/on around every migration (`dbAdmin`)  |
 * | `createIndex`            | index synchronization                                 |
 * | `dropIndex`              | index synchronization                                 |
 */
export const MIGRATION_PRIVILEGE_ACTIONS: readonly string[] = [
  "find",
  "insert",
  "update",
  "remove",
  "listCollections",
  "listIndexes",
  "createCollection",
  "dropCollection",
  "renameCollectionSameDB",
  "collMod",
  "createIndex",
  "dropIndex",
];

/**
 * Actions granted by the built-in `dbAdmin` role but NOT by `readWrite`.
 * Used to phrase the "which role is missing" hint.
 */
export const DB_ADMIN_ONLY_ACTIONS: readonly string[] = ["collMod"];

/**
 * One entry of `connectionStatus.authInfo.authenticatedUserPrivileges`
 */
export interface ServerPrivilege {
  resource: {
    /** Database name, or `""` for every database */
    db?: string;
    /** Collection name, or `""` for every (non-system) collection */
    collection?: string;
    cluster?: boolean;
    anyResource?: boolean;
    system_buckets?: string;
  };
  actions: string[];
}

/**
 * The `authInfo` document returned by `connectionStatus`
 */
export interface ConnectionAuthInfo {
  authenticatedUsers: Array<{ user: string; db: string }>;
  authenticatedUserRoles: Array<{ role: string; db: string }>;
  /** Only present when the command was run with `showPrivileges: true` */
  authenticatedUserPrivileges?: ServerPrivilege[];
}

/**
 * Result of matching a list of server privileges against required actions
 */
export interface PrivilegeEvaluation {
  /** Required actions granted on the whole database (or on every database) */
  granted: string[];
  /** Required actions NO database-wide grant covers */
  missing: string[];
  /**
   * For each missing action, the collections on which it IS granted.
   * A collection-scoped grant does not cover a migration — it touches the
   * history collection and every collection of every schema — but naming
   * them explains why an account that "has the right" still gets refused.
   */
  collectionScoped: Record<string, string[]>;
}

/**
 * Outcome of {@link checkMigrationPrivileges}
 */
export type MigrationPrivilegeCheck =
  | {
      /** Every required action is granted database-wide */
      status: "ok";
      database: string;
      users: Array<{ user: string; db: string }>;
      roles: Array<{ role: string; db: string }>;
      required: string[];
    }
  | {
      /** At least one required action is not granted database-wide */
      status: "missing";
      database: string;
      users: Array<{ user: string; db: string }>;
      roles: Array<{ role: string; db: string }>;
      required: string[];
      missing: string[];
      collectionScoped: Record<string, string[]>;
    }
  | {
      /**
       * Nothing could be verified: access control is disabled (no authenticated
       * user, so nothing can be refused either), or the server did not answer
       * `connectionStatus` the way MongoDB does. Never a reason to abort.
       */
      status: "skipped";
      database: string;
      reason: string;
      required: string[];
    };

/**
 * Whether a privilege resource covers every (non-system) collection of `dbName`
 */
function coversWholeDatabase(
  resource: ServerPrivilege["resource"],
  dbName: string,
): boolean {
  if (resource.anyResource === true) return true;
  if (resource.cluster === true) return false;
  if (typeof resource.db !== "string") return false;
  if (resource.db !== "" && resource.db !== dbName) return false;
  return resource.collection === "";
}

/**
 * Collection name when a privilege resource targets ONE collection of `dbName`
 */
function scopedCollection(
  resource: ServerPrivilege["resource"],
  dbName: string,
): string | undefined {
  if (typeof resource.db !== "string") return undefined;
  if (resource.db !== "" && resource.db !== dbName) return undefined;
  const collection = resource.collection;
  if (!collection || collection.startsWith("system.")) return undefined;
  return collection;
}

/**
 * Matches the privileges reported by the server against the actions a
 * migration run needs on `dbName`.
 *
 * Pure: takes the `authenticatedUserPrivileges` array as the server returns
 * it (built-in roles already expanded into actions), returns which required
 * actions are granted on the whole database, which are missing, and — for
 * the missing ones — whether a collection-scoped grant exists.
 *
 * @param privileges - `authInfo.authenticatedUserPrivileges` from `connectionStatus`
 * @param dbName - Database the migration targets
 * @param required - Actions to look for (defaults to {@link MIGRATION_PRIVILEGE_ACTIONS})
 */
export function evaluatePrivileges(
  privileges: readonly ServerPrivilege[],
  dbName: string,
  required: readonly string[] = MIGRATION_PRIVILEGE_ACTIONS,
): PrivilegeEvaluation {
  const databaseWide = new Set<string>();
  const perCollection = new Map<string, Set<string>>();

  for (const privilege of privileges) {
    const actions = Array.isArray(privilege.actions) ? privilege.actions : [];
    if (coversWholeDatabase(privilege.resource, dbName)) {
      for (const action of actions) databaseWide.add(action);
      continue;
    }
    const collection = scopedCollection(privilege.resource, dbName);
    if (collection === undefined) continue;
    for (const action of actions) {
      let collections = perCollection.get(action);
      if (!collections) {
        collections = new Set();
        perCollection.set(action, collections);
      }
      collections.add(collection);
    }
  }

  const granted: string[] = [];
  const missing: string[] = [];
  const collectionScoped: Record<string, string[]> = {};

  for (const action of required) {
    if (databaseWide.has(action)) {
      granted.push(action);
      continue;
    }
    missing.push(action);
    const collections = perCollection.get(action);
    if (collections && collections.size > 0) {
      collectionScoped[action] = [...collections].sort();
    }
  }

  return { granted, missing, collectionScoped };
}

/**
 * Options for {@link checkMigrationPrivileges}
 */
export interface CheckMigrationPrivilegesOptions {
  /** Actions to require (defaults to {@link MIGRATION_PRIVILEGE_ACTIONS}) */
  actions?: readonly string[];
}

/**
 * Asks the server what the connected account may do on `db` and compares it
 * with what a migration run needs.
 *
 * Run it BEFORE `applyMigration`: a missing `collMod` otherwise surfaces
 * after validators were disabled and documents rewritten.
 *
 * Never throws for a server that cannot answer: a proxy or a MongoDB-compatible
 * service that does not implement `connectionStatus`, or a deployment with
 * access control disabled, yields `status: "skipped"` with the reason, and
 * the caller decides whether to proceed.
 *
 * @example
 * ```typescript
 * const check = await checkMigrationPrivileges(db);
 * if (check.status === "missing") {
 *   throw new Error(`Account lacks ${check.missing.join(", ")} on ${check.database}`);
 * }
 * ```
 */
export async function checkMigrationPrivileges(
  db: Db,
  options: CheckMigrationPrivilegesOptions = {},
): Promise<MigrationPrivilegeCheck> {
  const required = [...(options.actions ?? MIGRATION_PRIVILEGE_ACTIONS)];
  const database = db.databaseName;

  let authInfo: ConnectionAuthInfo | undefined;
  try {
    const response = await db.command({
      connectionStatus: 1,
      showPrivileges: true,
    });
    authInfo = response?.authInfo as ConnectionAuthInfo | undefined;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      status: "skipped",
      database,
      required,
      reason: `connectionStatus failed: ${message}`,
    };
  }

  if (!authInfo || !Array.isArray(authInfo.authenticatedUsers)) {
    return {
      status: "skipped",
      database,
      required,
      reason: "server did not return authInfo for connectionStatus",
    };
  }

  if (authInfo.authenticatedUsers.length === 0) {
    return {
      status: "skipped",
      database,
      required,
      reason: "no authenticated user (access control is disabled)",
    };
  }

  const privileges = authInfo.authenticatedUserPrivileges;
  if (!Array.isArray(privileges)) {
    return {
      status: "skipped",
      database,
      required,
      reason: "server did not report the account's privileges",
    };
  }

  const users = authInfo.authenticatedUsers;
  const roles = Array.isArray(authInfo.authenticatedUserRoles)
    ? authInfo.authenticatedUserRoles
    : [];
  const evaluation = evaluatePrivileges(privileges, database, required);

  if (evaluation.missing.length === 0) {
    return { status: "ok", database, users, roles, required };
  }

  return {
    status: "missing",
    database,
    users,
    roles,
    required,
    missing: evaluation.missing,
    collectionScoped: evaluation.collectionScoped,
  };
}

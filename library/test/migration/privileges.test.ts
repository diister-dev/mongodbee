/**
 * Tests for the migration privilege pre-flight
 *
 * The fixtures are `connectionStatus { showPrivileges: true }` answers
 * captured from a MongoDB 8.0 server with access control enabled, for users
 * holding the built-in roles a migration account realistically has.
 */

import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import type { Db } from "../../src/mongodb.ts";
import {
  checkMigrationPrivileges,
  evaluatePrivileges,
  MIGRATION_PRIVILEGE_ACTIONS,
  type ServerPrivilege,
} from "../../src/migration/privileges.ts";
import { withDatabase } from "../+shared.ts";

const READ_WRITE_ACTIONS = [
  "changeStream",
  "collStats",
  "convertToCapped",
  "createCollection",
  "createIndex",
  "createSearchIndexes",
  "dbHash",
  "dbStats",
  "dropCollection",
  "dropIndex",
  "dropSearchIndex",
  "find",
  "insert",
  "killCursors",
  "listCollections",
  "listIndexes",
  "listSearchIndexes",
  "updateSearchIndex",
  "planCacheRead",
  "remove",
  "renameCollectionSameDB",
  "update",
];

const DB_ADMIN_ACTIONS = [
  "bypassDocumentValidation",
  "collMod",
  "collStats",
  "compact",
  "convertToCapped",
  "createCollection",
  "createIndex",
  "dbHash",
  "dbStats",
  "dropCollection",
  "dropDatabase",
  "dropIndex",
  "enableProfiler",
  "listCollections",
  "listIndexes",
  "planCacheIndexFilter",
  "planCacheRead",
  "planCacheWrite",
  "reIndex",
  "renameCollectionSameDB",
  "storageDetails",
  "validate",
];

const READ_ACTIONS = [
  "changeStream",
  "collStats",
  "dbHash",
  "dbStats",
  "find",
  "killCursors",
  "listCollections",
  "listIndexes",
  "planCacheRead",
];

/** `readWrite@app` — the account from the bug report */
const readWriteOnApp: ServerPrivilege[] = [
  { resource: { db: "app", collection: "" }, actions: READ_WRITE_ACTIONS },
  {
    resource: { db: "app", collection: "system.js" },
    actions: ["changeStream", "collStats", "find"],
  },
];

/** `readWrite@app` + `dbAdmin@app` */
const readWriteDbAdminOnApp: ServerPrivilege[] = [
  {
    resource: { db: "app", collection: "" },
    actions: [...new Set([...READ_WRITE_ACTIONS, ...DB_ADMIN_ACTIONS])],
  },
];

/** `readWriteAnyDatabase@admin` — db-less resource, still no collMod */
const readWriteAnyDatabase: ServerPrivilege[] = [
  { resource: { cluster: true }, actions: ["listDatabases"] },
  { resource: { system_buckets: "" }, actions: READ_WRITE_ACTIONS },
  { resource: { db: "", collection: "" }, actions: READ_WRITE_ACTIONS },
];

/** `root@admin` — trimmed to the entries that matter */
const rootUser: ServerPrivilege[] = [
  { resource: { anyResource: true }, actions: ["listCollections", "validate"] },
  { resource: { cluster: true }, actions: ["addShard", "shutdown"] },
  {
    resource: { db: "", collection: "" },
    actions: [...new Set([...READ_WRITE_ACTIONS, ...DB_ADMIN_ACTIONS])],
  },
  { resource: { db: "admin", collection: "system.users" }, actions: ["find"] },
];

/** A custom role granting everything, but on ONE collection of `app` */
const collectionScopedOnUsers: ServerPrivilege[] = [
  {
    resource: { db: "app", collection: "users" },
    actions: [...MIGRATION_PRIVILEGE_ACTIONS],
  },
];

test("evaluatePrivileges - readWrite alone is missing exactly collMod", () => {
  const result = evaluatePrivileges(readWriteOnApp, "app");
  assertEquals(result.missing, ["collMod"]);
  assertEquals(result.collectionScoped, {});
  assertEquals(
    result.granted,
    MIGRATION_PRIVILEGE_ACTIONS.filter((a) => a !== "collMod"),
  );
});

test("evaluatePrivileges - readWrite + dbAdmin covers everything", () => {
  const result = evaluatePrivileges(readWriteDbAdminOnApp, "app");
  assertEquals(result.missing, []);
  assertEquals(result.granted, [...MIGRATION_PRIVILEGE_ACTIONS]);
});

test("evaluatePrivileges - a grant on another database does not count", () => {
  const result = evaluatePrivileges(readWriteDbAdminOnApp, "other");
  assertEquals(result.granted, []);
  assertEquals(result.missing, [...MIGRATION_PRIVILEGE_ACTIONS]);
});

test("evaluatePrivileges - readWriteAnyDatabase matches any db but lacks collMod", () => {
  const result = evaluatePrivileges(readWriteAnyDatabase, "app");
  assertEquals(result.missing, ["collMod"]);
});

test("evaluatePrivileges - root covers everything through the db-less resource", () => {
  const result = evaluatePrivileges(rootUser, "whatever");
  assertEquals(result.missing, []);
});

test("evaluatePrivileges - anyResource covers every action it lists", () => {
  const result = evaluatePrivileges(
    [{ resource: { anyResource: true }, actions: ["collMod", "find"] }],
    "app",
    ["collMod", "find", "insert"],
  );
  assertEquals(result.granted, ["collMod", "find"]);
  assertEquals(result.missing, ["insert"]);
});

test("evaluatePrivileges - cluster resources never satisfy database actions", () => {
  const result = evaluatePrivileges(
    [{ resource: { cluster: true }, actions: ["find", "collMod"] }],
    "app",
    ["find", "collMod"],
  );
  assertEquals(result.missing, ["find", "collMod"]);
});

test("evaluatePrivileges - collection-scoped grants are reported, not counted", () => {
  const result = evaluatePrivileges(collectionScopedOnUsers, "app");
  assertEquals(result.granted, []);
  assertEquals(result.missing, [...MIGRATION_PRIVILEGE_ACTIONS]);
  for (const action of MIGRATION_PRIVILEGE_ACTIONS) {
    assertEquals(result.collectionScoped[action], ["users"]);
  }
});

test("evaluatePrivileges - system collections are not collection-scoped hints", () => {
  const result = evaluatePrivileges(
    [{ resource: { db: "app", collection: "system.js" }, actions: ["find"] }],
    "app",
    ["find"],
  );
  assertEquals(result.missing, ["find"]);
  assertEquals(result.collectionScoped, {});
});

test("evaluatePrivileges - read-only role is missing every write and DDL action", () => {
  const result = evaluatePrivileges(
    [{ resource: { db: "app", collection: "" }, actions: READ_ACTIONS }],
    "app",
  );
  assertEquals(result.granted, ["find", "listCollections", "listIndexes"]);
  assert(result.missing.includes("insert"));
  assert(result.missing.includes("collMod"));
  assert(result.missing.includes("createIndex"));
});

test("evaluatePrivileges - tolerates malformed entries", () => {
  const result = evaluatePrivileges(
    [
      { resource: {}, actions: ["find"] },
      { resource: { db: "app", collection: "" } } as unknown as ServerPrivilege,
      {
        resource: { db: "app", collection: "" },
        actions: ["find", "collMod"],
      },
    ],
    "app",
    ["find", "collMod"],
  );
  assertEquals(result.missing, []);
});

/** A `Db` stand-in whose `command` answers `connectionStatus` with `authInfo` */
function fakeDb(
  databaseName: string,
  answer: () => Promise<unknown>,
): { db: Db; commands: unknown[] } {
  const commands: unknown[] = [];
  const db = {
    databaseName,
    command(cmd: unknown) {
      commands.push(cmd);
      return answer();
    },
  } as unknown as Db;
  return { db, commands };
}

function authInfo(
  users: Array<{ user: string; db: string }>,
  roles: Array<{ role: string; db: string }>,
  privileges?: ServerPrivilege[],
) {
  return {
    ok: 1,
    authInfo: {
      authenticatedUsers: users,
      authenticatedUserRoles: roles,
      ...(privileges ? { authenticatedUserPrivileges: privileges } : {}),
    },
  };
}

test("checkMigrationPrivileges - asks connectionStatus with showPrivileges", async () => {
  const { db, commands } = fakeDb("app", () =>
    Promise.resolve(
      authInfo(
        [{ user: "rw", db: "app" }],
        [
          {
            role: "readWrite",
            db: "app",
          },
        ],
        readWriteOnApp,
      ),
    ),
  );
  await checkMigrationPrivileges(db);
  assertEquals(commands, [{ connectionStatus: 1, showPrivileges: true }]);
});

test("checkMigrationPrivileges - readWrite account is refused for collMod", async () => {
  const { db } = fakeDb("app", () =>
    Promise.resolve(
      authInfo(
        [{ user: "rw", db: "app" }],
        [
          {
            role: "readWrite",
            db: "app",
          },
        ],
        readWriteOnApp,
      ),
    ),
  );
  const check = await checkMigrationPrivileges(db);
  assertEquals(check.status, "missing");
  if (check.status !== "missing") return;
  assertEquals(check.database, "app");
  assertEquals(check.users, [{ user: "rw", db: "app" }]);
  assertEquals(check.roles, [{ role: "readWrite", db: "app" }]);
  assertEquals(check.missing, ["collMod"]);
  assertEquals(check.required, [...MIGRATION_PRIVILEGE_ACTIONS]);
});

test("checkMigrationPrivileges - readWrite + dbAdmin account passes", async () => {
  const { db } = fakeDb("app", () =>
    Promise.resolve(
      authInfo(
        [{ user: "rwadmin", db: "app" }],
        [
          { role: "dbAdmin", db: "app" },
          { role: "readWrite", db: "app" },
        ],
        readWriteDbAdminOnApp,
      ),
    ),
  );
  const check = await checkMigrationPrivileges(db);
  assertEquals(check.status, "ok");
  if (check.status !== "ok") return;
  assertEquals(
    check.roles.map((r) => r.role),
    ["dbAdmin", "readWrite"],
  );
});

test("checkMigrationPrivileges - custom action list is honoured", async () => {
  const { db } = fakeDb("app", () =>
    Promise.resolve(
      authInfo(
        [{ user: "rw", db: "app" }],
        [
          {
            role: "readWrite",
            db: "app",
          },
        ],
        readWriteOnApp,
      ),
    ),
  );
  const check = await checkMigrationPrivileges(db, {
    actions: ["find", "insert"],
  });
  assertEquals(check.status, "ok");
  assertEquals(check.required, ["find", "insert"]);
});

test("checkMigrationPrivileges - no authenticated user is skipped, not refused", async () => {
  const { db } = fakeDb("app", () => Promise.resolve(authInfo([], [], [])));
  const check = await checkMigrationPrivileges(db);
  assertEquals(check.status, "skipped");
  if (check.status !== "skipped") return;
  assert(check.reason.includes("no authenticated user"), check.reason);
});

test("checkMigrationPrivileges - server without privilege report is skipped", async () => {
  const { db } = fakeDb("app", () =>
    Promise.resolve(
      authInfo(
        [{ user: "rw", db: "app" }],
        [
          {
            role: "readWrite",
            db: "app",
          },
        ],
      ),
    ),
  );
  const check = await checkMigrationPrivileges(db);
  assertEquals(check.status, "skipped");
});

test("checkMigrationPrivileges - server without authInfo is skipped", async () => {
  const { db } = fakeDb("app", () => Promise.resolve({ ok: 1 }));
  const check = await checkMigrationPrivileges(db);
  assertEquals(check.status, "skipped");
});

test("checkMigrationPrivileges - connectionStatus failure is skipped with the reason", async () => {
  const { db } = fakeDb("app", () =>
    Promise.reject(new Error("no such command: 'connectionStatus'")),
  );
  const check = await checkMigrationPrivileges(db);
  assertEquals(check.status, "skipped");
  if (check.status !== "skipped") return;
  assert(check.reason.includes("no such command"), check.reason);
});

test("checkMigrationPrivileges - runs against a real server", async () => {
  await withDatabase("privileges", async (db) => {
    const check = await checkMigrationPrivileges(db);
    // The suite's server either has access control disabled (skipped) or
    // runs as an account able to create and drop test databases (ok). It is
    // never a refusal: that would mean the rest of the suite cannot run.
    assert(check.status !== "missing", JSON.stringify(check));
    assertEquals(check.database, db.databaseName);
  });
});

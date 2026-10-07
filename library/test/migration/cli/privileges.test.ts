/**
 * Tests for the CLI privilege pre-flight wrapper
 *
 * Covers how `ensureMigrationPrivileges` reports and reacts, with a `Db`
 * stand-in answering `connectionStatus`.
 */

import { test } from "../../+harness.ts";
import { assert, assertEquals, assertRejects } from "../../+assert.ts";
import { stripAnsiCode } from "../../../src/utils/colors.ts";
import type { Db } from "../../../src/mongodb.ts";
import { ensureMigrationPrivileges } from "../../../src/migration/cli/utils/privileges.ts";
import type { ServerPrivilege } from "../../../src/migration/privileges.ts";

const readWriteOnApp: ServerPrivilege[] = [
  {
    resource: { db: "app", collection: "" },
    actions: [
      "find",
      "insert",
      "update",
      "remove",
      "listCollections",
      "listIndexes",
      "createCollection",
      "dropCollection",
      "renameCollectionSameDB",
      "createIndex",
      "dropIndex",
    ],
  },
];

const withCollMod: ServerPrivilege[] = [
  {
    resource: { db: "app", collection: "" },
    actions: [...readWriteOnApp[0].actions, "collMod"],
  },
];

function fakeDb(answer: () => Promise<unknown>): { db: Db; calls: number } {
  const state = { calls: 0 };
  const db = {
    databaseName: "app",
    command() {
      state.calls++;
      return answer();
    },
  } as unknown as Db;
  return {
    db,
    get calls() {
      return state.calls;
    },
  };
}

function status(
  privileges: ServerPrivilege[],
  roles = [{ role: "readWrite", db: "app" }],
) {
  return {
    ok: 1,
    authInfo: {
      authenticatedUsers: [{ user: "rw", db: "app" }],
      authenticatedUserRoles: roles,
      authenticatedUserPrivileges: privileges,
    },
  };
}

/** Runs `work` with console.log captured; returns the printed lines, colors stripped */
async function captureLog(work: () => Promise<void>): Promise<string[]> {
  const lines: string[] = [];
  const original = console.log;
  console.log = (...args: unknown[]) => {
    lines.push(stripAnsiCode(args.map(String).join(" ")));
  };
  try {
    await work();
  } finally {
    console.log = original;
  }
  return lines;
}

test("ensureMigrationPrivileges - refuses a readWrite-only account", async () => {
  const { db } = fakeDb(() => Promise.resolve(status(readWriteOnApp)));
  const lines = await captureLog(async () => {
    const error = await assertRejects(
      () => ensureMigrationPrivileges(db),
      Error,
    );
    assert(error.message.includes("missing collMod"), error.message);
    assert(error.message.includes('database "app"'), error.message);
    assert(error.message.includes("Grant dbAdmin"), error.message);
  });
  const text = lines.join("\n");
  assert(text.includes("Insufficient privileges"), text);
  assert(text.includes("Account:  rw@app"), text);
  assert(text.includes("Roles:    readWrite@app"), text);
  assert(text.includes("- collMod"), text);
  // The exact grant to run, on the user's auth database
  assert(text.includes("use app"), text);
  assert(
    text.includes(
      'db.grantRolesToUser("rw", [{ role: "dbAdmin", db: "app" }])',
    ),
    text,
  );
  assert(text.includes("--skip-privilege-check"), text);
});

test("ensureMigrationPrivileges - suggests readWrite too when write actions are missing", async () => {
  const readOnly: ServerPrivilege[] = [
    {
      resource: { db: "app", collection: "" },
      actions: ["find", "listCollections", "listIndexes"],
    },
  ];
  const { db } = fakeDb(() =>
    Promise.resolve(status(readOnly, [{ role: "read", db: "app" }])),
  );
  const lines = await captureLog(async () => {
    const error = await assertRejects(
      () => ensureMigrationPrivileges(db),
      Error,
    );
    assert(error.message.includes("Grant readWrite + dbAdmin"), error.message);
  });
  const text = lines.join("\n");
  assert(
    text.includes(
      '[{ role: "readWrite", db: "app" }, { role: "dbAdmin", db: "app" }]',
    ),
    text,
  );
});

test("ensureMigrationPrivileges - names collections a missing action is scoped to", async () => {
  const scoped: ServerPrivilege[] = [
    ...readWriteOnApp,
    { resource: { db: "app", collection: "users" }, actions: ["collMod"] },
    { resource: { db: "app", collection: "posts" }, actions: ["collMod"] },
  ];
  const { db } = fakeDb(() => Promise.resolve(status(scoped)));
  const lines = await captureLog(async () => {
    await assertRejects(() => ensureMigrationPrivileges(db), Error);
  });
  const text = lines.join("\n");
  assert(
    text.includes("- collMod (granted only on collection(s): posts, users)"),
    text,
  );
});

test("ensureMigrationPrivileges - dry run reports but does not abort", async () => {
  const { db } = fakeDb(() => Promise.resolve(status(readWriteOnApp)));
  let check: Awaited<ReturnType<typeof ensureMigrationPrivileges>> | undefined;
  const lines = await captureLog(async () => {
    check = await ensureMigrationPrivileges(db, { dryRun: true });
  });
  assertEquals(check?.status, "missing");
  const text = lines.join("\n");
  assert(text.includes("[DRY RUN]"), text);
});

test("ensureMigrationPrivileges - passes an account with every action", async () => {
  const { db } = fakeDb(() =>
    Promise.resolve(
      status(withCollMod, [
        { role: "dbAdmin", db: "app" },
        { role: "readWrite", db: "app" },
      ]),
    ),
  );
  let check: Awaited<ReturnType<typeof ensureMigrationPrivileges>> | undefined;
  const lines = await captureLog(async () => {
    check = await ensureMigrationPrivileges(db);
  });
  assertEquals(check?.status, "ok");
  const text = lines.join("\n");
  assert(text.includes("Account privileges verified"), text);
  assert(text.includes("rw@app: dbAdmin@app, readWrite@app"), text);
});

test("ensureMigrationPrivileges - skip flag never queries the server", async () => {
  const fake = fakeDb(() => Promise.reject(new Error("must not be called")));
  let check: Awaited<ReturnType<typeof ensureMigrationPrivileges>> | undefined;
  const lines = await captureLog(async () => {
    check = await ensureMigrationPrivileges(fake.db, { skip: true });
  });
  assertEquals(check, undefined);
  assertEquals(fake.calls, 0);
  assert(lines.join("\n").includes("skipped"), lines.join("\n"));
});

test("ensureMigrationPrivileges - unverifiable server warns and proceeds", async () => {
  const { db } = fakeDb(() => Promise.reject(new Error("no such command")));
  let check: Awaited<ReturnType<typeof ensureMigrationPrivileges>> | undefined;
  const lines = await captureLog(async () => {
    check = await ensureMigrationPrivileges(db);
  });
  assertEquals(check?.status, "skipped");
  const text = lines.join("\n");
  assert(text.includes("not verified"), text);
  assert(text.includes("no such command"), text);
});

test("ensureMigrationPrivileges - disabled access control proceeds silently", async () => {
  const { db } = fakeDb(() =>
    Promise.resolve({
      ok: 1,
      authInfo: {
        authenticatedUsers: [],
        authenticatedUserRoles: [],
        authenticatedUserPrivileges: [],
      },
    }),
  );
  let check: Awaited<ReturnType<typeof ensureMigrationPrivileges>> | undefined;
  await captureLog(async () => {
    check = await ensureMigrationPrivileges(db);
  });
  assertEquals(check?.status, "skipped");
});

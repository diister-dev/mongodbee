/**
 * Tests for the baseline command and for `migrate --target`.
 *
 * Both narrow what reaches a real database, so the assertions that matter are
 * the negative ones: that `baseline` writes to the ledger and NOTHING else, and
 * that `--target` genuinely leaves the later migrations unapplied rather than
 * merely reporting that it did.
 *
 * @module
 */

import {
  assert,
  assertEquals,
  assertRejects,
  assertStringIncludes,
} from "../../+assert.ts";
import { test } from "../../+harness.ts";
import process from "node:process";
import { writeFile } from "node:fs/promises";
import { MongoClient } from "../../../src/mongodb.ts";
import { initCommand } from "../../../src/migration/cli/commands/init.ts";
import { generateCommand } from "../../../src/migration/cli/commands/generate.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { baselineCommand } from "../../../src/migration/cli/commands/baseline.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { getMigrationOperationsCollection } from "../../../src/migration/history.ts";
import {
  delay,
  getMigrationPath,
  getMigrationsDir,
  listMigrationFiles,
  readFile,
  withTempDir,
} from "./shared.ts";

const TEST_MONGODB_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";

function generateTestDbName(): string {
  return `mongodbee_test_baseline_${crypto
    .randomUUID()
    .replace(/-/g, "")
    .substring(0, 8)}`;
}

async function withTestDb(
  work: (
    db: ReturnType<MongoClient["db"]>,
    client: MongoClient,
    dbName: string,
  ) => Promise<void>,
) {
  const dbName = generateTestDbName();
  const client = new MongoClient(TEST_MONGODB_URI);

  try {
    await client.connect();
    const db = client.db(dbName);
    await db.dropDatabase();
    await work(db, client, dbName);
  } finally {
    try {
      await client.db(dbName).dropDatabase();
    } catch {
      // Ignore cleanup errors
    }
    await client.close();
  }
}

async function setupTestConfig(tempDir: string, dbName: string) {
  await writeFile(
    `${tempDir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
}

/** Turns the newest generated migration into one that creates `users`. */
async function makeMigrationCreateUsers(tempDir: string) {
  const files = await listMigrationFiles(getMigrationsDir(tempDir));
  const migrationPath = getMigrationPath(tempDir, files[files.length - 1]);
  let content = await readFile(migrationPath);
  assert(content !== null);

  content = `import * as v from "valibot";\n` + content;
  content = content.replace(
    `collections: {`,
    `collections: {
    users: {
      name: v.string(),
    }
  `,
  );
  content = content.replace(
    "migrate(migration) {",
    `migrate(migration) {
    migration.createCollection("users");`,
  );
  await writeFile(migrationPath, content);

  let updatedSchema = await readFile(`${tempDir}/schemas.ts`);
  assert(updatedSchema !== null);
  updatedSchema = `import * as v from "valibot";\n` + updatedSchema;
  updatedSchema = updatedSchema.replace(
    "collections: {",
    `collections: {
      users: {
        name: v.string(),
      },
    `,
  );
  await writeFile(`${tempDir}/schemas.ts`, updatedSchema);
}

test("baseline - records the chain as applied without executing it", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      await generateCommand({ name: "create_users", cwd: tempDir });
      await makeMigrationCreateUsers(tempDir);

      await baselineCommand({ cwd: tempDir, force: true });

      // The whole point: the ledger moved, the database did not.
      assertEquals((await getAppliedMigrationIds(db)).length, 1);
      const collections = (await db.listCollections().toArray()).map(
        (c) => c.name,
      );
      assertEquals(
        collections.includes("users"),
        false,
        "baseline must never run the operations it records",
      );
    });
  });
});

test("baseline - the record says the migration was adopted, not run", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      await generateCommand({ name: "first", cwd: tempDir });

      await baselineCommand({ cwd: tempDir, force: true });

      // The status has to read "applied" for `migrate` to agree there is
      // nothing left, so provenance is the only thing left to tell an operator
      // these operations never ran here.
      const ops = await getMigrationOperationsCollection(db).find({}).toArray();
      assertEquals(ops.length, 1);
      assertEquals(ops[0].operation, "applied");
      assertEquals(ops[0].adopted, true);
    });
  });
});

test("baseline - leaves migrations after the target pending", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "third", cwd: tempDir });

      await baselineCommand({ cwd: tempDir, target: "second", force: true });

      assertEquals((await getAppliedMigrationIds(db)).length, 2);

      // And the tail is genuinely still pending, which is the shape a database
      // adopted mid-chain has to end up in.
      await migrateCommand({ cwd: tempDir, force: true });
      assertEquals((await getAppliedMigrationIds(db)).length, 3);
    });
  });
});

test("baseline - refuses to contradict a later applied migration", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });

      await migrateCommand({ cwd: tempDir, force: true });

      // Claiming the database sits at "first" while the history says "second"
      // ran is a contradiction, and neither side can be assumed correct.
      const error = await assertRejects(
        () => baselineCommand({ cwd: tempDir, target: "first", force: true }),
        Error,
      );
      assertStringIncludes(error.message, "AFTER");
    });
  });
});

test("baseline - is idempotent on an already covered ledger", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      await generateCommand({ name: "first", cwd: tempDir });

      await baselineCommand({ cwd: tempDir, force: true });
      await baselineCommand({ cwd: tempDir, force: true });

      assertEquals((await getAppliedMigrationIds(db)).length, 1);
    });
  });
});

test("migrate --target - applies a prefix and leaves the rest pending", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (db, _client, dbName) => {
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "third", cwd: tempDir });

      await migrateCommand({ cwd: tempDir, force: true, target: "second" });

      const applied = await getAppliedMigrationIds(db);
      assertEquals(applied.length, 2);

      await migrateCommand({ cwd: tempDir, force: true });
      assertEquals((await getAppliedMigrationIds(db)).length, 3);
    });
  });
});

test("migrate --target - says so when the target is already applied", async () => {
  await withTempDir(async (tempDir) => {
    await withTestDb(async (_db, _client, dbName) => {
      await initCommand({ cwd: tempDir });
      await setupTestConfig(tempDir, dbName);
      await generateCommand({ name: "first", cwd: tempDir });
      await delay(10);
      await generateCommand({ name: "second", cwd: tempDir });

      await migrateCommand({ cwd: tempDir, force: true });

      // Reporting "nothing to do" would read as success; the operator asked for
      // something that cannot happen and has to be told which.
      const error = await assertRejects(
        () => migrateCommand({ cwd: tempDir, force: true, target: "first" }),
        Error,
      );
      assertStringIncludes(error.message, "already applied");
    });
  });
});

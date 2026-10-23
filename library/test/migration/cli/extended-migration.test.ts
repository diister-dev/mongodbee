import { mkdir, writeFile } from "node:fs/promises";
import process from "node:process";
import { assert, assertEquals } from "../../+assert.ts";
import { test } from "../../+harness.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { rollbackCommand } from "../../../src/migration/cli/commands/rollback.ts";
import { runCheck } from "../../../src/migration/cli/commands/check.ts";
import { checkMigrationStatus } from "../../../src/migration/check-status.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { MIGRATION_OPERATIONS_COLLECTION } from "../../../src/migration/history.ts";
import { type Db, MongoClient } from "../../../src/mongodb.ts";
import { withTempDir } from "./shared.ts";

const TEST_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";
const SRC = new URL("../../../src/", import.meta.url).href;
const BASE = "2026_01_01_0900_BASE0001@base";
const BATCH = "2026_01_02_0900_BATCH001@batch";
const LATER = "2026_01_03_0900_LATER001@later";

const PLUS_ONE = `.collection("alpha").transform({ up: (doc) => ({ ...doc, n: doc.n + 1 }), down: (doc) => ({ ...doc, n: doc.n - 1 }) }).end()`;
const TIMES_TEN = `.collection("alpha").transform({ up: (doc) => ({ ...doc, n: doc.n * 10 }), down: (doc) => ({ ...doc, n: doc.n / 10 }) }).end()`;
const CREATE_BETA = `.createCollection("beta").end()`;

type Version = {
  readonly batch: string;
  readonly batchCollections?: string;
  readonly later?: boolean;
};

const ALPHA = "alpha: { n: v.number() }";
const BETA = "beta: { label: v.string() }";

function migration(
  id: string,
  parent: string | null,
  collections: string,
  steps: string,
): string {
  return `
import * as v from "${SRC}schema.ts";
import { migrationDefinition } from "${SRC}migration/definition.ts";
${parent ? `import parent from "./${parent}.ts";` : ""}
export default migrationDefinition(${JSON.stringify(id)}, ${JSON.stringify(id.split("@")[1])}, {
  parent: ${parent ? "parent" : "null"},
  schemas: { collections: { ${collections} } },
  migrate: (b) => b${steps}.compile(),
});
`;
}

async function project(
  root: string,
  name: string,
  dbName: string,
  version: Version,
): Promise<string> {
  const dir = `${root}/${name}`;
  await mkdir(`${dir}/migrations`, { recursive: true });
  const batchCollections = version.batchCollections ?? ALPHA;
  await writeFile(
    `${dir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: ${JSON.stringify(TEST_URI)} }, name: ${JSON.stringify(dbName)} }, paths: { migrations: "./migrations", schemas: "./schemas.ts" }, migration: { validation: { frozen: false } } };`,
  );
  await writeFile(
    `${dir}/schemas.ts`,
    `import * as v from "${SRC}schema.ts";\nexport const schemas = { collections: { ${batchCollections} } };\n`,
  );
  await writeFile(
    `${dir}/migrations/${BASE}.ts`,
    migration(
      BASE,
      null,
      ALPHA,
      `.createCollection("alpha").seed([{ n: 1 }]).end()`,
    ),
  );
  await writeFile(
    `${dir}/migrations/${BATCH}.ts`,
    migration(BATCH, BASE, batchCollections, version.batch),
  );
  if (version.later) {
    await writeFile(
      `${dir}/migrations/${LATER}.ts`,
      migration(LATER, BATCH, batchCollections, ""),
    );
  }
  return dir;
}

async function alphaValue(db: Db): Promise<unknown> {
  const [doc] = await db.collection("alpha").find({}).toArray();
  return doc?.n;
}

async function withProject(
  work: (db: Db, root: string, dbName: string) => Promise<void>,
): Promise<void> {
  await withTempDir(async (root) => {
    const dbName = `mongodbee_test_extended_${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`;
    const client = new MongoClient(TEST_URI);
    await client.connect();
    try {
      await work(client.db(dbName), root, dbName);
    } finally {
      await client.db(dbName).dropDatabase();
      await client.close();
    }
  });
}

async function errorOf(work: () => Promise<unknown>): Promise<string> {
  try {
    await work();
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
  throw new Error("expected a refusal");
}

test({
  name: "migrate runs only the operations added to a migration after it was applied",
  timeout: 60_000,
  fn: async () => {
    await withProject(async (db, root, dbName) => {
      const v1 = await project(root, "v1", dbName, { batch: PLUS_ONE });
      await migrateCommand({ cwd: v1, force: true });
      assertEquals(await alphaValue(db), 2);

      const v2 = await project(root, "v2", dbName, {
        batch: PLUS_ONE + TIMES_TEN,
      });
      await migrateCommand({ cwd: v2, force: true });

      assertEquals(await alphaValue(db), 20);
      assertEquals(await getAppliedMigrationIds(db), [BASE, BATCH]);

      await migrateCommand({ cwd: v2, force: true });
      assertEquals(await alphaValue(db), 20);

      await rollbackCommand({ cwd: v2, force: true });
      assertEquals(await alphaValue(db), 1);
      assertEquals(await getAppliedMigrationIds(db), [BASE]);
    });
  },
});

test({
  name: "rollback of an extended migration undoes only the operations that ran",
  timeout: 60_000,
  fn: async () => {
    await withProject(async (db, root, dbName) => {
      const v1 = await project(root, "v1", dbName, { batch: PLUS_ONE });
      await migrateCommand({ cwd: v1, force: true });
      const v2 = await project(root, "v2", dbName, {
        batch: PLUS_ONE + TIMES_TEN,
      });

      await rollbackCommand({ cwd: v2, force: true });

      assertEquals(await alphaValue(db), 1);
      assertEquals(await getAppliedMigrationIds(db), [BASE]);

      await migrateCommand({ cwd: v2, force: true });
      assertEquals(await alphaValue(db), 20);
    });
  },
});

test({
  name: "migrate and rollback refuse a migration whose applied operations changed, naming it and the operation",
  timeout: 60_000,
  fn: async () => {
    await withProject(async (db, root, dbName) => {
      const v1 = await project(root, "v1", dbName, { batch: PLUS_ONE });
      await migrateCommand({ cwd: v1, force: true });
      const inserted = await project(root, "inserted", dbName, {
        batch: CREATE_BETA + PLUS_ONE,
        batchCollections: `${ALPHA}, ${BETA}`,
      });

      const migrateError = await errorOf(() =>
        migrateCommand({ cwd: inserted, force: true }),
      );
      assert(migrateError.includes(BATCH), migrateError);
      assert(migrateError.includes("operation #1"), migrateError);
      assert(migrateError.includes("create_collection: beta"), migrateError);

      const rollbackError = await errorOf(() =>
        rollbackCommand({ cwd: inserted, force: true }),
      );
      assert(rollbackError.includes(BATCH), rollbackError);

      assertEquals(await alphaValue(db), 2);
      assertEquals(await getAppliedMigrationIds(db), [BASE, BATCH]);
      assertEquals(
        (await db.listCollections({ name: "beta" }).toArray()).length,
        0,
      );
    });
  },
});

test({
  name: "migrate refuses to extend a migration that a later applied migration builds on",
  timeout: 60_000,
  fn: async () => {
    await withProject(async (db, root, dbName) => {
      const v1 = await project(root, "v1", dbName, {
        batch: PLUS_ONE,
        later: true,
      });
      await migrateCommand({ cwd: v1, force: true });
      const v2 = await project(root, "v2", dbName, {
        batch: PLUS_ONE + TIMES_TEN,
        later: true,
      });

      const error = await errorOf(() =>
        migrateCommand({ cwd: v2, force: true }),
      );

      assert(error.includes(BATCH), error);
      assert(error.includes(LATER), error);
      assertEquals(await alphaValue(db), 2);

      await rollbackCommand({ cwd: v2, force: true });
      await migrateCommand({ cwd: v2, force: true });
      assertEquals(await alphaValue(db), 20);
    });
  },
});

test({
  name: "a migration applied before operations were recorded gets them at the next migrate, and its later extension runs",
  timeout: 60_000,
  fn: async () => {
    await withProject(async (db, root, dbName) => {
      const v1 = await project(root, "v1", dbName, { batch: PLUS_ONE });
      await migrateCommand({ cwd: v1, force: true });
      const history = db.collection(MIGRATION_OPERATIONS_COLLECTION);
      await history.updateMany({}, { $unset: { steps: "" } });

      await migrateCommand({ cwd: v1, force: true });
      assertEquals(await alphaValue(db), 2);
      assertEquals(
        await history.countDocuments({ steps: { $exists: true } }),
        2,
      );
      assertEquals(await history.countDocuments({}), 2);

      const v2 = await project(root, "v2", dbName, {
        batch: PLUS_ONE + TIMES_TEN,
      });
      await migrateCommand({ cwd: v2, force: true });
      assertEquals(await alphaValue(db), 20);
    });
  },
});

test({
  name: "a migration applied before operations were recorded still rolls back whole",
  timeout: 60_000,
  fn: async () => {
    await withProject(async (db, root, dbName) => {
      const v1 = await project(root, "v1", dbName, { batch: PLUS_ONE });
      await migrateCommand({ cwd: v1, force: true });
      await db
        .collection(MIGRATION_OPERATIONS_COLLECTION)
        .updateMany({}, { $unset: { steps: "" } });

      await rollbackCommand({ cwd: v1, force: true });

      assertEquals(await alphaValue(db), 1);
      assertEquals(await getAppliedMigrationIds(db), [BASE]);
    });
  },
});

test({
  name: "check and checkMigrationStatus report an applied migration extended since",
  timeout: 60_000,
  fn: async () => {
    await withProject(async (db, root, dbName) => {
      const v1 = await project(root, "v1", dbName, { batch: PLUS_ONE });
      await migrateCommand({ cwd: v1, force: true });
      const clean = await runCheck({ cwd: v1, mode: "quick" });
      assertEquals(clean.database, { reachable: true, changed: [] });
      assertEquals(clean.valid, true);

      const v2 = await project(root, "v2", dbName, {
        batch: PLUS_ONE + TIMES_TEN,
      });
      const report = await runCheck({ cwd: v2, mode: "quick" });
      assertEquals(report.valid, false);
      assert(report.database?.reachable === true);
      assertEquals(report.database.changed.length, 1);
      assert(report.database.changed[0].includes(BATCH));
      assert(
        report.database.changed[0].includes("#2 transform_collection: alpha"),
        report.database.changed[0],
      );

      const status = await checkMigrationStatus({
        cwd: v2,
        db,
        strictValidation: false,
      });
      assertEquals(status.ok, false);
      assertEquals(status.database?.changedIds, [BATCH]);
      assertEquals(status.database?.isUpToDate, false);
    });
  },
});

test({
  name: "check says so when the database cannot be reached, and still passes",
  timeout: 60_000,
  fn: async () => {
    await withTempDir(async (root) => {
      const dir = await project(root, "v1", "unreachable", {
        batch: PLUS_ONE,
      });
      await writeFile(
        `${dir}/mongodbee.config.ts`,
        `export default { database: { connection: { uri: "mongodb://127.0.0.1:1/?directConnection=true" }, name: "unreachable" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" }, migration: { validation: { frozen: false } } };`,
      );

      const report = await runCheck({ cwd: dir, mode: "quick" });

      assertEquals(report.valid, true);
      assertEquals(report.database?.reachable, false);
    });
  },
});

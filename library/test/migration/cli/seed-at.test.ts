import { mkdir, writeFile } from "node:fs/promises";
import process from "node:process";
import { test } from "../../+harness.ts";
import { assert, assertEquals, assertRejects } from "../../+assert.ts";
import { type Db, MongoClient } from "../../../src/mongodb.ts";
import { seedCommand } from "../../../src/migration/cli/commands/seed.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { syncCommand } from "../../../src/migration/cli/commands/sync.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { MIGRATION_OPERATIONS_COLLECTION } from "../../../src/migration/history.ts";
import {
  discoverMultiCollectionInstances,
  getMultiCollectionMigrations,
} from "../../../src/migration/multicollection-registry.ts";
import { runCli, withTempDir } from "./shared.ts";

const TEST_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";
const SRC = new URL("../../../src/", import.meta.url).href;

const BIRTH = "2026_01_01_0900_SEEDAT1@birth";
const MOVE = "2026_02_01_0900_SEEDAT2@move";
const BROKEN = "2026_03_01_0900_SEEDAT3@broken";

const ITEMS_V1 = `{ _id: dbId("item"), code: withIndex(v.pipe(v.string(), v.minLength(1)), { unique: true }), label: v.string() }`;
const ITEMS_V2 = `{ _id: dbId("item"), code: withIndex(v.pipe(v.string(), v.minLength(1)), { unique: true }), title: v.string() }`;
const EXPOSITIONS = `{ _id: refId("exposition"), name: v.string() }`;
const BADGE_V1 = `{ _id: dbId("badge"), label: withIndex(v.pipe(v.string(), v.minLength(1)), { unique: true }) }`;
const BADGE_V2 = `{ _id: dbId("badge"), label: withIndex(v.pipe(v.string(), v.minLength(1)), { unique: true }), shout: v.string() }`;

const MOVE_BODY = `b.collection("items").transform({
    up: (doc) => { const { label, ...rest } = doc; return { ...rest, title: String(label).toUpperCase() }; },
    down: (doc) => { const { title, ...rest } = doc; return { ...rest, label: String(title).toLowerCase() }; },
  }).end()
  .multiModelInstances("exposition").type("badge").transform({
    up: (doc) => ({ ...doc, shout: String(doc.label).toUpperCase() }),
    down: (doc) => { const { shout, ...rest } = doc; return rest; },
  }).end().end()
  .renameCollection("archive", "archive_v2")
  .compile()`;

const BROKEN_BODY = `b.collection("items").transform({
    up: (doc) => { if (String(doc.code).startsWith("real-")) throw new Error("injected migration failure"); return doc; },
    down: (doc) => doc,
  }).end().compile()`;

function migration(
  id: string,
  parent: string | null,
  schemas: string,
  body: string,
): string {
  return `
import * as v from "${SRC}schema.ts";
import { dbId, refId } from "${SRC}ids.ts";
import { withIndex } from "${SRC}indexes.ts";
import { migrationDefinition } from "${SRC}migration/definition.ts";
${parent ? `import parent from "./${parent}.ts";` : ""}
export default migrationDefinition(${JSON.stringify(id)}, ${JSON.stringify(id.split("@")[1])}, {
  parent: ${parent ? "parent" : "null"},
  schemas: ${schemas},
  migrate: (b) => ${body},
});
`;
}

const V1 = `{ collections: { items: ${ITEMS_V1}, archive: { _id: dbId("archive"), n: v.number() }, expositions: ${EXPOSITIONS} }, multiModels: { exposition: { badge: ${BADGE_V1} } } }`;
const V2 = `{ collections: { items: ${ITEMS_V2}, archive_v2: { _id: dbId("archive"), n: v.number() }, expositions: ${EXPOSITIONS} }, multiModels: { exposition: { badge: ${BADGE_V2} } } }`;

interface ProjectOptions {
  readonly withBroken?: boolean;
  readonly scenarioShape?: string;
  readonly scenarioStages?: string;
}

async function writeProject(
  dir: string,
  dbName: string,
  options: ProjectOptions = {},
): Promise<void> {
  await mkdir(`${dir}/migrations`, { recursive: true });
  await writeFile(
    `${dir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: ${JSON.stringify(TEST_URI)} }, name: ${JSON.stringify(dbName)} }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
  await writeFile(
    `${dir}/migrations/${BIRTH}.ts`,
    migration(BIRTH, null, V1, "b.compile()"),
  );
  await writeFile(
    `${dir}/migrations/${MOVE}.ts`,
    migration(MOVE, BIRTH, V2, MOVE_BODY),
  );
  if (options.withBroken) {
    await writeFile(
      `${dir}/migrations/${BROKEN}.ts`,
      migration(BROKEN, MOVE, V2, BROKEN_BODY),
    );
  }
  await writeFile(
    `${dir}/schemas.ts`,
    `
import * as v from "${SRC}schema.ts";
import { dbId, refId } from "${SRC}ids.ts";
import { withIndex } from "${SRC}indexes.ts";
export const schemas = ${V2};
`,
  );
  await writeFile(
    `${dir}/scenario.ts`,
    `export const scenario = { name: "seed-at", birth: ${JSON.stringify(BIRTH)}, shape: ${
      options.scenarioShape ??
      "{ items: 4, archive: 3, expositions: 2, badge: 3 }"
    }${options.scenarioStages ? `, stages: ${options.scenarioStages}` : ""} };`,
  );
}

async function withProject(
  options: ProjectOptions,
  fn: (dir: string, db: Db, name: string) => Promise<void>,
): Promise<void> {
  await withTempDir(async (dir) => {
    const name = `mongodbee_test_seedat_${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`;
    await writeProject(dir, name, options);
    const client = new MongoClient(TEST_URI);
    await client.connect();
    try {
      await fn(dir, client.db(name), name);
    } finally {
      await client.db(name).dropDatabase();
      await client.close();
    }
  });
}

async function collectionNames(db: Db): Promise<string[]> {
  return (await db.listCollections({}, { nameOnly: true }).toArray())
    .map((c) => c.name)
    .filter((n) => !n.startsWith("system."))
    .sort();
}

async function dataSnapshot(
  db: Db,
): Promise<Record<string, Record<string, unknown>[]>> {
  const snapshot: Record<string, Record<string, unknown>[]> = {};
  for (const name of await collectionNames(db)) {
    if (name.startsWith("__dbee")) continue;
    snapshot[name] = (await db
      .collection(name)
      .find({ _type: { $nin: ["_information", "_migrations"] } })
      .sort({ _id: 1 })
      .toArray()) as Record<string, unknown>[];
  }
  return snapshot;
}

test({
  name: "seed: generated multi-model instances carry their markers, so migrate and sync run on the seeded database",
  timeout: 120_000,
  fn: async () => {
    await withProject({}, async (dir, db) => {
      await seedCommand({
        cwd: dir,
        scenario: "./scenario.ts",
        at: BIRTH,
        json: true,
      });
      const instances = await discoverMultiCollectionInstances(
        db,
        "exposition",
      );
      assertEquals(instances.length, 2);
      for (const name of instances) {
        const history = await getMultiCollectionMigrations(db, name);
        assertEquals(history?.fromMigrationId, BIRTH);
      }
      await migrateCommand({ cwd: dir, force: true, mode: "quick" });
      assertEquals(await getAppliedMigrationIds(db), [BIRTH, MOVE]);
      await syncCommand({ cwd: dir, force: true });
      for (const name of instances) {
        const info = await db.listCollections({ name }).toArray();
        const options = (info[0] as { options?: { validator?: unknown } })
          .options;
        assert(options?.validator, `${name} must carry a validator`);
        const indexes = await db.collection(name).indexes();
        assert(
          indexes.some((i) => i.unique === true),
          `${name} must carry the unique badge index`,
        );
        const history = await getMultiCollectionMigrations(db, name);
        assert(
          history?.appliedMigrations.some((m) => m.id === MOVE),
          "the instance records the migration it went through",
        );
        const badges = await db
          .collection(name)
          .find({ _type: "badge" })
          .toArray();
        assertEquals(badges.length, 3);
        for (const badge of badges) {
          assertEquals(badge.shout, String(badge.label).toUpperCase());
        }
      }
    });
  },
});

test({
  name: "seed: --force is refused and names its two replacements",
  timeout: 60_000,
  fn: async () => {
    await withProject({}, async (dir, db) => {
      await assertRejects(
        () =>
          seedCommand({
            cwd: dir,
            scenario: "./scenario.ts",
            force: true,
            json: true,
          }),
        Error,
        "--allow-non-empty",
      );
      assertEquals(await collectionNames(db), []);
    });
  },
});

test({
  name: "seed: a non-empty database is refused without --allow-non-empty and kept intact",
  timeout: 60_000,
  fn: async () => {
    await withProject({}, async (dir, db) => {
      await db.collection("unrelated").insertOne({ _id: "kept" } as never);
      await assertRejects(
        () => seedCommand({ cwd: dir, scenario: "./scenario.ts", json: true }),
        Error,
        "--allow-non-empty",
      );
      assertEquals(await collectionNames(db), ["unrelated"]);
    });
  },
});

test({
  name: "seed: --allow-non-empty writes next to unrelated collections and leaves them alone",
  timeout: 60_000,
  fn: async () => {
    await withProject({}, async (dir, db) => {
      await db.collection("unrelated").insertOne({ _id: "kept" } as never);
      await seedCommand({
        cwd: dir,
        scenario: "./scenario.ts",
        allowNonEmpty: true,
        json: true,
      });
      assertEquals(await db.collection("unrelated").find({}).toArray(), [
        { _id: "kept" },
      ] as never);
      assertEquals(await getAppliedMigrationIds(db), [BIRTH, MOVE]);
      assertEquals(await db.collection("items").countDocuments(), 4);
    });
  },
});

test({
  name: "seed: --allow-non-empty never drops a collection the schemas manage, even an empty one",
  timeout: 60_000,
  fn: async () => {
    await withProject({}, async (dir, db) => {
      await db.createCollection("items");
      await db.collection("items").createIndex({ legacy: 1 });
      await assertRejects(
        () =>
          seedCommand({
            cwd: dir,
            scenario: "./scenario.ts",
            allowNonEmpty: true,
            json: true,
          }),
        Error,
        "items",
      );
      assert(
        (await db.collection("items").indexes()).some(
          (i) => "legacy" in (i.key ?? {}),
        ),
        "the existing collection must not have been dropped",
      );
    });
  },
});

test({
  name: "seed: --allow-non-empty refuses a database that already has a migration ledger",
  timeout: 60_000,
  fn: async () => {
    await withProject({}, async (dir, db) => {
      await seedCommand({
        cwd: dir,
        scenario: "./scenario.ts",
        at: BIRTH,
        json: true,
      });
      for (const name of await collectionNames(db)) {
        if (name !== MIGRATION_OPERATIONS_COLLECTION) {
          await db.collection(name).drop();
        }
      }
      const ledgerBefore = await getAppliedMigrationIds(db);
      await assertRejects(
        () =>
          seedCommand({
            cwd: dir,
            scenario: "./scenario.ts",
            allowNonEmpty: true,
            json: true,
          }),
        Error,
        "ledger",
      );
      assertEquals(await getAppliedMigrationIds(db), ledgerBefore);
    });
  },
});

test({
  name: "seed: a failing insert leaves the database empty, ledger included",
  timeout: 60_000,
  fn: async () => {
    await withProject({}, async (dir, db) => {
      await writeFile(
        `${dir}/scenario.ts`,
        `export const scenario = { name: "dup", birth: ${JSON.stringify(BIRTH)}, shape: { items: 3, archive: 0, expositions: 1, badge: 1 }, rules: { items: { code: () => "same" } } };`,
      );
      await assertRejects(
        () =>
          seedCommand({
            cwd: dir,
            scenario: "./scenario.ts",
            allowViolations: true,
            json: true,
          }),
        Error,
        "duplicate key",
      );
      assertEquals(await collectionNames(db), []);
    });
  },
});

test({
  name: "seed: replay=mongo writes at birth then runs the real migrate, and matches the in-memory replay",
  timeout: 180_000,
  fn: async () => {
    await withProject({}, async (dir, db, name) => {
      await seedCommand({
        cwd: dir,
        scenario: "./scenario.ts",
        at: MOVE,
        json: true,
      });
      const memory = await dataSnapshot(db);
      const memoryLedger = await getAppliedMigrationIds(db);
      await db.dropDatabase();
      await seedCommand({
        cwd: dir,
        scenario: "./scenario.ts",
        at: MOVE,
        replay: "mongo",
        json: true,
      });
      const mongo = await dataSnapshot(db);
      assertEquals(Object.keys(mongo).sort(), Object.keys(memory).sort());
      assertEquals(mongo, memory);
      assertEquals(await getAppliedMigrationIds(db), memoryLedger);
      assert(mongo.archive_v2.length === 3, `archive moved in ${name}`);
      const ledger = await db
        .collection(MIGRATION_OPERATIONS_COLLECTION)
        .find({}, { projection: { _id: 0, migrationId: 1, adopted: 1 } })
        .toArray();
      assertEquals(
        ledger.map((r) => [r.migrationId, r.adopted === true]),
        [
          [BIRTH, true],
          [MOVE, false],
        ],
        "birth is baselined, the later migration really ran",
      );
      assert(
        mongo.items.every(
          (i) => typeof i.title === "string" && !("label" in i),
        ),
      );
    });
  },
});

test({
  name: "seed: replay=mongo removes data and ledger when the real migrate fails after applying a migration",
  timeout: 180_000,
  fn: async () => {
    await withProject({ withBroken: true }, async (dir, db) => {
      await writeFile(
        `${dir}/scenario.ts`,
        `export const scenario = { name: "real", birth: ${JSON.stringify(BIRTH)}, shape: { items: 3, archive: 1, expositions: 1, badge: 1 }, rules: { items: { code: (c) => "real-" + c.index } } };`,
      );
      await assertRejects(
        () =>
          seedCommand({
            cwd: dir,
            scenario: "./scenario.ts",
            at: BROKEN,
            replay: "mongo",
            json: true,
          }),
        Error,
        "injected migration failure",
      );
      assertEquals(await collectionNames(db), []);
    });
  },
});

test({
  name: "cli: `migrate --help` prints the help and touches no database",
  timeout: 60_000,
  fn: async () => {
    await withProject({}, async (dir, db) => {
      const { code, stdout } = await runCli(dir, ["migrate", "--help"]);
      assertEquals(code, 0);
      assert(stdout.includes("MIGRATE OPTIONS"), stdout);
      assert(!stdout.includes("Applying migrations"), stdout);
      assertEquals(await collectionNames(db), []);
    });
  },
});

test({
  name: "seed: a stage adds data at its migration, identically with replay=memory and replay=mongo",
  timeout: 240_000,
  fn: async () => {
    const stages = `{ ${JSON.stringify(MOVE)}: { shape: { archive_v2: 2 } } }`;
    await withProject({ scenarioStages: stages }, async (dir, db) => {
      await seedCommand({
        cwd: dir,
        scenario: "./scenario.ts",
        at: MOVE,
        json: true,
      });
      const memory = await dataSnapshot(db);
      await db.dropDatabase();
      await seedCommand({
        cwd: dir,
        scenario: "./scenario.ts",
        at: MOVE,
        replay: "mongo",
        json: true,
      });
      const mongo = await dataSnapshot(db);
      assertEquals(
        memory.archive_v2.length,
        5,
        "3 renamed at MOVE plus 2 from the stage",
      );
      assertEquals(mongo, memory);
    });
    await withProject({ scenarioStages: stages }, async (dir, db) => {
      await seedCommand({
        cwd: dir,
        scenario: "./scenario.ts",
        at: BIRTH,
        replay: "mongo",
        json: true,
      });
      const birthOnly = await dataSnapshot(db);
      assertEquals(
        birthOnly.archive.length,
        3,
        "a stage after --at is not applied",
      );
      assert(!("archive_v2" in birthOnly));
    });
  },
});

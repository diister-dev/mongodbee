import { mkdir, writeFile } from "node:fs/promises";
import process from "node:process";
import { newId } from "../../../src/ids.ts";
import { extractCommand } from "../../../src/migration/cli/commands/extract.ts";
import { markMigrationAsAdopted } from "../../../src/migration/state.ts";
import {
  buildMigrationChain,
  loadAllMigrations,
} from "../../../src/migration/discovery.ts";
import { detectInstancesNeedingCatchUp } from "../../../src/migration/catch-up.ts";
import { MongoClient } from "../../../src/mongodb.ts";
import { assert, assertEquals } from "../../+assert.ts";
import { test } from "../../+harness.ts";
import { withTempDir } from "./shared.ts";

const TEST_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";
const SRC = new URL("../../../src/", import.meta.url).href;
const BIRTH = "2026_01_01_0900_BIRTH01@birth";
const NEXT = "2026_02_01_0900_NEXT001@next";

const HEADER = `
import * as v from "${SRC}schema.ts";
import { dbId, refId } from "${SRC}ids.ts";
import { withIndex } from "${SRC}indexes.ts";
import { notPersonal, personal, personId } from "${SRC}privacy/mod.ts";
`;

async function writeProject(
  dir: string,
  schemasBody: string,
  migrations: readonly string[] = [BIRTH],
): Promise<void> {
  await mkdir(`${dir}/migrations`, { recursive: true });
  await writeFile(
    `${dir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: ${JSON.stringify(TEST_URI)} } }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
  await writeFile(
    `${dir}/schemas.ts`,
    `${HEADER}\nexport const schemas = ${schemasBody};\n`,
  );
  for (const [index, id] of migrations.entries()) {
    const parent = index === 0 ? null : migrations[index - 1];
    await writeFile(
      `${dir}/migrations/${id}.ts`,
      `
import { migrationDefinition } from "${SRC}migration/definition.ts";
import { schemas } from "../schemas.ts";
${parent ? `import parent from "./${parent}.ts";` : ""}
export default migrationDefinition(${JSON.stringify(id)}, "step", {
  parent: ${parent ? "parent" : "null"},
  schemas,
  migrate: (b) => b.compile(),
});
`,
    );
  }
}

function names() {
  const tag = crypto.randomUUID().replace(/-/g, "").slice(0, 8);
  return {
    source: `mongodbee_test_r2_src_${tag}`,
    target: `mongodbee_test_r2_dst_${tag}`,
  };
}

type Raw = { _id: string; [key: string]: unknown };

async function withWorld(
  work: (ctx: {
    client: MongoClient;
    source: string;
    target: string;
  }) => Promise<void>,
): Promise<void> {
  const { source, target } = names();
  const client = new MongoClient(TEST_URI);
  await client.connect();
  try {
    await work({ client, source, target });
  } finally {
    await client.db(source).dropDatabase();
    await client.db(target).dropDatabase();
    await client.close();
  }
}

const STRENGTH_ONE = `{
  collections: {
    users: { _id: personId("user"), login: personal(v.string(), { role: "direct" }) },
    tags: {
      _id: notPersonal(dbId("tag"), "vocabulary", { strict: "keep" }),
      label: withIndex(v.string(), { unique: true, collation: { locale: "fr", strength: 1 } }),
    },
  },
}`;

test({
  name: "V6 extract: --dry-run passes where extract then fails on a unique index the in-memory check folds differently, and the failure leaves no collection behind",
  timeout: 60_000,
  fn: async () => {
    await withTempDir(async (dir) => {
      await writeProject(dir, STRENGTH_ONE);
      await withWorld(async ({ client, source, target }) => {
        const src = client.db(source);
        await src
          .collection<Raw>("users")
          .insertOne({ _id: `user:${newId()}`, login: "alice" });
        await src.collection<Raw>("tags").insertMany([
          { _id: `tag:${newId()}`, label: "Cafe" },
          { _id: `tag:${newId()}`, label: "Café" },
        ]);
        await markMigrationAsAdopted(src, BIRTH, "step");
        const outcome = async (dryRun: boolean) => {
          try {
            await extractCommand({
              cwd: dir,
              fromDb: source,
              toDb: target,
              secret: "r2",
              json: true,
              dryRun,
            });
            return "ok";
          } catch (error) {
            return error instanceof Error
              ? error.message.slice(0, 40)
              : "error";
          }
        };
        const dry = await outcome(true);
        const real = await outcome(false);
        const left = (
          await client
            .db(target)
            .listCollections({}, { nameOnly: true })
            .toArray()
        ).map((c) => c.name);
        assertEquals(
          { same: dry === real, left },
          { same: true, left: [] },
          `dry=${dry} real=${real}`,
        );
      });
    });
  },
});

const MULTI_MODEL = `{
  collections: {
    users: { _id: personId("user"), login: personal(v.string(), { role: "direct" }) },
  },
  multiModels: {
    exposition: {
      zone: { _id: refId("zone"), label: v.string() },
    },
  },
}`;

test({
  name: "V7 extract: a source behind the head replays in memory but its multi-model instances keep the source ledger, so the target asks for a catch-up",
  timeout: 60_000,
  fn: async () => {
    await withTempDir(async (dir) => {
      await writeProject(dir, MULTI_MODEL, [BIRTH, NEXT]);
      await withWorld(async ({ client, source, target }) => {
        const src = client.db(source);
        await src
          .collection<Raw>("users")
          .insertOne({ _id: `user:${newId()}`, login: "alice" });
        const instance = `exposition:${newId()}`;
        await src.collection<Raw>(instance).insertMany([
          {
            _id: "_information",
            _type: "_information",
            collectionType: "exposition",
            createdAt: new Date("2026-01-01T00:00:00Z"),
          },
          {
            _id: "_migrations",
            _type: "_migrations",
            fromMigrationId: BIRTH,
            mongodbeeVersion: "0.0.0-test",
            appliedMigrations: [
              {
                id: BIRTH,
                operation: "applied",
                appliedAt: new Date("2026-01-01T00:00:00Z"),
                status: "success",
                mongodbeeVersion: "0.0.0-test",
              },
            ],
          },
          { _id: `zone:${newId()}`, _type: "zone", label: "Zone A" },
        ]);
        await markMigrationAsAdopted(src, BIRTH, "step");
        await extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: "r2",
          json: true,
        });
        const chain = buildMigrationChain(
          await loadAllMigrations(`${dir}/migrations`),
        );
        const catchUp = await detectInstancesNeedingCatchUp(
          client.db(target),
          chain,
        );
        assertEquals(catchUp.totalInstances, 0);
      });
    });
  },
});

test({
  name: "V8 extract --force: a failed write leaves the pre-existing collection's indexes as they were",
  timeout: 60_000,
  fn: async () => {
    await withTempDir(async (dir) => {
      await writeProject(dir, STRENGTH_ONE);
      await withWorld(async ({ client, source, target }) => {
        const src = client.db(source);
        await src
          .collection<Raw>("users")
          .insertOne({ _id: `user:${newId()}`, login: "alice" });
        await src.collection<Raw>("tags").insertMany([
          { _id: `tag:${newId()}`, label: "Cafe" },
          { _id: `tag:${newId()}`, label: "Café" },
        ]);
        await markMigrationAsAdopted(src, BIRTH, "step");
        const dst = client.db(target);
        await dst
          .collection<Raw>("users")
          .insertOne({ _id: "keep-me", login: "local" });
        const snapshot = async () => ({
          collections: (
            await dst.listCollections({}, { nameOnly: false }).toArray()
          )
            .map((c) => ({ name: c.name, options: c.options }))
            .sort((a, b) => a.name.localeCompare(b.name)),
          indexes: (await dst.collection("users").indexes())
            .map((i) => i.name)
            .sort(),
          docs: await dst.collection("users").find({}).toArray(),
        });
        const before = await snapshot();
        let failed = false;
        try {
          await extractCommand({
            cwd: dir,
            fromDb: source,
            toDb: target,
            secret: "r2",
            json: true,
            force: true,
          });
        } catch {
          failed = true;
        }
        assertEquals(
          { failed, after: await snapshot() },
          { failed: true, after: before },
        );
      });
    });
  },
});

test({
  name: "guard extract: same secret and same source give the same target whatever the source insertion order, and --dry-run reports what extract writes",
  timeout: 120_000,
  fn: async () => {
    const fixture = await import("./extract-fixture.ts");
    await fixture.withProject(async (dir) => {
      const client = new MongoClient(TEST_URI);
      await client.connect();
      const dbs = [
        fixture.dbName("a"),
        fixture.dbName("b"),
        fixture.dbName("ta"),
        fixture.dbName("tb"),
      ];
      try {
        const [a, b, ta, tb] = dbs.map((n) => client.db(n));
        await fixture.populateSource(a);
        const names = (await a.listCollections().toArray()).map((c) => c.name);
        for (const name of [...names].reverse()) {
          const docs = await a.collection(name).find({}).toArray();
          if (docs.length > 0)
            await b.collection(name).insertMany(docs.reverse());
        }
        await markMigrationAsAdopted(a, BIRTH, "birth");
        await markMigrationAsAdopted(b, BIRTH, "birth");
        const logs: string[] = [];
        const original = console.log;
        console.log = (line: unknown) => logs.push(String(line));
        try {
          await extractCommand({
            cwd: dir,
            fromDb: dbs[0],
            dryRun: true,
            secret: "r2",
            json: true,
          });
          await extractCommand({
            cwd: dir,
            fromDb: dbs[0],
            toDb: dbs[2],
            secret: "r2",
            json: true,
          });
          await extractCommand({
            cwd: dir,
            fromDb: dbs[1],
            toDb: dbs[3],
            secret: "r2",
            json: true,
          });
        } finally {
          console.log = original;
        }
        assertEquals(logs[0], logs[1]);
        const dump = async (db: typeof ta) => {
          const out: Record<string, unknown> = {};
          for (const { name } of await db.listCollections().toArray()) {
            if (name.startsWith("mongodbee_") || name.startsWith("__dbee"))
              continue;
            out[name] = await db
              .collection(name)
              .find({})
              .sort({ _id: 1 })
              .toArray();
          }
          return Object.fromEntries(
            Object.entries(out).sort(([x], [y]) => x.localeCompare(y)),
          );
        };
        assertEquals(
          JSON.stringify(await dump(tb)),
          JSON.stringify(await dump(ta)),
        );
      } finally {
        for (const n of dbs) await client.db(n).dropDatabase();
        await client.close();
      }
    });
  },
});

const PLAIN_UNIQUE = `{
  collections: {
    users: { _id: personId("user"), login: personal(v.string(), { role: "direct" }) },
    tags: {
      _id: notPersonal(dbId("tag"), "vocabulary", { strict: "keep" }),
      label: withIndex(v.string(), { unique: true }),
    },
  },
}`;

test({
  name: "V20 extract --allow-violations never writes data that breaks a unique index, and says so before writing anything",
  timeout: 60_000,
  fn: async () => {
    await withTempDir(async (dir) => {
      await writeProject(dir, PLAIN_UNIQUE);
      await withWorld(async ({ client, source, target }) => {
        const src = client.db(source);
        await src
          .collection<Raw>("users")
          .insertOne({ _id: `user:${newId()}`, login: "alice" });
        await src.collection<Raw>("tags").insertMany([
          { _id: `tag:${newId()}`, label: "Cafe" },
          { _id: `tag:${newId()}`, label: "Cafe" },
        ]);
        await markMigrationAsAdopted(src, BIRTH, "step");
        let message: string | undefined;
        try {
          await extractCommand({
            cwd: dir,
            fromDb: source,
            toDb: target,
            secret: "r2",
            json: true,
            allowViolations: true,
          });
        } catch (error) {
          message = error instanceof Error ? error.message : String(error);
        }
        assert(message?.includes("unique index"), String(message));
        assert(message?.includes("even with --allow-violations"));
        assertEquals(await client.db(target).listCollections().toArray(), []);
      });
    });
  },
});

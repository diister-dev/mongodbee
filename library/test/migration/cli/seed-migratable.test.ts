import { mkdir, writeFile } from "node:fs/promises";
import process from "node:process";
import { test } from "../../+harness.ts";
import { assert, assertEquals } from "../../+assert.ts";
import { MongoClient } from "../../../src/mongodb.ts";
import { seedCommand } from "../../../src/migration/cli/commands/seed.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { withTempDir } from "./shared.ts";

const TEST_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";
const SRC = new URL("../../../src/", import.meta.url).href;

const BIRTH = "2026_01_01_0900_BIRTH01@birth";
const RENAME = "2026_02_01_0900_RENAM01@rename";

const ITEMS = `{ _id: dbId("item"), code: withIndex(v.pipe(v.string(), v.minLength(1)), { unique: true }), label: v.string() }`;
const ARCHIVE = `{ _id: dbId("archive"), n: v.number() }`;

function migration(
  id: string,
  name: string,
  parent: string | null,
  collections: string,
  body: string,
): string {
  return `
import * as v from "${SRC}schema.ts";
import { dbId } from "${SRC}ids.ts";
import { withIndex } from "${SRC}indexes.ts";
import { migrationDefinition } from "${SRC}migration/definition.ts";
${parent ? `import parent from "./${parent}";` : ""}
export default migrationDefinition(${JSON.stringify(id)}, ${JSON.stringify(name)}, {
  parent: ${parent ? "parent" : "null"},
  schemas: { collections: ${collections} },
  migrate: (b) => ${body},
});
`;
}

async function writeProject(dir: string, dbName: string): Promise<void> {
  await mkdir(`${dir}/migrations`, { recursive: true });
  await writeFile(
    `${dir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: ${JSON.stringify(TEST_URI)} }, name: ${JSON.stringify(dbName)} }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
  await writeFile(
    `${dir}/migrations/${BIRTH}.ts`,
    migration(
      BIRTH,
      "birth",
      null,
      `{ items: ${ITEMS}, archive: ${ARCHIVE} }`,
      "b.compile()",
    ),
  );
  await writeFile(
    `${dir}/migrations/${RENAME}.ts`,
    migration(
      RENAME,
      "rename",
      `${BIRTH}.ts`,
      `{ items: ${ITEMS}, archive_v2: ${ARCHIVE} }`,
      `b.renameCollection("archive", "archive_v2").compile()`,
    ),
  );
  await writeFile(
    `${dir}/schemas.ts`,
    `
import * as v from "${SRC}schema.ts";
import { dbId } from "${SRC}ids.ts";
import { withIndex } from "${SRC}indexes.ts";
export const schemas = { collections: { items: ${ITEMS}, archive_v2: ${ARCHIVE} } };
`,
  );
  await writeFile(
    `${dir}/scenario.ts`,
    `export const scenario = { name: "items", birth: ${JSON.stringify(BIRTH)}, shape: { items: 5, archive: 0 } };`,
  );
}

test({
  name: "seed: a seeded database carries its indexes and empty collections, so the next migrations run on it",
  timeout: 60_000,
  ignore: true,
  fn: async () => {
    await withTempDir(async (dir) => {
      const name = `mongodbee_test_seedmig_${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`;
      await writeProject(dir, name);
      const client = new MongoClient(TEST_URI);
      await client.connect();
      try {
        await seedCommand({
          cwd: dir,
          scenario: "./scenario.ts",
          at: BIRTH,
          json: true,
        });
        const db = client.db(name);
        const indexes = await db.collection("items").indexes();
        assert(
          indexes.some((i) => i.unique === true && "code" in (i.key ?? {})),
          "the unique index of the seeded collection must exist",
        );
        const names = (
          await db.listCollections({}, { nameOnly: true }).toArray()
        ).map((c) => c.name);
        assert(
          names.includes("archive"),
          "an empty collection must still exist",
        );
        await migrateCommand({ cwd: dir, force: true });
        assertEquals(await getAppliedMigrationIds(db), [BIRTH, RENAME]);
      } finally {
        await client.db(name).dropDatabase();
        await client.close();
      }
    });
  },
});

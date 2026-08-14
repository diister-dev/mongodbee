/**
 * Verrou — `migrate --last N` must never apply a migration it did not
 * validate.
 *
 * `--last N` narrows VALIDATION; it has never narrowed what gets APPLIED —
 * STEP 4 of the command applies every pending migration regardless. While the
 * migrations outside the window were still being fully simulated (to propagate
 * state to the window), that mismatch was invisible. Seeding the window from
 * the parent's schemas removed those simulations, so the gap became real:
 * `--last 1` in front of two pending migrations would have simulated the last
 * one and applied both.
 *
 * The command therefore widens its own window down to the earliest PENDING
 * migration. Below that floor `--last N` is a request to skip validating
 * something that is about to be written to a real database.
 */
import { assert, assertEquals, assertRejects } from "@std/assert";
import * as path from "@std/path";
import { MongoClient } from "../../../src/mongodb.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import {
  buildMigrationChain,
  loadAllMigrations,
} from "../../../src/migration/discovery.ts";
import { validateMigrationsWithSimulation } from "../../../src/migration/cli/utils/validate-migrations.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";

const TEST_MONGODB_URI = Deno.env.get("TEST_MONGODB_URI") ||
  Deno.env.get("MONGODBEE_TEST_URI") ||
  "mongodb://localhost:27017";

const LIB = Deno.cwd(); // `deno test` runs from the library directory
const DEFINITION = path.resolve(LIB, "src/migration/definition.ts");
const SCHEMA = path.resolve(LIB, "src/schema.ts");

const ROOT_ID = "2025_01_01_0000_AAAAAAAAAAAAAAAAAAAAAAAAAA@root";
const BROKEN_ID = "2025_01_02_0000_BBBBBBBBBBBBBBBBBBBBBBBBBB@broken";
const LEAF_ID = "2025_01_03_0000_CCCCCCCCCCCCCCCCCCCCCCCCCC@leaf";

const rootFile = `
import { migrationDefinition } from "${DEFINITION}";
import * as v from "${SCHEMA}";
export default migrationDefinition("${ROOT_ID}", "root", {
  parent: null,
  schemas: { collections: { users: { _id: v.string() } }, multiModels: {} },
  migrate(m) {
    m.createCollection("users");
    return m.compile();
  },
});
`;

// Broken, and broken LOCALLY: `tags` is declared but never created. Nothing
// it does corrupts the state its child inherits, so `leaf` stays valid on its
// own — which is what makes this a test of the window and not of the seeding.
const brokenFile = `
import { migrationDefinition } from "${DEFINITION}";
import * as v from "${SCHEMA}";
import root from "./${ROOT_ID}.ts";
export default migrationDefinition("${BROKEN_ID}", "broken", {
  parent: root,
  schemas: {
    collections: { users: { _id: v.string() }, tags: { _id: v.string() } },
    multiModels: {},
  },
  migrate(m) {
    return m.compile();
  },
});
`;

const leafFile = `
import { migrationDefinition } from "${DEFINITION}";
import * as v from "${SCHEMA}";
import broken from "./${BROKEN_ID}.ts";
export default migrationDefinition("${LEAF_ID}", "leaf", {
  parent: broken,
  schemas: {
    collections: { users: { _id: v.string() }, tags: { _id: v.string() } },
    multiModels: {},
  },
  migrate(m) {
    m.createCollection("tags");
    return m.compile();
  },
});
`;

const rootSchemas = `
import * as v from "${SCHEMA}";
export const schemas = { collections: { users: { _id: v.string() } }, multiModels: {} };
`;

const leafSchemas = `
import * as v from "${SCHEMA}";
export const schemas = {
  collections: { users: { _id: v.string() }, tags: { _id: v.string() } },
  multiModels: {},
};
`;

/**
 * A project directory per step. The schema-consistency gate reads
 * `schemas.ts` through a dynamic import, which Deno caches by path — the same
 * file rewritten in place between two runs would be read back stale.
 */
async function writeProject(
  files: Record<string, string>,
  schemas: string,
  dbName: string,
): Promise<string> {
  const dir = await Deno.makeTempDir({ prefix: "mongodbee_lastn_pending_" });
  const migrationsDir = path.join(dir, "migrations");
  await Deno.mkdir(migrationsDir);
  for (const [name, content] of Object.entries(files)) {
    await Deno.writeTextFile(path.join(migrationsDir, `${name}.ts`), content);
  }
  await Deno.writeTextFile(path.join(dir, "schemas.ts"), schemas);
  await Deno.writeTextFile(
    path.join(dir, "mongodbee.config.ts"),
    `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
  return dir;
}

Deno.test("migrate --last N: the window widens to cover every pending migration", async () => {
  const dbName = `mongodbee_test_lastn_${
    crypto.randomUUID().replace(/-/g, "").substring(0, 8)
  }`;
  const client = new MongoClient(TEST_MONGODB_URI);
  await client.connect();
  const db = client.db(dbName);
  await db.dropDatabase();

  const appliedDir = await writeProject(
    { [ROOT_ID]: rootFile },
    rootSchemas,
    dbName,
  );
  const fullDir = await writeProject(
    { [ROOT_ID]: rootFile, [BROKEN_ID]: brokenFile, [LEAF_ID]: leafFile },
    leafSchemas,
    dbName,
  );

  try {
    // The root lands first, so it is history by the time the other two become
    // pending — exactly the situation `--last N` is reached for.
    await migrateCommand({ cwd: appliedDir, force: true });
    assertEquals((await getAppliedMigrationIds(db)).length, 1);

    // Negative control: `leaf` on its own IS valid. Whatever blocks the
    // migrate below can only be the window reaching past it.
    const chain = buildMigrationChain(
      await loadAllMigrations(path.join(fullDir, "migrations")),
    );
    assertEquals(chain.length, 3);
    const windowOnly = await validateMigrationsWithSimulation(chain, {
      lastN: 1,
    });
    assertEquals(windowOnly.map((r) => r.valid), [true]);

    // `--last 1` names only `leaf`. `broken` is pending right behind it and
    // is about to be applied, so the window has to widen to reach it.
    await assertRejects(
      () => migrateCommand({ cwd: fullDir, force: true, last: 1 }),
      Error,
      "Migration validation failed",
    );

    const after = await getAppliedMigrationIds(db);
    assertEquals(after.length, 1, "nothing may be applied after the gate");
    assert(!after.includes(BROKEN_ID));
    assert(!after.includes(LEAF_ID));
  } finally {
    try {
      await db.dropDatabase();
    } catch {
      // Ignore cleanup errors
    }
    await client.close();
    for (const dir of [appliedDir, fullDir]) {
      try {
        await Deno.remove(dir, { recursive: true });
      } catch {
        // Ignore cleanup errors
      }
    }
  }
});

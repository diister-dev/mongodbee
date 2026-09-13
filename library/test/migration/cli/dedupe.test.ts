import { test } from "../../+harness.ts";
import process from "node:process";
import { mkdir, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { assert, assertEquals, assertRejects } from "../../+assert.ts";
import { MongoClient } from "../../../src/mongodb.ts";
import { checkCommand } from "../../../src/migration/cli/commands/check.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { withTempDir } from "./shared.ts";

const TEST_MONGODB_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

const SEED_MIGRATION = `import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";
import { refId } from "@diister/mongodbee/ids";

export default migrationDefinition("2026_01_01_0000_DDP01", "seed", {
  parent: null,
  schemas: {
    scopedMultiCollections: {
      "+expositions": {
        scope: refId("exposition"),
        types: { user_role: { userId: refId("user"), roleId: refId("role") } },
      },
    },
  },
  migrate(migration) {
    return migration
      .createScopedMultiCollection("+expositions")
      .type("user_role")
      .seed("${EXPO_A}", [
        { _id: "user_role:a1", userId: "user:u1", roleId: "role:ORGANIZER" },
        { _id: "user_role:a2", userId: "user:u1", roleId: "role:ORGANIZER" },
        { _id: "user_role:a3", userId: "user:u1", roleId: "role:SECURITY" },
      ])
      .seed("${EXPO_B}", [
        { _id: "user_role:b1", userId: "user:u1", roleId: "role:ORGANIZER" },
      ])
      .end()
      .end()
      .compile();
  },
});
`;

const DECLARED_TYPES = `import * as v from "valibot";
import { defineType, unique } from "@diister/mongodbee";
import { refId } from "@diister/mongodbee/ids";

export const UserRole = defineType({
  schema: v.object({ userId: refId("user"), roleId: refId("role") }),
  indexes: (f) => [unique(f.userId, f.roleId)],
});

export const schemas = {
  scopedMultiCollections: {
    "+expositions": { scope: refId("exposition"), types: { user_role: UserRole } },
  },
};
`;

const DEDUPE_MIGRATION = `import { migrationDefinition } from "@diister/mongodbee/migration";
import parent from "./2026_01_01_0000_DDP01@seed.ts";
import { schemas } from "../schemas.ts";

export default migrationDefinition("2026_01_02_0000_DDP02", "dedupe_then_unique", {
  parent,
  schemas,
  migrate(migration) {
    return migration
      .scopedMultiCollection("+expositions")
      .type("user_role")
      .dedupe({ by: ["userId", "roleId"], keep: "first" })
      .end()
      .end()
      .compile();
  },
});
`;

test({
  name: "dedupe end to end: a dirty corpus is cleaned before the unique index lands",
  timeout: 120_000,
  fn: async () => {
    await withTempDir(async (tempDir) => {
      const dbName = `mongodbee_test_dedupe_${crypto
        .randomUUID()
        .replace(/-/g, "")
        .substring(0, 8)}`;
      const client = new MongoClient(TEST_MONGODB_URI);
      await client.connect();
      const db = client.db(dbName);
      await db.dropDatabase();

      try {
        await mkdir(path.join(tempDir, "migrations"), { recursive: true });
        await writeFile(
          path.join(tempDir, "mongodbee.config.ts"),
          `export default { database: { connection: { uri: "${TEST_MONGODB_URI}" }, name: "${dbName}" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
        );
        await writeFile(path.join(tempDir, "schemas.ts"), DECLARED_TYPES);
        await writeFile(
          path.join(tempDir, "migrations", "2026_01_01_0000_DDP01@seed.ts"),
          SEED_MIGRATION,
        );
        await writeFile(
          path.join(
            tempDir,
            "migrations",
            "2026_01_02_0000_DDP02@dedupe_then_unique.ts",
          ),
          DEDUPE_MIGRATION,
        );

        await checkCommand({ cwd: tempDir, mode: "quick" });
        await migrateCommand({ cwd: tempDir, force: true });
        assertEquals((await getAppliedMigrationIds(db)).length, 2);

        type StoredRow = { _id: string; [field: string]: unknown };
        const expositions = db.collection<StoredRow>("+expositions");
        const survivors = (
          await expositions.find({ _type: "user_role" }).toArray()
        )
          .map((row) => row._id)
          .sort();
        assertEquals(survivors, [
          "user_role:a1",
          "user_role:a3",
          "user_role:b1",
        ]);

        const names = (await expositions.indexes()).map((i) => i.name);
        assert(
          names.includes("_scope__type_user_role__idx_userId_asc_roleId_asc"),
          `missing unique index: ${names.join(", ")}`,
        );

        await assertRejects(
          () =>
            expositions.insertOne({
              _id: "user_role:a9",
              _type: "user_role",
              _scope: EXPO_A,
              userId: "user:u1",
              roleId: "role:ORGANIZER",
            }),
          Error,
          "E11000",
        );
      } finally {
        await db.dropDatabase();
        await client.close();
      }
    });
  },
});

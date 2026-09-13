import { test } from "../../+harness.ts";
import process from "node:process";
import { mkdir, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { assert, assertEquals, assertRejects } from "../../+assert.ts";
import { MongoClient } from "../../../src/mongodb.ts";
import { checkCommand } from "../../../src/migration/cli/commands/check.ts";
import { migrateCommand } from "../../../src/migration/cli/commands/migrate.ts";
import { rollbackCommand } from "../../../src/migration/cli/commands/rollback.ts";
import { checkMigrationStatus } from "../../../src/migration/check-status.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { withTempDir } from "./shared.ts";

const TEST_MONGODB_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";

const IMPORTS = `import * as v from "valibot";
import { defineType, desc, index, unique } from "@diister/mongodbee";
import { dbId, refId } from "@diister/mongodbee/ids";
`;

const TYPES = `
export const UserRole = defineType({
  schema: v.object({
    userId: refId("user"),
    roleId: refId("role"),
    status: v.picklist(["active", "revoked"]),
    addedAt: v.date(),
  }),
  indexes: (f) => [
    unique(f.userId, f.roleId).where(f.status, "active").named("active_role"),
    index(f.roleId, desc(f.addedAt), desc(f._id)).named("roster"),
  ],
});

export const AuditEvent = defineType({
  schema: v.object({
    _id: dbId("audit_event"),
    actorId: refId("user"),
    at: v.date(),
    kind: v.string(),
  }),
  indexes: (f) => [index(f.actorId, desc(f.at)).named("by_actor")],
});

export const schemas = {
  collections: {
    users: { _id: dbId("user"), name: v.string() },
    audit_event: AuditEvent,
  },
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: { user_role: UserRole },
    },
  },
};
`;

const SCHEMAS_FILE = `${IMPORTS}${TYPES}`;

const MIGRATION_FILE = `import { migrationDefinition } from "@diister/mongodbee/migration";
${IMPORTS}${TYPES}
export default migrationDefinition("2026_01_01_0000_DEFTYPE01", "initial", {
  parent: null,
  schemas,
  migrate(migration) {
    migration.createCollection("users");
    migration.createCollection("audit_event");
    migration.createScopedMultiCollection("+expositions");
    return migration.compile();
  },
});
`;

test({
  name: "defineType end to end: check, migrate, live constraint, index audit, rollback",
  timeout: 120_000,
  fn: async () => {
    await withTempDir(async (tempDir) => {
      const dbName = `mongodbee_test_definetype_${crypto
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
        await writeFile(path.join(tempDir, "schemas.ts"), SCHEMAS_FILE);
        await writeFile(
          path.join(
            tempDir,
            "migrations",
            "2026_01_01_0000_DEFTYPE01@initial.ts",
          ),
          MIGRATION_FILE,
        );

        await checkCommand({ cwd: tempDir, mode: "quick" });

        await migrateCommand({ cwd: tempDir, force: true });
        assertEquals((await getAppliedMigrationIds(db)).length, 1);

        const expoIndexes = (await db.collection("+expositions").indexes()).map(
          (i) => i.name,
        );
        assert(
          expoIndexes.includes("_scope__type_user_role__idx_active_role"),
          `missing active_role: ${expoIndexes.join(", ")}`,
        );
        assert(
          expoIndexes.includes("_scope__type_user_role__idx_roster"),
          `missing roster: ${expoIndexes.join(", ")}`,
        );

        const auditIndexes = (await db.collection("audit_event").indexes()).map(
          (i) => i.name,
        );
        assert(
          auditIndexes.includes("_idx_by_actor"),
          `missing by_actor: ${auditIndexes.join(", ")}`,
        );

        type StoredRow = { _id: string; [field: string]: unknown };
        const assignment: StoredRow = {
          _id: "user_role:a1",
          _type: "user_role",
          _scope: "exposition:e1",
          userId: "user:u1",
          roleId: "role:ORGANIZER",
          status: "active",
          addedAt: new Date("2026-09-01T10:00:00Z"),
        };
        const expositions = db.collection<StoredRow>("+expositions");
        await expositions.insertOne(assignment);
        await assertRejects(
          () => expositions.insertOne({ ...assignment, _id: "user_role:a2" }),
          Error,
          "E11000",
          "the migrated composite unique must reject the duplicate assignment",
        );
        await expositions.insertOne({
          ...assignment,
          _id: "user_role:a3",
          status: "revoked",
        });
        assertEquals(
          await expositions.countDocuments({ _type: "user_role" }),
          2,
        );

        const healthy = await checkMigrationStatus({
          cwd: tempDir,
          db,
          strictValidation: false,
        });
        assertEquals(healthy.indexes?.areIndexesValid, true);

        await db.collection("audit_event").dropIndex("_idx_by_actor");
        const degraded = await checkMigrationStatus({
          cwd: tempDir,
          db,
          strictValidation: false,
        });
        assertEquals(degraded.indexes?.areIndexesValid, false);
        assert(
          degraded.indexes?.issues.some(
            (issue) => issue.type === "missing" && issue.path === "by_actor",
          ),
          JSON.stringify(degraded.indexes?.issues),
        );

        await rollbackCommand({ cwd: tempDir, force: true });
        assertEquals((await getAppliedMigrationIds(db)).length, 0);
      } finally {
        await db.dropDatabase();
        await client.close();
      }
    });
  },
});

import { assert, assertEquals, assertRejects } from "@std/assert";
import { MongoClient } from "../../../src/mongodb.ts";
import { classifyCommand } from "../../../src/migration/cli/commands/classify.ts";
import { seedCommand } from "../../../src/migration/cli/commands/seed.ts";
import { extractCommand } from "../../../src/migration/cli/commands/extract.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { withTempDir } from "./shared.ts";

const TEST_URI = Deno.env.get("TEST_MONGODB_URI") ||
  Deno.env.get("MONGODBEE_TEST_URI") ||
  "mongodb://localhost:27017";
const SRC = new URL("../../../src/", import.meta.url).href;

const BIRTH = "2026_01_01_0900_BIRTH01@birth";
const VERSION = "2026_02_01_0900_VERSN01@version";
const ADMIN = "user:01j5zk3v8n2q4x6y8z0b1c3d5e";

const SHARED = `
import * as v from "${SRC}schema.ts";
import { dbId, refId } from "${SRC}ids.ts";
import { mention, personal, personId } from "${SRC}privacy/mod.ts";
export const EmailSchema = personal(v.pipe(v.string(), v.email()), { role: "direct", consistent: "person" });
export const users = { _id: personId("user"), email: EmailSchema, firstname: personal(v.pipe(v.string(), v.minLength(2)), { role: "direct" }), role: v.picklist(["admin", "member"]) };
export const expositions = { _id: refId("exposition"), name: v.pipe(v.string(), v.minLength(1)), createdBy: mention(refId("user")) };
export const participantV1 = { _id: personId("participant", { of: ["user"] }), userId: refId("user"), kind: v.picklist(["visitor", "exhibitor"]) };
export const participantV2 = { ...participantV1, versionId: personal(v.string(), { role: "technical" }) };
export { v, refId, dbId, personId, personal, mention };
`;

function migrationFile(
  id: string,
  name: string,
  parentFile: string | null,
  participant: "participantV1" | "participantV2",
  body: string,
): string {
  return `
import { migrationDefinition } from "${SRC}migration/definition.ts";
import { users, expositions, ${participant}, refId } from "../lib.ts";
${parentFile ? `import parent from "./${parentFile}";` : ""}
export default migrationDefinition(${JSON.stringify(id)}, ${
    JSON.stringify(name)
  }, {
  parent: ${parentFile ? "parent" : "null"},
  schemas: {
    collections: { "+users": users, expositions },
    scopedMultiCollections: { "+expo": { scope: refId("exposition"), types: { participant: ${participant} } } },
  },
  migrate: (b) => ${body},
});
`;
}

async function writeProject(dir: string, dbName: string): Promise<void> {
  await Deno.mkdir(`${dir}/migrations`, { recursive: true });
  await Deno.writeTextFile(
    `${dir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: ${
      JSON.stringify(TEST_URI)
    } }, name: ${
      JSON.stringify(dbName)
    } }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
  await Deno.writeTextFile(`${dir}/lib.ts`, SHARED);
  await Deno.writeTextFile(
    `${dir}/migrations/${BIRTH}.ts`,
    migrationFile(BIRTH, "birth", null, "participantV1", "b.compile()"),
  );
  await Deno.writeTextFile(
    `${dir}/migrations/${VERSION}.ts`,
    migrationFile(
      VERSION,
      "version",
      `${BIRTH}.ts`,
      "participantV2",
      `b.scopedMultiCollection("+expo").type("participant").transform({
      up: (doc, ctx) => ({ ...doc, versionId: ctx.newId() }),
      down: (doc) => { const { versionId: _v, ...rest } = doc; return rest; },
    }).end().end().compile()`,
    ),
  );
  await Deno.writeTextFile(
    `${dir}/schemas.ts`,
    `
import { users, expositions, participantV2, refId } from "./lib.ts";
export const schemas = {
  collections: { "+users": users, expositions },
  scopedMultiCollections: { "+expo": { scope: refId("exposition"), types: { participant: participantV2 } } },
};
`,
  );
  await Deno.writeTextFile(
    `${dir}/scenario.ts`,
    `
export const scenario = {
  name: "salon",
  birth: ${JSON.stringify(BIRTH)},
  anchors: { collections: { "+users": [{ _id: ${
      JSON.stringify(ADMIN)
    }, email: "admin@diister.fr", firstname: "Admin", role: "admin" }] } },
  shape: { "+users": 9, expositions: 2, participant: { per: "scope", count: 5 } },
};
`,
  );
}

function dbName(tag: string): string {
  return `mongodbee_test_privacy_${tag}_${
    crypto.randomUUID().replace(/-/g, "").slice(0, 8)
  }`;
}

Deno.test("cli: classify reports the plan of the project schemas and fails on classification errors", async () => {
  await withTempDir(async (dir) => {
    await writeProject(dir, dbName("classify"));
    await classifyCommand({ cwd: dir });
    await classifyCommand({ cwd: dir, at: BIRTH, json: true });
  });
  await withTempDir(async (dir) => {
    await writeProject(dir, dbName("classify"));
    await Deno.writeTextFile(
      `${dir}/schemas.ts`,
      `
import { v, dbId } from "./lib.ts";
export const schemas = { collections: { users: { _id: dbId("user"), email: v.pipe(v.string(), v.email()) } } };
`,
    );
    await assertRejects(
      () => classifyCommand({ cwd: dir, json: true }),
      Error,
      "classification has 1 error",
    );
  });
});

Deno.test("cli: seed writes the world at the head and baselines the ledger, extract copies it pseudonymised", async () => {
  await withTempDir(async (dir) => {
    const source = dbName("seed");
    const target = dbName("extract");
    await writeProject(dir, source);
    const client = new MongoClient(TEST_URI);
    await client.connect();
    try {
      await seedCommand({ cwd: dir, scenario: "./scenario.ts", dryRun: true });
      await seedCommand({ cwd: dir, scenario: "./scenario.ts", json: true });
      const db = client.db(source);
      const users = await db.collection("+users").find({}).toArray();
      const expo = await db.collection("+expo").find({}).toArray();
      assertEquals(users.length, 10);
      assert(users.some((u) => String(u._id) === ADMIN));
      assertEquals(expo.filter((d) => d._type === "participant").length, 10);
      for (const p of expo) {
        assert(
          typeof p.versionId === "string",
          "the version migration must have been replayed",
        );
      }
      assertEquals(await getAppliedMigrationIds(db), [BIRTH, VERSION]);
      await assertRejects(
        () => seedCommand({ cwd: dir, scenario: "./scenario.ts", json: true }),
        Error,
        "already holds",
      );

      await assertRejects(
        () =>
          extractCommand({
            cwd: dir,
            fromDb: source,
            toDb: source,
            secret: "s3cret",
            json: true,
          }),
        Error,
        "own source",
      );
      await extractCommand({
        cwd: dir,
        fromDb: source,
        toDb: target,
        secret: "s3cret",
        shiftDays: 2,
        json: true,
      });
      const out = client.db(target);
      const outUsers = await out.collection("+users").find({}).toArray();
      const outExpo = await out.collection("+expo").find({}).toArray();
      assertEquals(outUsers.length, 10);
      assertEquals(outExpo.length, 10);
      const sourceIds = new Set(users.map((u) => String(u._id)));
      const sourceEmails = new Set(users.map((u) => u.email));
      for (const u of outUsers) {
        assert(!sourceIds.has(String(u._id)), "ids must be remapped");
        assert(!sourceEmails.has(u.email), "emails must be pseudonymised");
      }
      const outUserIds = new Set(outUsers.map((u) => String(u._id)));
      const outExpoIds = new Set(
        (await out.collection("expositions").find({}).toArray()).map((e) =>
          String(e._id)
        ),
      );
      for (const p of outExpo) {
        assert(
          outUserIds.has(String(p.userId)),
          "participant.userId must still join a user",
        );
        assert(
          outExpoIds.has(String(p._scope)),
          "_scope must still join an exposition",
        );
        assertEquals(p.kind === "visitor" || p.kind === "exhibitor", true);
      }
      assertEquals(await getAppliedMigrationIds(out), [BIRTH, VERSION]);
    } finally {
      await client.db(source).dropDatabase();
      await client.db(target).dropDatabase();
      await client.close();
    }
  });
});

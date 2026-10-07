import { mkdir, writeFile } from "node:fs/promises";
import process from "node:process";
import { assert, assertEquals } from "../../+assert.ts";
import { test } from "../../+harness.ts";
import { newId } from "../../../src/ids.ts";
import { extractCommand } from "../../../src/migration/cli/commands/extract.ts";
import { getAppliedMigrationIds } from "../../../src/migration/state.ts";
import { MongoClient } from "../../../src/mongodb.ts";
import { withTempDir } from "./shared.ts";

const TEST_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";
const SRC = new URL("../../../src/", import.meta.url).href;
const BIRTH = "2026_01_01_0900_BIRTH01@birth";
const VERSION = "2026_02_01_0900_VERSN01@version";

const SHARED = `
import * as v from "${SRC}schema.ts";
import { refId } from "${SRC}ids.ts";
import { personal, personId } from "${SRC}privacy/mod.ts";
export const users = { _id: personId("user"), email: personal(v.pipe(v.string(), v.email()), { role: "direct", consistent: "person" }) };
export const participantV1 = { _id: personId("participant", { of: ["user"] }), userId: refId("user") };
export const participantV2 = { ...participantV1, versionId: personal(v.string(), { role: "technical" }) };
export { refId };
`;

function migrationFile(
  id: string,
  parentFile: string | null,
  participant: "participantV1" | "participantV2",
  body: string,
): string {
  return `
import { migrationDefinition } from "${SRC}migration/definition.ts";
import { users, ${participant} } from "../lib.ts";
${parentFile ? `import parent from "./${parentFile}";` : ""}
export default migrationDefinition(${JSON.stringify(id)}, "step", {
  parent: ${parentFile ? "parent" : "null"},
  schemas: { collections: { users, participants: ${participant} } },
  migrate: (b) => ${body},
});
`;
}

test({
  name: "extract: --from-migration replays the later migrations in memory before pseudonymising, and baselines the whole chain",
  timeout: 60_000,
  fn: async () => {
    await withTempDir(async (dir) => {
      const tag = crypto.randomUUID().replace(/-/g, "").slice(0, 8);
      const source = `mongodbee_test_replay_src_${tag}`;
      const target = `mongodbee_test_replay_dst_${tag}`;
      await mkdir(`${dir}/migrations`, { recursive: true });
      await writeFile(
        `${dir}/mongodbee.config.ts`,
        `export default { database: { connection: { uri: ${JSON.stringify(TEST_URI)} } }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
      );
      await writeFile(`${dir}/lib.ts`, SHARED);
      await writeFile(
        `${dir}/migrations/${BIRTH}.ts`,
        migrationFile(BIRTH, null, "participantV1", "b.compile()"),
      );
      await writeFile(
        `${dir}/migrations/${VERSION}.ts`,
        migrationFile(
          VERSION,
          `${BIRTH}.ts`,
          "participantV2",
          `b.collection("participants").transform({
            up: (doc, ctx) => ({ ...doc, versionId: ctx.newId() }),
            down: (doc) => { const { versionId: _v, ...rest } = doc; return rest; },
          }).end().compile()`,
        ),
      );
      const client = new MongoClient(TEST_URI);
      await client.connect();
      try {
        const userId = `user:${newId()}`;
        await client
          .db(source)
          .collection<{ _id: string; [key: string]: unknown }>("users")
          .insertOne({ _id: userId, email: "replay.real@acme-corp.example" });
        await client
          .db(source)
          .collection<{ _id: string; [key: string]: unknown }>("participants")
          .insertOne({ _id: `participant:${newId()}`, userId });
        await extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          fromMigration: BIRTH,
          secret: "replay-secret",
          json: true,
        });
        const out = client.db(target);
        const [participant] = await out
          .collection("participants")
          .find({})
          .toArray();
        assert(
          typeof participant.versionId === "string",
          "the version migration must have been replayed",
        );
        const [user] = await out.collection("users").find({}).toArray();
        assertEquals(participant.userId, user._id);
        assertEquals(await getAppliedMigrationIds(out), [BIRTH, VERSION]);
      } finally {
        await client.db(source).dropDatabase();
        await client.db(target).dropDatabase();
        await client.close();
      }
    });
  },
});

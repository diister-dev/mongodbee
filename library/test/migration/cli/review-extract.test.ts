import { mkdir, writeFile } from "node:fs/promises";
import process from "node:process";
import { newId } from "../../../src/ids.ts";
import { extractCommand } from "../../../src/migration/cli/commands/extract.ts";
import { markMigrationAsAdopted } from "../../../src/migration/state.ts";
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
const VERSION = "2026_02_01_0900_VERSN01@version";

const SHARED = `
import * as v from "${SRC}schema.ts";
import { refId } from "${SRC}ids.ts";
import { withIndex } from "${SRC}indexes.ts";
import { personal, personId } from "${SRC}privacy/mod.ts";
export const users = {
  _id: personId("user"),
  login: withIndex(personal(v.string(), { role: "direct" }), { unique: true }),
};
export const participantV1 = { _id: personId("participant", { of: ["user"] }), userId: refId("user") };
export const participantV2 = { ...participantV1, versionId: personal(v.string(), { role: "technical" }) };
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

async function writeProject(dir: string, withVersion: boolean): Promise<void> {
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
  if (withVersion) {
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
  }
}

function names() {
  const tag = crypto.randomUUID().replace(/-/g, "").slice(0, 8);
  return {
    source: `mongodbee_test_review_src_${tag}`,
    target: `mongodbee_test_review_dst_${tag}`,
  };
}

type Raw = { _id: string; [key: string]: unknown };

test({
  // TODO(privacy): C8, extract must read the source ledger and default --from-migration to its last applied id (or refuse when it is behind the chain head)
  ignore: true,
  name: "C8 extract: a source behind the chain head is replayed, not baselined at head with old-shaped documents",
  timeout: 60_000,
  fn: async () => {
    await withTempDir(async (dir) => {
      await writeProject(dir, true);
      const { source, target } = names();
      const client = new MongoClient(TEST_URI);
      await client.connect();
      try {
        const src = client.db(source);
        const userId = `user:${newId()}`;
        await src
          .collection<Raw>("users")
          .insertOne({ _id: userId, login: "alice" });
        await src
          .collection<Raw>("participants")
          .insertOne({ _id: `participant:${newId()}`, userId });
        await markMigrationAsAdopted(src, BIRTH, "step");
        let refused = false;
        try {
          await extractCommand({
            cwd: dir,
            fromDb: source,
            toDb: target,
            secret: "review-secret",
            json: true,
          });
        } catch {
          refused = true;
        }
        if (!refused) {
          const [participant] = await client
            .db(target)
            .collection("participants")
            .find({})
            .toArray();
          assert(
            typeof participant.versionId === "string",
            "source ledger is at BIRTH, target ledger says VERSION, but VERSION never ran on the data",
          );
        }
      } finally {
        await client.db(source).dropDatabase();
        await client.db(target).dropDatabase();
        await client.close();
      }
    });
  },
});

test({
  // TODO(privacy): C2+C9, unique pseudonyms fold case (C2); populateDatabase must also undo inserts into collections that pre-existed empty
  ignore: true,
  name: "C2+C9 extract: logins differing only by case extract cleanly, and a failed write leaves the target empty",
  timeout: 60_000,
  fn: async () => {
    await withTempDir(async (dir) => {
      await writeProject(dir, false);
      const { source, target } = names();
      const client = new MongoClient(TEST_URI);
      await client.connect();
      try {
        const src = client.db(source);
        const ids = [`user:${newId()}`, `user:${newId()}`, `user:${newId()}`];
        await src.collection<Raw>("users").insertMany([
          { _id: ids[0], login: "Bob" },
          { _id: ids[1], login: "bob" },
          { _id: ids[2], login: "carol" },
        ]);
        await client.db(target).createCollection("users");
        let error: unknown;
        try {
          await extractCommand({
            cwd: dir,
            fromDb: source,
            toDb: target,
            secret: "review-secret",
            json: true,
          });
        } catch (e) {
          error = e;
        }
        const left = await client
          .db(target)
          .collection("users")
          .countDocuments();
        assertEquals(
          { error: error instanceof Error ? error.message : undefined, left },
          { error: undefined, left: 3 },
        );
      } finally {
        await client.db(source).dropDatabase();
        await client.db(target).dropDatabase();
        await client.close();
      }
    });
  },
});

test({
  // TODO(privacy): C12, extract passes timeShiftMs = 0 when --shift-days is absent, so the strict posture's secret-derived shift never applies (leak L12 via the CLI); pass undefined unless --shift-days is given
  ignore: true,
  name: "C12 extract: the strict default posture shifts ulid timestamps like the library does",
  timeout: 60_000,
  fn: async () => {
    const { decodeTime } = await import("../../../src/utils/ulid.ts");
    await withTempDir(async (dir) => {
      await writeProject(dir, false);
      const { source, target } = names();
      const client = new MongoClient(TEST_URI);
      await client.connect();
      try {
        const userId = `user:${newId()}`;
        await client
          .db(source)
          .collection<Raw>("users")
          .insertOne({ _id: userId, login: "alice" });
        await extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: "review-secret",
          json: true,
        });
        const [user] = await client
          .db(target)
          .collection("users")
          .find({})
          .toArray();
        const time = (id: string) => decodeTime(id.split(":")[1].toUpperCase());
        assert(
          time(String(user._id)) !== time(userId),
          "strict extract kept the exact creation millisecond of every id",
        );
      } finally {
        await client.db(source).dropDatabase();
        await client.db(target).dropDatabase();
        await client.close();
      }
    });
  },
});

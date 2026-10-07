import { mkdir, writeFile } from "node:fs/promises";
import process from "node:process";
import type { Db } from "../../../src/mongodb.ts";
import { newId } from "../../../src/ids.ts";
import { withTempDir } from "./shared.ts";

export const TEST_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";

const SRC = new URL("../../../src/", import.meta.url).href;

export const REAL = {
  emails: [
    "alice.real@acme-corp.example",
    "bob.real@acme-corp.example",
    "carol.real@acme-corp.example",
    "dave.real@acme-corp.example",
  ],
  firstnames: ["Alicereal", "Bobreal", "Carolreal", "Davereal"],
  badges: ["BADGE-REAL-1", "BADGE-REAL-2", "BADGE-REAL-3"],
  zoneLabels: ["Zone Secrète Réelle A", "Zone Secrète Réelle B"],
  expositionNames: ["Salon Réel 2026", "Salon Réel 2027"],
} as const;

export const SCHEMAS_SOURCE = `
import * as v from "${SRC}schema.ts";
import { dbId, refId } from "${SRC}ids.ts";
import { withIndex } from "${SRC}indexes.ts";
import { mention, personal, personId } from "${SRC}privacy/mod.ts";

export const EmailSchema = withIndex(
  personal(v.pipe(v.string(), v.email()), { role: "direct", consistent: "person" }),
  { unique: true },
);

export const schemas = {
  collections: {
    users: {
      _id: personId("user"),
      email: EmailSchema,
      firstname: personal(v.pipe(v.string(), v.minLength(2)), { role: "direct" }),
      role: v.picklist(["admin", "member"]),
    },
    expositions: {
      _id: refId("exposition"),
      name: v.pipe(v.string(), v.minLength(1)),
      createdBy: mention(refId("user")),
    },
  },
  multiCollections: {
    scans: {
      scan: {
        _id: refId("scan"),
        userId: refId("user"),
        expositionId: refId("exposition"),
        badge: personal(v.string(), { role: "content" }),
      },
      note: {
        _id: refId("note"),
        authorId: mention(refId("user")),
        participantId: v.optional(refId("participant")),
        flag: v.boolean(),
      },
    },
  },
  scopedMultiCollections: {
    expo: {
      scope: refId("exposition"),
      types: {
        participant: {
          _id: personId("participant", { of: ["user"] }),
          userId: refId("user"),
          kind: v.picklist(["visitor", "exhibitor"]),
        },
      },
    },
  },
  multiModels: {
    exposition: {
      zone: {
        _id: refId("zone"),
        ownerId: mention(refId("user")),
        label: v.string(),
      },
    },
  },
};
`;

export const BIRTH = "2026_01_01_0900_BIRTH01@birth";

export async function writeProject(dir: string): Promise<void> {
  await mkdir(`${dir}/migrations`, { recursive: true });
  await writeFile(
    `${dir}/migrations/${BIRTH}.ts`,
    `
import { migrationDefinition } from "${SRC}migration/definition.ts";
import { schemas } from "../schemas.ts";
export default migrationDefinition(${JSON.stringify(BIRTH)}, "birth", {
  parent: null,
  schemas,
  migrate: (b) => b.compile(),
});
`,
  );
  await writeFile(
    `${dir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: ${JSON.stringify(
      TEST_URI,
    )} }, name: "unused" }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
  await writeFile(`${dir}/schemas.ts`, SCHEMAS_SOURCE);
}

export function dbName(tag: string): string {
  return `mongodbee_test_privacy_${tag}_${crypto
    .randomUUID()
    .replace(/-/g, "")
    .slice(0, 8)}`;
}

export interface SourceWorld {
  readonly userIds: string[];
  readonly expositionIds: string[];
  readonly participantIds: string[];
  readonly instanceNames: string[];
}

export function rawCollection(db: Db, name: string) {
  return db.collection<{ _id: string; [key: string]: unknown }>(name);
}

export async function populateSource(db: Db): Promise<SourceWorld> {
  const userIds = REAL.emails.map(() => `user:${newId()}`);
  const expositionIds = REAL.expositionNames.map(() => `exposition:${newId()}`);
  const instanceNames = [...expositionIds];

  await rawCollection(db, "users").insertMany(
    userIds.map((_id, i) => ({
      _id,
      email: REAL.emails[i],
      firstname: REAL.firstnames[i],
      role: i === 0 ? "admin" : "member",
    })),
  );
  await rawCollection(db, "expositions").insertMany(
    expositionIds.map((_id, i) => ({
      _id,
      name: REAL.expositionNames[i],
      createdBy: userIds[0],
    })),
  );
  const participantIds = userIds.map(() => `participant:${newId()}`);
  await rawCollection(db, "expo").insertMany(
    userIds.map((userId, i) => ({
      _id: participantIds[i],
      _type: "participant",
      _scope: expositionIds[i % 2],
      userId,
      kind: i % 2 === 0 ? "visitor" : "exhibitor",
    })),
  );
  await rawCollection(db, "scans").insertMany([
    ...REAL.badges.map((badge, i) => ({
      _id: `scan:${newId()}`,
      _type: "scan",
      userId: userIds[i],
      expositionId: expositionIds[i % 2],
      badge,
    })),
    {
      _id: `note:${newId()}`,
      _type: "note",
      authorId: userIds[1],
      participantId: participantIds[1],
      flag: true,
    },
    { _id: `ghost:${newId()}`, _type: "ghost", secret: REAL.badges[0] },
  ]);
  for (const [index, name] of instanceNames.entries()) {
    const coll = rawCollection(db, name);
    await coll.insertMany([
      {
        _id: "_information",
        _type: "_information",
        collectionType: "exposition",
        createdAt: new Date("2026-01-01T00:00:00Z"),
      },
      {
        _id: "_migrations",
        _type: "_migrations",
        fromMigrationId: "2026_01_01_0900_BIRTH01@birth",
        mongodbeeVersion: "0.0.0-test",
        appliedMigrations: [
          {
            id: "2026_01_01_0900_BIRTH01@birth",
            operation: "applied",
            appliedAt: new Date("2026-01-01T00:00:00Z"),
            status: "success",
            mongodbeeVersion: "0.0.0-test",
          },
        ],
      },
      {
        _id: `zone:${newId()}`,
        _type: "zone",
        ownerId: userIds[index],
        label: REAL.zoneLabels[index],
      },
      {
        _id: `zone:${newId()}`,
        _type: "zone",
        ownerId: userIds[index + 1],
        label: `${REAL.zoneLabels[index]} bis`,
      },
    ]);
  }
  return { userIds, expositionIds, participantIds, instanceNames };
}

export async function withProject(
  work: (dir: string) => Promise<void>,
): Promise<void> {
  await withTempDir(async (dir) => {
    await writeProject(dir);
    await work(dir);
  });
}

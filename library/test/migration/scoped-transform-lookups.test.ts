/**
 * A scoped transform declaring `lookups` receives, per document, the document
 * of another physical collection its key names, and a lookup can follow the
 * previous one: a participant names its session, the session names its link
 * by code. A key that matches nothing yields `undefined` instead of failing,
 * so a repair over data whose references expired leaves those documents as
 * they are. Verified on both appliers, across batches.
 */
import { test } from "../+harness.ts";
import { assertEquals, assertRejects, assertThrows } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import type {
  TransformLookup,
  TransformScope,
} from "../../src/migration/types.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const EXPO = "exposition:expoaaaaa01";

const SCHEMAS = {
  collections: {
    sessions: { invitationCode: v.optional(v.string()) },
    links: { code: v.string(), ownerId: v.string() },
  },
  scopedMultiCollections: {
    expo: {
      scope: refId("exposition"),
      types: {
        participant: {
          sessionId: v.optional(v.string()),
          linkOwner: v.optional(v.string()),
        },
      },
    },
  },
};

const SESSIONS = [
  { _id: "session:s1", invitationCode: "CODE-A" },
  { _id: "session:s2", invitationCode: "CODE-B" },
  { _id: "session:s3" },
  { _id: "session:s4", invitationCode: "CODE-GONE" },
];

const LINKS = [
  { _id: "link:l1", code: "CODE-A", ownerId: "org:1" },
  { _id: "link:l2", code: "CODE-B", ownerId: "org:2" },
];

const PARTICIPANTS = [
  { sessionId: "session:s1" },
  { sessionId: "session:s2" },
  { sessionId: "session:s3" },
  { sessionId: "session:s4" },
  { sessionId: "session:expired" },
  {},
];

const EXPECTED_OWNERS = [
  ["-", null],
  ["session:expired", null],
  ["session:s1", "org:1"],
  ["session:s2", "org:2"],
  ["session:s3", null],
  ["session:s4", null],
];

const LOOKUPS: readonly TransformLookup[] = [
  {
    as: "session",
    collection: "sessions",
    key: (doc) =>
      typeof doc.sessionId === "string" ? doc.sessionId : undefined,
  },
  {
    as: "link",
    collection: "links",
    field: "code",
    key: (_doc, found) => {
      const code = found.session?.invitationCode;
      return typeof code === "string" ? code : undefined;
    },
  },
];

function withLinkOwner(doc: Record<string, unknown>, scope: TransformScope) {
  const owner = scope.found.link?.ownerId;
  return typeof owner === "string" ? { ...doc, linkOwner: owner } : doc;
}

function migrationV1() {
  return migrationDefinition("001", "create", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .createCollection("sessions")
        .end()
        .createCollection("links")
        .end()
        .createScopedMultiCollection("expo")
        .type("participant")
        .seed(EXPO, PARTICIPANTS)
        .end()
        .end()
        .compile(),
  });
}

function migrationV2(parent: ReturnType<typeof migrationDefinition>) {
  return migrationDefinition("002", "participant-link-owner", {
    parent,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .scopedMultiCollection("expo")
        .type("participant")
        .transform({
          lookups: LOOKUPS,
          up: withLinkOwner,
          down: ({ linkOwner: _drop, ...doc }) => doc,
        })
        .end()
        .end()
        .compile(),
  });
}

function ownersBySession(docs: readonly Record<string, unknown>[]) {
  return docs
    .filter((d) => d._type === "participant")
    .map((d) => [String(d.sessionId ?? "-"), d.linkOwner ?? null])
    .sort((a, b) => String(a[0]).localeCompare(String(b[0])));
}

test("memory: a lookup follows the previous one, and a missing reference leaves the document as it is", async () => {
  const state = createEmptyDatabaseState();
  const m1 = migrationV1();
  const m2 = migrationV2(m1);
  await createMemoryApplier(m1).applyMigration(
    state,
    m1.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
    "up",
  );
  state.collections.sessions.content.push(...SESSIONS);
  state.collections.links.content.push(...LINKS);

  const ops2 = m2.migrate(
    migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }),
  ).operations;
  await createMemoryApplier(m2).applyMigration(state, ops2, "up");
  assertEquals(
    ownersBySession(state.scopedMultiCollections.expo.content),
    EXPECTED_OWNERS,
  );

  await createMemoryApplier(m2).applyMigration(state, ops2, "down");
  assertEquals(
    ownersBySession(state.scopedMultiCollections.expo.content),
    EXPECTED_OWNERS.map(([session]) => [session, null]),
  );
});

test("mongodb: a lookup follows the previous one across batches, and a missing reference leaves the document as it is", async () => {
  await withDatabase("scoped-transform-lookups", async (db) => {
    const m1 = migrationV1();
    const m2 = migrationV2(m1);
    await createMongodbApplier(db, m1, {
      currentMigrationId: m1.id,
    }).applyMigration(
      m1.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
      "up",
    );
    await db.collection("sessions").insertMany(SESSIONS as never);
    await db.collection("links").insertMany(LINKS as never);

    const ops2 = m2.migrate(
      migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }),
    ).operations;
    await createMongodbApplier(db, m2, {
      currentMigrationId: m2.id,
      batchSize: 2,
    }).applyMigration(ops2, "up");

    const docs = (await db
      .collection("expo")
      .find({} as never)
      .toArray()) as Record<string, unknown>[];
    assertEquals(ownersBySession(docs), EXPECTED_OWNERS);
  });
});

test("a transform without lookups gets an empty found", async () => {
  const state = createEmptyDatabaseState();
  const m1 = migrationV1();
  await createMemoryApplier(m1).applyMigration(
    state,
    m1.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
    "up",
  );
  const m2 = migrationDefinition("002", "no-lookups", {
    parent: m1,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .scopedMultiCollection("expo")
        .type("participant")
        .transform({
          up: (doc, scope) => ({
            ...doc,
            foundKeys: Object.keys(scope.found),
          }),
          down: (doc) => doc,
        })
        .end()
        .end()
        .compile(),
  });
  await createMemoryApplier(m2).applyMigration(
    state,
    m2.migrate(migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }))
      .operations,
    "up",
  );
  for (const doc of state.scopedMultiCollections.expo.content) {
    assertEquals(doc.foundKeys, []);
  }
});

function migrationWithLookups(
  parent: ReturnType<typeof migrationDefinition>,
  lookups: readonly TransformLookup[],
) {
  return migrationDefinition("002", "lookups", {
    parent,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .scopedMultiCollection("expo")
        .type("participant")
        .transform({ lookups, up: withLinkOwner, down: (doc) => doc })
        .end()
        .end()
        .compile(),
  });
}

test("two lookups under one name are refused when the migration is built", () => {
  const m2 = migrationWithLookups(migrationV1(), [
    LOOKUPS[0],
    { ...LOOKUPS[1], as: "session" },
  ]);
  assertThrows(
    () =>
      m2.migrate(
        migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }),
      ),
    Error,
    'two lookups are named "session"',
  );
});

test("a key that throws names its lookup and the document it was computing", async () => {
  const state = createEmptyDatabaseState();
  const m1 = migrationV1();
  await createMemoryApplier(m1).applyMigration(
    state,
    m1.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
    "up",
  );
  const m2 = migrationWithLookups(m1, [
    {
      as: "session",
      collection: "sessions",
      key: () => {
        throw new Error("no session field");
      },
    },
  ]);
  const ops2 = m2.migrate(
    migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }),
  ).operations;
  const error = await assertRejects(
    async () => {
      await createMemoryApplier(m2).applyMigration(state, ops2, "up");
    },
    Error,
    'lookup "session" could not compute a key for document',
  );
  assertEquals(error.message.endsWith("no session field"), true);
});

test("a lookup into the collection the transform rewrites is refused when the migration is built", () => {
  const m2 = migrationWithLookups(migrationV1(), [
    { as: "other", collection: "expo", key: () => undefined },
  ]);
  assertThrows(
    () =>
      m2.migrate(
        migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }),
      ),
    Error,
    'lookup "other" reads "expo"',
  );
});

test("mongodb, not strict: a document whose key fails is skipped, the others are still rewritten", async () => {
  await withDatabase("scoped-transform-lookups-lenient", async (db) => {
    const m1 = migrationV1();
    await createMongodbApplier(db, m1, {
      currentMigrationId: m1.id,
    }).applyMigration(
      m1.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
      "up",
    );
    await db.collection("sessions").insertMany(SESSIONS as never);
    await db.collection("links").insertMany(LINKS as never);
    const m2 = migrationWithLookups(m1, [
      {
        as: "session",
        collection: "sessions",
        key: (doc) => {
          if (doc.sessionId === "session:s2") return { not: "a key" } as never;
          return typeof doc.sessionId === "string" ? doc.sessionId : undefined;
        },
      },
      LOOKUPS[1],
    ]);
    await createMongodbApplier(db, m2, {
      currentMigrationId: m2.id,
      strictValidation: false,
    }).applyMigration(
      m2.migrate(migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }))
        .operations,
      "up",
    );
    const docs = (await db
      .collection("expo")
      .find({} as never)
      .toArray()) as Record<string, unknown>[];
    const owners = Object.fromEntries(ownersBySession(docs));
    assertEquals(owners["session:s1"], "org:1");
    assertEquals(owners["session:s2"], null);
  });
});

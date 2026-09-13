import { test } from "../+harness.ts";
import { assertEquals, assertRejects, assertThrows } from "../+assert.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

const assignment = {
  userId: v.string(),
  roleId: v.string(),
  status: v.string(),
};

const SCHEMAS = {
  collections: {
    audit: {
      _id: v.string(),
      actorId: v.optional(v.string()),
      kind: v.string(),
    },
  },
  multiCollections: {
    roles: { assignment },
  },
  scopedMultiCollections: {
    "+expositions": { scope: refId("exposition"), types: { assignment } },
  },
};

function seeded() {
  return migrationDefinition("001", "seed", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .createCollection("audit")
        .seed([
          { _id: "audit:c1", actorId: "x", kind: "k" },
          { _id: "audit:c2", actorId: "x", kind: "k" },
          { _id: "audit:c3", actorId: "y", kind: "k" },
          { _id: "audit:c4", kind: "k" },
          { _id: "audit:c5", kind: "k" },
        ])
        .end()
        .createMultiCollection("roles")
        .type("assignment")
        .seed([
          {
            _id: "assignment:m1",
            userId: "u1",
            roleId: "ORG",
            status: "active",
          },
          {
            _id: "assignment:m2",
            userId: "u1",
            roleId: "ORG",
            status: "active",
          },
          {
            _id: "assignment:m3",
            userId: "u2",
            roleId: "ORG",
            status: "active",
          },
        ])
        .end()
        .end()
        .createScopedMultiCollection("+expositions")
        .type("assignment")
        .seed(EXPO_A, [
          {
            _id: "assignment:a1",
            userId: "u1",
            roleId: "ORG",
            status: "active",
          },
          {
            _id: "assignment:a2",
            userId: "u1",
            roleId: "ORG",
            status: "active",
          },
          {
            _id: "assignment:a3",
            userId: "u1",
            roleId: "ORG",
            status: "revoked",
          },
          {
            _id: "assignment:a4",
            userId: "u1",
            roleId: "SEC",
            status: "active",
          },
          {
            _id: "assignment:a5",
            userId: "u2",
            roleId: "ORG",
            status: "active",
          },
        ])
        .seed(EXPO_B, [
          {
            _id: "assignment:b1",
            userId: "u1",
            roleId: "ORG",
            status: "active",
          },
        ])
        .end()
        .end()
        .compile(),
  });
}

function deduped(parent: ReturnType<typeof migrationDefinition>) {
  return migrationDefinition("002", "dedupe", {
    parent,
    schemas: SCHEMAS,
    migrate: (b) =>
      b
        .scopedMultiCollection("+expositions")
        .type("assignment")
        .dedupe({ by: ["userId", "roleId"], where: { status: "active" } })
        .end()
        .end()
        .multiCollection("roles")
        .type("assignment")
        .dedupe({ by: ["userId", "roleId"], keep: "last" })
        .end()
        .end()
        .collection("audit")
        .dedupe({ by: ["actorId", "kind"] })
        .end()
        .compile(),
  });
}

function ids(docs: Record<string, unknown>[]): string[] {
  return docs.map((doc) => String(doc._id)).sort();
}

test("dedupe: memory applier keeps one document per key in each family", async () => {
  const state = createEmptyDatabaseState();
  const m1 = seeded();
  const m2 = deduped(m1);

  await createMemoryApplier(m1).applyMigration(
    state,
    m1.migrate(migrationBuilder({ schemas: SCHEMAS })).operations,
    "up",
  );

  const ops = m2.migrate(
    migrationBuilder({ schemas: SCHEMAS, parentSchemas: SCHEMAS }),
  ).operations;
  const applier = createMemoryApplier(m2);
  await applier.applyMigration(state, ops, "up");

  assertEquals(ids(state.scopedMultiCollections["+expositions"].content), [
    "assignment:a1",
    "assignment:a3",
    "assignment:a4",
    "assignment:a5",
    "assignment:b1",
  ]);
  assertEquals(ids(state.multiCollections.roles.content), [
    "assignment:m2",
    "assignment:m3",
  ]);
  assertEquals(ids(state.collections.audit.content), [
    "audit:c1",
    "audit:c3",
    "audit:c4",
    "audit:c5",
  ]);

  await assertRejects(
    () => applier.applyMigration(state, ops, "down"),
    Error,
    "irreversible",
  );
});

test("dedupe: the builder refuses an empty key and a reserved key", () => {
  const b = migrationBuilder({ schemas: SCHEMAS });
  assertThrows(
    () => b.collection("audit").dedupe({ by: [] }),
    Error,
    "at least one key",
  );
  assertThrows(
    () =>
      b
        .scopedMultiCollection("+expositions")
        .type("assignment")
        .dedupe({ by: ["_scope", "userId"] }),
    Error,
    "cannot be a dedupe key",
  );
  assertThrows(
    () =>
      b
        .scopedMultiCollection("+expositions")
        .type("assignment")
        .dedupe({ by: ["userId"], where: { _type: "assignment" } }),
    Error,
    "not allowed",
  );
});

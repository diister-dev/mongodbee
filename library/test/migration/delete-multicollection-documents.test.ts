import { assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import {
  getIrreversibleOperations,
  migrationBuilder,
} from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const SCHEMAS = {
  multiCollections: {
    "+tenants": {
      role: { _id: refId("role"), name: v.string(), isSystem: v.boolean() },
      member: { _id: refId("member"), name: v.string() },
    },
  },
};

function buildPair() {
  const parent = migrationDefinition("001", "seed-tenants", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) => {
      b.createMultiCollection("+tenants");
      b.multiCollection("+tenants")
        .type("role")
        .seed([
          { name: "owner", isSystem: true },
          { name: "admin", isSystem: true },
          { name: "custom", isSystem: false },
        ])
        .end()
        .type("member")
        .seed([{ name: "alice" }])
        .end();
      return b.compile();
    },
  });

  const child = migrationDefinition("002", "drop-system-roles", {
    parent,
    schemas: SCHEMAS,
    migrate: (b) => {
      b.multiCollection("+tenants").type("role").deleteWhere({ isSystem: true })
        .end();
      return b.compile();
    },
  });

  return { parent, child };
}

Deno.test("deleteWhere: memory applier drops only the matching documents of the type", async () => {
  const { parent, child } = buildPair();
  const parentOps = parent.migrate(
    migrationBuilder({ schemas: parent.schemas }),
  ).operations;
  const childCompiled = child.migrate(
    migrationBuilder({ schemas: child.schemas, parentSchemas: parent.schemas }),
  );

  const applier = createMemoryApplier(parent);
  let state = await applier.applyMigration(
    createEmptyDatabaseState(),
    parentOps,
    "up",
  );
  state = await applier.applyMigration(state, childCompiled.operations, "up");

  const content = state.multiCollections["+tenants"].content;
  assertEquals(
    content.filter((d) => d._type === "role").map((d) => d.name),
    ["custom"],
  );
  assertEquals(content.filter((d) => d._type === "member").length, 1);

  assertEquals(getIrreversibleOperations(childCompiled.operations).length, 1);
  await assertRejects(
    () => applier.applyMigration(state, childCompiled.operations, "down"),
    Error,
    "irreversible",
  );
});

Deno.test("deleteWhere: mongodb applier agrees with the memory semantics", async () => {
  await withDatabase("delete-multicollection-documents", async (db) => {
    const { parent, child } = buildPair();
    const parentOps = parent.migrate(
      migrationBuilder({ schemas: parent.schemas }),
    ).operations;
    const childOps = child.migrate(
      migrationBuilder({
        schemas: child.schemas,
        parentSchemas: parent.schemas,
      }),
    ).operations;

    const parentApplier = createMongodbApplier(db, parent, {
      currentMigrationId: parent.id,
    });
    await parentApplier.applyMigration(parentOps, "up");
    const tenants = db.collection("+tenants");
    assertEquals(await tenants.countDocuments(), 4);

    const childApplier = createMongodbApplier(db, child, {
      currentMigrationId: child.id,
    });
    await childApplier.applyMigration(childOps, "up");

    const roles = await tenants.find({ _type: "role" }).toArray();
    assertEquals(roles.map((d) => d.name), ["custom"]);
    assertEquals(await tenants.countDocuments({ _type: "member" }), 1);
  });
});

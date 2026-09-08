/**
 * `deleteType` on a scoped multi-collection: drops every document of the type
 * across ALL scopes (one physical collection), leaves sibling types intact,
 * marks the migration irreversible, and both appliers agree.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { assertRejects } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import {
  getIrreversibleOperations,
  migrationBuilder,
} from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import { dbId, refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const SCHEMAS = {
  scopedMultiCollections: {
    "+items": {
      scope: refId("expo"),
      types: {
        keep: { _id: dbId("keep"), label: v.string() },
        drop: { _id: dbId("drop"), note: v.string() },
      },
    },
  },
};

function buildPair() {
  const parent = migrationDefinition("001", "seed-items", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) => {
      b.createScopedMultiCollection("+items");
      b.scopedMultiCollection("+items")
        .type("keep")
        .seed("expo:a", [{ label: "ka" }])
        .seed("expo:b", [{ label: "kb" }])
        .end()
        .type("drop")
        .seed("expo:a", [{ note: "da" }])
        .seed("expo:b", [{ note: "db" }])
        .end();
      return b.compile();
    },
  });

  const child = migrationDefinition("002", "drop-type", {
    parent,
    schemas: {
      scopedMultiCollections: {
        "+items": {
          scope: refId("expo"),
          types: { keep: SCHEMAS.scopedMultiCollections["+items"].types.keep },
        },
      },
    },
    migrate: (b) => {
      b.scopedMultiCollection("+items").deleteType("drop").end();
      return b.compile();
    },
  });

  return { parent, child };
}

test("deleteType (scoped): memory applier drops the type across all scopes, siblings survive", async () => {
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

  const content = state.scopedMultiCollections["+items"].content;
  assertEquals(
    content.filter((d) => d._type === "drop"),
    [],
  );
  const kept = content.filter((d) => d._type === "keep");
  assertEquals(kept.length, 2);
  assertEquals(
    new Set(kept.map((d) => d._scope)),
    new Set(["expo:a", "expo:b"]),
  );

  // Irreversible: marked on the migration, and rollback refuses up front.
  assertEquals(getIrreversibleOperations(childCompiled.operations).length, 1);
  await assertRejects(
    () => applier.applyMigration(state, childCompiled.operations, "down"),
    Error,
    "irreversible",
  );
});

test("deleteType (scoped): mongodb applier agrees with the memory semantics", async () => {
  await withDatabase("delete-scoped-type", async (db) => {
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
    const items = db.collection("+items");
    assertEquals(await items.countDocuments(), 4);

    const childApplier = createMongodbApplier(db, child, {
      currentMigrationId: child.id,
    });
    await childApplier.applyMigration(childOps, "up");

    assertEquals(await items.countDocuments({ _type: "drop" }), 0);
    const kept = await items.find({ _type: "keep" }).toArray();
    assertEquals(kept.length, 2);
    assertEquals(
      new Set(kept.map((d) => d._scope as string)),
      new Set(["expo:a", "expo:b"]),
    );
    assert(kept.every((d) => typeof d.label === "string"));
  });
});

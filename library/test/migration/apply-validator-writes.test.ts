/**
 * A migration switches off only the validators of the collections it writes
 * into, and restores only the validators that differ from the target. It
 * used to switch off and back on EVERY declared collection on every
 * migration: two collMods per collection per migration were most of an
 * apply's time on a fresh database. The database it leaves is the same.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { TEST_URI } from "../+shared.ts";
import { MongoClient } from "../../src/mongodb.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import type { MigrationDefinition } from "../../src/migration/types.ts";
import * as v from "../../src/schema.ts";

const V1 = {
  collections: {
    written: { name: v.string() },
    untouched: { label: v.string() },
  },
};
const V2 = {
  collections: {
    written: { name: v.string(), flag: v.boolean() },
    untouched: { label: v.string() },
  },
};

function chain(): [MigrationDefinition, MigrationDefinition] {
  const m1 = migrationDefinition("001", "init", {
    parent: null,
    schemas: V1,
    migrate: (b) =>
      b
        .createCollection("written")
        .end()
        .createCollection("untouched")
        .end()
        .compile(),
  });
  const m2 = migrationDefinition("002", "flag", {
    parent: m1,
    schemas: V2,
    migrate: (b) =>
      b
        .collection("written")
        .transform({
          up: (doc) => ({ ...doc, flag: true }),
          down: ({ flag: _flag, ...doc }) => doc,
        })
        .end()
        .compile(),
  });
  return [m1, m2];
}

async function apply(
  db: import("../../src/mongodb.ts").Db,
  m: MigrationDefinition,
) {
  const ops = m.migrate(
    migrationBuilder({ schemas: m.schemas, parentSchemas: m.parent?.schemas }),
  ).operations;
  await createMongodbApplier(db, m).applyMigration(ops, "up");
}

test("apply: only the collections a migration writes lose their validator, and the result is unchanged", async () => {
  const client = new MongoClient(TEST_URI, { monitorCommands: true });
  const collMods: string[] = [];
  client.on("commandStarted", (event) => {
    if (event.commandName === "collMod") {
      collMods.push(String(event.command.collMod));
    }
  });
  const db = client.db(`@TEST_applywrites@${crypto.randomUUID().slice(0, 8)}`);
  try {
    const [m1, m2] = chain();
    await apply(db, m1);
    await db.collection("written").insertOne({ name: "a" } as never);
    await db.collection("untouched").insertOne({ label: "b" } as never);

    collMods.length = 0;
    await apply(db, m2);

    assertEquals(collMods.includes("untouched"), false, collMods.join(", "));
    assert(collMods.includes("written"), collMods.join(", "));

    const options = Object.fromEntries(
      (await db.listCollections().toArray()).map((c) => [
        c.name,
        (c as { options?: Record<string, unknown> }).options,
      ]),
    );
    assertEquals(options.written?.validationLevel, "strict");
    assertEquals(options.untouched?.validationLevel, "strict");
    const required = (name: string) => {
      const validator = options[name]?.validator as
        | { $jsonSchema: { required?: string[] } }
        | undefined;
      return [...(validator?.$jsonSchema.required ?? [])].sort();
    };
    assertEquals(required("written"), ["flag", "name"]);
    assertEquals(required("untouched"), ["label"]);
    assertEquals<unknown>(
      await db.collection("written").findOne({}, { projection: { _id: 0 } }),
      { name: "a", flag: true },
    );
  } finally {
    await db.dropDatabase();
    await client.close();
  }
});

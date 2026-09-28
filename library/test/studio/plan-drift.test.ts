import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import type { Db } from "../../src/mongodb.ts";
import * as v from "../../src/schema.ts";
import { refId } from "../../src/ids.ts";
import { index, unique } from "../../src/indexes.ts";
import { defineType } from "../../src/type-definition.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import { recordOperation } from "../../src/migration/history.ts";
import type {
  MigrationDefinition,
  SchemasDefinition,
} from "../../src/migration/types.ts";
import type { StudioContext } from "../../src/studio/context.ts";
import { getPlan } from "../../src/studio/api/plan.ts";
import { getDrift, schemaDrift } from "../../src/studio/api/drift.ts";
import { getHistory } from "../../src/studio/api/history.ts";
import { getCollectionSchema } from "../../src/studio/api/schema.ts";
import { getIndexReport } from "../../src/studio/api/indexes.ts";

const Users = defineType({
  schema: v.object({ name: v.string(), team: v.string() }),
  indexes: (f) => [index(f.team)],
});

const UsersUnique = defineType({
  schema: v.object({ name: v.string(), team: v.string() }),
  indexes: (f) => [index(f.team), unique(f.name)],
});

const catalog = {
  category: { label: v.string() },
  product: { title: v.string(), categoryId: refId("category") },
};

const V1: SchemasDefinition = {
  collections: { users: Users },
  multiCollections: { catalog },
};

const V2: SchemasDefinition = {
  collections: { users: UsersUnique },
  multiCollections: { catalog },
};

function chain(): MigrationDefinition[] {
  const first = migrationDefinition("001", "initial", {
    parent: null,
    schemas: V1,
    migrate: (m) =>
      m
        .createCollection("users")
        .seed([
          { name: "Ada", team: "core" },
          { name: "Ada", team: "infra" },
          { name: "Bob", team: "core" },
          { name: "Bob", team: "core" },
          { name: "Cy", team: "core" },
        ])
        .end()
        .createMultiCollection("catalog")
        .type("category")
        .seed([
          { _id: "category:books", label: "Books" },
          { _id: "category:music", label: "Music" },
        ])
        .end()
        .type("product")
        .seed([
          { title: "A", categoryId: "category:books" },
          { title: "B", categoryId: "category:books" },
          { title: "C", categoryId: "category:music" },
        ])
        .end()
        .end()
        .compile(),
  });
  const second = migrationDefinition("002", "unique names", {
    parent: first,
    schemas: V2,
    migrate: (m) => m.updateIndexes("users").compile(),
  });
  const third = migrationDefinition("003", "drop books", {
    parent: second,
    schemas: V2,
    migrate: (m) =>
      m
        .multiCollection("catalog")
        .type("category")
        .deleteWhere({ label: "Books" })
        .end()
        .end()
        .collection("users")
        .transform({
          up: (doc: Record<string, unknown>) => ({
            ...doc,
            team: String(doc.team).toUpperCase(),
          }),
          down: (doc: Record<string, unknown>) => ({
            ...doc,
            team: String(doc.team).toLowerCase(),
          }),
          lossy: true,
        })
        .end()
        .compile(),
  });
  return [first, second, third];
}

async function seed(db: Db): Promise<StudioContext> {
  const migrations = chain();
  const [first] = migrations;
  await createMongodbApplier(db, first, {
    currentMigrationId: first.id,
  }).applyMigration(
    first.migrate(migrationBuilder({ schemas: first.schemas })).operations,
    "up",
  );
  await recordOperation(db, first.id, first.name, "applied", 21);
  return {
    db,
    schemas: V2,
    schemasSource: "project",
    migrations,
    migrationFiles: new Map(migrations.map((m) => [m.id, `${m.id}.ts`])),
    warnings: [],
  };
}

test("plan lists pending migrations with live impact estimates", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const plan = await getPlan(context);
    assertEquals(plan.applied, 1);
    assertEquals(plan.pending, 2);
    assertEquals(
      plan.migrations.map((m) => m.id),
      ["002", "003"],
    );
    assert(
      plan.blocking.some((line) => line.includes("mongodbee sync refuses")),
    );
    assertEquals(plan.commands, ["mongodbee check", "mongodbee migrate"]);

    const [uniqueNames, dropBooks] = plan.migrations;
    assertEquals(uniqueNames.command, "mongodbee migrate --target 002");
    const build = uniqueNames.indexes.find(
      (b) => b.collection === "users" && b.unique,
    );
    assert(build, "the unique index is planned");
    assertEquals(build.change, "new");
    assertEquals(build.documents.value, 5);
    assertEquals(build.duplicates?.groups, 2);
    assertEquals(build.duplicates?.documents, 4);
    assertEquals(
      build.duplicates?.sample.map((s) => [s.key.name, s.count]).sort(),
      [
        ["Ada", 2],
        ["Bob", 2],
      ],
    );
    assert(
      uniqueNames.blocking.some((line) =>
        line.includes("duplicate key groups"),
      ),
    );
    assertEquals(uniqueNames.rollback, "possible");

    const deletion = dropBooks.operations.find(
      (op) => op.type === "delete_multicollection_documents",
    );
    assertEquals(deletion?.impact.documents?.value, 1);
    assertEquals(deletion?.impact.verb, "removed");
    assertEquals(deletion?.impact.dangling?.references, [
      { location: "multiCollections/catalog", path: "categoryId", count: 2 },
    ]);
    assertEquals(
      deletion?.reasons.map((r) => r.flag),
      ["irreversible"],
    );
    const transform = dropBooks.operations.find(
      (op) => op.type === "transform_collection",
    );
    assertEquals(transform?.impact.documents?.value, 5);
    assertEquals(
      transform?.reasons.map((r) => r.flag),
      ["lossy"],
    );
    assertEquals(dropBooks.rollback, "impossible");
  });
});

test("drift compares schemas.ts with the last migration and the live database", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const clean = await getDrift(context);
    assertEquals(clean.schema.rows, []);
    assertEquals(clean.schema.command, undefined);
    assertEquals(
      clean.validators.rows.map((r) => [r.collection, r.status]),
      [
        ["users", "matching"],
        ["catalog", "matching"],
      ],
    );
    const users = clean.indexes.find((r) => r.collection === "users");
    assertEquals(users?.summary.missing, 1);

    const drifted: SchemasDefinition = {
      collections: {
        users: defineType({
          schema: v.object({
            name: v.string(),
            team: v.string(),
            nickname: v.optional(v.string()),
          }),
          indexes: (f) => [index(f.team), unique(f.name)],
        }),
      },
      multiCollections: { catalog: { ...catalog, tag: { label: v.string() } } },
    };
    const report = await getDrift({ ...context, schemas: drifted });
    assertEquals(report.schema.command, "mongodbee generate");
    const rows = report.schema.rows.map((r) => [
      r.collection,
      r.type ?? "",
      r.change,
      r.fields.map((f) => `${f.field}:${f.change}`),
    ]);
    assertEquals(rows, [
      ["users", "", "changed", ["nickname:added"]],
      ["catalog", "tag", "added", ["label:added"]],
    ]);

    await db.command({
      collMod: "users",
      validator: { $jsonSchema: { bsonType: "object" } },
    });
    const tampered = await getDrift(context);
    assertEquals(
      tampered.validators.rows.find((r) => r.collection === "users")?.status,
      "different",
    );
  });
});

test("schemaDrift reports removed collections and types", () => {
  const rows = schemaDrift(V2, {
    collections: {},
    multiCollections: { catalog: { category: catalog.category } },
  });
  assertEquals(
    rows.map((r) => [r.collection, r.type ?? "", r.change]),
    [
      ["users", "", "removed"],
      ["catalog", "product", "removed"],
    ],
  );
});

test("history lists applied runs with durations", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    await recordOperation(db, "002", "unique names", "applied", 7);
    await recordOperation(db, "002", "unique names", "reverted", 3);
    const history = await getHistory(context);
    assertEquals(history.total, 3);
    assertEquals(history.applied, 2);
    assertEquals(history.reverted, 1);
    assertEquals(
      history.entries.map((e) => [e.migrationId, e.operation, e.duration]),
      [
        ["002", "reverted", 3],
        ["002", "applied", 7],
        ["001", "applied", 21],
      ],
    );
  });
});

test("schema API pairs each type with its $jsonSchema and the live validator", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const users = await getCollectionSchema(context, "users");
    assertEquals(users.validator?.status, "matching");
    assertEquals(users.validator?.baseline?.id, "001");
    const json = users.types[0].jsonSchema as {
      bsonType: string;
      properties: Record<string, { bsonType?: string }>;
    };
    assertEquals(json.bsonType, "object");
    assertEquals(json.properties.name.bsonType, "string");
    assert(users.validator?.actual, "the live validator is returned");

    await db.command({
      collMod: "users",
      validator: { $jsonSchema: { bsonType: "object" } },
    });
    const tampered = await getCollectionSchema(context, "users");
    assertEquals(tampered.validator?.status, "different");
  });
});

test("index report carries usage and size when the server exposes them", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    await db
      .collection("users")
      .find({ _id: "none" as never })
      .toArray();
    const report = await getIndexReport(context, "users");
    assertEquals(report.stats.usage, true);
    assertEquals(report.stats.size, true);
    const id = report.rows.find((row) => row.name === "_id_");
    assert((id?.usage?.ops ?? 0) >= 1, "the _id index was used");
    assert((id?.sizeBytes ?? 0) > 0, "the _id index has a size");
    const missing = report.rows.find((row) => row.status === "missing");
    assertEquals(missing?.usage, undefined);
    assertEquals(missing?.sizeBytes, undefined);
  });
});

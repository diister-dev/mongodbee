import { request as httpRequest } from "node:http";
import { test } from "../+harness.ts";
import { assert, assertEquals, assertRejects } from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import type { Db } from "../../src/mongodb.ts";
import * as v from "../../src/schema.ts";
import { refId } from "../../src/ids.ts";
import { index, unique, withIndex } from "../../src/indexes.ts";
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
import { getOverview } from "../../src/studio/api/overview.ts";
import {
  getDocument,
  listDocuments,
  listScopes,
  parseCondition,
} from "../../src/studio/api/documents.ts";
import { getCollectionSchema } from "../../src/studio/api/schema.ts";
import { getIndexReport } from "../../src/studio/api/indexes.ts";
import { getMigrationsReport } from "../../src/studio/api/migrations.ts";
import {
  creationTimeline,
  getCollectionSummary,
  idTimestamp,
} from "../../src/studio/api/summary.ts";
import { listFieldValues } from "../../src/studio/api/values.ts";
import { labelOf, resolveLabels } from "../../src/studio/api/labels.ts";
import { getFieldCoverage } from "../../src/studio/api/coverage.ts";
import { handleApiRequest } from "../../src/studio/router.ts";
import { StudioHttpError } from "../../src/studio/http.ts";
import { startStudioServer } from "../../src/studio/server.ts";

const EXPO_A = "exposition:alpha01";
const EXPO_B = "exposition:beta02";

const Users = defineType({
  schema: v.object({
    email: withIndex(v.pipe(v.string(), v.email()), { unique: true }),
    name: v.pipe(v.string(), v.minLength(1), v.maxLength(80)),
    age: v.optional(v.pipe(v.number(), v.integer(), v.minValue(0))),
    role: v.picklist(["admin", "viewer"]),
    address: v.optional(
      v.object({ city: v.string(), zip: v.nullable(v.string()) }),
    ),
    managerId: v.nullable(refId("user")),
  }),
  indexes: (f) => [index(f.role, f.name)],
});

const catalogTypes = {
  product: { sku: v.string(), price: v.number() },
  category: { label: v.string() },
};

const workspaceTypes = {
  note: { title: v.string() },
};

const Artwork = defineType({
  schema: v.object({
    title: v.string(),
    status: v.picklist(["draft", "published"]),
    tags: v.array(v.string()),
  }),
  indexes: (f) => [unique(f.title)],
});

const expoTypes = {
  artwork: Artwork,
  visit: {
    kind: v.variant("type", [
      v.object({ type: v.literal("guided"), guide: v.string() }),
      v.object({ type: v.literal("free") }),
    ]),
  },
};

const SCHEMAS: SchemasDefinition = {
  collections: { users: Users },
  multiCollections: { catalog: catalogTypes },
  multiModels: { workspace: workspaceTypes },
  scopedMultiCollections: {
    expo: { scope: refId("exposition"), types: expoTypes },
  },
};

function initialMigration(): MigrationDefinition {
  return migrationDefinition("001", "initial", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (m) =>
      m
        .createCollection("users")
        .seed(
          Array.from({ length: 7 }, (_, i) => ({
            _id: `u${i}`,
            email: `user${i}@example.com`,
            name: `User ${i}`,
            role: i < 2 ? "admin" : "viewer",
            managerId: null,
            ...(i % 2 ? { age: 20 + i } : {}),
          })),
        )
        .end()
        .createMultiCollection("catalog")
        .type("category")
        .seed([{ label: "Books" }])
        .end()
        .type("product")
        .seed([
          { sku: "A", price: 1 },
          { sku: "B", price: 2 },
          { sku: "C", price: 3 },
        ])
        .end()
        .end()
        .createMultiModelInstance("workspace:alpha", "workspace")
        .type("note")
        .seed([{ title: "one" }, { title: "two" }])
        .end()
        .end()
        .createScopedMultiCollection("expo")
        .type("artwork")
        .seed(EXPO_A, [
          { title: "a1", status: "draft", tags: [] },
          { title: "a2", status: "published", tags: ["x"] },
        ])
        .seed(EXPO_B, [
          { title: "b1", status: "draft", tags: [] },
          { title: "b2", status: "draft", tags: [] },
          { title: "b3", status: "published", tags: [] },
          { title: "b4", status: "published", tags: [] },
        ])
        .end()
        .type("visit")
        .seed(EXPO_A, [{ kind: { type: "free" } }])
        .end()
        .end()
        .compile(),
  });
}

function secondMigration(parent: MigrationDefinition): MigrationDefinition {
  return migrationDefinition("002", "rename users", {
    parent,
    schemas: SCHEMAS,
    migrate: (m) =>
      m
        .collection("users")
        .transform({
          up: (doc: Record<string, unknown>) => doc,
          down: (doc: Record<string, unknown>) => doc,
          lossy: true,
        })
        .end()
        .compile(),
  });
}

async function seed(db: Db): Promise<StudioContext> {
  const first = initialMigration();
  const second = secondMigration(first);
  await createMongodbApplier(db, first, {
    currentMigrationId: first.id,
  }).applyMigration(
    first.migrate(migrationBuilder({ schemas: first.schemas })).operations,
    "up",
  );
  await recordOperation(db, first.id, first.name, "applied", 12);
  await db.collection("legacy").insertMany([{ a: 1 }, { a: 2 }]);
  return {
    db,
    schemas: SCHEMAS,
    schemasSource: "project",
    migrations: [first, second],
    migrationFiles: new Map([
      [first.id, "001.ts"],
      [second.id, "002.ts"],
    ]),
    warnings: [],
  };
}

test("studio overview: counts per kind, type and scope", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const overview = await getOverview(context, { topScopes: 1 });
    const byName = new Map(overview.collections.map((c) => [c.name, c]));

    const users = byName.get("users")!;
    assertEquals(users.kind, "collection");
    assertEquals(users.total, 7);

    const catalog = byName.get("catalog")!;
    assertEquals(catalog.kind, "multiCollection");
    const catalogCounts = Object.fromEntries(
      catalog.types.filter((t) => !t.meta).map((t) => [t.name, t.count]),
    );
    assertEquals(catalogCounts, { product: 3, category: 1 });

    const instance = byName.get("workspace:alpha")!;
    assertEquals(instance.kind, "multiModelInstance");
    assertEquals(instance.model, "workspace");
    assertEquals(instance.types.find((t) => t.name === "note")?.count, 2);
    assert(instance.types.some((t) => t.meta));

    const expo = byName.get("expo")!;
    assertEquals(expo.kind, "scopedMultiCollection");
    assertEquals(expo.total, 7);
    assertEquals(expo.scopes?.distinct, 2);
    assertEquals(expo.scopes?.top.length, 1);
    assertEquals(expo.scopes?.top[0].scope, EXPO_B);
    assertEquals(expo.scopes?.top[0].count, 4);

    const scopes = await listScopes(context, "expo", 10);
    const alpha = scopes.top.find((s) => s.scope === EXPO_A)!;
    assertEquals(alpha.types, [
      { type: "artwork", count: 2 },
      { type: "visit", count: 1 },
    ]);

    assertEquals(byName.get("legacy")?.kind, "undeclared");
    assertEquals(byName.get("legacy")?.total, 2);
    assertEquals(byName.get("__dbee_migration__")?.internal, true);
    assertEquals(byName.get("__dbee_migration__")?.kind, "internal");
  });
});

test("studio documents: keyset pagination on _id both ways", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const ids = (page: { items: Record<string, unknown>[] }) =>
      page.items.map((item) => item._id);

    const first = await listDocuments(context, "users", { limit: 3 });
    assertEquals(ids(first), ["u0", "u1", "u2"]);
    assertEquals(first.hasNext, true);
    assertEquals(first.hasPrevious, false);

    const second = await listDocuments(context, "users", {
      limit: 3,
      after: first.lastId,
    });
    assertEquals(ids(second), ["u3", "u4", "u5"]);
    assertEquals(second.hasPrevious, true);

    const last = await listDocuments(context, "users", {
      limit: 3,
      after: second.lastId,
    });
    assertEquals(ids(last), ["u6"]);
    assertEquals(last.hasNext, false);

    const back = await listDocuments(context, "users", {
      limit: 3,
      before: second.firstId,
    });
    assertEquals(ids(back), ["u0", "u1", "u2"]);
    assertEquals(back.hasPrevious, false);
    assertEquals(back.hasNext, true);
  });
});

test("studio documents: type, scope and equality filters", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);

    const products = await listDocuments(context, "catalog", {
      type: "product",
    });
    assertEquals(products.items.length, 3);
    assert(products.items.every((item) => item._type === "product"));

    const everything = await listDocuments(context, "workspace:alpha", {});
    assert(
      everything.items.every((item) => item._type === "note"),
      "metadata documents stay hidden unless asked for",
    );

    const drafts = await listDocuments(context, "expo", {
      type: "artwork",
      scope: EXPO_B,
      filters: { status: "draft" },
    });
    assertEquals(drafts.items.map((item) => item.title).sort(), ["b1", "b2"]);

    const aged = await listDocuments(context, "users", {
      filters: { age: "21" },
    });
    assertEquals(
      aged.items.map((item) => item._id),
      ["u1"],
    );

    const priced = await listDocuments(context, "catalog", {
      type: "product",
      filters: { price: "2" },
    });
    assertEquals(
      priced.items.map((item) => item.sku),
      ["B"],
    );

    const literal = await listDocuments(context, "users", {
      filters: { name: '{"$ne":null}' },
    });
    assertEquals(literal.items.length, 0);

    await assertRejects(
      () => listDocuments(context, "legacy", { filters: { a: '{"$gt":0}' } }),
      StudioHttpError,
    );
    await assertRejects(
      () => listDocuments(context, "users", { filters: { "a.b": "1" } }),
      StudioHttpError,
    );
    await assertRejects(
      () => listDocuments(context, "users", { scope: EXPO_A }),
      StudioHttpError,
    );

    const one = await getDocument(context, "users", '"u4"');
    assertEquals(one.email, "user4@example.com");
    await assertRejects(
      () => getDocument(context, "users", '"missing"'),
      StudioHttpError,
    );
    await assertRejects(
      () => listDocuments(context, "nope", {}),
      StudioHttpError,
    );
  });
});

test("studio schema: serializable tree of each type", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const users = await getCollectionSchema(context, "users");
    assertEquals(users.implicitFields, ["_id"]);
    const fields = users.types[0].fields;

    assertEquals(fields.email.kind, "string");
    assertEquals(fields.email.index, { unique: true });
    assertEquals(fields.name.checks, [
      { type: "min_length", requirement: 1 },
      { type: "max_length", requirement: 80 },
    ]);
    assertEquals(fields.age.optional, true);
    assertEquals(fields.age.checks, [
      { type: "integer" },
      { type: "min_value", requirement: 0 },
    ]);
    assertEquals(fields.role.values, ["admin", "viewer"]);
    assertEquals(fields.address.optional, true);
    assertEquals(fields.address.entries?.zip.nullable, true);
    assertEquals(fields.managerId.ref, "user");
    assertEquals(fields.managerId.nullable, true);
    assertEquals(users.types[0].indexes, [{ key: { role: 1, name: 1 } }]);

    const expo = await getCollectionSchema(context, "expo");
    assertEquals(expo.implicitFields, ["_id", "_type", "_scope"]);
    assertEquals(expo.scope?.ref, "exposition");
    const artwork = expo.types.find((type) => type.name === "artwork")!;
    assertEquals(artwork.fields.tags.item?.kind, "string");
    const visit = expo.types.find((type) => type.name === "visit")!;
    assertEquals(visit.fields.kind.discriminator, "type");
    assertEquals(
      visit.fields.kind.options?.map((o) => o.entries?.type.literal),
      ["guided", "free"],
    );

    JSON.parse(JSON.stringify(expo));
  });
});

test("studio indexes: declared versus actual", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);

    const clean = await getIndexReport(context, "expo");
    assertEquals(clean.summary.missing, 0);
    assertEquals(clean.summary.extra, 0);
    assertEquals(clean.summary.different, 0);
    assert(clean.rows.some((row) => row.name.includes("_idx_title_asc")));

    const users = db.collection("users");
    await users.dropIndex("email");
    await users.createIndex({ email: 1 }, { name: "email" });
    await users.createIndex({ name: 1 }, { name: "manual" });
    const composite = (await users.indexes()).find((i) =>
      i.name?.startsWith("_idx_"),
    );
    assert(composite?.name);
    await users.dropIndex(composite.name);

    const report = await getIndexReport(context, "users");
    const status = Object.fromEntries(report.rows.map((r) => [r.name, r]));
    assertEquals(status.email.status, "different");
    assertEquals(status.email.differences, ["unique"]);
    assertEquals(status.manual.status, "extra");
    assertEquals(status[composite.name].status, "missing");
    assertEquals(status[composite.name].hint, {
      reason: "not-synced",
      command: "mongodbee migrate",
    });
    assertEquals(status._id_.status, "matching");

    const UsersWithCity = defineType({
      schema: v.object({ ...Users.schema.entries }),
      indexes: (f) => [index(f.role, f.name), index(f.name)],
    });
    const nextSchemas: SchemasDefinition = {
      ...SCHEMAS,
      collections: { users: UsersWithCity },
    };
    const [first] = context.migrations;
    const declaring = migrationDefinition("002", "name index", {
      parent: first,
      schemas: nextSchemas,
      migrate: (m) => m.updateIndexes("users").compile(),
    });
    const withPending = await getIndexReport(
      { ...context, schemas: nextSchemas, migrations: [first, declaring] },
      "users",
    );
    const nameIndex = withPending.rows.find((r) => r.name === "_idx_name_asc");
    assertEquals(nameIndex?.status, "missing");
    assertEquals(nameIndex?.hint?.reason, "pending-migration");

    const unmigrated = await getIndexReport(
      { ...context, schemas: nextSchemas, migrations: [first] },
      "users",
    );
    assertEquals(
      unmigrated.rows.find((r) => r.name === "_idx_name_asc")?.hint?.reason,
      "no-migration",
    );

    const legacy = await getIndexReport(context, "legacy");
    assertEquals(legacy.declaredKnown, false);
    assertEquals(
      legacy.rows.map((r) => r.status),
      ["matching"],
    );
  });
});

test("studio migrations: applied and pending state with operations", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    await recordOperation(db, "000-gone", "gone", "applied");

    const report = await getMigrationsReport(context);
    assertEquals(report.total, 2);
    assertEquals(report.applied, 2);
    assertEquals(report.pending, 1);

    const [first, second, gone] = report.migrations;
    assertEquals(first.status, "applied");
    assertEquals(first.fileName, "001.ts");
    assert(first.appliedAt instanceof Date);
    assertEquals(first.duration, 12);
    const seedUsers = first.operations.find(
      (op) => op.type === "seed_collection",
    );
    assertEquals(seedUsers?.target, "users");
    assertEquals(seedUsers?.documentCount, 7);
    assertEquals(seedUsers?.label, "Seed collection");
    assertEquals(seedUsers?.details.documents, undefined);
    const scopedSeed = first.operations.find(
      (op) => op.type === "seed_scoped_multicollection_type",
    );
    assertEquals(scopedSeed?.target, "expo.artwork");
    assertEquals(scopedSeed?.scope, EXPO_A);
    assertEquals(first.properties, []);
    assert(first.operations.every((op) => op.flags.length === 0));

    assertEquals(second.status, "pending");
    assertEquals(second.parentId, "001");
    assertEquals(second.properties, ["lossy"]);
    assertEquals(second.operations[0].type, "transform_collection");
    assertEquals(second.operations[0].flags, ["lossy"]);
    assertEquals(second.operations[0].details.up, undefined);

    assertEquals(gone.id, "000-gone");
    assertEquals(gone.missingFile, true);
    assertEquals(gone.position, null);
  });
});

test("studio router: read-only JSON surface", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const base = "http://127.0.0.1:4983";

    const refused = await handleApiRequest(
      context,
      new Request(`${base}/api/overview`, { method: "POST" }),
    );
    assertEquals(refused.status, 405);

    const missing = await handleApiRequest(
      context,
      new Request(`${base}/api/unknown`),
    );
    assertEquals(missing.status, 404);

    const page = await handleApiRequest(
      context,
      new Request(
        `${base}/api/collections/users/documents?limit=2&f.role=admin`,
      ),
    );
    assertEquals(page.status, 200);
    const body = await page.json();
    assertEquals(body.items.length, 2);
    assertEquals(body.lastId, '"u1"');

    const migrations = await handleApiRequest(
      context,
      new Request(`${base}/api/migrations`),
    );
    const report = await migrations.json();
    assert(typeof report.migrations[0].appliedAt.$date === "string");

    const unknown = await handleApiRequest(
      context,
      new Request(`${base}/api/collections/nope/schema`),
    );
    assertEquals(unknown.status, 404);
  });
});

function statusWithHost(url: string, host: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const request = httpRequest(url, { headers: { host } }, (response) => {
      response.resume();
      resolve(response.statusCode ?? 0);
    });
    request.on("error", reject);
    request.end();
  });
}

test({
  name: "studio server: serves the built UI and the API on loopback",
  timeout: 30_000,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      const context = await seed(db);
      const server = await startStudioServer(context, { port: 0 });
      try {
        const html = await (await fetch(`${server.url}/`)).text();
        const script = /src="(\/[^"]+\.js)"/.exec(html)?.[1];
        assert(script, "the page references its bundle");
        const bundle = await fetch(`${server.url}${script}`);
        assertEquals(bundle.status, 200);
        assert((await bundle.text()).length > 1000);

        const meta = await (await fetch(`${server.url}/api/meta`)).json();
        assertEquals(meta.readOnly, true);

        assertEquals(
          await statusWithHost(`${server.url}/api/meta`, "attacker.example"),
          403,
        );
      } finally {
        await server.stop();
      }
    });
  },
});

test("studio documents: conditions, sort, offset paging and a bounded count", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const ids = (page: { items: Record<string, unknown>[] }) =>
      page.items.map((item) => item._id);

    const adults = await listDocuments(context, "users", {
      conditions: [{ field: "age", op: "gte", value: "23" }],
      count: true,
    });
    assertEquals(ids(adults), ["u3", "u5"]);
    assertEquals(adults.total, { value: 2 });

    const noAge = await listDocuments(context, "users", {
      conditions: [{ field: "age", op: "missing", value: "" }],
    });
    assertEquals(ids(noAge), ["u0", "u2", "u4", "u6"]);

    const named = await listDocuments(context, "users", {
      conditions: [
        { field: "name", op: "contains", value: "user 1" },
        { field: "age", op: "exists", value: "" },
      ],
    });
    assertEquals(ids(named), ["u1"]);

    const escaped = await listDocuments(context, "users", {
      conditions: [{ field: "name", op: "contains", value: ".*" }],
    });
    assertEquals(ids(escaped), []);

    const byAge = await listDocuments(context, "users", {
      conditions: [{ field: "age", op: "exists", value: "" }],
      sort: "age",
      direction: "desc",
      limit: 2,
    });
    assertEquals(ids(byAge), ["u5", "u3"]);
    assertEquals(byAge.sort, {
      field: "age",
      direction: "desc",
      paging: "offset",
    });
    assertEquals(byAge.hasNext, true);
    const next = await listDocuments(context, "users", {
      conditions: [{ field: "age", op: "exists", value: "" }],
      sort: "age",
      direction: "desc",
      limit: 2,
      offset: 2,
    });
    assertEquals(ids(next), ["u1"]);
    assertEquals(next.hasPrevious, true);
    assertEquals(next.hasNext, false);

    const newestFirst = await listDocuments(context, "users", {
      direction: "desc",
      limit: 3,
    });
    assertEquals(ids(newestFirst), ["u6", "u5", "u4"]);
    const older = await listDocuments(context, "users", {
      direction: "desc",
      limit: 3,
      after: newestFirst.lastId,
    });
    assertEquals(ids(older), ["u3", "u2", "u1"]);

    await assertRejects(() =>
      listDocuments(context, "users", {
        conditions: [{ field: "age", op: "gt", value: "old" }],
      }),
    );
  });
});

test("studio documents: condition parsing rejects operators and bad paths", () => {
  assertEquals(parseCondition("address.city:eq:Lyon"), {
    field: "address.city",
    op: "eq",
    value: "Lyon",
  });
  assertEquals(parseCondition("note:contains:a:b"), {
    field: "note",
    op: "contains",
    value: "a:b",
  });
  let rejected = 0;
  for (const raw of ["age", "age:regex:x", "$where:eq:1", "a..b:eq:1"]) {
    try {
      parseCondition(raw);
    } catch {
      rejected++;
    }
  }
  assertEquals(rejected, 4);
});

test("studio summary: types, scope coverage and scope sizes", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const expo = await getCollectionSummary(context, "expo");
    assertEquals(expo.kind, "scopedMultiCollection");
    assertEquals(expo.total, 7);
    const artwork = expo.types.find((type) => type.name === "artwork")!;
    assertEquals(artwork.count, 6);
    assertEquals(artwork.scopes, 2);
    assert(typeof artwork.firstId === "string");
    assert((artwork.firstId as string) <= (artwork.lastId as string));
    const visit = expo.types.find((type) => type.name === "visit")!;
    assertEquals([visit.count, visit.scopes], [1, 1]);
    assertEquals(expo.scopes?.distinct, 2);
    assertEquals(expo.scopes?.unscoped, 0);
    assertEquals(expo.scopes?.sizes, [
      { from: 1, to: 9, scopes: 2 },
      { from: 10, to: 99, scopes: 0 },
      { from: 100, to: 999, scopes: 0 },
      { from: 1000, to: 9999, scopes: 0 },
      { from: 10000, scopes: 0 },
    ]);

    const catalog = await getCollectionSummary(context, "catalog");
    assertEquals(catalog.scopes, undefined);
    assertEquals(
      catalog.types
        .filter((type) => !type.meta)
        .map((type) => [type.name, type.count]),
      [
        ["product", 3],
        ["category", 1],
      ],
    );

    const users = await getCollectionSummary(context, "users");
    assertEquals(users.kind, "collection");
    assertEquals(users.total, 7);
    assertEquals(
      users.types.map((t) => [t.name, t.count]),
      [["users", 7]],
    );
    assertEquals(users.created?.sampled, 7);
    assertEquals(users.created?.dated, 0);

    assertEquals(expo.created?.sampled, 7);
    assertEquals(
      expo.created?.months.reduce((sum, month) => sum + month.count, 0),
      expo.created?.dated,
    );
  });
});

test("creationTimeline buckets id times by month, fills gaps and sets future ids aside", () => {
  const now = Date.UTC(2026, 8, 28);
  const hex = (time: number) =>
    Math.floor(time / 1000)
      .toString(16)
      .padStart(8, "0") + "0".repeat(16);
  const timeline = creationTimeline(
    [
      hex(Date.UTC(2026, 5, 3)),
      `user:${hex(Date.UTC(2026, 7, 9))}`,
      hex(Date.UTC(2026, 7, 20)),
      hex(Date.UTC(2029, 1, 4)),
      "u0",
      42,
    ],
    now,
  );
  assertEquals(timeline, {
    sampled: 6,
    dated: 3,
    future: 1,
    months: [
      { month: "2026-06", count: 1 },
      { month: "2026-07", count: 0 },
      { month: "2026-08", count: 2 },
    ],
    newest: Date.UTC(2026, 7, 20),
  });
  assertEquals(idTimestamp("not an id"), null);
});

test("studio values: frequent values of a field, narrowed by the query", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const params = (entries: [string, string][]) =>
      new URLSearchParams(entries);

    const statuses = await listFieldValues(
      context,
      "expo",
      params([
        ["field", "status"],
        ["type", "artwork"],
      ]),
    );
    assertEquals(statuses.values, [
      { value: "draft", count: 3 },
      { value: "published", count: 3 },
    ]);
    assertEquals([statuses.scanned, statuses.sampled], [6, false]);

    const inScope = await listFieldValues(
      context,
      "expo",
      params([
        ["field", "status"],
        ["type", "artwork"],
        ["scope", EXPO_A],
        ["q", "PUB"],
      ]),
    );
    assertEquals(inScope.values, [{ value: "published", count: 1 }]);

    const tags = await listFieldValues(
      context,
      "expo",
      params([
        ["field", "tags"],
        ["type", "artwork"],
      ]),
    );
    assertEquals(tags.values, [{ value: "x", count: 1 }]);

    const narrowed = await listFieldValues(
      context,
      "expo",
      params([
        ["field", "title"],
        ["type", "artwork"],
        ["w", "status:eq:draft"],
        ["limit", "2"],
      ]),
    );
    assertEquals(
      narrowed.values.map((row) => row.value),
      ["a1", "b1"],
    );

    await assertRejects(
      () => listFieldValues(context, "expo", params([["field", "$where"]])),
      StudioHttpError,
    );
    await assertRejects(
      () =>
        listFieldValues(
          context,
          "expo",
          params([
            ["field", "title"],
            ["q", "x".repeat(201)],
          ]),
        ),
      StudioHttpError,
    );
  });
});

test("studio documents: dotted conditions use the nested field's schema", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const free = await listDocuments(context, "expo", {
      type: "visit",
      conditions: [{ field: "kind.type", op: "eq", value: "free" }],
      count: true,
    });
    assertEquals(free.total, { value: 1 });
    const guided = await listDocuments(context, "expo", {
      type: "visit",
      conditions: [{ field: "kind.type", op: "eq", value: "guided" }],
      count: true,
    });
    assertEquals(guided.total, { value: 0 });
    await db
      .collection("users")
      .updateOne(
        { _id: "u1" as never },
        { $set: { address: { city: "10", zip: null } } },
      );
    const textCompare = await listDocuments(context, "users", {
      conditions: [{ field: "address.city", op: "gte", value: "1" }],
      count: true,
    });
    assertEquals(textCompare.total, { value: 1 });
  });
});

test("studio labels: a readable name for typed ids, from the document itself", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const artworks = await listDocuments(context, "expo", {
      type: "artwork",
      limit: 2,
    });
    const ids = artworks.items.map((item) => item._id as string);
    const products = await listDocuments(context, "catalog", {
      type: "product",
      limit: 1,
    });
    const productId = products.items[0]._id as string;
    const labels = await resolveLabels(context, [
      ...ids,
      productId,
      "ghost:01abc",
      "not a typed id",
    ]);
    assertEquals(
      labels
        .filter((label) => label.collection === "expo")
        .map((label) => [label.id, label.label, label.field, label.type]),
      artworks.items.map((item) => [item._id, item.title, "title", "artwork"]),
    );
    assertEquals(
      labels.some((label) => label.id === productId),
      false,
    );
    await assertRejects(
      () =>
        resolveLabels(
          context,
          Array.from({ length: 101 }, (_, i) => `artwork:${i}`),
        ),
      StudioHttpError,
    );
  });
});

test("labelOf picks a name, a localized title, a person or a nested identity", () => {
  assertEquals(labelOf({ name: "  Salon  " }), {
    label: "Salon",
    field: "name",
  });
  assertEquals(labelOf({ title: { fr: "Atelier", en: "Workshop" } }), {
    label: "Atelier",
    field: "title",
  });
  assertEquals(labelOf({ firstname: "Ada", lastname: "Lovelace" }), {
    label: "Ada Lovelace",
    field: "name",
  });
  assertEquals(labelOf({ identity: { displayName: "Acme" } }), {
    label: "Acme",
    field: "identity.displayName",
  });
  assertEquals(labelOf({ sku: "A" }), undefined);
  assertEquals(labelOf({ name: "x".repeat(200) })?.label.length, 80);
});

test("labelOf prefers a person's name to their email", () => {
  assertEquals(
    labelOf({
      email: "ada@example.test",
      firstname: "Ada",
      lastname: "Lovelace",
    }),
    {
      label: "Ada Lovelace",
      field: "name",
    },
  );
  assertEquals(labelOf({ email: "ada@example.test" }), {
    label: "ada@example.test",
    field: "email",
  });
});

test("studio coverage: how many sampled documents fill each field", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const users = await getFieldCoverage(
      context,
      "users",
      new URLSearchParams(),
    );
    assertEquals(users.sampled, 7);
    assertEquals(users.fields.email, 7);
    assertEquals(users.fields.age, 3);
    assertEquals(users.fields.address, undefined);
    const drafts = await getFieldCoverage(
      context,
      "expo",
      new URLSearchParams([
        ["type", "artwork"],
        ["w", "status:eq:draft"],
      ]),
    );
    assertEquals(drafts.sampled, 3);
    assertEquals(drafts.fields.title, 3);
    assertEquals(drafts.fields._scope, 3);
  });
});

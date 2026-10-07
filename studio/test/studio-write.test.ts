import { test } from "../../library/test/+harness.ts";
import { assert, assertEquals } from "../../library/test/+assert.ts";
import { withDatabase } from "./+db.ts";
import type { Db } from "@diister/mongodbee";
import * as v from "@diister/mongodbee/schema";
import { refId } from "@diister/mongodbee";
import { migrationDefinition } from "@diister/mongodbee/migration";
import { migrationBuilder } from "@diister/mongodbee/migration";
import { createMongodbApplier } from "@diister/mongodbee/migration";
import { recordOperation } from "./+db.ts";
import type { SchemasDefinition } from "@diister/mongodbee/migration";
import type { StudioContext } from "../src/context.ts";
import { listDocuments } from "../src/api/documents.ts";
import { handleApiRequest } from "../src/router.ts";
import { checkInsert, checkUpdate } from "../src/api/write.ts";

const EXPO = "exposition:alpha01";

const SCHEMAS: SchemasDefinition = {
  collections: {
    users: {
      name: v.pipe(v.string(), v.minLength(1)),
      age: v.optional(v.number()),
    },
  },
  multiCollections: {
    catalog: {
      product: { sku: v.string(), price: v.number() },
    },
  },
  scopedMultiCollections: {
    expo: {
      scope: refId("exposition"),
      types: { artwork: { title: v.string() } },
    },
  },
};

const BASE = "http://127.0.0.1:4983";

async function seed(db: Db, write = true): Promise<StudioContext> {
  const first = migrationDefinition("001", "initial", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (m) =>
      m
        .createCollection("users")
        .seed([
          { _id: "u1", name: "Ada", age: 36 },
          { _id: "u2", name: "Alan" },
        ])
        .end()
        .createMultiCollection("catalog")
        .type("product")
        .seed([{ sku: "A", price: 1 }])
        .end()
        .end()
        .createScopedMultiCollection("expo")
        .type("artwork")
        .seed(EXPO, [{ title: "one" }])
        .end()
        .end()
        .compile(),
  });
  await createMongodbApplier(db, first, {
    currentMigrationId: first.id,
  }).applyMigration(
    first.migrate(migrationBuilder({ schemas: first.schemas })).operations,
    "up",
  );
  await recordOperation(db, first.id, first.name, "applied", 5);
  return {
    db,
    schemas: SCHEMAS,
    schemasSource: "project",
    migrations: [first],
    migrationFiles: new Map([[first.id, "001.ts"]]),
    warnings: [],
    write,
  };
}

function send(
  context: StudioContext,
  method: string,
  path: string,
  body: unknown,
  headers: Record<string, string> = {},
) {
  return handleApiRequest(
    context,
    new Request(`${BASE}${path}`, {
      method,
      headers: {
        "content-type": "application/json",
        "x-mongodbee-studio": "write",
        ...headers,
      },
      body: JSON.stringify(body),
    }),
  );
}

test("studio write: refused unless --write, the header and the studio origin", async (t) => {
  await withDatabase(t.name, async (db) => {
    const readOnly = await seed(db, false);
    const refused = await send(
      readOnly,
      "PATCH",
      "/api/collections/users/document",
      {
        id: '"u1"',
        set: { name: "Grace" },
      },
    );
    assertEquals(refused.status, 405);

    const context = { ...readOnly, write: true };
    const noHeader = await handleApiRequest(
      context,
      new Request(`${BASE}/api/collections/users/document`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: '"u1"', set: { name: "Grace" } }),
      }),
    );
    assertEquals(noHeader.status, 403);

    const foreign = await send(
      context,
      "PATCH",
      "/api/collections/users/document",
      { id: '"u1"', set: { name: "Grace" } },
      { origin: "http://evil.example" },
    );
    assertEquals(foreign.status, 403);

    const users = await listDocuments(context, "users", {});
    assertEquals(users.items.find((u) => u._id === "u1")?.name, "Ada");
  });
});

test("studio write: guarded update of a plain collection", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const ok = await send(context, "PATCH", "/api/collections/users/document", {
      id: '"u1"',
      set: { name: "Grace" },
      unset: ["age"],
      expected: { name: "Ada", age: 36 },
    });
    assertEquals(ok.status, 200);
    const after = await db.collection("users").findOne({ _id: "u1" as never });
    assertEquals([after?.name, after?.age], ["Grace", undefined]);

    const stale = await send(
      context,
      "PATCH",
      "/api/collections/users/document",
      {
        id: '"u1"',
        set: { name: "Linus" },
        expected: { name: "Ada" },
      },
    );
    assertEquals(stale.status, 409);

    const invalid = await send(
      context,
      "PATCH",
      "/api/collections/users/document",
      {
        id: '"u1"',
        set: { name: "" },
        expected: { name: "Grace" },
      },
    );
    assertEquals(invalid.status, 422);
    const details = await invalid.json();
    assert(
      details.issues.some((issue: { path: string }) => issue.path === "name"),
    );

    const protectedField = await send(
      context,
      "PATCH",
      "/api/collections/users/document",
      {
        id: '"u1"',
        set: { _id: "u9" },
      },
    );
    assertEquals(protectedField.status, 400);

    const computedField = await send(
      context,
      "PATCH",
      "/api/collections/users/document",
      {
        id: '"u1"',
        set: { _computed: { _rev: 9 } },
      },
    );
    assertEquals(computedField.status, 400);
    assert(
      (await computedField.json()).error.includes("maintained by mongodbee"),
    );
  });
});

test("studio write: insert, update and delete in multi and scoped collections", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);

    const created = await send(
      context,
      "POST",
      "/api/collections/catalog/documents",
      {
        type: "product",
        document: { sku: "B", price: 2 },
      },
    );
    assertEquals(created.status, 200);
    const productId = (await created.json()).id as string;
    assert(productId.startsWith("product:"));

    const badInsert = await send(
      context,
      "POST",
      "/api/collections/catalog/documents",
      {
        type: "product",
        document: { sku: "C", price: "free" },
      },
    );
    assertEquals(badInsert.status, 422);

    const updated = await send(
      context,
      "PATCH",
      "/api/collections/catalog/document",
      {
        id: JSON.stringify(productId),
        type: "product",
        set: { price: 3 },
        expected: { price: 2 },
      },
    );
    assertEquals(updated.status, 200);

    const wrongConfirm = await send(
      context,
      "DELETE",
      "/api/collections/catalog/document",
      {
        id: JSON.stringify(productId),
        type: "product",
        confirm: "yes",
      },
    );
    assertEquals(wrongConfirm.status, 400);
    const deleted = await send(
      context,
      "DELETE",
      "/api/collections/catalog/document",
      {
        id: JSON.stringify(productId),
        type: "product",
        confirm: productId,
      },
    );
    assertEquals(deleted.status, 200);
    assertEquals(
      await db
        .collection("catalog")
        .countDocuments({ _id: productId as never }),
      0,
    );

    const noScope = await send(
      context,
      "POST",
      "/api/collections/expo/documents",
      {
        type: "artwork",
        document: { title: "two" },
      },
    );
    assertEquals(noScope.status, 400);

    const artwork = await send(
      context,
      "POST",
      "/api/collections/expo/documents",
      {
        type: "artwork",
        scope: EXPO,
        document: { title: "two" },
      },
    );
    assertEquals(artwork.status, 200);
    const artworkId = (await artwork.json()).id as string;
    const stored = await db
      .collection("expo")
      .findOne({ _id: artworkId as never });
    assertEquals(
      [stored?._scope, stored?._type, stored?.title],
      [EXPO, "artwork", "two"],
    );

    const otherScope = await send(
      context,
      "PATCH",
      "/api/collections/expo/document",
      {
        id: JSON.stringify(artworkId),
        type: "artwork",
        scope: "exposition:beta02",
        set: { title: "moved" },
        expected: { title: "two" },
      },
    );
    assertEquals(otherScope.status, 409);
  });
});

test("studio write: refused while migrations are pending", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const pending = migrationDefinition("002", "later", {
      parent: context.migrations[0],
      schemas: SCHEMAS,
      migrate: (m) => m.compile(),
    });
    const withPending = {
      ...context,
      migrations: [...context.migrations, pending],
    };
    const response = await send(
      withPending,
      "PATCH",
      "/api/collections/users/document",
      {
        id: '"u1"',
        set: { name: "Grace" },
        expected: { name: "Ada" },
      },
    );
    assertEquals(response.status, 409);
    assert((await response.json()).error.includes("pending"));
  });
});

test("write checks name the field and the nested path of every issue", () => {
  const fields = {
    name: v.pipe(v.string(), v.minLength(1)),
    address: v.object({ city: v.string() }),
    nick: v.optional(v.string()),
  };
  assertEquals(
    checkUpdate(fields, { address: { city: 3 } }, []).map(
      (issue) => issue.path,
    ),
    ["address.city"],
  );
  assertEquals(checkUpdate(fields, { ghost: 1 }, ["name", "nick"]), [
    { path: "ghost", message: "Not declared in the schema" },
    { path: "name", message: "Required: it cannot be removed" },
  ]);
  assertEquals(checkUpdate(fields, { name: "Ada", nick: "A" }, []), []);
  assertEquals(
    checkInsert(fields, { _id: "x", name: "Ada" }).map((issue) => issue.path),
    ["address"],
  );
});

test("studio write: a deletion can be undone with its token, once", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const deleted = await send(
      context,
      "DELETE",
      "/api/collections/users/document",
      {
        id: '"u1"',
        confirm: "u1",
      },
    );
    assertEquals(deleted.status, 200);
    const { restoreToken } = await deleted.json();
    assert(typeof restoreToken === "string");
    assertEquals(
      await db.collection("users").countDocuments({ _id: "u1" as never }),
      0,
    );

    const restored = await send(
      context,
      "POST",
      "/api/collections/users/restore",
      { token: restoreToken },
    );
    assertEquals(restored.status, 200);
    const back = await db.collection("users").findOne({ _id: "u1" as never });
    assertEquals([back?.name, back?.age], ["Ada", 36]);

    const again = await send(
      context,
      "POST",
      "/api/collections/users/restore",
      { token: restoreToken },
    );
    assertEquals(again.status, 410);
    const elsewhere = await send(
      context,
      "POST",
      "/api/collections/catalog/restore",
      { token: "nope" },
    );
    assertEquals(elsewhere.status, 410);
  });
});

test("studio write: validate reports issues without writing", async (t) => {
  await withDatabase(t.name, async (db) => {
    const context = await seed(db);
    const response = await send(
      context,
      "POST",
      "/api/collections/catalog/validate",
      {
        type: "product",
        document: { _id: "product:x", _type: "product", sku: 3 },
      },
    );
    assertEquals(response.status, 200);
    const { issues } = await response.json();
    assertEquals(issues.map((issue: { path: string }) => issue.path).sort(), [
      "price",
      "sku",
    ]);
    assertEquals(
      await db
        .collection("catalog")
        .countDocuments({ _id: "product:x" as never }),
      0,
    );
  });
});

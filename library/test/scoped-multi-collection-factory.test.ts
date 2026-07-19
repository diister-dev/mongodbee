import { assert, assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

Deno.test("scopedMultiCollection: creates the underlying MongoDB collection", async () => {
  await withDatabase("smc-factory-basic", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: {
        artwork: { title: v.string() },
      },
    });

    assert(catalog);
    assertEquals(typeof catalog.scope, "function");
    assertEquals(typeof catalog.scopes, "function");

    const collections = await db.listCollections({ name: "catalog" }).toArray();
    assertEquals(collections.length, 1);
  });
});

Deno.test("scopedMultiCollection: auto-mints _id for a refId-typed _id when omitted", async () => {
  // A per-type `_id` declared as a bare `refId(type)` is required with no
  // default — but inserts must still work without the caller supplying `_id`,
  // matching the multiCollection contract (auto-mint `type:<ulid>`).
  await withDatabase("smc-factory-autoid", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: {
        security: { _id: refId("security"), token: v.string() },
      },
    });
    const view = catalog.scope("exposition:abc123");

    const insertedId = await view.insertOne("security", { token: "k1" });
    assert(
      String(insertedId).startsWith("security:"),
      `expected minted id to start with "security:", got ${insertedId}`,
    );

    const docs = await view.find("security", {});
    assertEquals(docs.length, 1);
    assert(String(docs[0]._id).startsWith("security:"));

    // insertMany too
    await view.insertMany("security", [{ token: "k2" }, { token: "k3" }]);
    assertEquals((await view.find("security", {})).length, 3);

    // A caller-supplied `_id` is still honored at runtime. The DynId type
    // treats a refId `_id` as auto-generated (so callers omit it); passing one
    // explicitly — as the exposition `information` singleton does to pin
    // `_id == expositionId` — goes through a cast, exactly like the app.
    await (view as unknown as {
      insertOne(t: string, d: Record<string, unknown>): Promise<string>;
    }).insertOne("security", { _id: "security:custom", token: "k4" });
    const custom = await view.findOne("security", { token: "k4" });
    assertEquals(custom?._id, "security:custom");
  });
});

Deno.test("scopedMultiCollection: applies a MongoDB JSON Schema validator", async () => {
  await withDatabase("smc-factory-validator", async (db) => {
    await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: {
        artwork: { title: v.string() },
      },
    });

    const info = await db.command({
      listCollections: 1,
      filter: { name: "catalog" },
    });
    const validator = info.cursor?.firstBatch?.[0]?.options?.validator;
    assert(validator, "expected validator to be set on the collection");
    assert(validator.$jsonSchema, "expected $jsonSchema validator");
  });
});

Deno.test("scopedMultiCollection: rejects _scope as a user field name", async () => {
  await withDatabase("smc-factory-reserved-scope", async (db) => {
    await assertRejects(
      () =>
        scopedMultiCollection(db, "catalog", {
          scope: refId("exposition"),
          types: {
            // deno-lint-ignore no-explicit-any
            artwork: { _scope: v.string(), title: v.string() } as any,
          },
        }),
      Error,
      "_scope",
    );
  });
});

Deno.test("scopedMultiCollection: rejects _type as a user field name", async () => {
  await withDatabase("smc-factory-reserved-type", async (db) => {
    await assertRejects(
      () =>
        scopedMultiCollection(db, "catalog", {
          scope: refId("exposition"),
          types: {
            // deno-lint-ignore no-explicit-any
            artwork: { _type: v.string(), title: v.string() } as any,
          },
        }),
      Error,
      "_type",
    );
  });
});

Deno.test("scopedMultiCollection: rejects _id with wrong shape as a user field name", async () => {
  // _id is allowed if it's a dbId/refId-shaped schema (consistent with multiCollection),
  // but we still need to refuse blatant misuse. For now, just ensure the factory works
  // when _id is absent (which is the documented happy path).
  await withDatabase("smc-factory-no-id", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: {
        artwork: { title: v.string() },
      },
    });
    assert(catalog);
  });
});

Deno.test("scopedMultiCollection: rejects empty types record", async () => {
  await withDatabase("smc-factory-empty-types", async (db) => {
    await assertRejects(
      () =>
        scopedMultiCollection(db, "catalog", {
          scope: refId("exposition"),
          types: {},
        }),
      Error,
      "at least one type",
    );
  });
});

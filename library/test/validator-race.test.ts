import { test } from "./+harness.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { resetRuntimeConfig, setRuntimeConfig } from "../src/runtime-config.ts";
import { dirtyEquivalent } from "../src/utils/object.ts";
import { refId } from "../src/ids.ts";
import * as v from "../src/schema.ts";
import type { Db } from "../src/mongodb.ts";

// Two processes booting against the same empty database both see a collection
// missing and both `create` it. MongoDB 6 rejects the loser with
// NamespaceExists (code 48); 8.0 does too when the options differ. The loser
// must carry on as if the collection had existed all along — before this was
// handled, an api and a worker starting together crashed one of them.
//
// The race is reproduced deterministically rather than by timing: the first
// time a factory lists the collection and finds nothing, "another process"
// creates it — with a different validator, so every MongoDB version rejects
// the second create, on 8.0 as well as 6.0.

const STALE_VALIDATOR = { $jsonSchema: { bsonType: "object" } };
const DOCUMENT_FAILED_VALIDATION = 121;
const EXPO_A = "exposition:expoaaaaa01";

function raceOnFirstListing(db: Db, name: string): Db {
  let interposed = false;
  return new Proxy(db, {
    get(target, prop) {
      const value = Reflect.get(target, prop, target);
      if (prop !== "listCollections") {
        return typeof value === "function" ? value.bind(target) : value;
      }
      return (filter?: { name?: string }, options?: object) => {
        const cursor = target.listCollections(filter, options);
        if (interposed || filter?.name !== name) return cursor;
        const toArray = cursor.toArray.bind(cursor);
        cursor.toArray = async () => {
          const found = await toArray();
          if (found.length === 0 && !interposed) {
            interposed = true;
            await target.createCollection(name, { validator: STALE_VALIDATOR });
          }
          return found;
        };
        return cursor;
      };
    },
  }) as Db;
}

function failingCreate(db: Db, error: unknown): Db {
  return new Proxy(db, {
    get(target, prop) {
      if (prop === "createCollection") return () => Promise.reject(error);
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  }) as Db;
}

async function currentValidator(db: Db, name: string): Promise<unknown> {
  const res = await db.command({ listCollections: 1, filter: { name } });
  return res.cursor?.firstBatch?.[0]?.options?.validator ?? null;
}

async function inAutoMode(work: () => Promise<void>): Promise<void> {
  setRuntimeConfig({ runtime: { schemaManagement: "auto" } });
  try {
    await work();
  } finally {
    resetRuntimeConfig();
  }
}

test("collection(): losing the create race applies the validator instead of crashing", async () => {
  await inAutoMode(() =>
    withDatabase("validator-race-collection", async (db) => {
      const users = await collection(raceOnFirstListing(db, "users"), "users", {
        name: v.string(),
      });

      assert(
        !dirtyEquivalent(await currentValidator(db, "users"), STALE_VALIDATOR),
      );
      await users.insertOne({ name: "ok" });
      const error = await assertRejects(() =>
        db.collection("users").insertOne({ name: 5 } as never),
      );
      assertEquals(
        (error as { code?: number }).code,
        DOCUMENT_FAILED_VALIDATION,
      );
    }),
  );
});

test("multiCollection(): losing the create race applies the validator instead of crashing", async () => {
  await inAutoMode(() =>
    withDatabase("validator-race-multi", async (db) => {
      const catalog = await multiCollection(
        raceOnFirstListing(db, "catalog"),
        "catalog",
        {
          artwork: { title: v.string() },
        },
      );

      assert(
        !dirtyEquivalent(
          await currentValidator(db, "catalog"),
          STALE_VALIDATOR,
        ),
      );
      await catalog.insertOne("artwork", { title: "Mona Lisa" });
      const error = await assertRejects(() =>
        db
          .collection("catalog")
          .insertOne({ _type: "artwork", title: 5 } as never),
      );
      assertEquals(
        (error as { code?: number }).code,
        DOCUMENT_FAILED_VALIDATION,
      );
    }),
  );
});

test("scopedMultiCollection(): losing the create race applies the validator instead of crashing", async () => {
  await inAutoMode(() =>
    withDatabase("validator-race-scoped", async (db) => {
      const catalog = await scopedMultiCollection(
        raceOnFirstListing(db, "catalog"),
        "catalog",
        {
          scope: refId("exposition"),
          types: { artwork: { title: v.string() } },
        },
      );

      assert(
        !dirtyEquivalent(
          await currentValidator(db, "catalog"),
          STALE_VALIDATOR,
        ),
      );
      await catalog.scope(EXPO_A).insertOne("artwork", { title: "Mona Lisa" });
      const error = await assertRejects(() =>
        db
          .collection("catalog")
          .insertOne({ _type: "artwork", title: 5 } as never),
      );
      assertEquals(
        (error as { code?: number }).code,
        DOCUMENT_FAILED_VALIDATION,
      );
    }),
  );
});

test("only NamespaceExists is absorbed: any other create failure still propagates", async () => {
  await inAutoMode(() =>
    withDatabase("validator-race-other-error", async (db) => {
      const unauthorized = Object.assign(
        new Error("not authorized on db to execute command"),
        {
          code: 13,
        },
      );
      const error = await assertRejects(() =>
        collection(failingCreate(db, unauthorized), "users", {
          name: v.string(),
        }),
      );
      assertEquals((error as { code?: number }).code, 13);
    }),
  );
});

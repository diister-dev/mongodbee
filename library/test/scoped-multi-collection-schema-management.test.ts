import { assert, assertEquals } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { resetRuntimeConfig, setRuntimeConfig } from "../src/runtime-config.ts";
import { migrationDefinition } from "../src/migration/definition.ts";
import { migrationBuilder } from "../src/migration/builder.ts";
import { createMongodbApplier } from "../src/migration/appliers/mongodb.ts";
import { refId } from "../src/ids.ts";
import * as v from "../src/schema.ts";
import type { Db } from "../src/mongodb.ts";

const EXPO_A = "exposition:expoaaaaa01";
const BASE_INDEX = "_scope_1__type_1__id_1";
const STALE_VALIDATOR = { $jsonSchema: { bsonType: "object" } };

const CATALOG = {
  scope: refId("exposition"),
  types: { artwork: { title: v.string() } },
};

async function collectionExists(db: Db, name: string): Promise<boolean> {
  const found = await db.listCollections({ name }).toArray();
  return found.length > 0;
}

async function currentValidator(db: Db, name: string): Promise<unknown> {
  const res = await db.command({ listCollections: 1, filter: { name } });
  return res.cursor?.firstBatch?.[0]?.options?.validator ?? null;
}

async function hasBaseIndex(db: Db, name: string): Promise<boolean> {
  const indexes = await db.collection(name).indexes();
  return indexes.some((i) => i.name === BASE_INDEX);
}

async function withGlobalMode(
  mode: "auto" | "managed",
  work: () => Promise<void>,
): Promise<void> {
  setRuntimeConfig({ runtime: { schemaManagement: mode } });
  try {
    await work();
  } finally {
    resetRuntimeConfig();
  }
}

Deno.test(
  "managed (global default): init issues no DDL, the collection is not created",
  async () => {
    await withDatabase("smc-sm-managed-default", async (db) => {
      const catalog = await scopedMultiCollection(db, "catalog", CATALOG);
      assertEquals(await collectionExists(db, "catalog"), false);

      await catalog.scope(EXPO_A).insertOne("artwork", { title: "Mona Lisa" });
      assertEquals(await catalog.scope(EXPO_A).countDocuments("artwork"), 1);
      assertEquals(await currentValidator(db, "catalog"), null);
      assertEquals(await hasBaseIndex(db, "catalog"), false);
    });
  },
);

Deno.test(
  "local auto overrides a managed global: validator and base index are applied",
  async () => {
    await withDatabase("smc-sm-local-auto", async (db) => {
      await withGlobalMode("managed", async () => {
        await scopedMultiCollection(db, "catalog", {
          ...CATALOG,
          schemaManagement: "auto",
        });
      });
      const validator = await currentValidator(db, "catalog") as {
        $jsonSchema?: unknown;
      } | null;
      assert(validator?.$jsonSchema, "expected a $jsonSchema validator");
      assertEquals(await hasBaseIndex(db, "catalog"), true);
    });
  },
);

Deno.test(
  "inherit follows an auto global: validator and base index are applied",
  async () => {
    await withDatabase("smc-sm-global-auto", async (db) => {
      await withGlobalMode("auto", async () => {
        await scopedMultiCollection(db, "catalog", CATALOG);
      });
      const validator = await currentValidator(db, "catalog") as {
        $jsonSchema?: unknown;
      } | null;
      assert(validator?.$jsonSchema, "expected a $jsonSchema validator");
      assertEquals(await hasBaseIndex(db, "catalog"), true);
    });
  },
);

Deno.test(
  "local managed overrides an auto global: no DDL",
  async () => {
    await withDatabase("smc-sm-local-managed", async (db) => {
      await withGlobalMode("auto", async () => {
        await scopedMultiCollection(db, "catalog", {
          ...CATALOG,
          schemaManagement: "managed",
        });
      });
      assertEquals(await collectionExists(db, "catalog"), false);
    });
  },
);

Deno.test(
  "managed leaves a stale validator untouched, auto rewrites it",
  async () => {
    await withDatabase("smc-sm-stale-validator", async (db) => {
      await db.createCollection("catalog", { validator: STALE_VALIDATOR });

      await withGlobalMode("managed", async () => {
        await scopedMultiCollection(db, "catalog", CATALOG);
      });
      assertEquals(await currentValidator(db, "catalog"), STALE_VALIDATOR);
      assertEquals(await hasBaseIndex(db, "catalog"), false);

      await withGlobalMode("auto", async () => {
        await scopedMultiCollection(db, "catalog", CATALOG);
      });
      const validator = await currentValidator(db, "catalog") as {
        $jsonSchema?: { anyOf?: unknown[] };
      };
      assert(
        Array.isArray(validator.$jsonSchema?.anyOf),
        "expected the union validator to replace the stale one",
      );
      assertEquals(await hasBaseIndex(db, "catalog"), true);
    });
  },
);

Deno.test(
  "auto init inside a session issues no DDL",
  async () => {
    await withDatabase("smc-sm-inside-session", async (db) => {
      const outer = await scopedMultiCollection(db, "outer", {
        ...CATALOG,
        schemaManagement: "auto",
      });

      await outer.withSession(async () => {
        const inner = await scopedMultiCollection(db, "inner", {
          ...CATALOG,
          schemaManagement: "auto",
        });
        await inner.scope(EXPO_A).insertOne("artwork", { title: "Guernica" });
      });

      assertEquals(await currentValidator(db, "inner"), null);
      assertEquals(await hasBaseIndex(db, "inner"), false);
      assertEquals(
        await db.collection("inner").countDocuments({} as never),
        1,
      );
    });
  },
);

Deno.test(
  "applier: create_scoped_multicollection applies DDL under a managed global",
  async () => {
    await withDatabase("smc-sm-applier-create", async (db) => {
      const schemas = {
        collections: {},
        scopedMultiCollections: { catalog: CATALOG },
      };
      const migration = migrationDefinition("001", "create-catalog", {
        parent: null,
        schemas,
        migrate: (b) =>
          b.createScopedMultiCollection("catalog")
            .type("artwork")
            .seed(EXPO_A, [{ title: "Mona Lisa" }])
            .end()
            .end()
            .compile(),
      });
      const operations =
        migration.migrate(migrationBuilder({ schemas })).operations;

      await withGlobalMode("managed", async () => {
        await createMongodbApplier(db, migration, {
          currentMigrationId: migration.id,
        }).applyMigration(operations, "up");
      });

      const validator = await currentValidator(db, "catalog") as {
        $jsonSchema?: { anyOf?: unknown[] };
      };
      assert(
        Array.isArray(validator.$jsonSchema?.anyOf),
        "expected the migration to install the union validator",
      );
      assertEquals(await hasBaseIndex(db, "catalog"), true);
      assertEquals(
        await db.collection("catalog").countDocuments(
          { _type: "artwork" } as never,
        ),
        1,
      );
    });
  },
);

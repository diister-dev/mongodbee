import { assert, assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { collection } from "../src/collection.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import {
  applyCollectionIndexes,
  applyMultiCollectionIndexes,
} from "../src/indexes-applier.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

Deno.test("auto-index: { _scope: 1, _type: 1 } is created at init", async () => {
  await withDatabase("smc-idx-base", async (db) => {
    await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { title: v.string() } },
    });

    const idx = await db.collection("catalog").indexes();
    const found = idx.find((i) =>
      i.key && i.key._scope === 1 && i.key._type === 1
    );
    assert(
      found,
      `expected {_scope:1,_type:1} index, got ${JSON.stringify(idx)}`,
    );
  });
});

Deno.test(
  "scoped uniqueness: same email in two scopes is allowed",
  async () => {
    await withDatabase("smc-idx-scoped-unique", async (db) => {
      const catalog = await scopedMultiCollection(db, "catalog", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          user: {
            email: withIndex(v.string(), { unique: true }),
            name: v.string(),
          },
        },
      });

      const a = catalog.scope(EXPO_A);
      const b = catalog.scope(EXPO_B);

      await a.insertOne("user", { email: "x@example.com", name: "Alice" });
      // Same email, different scope → must succeed
      await b.insertOne("user", { email: "x@example.com", name: "Bob" });

      assertEquals(await a.countDocuments("user"), 1);
      assertEquals(await b.countDocuments("user"), 1);
    });
  },
);

Deno.test(
  "scoped uniqueness: duplicate email in same scope+type fails",
  async () => {
    await withDatabase("smc-idx-scoped-dup", async (db) => {
      const catalog = await scopedMultiCollection(db, "catalog", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          user: {
            email: withIndex(v.string(), { unique: true }),
            name: v.string(),
          },
        },
      });

      const a = catalog.scope(EXPO_A);
      await a.insertOne("user", { email: "x@example.com", name: "Alice" });
      await assertRejects(
        () => a.insertOne("user", { email: "x@example.com", name: "Alice2" }),
      );
    });
  },
);

Deno.test("global uniqueness: same field across scopes is rejected", async () => {
  await withDatabase("smc-idx-global", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        catalog: {
          slug: withIndex(v.string(), { unique: true, global: true }),
          title: v.string(),
        },
      },
    });

    const a = catalog.scope(EXPO_A);
    const b = catalog.scope(EXPO_B);

    await a.insertOne("catalog", { slug: "shared-slug", title: "in A" });
    await assertRejects(
      () => b.insertOne("catalog", { slug: "shared-slug", title: "in B" }),
    );
  });
});

Deno.test(
  "uniqueness is scoped per-type: same field name across two types does not collide",
  async () => {
    await withDatabase("smc-idx-per-type", async (db) => {
      const catalog = await scopedMultiCollection(db, "catalog", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          user: {
            email: withIndex(v.string(), { unique: true }),
            name: v.string(),
          },
          admin: {
            email: withIndex(v.string(), { unique: true }),
            level: v.number(),
          },
        },
      });

      const a = catalog.scope(EXPO_A);
      await a.insertOne("user", { email: "x@example.com", name: "Alice" });
      // Same email but different type — must succeed
      await a.insertOne("admin", { email: "x@example.com", level: 9 });

      assertEquals(await a.countDocuments("user"), 1);
      assertEquals(await a.countDocuments("admin"), 1);
    });
  },
);

Deno.test(
  "scoped-unique index has compound shape { _scope:1, _type:1, field:1 }",
  async () => {
    await withDatabase("smc-idx-shape-scoped", async (db) => {
      await scopedMultiCollection(db, "catalog", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          user: {
            email: withIndex(v.string(), { unique: true }),
            name: v.string(),
          },
        },
      });

      const idx = await db.collection("catalog").indexes();
      const found = idx.find(
        (i) =>
          i.key?._scope === 1 &&
          i.key?._type === 1 &&
          i.key?.email === 1 &&
          i.unique === true,
      );
      assert(found, `expected scoped unique index, got ${JSON.stringify(idx)}`);
    });
  },
);

Deno.test(
  "global-unique index has compound shape { _type:1, field:1 } (no _scope)",
  async () => {
    await withDatabase("smc-idx-shape-global", async (db) => {
      await scopedMultiCollection(db, "catalog", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          catalog: {
            slug: withIndex(v.string(), { unique: true, global: true }),
            title: v.string(),
          },
        },
      });

      const idx = await db.collection("catalog").indexes();
      const found = idx.find(
        (i) =>
          i.key?._type === 1 &&
          i.key?.slug === 1 &&
          i.key?._scope === undefined &&
          i.unique === true,
      );
      assert(found, `expected global unique index, got ${JSON.stringify(idx)}`);
    });
  },
);

// [C1] regression : a second init that ADDS a sibling type sharing the same
// unique field name must not drop/recreate the first type's live index. Before
// the fix, the key-fallback matched the sibling's identical key pattern and
// oscillated on every init, leaving one type's uniqueness unenforced.
Deno.test(
  "double init: adding a sibling type with the same unique field keeps both indexes",
  async () => {
    await withDatabase("smc-idx-double-init", async (db) => {
      // First init : only `user`.
      await scopedMultiCollection(db, "catalog", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          user: {
            email: withIndex(v.string(), { unique: true }),
            name: v.string(),
          },
        },
      });

      const names1 = (await db.collection("catalog").indexes())
        .map((i) => i.name)
        .sort();
      assert(
        names1.includes("_scope__type_user_email"),
        `init1 missing user index: ${JSON.stringify(names1)}`,
      );

      // Second init : same collection, now with `admin` sharing `email`.
      const catalog = await scopedMultiCollection(db, "catalog", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          user: {
            email: withIndex(v.string(), { unique: true }),
            name: v.string(),
          },
          admin: {
            email: withIndex(v.string(), { unique: true }),
            level: v.number(),
          },
        },
      });

      const names2 = (await db.collection("catalog").indexes())
        .map((i) => i.name)
        .sort();

      // Both per-type unique indexes are present after the second init...
      assert(
        names2.includes("_scope__type_user_email"),
        `init2 dropped user index: ${JSON.stringify(names2)}`,
      );
      assert(
        names2.includes("_scope__type_admin_email"),
        `init2 missing admin index: ${JSON.stringify(names2)}`,
      );
      // ...and every index that existed after init 1 survived init 2 (no churn
      // of the user index — it was neither dropped nor renamed).
      for (const n of names1) {
        assert(
          names2.includes(n),
          `init2 dropped index ${n}: ${JSON.stringify(names2)}`,
        );
      }

      // Both per-type uniqueness constraints are actually enforced (proves the
      // user index is still live and not silently gone).
      const a = catalog.scope(EXPO_A);
      await a.insertOne("user", { email: "x@example.com", name: "Alice" });
      await a.insertOne("admin", { email: "x@example.com", level: 9 });
      await assertRejects(
        () => a.insertOne("user", { email: "x@example.com", name: "Dup" }),
      );
      await assertRejects(
        () => a.insertOne("admin", { email: "x@example.com", level: 1 }),
      );
    });
  },
);

// [C2] regression : re-opening a collection previously managed by
// `multiCollection` as a scoped multi-collection must clean up the legacy
// per-field indexes (`<type>_<field>`, key `{field:1}`, cross-scope unique) so
// the new scoped-unique semantics can allow the same value in two scopes.
Deno.test(
  "legacy multiCollection indexes are cleaned up when re-opened as scoped",
  async () => {
    await withDatabase("smc-idx-legacy-cleanup", async (db) => {
      // 1. Create the collection in the legacy `multiCollection` format. This
      //    produces `user_email` (key {email:1}, pfe {_type:"user"}, unique)
      //    which enforces CROSS-scope uniqueness.
      const legacyModel = defineModel("catalog", {
        schema: {
          user: {
            email: withIndex(v.string(), { unique: true }),
            name: v.string(),
          },
        },
      });
      await multiCollection(db, "catalog", legacyModel, {
        schemaManagement: "auto",
      });

      const before = await db.collection("catalog").indexes();
      assert(
        before.some((i) => i.name === "user_email"),
        `expected legacy user_email index, got ${JSON.stringify(before)}`,
      );

      // 2. Re-open the same collection name as a scoped multi-collection with
      //    the same types.
      const catalog = await scopedMultiCollection(db, "catalog", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          user: {
            email: withIndex(v.string(), { unique: true }),
            name: v.string(),
          },
        },
      });

      const after = await db.collection("catalog").indexes();
      // 3a. Legacy unique per-field index is gone...
      assert(
        !after.some((i) => i.name === "user_email"),
        `legacy user_email index should be dropped, got ${
          JSON.stringify(after)
        }`,
      );
      // ...the bare _type_1 index is preserved...
      assert(
        after.some((i) => i.name === "_type_1"),
        `bare _type_1 index should be preserved, got ${JSON.stringify(after)}`,
      );
      // ...and the scoped compound unique index exists.
      assert(
        after.some((i) =>
          i.key?._scope === 1 && i.key?._type === 1 && i.key?.email === 1 &&
          i.unique === true
        ),
        `expected scoped unique index, got ${JSON.stringify(after)}`,
      );

      // 3b. The same email value can now be inserted in two different scopes
      //     without E11000.
      const a = catalog.scope(EXPO_A);
      const b = catalog.scope(EXPO_B);
      await a.insertOne("user", { email: "shared@example.com", name: "Alice" });
      await b.insertOne("user", { email: "shared@example.com", name: "Bob" });
      assertEquals(await a.countDocuments("user"), 1);
      assertEquals(await b.countDocuments("user"), 1);
    });
  },
);

// [N3] The default paginate sort appends `_id` as a tie-breaker, so the
// effective default sort is `{_id:1}` under `{_scope,_type}` equality. The base
// index must end in `_id` for that sort to ride an index — hence
// `{_scope:1,_type:1,_id:1}`. This test pins the exact key shape AND asserts the
// index set is byte-for-byte stable across two successive inits (no oscillation
// churn from the reconcile loop).
Deno.test(
  "N3: base index is {_scope:1,_type:1,_id:1} and stable across re-inits",
  async () => {
    await withDatabase("smc-idx-base-idn3", async (db) => {
      const cfg = {
        scope: refId("exposition"),
        types: { artwork: { title: v.string() } },
        schemaManagement: "auto" as const,
      };

      await scopedMultiCollection(db, "catalog", cfg);
      const first = await db.collection("catalog").indexes();
      const base = first.find((i) => i.name === "_scope_1__type_1__id_1");
      assert(
        base,
        `expected base index _scope_1__type_1__id_1, got ${
          JSON.stringify(first.map((i) => i.name))
        }`,
      );
      // Trailing _id is what makes the default paginate sort index-served.
      assertEquals(base?.key, { _scope: 1, _type: 1, _id: 1 });
      const names1 = first.map((i) => i.name).sort();

      // Second init on the same collection must not add, drop, or rename any
      // index (the reconcile loop sees matching name+key+options → no-op).
      await scopedMultiCollection(db, "catalog", cfg);
      const names2 = (await db.collection("catalog").indexes())
        .map((i) => i.name)
        .sort();
      assertEquals(
        names2,
        names1,
        `re-init changed the index set: ${JSON.stringify(names1)} -> ${
          JSON.stringify(names2)
        }`,
      );
    });
  },
);

// [N3] Migration : a collection carrying the pre-N3 base index
// (`_scope_1__type_1`, key `{_scope:1,_type:1}`) must have it dropped and
// replaced by the `_id`-terminated base index — idempotently, with no
// oscillation on a repeated init.
Deno.test(
  "N3 migration: old {_scope:1,_type:1} base index is replaced by {_scope:1,_type:1,_id:1}",
  async () => {
    await withDatabase("smc-idx-base-migration", async (db) => {
      // Simulate a pre-N3 collection : create it and hand-plant the OLD base
      // index shape/name.
      await db.createCollection("catalog");
      await db.collection("catalog").createIndex(
        { _scope: 1, _type: 1 },
        { name: "_scope_1__type_1" },
      );

      const cfg = {
        scope: refId("exposition"),
        types: { artwork: { title: v.string() } },
        schemaManagement: "auto" as const,
      };

      // First (migrating) init.
      await scopedMultiCollection(db, "catalog", cfg);
      const afterNames1 = (await db.collection("catalog").indexes())
        .map((i) => i.name)
        .sort();
      assert(
        !afterNames1.includes("_scope_1__type_1"),
        `old base index should be dropped, got ${JSON.stringify(afterNames1)}`,
      );
      assert(
        afterNames1.includes("_scope_1__type_1__id_1"),
        `new base index should exist, got ${JSON.stringify(afterNames1)}`,
      );

      // Second init : must be a fixed point — the old index does not come back
      // and nothing else churns.
      await scopedMultiCollection(db, "catalog", cfg);
      const afterNames2 = (await db.collection("catalog").indexes())
        .map((i) => i.name)
        .sort();
      assertEquals(
        afterNames2,
        afterNames1,
        `second init oscillated: ${JSON.stringify(afterNames1)} -> ${
          JSON.stringify(afterNames2)
        }`,
      );
    });
  },
);

// [N4] The unscoped admin view queries `{_type: ...}` with no `_scope` term. A
// non-unique `{_type:1}` index (named `_type_1`, matching the plain
// multiCollection applier so a converted collection adopts it) must always be
// present to avoid a COLLSCAN.
Deno.test(
  "N4: a non-unique {_type:1} index (_type_1) is always created",
  async () => {
    await withDatabase("smc-idx-type-index", async (db) => {
      await scopedMultiCollection(db, "catalog", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: {
          artwork: { title: v.string() },
          artist: { name: v.string() },
        },
      });

      const idx = await db.collection("catalog").indexes();
      const found = idx.find((i) => i.name === "_type_1");
      assert(
        found,
        `expected _type_1 index, got ${JSON.stringify(idx.map((i) => i.name))}`,
      );
      assertEquals(found?.key, { _type: 1 });
      assert(!found?.unique, "the _type index must be non-unique");
    });
  },
);

// [N5] `global` is a scoped-multi-collection sentinel that MongoDB does not
// understand. A plain `collection()` (and `multiCollection`) must strip it
// before createIndex — otherwise MongoDB rejects the spec with
// InvalidIndexSpecificationOption (197) and init throws.
Deno.test(
  "N5: plain collection() with { global: true } index metadata initializes without throwing",
  async () => {
    await withDatabase("smc-idx-global-strip", async (db) => {
      const users = await collection(db, "users", {
        slug: withIndex(v.string(), { unique: true, global: true }),
        name: v.string(),
      }, { schemaManagement: "auto" });

      // Init did not throw. The `global` sentinel was stripped but the `unique`
      // index was still created and is enforced.
      const idx = await db.collection("users").indexes();
      assert(
        idx.some((i) => i.key?.slug === 1 && i.unique === true),
        `expected a unique slug index, got ${JSON.stringify(idx)}`,
      );
      await users.insertOne({ slug: "a", name: "n1" });
      await assertRejects(
        () => users.insertOne({ slug: "a", name: "n2" }),
      );
    });
  },
);

// [N5] Same guarantee on the `multiCollection` applier path.
Deno.test(
  "N5: multiCollection with { global: true } index metadata initializes without throwing",
  async () => {
    await withDatabase("smc-idx-global-strip-mc", async (db) => {
      const model = defineModel("catalog", {
        schema: {
          item: {
            slug: withIndex(v.string(), { unique: true, global: true }),
            name: v.string(),
          },
        },
      });

      await multiCollection(db, "catalog", model, {
        schemaManagement: "auto",
      });

      const idx = await db.collection("catalog").indexes();
      assert(
        idx.some((i) => i.name === "item_slug" && i.unique === true),
        `expected unique item_slug index, got ${
          JSON.stringify(idx.map((i) => i.name))
        }`,
      );
    });
  },
);

// [N5] Hermetic guard for the metadata strip. The end-to-end tests above pass
// even without the fix on drivers/servers that silently drop unknown index
// options (e.g. MongoDB 8.x drops `global`), so they cannot by themselves prove
// the sentinel is stripped. These tests intercept the exact options handed to
// createIndex and assert `global` never leaks — they fail if the strip is
// removed, regardless of server tolerance.
function fakeIndexCollection(name: string) {
  const created: Array<{ key: unknown; options: Record<string, unknown> }> = [];
  const dropped: string[] = [];
  const collection = {
    collectionName: name,
    indexes: () => Promise.resolve([{ name: "_id_", key: { _id: 1 } }]),
    createIndex: (key: unknown, options: Record<string, unknown>) => {
      created.push({ key, options });
      return Promise.resolve(options?.name as string);
    },
    dropIndex: (n: string) => {
      dropped.push(n);
      return Promise.resolve();
    },
  };
  return { collection, created, dropped };
}

Deno.test(
  "N5 (unit): applyCollectionIndexes strips `global` before createIndex",
  async () => {
    const { collection, created } = fakeIndexCollection("users");
    const schema = v.object({
      slug: withIndex(v.string(), { unique: true, global: true }),
      name: v.string(),
    });
    // deno-lint-ignore no-explicit-any
    await applyCollectionIndexes(collection as any, schema as any);

    const slug = created.find((c) => c.options.name === "slug");
    assert(
      slug,
      `expected a createIndex for slug, got ${JSON.stringify(created)}`,
    );
    // The valid option survives...
    assertEquals(slug?.options.unique, true);
    // ...but the mongodbee-only sentinel never reaches MongoDB.
    assert(
      !("global" in (slug?.options ?? {})),
      `global must be stripped, got ${JSON.stringify(slug?.options)}`,
    );
  },
);

Deno.test(
  "N5 (unit): applyMultiCollectionIndexes strips `global` before createIndex",
  async () => {
    const { collection, created } = fakeIndexCollection("catalog");
    const schemasPerType = {
      item: v.object({
        slug: withIndex(v.string(), { unique: true, global: true }),
        name: v.string(),
      }),
    };
    // deno-lint-ignore no-explicit-any
    await applyMultiCollectionIndexes(collection as any, schemasPerType as any);

    const slug = created.find((c) => c.options.name === "item_slug");
    assert(
      slug,
      `expected a createIndex for item_slug, got ${JSON.stringify(created)}`,
    );
    assertEquals(slug?.options.unique, true);
    assert(
      !("global" in (slug?.options ?? {})),
      `global must be stripped, got ${JSON.stringify(slug?.options)}`,
    );
  },
);

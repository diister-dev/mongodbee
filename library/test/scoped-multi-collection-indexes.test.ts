import { assert, assertEquals, assertRejects } from "@std/assert";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

Deno.test("auto-index: { _scope: 1, _type: 1 } is created at init", async () => {
  await withDatabase("smc-idx-base", async (db) => {
    await scopedMultiCollection(db, "catalog", {
      scope: refId("exposition"),
      types: { artwork: { title: v.string() } },
    });

    const idx = await db.collection("catalog").indexes();
    const found = idx.find((i) =>
      i.key && i.key._scope === 1 && i.key._type === 1
    );
    assert(found, `expected {_scope:1,_type:1} index, got ${JSON.stringify(idx)}`);
  });
});

Deno.test(
  "scoped uniqueness: same email in two scopes is allowed",
  async () => {
    await withDatabase("smc-idx-scoped-unique", async (db) => {
      const catalog = await scopedMultiCollection(db, "catalog", {
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

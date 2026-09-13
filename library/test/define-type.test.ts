import { test } from "./+harness.ts";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { collection } from "../src/collection.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { desc, index, unique, withIndex } from "../src/indexes.ts";
import { defineType, isTypeDefinition } from "../src/type-definition.ts";
import { projectComposites } from "../src/indexes-applier.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";
const USER_A = "user:aaaaaaaaaa";
const USER_B = "user:bbbbbbbbbb";

const UserRole = defineType({
  schema: v.object({
    userId: refId("user"),
    roleId: refId("role"),
  }),
  indexes: (f) => [unique(f.userId, f.roleId)],
});

const TeamRole = defineType({
  schema: v.object({
    userId: refId("user"),
    roleId: refId("role"),
    status: v.picklist(["active", "revoked"]),
    addedAt: v.date(),
    addedBy: v.object({
      kind: v.picklist(["user", "system"]),
      userId: v.optional(refId("user")),
    }),
    context: v.object({
      organizationId: v.optional(refId("expo_organization")),
    }),
  }),
  indexes: (f) => [
    unique(f.userId, f.roleId).where(f.status, "active").named("active_role"),
    index(f.roleId, desc(f.addedAt), desc(f._id)).named("roster"),
    index(f.addedBy.userId, desc(f.addedAt)).where(f.addedBy.kind, "user"),
    index(f.context.organizationId, f.roleId).exists(f.context.organizationId),
  ],
});

test("defineType: exposes the schema, its entries and the resolved indexes", () => {
  assert(isTypeDefinition(UserRole));
  assertEquals(Object.keys(UserRole.entries), ["userId", "roleId"]);
  assertEquals(UserRole.indexes, [
    { key: { userId: 1, roleId: 1 }, unique: true },
  ]);

  const parsed = v.parse(UserRole.schema, {
    userId: USER_A,
    roleId: "role:ORGANIZER",
  });
  assertEquals(Object.keys(parsed).sort(), ["roleId", "userId"]);
});

test("defineType: the declaration derives from the fields, nested paths included", () => {
  assertEquals(TeamRole.indexes[0], {
    name: "active_role",
    key: { userId: 1, roleId: 1 },
    unique: true,
    partialFilterExpression: { status: "active" },
  });
  assertEquals(TeamRole.indexes[1], {
    name: "roster",
    key: { roleId: 1, addedAt: -1, _id: -1 },
  });
  assertEquals(TeamRole.indexes[2], {
    key: { "addedBy.userId": 1, addedAt: -1 },
    partialFilterExpression: { "addedBy.kind": "user" },
  });
  assertEquals(TeamRole.indexes[3], {
    key: { "context.organizationId": 1, roleId: 1 },
    partialFilterExpression: { "context.organizationId": { $exists: true } },
  });
});

test("defineType: a typo in a field path is a compile error and a runtime error", () => {
  let thrown: unknown;
  try {
    defineType({
      schema: v.object({
        userId: refId("user"),
        addedBy: v.object({ kind: v.string() }),
      }),
      indexes: (f) => [
        // @ts-expect-error unknown field
        unique(f.userIdd),
        // @ts-expect-error unknown nested field
        index(f.addedBy.kindd),
      ],
    });
  } catch (error) {
    thrown = error;
  }
  assertEquals(
    (thrown as Error | undefined)?.message,
    'defineType: key path "userIdd" does not exist in the schema',
  );
});

test("defineType: several where clauses are AND-merged", () => {
  const Type = defineType({
    schema: v.object({ a: v.string(), b: v.string(), c: v.string() }),
    indexes: (f) => [
      index(f.a)
        .where(f.b, "x")
        .where(f.c, { $in: ["y", "z"] }),
    ],
  });
  assertEquals(Type.indexes[0].partialFilterExpression, {
    $and: [{ b: "x" }, { c: { $in: ["y", "z"] } }],
  });
});

test("projection: one declaration, three families", () => {
  const plain = projectComposites(UserRole.indexes, "collection");
  assertEquals(plain[0].name, "_idx_userId_asc_roleId_asc");
  assertEquals(plain[0].key, { userId: 1, roleId: 1 });
  assertEquals(plain[0].options.partialFilterExpression, undefined);

  const multi = projectComposites(UserRole.indexes, "multi", "user_role");
  assertEquals(multi[0].name, "user_role__idx_userId_asc_roleId_asc");
  assertEquals(multi[0].options.partialFilterExpression, {
    _type: { $eq: "user_role" },
  });

  const scoped = projectComposites(UserRole.indexes, "scoped", "user_role");
  assertEquals(
    scoped[0].name,
    "_scope__type_user_role__idx_userId_asc_roleId_asc",
  );
  assertEquals(scoped[0].key, { _scope: 1, _type: 1, userId: 1, roleId: 1 });
  assertEquals(scoped[0].options.unique, true);
});

test("projection: acrossScopes drops _scope, a custom name keeps the owned prefix", () => {
  const Page = defineType({
    schema: v.object({ slug: v.string(), locale: v.string() }),
    indexes: (f) => [
      unique(f.slug, f.locale).acrossScopes().named("public_slug"),
    ],
  });
  const scoped = projectComposites(Page.indexes, "scoped", "page");
  assertEquals(scoped[0].name, "__type_page__idx_public_slug");
  assertEquals(scoped[0].key, { _type: 1, slug: 1, locale: 1 });
});

test("projection: two directions on the same path derive distinct names", () => {
  const asc = projectComposites(
    defineType({
      schema: v.object({ at: v.date() }),
      indexes: (f) => [index(f.at)],
    }).indexes,
    "collection",
  );
  const descending = projectComposites(
    defineType({
      schema: v.object({ at: v.date() }),
      indexes: (f) => [index(desc(f.at))],
    }).indexes,
    "collection",
  );
  assert(asc[0].name !== descending[0].name, `same name: ${asc[0].name}`);
});

test("live scoped: the composite unique rejects the duplicate assignment", async () => {
  await withDatabase("dt-scoped", async (db) => {
    const expo = await scopedMultiCollection(db, "+expositions", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { user_role: UserRole },
    });

    const a = expo.scope(EXPO_A);
    const b = expo.scope(EXPO_B);

    await a.insertOne("user_role", {
      userId: USER_A,
      roleId: "role:ORGANIZER",
    });
    await assertRejects(
      () =>
        a.insertOne("user_role", { userId: USER_A, roleId: "role:ORGANIZER" }),
      undefined,
      undefined,
      "a duplicate (userId, roleId) must be refused",
    );
    await a.insertOne("user_role", { userId: USER_A, roleId: "role:SECURITY" });
    await a.insertOne("user_role", {
      userId: USER_B,
      roleId: "role:ORGANIZER",
    });
    await b.insertOne("user_role", {
      userId: USER_A,
      roleId: "role:ORGANIZER",
    });

    assertEquals(await a.countDocuments("user_role"), 3);
    assertEquals(await b.countDocuments("user_role"), 1);
  });
});

test("live scoped: the index is owned by the chain, stable across inits, pruned on removal", async () => {
  await withDatabase("dt-owned", async (db) => {
    const name = "_scope__type_user_role__idx_userId_asc_roleId_asc";

    await scopedMultiCollection(db, "+expositions", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { user_role: UserRole },
    });
    const first = await db.collection("+expositions").indexes();
    assert(
      first.some((i) => i.name === name),
      `missing ${name}`,
    );

    await scopedMultiCollection(db, "+expositions", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { user_role: UserRole },
    });
    const second = await db.collection("+expositions").indexes();
    assertEquals(
      second.map((i) => i.name).sort(),
      first.map((i) => i.name).sort(),
    );

    await scopedMultiCollection(db, "+expositions", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { user_role: { userId: refId("user"), roleId: refId("role") } },
    });
    const third = await db.collection("+expositions").indexes();
    assert(!third.some((i) => i.name === name), "index survived the removal");
  });
});

test("live scoped: the rich shape behaves as declared", async () => {
  await withDatabase("dt-rich", async (db) => {
    const expo = await scopedMultiCollection(db, "+expositions", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { user_role: TeamRole },
    });

    const a = expo.scope(EXPO_A);
    const organizer = {
      userId: USER_A,
      roleId: "role:ORGANIZER",
      status: "active" as const,
      addedAt: new Date("2026-09-01T10:00:00Z"),
      addedBy: { kind: "user" as const, userId: USER_B },
      context: {},
    };

    await a.insertOne("user_role", organizer);
    await assertRejects(
      () => a.insertOne("user_role", { ...organizer, addedAt: new Date() }),
      undefined,
      undefined,
      "a second active ORGANIZER row for the same user must collide",
    );
    await a.insertOne("user_role", { ...organizer, status: "revoked" });
    await a.insertOne("user_role", { ...organizer, status: "revoked" });
    await a.insertOne("user_role", {
      ...organizer,
      roleId: "role:SECURITY",
      context: { organizationId: "expo_organization:orgaaaaaaa" },
    });
    assertEquals(await a.countDocuments("user_role"), 4);

    const live = (await db.collection("+expositions").indexes())
      .map((i) => i.name)
      .filter((n) => n?.includes("__idx_"))
      .sort();
    assertEquals(live, [
      "_scope__type_user_role__idx_active_role",
      "_scope__type_user_role__idx_addedBy.userId_asc_addedAt_desc",
      "_scope__type_user_role__idx_context.organizationId_asc_roleId_asc",
      "_scope__type_user_role__idx_roster",
    ]);

    const roster = (await db.collection("+expositions").indexes()).find(
      (i) => i.name === "_scope__type_user_role__idx_roster",
    );
    assertEquals(roster?.key, {
      _scope: 1,
      _type: 1,
      roleId: 1,
      addedAt: -1,
      _id: -1,
    });
  });
});

test("live scoped: a dirty corpus makes the index application fail loudly", async () => {
  await withDatabase("dt-dirty", async (db) => {
    const clean = await scopedMultiCollection(db, "+expositions", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { user_role: { userId: refId("user"), roleId: refId("role") } },
    });
    const a = clean.scope(EXPO_A);
    await a.insertOne("user_role", {
      userId: USER_A,
      roleId: "role:ORGANIZER",
    });
    await a.insertOne("user_role", {
      userId: USER_A,
      roleId: "role:ORGANIZER",
    });

    let thrown: unknown;
    try {
      await scopedMultiCollection(db, "+expositions", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: { user_role: UserRole },
      });
    } catch (error) {
      thrown = error;
    }
    assert(thrown instanceof Error, "declaring the unique must fail");
    assert(
      (thrown as Error).message.includes("E11000"),
      (thrown as Error).message,
    );
  });
});

test("live scoped: the definition survives a module-style aggregation chain", async () => {
  await withDatabase("dt-aggregation", async (db) => {
    const coreSubCollectionSchemas = {
      user_role: UserRole,
      information: { name: v.string() },
    };
    const badgesSubCollectionSchemas = { badge: { label: v.string() } };

    const model = defineModel("exposition", {
      schema: { ...coreSubCollectionSchemas, ...badgesSubCollectionSchemas },
    });
    assertEquals(model.getSummary().fieldCount, 4);

    const expo = await scopedMultiCollection(db, "+expositions", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: model.schema,
    });

    const a = expo.scope(EXPO_A);
    await a.insertOne("user_role", {
      userId: USER_A,
      roleId: "role:ORGANIZER",
    });
    await assertRejects(
      () =>
        a.insertOne("user_role", { userId: USER_A, roleId: "role:ORGANIZER" }),
      undefined,
      undefined,
      "the constraint must survive the aggregation chain",
    );
    await a.insertOne("information", { name: "Salon" });
    await a.insertOne("badge", { label: "VIP" });
  });
});

test("live scoped: withIndex on a field and defineType indexes coexist", async () => {
  await withDatabase("dt-coexist", async (db) => {
    await scopedMultiCollection(db, "+expositions", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        user_role: defineType({
          schema: v.object({
            userId: withIndex(refId("user")),
            roleId: refId("role"),
            addedAt: v.date(),
          }),
          indexes: (f) => [index(f.roleId, desc(f.addedAt))],
        }),
      },
    });

    const names = (await db.collection("+expositions").indexes())
      .map((i) => i.name)
      .filter((n) => n?.startsWith("_scope__type_user_role"))
      .sort();
    assertEquals(names, [
      "_scope__type_user_role__idx_roleId_asc_addedAt_desc",
      "_scope__type_user_role_userId",
    ]);
  });
});

test("live multi: the same definition constrains a plain multi-collection", async () => {
  await withDatabase("dt-multi", async (db) => {
    const mc = await multiCollection(
      db,
      "roles",
      { user_role: UserRole },
      { schemaManagement: "auto" },
    );

    await mc.insertOne("user_role", {
      userId: USER_A,
      roleId: "role:ORGANIZER",
    });
    await assertRejects(
      () =>
        mc.insertOne("user_role", { userId: USER_A, roleId: "role:ORGANIZER" }),
      undefined,
      undefined,
      "a duplicate must be refused in a multi-collection too",
    );

    const idx = await db.collection("roles").indexes();
    assert(idx.some((i) => i.name === "user_role__idx_userId_asc_roleId_asc"));
  });
});

test("live multi: a model carrying definitions keeps its constraints", async () => {
  await withDatabase("dt-multi-model", async (db) => {
    const model = defineModel("roles", { schema: { user_role: UserRole } });
    const mc = await multiCollection(db, "roles", model, {
      schemaManagement: "auto",
    });

    await mc.insertOne("user_role", {
      userId: USER_A,
      roleId: "role:ORGANIZER",
    });
    await assertRejects(
      () =>
        mc.insertOne("user_role", { userId: USER_A, roleId: "role:ORGANIZER" }),
      undefined,
      undefined,
      "a duplicate must be refused through a model too",
    );
  });
});

test("live plain: the same definition constrains a simple collection", async () => {
  await withDatabase("dt-plain", async (db) => {
    const roles = await collection(db, "user_roles", UserRole, {
      schemaManagement: "auto",
    });

    await roles.insertOne({ userId: USER_A, roleId: "role:ORGANIZER" });
    await assertRejects(
      () => roles.insertOne({ userId: USER_A, roleId: "role:ORGANIZER" }),
      undefined,
      undefined,
      "a duplicate must be refused in a plain collection too",
    );

    const idx = await db.collection("user_roles").indexes();
    assert(idx.some((i) => i.name === "_idx_userId_asc_roleId_asc"));
  });
});

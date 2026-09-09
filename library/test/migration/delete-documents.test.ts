import { test } from "../+harness.ts";
import {
  assert,
  assertEquals,
  assertRejects,
  assertThrows,
} from "../+assert.ts";
import { withDatabase } from "../+shared.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import {
  getIrreversibleOperations,
  getMigrationSummary,
  migrationBuilder,
} from "../../src/migration/builder.ts";
import { createMemoryApplier } from "../../src/migration/appliers/memory.ts";
import { createMongodbApplier } from "../../src/migration/appliers/mongodb.ts";
import {
  createEmptyDatabaseState,
  type MigrationDefinition,
} from "../../src/migration/types.ts";
import { createSimulationValidator } from "../../src/migration/validators/simulation.ts";
import { refId } from "../../src/ids.ts";
import * as v from "../../src/schema.ts";

const SCHEMAS = {
  collections: {
    users: {
      _id: refId("user"),
      name: v.string(),
      age: v.number(),
      status: v.picklist(["active", "banned"]),
    },
    posts: { _id: refId("post"), authorId: refId("user"), title: v.string() },
  },
  multiModels: {
    shop: {
      item: { _id: refId("item"), sku: v.string(), stock: v.number() },
      order: { _id: refId("order"), itemId: refId("item"), qty: v.number() },
    },
  },
  scopedMultiCollections: {
    "+expo": {
      scope: refId("exposition"),
      types: {
        ticket: { _id: refId("ticket"), holder: v.string(), used: v.boolean() },
      },
    },
  },
};

function parentMigration(): MigrationDefinition {
  return migrationDefinition("001", "seed-everything", {
    parent: null,
    schemas: SCHEMAS,
    migrate: (b) => {
      b.createCollection("users")
        .seed([
          { _id: "user:a", name: "alice", age: 30, status: "active" },
          { _id: "user:b", name: "bob", age: 17, status: "banned" },
          { _id: "user:c", name: "carol", age: 45, status: "banned" },
        ])
        .end();
      b.createCollection("posts")
        .seed([{ _id: "post:1", authorId: "user:b", title: "hello" }])
        .end();
      b.createMultiModelInstance("shop:main", "shop")
        .type("item")
        .seed([
          { _id: "item:1", sku: "A", stock: 0 },
          { _id: "item:2", sku: "B", stock: 5 },
        ])
        .end()
        .type("order")
        .seed([{ _id: "order:1", itemId: "item:1", qty: 1 }])
        .end()
        .end();
      b.createMultiModelInstance("shop:other", "shop")
        .type("item")
        .seed([{ _id: "item:3", sku: "C", stock: 0 }])
        .end()
        .end();
      b.createScopedMultiCollection("+expo")
        .type("ticket")
        .seed("exposition:x", [
          { _id: "ticket:1", holder: "alice", used: true },
          { _id: "ticket:2", holder: "bob", used: false },
        ])
        .seed("exposition:y", [
          { _id: "ticket:3", holder: "carol", used: true },
        ])
        .end()
        .end();
      return b.compile();
    },
  });
}

function childMigration(
  parent: MigrationDefinition,
  migrate: Parameters<typeof migrationDefinition>[2]["migrate"],
): MigrationDefinition {
  return migrationDefinition("002", "delete-things", {
    parent,
    schemas: SCHEMAS,
    migrate,
  });
}

async function memoryStateAfter(
  parent: MigrationDefinition,
  child: MigrationDefinition,
) {
  const parentOps = parent.migrate(
    migrationBuilder({ schemas: parent.schemas }),
  ).operations;
  const childOps = child.migrate(
    migrationBuilder({
      schemas: child.schemas,
      parentSchemas: parent.schemas,
    }),
  ).operations;
  const applier = createMemoryApplier(parent);
  let state = await applier.applyMigration(
    createEmptyDatabaseState(),
    parentOps,
    "up",
  );
  state = await createMemoryApplier(child).applyMigration(
    state,
    childOps,
    "up",
  );
  return { state, childOps };
}

test("deleteWhere on a collection: operators, irreversibility, memory and mongo agree", async () => {
  const parent = parentMigration();
  const child = childMigration(parent, (b) => {
    b.collection("users")
      .deleteWhere({ status: "banned", age: { $lt: 18 } })
      .end();
    return b.compile();
  });
  const { state, childOps } = await memoryStateAfter(parent, child);
  assertEquals(
    state.collections.users.content.map((u) => String(u._id)),
    ["user:a", "user:c"],
  );
  assertEquals(getIrreversibleOperations(childOps).length, 1);
  const compiled = child.migrate(
    migrationBuilder({ schemas: child.schemas, parentSchemas: parent.schemas }),
  );
  assertEquals(getMigrationSummary(compiled).deletes, 1);
  assert(getMigrationSummary(compiled).isIrreversible);
  await assertRejects(
    () => createMemoryApplier(child).applyMigration(state, childOps, "down"),
    Error,
    "irreversible",
  );

  await withDatabase("delete-collection-documents", async (db) => {
    const parentOps = parent.migrate(
      migrationBuilder({ schemas: parent.schemas }),
    ).operations;
    await createMongodbApplier(db, parent, {
      currentMigrationId: parent.id,
    }).applyMigration(parentOps, "up");
    await createMongodbApplier(db, child, {
      currentMigrationId: child.id,
    }).applyMigration(childOps, "up");
    const ids = (
      await db.collection("users").find({}).sort({ _id: 1 }).toArray()
    ).map((u) => String(u._id));
    assertEquals(ids, ["user:a", "user:c"]);
  });
});

test("deleteWhere on a scoped type honours the scope filter and never leaves its type", async () => {
  const parent = parentMigration();
  const child = childMigration(parent, (b) => {
    b.scopedMultiCollection("+expo")
      .type("ticket")
      .deleteWhere({ used: true }, { scopeFilter: ["exposition:x"] })
      .end()
      .end();
    return b.compile();
  });
  const { state, childOps } = await memoryStateAfter(parent, child);
  const remaining = state.scopedMultiCollections["+expo"].content
    .map((d) => String(d._id))
    .sort();
  assertEquals(remaining, ["ticket:2", "ticket:3"]);

  await withDatabase("delete-scoped-documents", async (db) => {
    const parentOps = parent.migrate(
      migrationBuilder({ schemas: parent.schemas }),
    ).operations;
    await createMongodbApplier(db, parent, {
      currentMigrationId: parent.id,
    }).applyMigration(parentOps, "up");
    await createMongodbApplier(db, child, {
      currentMigrationId: child.id,
    }).applyMigration(childOps, "up");
    const ids = (
      await db
        .collection("+expo")
        .find({ _type: "ticket" })
        .sort({ _id: 1 })
        .toArray()
    ).map((d) => String(d._id));
    assertEquals(ids, ["ticket:2", "ticket:3"]);
  });
});

test("deleteWhere on one multi-model instance and on every instance of a model", async () => {
  const parent = parentMigration();
  const one = childMigration(parent, (b) => {
    b.multiModelInstance("shop:main", "shop")
      .type("item")
      .deleteWhere({
        stock: 0,
      })
      .end()
      .end();
    return b.compile();
  });
  const { state: afterOne } = await memoryStateAfter(parent, one);
  assertEquals(
    afterOne.multiModels["shop:main"].content.map((d) => String(d._id)),
    ["item:2", "order:1"],
  );
  assertEquals(
    afterOne.multiModels["shop:other"].content.map((d) => String(d._id)),
    ["item:3"],
  );

  const all = childMigration(parent, (b) => {
    b.multiModelInstances("shop")
      .type("item")
      .deleteWhere({ stock: 0 })
      .end()
      .end();
    return b.compile();
  });
  const { state: afterAll, childOps } = await memoryStateAfter(parent, all);
  assertEquals(
    afterAll.multiModels["shop:main"].content.map((d) => String(d._id)),
    ["item:2", "order:1"],
  );
  assertEquals(
    afterAll.multiModels["shop:other"].content.map((d) => String(d._id)),
    [],
  );

  await withDatabase("delete-multimodel-documents", async (db) => {
    const parentOps = parent.migrate(
      migrationBuilder({ schemas: parent.schemas }),
    ).operations;
    await createMongodbApplier(db, parent, {
      currentMigrationId: parent.id,
    }).applyMigration(parentOps, "up");
    await createMongodbApplier(db, all, {
      currentMigrationId: all.id,
    }).applyMigration(childOps, "up");
    assertEquals(
      (await db.collection("shop:main").find({ _type: "item" }).toArray()).map(
        (d) => String(d._id),
      ),
      ["item:2"],
    );
    assertEquals(
      await db.collection("shop:other").countDocuments({ _type: "item" }),
      0,
    );
  });
});

test("simulation warns when a deletion matches nothing and when it leaves dangling references", async () => {
  const parent = parentMigration();
  const silent = childMigration(parent, (b) => {
    b.collection("users").deleteWhere({ name: "nobody-has-this-name" }).end();
    return b.compile();
  });
  const validator = createSimulationValidator({ powerLevel: "quick" });
  const nothing = await validator.validateMigration(silent);
  assert(
    nothing.warnings.some((w) => w.includes("matched no simulated document")),
    nothing.warnings.join("\n"),
  );

  const orphaning = childMigration(parent, (b) => {
    b.collection("users").deleteWhere({}).end();
    return b.compile();
  });
  const dangling = await validator.validateMigration(orphaning);
  assert(
    dangling.warnings.some(
      (w) => w.includes("still referenced") && w.includes("authorId"),
    ),
    dangling.warnings.join("\n"),
  );
});

const ISOLATION = {
  multiCollections: {
    "+tenants": {
      role: { _id: refId("role"), name: v.string(), flagged: v.boolean() },
      member: { _id: refId("member"), name: v.string(), flagged: v.boolean() },
    },
  },
  scopedMultiCollections: {
    "+expo": {
      scope: refId("exposition"),
      types: {
        ticket: {
          _id: refId("ticket"),
          holder: v.string(),
          flagged: v.boolean(),
        },
        badge: {
          _id: refId("badge"),
          holder: v.string(),
          flagged: v.boolean(),
        },
      },
    },
  },
};

function isolationParent(): MigrationDefinition {
  return migrationDefinition("001", "seed-isolation", {
    parent: null,
    schemas: ISOLATION,
    migrate: (b) => {
      b.createMultiCollection("+tenants")
        .type("role")
        .seed([{ _id: "role:1", name: "x", flagged: true }])
        .end()
        .type("member")
        .seed([{ _id: "member:1", name: "x", flagged: true }])
        .end()
        .end();
      b.createScopedMultiCollection("+expo")
        .type("ticket")
        .seed("exposition:a", [
          { _id: "ticket:a1", holder: "x", flagged: true },
          { _id: "ticket:a2", holder: "x", flagged: false },
        ])
        .seed("exposition:b", [
          {
            _id: "ticket:b1",
            holder: "x",
            flagged: true,
          },
        ])
        .seed("exposition:c", [
          {
            _id: "ticket:c1",
            holder: "x",
            flagged: true,
          },
        ])
        .end()
        .type("badge")
        .seed("exposition:a", [{ _id: "badge:a1", holder: "x", flagged: true }])
        .end()
        .end();
      return b.compile();
    },
  });
}

async function isolationRun(
  child: MigrationDefinition,
  parent: MigrationDefinition,
) {
  const parentOps = parent.migrate(
    migrationBuilder({ schemas: parent.schemas }),
  ).operations;
  const childOps = child.migrate(
    migrationBuilder({
      schemas: child.schemas,
      parentSchemas: parent.schemas,
    }),
  ).operations;
  let state = await createMemoryApplier(parent).applyMigration(
    createEmptyDatabaseState(),
    parentOps,
    "up",
  );
  state = await createMemoryApplier(child).applyMigration(
    state,
    childOps,
    "up",
  );
  const memoryIds = {
    tenants: state.multiCollections["+tenants"].content
      .map((d) => String(d._id))
      .sort(),
    expo: state.scopedMultiCollections["+expo"].content
      .map((d) => String(d._id))
      .sort(),
  };
  let mongoIds = { tenants: [] as string[], expo: [] as string[] };
  await withDatabase("delete-isolation", async (db) => {
    await createMongodbApplier(db, parent, {
      currentMigrationId: parent.id,
    }).applyMigration(parentOps, "up");
    await createMongodbApplier(db, child, {
      currentMigrationId: child.id,
    }).applyMigration(childOps, "up");
    mongoIds = {
      tenants: (await db.collection("+tenants").find({}).toArray())
        .map((d) => String(d._id))
        .sort(),
      expo: (await db.collection("+expo").find({}).toArray())
        .map((d) => String(d._id))
        .sort(),
    };
  });
  assertEquals(mongoIds, memoryIds);
  return memoryIds;
}

test("deleteWhere never crosses a type boundary, in a multi-collection or a scoped one", async () => {
  const parent = isolationParent();
  const child = migrationDefinition("002", "delete-flagged", {
    parent,
    schemas: ISOLATION,
    migrate: (b) => {
      b.multiCollection("+tenants")
        .type("role")
        .deleteWhere({ flagged: true })
        .end()
        .end();
      b.scopedMultiCollection("+expo")
        .type("ticket")
        .deleteWhere({
          flagged: true,
        })
        .end()
        .end();
      return b.compile();
    },
  });
  const ids = await isolationRun(child, parent);
  assertEquals(ids.tenants, ["member:1"]);
  assertEquals(ids.expo, ["badge:a1", "ticket:a2"]);
});

test("deleteWhere with a scope filter touches only the listed scopes, and reserved keys are refused", async () => {
  const parent = isolationParent();
  assertThrows(
    () =>
      migrationDefinition("002", "delete-widening", {
        parent,
        schemas: ISOLATION,
        migrate: (b) => {
          b.scopedMultiCollection("+expo")
            .type("ticket")
            .deleteWhere(
              { flagged: true, _scope: "exposition:c" },
              {
                scopeFilter: ["exposition:a"],
              },
            )
            .end()
            .end();
          return b.compile();
        },
      }).migrate(
        migrationBuilder({ schemas: ISOLATION, parentSchemas: ISOLATION }),
      ),
    Error,
    '"_scope" is not allowed',
  );
  assertThrows(
    () =>
      migrationDefinition("002", "delete-retyping", {
        parent,
        schemas: ISOLATION,
        migrate: (b) => {
          b.multiCollection("+tenants")
            .type("role")
            .deleteWhere({
              _type: "member",
            })
            .end()
            .end();
          return b.compile();
        },
      }).migrate(
        migrationBuilder({ schemas: ISOLATION, parentSchemas: ISOLATION }),
      ),
    Error,
    '"_type" is not allowed',
  );

  const honest = migrationDefinition("003", "delete-in-scopes", {
    parent,
    schemas: ISOLATION,
    migrate: (b) => {
      b.scopedMultiCollection("+expo")
        .type("ticket")
        .deleteWhere(
          { flagged: true },
          {
            scopeFilter: ["exposition:a", "exposition:b"],
          },
        )
        .end()
        .end();
      return b.compile();
    },
  });
  const after = await isolationRun(honest, parent);
  assertEquals(after.expo, ["badge:a1", "ticket:a2", "ticket:c1"]);
  assertEquals(after.tenants, ["member:1", "role:1"]);
});

test("simulation exercises a scoped deletion and reports the unexercised one", async () => {
  const parent = isolationParent();
  const validator = createSimulationValidator({ powerLevel: "quick" });
  const exercised = migrationDefinition("002", "delete-flagged-tickets", {
    parent,
    schemas: ISOLATION,
    migrate: (b) => {
      b.scopedMultiCollection("+expo")
        .type("ticket")
        .deleteWhere({
          flagged: true,
        })
        .end()
        .end();
      return b.compile();
    },
  });
  const ok = await validator.validateMigration(exercised);
  assert(ok.success, ok.errors.join("\n"));
  assert(
    !ok.warnings.some((w) => w.includes("matched no simulated document")),
    ok.warnings.join("\n"),
  );

  const silent = migrationDefinition("003", "delete-nobody", {
    parent,
    schemas: ISOLATION,
    migrate: (b) => {
      b.scopedMultiCollection("+expo")
        .type("ticket")
        .deleteWhere({
          holder: "nobody-is-called-this",
        })
        .end()
        .end();
      return b.compile();
    },
  });
  const warned = await validator.validateMigration(silent);
  assert(
    warned.warnings.some(
      (w) =>
        w.includes("delete_scoped_multicollection_documents") &&
        w.includes("matched no simulated document"),
    ),
    warned.warnings.join("\n"),
  );
});

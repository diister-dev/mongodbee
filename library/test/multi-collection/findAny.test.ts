import * as v from "../../src/schema.ts";
import { assertEquals, assertNotEquals } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

Deno.test("findAny - cross-type filter on shared field", async (t) => {
  await withDatabase(t.name, async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string(), active: v.boolean() },
        group: { name: v.string(), active: v.boolean() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    await collection.insertOne("user", { name: "John", active: false });
    await collection.insertOne("user", { name: "Jane", active: true });
    await collection.insertOne("group", { name: "Admins", active: false });
    await collection.insertOne("group", { name: "Users", active: true });

    // Cross-type: any doc (user or group) that is active
    const actives = await collection.findAny({ active: true });
    assertEquals(actives.length, 2);

    const names = actives.map((d) => d.name).sort();
    assertEquals(names, ["Jane", "Users"]);

    // Both types should be represented
    const types = new Set(actives.map((d) => d._type));
    assertEquals(types.size, 2);
  });
});

Deno.test("findAny - by _type field", async (t) => {
  await withDatabase(t.name, async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string() },
        group: { name: v.string() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    await collection.insertOne("user", { name: "John" });
    await collection.insertOne("user", { name: "Jane" });
    await collection.insertOne("group", { name: "Admins" });

    const onlyUsers = await collection.findAny({ _type: "user" });
    assertEquals(onlyUsers.length, 2);
    for (const doc of onlyUsers) {
      assertEquals(doc._type, "user");
    }
  });
});

Deno.test("findAny - complex $or with per-type discriminators", async (t) => {
  await withDatabase(t.name, async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string(), age: v.number() },
        group: { name: v.string(), memberCount: v.number() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    await collection.insertOne("user", { name: "John", age: 25 });
    await collection.insertOne("user", { name: "Jane", age: 30 });
    await collection.insertOne("group", { name: "Small", memberCount: 5 });
    await collection.insertOne("group", { name: "Large", memberCount: 100 });

    // Same pattern used by expo-visibility provider:
    // each $or branch is self-discriminating via _type + type-specific fields.
    const matches = await collection.findAny({
      $or: [
        { _type: "user", age: { $lt: 30 } },
        { _type: "group", memberCount: { $lt: 10 } },
      ],
    });

    assertEquals(matches.length, 2);
    const names = matches.map((d) => d.name).sort();
    assertEquals(names, ["John", "Small"]);
  });
});

Deno.test("findAny - no matches returns empty array", async (t) => {
  await withDatabase(t.name, async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    await collection.insertOne("user", { name: "John" });

    const matches = await collection.findAny({ name: "NonExistent" });
    assertEquals(matches.length, 0);
  });
});

Deno.test("findAny - honors FindOptions (limit)", async (t) => {
  await withDatabase(t.name, async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string() },
        group: { name: v.string() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    for (let i = 0; i < 5; i++) {
      await collection.insertOne("user", { name: `user${i}` });
      await collection.insertOne("group", { name: `group${i}` });
    }

    // limit caps total docs across types
    const limited = await collection.findAny({}, { limit: 3 });
    assertEquals(limited.length, 3);
  });
});

Deno.test("findOneAny - returns first matching doc across types", async (t) => {
  await withDatabase(t.name, async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string(), active: v.boolean() },
        group: { name: v.string(), active: v.boolean() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    await collection.insertOne("user", { name: "John", active: false });
    await collection.insertOne("group", { name: "Admins", active: true });

    const match = await collection.findOneAny({ active: true });
    assertNotEquals(match, null);
    assertEquals(match!.name, "Admins");
    assertEquals(match!._type, "group");
  });
});

Deno.test("findOneAny - returns null when no match", async (t) => {
  await withDatabase(t.name, async (db) => {
    const model = defineModel("test", {
      schema: {
        user: { name: v.string() },
      },
    });

    const collection = await multiCollection(db, "test", model);

    await collection.insertOne("user", { name: "John" });

    const match = await collection.findOneAny({ name: "Ghost" });
    assertEquals(match, null);
  });
});

Deno.test("findOneAny - $or cross-type existence check (the expo-visibility pattern)", async (t) => {
  await withDatabase(t.name, async (db) => {
    const model = defineModel("test", {
      schema: {
        participant: {
          personRef: v.object({ kind: v.string(), userId: v.string() }),
          status: v.string(),
        },
        expo_organization: {
          entrepriseId: v.string(),
          invalidatedAt: v.optional(v.string()),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    await collection.insertOne("participant", {
      personRef: { kind: "user", userId: "user:alice" },
      status: "active",
    });
    await collection.insertOne("expo_organization", {
      entrepriseId: "entreprise:acme",
    });

    // Branche participant matche → one round-trip returns truthy
    const viaParticipant = await collection.findOneAny({
      $or: [
        {
          _type: "participant",
          "personRef.kind": "user",
          "personRef.userId": "user:alice",
          status: "active",
        },
        {
          _type: "expo_organization",
          entrepriseId: { $in: ["entreprise:nope"] },
          invalidatedAt: { $exists: false },
        },
      ],
    });
    assertNotEquals(viaParticipant, null);

    // Branche entreprise matche pour un user sans participant
    const viaEntreprise = await collection.findOneAny({
      $or: [
        {
          _type: "participant",
          "personRef.kind": "user",
          "personRef.userId": "user:bob",
          status: "active",
        },
        {
          _type: "expo_organization",
          entrepriseId: { $in: ["entreprise:acme"] },
          invalidatedAt: { $exists: false },
        },
      ],
    });
    assertNotEquals(viaEntreprise, null);

    // Aucune branche ne matche
    const noMatch = await collection.findOneAny({
      $or: [
        {
          _type: "participant",
          "personRef.kind": "user",
          "personRef.userId": "user:ghost",
          status: "active",
        },
        {
          _type: "expo_organization",
          entrepriseId: { $in: ["entreprise:ghost"] },
          invalidatedAt: { $exists: false },
        },
      ],
    });
    assertEquals(noMatch, null);
  });
});

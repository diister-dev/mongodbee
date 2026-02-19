import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { withIndex } from "../../src/indexes.ts";
import { defineModel } from "../../src/multi-collection-model.ts";

test("Ensure Schema are not recreated", async () => {
  await withDatabase("Ensure Schema are not recreated", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          mail: v.string(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    const userA = await collection.insertOne("user", {
      name: "John",
      mail: "john@doe.d",
    });

    const userB = await collection.insertOne("user", {
      name: "Jane",
      mail: "jane@doe.d",
    });

    const users = await collection.find("user").toArray();
    expect(users.length).toEqual(2);

    // Close connection
    const collection2 = await multiCollection(db, "test", model);

    const users2 = await collection2.find("user").toArray();
    expect(users2.length).toEqual(2);
  });
});

test("Ensure Schema are updated if changed", async () => {
  await withDatabase("Ensure Schema are updated if changed", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          mail: v.string(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model);

    const userA = await collection.insertOne("user", {
      name: "John",
      mail: "john@doe.d",
    });

    const users = await collection.find("user").toArray();
    expect(users.length).toEqual(1);

    // Close connection
    const model2 = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          mail: v.string(),
          age: v.optional(v.number()),
        },
      },
    });

    const collection2 = await multiCollection(db, "test", model2);

    // Existing user should still be there
    const users2 = await collection2.find("user").toArray();
    expect(users2.length).toEqual(1);
    expect(users2[0].age).toEqual(undefined);

    // New user should be insertable with age
    const userB = await collection2.insertOne("user", {
      name: "Jane",
      mail: "jane@doe.d",
      age: 30,
    });

    const users3 = await collection2.find("user").toArray();
    expect(users3.length).toEqual(2);
    expect(users3[1].age).toEqual(30);
  });
});

test("Ensure indexes are not recreated if already exist", async () => {
  await withDatabase("Ensure indexes are not recreated if already exist", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: {
          name: withIndex(v.string(), { unique: true }),
          mail: v.string(),
        },
      },
    });

    const collection = await multiCollection(db, "test", model, { schemaManagement: "auto" });

    const userA = await collection.insertOne("user", {
      name: "John",
      mail: "john@doe.d",
    });

    await expect(
      async () => {
        await collection.insertOne("user", {
          name: "John",
          mail: "john2@doe.d",
        });
      },
    ).rejects.toThrow("E11000 duplicate key error collection");

    // Close connection
    const collection2 = await multiCollection(db, "test", model, { schemaManagement: "auto" });

    await expect(
      async () => {
        await collection2.insertOne("user", {
          name: "John",
          mail: "john3@doe.d",
        });
      },
    ).rejects.toThrow("E11000 duplicate key error collection");

    const userB = await collection2.insertOne("user", {
      name: "Jane",
      mail: "jane@doe.d",
    });

    const users2 = await collection2.find("user").toArray();
    expect(users2.length).toEqual(2);
  });
});

test("Ensure indexes are updated if changed", async () => {
  await withDatabase("Ensure indexes are updated if changed", async (db) => {
    const model = defineModel("test", {
      schema: {
        user: {
          name: withIndex(v.string(), { unique: true }),
          mail: v.string(),
        },
      },
    });
    const collection = await multiCollection(db, "test", model, { schemaManagement: "auto" });

    const userA = await collection.insertOne("user", {
      name: "John",
      mail: "john@doe.d",
    });

    await expect(
      async () => {
        await collection.insertOne("user", {
          name: "John",
          mail: "john2@doe.d",
        });
      },
    ).rejects.toThrow("E11000 duplicate key error collection");

    // Close connection
    const model2 = defineModel("test", {
      schema: {
        user: {
          name: withIndex(v.string(), { unique: false }), // Change index to non-unique
          mail: v.string(),
        },
      },
    });
    const collection2 = await multiCollection(db, "test", model2, { schemaManagement: "auto" });

    // Should be able to insert user with same name now
    const userB = await collection2.insertOne("user", {
      name: "John",
      mail: "john3@doe.d",
    });

    const users2 = await collection2.find("user").toArray();
    expect(users2.length).toEqual(2);
  });
});

import * as v from "../../src/schema.ts";
import { assert, assertEquals, assertRejects } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { withIndex } from "@diister/mongodbee";
import { defineModel } from "../../src/multi-collection-model.ts";

Deno.test("Ensure Schema are not recreated", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    const users = await collection.find("user");
    assertEquals(users.length, 2);

    // Close connection
    const collection2 = await multiCollection(db, "test", model);

    const users2 = await collection2.find("user");
    assertEquals(users2.length, 2);
  });
});

Deno.test("Ensure Schema are updated if changed", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    const users = await collection.find("user");
    assertEquals(users.length, 1);

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
    const users2 = await collection2.find("user");
    assertEquals(users2.length, 1);
    assertEquals(users2[0].age, undefined);

    // New user should be insertable with age
    const userB = await collection2.insertOne("user", {
      name: "Jane",
      mail: "jane@doe.d",
      age: 30,
    });

    const users3 = await collection2.find("user");
    assertEquals(users3.length, 2);
    assertEquals(users3[1].age, 30);
  });
});

Deno.test("Ensure indexes are not recreated if already exist", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    await assertRejects(
      async () => {
        await collection.insertOne("user", {
          name: "John",
          mail: "john2@doe.d",
        });
      },
      Error,
      "E11000 duplicate key error collection",
    );

    // Close connection
    const collection2 = await multiCollection(db, "test", model, { schemaManagement: "auto" });

    await assertRejects(
      async () => {
        await collection2.insertOne("user", {
          name: "John",
          mail: "john3@doe.d",
        });
      },
      Error,
      "E11000 duplicate key error collection",
    );

    const userB = await collection2.insertOne("user", {
      name: "Jane",
      mail: "jane@doe.d",
    });

    const users2 = await collection2.find("user");
    assertEquals(users2.length, 2);
  });
});

Deno.test("Ensure indexes are updated if changed", async (t) => {
  await withDatabase(t.name, async (db) => {
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

    await assertRejects(
      async () => {
        await collection.insertOne("user", {
          name: "John",
          mail: "john2@doe.d",
        });
      },
      Error,
      "E11000 duplicate key error collection",
    );

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

    const users2 = await collection2.find("user");
    assertEquals(users2.length, 2);
  });
});

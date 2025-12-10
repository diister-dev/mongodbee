import * as v from "../../src/schema.ts";
import { assertEquals, assertRejects } from "@std/assert";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import assert from "node:assert";
import { defineModel } from "../../src/multi-collection-model.ts";
import { partial, removeField } from "../../src/sanitizer.ts";

Deno.test("UpdateOne: Basic update test", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          mail: v.string(),
          age: v.number(),
        },
        group: {
          name: v.string(),
          members: v.array(v.string()),
          metadata: v.object({
            createdAt: v.string(),
            type: v.string(),
          }),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert a user to update later
    const userId = await collection.insertOne("user", {
      name: "John",
      mail: "john@example.com",
      age: 30,
    });

    // Update simple property
    await collection.updateOne("user", userId, {
      name: "John Smith",
    });

    // Verify update
    const updatedUser = await collection.findOne("user", { _id: userId });
    assert(updatedUser !== null);
    assertEquals(updatedUser.name, "John Smith");
    assertEquals(updatedUser.mail, "john@example.com");
    assertEquals(updatedUser.age, 30);

    // Insert a group with nested object
    const groupId = await collection.insertOne("group", {
      name: "Team A",
      members: [userId],
      metadata: {
        createdAt: "2023-01-01",
        type: "public",
      },
    });

    // Update nested property
    await collection.updateOne("group", groupId, {
      "metadata.type": "private",
    });

    // Verify nested update
    const updatedGroup = await collection.findOne("group", { _id: groupId });
    assert(updatedGroup !== null);
    assertEquals(updatedGroup.name, "Team A");
    assertEquals(updatedGroup.metadata.createdAt, "2023-01-01");
    assertEquals(updatedGroup.metadata.type, "private");
  });
});

Deno.test("UpdateOne: Array updates test", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        group: {
          name: v.string(),
          members: v.array(v.string()),
          tags: v.array(v.string()),
          nested: v.object({
            key: v.string(),
            value: v.string(),
          }),
          nestedData: v.array(v.object({
            key: v.string(),
            value: v.string(),
          })),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Create test users
    const members = [
      "user:abc123",
      "user:def456",
    ];

    // Insert a group with array
    const groupId = await collection.insertOne("group", {
      name: "Team A",
      members,
      tags: ["important", "active"],
      nestedData: [
        { key: "location", value: "office" },
        { key: "priority", value: "high" },
      ],
      nested: {
        key: "status",
        value: "active",
      },
    });

    // Update array element
    await collection.updateOne("group", groupId, {
      tags: ["important", "inactive"],
    });

    // Update nested object in array
    await collection.updateOne("group", groupId, {
      "nestedData.0.value": "home",
    });

    // Verify array updates
    const updatedGroup = await collection.findOne("group", { _id: groupId });
    assert(updatedGroup !== null);
    assertEquals(updatedGroup.tags, ["important", "inactive"]);
    assertEquals(updatedGroup.nestedData[0].value, "home");
    assertEquals(updatedGroup.nestedData[0].key, "location");
    assertEquals(updatedGroup.nestedData[1].key, "priority");
  });
});

Deno.test("UpdateOne: Non-existent document", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          age: v.number(),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Try to update non-existent document
    await assertRejects(
      async () => {
        await collection.updateOne("user", "user:nonexistent", {
          name: "Updated Name",
        });
      },
      Error,
      "No element that match the filter to update",
    );
  });
});

Deno.test("UpdateOne: Invalid id format test", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          age: v.number(),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert test user
    const userId = await collection.insertOne("user", {
      name: "John",
      age: 30,
    });

    // Try to update with wrong id format
    await assertRejects(
      async () => {
        await collection.updateOne("user", "invalidformat", {
          name: "John Smith",
        });
      },
    );

    // Try to update with id from wrong collection type
    await assertRejects(
      async () => {
        await collection.updateOne("user", "group:abc123", {
          name: "John Smith",
        });
      },
    );
  });
});

Deno.test("UpdateOne: Support optional object entry", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          age: v.number(),
          address: v.optional(v.object({
            city: v.string(),
            country: v.string(),
          })),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert test user
    const userId = await collection.insertOne("user", {
      name: "John",
      age: 30,
    });

    // Update with optional field
    await collection.updateOne("user", userId, {
      address: {
        city: "New York",
        country: "USA",
      },
    });

    // Verify update
    const updatedUser = await collection.findOne("user", { _id: userId });
    assert(updatedUser !== null);
    assertEquals(updatedUser.name, "John");
    assertEquals(updatedUser.age, 30);
    assertEquals(updatedUser.address?.city, "New York");
    assertEquals(updatedUser.address?.country, "USA");
  });
});

Deno.test("UpdateOne: Multiple updates at once", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          email: v.string(),
          profile: v.object({
            age: v.number(),
            address: v.object({
              city: v.string(),
              country: v.string(),
            }),
          }),
          tags: v.array(v.string()),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert test user with nested structure
    const userId = await collection.insertOne("user", {
      name: "John Doe",
      email: "john@example.com",
      profile: {
        age: 30,
        address: {
          city: "New York",
          country: "USA",
        },
      },
      tags: ["developer", "admin"],
    });

    // Update multiple fields at different levels in a single call
    await collection.updateOne("user", userId, {
      name: "Jane Doe",
      "profile.age": 28,
      "profile.address.city": "San Francisco",
      "tags.0": "designer",
    });

    // Verify all updates were applied
    const updatedUser = await collection.findOne("user", { _id: userId });
    assert(updatedUser !== null);
    assertEquals(updatedUser.name, "Jane Doe");
    assertEquals(updatedUser.email, "john@example.com"); // unchanged
    assertEquals(updatedUser.profile.age, 28);
    assertEquals(updatedUser.profile.address.city, "San Francisco");
    assertEquals(updatedUser.profile.address.country, "USA"); // unchanged
    assertEquals(updatedUser.tags[0], "designer");
    assertEquals(updatedUser.tags[1], "admin"); // unchanged
  });
});

Deno.test("UpdateOne: Update Complex array", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          email: v.string(),
          tags: v.array(v.object({
            name: v.string(),
            value: v.string(),
          })),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert test user with nested structure
    const userId = await collection.insertOne("user", {
      name: "John Doe",
      email: "john@example.com",
      tags: [
        { name: "role", value: "admin" },
        { name: "status", value: "active" },
      ],
    });

    // Update multiple fields at different levels in a single call
    await collection.updateOne("user", userId, {
      name: "Jane Doe",
      "tags.0.value": "super-admin",
      "tags.1.name": "state",
    });

    // Verify all updates were applied
    const updatedUser = await collection.findOne("user", { _id: userId });
    assert(updatedUser !== null);
    assertEquals(updatedUser.name, "Jane Doe");
    assertEquals(updatedUser.email, "john@example.com"); // unchanged
    assertEquals(updatedUser.tags[0].name, "role");
    assertEquals(updatedUser.tags[0].value, "super-admin");
    assertEquals(updatedUser.tags[1].name, "state");
    assertEquals(updatedUser.tags[1].value, "active");

    // Change an entire array element
    await collection.updateOne("user", userId, {
      "tags.1": { name: "location", value: "USA" },
      "tags.2": { name: "extra", value: "new" },
    });

    // Verify the entire array element was changed
    const updatedUser2 = await collection.findOne("user", { _id: userId });
    assert(updatedUser2 !== null);
    assertEquals(updatedUser2.tags[0].name, "role");
    assertEquals(updatedUser2.tags[0].value, "super-admin");
    assertEquals(updatedUser2.tags[1].name, "location");
    assertEquals(updatedUser2.tags[1].value, "USA");
    assertEquals(updatedUser2.tags[2].name, "extra");
    assertEquals(updatedUser2.tags[2].value, "new");
    assertEquals(updatedUser2.name, "Jane Doe");
    assertEquals(updatedUser.email, "john@example.com");
  });
});

Deno.test("UpdateOne: Remove optional field with removeField()", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          email: v.string(),
          phone: v.optional(v.string()),
          bio: v.optional(v.string()),
          age: v.optional(v.number()),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert test user with optional fields
    const userId = await collection.insertOne("user", {
      name: "John Doe",
      email: "john@example.com",
      phone: "123-456-7890",
      bio: "Software developer",
      age: 30,
    });

    // Verify initial state
    const initialUser = await collection.findOne("user", { _id: userId });
    assert(initialUser !== null);
    assertEquals(initialUser.phone, "123-456-7890");
    assertEquals(initialUser.bio, "Software developer");
    assertEquals(initialUser.age, 30);

    // Remove phone field using removeField()
    await collection.updateOne("user", userId, {
      phone: removeField(),
    });

    // Verify phone was removed
    const afterPhoneRemoval = await collection.findOne("user", { _id: userId });
    assert(afterPhoneRemoval !== null);
    assertEquals(afterPhoneRemoval.name, "John Doe");
    assertEquals(afterPhoneRemoval.email, "john@example.com");
    assertEquals(afterPhoneRemoval.phone, undefined);
    assertEquals(afterPhoneRemoval.bio, "Software developer");
    assertEquals(afterPhoneRemoval.age, 30);

    // Remove multiple fields at once
    await collection.updateOne("user", userId, {
      bio: removeField(),
      age: removeField(),
    });

    // Verify both fields were removed
    const afterMultipleRemoval = await collection.findOne("user", { _id: userId });
    assert(afterMultipleRemoval !== null);
    assertEquals(afterMultipleRemoval.name, "John Doe");
    assertEquals(afterMultipleRemoval.email, "john@example.com");
    assertEquals(afterMultipleRemoval.phone, undefined);
    assertEquals(afterMultipleRemoval.bio, undefined);
    assertEquals(afterMultipleRemoval.age, undefined);
  });
});

Deno.test("UpdateOne: Mix update and remove fields", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        product: {
          name: v.string(),
          price: v.number(),
          description: v.optional(v.string()),
          stock: v.optional(v.number()),
          category: v.optional(v.string()),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert test product
    const productId = await collection.insertOne("product", {
      name: "Laptop",
      price: 999.99,
      description: "High-performance laptop",
      stock: 50,
      category: "Electronics",
    });

    // Update some fields and remove others in a single operation
    await collection.updateOne("product", productId, {
      name: "Gaming Laptop",
      price: 1299.99,
      description: removeField(),
      category: "Gaming",
    });

    // Verify mixed update/remove
    const updatedProduct = await collection.findOne("product", { _id: productId });
    assert(updatedProduct !== null);
    assertEquals(updatedProduct.name, "Gaming Laptop");
    assertEquals(updatedProduct.price, 1299.99);
    assertEquals(updatedProduct.description, undefined); // Removed
    assertEquals(updatedProduct.stock, 50); // Unchanged
    assertEquals(updatedProduct.category, "Gaming");
  });
});

Deno.test("UpdateOne: Remove field with removeField() vs undefined", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          email: v.optional(v.string()),
          phone: v.optional(v.string()),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert test user
    const userId = await collection.insertOne("user", {
      name: "John",
      email: "john@example.com",
      phone: "123-456",
    });

    // removeField() explicitly removes, undefined is ignored (field not touched)
    await collection.updateOne("user", userId, {
      email: removeField(), // Explicit removal - will be unset
      phone: undefined, // Ignored - field remains unchanged
    });

    // email should be removed, phone should remain unchanged
    const updatedUser = await collection.findOne("user", { _id: userId });
    assert(updatedUser !== null);
    assertEquals(updatedUser.name, "John");
    assertEquals(updatedUser.email, undefined); // Removed by removeField()
    assertEquals(updatedUser.phone, "123-456"); // Unchanged because undefined is ignored
  });
});

Deno.test("UpdateOne: Remove nested field with removeField()", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          settings: v.optional(v.object({
            theme: v.optional(v.string()),
            language: v.optional(v.string()),
            notifications: v.optional(v.object({
              email: v.optional(v.boolean()),
              push: v.optional(v.boolean()),
            })),
          })),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert user with nested settings
    const userId = await collection.insertOne("user", {
      name: "John",
      settings: {
        theme: "dark",
        language: "en",
        notifications: {
          email: true,
          push: false,
        },
      },
    });

    // Verify initial state
    const initialUser = await collection.findOne("user", { _id: userId });
    assert(initialUser !== null);
    assertEquals(initialUser.settings?.theme, "dark");
    assertEquals(initialUser.settings?.language, "en");
    assertEquals(initialUser.settings?.notifications?.email, true);

    // Remove nested field using removeField() with partial() for merge behavior
    await collection.updateOne("user", userId, {
      settings: partial({
        theme: removeField(),
        language: "fr", // Update this one
      }),
    });

    // Verify nested removal
    const afterUpdate = await collection.findOne("user", { _id: userId });
    assert(afterUpdate !== null);
    assertEquals(afterUpdate.name, "John");
    assertEquals(afterUpdate.settings?.theme, undefined); // Removed
    assertEquals(afterUpdate.settings?.language, "fr"); // Updated
    assertEquals(afterUpdate.settings?.notifications?.email, true); // Unchanged
  });
});

Deno.test("UpdateOne: Remove deeply nested field with removeField()", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          profile: v.optional(v.object({
            bio: v.optional(v.string()),
            social: v.optional(v.object({
              twitter: v.optional(v.string()),
              github: v.optional(v.string()),
              linkedin: v.optional(v.string()),
            })),
          })),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert user with deeply nested data
    const userId = await collection.insertOne("user", {
      name: "John",
      profile: {
        bio: "Developer",
        social: {
          twitter: "@john",
          github: "john123",
          linkedin: "john-doe",
        },
      },
    });

    // Remove deeply nested fields using partial() for merge behavior
    await collection.updateOne("user", userId, {
      profile: partial({
        social: partial({
          twitter: removeField(),
          linkedin: removeField(),
          github: "john-updated", // Update this one
        }),
      }),
    });

    // Verify deep removal
    const afterUpdate = await collection.findOne("user", { _id: userId });
    assert(afterUpdate !== null);
    assertEquals(afterUpdate.name, "John");
    assertEquals(afterUpdate.profile?.bio, "Developer"); // Unchanged
    assertEquals(afterUpdate.profile?.social?.twitter, undefined); // Removed
    assertEquals(afterUpdate.profile?.social?.github, "john-updated"); // Updated
    assertEquals(afterUpdate.profile?.social?.linkedin, undefined); // Removed
  });
});

Deno.test("UpdateOne: Remove entire nested object with removeField()", async (t) => {
  await withDatabase(t.name, async (db) => {
    const testModel = defineModel("test", {
      schema: {
        user: {
          name: v.string(),
          metadata: v.optional(v.object({
            createdAt: v.optional(v.string()),
            updatedAt: v.optional(v.string()),
          })),
          preferences: v.optional(v.object({
            theme: v.optional(v.string()),
          })),
        },
      },
    });

    const collection = await multiCollection(db, "test", testModel);

    // Insert user with nested objects
    const userId = await collection.insertOne("user", {
      name: "John",
      metadata: {
        createdAt: "2024-01-01",
        updatedAt: "2024-01-02",
      },
      preferences: {
        theme: "dark",
      },
    });

    // Remove entire nested object
    await collection.updateOne("user", userId, {
      metadata: removeField(), // Remove entire object
      preferences: {
        theme: "light", // Update nested field
      },
    });

    // Verify removal
    const afterUpdate = await collection.findOne("user", { _id: userId });
    assert(afterUpdate !== null);
    assertEquals(afterUpdate.name, "John");
    assertEquals(afterUpdate.metadata, undefined); // Entire object removed
    assertEquals(afterUpdate.preferences?.theme, "light"); // Updated
  });
});

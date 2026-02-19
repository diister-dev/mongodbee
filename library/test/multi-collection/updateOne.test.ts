import * as v from "../../src/schema.ts";
import { test, expect } from "vitest";
import { multiCollection } from "../../src/multi-collection.ts";
import { withDatabase } from "../+shared.ts";
import { defineModel } from "../../src/multi-collection-model.ts";
import { partial, removeField } from "../../src/sanitizer.ts";

test("UpdateOne: Basic update test", async () => {
  await withDatabase("UpdateOne: Basic update test", async (db) => {
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
    await collection.updateById("user", userId, {
      name: "John Smith",
    });

    // Verify update
    const updatedUser = await collection.findOne("user", { _id: userId });
    expect(updatedUser).not.toBeNull();
    expect(updatedUser!.name).toEqual("John Smith");
    expect(updatedUser!.mail).toEqual("john@example.com");
    expect(updatedUser!.age).toEqual(30);

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
    await collection.updateById("group", groupId, {
      "metadata.type": "private",
    });

    // Verify nested update
    const updatedGroup = await collection.findOne("group", { _id: groupId });
    expect(updatedGroup).not.toBeNull();
    expect(updatedGroup!.name).toEqual("Team A");
    expect(updatedGroup!.metadata.createdAt).toEqual("2023-01-01");
    expect(updatedGroup!.metadata.type).toEqual("private");
  });
});

test("UpdateOne: Array updates test", async () => {
  await withDatabase("UpdateOne: Array updates test", async (db) => {
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
    await collection.updateById("group", groupId, {
      tags: ["important", "inactive"],
    });

    // Update nested object in array
    await collection.updateById("group", groupId, {
      "nestedData.0.value": "home",
    });

    // Verify array updates
    const updatedGroup = await collection.findOne("group", { _id: groupId });
    expect(updatedGroup).not.toBeNull();
    expect(updatedGroup!.tags).toEqual(["important", "inactive"]);
    expect(updatedGroup!.nestedData[0].value).toEqual("home");
    expect(updatedGroup!.nestedData[0].key).toEqual("location");
    expect(updatedGroup!.nestedData[1].key).toEqual("priority");
  });
});

test("UpdateOne: Non-existent document", async () => {
  await withDatabase("UpdateOne: Non-existent document", async (db) => {
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
    await expect(
      collection.updateById("user", "user:nonexistent", {
        name: "Updated Name",
      }),
    ).rejects.toThrow("No element that match the filter to update");
  });
});

test("UpdateOne: Invalid id format test", async () => {
  await withDatabase("UpdateOne: Invalid id format test", async (db) => {
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
    await expect(
      collection.updateById("user", "invalidformat", {
        name: "John Smith",
      }),
    ).rejects.toThrow();

    // Try to update with id from wrong collection type
    await expect(
      collection.updateById("user", "group:abc123", {
        name: "John Smith",
      }),
    ).rejects.toThrow();
  });
});

test("UpdateOne: Support optional object entry", async () => {
  await withDatabase("UpdateOne: Support optional object entry", async (db) => {
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
    await collection.updateById("user", userId, {
      address: {
        city: "New York",
        country: "USA",
      },
    });

    // Verify update
    const updatedUser = await collection.findOne("user", { _id: userId });
    expect(updatedUser).not.toBeNull();
    expect(updatedUser!.name).toEqual("John");
    expect(updatedUser!.age).toEqual(30);
    expect(updatedUser!.address?.city).toEqual("New York");
    expect(updatedUser!.address?.country).toEqual("USA");
  });
});

test("UpdateOne: Multiple updates at once", async () => {
  await withDatabase("UpdateOne: Multiple updates at once", async (db) => {
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
    await collection.updateById("user", userId, {
      name: "Jane Doe",
      "profile.age": 28,
      "profile.address.city": "San Francisco",
      "tags.0": "designer",
    });

    // Verify all updates were applied
    const updatedUser = await collection.findOne("user", { _id: userId });
    expect(updatedUser).not.toBeNull();
    expect(updatedUser!.name).toEqual("Jane Doe");
    expect(updatedUser!.email).toEqual("john@example.com"); // unchanged
    expect(updatedUser!.profile.age).toEqual(28);
    expect(updatedUser!.profile.address.city).toEqual("San Francisco");
    expect(updatedUser!.profile.address.country).toEqual("USA"); // unchanged
    expect(updatedUser!.tags[0]).toEqual("designer");
    expect(updatedUser!.tags[1]).toEqual("admin"); // unchanged
  });
});

test("UpdateOne: Update Complex array", async () => {
  await withDatabase("UpdateOne: Update Complex array", async (db) => {
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
    await collection.updateById("user", userId, {
      name: "Jane Doe",
      "tags.0.value": "super-admin",
      "tags.1.name": "state",
    });

    // Verify all updates were applied
    const updatedUser = await collection.findOne("user", { _id: userId });
    expect(updatedUser).not.toBeNull();
    expect(updatedUser!.name).toEqual("Jane Doe");
    expect(updatedUser!.email).toEqual("john@example.com"); // unchanged
    expect(updatedUser!.tags[0].name).toEqual("role");
    expect(updatedUser!.tags[0].value).toEqual("super-admin");
    expect(updatedUser!.tags[1].name).toEqual("state");
    expect(updatedUser!.tags[1].value).toEqual("active");

    // Change an entire array element
    await collection.updateById("user", userId, {
      "tags.1": { name: "location", value: "USA" },
      "tags.2": { name: "extra", value: "new" },
    });

    // Verify the entire array element was changed
    const updatedUser2 = await collection.findOne("user", { _id: userId });
    expect(updatedUser2).not.toBeNull();
    expect(updatedUser2!.tags[0].name).toEqual("role");
    expect(updatedUser2!.tags[0].value).toEqual("super-admin");
    expect(updatedUser2!.tags[1].name).toEqual("location");
    expect(updatedUser2!.tags[1].value).toEqual("USA");
    expect(updatedUser2!.tags[2].name).toEqual("extra");
    expect(updatedUser2!.tags[2].value).toEqual("new");
    expect(updatedUser2!.name).toEqual("Jane Doe");
    expect(updatedUser!.email).toEqual("john@example.com");
  });
});

test("UpdateOne: Remove optional field with removeField()", async () => {
  await withDatabase("UpdateOne: Remove optional field with removeField()", async (db) => {
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
    expect(initialUser).not.toBeNull();
    expect(initialUser!.phone).toEqual("123-456-7890");
    expect(initialUser!.bio).toEqual("Software developer");
    expect(initialUser!.age).toEqual(30);

    // Remove phone field using removeField()
    await collection.updateById("user", userId, {
      phone: removeField(),
    });

    // Verify phone was removed
    const afterPhoneRemoval = await collection.findOne("user", { _id: userId });
    expect(afterPhoneRemoval).not.toBeNull();
    expect(afterPhoneRemoval!.name).toEqual("John Doe");
    expect(afterPhoneRemoval!.email).toEqual("john@example.com");
    expect(afterPhoneRemoval!.phone).toEqual(undefined);
    expect(afterPhoneRemoval!.bio).toEqual("Software developer");
    expect(afterPhoneRemoval!.age).toEqual(30);

    // Remove multiple fields at once
    await collection.updateById("user", userId, {
      bio: removeField(),
      age: removeField(),
    });

    // Verify both fields were removed
    const afterMultipleRemoval = await collection.findOne("user", { _id: userId });
    expect(afterMultipleRemoval).not.toBeNull();
    expect(afterMultipleRemoval!.name).toEqual("John Doe");
    expect(afterMultipleRemoval!.email).toEqual("john@example.com");
    expect(afterMultipleRemoval!.phone).toEqual(undefined);
    expect(afterMultipleRemoval!.bio).toEqual(undefined);
    expect(afterMultipleRemoval!.age).toEqual(undefined);
  });
});

test("UpdateOne: Mix update and remove fields", async () => {
  await withDatabase("UpdateOne: Mix update and remove fields", async (db) => {
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
    await collection.updateById("product", productId, {
      name: "Gaming Laptop",
      price: 1299.99,
      description: removeField(),
      category: "Gaming",
    });

    // Verify mixed update/remove
    const updatedProduct = await collection.findOne("product", { _id: productId });
    expect(updatedProduct).not.toBeNull();
    expect(updatedProduct!.name).toEqual("Gaming Laptop");
    expect(updatedProduct!.price).toEqual(1299.99);
    expect(updatedProduct!.description).toEqual(undefined); // Removed
    expect(updatedProduct!.stock).toEqual(50); // Unchanged
    expect(updatedProduct!.category).toEqual("Gaming");
  });
});

test("UpdateOne: Remove field with removeField() vs undefined", async () => {
  await withDatabase("UpdateOne: Remove field with removeField() vs undefined", async (db) => {
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
    await collection.updateById("user", userId, {
      email: removeField(), // Explicit removal - will be unset
      phone: undefined, // Ignored - field remains unchanged
    });

    // email should be removed, phone should remain unchanged
    const updatedUser = await collection.findOne("user", { _id: userId });
    expect(updatedUser).not.toBeNull();
    expect(updatedUser!.name).toEqual("John");
    expect(updatedUser!.email).toEqual(undefined); // Removed by removeField()
    expect(updatedUser!.phone).toEqual("123-456"); // Unchanged because undefined is ignored
  });
});

test("UpdateOne: Remove nested field with removeField()", async () => {
  await withDatabase("UpdateOne: Remove nested field with removeField()", async (db) => {
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
    expect(initialUser).not.toBeNull();
    expect(initialUser!.settings?.theme).toEqual("dark");
    expect(initialUser!.settings?.language).toEqual("en");
    expect(initialUser!.settings?.notifications?.email).toEqual(true);

    // Remove nested field using removeField() with partial() for merge behavior
    await collection.updateById("user", userId, {
      settings: partial({
        theme: removeField(),
        language: "fr", // Update this one
      }),
    });

    // Verify nested removal
    const afterUpdate = await collection.findOne("user", { _id: userId });
    expect(afterUpdate).not.toBeNull();
    expect(afterUpdate!.name).toEqual("John");
    expect(afterUpdate!.settings?.theme).toEqual(undefined); // Removed
    expect(afterUpdate!.settings?.language).toEqual("fr"); // Updated
    expect(afterUpdate!.settings?.notifications?.email).toEqual(true); // Unchanged
  });
});

test("UpdateOne: Remove deeply nested field with removeField()", async () => {
  await withDatabase("UpdateOne: Remove deeply nested field with removeField()", async (db) => {
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
    await collection.updateById("user", userId, {
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
    expect(afterUpdate).not.toBeNull();
    expect(afterUpdate!.name).toEqual("John");
    expect(afterUpdate!.profile?.bio).toEqual("Developer"); // Unchanged
    expect(afterUpdate!.profile?.social?.twitter).toEqual(undefined); // Removed
    expect(afterUpdate!.profile?.social?.github).toEqual("john-updated"); // Updated
    expect(afterUpdate!.profile?.social?.linkedin).toEqual(undefined); // Removed
  });
});

test("UpdateOne: Remove entire nested object with removeField()", async () => {
  await withDatabase("UpdateOne: Remove entire nested object with removeField()", async (db) => {
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
    await collection.updateById("user", userId, {
      metadata: removeField(), // Remove entire object
      preferences: {
        theme: "light", // Update nested field
      },
    });

    // Verify removal
    const afterUpdate = await collection.findOne("user", { _id: userId });
    expect(afterUpdate).not.toBeNull();
    expect(afterUpdate!.name).toEqual("John");
    expect(afterUpdate!.metadata).toEqual(undefined); // Entire object removed
    expect(afterUpdate!.preferences?.theme).toEqual("light"); // Updated
  });
});

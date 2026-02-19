import * as v from "../src/schema.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { test, expect } from "vitest";
import { defineModel } from "../src/multi-collection-model.ts";
import { withDatabase } from "./+shared.ts";

const deepSchema = {
  name: v.string(),
  profile: v.optional(v.object({
    bio: v.optional(v.string()),
    settings: v.optional(v.object({
      theme: v.optional(v.string()),
      notifications: v.optional(v.object({
        email: v.optional(v.boolean()),
        push: v.optional(v.boolean()),
      })),
    })),
    tags: v.optional(v.array(v.string())),
  })),
  metadata: v.optional(v.array(v.object({
    key: v.string(),
    value: v.optional(v.string()),
    nested: v.optional(v.object({
      level1: v.optional(v.string()),
      level2: v.optional(v.object({
        deepValue: v.optional(v.string()),
      })),
    })),
  }))),
};

test("Deep sanitization: Collection removes nested undefined values", async () => {
  await withDatabase("deep_sanitize_collection", async (db) => {
    const users = await collection(db, "users_deep", deepSchema, {
      undefinedBehavior: "remove",
    });

    await users.insertOne({
      name: "DeepUser",
      profile: {
        bio: "Test bio",
        settings: {
          theme: undefined, // Should be removed
          notifications: {
            email: true,
            push: undefined, // Should be removed
          },
        },
        tags: ["tag1", "tag2"],
      },
      metadata: [
        {
          key: "key1",
          value: undefined, // Should be removed
          nested: {
            level1: "value1",
            level2: {
              deepValue: undefined, // Should be removed
            },
          },
        },
        {
          key: "key2",
          value: "value2",
          nested: undefined, // Should be removed
        },
      ],
    });

    const user = await users.findOne({ name: "DeepUser" });
    expect(user).not.toBeNull();

    // Check top level
    expect(user.name).toEqual("DeepUser");
    expect("profile" in user).toBeTruthy();
    expect("metadata" in user).toBeTruthy();

    // Check profile level
    expect(user.profile !== undefined).toBeTruthy();
    expect(user.profile.bio).toEqual("Test bio");
    expect("settings" in user.profile).toBeTruthy();
    expect("tags" in user.profile).toBeTruthy();

    // Check settings level
    expect(user.profile.settings !== undefined).toBeTruthy();
    expect(!("theme" in user.profile.settings)).toBeTruthy(); // Should be removed
    expect("notifications" in user.profile.settings).toBeTruthy();

    // Check notifications level
    expect(user.profile.settings.notifications !== undefined).toBeTruthy();
    expect(user.profile.settings.notifications.email).toEqual(true);
    expect(!("push" in user.profile.settings.notifications)).toBeTruthy(); // Should be removed

    // Check metadata array
    expect(user.metadata !== undefined).toBeTruthy();
    expect(user.metadata.length).toEqual(2);

    // Check first metadata item
    const meta1 = user.metadata[0];
    expect(meta1.key).toEqual("key1");
    expect(!("value" in meta1)).toBeTruthy(); // Should be removed
    expect("nested" in meta1).toBeTruthy();

    // Check nested in first metadata
    expect(meta1.nested !== undefined).toBeTruthy();
    expect(meta1.nested.level1).toEqual("value1");
    expect("level2" in meta1.nested).toBeTruthy();
    expect(meta1.nested.level2 !== undefined).toBeTruthy();
    expect(!("deepValue" in meta1.nested.level2)).toBeTruthy(); // Should be removed

    // Check second metadata item
    const meta2 = user.metadata[1];
    expect(meta2.key).toEqual("key2");
    expect(meta2.value).toEqual("value2");
    expect(!("nested" in meta2)).toBeTruthy(); // Should be removed
  });
});

test("Deep sanitization: MultiCollection removes nested undefined values", async () => {
  await withDatabase("deep_sanitize_multi", async (db) => {
    const model = defineModel("deep_docs", {
      schema: {
        users: deepSchema,
      },
    });

    const mc = await multiCollection(db, "deep_docs", model, {
      undefinedBehavior: "remove",
    });

    const userId = await mc.insertOne("users", {
      name: "DeepMultiUser",
      profile: {
        bio: undefined, // Should be removed
        settings: {
          theme: "dark",
          notifications: {
            email: undefined, // Should be removed
            push: false,
          },
        },
        tags: undefined, // Should be removed
      },
      metadata: [
        {
          key: "nested_key",
          value: "nested_value",
          nested: {
            level1: undefined, // Should be removed
            level2: {
              deepValue: "deep!",
            },
          },
        },
      ],
    });

    const user = await mc.findOne("users", { _id: userId });
    expect(user).not.toBeNull();

    expect(user.name).toEqual("DeepMultiUser");
    expect("profile" in user).toBeTruthy();
    expect("metadata" in user).toBeTruthy();

    // Check profile
    expect(user.profile !== undefined).toBeTruthy();
    expect(!("bio" in user.profile)).toBeTruthy(); // Should be removed
    expect(!("tags" in user.profile)).toBeTruthy(); // Should be removed
    expect("settings" in user.profile).toBeTruthy();

    // Check settings
    expect(user.profile.settings !== undefined).toBeTruthy();
    expect(user.profile.settings.theme).toEqual("dark");
    expect("notifications" in user.profile.settings).toBeTruthy();

    // Check notifications
    expect(user.profile.settings.notifications !== undefined).toBeTruthy();
    expect(!("email" in user.profile.settings.notifications)).toBeTruthy(); // Should be removed
    expect(user.profile.settings.notifications.push).toEqual(false);

    // Check metadata
    expect(user.metadata !== undefined).toBeTruthy();
    expect(user.metadata.length).toEqual(1);

    const meta = user.metadata[0];
    expect(meta.key).toEqual("nested_key");
    expect(meta.value).toEqual("nested_value");
    expect("nested" in meta).toBeTruthy();

    // Check deep nested
    expect(meta.nested !== undefined).toBeTruthy();
    expect(!("level1" in meta.nested)).toBeTruthy(); // Should be removed
    expect("level2" in meta.nested).toBeTruthy();
    expect(meta.nested.level2 !== undefined).toBeTruthy();
    expect(meta.nested.level2.deepValue).toEqual("deep!");
  });
});

test("Deep sanitization: Arrays with undefined items", async () => {
  await withDatabase("deep_sanitize_arrays", async (db) => {
    const arraySchema = {
      name: v.string(),
      items: v.optional(v.array(v.object({
        id: v.string(),
        data: v.optional(v.string()),
        nested: v.optional(v.object({
          value: v.optional(v.string()),
        })),
      }))),
    };

    const coll = await collection(db, "array_test", arraySchema, {
      undefinedBehavior: "remove",
    });

    await coll.insertOne({
      name: "ArrayTest",
      items: [
        {
          id: "1",
          data: "data1",
          nested: {
            value: undefined, // Should be removed
          },
        },
        {
          id: "2",
          data: undefined, // Should be removed
          nested: undefined, // Should be removed
        },
        {
          id: "3",
          data: "data3",
          nested: {
            value: "value3",
          },
        },
      ],
    });

    const doc = await coll.findOne({ name: "ArrayTest" });
    expect(doc).not.toBeNull();

    expect(doc.items !== undefined).toBeTruthy();
    expect(doc.items.length).toEqual(3);

    // First item
    expect(doc.items[0].id).toEqual("1");
    expect(doc.items[0].data).toEqual("data1");
    expect("nested" in doc.items[0]).toBeTruthy();
    expect(doc.items[0].nested !== undefined).toBeTruthy();
    expect(!("value" in doc.items[0].nested)).toBeTruthy(); // Should be removed

    // Second item
    expect(doc.items[1].id).toEqual("2");
    expect(!("data" in doc.items[1])).toBeTruthy(); // Should be removed
    expect(!("nested" in doc.items[1])).toBeTruthy(); // Should be removed

    // Third item
    expect(doc.items[2].id).toEqual("3");
    expect(doc.items[2].data).toEqual("data3");
    expect("nested" in doc.items[2]).toBeTruthy();
    expect(doc.items[2].nested !== undefined).toBeTruthy();
    expect(doc.items[2].nested.value).toEqual("value3");
  });
});

test("Deep sanitization: Comparison with shallow mode", async () => {
  await withDatabase("deep_sanitize_shallow", async (db) => {
    // Test that our fix ensures deep sanitization by default
    const simpleSchema = {
      name: v.string(),
      nested: v.optional(v.object({
        value: v.optional(v.string()),
      })),
    };

    const coll = await collection(db, "deep_test", simpleSchema, {
      undefinedBehavior: "remove",
    });

    await coll.insertOne({
      name: "Test",
      nested: {
        value: undefined, // Should be removed with deep=true
      },
    });

    const doc = await coll.findOne({ name: "Test" });
    expect(doc).not.toBeNull();

    expect(doc.name).toEqual("Test");
    expect("nested" in doc).toBeTruthy();
    expect(doc.nested !== undefined).toBeTruthy();

    // With deep=true (our fix), this undefined should be removed
    expect(!("value" in doc.nested)).toBeTruthy();

    // The nested object should still exist but be empty
    expect(Object.keys(doc.nested).length).toEqual(0);
  });
});

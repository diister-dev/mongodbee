import {
  extractFieldsToRemove,
  partial,
  removeField,
  removeUndefined,
  sanitizeDocument,
  sanitizeForMongoDB,
  undefinedToNull,
} from "../src/sanitizer.ts";
import { assert, assertEquals } from "@std/assert";

Deno.test("removeUndefined - simple object", () => {
  const input = {
    a: "hello",
    b: undefined,
    c: 42,
    d: null,
  };

  const result = removeUndefined(input);

  assertEquals(result, {
    a: "hello",
    c: 42,
    d: null,
  });

  // Should not have property 'b'
  assert(!("b" in result));
});

Deno.test("removeUndefined - nested object", () => {
  const input = {
    user: {
      name: "John",
      age: undefined,
      address: {
        street: "123 Main St",
        city: undefined,
        country: "USA",
      },
    },
    metadata: undefined,
  };

  const result = removeUndefined(input);

  assertEquals(result, {
    user: {
      name: "John",
      address: {
        street: "123 Main St",
        country: "USA",
      },
    },
  });
});

Deno.test("removeUndefined - arrays", () => {
  const input = {
    tags: ["a", undefined, "b", "c"],
    items: [
      { name: "item1", value: undefined },
      { name: "item2", value: 42 },
      undefined,
    ],
  };

  const result = removeUndefined(input);

  assertEquals(result, {
    tags: ["a", "b", "c"],
    items: [
      { name: "item1" },
      { name: "item2", value: 42 },
    ],
  });
});

Deno.test("undefinedToNull - converts undefined to null", () => {
  const input = {
    a: "hello",
    b: undefined,
    c: {
      d: undefined,
      e: "world",
    },
  };

  const result = undefinedToNull(input);

  assertEquals(result, {
    a: "hello",
    b: null,
    c: {
      d: null,
      e: "world",
    },
  });
});

Deno.test("sanitizeDocument - remove behavior", () => {
  const input = {
    required: "value",
    optional: undefined,
    nested: {
      prop: undefined,
    },
  };

  const result = sanitizeDocument(input, {
    undefinedBehavior: "remove",
    deep: true,
  });

  assertEquals(result, {
    required: "value",
    nested: {},
  });
});

Deno.test("sanitizeDocument - convert to null behavior", () => {
  const input = {
    required: "value",
    optional: undefined,
    nested: {
      prop: undefined,
    },
  };

  const result = sanitizeDocument(input, {
    undefinedBehavior: "convert-to-null",
    deep: true,
  });

  assertEquals(result, {
    required: "value",
    optional: null,
    nested: {
      prop: null,
    },
  });
});

Deno.test("sanitizeDocument - shallow sanitization", () => {
  const input = {
    topLevel: undefined,
    nested: {
      prop: undefined,
    },
  };

  const result = sanitizeDocument(input, {
    undefinedBehavior: "remove",
    deep: false,
  });

  assertEquals(result, {
    nested: {
      prop: undefined, // Should remain undefined since it's not top level
    },
  });
});

Deno.test("MongoDB optional field scenario", () => {
  // Simulates what happens with valibot optional fields
  const mongoDocument = {
    _id: "123",
    name: "John",
    age: 30,
    email: "john@example.com",
    phone: undefined, // Optional field not provided
    address: {
      street: "123 Main St",
      city: "Somewhere",
      zipcode: undefined, // Optional nested field
    },
  };

  const sanitized = removeUndefined(mongoDocument);

  // Should be ready for MongoDB insertion
  assertEquals(sanitized, {
    _id: "123",
    name: "John",
    age: 30,
    email: "john@example.com",
    address: {
      street: "123 Main St",
      city: "Somewhere",
    },
  });

  // Verify undefined properties are completely removed
  assert(!("phone" in sanitized));
  assert(!("zipcode" in sanitized.address));
});

Deno.test("Enhanced sanitization with field removal", () => {
  const input = {
    keep: "this",
    remove: removeField(),
    ignore: undefined,
    nested: {
      keep: "nested value",
      remove: removeField(),
    },
  };

  const result = sanitizeForMongoDB(input, {
    undefinedBehavior: "remove",
    deep: true,
  });

  assertEquals(result as any, {
    keep: "this",
    nested: {
      keep: "nested value",
    },
  });
});

Deno.test("Different undefined behaviors in updates", () => {
  const input = {
    field1: "value1",
    field2: undefined,
    field3: "value3",
  };

  // Remove behavior (default)
  const removed = sanitizeForMongoDB(input, {
    undefinedBehavior: "remove",
    deep: true,
  });

  assertEquals(removed, {
    field1: "value1",
    field3: "value3",
  });

  // Error behavior
  try {
    sanitizeForMongoDB(input, {
      undefinedBehavior: "error",
      deep: true,
    });
    assert(false, "Should have thrown an error");
  } catch (error) {
    assert(error instanceof Error);
    assert(error.message.includes("Undefined values are not allowed"));
  }
});

Deno.test("Explicit field removal vs undefined", () => {
  // Scenario: Update a user, remove phone field, ignore email if undefined
  const updateData = {
    name: "John Updated",
    phone: removeField(), // Explicit removal
    email: undefined, // Implicit - behavior depends on config
  };

  // With 'remove' behavior - both phone and email are removed
  const withRemove = sanitizeForMongoDB(updateData, {
    undefinedBehavior: "remove",
    deep: true,
  });

  assertEquals(withRemove as any, {
    name: "John Updated",
  });

  // With 'ignore' behavior - phone removed, email ignored (not in result)
  const withIgnore = sanitizeForMongoDB(updateData, {
    undefinedBehavior: "ignore",
    deep: true,
  });

  assertEquals(withIgnore as any, {
    name: "John Updated",
  });
});

Deno.test("Configuration: ignore behavior vs remove behavior", () => {
  const testData = {
    field1: "value1",
    field2: undefined,
    field3: "value3",
    nested: {
      prop1: "nested1",
      prop2: undefined,
      prop3: "nested3",
    },
  };

  // Test 'ignore' behavior
  const ignoredResult = sanitizeForMongoDB(testData, {
    undefinedBehavior: "ignore",
    deep: true,
  });

  assertEquals(ignoredResult, {
    field1: "value1",
    field3: "value3",
    nested: {
      prop1: "nested1",
      prop3: "nested3",
    },
  });

  // Test 'remove' behavior
  const removedResult = sanitizeForMongoDB(testData, {
    undefinedBehavior: "remove",
    deep: true,
  });

  assertEquals(removedResult, {
    field1: "value1",
    field3: "value3",
    nested: {
      prop1: "nested1",
      prop3: "nested3",
    },
  });

  // Both should be identical for 'undefined' values (only difference is intent)
  assertEquals(ignoredResult, removedResult);
});

Deno.test("Configuration: removeField() works consistently across all behaviors", () => {
  const testData = {
    keep: "this",
    remove1: removeField(),
    remove2: removeField(),
    nested: {
      keep: "nested",
      remove: removeField(),
    },
  };

  // Test with 'remove' behavior
  const removeResult = sanitizeForMongoDB(testData, {
    undefinedBehavior: "remove",
    deep: true,
  });

  // Test with 'ignore' behavior
  const ignoreResult = sanitizeForMongoDB(testData, {
    undefinedBehavior: "ignore",
    deep: true,
  });

  const expectedResult = {
    keep: "this",
    nested: {
      keep: "nested",
    },
  };

  // removeField() should work the same regardless of undefinedBehavior setting
  assertEquals(removeResult, expectedResult);
  assertEquals(ignoreResult, expectedResult);
});

Deno.test("Configuration: error behavior throws on undefined", () => {
  const testDataWithUndefined = {
    field1: "value1",
    field2: undefined,
    field3: "value3",
  };

  const testDataWithRemoveField = {
    field1: "value1",
    field2: removeField(),
    field3: "value3",
  };

  // Should throw with undefined
  try {
    sanitizeForMongoDB(testDataWithUndefined, {
      undefinedBehavior: "error",
      deep: true,
    });
    assert(false, "Should have thrown an error for undefined");
  } catch (error) {
    assert(error instanceof Error);
    assert(error.message.includes("Undefined values are not allowed"));
  }

  // Should work fine with removeField()
  const result = sanitizeForMongoDB(testDataWithRemoveField, {
    undefinedBehavior: "error",
    deep: true,
  });

  assertEquals(result as any, {
    field1: "value1",
    field3: "value3",
  });
});

Deno.test("Real-world scenario: User profile update with different field intentions", () => {
  // Simulates a realistic user profile update scenario
  const currentProfile = {
    name: "John Doe",
    email: "john@example.com",
    phone: "123-456-7890",
    bio: "Software developer",
    avatar: "http://example.com/avatar.jpg",
  };

  // User wants to:
  // 1. Update name
  // 2. Remove phone (explicit)
  // 3. Not change email (undefined, should be ignored in 'ignore' mode)
  // 4. Clear bio (explicit removal)
  // 5. Update avatar
  const updateData = {
    name: "John Smith",
    email: undefined, // Don't touch this field
    phone: removeField(), // Explicitly remove
    bio: removeField(), // Explicitly remove
    avatar: "http://example.com/new-avatar.jpg",
  };

  // With 'ignore' behavior - undefined email should not appear in update
  const updateForIgnore = sanitizeForMongoDB(updateData, {
    undefinedBehavior: "ignore",
    deep: true,
  });

  // Should only include fields that were explicitly set or removed
  assertEquals(updateForIgnore as any, {
    name: "John Smith",
    avatar: "http://example.com/new-avatar.jpg",
    // Note: phone and bio are removed, email is not included
  });

  // With 'remove' behavior - undefined email will be removed
  const updateForRemove = sanitizeForMongoDB(updateData, {
    undefinedBehavior: "remove",
    deep: true,
  });

  // Same result in this case since removeField() and undefined both remove
  assertEquals(updateForRemove as any, {
    name: "John Smith",
    avatar: "http://example.com/new-avatar.jpg",
  });
});

Deno.test("Edge case: Empty objects and arrays after sanitization", () => {
  const testData = {
    emptyAfterSanitization: {
      prop1: undefined,
      prop2: removeField(),
      prop3: undefined,
    },
    arrayWithUndefined: [undefined, undefined, undefined],
    mixedArray: ["keep", undefined, removeField(), "also keep"],
    normalField: "normal value",
  };

  const result = sanitizeForMongoDB(testData, {
    undefinedBehavior: "remove",
    deep: true,
  });

  assertEquals(result as any, {
    emptyAfterSanitization: {}, // Empty object remains
    arrayWithUndefined: [], // Empty array remains
    mixedArray: ["keep", "also keep"], // Only defined values remain
    normalField: "normal value",
  });
});

Deno.test("Type consistency: Results should be properly typed", () => {
  interface TestInterface {
    required: string;
    optional?: string;
    nested?: {
      prop?: string;
    };
  }

  const testData: TestInterface = {
    required: "value",
    optional: undefined,
    nested: {
      prop: undefined,
    },
  };

  const result = sanitizeForMongoDB(testData, {
    undefinedBehavior: "remove",
    deep: true,
  });

  // Result should maintain type structure while removing undefined values
  assertEquals(result, {
    required: "value",
    nested: {},
  });

  // TypeScript should recognize this as the same interface type
  const typedResult = result as TestInterface;
  assertEquals(typedResult.required, "value");
  assert(!("optional" in typedResult)); // Field should not exist after sanitization
});

// Tests for extractFieldsToRemove with deep objects
// NOTE: Since v2, objects are NOT flattened by default (full replacement).
// Use partial() to opt-in to dot notation (merge behavior).

Deno.test("extractFieldsToRemove - simple flat object", () => {
  const result = extractFieldsToRemove({
    name: "John",
    email: removeField(),
    age: 30,
  });

  assertEquals(result.set, { name: "John", age: 30 });
  assertEquals(result.unset, { email: 1 });
});

Deno.test("extractFieldsToRemove - nested object WITHOUT partial (full replacement)", () => {
  // Without partial(), nested objects are kept as-is (full replacement)
  const result = extractFieldsToRemove({
    name: "John",
    settings: {
      theme: "dark",
      language: "fr",
    },
  });

  assertEquals(result.set, {
    name: "John",
    settings: {
      theme: "dark",
      language: "fr",
    },
  });
  assertEquals(result.unset, {});
});

Deno.test("extractFieldsToRemove - nested object WITH partial (merge/dot notation)", () => {
  // With partial(), nested objects use dot notation for merging
  const result = extractFieldsToRemove({
    name: "John",
    settings: partial({
      theme: removeField(),
      language: "fr",
    }),
  });

  assertEquals(result.set, {
    name: "John",
    "settings.language": "fr",
  });
  assertEquals(result.unset, { "settings.theme": 1 });
});

Deno.test("extractFieldsToRemove - deeply nested object with partial", () => {
  const result = extractFieldsToRemove({
    user: partial({
      profile: partial({
        avatar: removeField(),
        bio: "Hello",
        social: partial({
          twitter: removeField(),
          github: "user123",
        }),
      }),
    }),
  });

  assertEquals(result.set, {
    "user.profile.bio": "Hello",
    "user.profile.social.github": "user123",
  });
  assertEquals(result.unset, {
    "user.profile.avatar": 1,
    "user.profile.social.twitter": 1,
  });
});

Deno.test("extractFieldsToRemove - mixed with arrays", () => {
  const result = extractFieldsToRemove({
    name: "John",
    tags: ["a", "b", "c"], // Arrays should not be recursed into
    metadata: partial({
      created: removeField(),
      updated: "2024-01-01",
    }),
  });

  assertEquals(result.set, {
    name: "John",
    tags: ["a", "b", "c"],
    "metadata.updated": "2024-01-01",
  });
  assertEquals(result.unset, { "metadata.created": 1 });
});

Deno.test("extractFieldsToRemove - entire nested object removal", () => {
  const result = extractFieldsToRemove({
    name: "John",
    settings: removeField(), // Remove entire nested object
    profile: {
      bio: "Hello",
    },
  });

  // Without partial(), profile is kept as full object
  assertEquals(result.set, {
    name: "John",
    profile: {
      bio: "Hello",
    },
  });
  assertEquals(result.unset, { settings: 1 });
});

Deno.test("extractFieldsToRemove - empty nested object", () => {
  const result = extractFieldsToRemove({
    name: "John",
    empty: {},
    settings: partial({
      theme: removeField(),
    }),
  });

  assertEquals(result.set, { name: "John", empty: {} });
  assertEquals(result.unset, { "settings.theme": 1 });
});

Deno.test("extractFieldsToRemove - all fields removed in nested with partial", () => {
  const result = extractFieldsToRemove({
    name: "John",
    settings: partial({
      theme: removeField(),
      language: removeField(),
    }),
  });

  assertEquals(result.set, { name: "John" });
  assertEquals(result.unset, {
    "settings.theme": 1,
    "settings.language": 1,
  });
});

Deno.test("extractFieldsToRemove - preserves null values", () => {
  const result = extractFieldsToRemove({
    name: "John",
    email: null,
    settings: partial({
      theme: null,
      language: removeField(),
    }),
  });

  assertEquals(result.set, {
    name: "John",
    email: null,
    "settings.theme": null,
  });
  assertEquals(result.unset, { "settings.language": 1 });
});

Deno.test("extractFieldsToRemove - partial at different nesting levels", () => {
  // Only the level with partial() gets flattened
  const result = extractFieldsToRemove({
    user: partial({
      name: "John",
      address: {
        city: "Paris",
        country: "France",
      },
    }),
  });

  // user is flattened, but address inside is NOT (no partial on it)
  assertEquals(result.set, {
    "user.name": "John",
    "user.address": {
      city: "Paris",
      country: "France",
    },
  });
  assertEquals(result.unset, {});
});

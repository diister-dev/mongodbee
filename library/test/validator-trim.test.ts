import { test, expect } from "vitest";
import * as v from "../src/schema.ts";
import { toMongoValidator } from "../src/validator.ts";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";

test("Valibot trim action validation", () => {
  // Test how trim works with Valibot
  const trimSchema = v.object({
    name: v.pipe(v.string(), v.trim(), v.nonEmpty()),
    email: v.pipe(v.string(), v.trim(), v.regex(/^.+@.+\..+$/)),
    description: v.pipe(v.string(), v.trim()),
  });

  // Test that trim works correctly with Valibot
  const validWithSpaces = v.safeParse(trimSchema, {
    name: "  John  ",
    email: "  john@test.com  ",
    description: "  Some description  ",
  });

  expect(validWithSpaces.success).toBeTruthy();
  expect(validWithSpaces.output.name).toEqual("John");
  expect(validWithSpaces.output.email).toEqual("john@test.com");
  expect(validWithSpaces.output.description).toEqual("Some description");

  // Test that empty string after trim fails nonEmpty validation
  const invalidEmptyAfterTrim = v.safeParse(trimSchema, {
    name: "   ", // Only spaces, will be empty after trim
    email: "john@test.com",
    description: "test",
  });

  expect(!invalidEmptyAfterTrim.success).toBeTruthy();

  // Test valid minimal case
  const validMinimal = v.safeParse(trimSchema, {
    name: "John",
    email: "john@test.com",
    description: "",
  });

  expect(validMinimal.success).toBeTruthy();
});

test("MongoDB validator generation for trim", () => {
  const trimSchema = v.object({
    name: v.pipe(v.string(), v.trim(), v.nonEmpty()),
    email: v.pipe(v.string(), v.trim(), v.regex(/^.+@.+\..+$/)),
    description: v.pipe(v.string(), v.trim()),
  });

  const validator = toMongoValidator(trimSchema);
  const jsonSchema = validator.$jsonSchema!;

  // Trim action should not generate specific MongoDB validation
  // because trimming happens at application level, not database level
  expect(jsonSchema.properties!.name).toEqual({
    bsonType: "string",
    description: "must be a string",
    minLength: 1,
    minItems: 1,
  });

  expect(jsonSchema.properties!.email).toEqual({
    bsonType: "string",
    description: "must be a string",
    pattern: "^.+@.+\\..+$",
  });

  expect(jsonSchema.properties!.description).toEqual({
    bsonType: "string",
    description: "must be a string",
  });
});

test("Collection integration with trim", async () => {
  await withDatabase("Collection integration with trim", async (db) => {
    const users = await collection(db, "users", {
      name: v.pipe(v.string(), v.trim(), v.nonEmpty()),
      email: v.pipe(v.string(), v.trim(), v.regex(/^.+@.+\..+$/)),
      bio: v.pipe(v.string(), v.trim()),
    });

    // Insert with spaces - should be trimmed
    const userId = await users.insertOne({
      name: "  Alice  ",
      email: "  alice@test.com  ",
      bio: "  Software developer  ",
    });

    const insertedUser = await users.getById(userId);
    expect(insertedUser.name).toEqual("Alice");
    expect(insertedUser.email).toEqual("alice@test.com");
    expect(insertedUser.bio).toEqual("Software developer");

    // Try to insert empty name after trim - should fail
    try {
      await users.insertOne({
        name: "   ", // Only spaces
        email: "bob@test.com",
        bio: "test",
      });
      expect(false).toBeTruthy();
    } catch (error: unknown) {
      // Expected to fail due to nonEmpty validation
      const errorObj = error as { message?: string; issues?: unknown };
      expect(errorObj.message?.includes("validation") || errorObj.issues).toBeTruthy();
    }
  });
});

test("Trim with various string scenarios", () => {
  const schema = v.object({
    field: v.pipe(v.string(), v.trim(), v.nonEmpty()),
  });

  // Valid cases
  const cases = [
    { input: "hello", expected: "hello" },
    { input: "  hello  ", expected: "hello" },
    { input: "\t\nhello\t\n", expected: "hello" },
    { input: "  hello world  ", expected: "hello world" },
  ];

  for (const { input, expected } of cases) {
    const result = v.safeParse(schema, { field: input });
    expect(result.success).toBeTruthy();
    expect(result.output.field).toEqual(expected);
  }

  // Invalid cases (empty after trim)
  const invalidCases = ["", "   ", "\t\n", "  \t  \n  "];

  for (const input of invalidCases) {
    const result = v.safeParse(schema, { field: input });
    expect(!result.success).toBeTruthy();
  }
});

import { assertEquals, assert } from "@std/assert";
import * as v from '../src/schema.ts';
import { toMongoValidator } from '../src/validator.ts';
import { withDatabase } from './+shared.ts';
import { collection } from '../src/collection.ts';

Deno.test("Valibot trim action validation", () => {
    // Test how trim works with Valibot
    const trimSchema = v.object({
        name: v.pipe(v.string(), v.trim(), v.nonEmpty()),
        email: v.pipe(v.string(), v.trim(), v.regex(/^.+@.+\..+$/)),
        description: v.pipe(v.string(), v.trim())
    });

    // Test that trim works correctly with Valibot
    const validWithSpaces = v.safeParse(trimSchema, {
        name: "  John  ",
        email: "  john@test.com  ",
        description: "  Some description  "
    });
    
    assert(validWithSpaces.success);
    assertEquals(validWithSpaces.output.name, "John");
    assertEquals(validWithSpaces.output.email, "john@test.com");
    assertEquals(validWithSpaces.output.description, "Some description");

    // Test that empty string after trim fails nonEmpty validation
    const invalidEmptyAfterTrim = v.safeParse(trimSchema, {
        name: "   ",  // Only spaces, will be empty after trim
        email: "john@test.com",
        description: "test"
    });
    
    assert(!invalidEmptyAfterTrim.success);

    // Test valid minimal case
    const validMinimal = v.safeParse(trimSchema, {
        name: "John",
        email: "john@test.com", 
        description: ""
    });
    
    assert(validMinimal.success);
});

Deno.test("MongoDB validator generation for trim", () => {
    const trimSchema = v.object({
        name: v.pipe(v.string(), v.trim(), v.nonEmpty()),
        email: v.pipe(v.string(), v.trim(), v.regex(/^.+@.+\..+$/)),
        description: v.pipe(v.string(), v.trim())
    });

    const validator = toMongoValidator(trimSchema);
    const jsonSchema = validator.$jsonSchema!;
    
    // Trim action should not generate specific MongoDB validation
    // because trimming happens at application level, not database level
    assertEquals(jsonSchema.properties!.name, {
        bsonType: "string",
        description: "must be a string",
        minLength: 1,
        minItems: 1
    });

    assertEquals(jsonSchema.properties!.email, {
        bsonType: "string", 
        description: "must be a string",
        pattern: "^.+@.+\\..+$"
    });

    assertEquals(jsonSchema.properties!.description, {
        bsonType: "string",
        description: "must be a string"
    });
});

Deno.test("Collection integration with trim", async (t) => {
    await withDatabase(t.name, async (db) => {
        const users = await collection(db, "users", {
            name: v.pipe(v.string(), v.trim(), v.nonEmpty()),
            email: v.pipe(v.string(), v.trim(), v.regex(/^.+@.+\..+$/)),
            bio: v.pipe(v.string(), v.trim())
        });

        // Insert with spaces - should be trimmed
        const userId = await users.insertOne({
            name: "  Alice  ",
            email: "  alice@test.com  ",
            bio: "  Software developer  "
        });

        const insertedUser = await users.getById(userId);
        assertEquals(insertedUser.name, "Alice");
        assertEquals(insertedUser.email, "alice@test.com");
        assertEquals(insertedUser.bio, "Software developer");

        // Try to insert empty name after trim - should fail
        try {
            await users.insertOne({
                name: "   ",  // Only spaces
                email: "bob@test.com",
                bio: "test"
            });
            assert(false, "Should have failed");
        } catch (error: unknown) {
            // Expected to fail due to nonEmpty validation
            const errorObj = error as { message?: string; issues?: unknown };
            assert(errorObj.message?.includes("validation") || errorObj.issues);
        }
    });
});

Deno.test("Trim with various string scenarios", () => {
    const schema = v.object({
        field: v.pipe(v.string(), v.trim(), v.nonEmpty())
    });

    // Valid cases
    const cases = [
        { input: "hello", expected: "hello" },
        { input: "  hello  ", expected: "hello" },
        { input: "\t\nhello\t\n", expected: "hello" },
        { input: "  hello world  ", expected: "hello world" }
    ];

    for (const { input, expected } of cases) {
        const result = v.safeParse(schema, { field: input });
        assert(result.success, `Should succeed for input: "${input}"`);
        assertEquals(result.output.field, expected);
    }

    // Invalid cases (empty after trim)
    const invalidCases = ["", "   ", "\t\n", "  \t  \n  "];
    
    for (const input of invalidCases) {
        const result = v.safeParse(schema, { field: input });
        assert(!result.success, `Should fail for input: "${input}"`);
    }
});

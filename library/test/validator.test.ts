import { toMongoValidator } from "../src/validator.ts"
import * as v from "../src/schema.ts";
import { assert, assertEquals } from "jsr:@std/assert";

Deno.test("Simple schema test", () => {
    const schema = v.object({
        a: v.string(),
        b: v.object({
            c: v.number(),
        })
    })

    const validator = toMongoValidator(schema);

    assertEquals(validator, {
        "$jsonSchema": {
          bsonType: "object",
          properties: {
            a: {
              bsonType: "string",
              description: "must be a string",
            },
            b: {
              bsonType: "object",
              properties: {
                c: {
                  bsonType: "number",
                  description: "must be a number",
                },
              },
              required: [
                "c",
              ],
            },
          },
          required: [
            "a",
            "b",
          ],
        }
    })
});

Deno.test("Basic types schemas", () => {
    const schema = v.object({
        stringField: v.string(),
        numberField: v.number(),
        booleanField: v.boolean(),
        dateField: v.date(),
        nullField: v.null()
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;

    assertEquals(jsonSchema.properties!.stringField, {
        bsonType: "string",
        description: "must be a string"
    });

    assertEquals(jsonSchema.properties!.numberField, {
        bsonType: "number",
        description: "must be a number"
    });

    assertEquals(jsonSchema.properties!.booleanField, {
        bsonType: "bool",
        description: "must be a boolean"
    });

    assertEquals(jsonSchema.properties!.dateField, {
        bsonType: "date",
        description: "must be a date"
    });

    assertEquals(jsonSchema.properties!.nullField, {
        bsonType: "null",
        description: "must be null"
    });
});

Deno.test("String validations", () => {
    const schema = v.object({
        minLength: v.pipe(v.string(), v.minLength(5)),
        maxLength: v.pipe(v.string(), v.maxLength(10)),
        exactLength: v.pipe(v.string(), v.length(8)),
        pattern: v.pipe(v.string(), v.regex(/^[a-z]+$/)),
        combined: v.pipe(v.string(), v.minLength(3), v.maxLength(8), v.regex(/^[A-Z]+$/))
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    assertEquals(jsonSchema.properties!.minLength, {
        bsonType: "string",
        description: "must be a string",
        minLength: 5
    });

    assertEquals(jsonSchema.properties!.maxLength, {
        bsonType: "string",
        description: "must be a string",
        maxLength: 10
    });

    assertEquals(jsonSchema.properties!.exactLength, {
        bsonType: "string",
        description: "must be a string",
        minLength: 8,
        maxLength: 8
    });

    assertEquals(jsonSchema.properties!.pattern, {
        bsonType: "string",
        description: "must be a string",
        pattern: "^[a-z]+$"
    });
    
    assertEquals(jsonSchema.properties!.combined, {
        bsonType: "string",
        description: "must be a string",
        minLength: 3,
        maxLength: 8,
        pattern: "^[A-Z]+$"
    });
});

Deno.test("Number validations", () => {
    const schema = v.object({
        min: v.pipe(v.number(), v.minValue(5)),
        max: v.pipe(v.number(), v.maxValue(10)),
        range: v.pipe(v.number(), v.minValue(1), v.maxValue(100))
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    assertEquals(jsonSchema.properties!.min, {
        bsonType: "number",
        description: "must be a number",
        minimum: 5
    });

    assertEquals(jsonSchema.properties!.max, {
        bsonType: "number",
        description: "must be a number",
        maximum: 10
    });

    assertEquals(jsonSchema.properties!.range, {
        bsonType: "number",
        description: "must be a number",
        minimum: 1,
        maximum: 100
    });
});

Deno.test("Array schema", () => {
    const schema = v.object({
        simpleArray: v.array(v.string()),
        typedArray: v.array(v.number()),
        objectArray: v.array(v.object({ name: v.string() })),
        constrainedArray: v.pipe(
            v.array(v.string()),
            v.minLength(2),
            v.maxLength(5)
        )
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    assertEquals(jsonSchema.properties!.simpleArray, {
        bsonType: "array",
        items: {
            bsonType: "string",
            description: "must be a string"
        },
        description: "must be an array"
    });

    assertEquals(jsonSchema.properties!.typedArray, {
        bsonType: "array",
        items: {
            bsonType: "number",
            description: "must be a number"
        },
        description: "must be an array"
    });

    assertEquals(jsonSchema.properties!.objectArray, {
        bsonType: "array",
        items: {
            bsonType: "object",
            properties: {
                name: {
                    bsonType: "string",
                    description: "must be a string"
                }
            },
            required: ["name"]
        },
        description: "must be an array"
    });

    assertEquals(jsonSchema.properties!.constrainedArray, {
        bsonType: "array",
        items: {
            bsonType: "string",
            description: "must be a string"
        },
        minItems: 2,
        maxItems: 5,
        description: "must be an array"
    });
});

Deno.test("Optional fields", () => {
    const schema = v.object({
        required: v.string(),
        optional: v.optional(v.string()),
        optionalWithDefault: v.optional(v.number(), 42)
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    // Vérifie que le champ 'required' est dans la liste des champs requis
    assert(jsonSchema.required!.includes("required"));
    
    // Vérifie que le champ 'optional' n'est PAS dans la liste des champs requis
    assert(!jsonSchema.required!.includes("optional"));
    
    // Le champ 'optionalWithDefault' est requis car il a une valeur par défaut
    assert(jsonSchema.required!.includes("optionalWithDefault"));
    
    assertEquals(jsonSchema.properties!.optional, {
        bsonType: "string",
        description: "must be a string"
    });

    assertEquals(jsonSchema.properties!.optionalWithDefault, {
        bsonType: "number",
        description: "must be a number"
    });
});

Deno.test("Union schema", () => {
    const schema = v.object({
        stringOrNumber: v.union([v.string(), v.number()]),
        optionalField: v.union([v.string(), v.undefined()])
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    assertEquals(jsonSchema.properties!.stringOrNumber, {
        anyOf: [
            {
                bsonType: "string",
                description: "must be a string"
            },
            {
                bsonType: "number",
                description: "must be a number"
            }
        ]
    });

    assertEquals(jsonSchema.required!.includes("optionalField"), false);
});

Deno.test("Intersect schema", () => {
    const nameSchema = v.object({ name: v.string() });
    const ageSchema = v.object({ age: v.number() });
    
    const schema = v.object({
        person: v.intersect([nameSchema, ageSchema])
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    assertEquals(jsonSchema.properties!.person, {
        allOf: [
            {
                bsonType: "object",
                properties: {
                    name: {
                        bsonType: "string",
                        description: "must be a string"
                    }
                },
                required: ["name"]
            },
            {
                bsonType: "object",
                properties: {
                    age: {
                        bsonType: "number",
                        description: "must be a number"
                    }
                },
                required: ["age"]
            }
        ]
    });
});

Deno.test("Complex nested schema", () => {
    const schema = v.object({
        user: v.object({
            name: v.string(),
            age: v.pipe(v.number(), v.minValue(0)),
            contact: v.object({
                email: v.pipe(v.string(), v.regex(/^.+@.+\..+$/)),
                phone: v.optional(v.string())
            }),
            tags: v.array(v.string())
        }),
        metadata: v.object({
            createdAt: v.date(),
            updatedAt: v.optional(v.date())
        })
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    // Vérifier la structure générale
    assertEquals(jsonSchema.required!.includes("user"), true);
    assertEquals(jsonSchema.required!.includes("metadata"), true);
    
    // Vérifier les propriétés imbriquées
    const userProps = jsonSchema.properties!.user.properties!;
    assertEquals(userProps.name.bsonType, "string");
    assertEquals(userProps.age.minimum, 0);
    
    // Vérifier les imbrications profondes
    const contactProps = userProps.contact.properties!;
    assertEquals(contactProps.email.pattern, "^.+@.+\\..+$");
    assertEquals(userProps.contact.required!.includes("phone"), false);
    
    // Vérifier les tableaux
    assertEquals(userProps.tags.bsonType, "array");
    assertEquals(userProps.tags.items.bsonType, "string");
    
    // Vérifier les dates
    const metadataProps = jsonSchema.properties!.metadata.properties!;
    assertEquals(metadataProps.createdAt.bsonType, "date");
    assertEquals(metadataProps.updatedAt.bsonType, "date");
});

Deno.test("Multiple regex patterns should be combined correctly", () => {
    const schema = v.object({
        username: v.pipe(
            v.string(),
            v.regex(/^[a-z]/),
            v.regex(/[a-z0-9]+$/),
            v.minLength(3),
            v.maxLength(20)
        )
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    const usernameProps = jsonSchema.properties!.username;
    assertEquals(usernameProps.bsonType, "string");
    assertEquals(usernameProps.minLength, 3);
    assertEquals(usernameProps.maxLength, 20);
    assert(usernameProps.pattern!.includes("^[a-z]"));
    assert(usernameProps.pattern!.includes("[a-z0-9]+$"));
});

Deno.test("Any schema type", () => {
    const schema = v.object({
        anyField: v.any()
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    assert(jsonSchema.properties!.anyField === undefined); // 'any' doesn't add a validator
    assertEquals(jsonSchema.required!.includes("anyField"), true); // still a required field
});

Deno.test("Literal schema", () => {
    const schema = v.object({
        stringLiteral: v.literal("active"),
        numberLiteral: v.literal(42),
        booleanLiteral: v.literal(true)
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    assertEquals(jsonSchema.properties!.stringLiteral, {
        bsonType: "string",
        enum: ["active"],
        description: "must be active"
    });

    assertEquals(jsonSchema.properties!.numberLiteral, {
        bsonType: "number",
        enum: [42],
        description: "must be 42"
    });

    assertEquals(jsonSchema.properties!.booleanLiteral, {
        bsonType: "bool",
        enum: [true],
        description: "must be true"
    });
});

Deno.test("Enum schema", () => {
    enum StringEnum {
        Option1 = "option1",
        Option2 = "option2",
        Option3 = "option3"
    }

    enum NumericEnum {
        One = 1,
        Two = 2,
        Three = 3
    }
    
    const schema = v.object({
        stringEnum: v.enum(StringEnum),
        numberEnum: v.enum(NumericEnum)
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    assertEquals(jsonSchema.properties!.stringEnum, {
        bsonType: "string",
        enum: ["option1", "option2", "option3"],
        description: "must be one of the allowed values"
    });

    assertEquals(jsonSchema.properties!.numberEnum, {
        bsonType: "number",
        enum: [1, 2, 3],
        description: "must be one of the allowed values"
    });
});

Deno.test("Literal with pipes", () => {
    const schema = v.object({
        status: v.pipe(
            v.string(),
            v.literal("active")
        )
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    // For literal with pipes, the first validation in the pipe (v.string()) 
    // sets the bsonType, and then literal adds the enum constraint
    assertEquals(jsonSchema.properties!.status, {
        bsonType: "string",
        description: "must be a string",
        enum: ["active"]
    });
});

Deno.test("Record schema - string keys and number values", () => {
    const schema = v.object({
        map: v.record(v.string(), v.number())
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;

    assertEquals(jsonSchema.properties!.map, {
        bsonType: "object",
        description: "must be a record",
        additionalProperties: {
            bsonType: "number",
            description: "must be a number"
        }
    });
});

Deno.test("Record schema with key regex", () => {
    const schema = v.object({
        mapRegex: v.record(v.pipe(v.string(), v.regex(/^[a-z]+$/)), v.string())
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;

    assertEquals(jsonSchema.properties!.mapRegex.patternProperties, {
        "^[a-z]+$": {
            bsonType: "string",
            description: "must be a string"
        }
    });

    assertEquals(jsonSchema.properties!.mapRegex.additionalProperties, false);
});

Deno.test("Record validation should accept valid and reject invalid values", () => {
    const schema = v.object({
        map: v.record(v.string(), v.number())
    });

    // Valid document
    const ok = v.safeParse(schema, { map: { a: 1, b: 2 } });
    assert(ok.success);

    // Invalid value for a key
    const nokValue = v.safeParse(schema, { map: { a: "1" } });
    assert(!nokValue.success);

    // Invalid overall type
    const nokType = v.safeParse(schema, { map: "not-an-object" });
    assert(!nokType.success);
});

Deno.test("Record key regex validation should accept and reject keys", () => {
    const schema = v.object({
        mapRegex: v.record(v.pipe(v.string(), v.regex(/^[a-z]+$/)), v.string())
    });

    const ok = v.safeParse(schema, { mapRegex: { abc: "ok", xyz: "ok" } });
    assert(ok.success);

    const nok = v.safeParse(schema, { mapRegex: { Abc: "ok" } });
    assert(!nok.success);
});

Deno.test("Deep nested record schema: toMongoValidator structure and valibot validation", () => {
    const schema = v.object({
        level1: v.object({
            level2: v.record(v.string(), v.object({ x: v.number() }))
        })
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;

    // Check generated JSON Schema for deep nested record
    assertEquals(jsonSchema.properties!.level1.properties!.level2.additionalProperties, {
        bsonType: "object",
        properties: {
            x: {
                bsonType: "number",
                description: "must be a number"
            }
        },
        required: ["x"]
    });

    // Valid document
    const ok = v.safeParse(schema, { level1: { level2: { a: { x: 1 }, b: { x: 2 } } } });
    assert(ok.success);

    // Invalid document: nested value wrong type
    const nok = v.safeParse(schema, { level1: { level2: { a: { x: "no" } } } });
    assert(!nok.success);
});

Deno.test("Record schema complex cases", () => {
    // Record with enum keys
    const enumSchema = v.object({
        enumRecord: v.record(v.picklist(["a", "b", "c"]), v.number())
    });

    const enumValidator = toMongoValidator(enumSchema);
    const enumJsonSchema = enumValidator.$jsonSchema!;

    // Since enum doesn't produce a pattern, it should use additionalProperties
    assertEquals(enumJsonSchema.properties!.enumRecord, {
        bsonType: "object",
        description: "must be a record",
        additionalProperties: {
            bsonType: "number",
            description: "must be a number"
        }
    });

    // Record with complex value schema
    const complexSchema = v.object({
        complexRecord: v.record(
            v.pipe(v.string(), v.regex(/^key_/)),
            v.object({
                name: v.string(),
                count: v.number()
            })
        )
    });

    const complexValidator = toMongoValidator(complexSchema);
    const complexJsonSchema = complexValidator.$jsonSchema!;

    assertEquals(complexJsonSchema.properties!.complexRecord, {
        bsonType: "object",
        description: "must be a record",
        patternProperties: {
            "^key_": {
                bsonType: "object",
                properties: {
                    name: {
                        bsonType: "string",
                        description: "must be a string"
                    },
                    count: {
                        bsonType: "number",
                        description: "must be a number"
                    }
                },
                required: ["name", "count"]
            }
        },
        additionalProperties: false
    });
});

Deno.test("Nullable schema", () => {
    const schema = v.object({
        nullableString: v.nullable(v.string()),
        nullableNumber: v.nullable(v.number()),
        nullableObject: v.nullable(v.object({ name: v.string() })),
        nullableStringWithDefault: v.nullable(v.string(), "default")
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    // Test nullable string
    assertEquals(jsonSchema.properties!.nullableString, {
        anyOf: [
            {
                bsonType: "string",
                description: "must be a string"
            },
            {
                bsonType: "null"
            }
        ]
    });

    // Test nullable number
    assertEquals(jsonSchema.properties!.nullableNumber, {
        anyOf: [
            {
                bsonType: "number",
                description: "must be a number"
            },
            {
                bsonType: "null"
            }
        ]
    });

    // Test nullable object
    assertEquals(jsonSchema.properties!.nullableObject, {
        anyOf: [
            {
                bsonType: "object",
                properties: {
                    name: {
                        bsonType: "string",
                        description: "must be a string"
                    }
                },
                required: ["name"]
            },
            {
                bsonType: "null"
            }
        ]
    });

    // Test nullable with default (should still be required in MongoDB schema)
    assertEquals(jsonSchema.properties!.nullableStringWithDefault, {
        anyOf: [
            {
                bsonType: "string",
                description: "must be a string"
            },
            {
                bsonType: "null"
            }
        ]
    });

    // All nullable fields should be required in the schema (MongoDB doesn't handle defaults)
    assert(jsonSchema.required!.includes("nullableString"));
    assert(jsonSchema.required!.includes("nullableNumber"));
    assert(jsonSchema.required!.includes("nullableObject"));
    assert(jsonSchema.required!.includes("nullableStringWithDefault"));
});

Deno.test("Nullable with validations", () => {
    const schema = v.object({
        nullableEmail: v.nullable(v.pipe(v.string(), v.regex(/^.+@.+\..+$/))),
        nullableAge: v.nullable(v.pipe(v.number(), v.minValue(0), v.maxValue(120))),
        nullableArray: v.nullable(v.pipe(v.array(v.string()), v.minLength(1), v.maxLength(5)))
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    // Test nullable string with regex validation
    assertEquals(jsonSchema.properties!.nullableEmail, {
        anyOf: [
            {
                bsonType: "string",
                description: "must be a string",
                pattern: "^.+@.+\\..+$"
            },
            {
                bsonType: "null"
            }
        ]
    });

    // Test nullable number with range validation
    assertEquals(jsonSchema.properties!.nullableAge, {
        anyOf: [
            {
                bsonType: "number",
                description: "must be a number",
                minimum: 0,
                maximum: 120
            },
            {
                bsonType: "null"
            }
        ]
    });

    // Test nullable array with length validation
    assertEquals(jsonSchema.properties!.nullableArray, {
        anyOf: [
            {
                bsonType: "array",
                items: {
                    bsonType: "string",
                    description: "must be a string"
                },
                description: "must be an array",
                minItems: 1,
                maxItems: 5
            },
            {
                bsonType: "null"
            }
        ]
    });
});

Deno.test("Nullable validation with Valibot", () => {
    // Test that nullable schemas work correctly with Valibot validation
    const schema = v.object({
        nullableString: v.nullable(v.string()),
        nullableNumber: v.nullable(v.pipe(v.number(), v.minValue(0)))
    });

    // Valid: null values
    const validNull = v.safeParse(schema, { 
        nullableString: null, 
        nullableNumber: null 
    });
    assert(validNull.success);

    // Valid: actual values
    const validValues = v.safeParse(schema, { 
        nullableString: "test", 
        nullableNumber: 42 
    });
    assert(validValues.success);

    // Invalid: wrong types (should fail)
    const invalidType = v.safeParse(schema, { 
        nullableString: 123, 
        nullableNumber: "not a number" 
    });
    assert(!invalidType.success);

    // Invalid: number validation fails
    const invalidNumber = v.safeParse(schema, { 
        nullableString: "test", 
        nullableNumber: -5 
    });
    assert(!invalidNumber.success);

    // Valid: mixed null and values
    const mixedValid = v.safeParse(schema, { 
        nullableString: "test", 
        nullableNumber: null 
    });
    assert(mixedValid.success);
});

Deno.test("Nested nullable schemas", () => {
    const schema = v.object({
        user: v.nullable(v.object({
            name: v.string(),
            age: v.nullable(v.number())
        })),
        tags: v.array(v.nullable(v.string()))
    });

    const validator = toMongoValidator(schema);
    const jsonSchema = validator.$jsonSchema!;
    
    // Test nested nullable object
    assertEquals(jsonSchema.properties!.user, {
        anyOf: [
            {
                bsonType: "object",
                properties: {
                    name: {
                        bsonType: "string",
                        description: "must be a string"
                    },
                    age: {
                        anyOf: [
                            {
                                bsonType: "number",
                                description: "must be a number"
                            },
                            {
                                bsonType: "null"
                            }
                        ]
                    }
                },
                required: ["name", "age"]
            },
            {
                bsonType: "null"
            }
        ]
    });

    // Test array of nullable items
    assertEquals(jsonSchema.properties!.tags, {
        bsonType: "array",
        items: {
            anyOf: [
                {
                    bsonType: "string",
                    description: "must be a string"
                },
                {
                    bsonType: "null"
                }
            ]
        },
        description: "must be an array"
    });
});

Deno.test("Record validation edge cases", () => {
    // Test that patternProperties validation works
    const schema = v.object({
        data: v.record(v.pipe(v.string(), v.regex(/^[a-z]+$/)), v.number())
    });

    // Valid: keys match pattern, values are numbers
    const valid1 = v.safeParse(schema, { data: { abc: 1, xyz: 2 } });
    assert(valid1.success);

    // Invalid: key doesn't match pattern (contains uppercase)
    const invalid1 = v.safeParse(schema, { data: { Abc: 1 } });
    assert(!invalid1.success);

    // Invalid: value is wrong type
    const invalid2 = v.safeParse(schema, { data: { abc: "not-a-number" } });
    assert(!invalid2.success);

    // Valid: empty record
    const valid2 = v.safeParse(schema, { data: {} });
    assert(valid2.success);
});

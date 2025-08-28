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
        dateField: v.date()
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
        propertyNames: {
            bsonType: "string",
            description: "must be a string"
        },
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

    assertEquals(jsonSchema.properties!.mapRegex.propertyNames, {
        bsonType: "string",
        description: "must be a string",
        pattern: "^[a-z]+$"
    });

    assertEquals(jsonSchema.properties!.mapRegex.additionalProperties, {
        bsonType: "string",
        description: "must be a string"
    });
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

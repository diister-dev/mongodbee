import { toMongoValidator } from "../src/validator.ts";
import * as v from "../src/schema.ts";
import { test, expect } from "vitest";

test("Simple schema test", () => {
  const schema = v.object({
    a: v.string(),
    b: v.object({
      c: v.number(),
    }),
  });

  const validator = toMongoValidator(schema);

  expect(validator).toEqual({
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
    },
  });
});

test("Basic types schemas", () => {
  const schema = v.object({
    stringField: v.string(),
    numberField: v.number(),
    booleanField: v.boolean(),
    dateField: v.date(),
    nullField: v.null(),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.stringField).toEqual({
    bsonType: "string",
    description: "must be a string",
  });

  expect(jsonSchema.properties!.numberField).toEqual({
    bsonType: "number",
    description: "must be a number",
  });

  expect(jsonSchema.properties!.booleanField).toEqual({
    bsonType: "bool",
    description: "must be a boolean",
  });

  expect(jsonSchema.properties!.dateField).toEqual({
    bsonType: "date",
    description: "must be a date",
  });

  expect(jsonSchema.properties!.nullField).toEqual({
    bsonType: "null",
    description: "must be null",
  });
});

test("String validations", () => {
  const schema = v.object({
    minLength: v.pipe(v.string(), v.minLength(5)),
    maxLength: v.pipe(v.string(), v.maxLength(10)),
    exactLength: v.pipe(v.string(), v.length(8)),
    pattern: v.pipe(v.string(), v.regex(/^[a-z]+$/)),
    combined: v.pipe(
      v.string(),
      v.minLength(3),
      v.maxLength(8),
      v.regex(/^[A-Z]+$/),
    ),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.minLength).toEqual({
    bsonType: "string",
    description: "must be a string",
    minLength: 5,
  });

  expect(jsonSchema.properties!.maxLength).toEqual({
    bsonType: "string",
    description: "must be a string",
    maxLength: 10,
  });

  expect(jsonSchema.properties!.exactLength).toEqual({
    bsonType: "string",
    description: "must be a string",
    minLength: 8,
    maxLength: 8,
  });

  expect(jsonSchema.properties!.pattern).toEqual({
    bsonType: "string",
    description: "must be a string",
    pattern: "^[a-z]+$",
  });

  expect(jsonSchema.properties!.combined).toEqual({
    bsonType: "string",
    description: "must be a string",
    minLength: 3,
    maxLength: 8,
    pattern: "^[A-Z]+$",
  });
});

test("Number validations", () => {
  const schema = v.object({
    min: v.pipe(v.number(), v.minValue(5)),
    max: v.pipe(v.number(), v.maxValue(10)),
    range: v.pipe(v.number(), v.minValue(1), v.maxValue(100)),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.min).toEqual({
    bsonType: "number",
    description: "must be a number",
    minimum: 5,
  });

  expect(jsonSchema.properties!.max).toEqual({
    bsonType: "number",
    description: "must be a number",
    maximum: 10,
  });

  expect(jsonSchema.properties!.range).toEqual({
    bsonType: "number",
    description: "must be a number",
    minimum: 1,
    maximum: 100,
  });
});

test("Array schema", () => {
  const schema = v.object({
    simpleArray: v.array(v.string()),
    typedArray: v.array(v.number()),
    objectArray: v.array(v.object({ name: v.string() })),
    constrainedArray: v.pipe(
      v.array(v.string()),
      v.minLength(2),
      v.maxLength(5),
    ),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.simpleArray).toEqual({
    bsonType: "array",
    items: {
      bsonType: "string",
      description: "must be a string",
    },
    description: "must be an array",
  });

  expect(jsonSchema.properties!.typedArray).toEqual({
    bsonType: "array",
    items: {
      bsonType: "number",
      description: "must be a number",
    },
    description: "must be an array",
  });

  expect(jsonSchema.properties!.objectArray).toEqual({
    bsonType: "array",
    items: {
      bsonType: "object",
      properties: {
        name: {
          bsonType: "string",
          description: "must be a string",
        },
      },
      required: ["name"],
    },
    description: "must be an array",
  });

  expect(jsonSchema.properties!.constrainedArray).toEqual({
    bsonType: "array",
    items: {
      bsonType: "string",
      description: "must be a string",
    },
    minItems: 2,
    maxItems: 5,
    description: "must be an array",
  });
});

test("Optional fields", () => {
  const schema = v.object({
    required: v.string(),
    optional: v.optional(v.string()),
    optionalWithDefault: v.optional(v.number(), 42),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  // Verifie que le champ 'required' est dans la liste des champs requis
  expect(jsonSchema.required!.includes("required")).toBeTruthy();

  // Verifie que le champ 'optional' n'est PAS dans la liste des champs requis
  expect(!jsonSchema.required!.includes("optional")).toBeTruthy();

  // Le champ 'optionalWithDefault' est requis car il a une valeur par defaut
  expect(jsonSchema.required!.includes("optionalWithDefault")).toBeTruthy();

  expect(jsonSchema.properties!.optional).toEqual({
    bsonType: "string",
    description: "must be a string",
  });

  expect(jsonSchema.properties!.optionalWithDefault).toEqual({
    bsonType: "number",
    description: "must be a number",
  });
});

test("Union schema", () => {
  const schema = v.object({
    stringOrNumber: v.union([v.string(), v.number()]),
    optionalField: v.union([v.string(), v.undefined()]),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.stringOrNumber).toEqual({
    anyOf: [
      {
        bsonType: "string",
        description: "must be a string",
      },
      {
        bsonType: "number",
        description: "must be a number",
      },
    ],
  });

  expect(jsonSchema.required!.includes("optionalField")).toEqual(false);
});

test("Intersect schema", () => {
  const nameSchema = v.object({ name: v.string() });
  const ageSchema = v.object({ age: v.number() });

  const schema = v.object({
    person: v.intersect([nameSchema, ageSchema]),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.person).toEqual({
    allOf: [
      {
        bsonType: "object",
        properties: {
          name: {
            bsonType: "string",
            description: "must be a string",
          },
        },
        required: ["name"],
      },
      {
        bsonType: "object",
        properties: {
          age: {
            bsonType: "number",
            description: "must be a number",
          },
        },
        required: ["age"],
      },
    ],
  });
});

test("Complex nested schema", () => {
  const schema = v.object({
    user: v.object({
      name: v.string(),
      age: v.pipe(v.number(), v.minValue(0)),
      contact: v.object({
        email: v.pipe(v.string(), v.regex(/^.+@.+\..+$/)),
        phone: v.optional(v.string()),
      }),
      tags: v.array(v.string()),
    }),
    metadata: v.object({
      createdAt: v.date(),
      updatedAt: v.optional(v.date()),
    }),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  // Verifier la structure generale
  expect(jsonSchema.required!.includes("user")).toEqual(true);
  expect(jsonSchema.required!.includes("metadata")).toEqual(true);

  // Verifier les proprietes imbriquees
  const userProps = jsonSchema.properties!.user.properties!;
  expect(userProps.name.bsonType).toEqual("string");
  expect(userProps.age.minimum).toEqual(0);

  // Verifier les imbrications profondes
  const contactProps = userProps.contact.properties!;
  expect(contactProps.email.pattern).toEqual("^.+@.+\\..+$");
  expect(userProps.contact.required!.includes("phone")).toEqual(false);

  // Verifier les tableaux
  expect(userProps.tags.bsonType).toEqual("array");
  expect(userProps.tags.items.bsonType).toEqual("string");

  // Verifier les dates
  const metadataProps = jsonSchema.properties!.metadata.properties!;
  expect(metadataProps.createdAt.bsonType).toEqual("date");
  expect(metadataProps.updatedAt.bsonType).toEqual("date");
});

test("Multiple regex patterns should be combined correctly", () => {
  const schema = v.object({
    username: v.pipe(
      v.string(),
      v.regex(/^[a-z]/),
      v.regex(/[a-z0-9]+$/),
      v.minLength(3),
      v.maxLength(20),
    ),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  const usernameProps = jsonSchema.properties!.username;
  expect(usernameProps.bsonType).toEqual("string");
  expect(usernameProps.minLength).toEqual(3);
  expect(usernameProps.maxLength).toEqual(20);
  expect(usernameProps.pattern!.includes("^[a-z]")).toBeTruthy();
  expect(usernameProps.pattern!.includes("[a-z0-9]+$")).toBeTruthy();
});

test("Any schema type", () => {
  const schema = v.object({
    anyField: v.any(),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.anyField === undefined).toBeTruthy(); // 'any' doesn't add a validator
  expect(jsonSchema.required!.includes("anyField")).toEqual(true); // still a required field
});

test("Literal schema", () => {
  const schema = v.object({
    stringLiteral: v.literal("active"),
    numberLiteral: v.literal(42),
    booleanLiteral: v.literal(true),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.stringLiteral).toEqual({
    bsonType: "string",
    enum: ["active"],
    description: "must be active",
  });

  expect(jsonSchema.properties!.numberLiteral).toEqual({
    bsonType: "number",
    enum: [42],
    description: "must be 42",
  });

  expect(jsonSchema.properties!.booleanLiteral).toEqual({
    bsonType: "bool",
    enum: [true],
    description: "must be true",
  });
});

test("Enum schema", () => {
  enum StringEnum {
    Option1 = "option1",
    Option2 = "option2",
    Option3 = "option3",
  }

  enum NumericEnum {
    One = 1,
    Two = 2,
    Three = 3,
  }

  const schema = v.object({
    stringEnum: v.enum(StringEnum),
    numberEnum: v.enum(NumericEnum),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.stringEnum).toEqual({
    bsonType: "string",
    enum: ["option1", "option2", "option3"],
    description: "must be one of the allowed values",
  });

  expect(jsonSchema.properties!.numberEnum).toEqual({
    bsonType: "number",
    enum: [1, 2, 3],
    description: "must be one of the allowed values",
  });
});

test("Literal with pipes", () => {
  const schema = v.object({
    status: v.pipe(
      v.string(),
      v.literal("active"),
    ),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  // For literal with pipes, the first validation in the pipe (v.string())
  // sets the bsonType, and then literal adds the enum constraint
  expect(jsonSchema.properties!.status).toEqual({
    bsonType: "string",
    description: "must be a string",
    enum: ["active"],
  });
});

test("Record schema - string keys and number values", () => {
  const schema = v.object({
    map: v.record(v.string(), v.number()),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.map).toEqual({
    bsonType: "object",
    description: "must be a record",
    additionalProperties: {
      bsonType: "number",
      description: "must be a number",
    },
  });
});

test("Record schema with key regex", () => {
  const schema = v.object({
    mapRegex: v.record(v.pipe(v.string(), v.regex(/^[a-z]+$/)), v.string()),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  expect(jsonSchema.properties!.mapRegex.patternProperties).toEqual({
    "^[a-z]+$": {
      bsonType: "string",
      description: "must be a string",
    },
  });

  expect(jsonSchema.properties!.mapRegex.additionalProperties).toEqual(false);
});

test("Record validation should accept valid and reject invalid values", () => {
  const schema = v.object({
    map: v.record(v.string(), v.number()),
  });

  // Valid document
  const ok = v.safeParse(schema, { map: { a: 1, b: 2 } });
  expect(ok.success).toBeTruthy();

  // Invalid value for a key
  const nokValue = v.safeParse(schema, { map: { a: "1" } });
  expect(!nokValue.success).toBeTruthy();

  // Invalid overall type
  const nokType = v.safeParse(schema, { map: "not-an-object" });
  expect(!nokType.success).toBeTruthy();
});

test("Record key regex validation should accept and reject keys", () => {
  const schema = v.object({
    mapRegex: v.record(v.pipe(v.string(), v.regex(/^[a-z]+$/)), v.string()),
  });

  const ok = v.safeParse(schema, { mapRegex: { abc: "ok", xyz: "ok" } });
  expect(ok.success).toBeTruthy();

  const nok = v.safeParse(schema, { mapRegex: { Abc: "ok" } });
  expect(!nok.success).toBeTruthy();
});

test("Deep nested record schema: toMongoValidator structure and valibot validation", () => {
  const schema = v.object({
    level1: v.object({
      level2: v.record(v.string(), v.object({ x: v.number() })),
    }),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  // Check generated JSON Schema for deep nested record
  expect(
    jsonSchema.properties!.level1.properties!.level2.additionalProperties,
  ).toEqual({
    bsonType: "object",
    properties: {
      x: {
        bsonType: "number",
        description: "must be a number",
      },
    },
    required: ["x"],
  });

  // Valid document
  const ok = v.safeParse(schema, {
    level1: { level2: { a: { x: 1 }, b: { x: 2 } } },
  });
  expect(ok.success).toBeTruthy();

  // Invalid document: nested value wrong type
  const nok = v.safeParse(schema, { level1: { level2: { a: { x: "no" } } } });
  expect(!nok.success).toBeTruthy();
});

test("Record schema complex cases", () => {
  // Record with enum keys
  const enumSchema = v.object({
    enumRecord: v.record(v.picklist(["a", "b", "c"]), v.number()),
  });

  const enumValidator = toMongoValidator(enumSchema);
  const enumJsonSchema = enumValidator.$jsonSchema!;

  // Since enum doesn't produce a pattern, it should use additionalProperties
  expect(enumJsonSchema.properties!.enumRecord).toEqual({
    bsonType: "object",
    description: "must be a record",
    additionalProperties: {
      bsonType: "number",
      description: "must be a number",
    },
  });

  // Record with complex value schema
  const complexSchema = v.object({
    complexRecord: v.record(
      v.pipe(v.string(), v.regex(/^key_/)),
      v.object({
        name: v.string(),
        count: v.number(),
      }),
    ),
  });

  const complexValidator = toMongoValidator(complexSchema);
  const complexJsonSchema = complexValidator.$jsonSchema!;

  expect(complexJsonSchema.properties!.complexRecord).toEqual({
    bsonType: "object",
    description: "must be a record",
    patternProperties: {
      "^key_": {
        bsonType: "object",
        properties: {
          name: {
            bsonType: "string",
            description: "must be a string",
          },
          count: {
            bsonType: "number",
            description: "must be a number",
          },
        },
        required: ["name", "count"],
      },
    },
    additionalProperties: false,
  });
});

test("Nullable schema", () => {
  const schema = v.object({
    nullableString: v.nullable(v.string()),
    nullableNumber: v.nullable(v.number()),
    nullableObject: v.nullable(v.object({ name: v.string() })),
    nullableStringWithDefault: v.nullable(v.string(), "default"),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  // Test nullable string
  expect(jsonSchema.properties!.nullableString).toEqual({
    anyOf: [
      {
        bsonType: "string",
        description: "must be a string",
      },
      {
        bsonType: "null",
      },
    ],
  });

  // Test nullable number
  expect(jsonSchema.properties!.nullableNumber).toEqual({
    anyOf: [
      {
        bsonType: "number",
        description: "must be a number",
      },
      {
        bsonType: "null",
      },
    ],
  });

  // Test nullable object
  expect(jsonSchema.properties!.nullableObject).toEqual({
    anyOf: [
      {
        bsonType: "object",
        properties: {
          name: {
            bsonType: "string",
            description: "must be a string",
          },
        },
        required: ["name"],
      },
      {
        bsonType: "null",
      },
    ],
  });

  // Test nullable with default (should still be required in MongoDB schema)
  expect(jsonSchema.properties!.nullableStringWithDefault).toEqual({
    anyOf: [
      {
        bsonType: "string",
        description: "must be a string",
      },
      {
        bsonType: "null",
      },
    ],
  });

  // All nullable fields should be required in the schema (MongoDB doesn't handle defaults)
  expect(jsonSchema.required!.includes("nullableString")).toBeTruthy();
  expect(jsonSchema.required!.includes("nullableNumber")).toBeTruthy();
  expect(jsonSchema.required!.includes("nullableObject")).toBeTruthy();
  expect(jsonSchema.required!.includes("nullableStringWithDefault")).toBeTruthy();
});

test("Nullable with validations", () => {
  const schema = v.object({
    nullableEmail: v.nullable(v.pipe(v.string(), v.regex(/^.+@.+\..+$/))),
    nullableAge: v.nullable(v.pipe(v.number(), v.minValue(0), v.maxValue(120))),
    nullableArray: v.nullable(
      v.pipe(v.array(v.string()), v.minLength(1), v.maxLength(5)),
    ),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  // Test nullable string with regex validation
  expect(jsonSchema.properties!.nullableEmail).toEqual({
    anyOf: [
      {
        bsonType: "string",
        description: "must be a string",
        pattern: "^.+@.+\\..+$",
      },
      {
        bsonType: "null",
      },
    ],
  });

  // Test nullable number with range validation
  expect(jsonSchema.properties!.nullableAge).toEqual({
    anyOf: [
      {
        bsonType: "number",
        description: "must be a number",
        minimum: 0,
        maximum: 120,
      },
      {
        bsonType: "null",
      },
    ],
  });

  // Test nullable array with length validation
  expect(jsonSchema.properties!.nullableArray).toEqual({
    anyOf: [
      {
        bsonType: "array",
        items: {
          bsonType: "string",
          description: "must be a string",
        },
        description: "must be an array",
        minItems: 1,
        maxItems: 5,
      },
      {
        bsonType: "null",
      },
    ],
  });
});

test("Nullable validation with Valibot", () => {
  // Test that nullable schemas work correctly with Valibot validation
  const schema = v.object({
    nullableString: v.nullable(v.string()),
    nullableNumber: v.nullable(v.pipe(v.number(), v.minValue(0))),
  });

  // Valid: null values
  const validNull = v.safeParse(schema, {
    nullableString: null,
    nullableNumber: null,
  });
  expect(validNull.success).toBeTruthy();

  // Valid: actual values
  const validValues = v.safeParse(schema, {
    nullableString: "test",
    nullableNumber: 42,
  });
  expect(validValues.success).toBeTruthy();

  // Invalid: wrong types (should fail)
  const invalidType = v.safeParse(schema, {
    nullableString: 123,
    nullableNumber: "not a number",
  });
  expect(!invalidType.success).toBeTruthy();

  // Invalid: number validation fails
  const invalidNumber = v.safeParse(schema, {
    nullableString: "test",
    nullableNumber: -5,
  });
  expect(!invalidNumber.success).toBeTruthy();

  // Valid: mixed null and values
  const mixedValid = v.safeParse(schema, {
    nullableString: "test",
    nullableNumber: null,
  });
  expect(mixedValid.success).toBeTruthy();
});

test("Nested nullable schemas", () => {
  const schema = v.object({
    user: v.nullable(v.object({
      name: v.string(),
      age: v.nullable(v.number()),
    })),
    tags: v.array(v.nullable(v.string())),
  });

  const validator = toMongoValidator(schema);
  const jsonSchema = validator.$jsonSchema!;

  // Test nested nullable object
  expect(jsonSchema.properties!.user).toEqual({
    anyOf: [
      {
        bsonType: "object",
        properties: {
          name: {
            bsonType: "string",
            description: "must be a string",
          },
          age: {
            anyOf: [
              {
                bsonType: "number",
                description: "must be a number",
              },
              {
                bsonType: "null",
              },
            ],
          },
        },
        required: ["name", "age"],
      },
      {
        bsonType: "null",
      },
    ],
  });

  // Test array of nullable items
  expect(jsonSchema.properties!.tags).toEqual({
    bsonType: "array",
    items: {
      anyOf: [
        {
          bsonType: "string",
          description: "must be a string",
        },
        {
          bsonType: "null",
        },
      ],
    },
    description: "must be an array",
  });
});

test("Record validation edge cases", () => {
  // Test that patternProperties validation works
  const schema = v.object({
    data: v.record(v.pipe(v.string(), v.regex(/^[a-z]+$/)), v.number()),
  });

  // Valid: keys match pattern, values are numbers
  const valid1 = v.safeParse(schema, { data: { abc: 1, xyz: 2 } });
  expect(valid1.success).toBeTruthy();

  // Invalid: key doesn't match pattern (contains uppercase)
  const invalid1 = v.safeParse(schema, { data: { Abc: 1 } });
  expect(!invalid1.success).toBeTruthy();

  // Invalid: value is wrong type
  const invalid2 = v.safeParse(schema, { data: { abc: "not-a-number" } });
  expect(!invalid2.success).toBeTruthy();

  // Valid: empty record
  const valid2 = v.safeParse(schema, { data: {} });
  expect(valid2.success).toBeTruthy();
});

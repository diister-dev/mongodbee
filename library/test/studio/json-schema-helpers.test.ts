import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import {
  isRequired,
  jsonSchemaChild,
  jsonSchemaFactDetails,
  jsonSchemaFacts,
  jsonTypeOf,
} from "../../src/studio/ui/lib/json-schema.ts";

const USERS = {
  bsonType: "object",
  required: ["name", "role"],
  properties: {
    name: { bsonType: "string", minLength: 1, maxLength: 80 },
    age: { bsonType: "number", multipleOf: 1, minimum: 0 },
    role: { bsonType: "string", enum: ["admin", "editor", "viewer"] },
    email: { bsonType: "string", pattern: "^.+@.+$" },
    address: {
      bsonType: ["object", "null"],
      required: ["city"],
      properties: { city: { bsonType: "string" } },
    },
    tags: {
      bsonType: "array",
      items: {
        bsonType: "object",
        properties: { label: { bsonType: "string" } },
      },
    },
    when: { anyOf: [{ bsonType: "date" }, { bsonType: "string" }] },
  },
};

test("jsonTypeOf reads bsonType, type unions and anyOf branches", () => {
  assertEquals(jsonTypeOf(USERS.properties.name), "string");
  assertEquals(jsonTypeOf(USERS.properties.address), "object | null");
  assertEquals(jsonTypeOf(USERS.properties.when), "date | string");
  assertEquals(jsonTypeOf(undefined), "");
});

test("jsonSchemaFacts lists the enforced constraints in a stable order", () => {
  assertEquals(jsonSchemaFacts(USERS.properties.name), [
    "minLength 1",
    "maxLength 80",
  ]);
  assertEquals(jsonSchemaFacts(USERS.properties.age), [
    "minimum 0",
    "multipleOf 1",
  ]);
  assertEquals(jsonSchemaFacts(USERS.properties.role), ["enum of 3"]);
  assertEquals(jsonSchemaFacts(USERS.properties.email), ["pattern"]);
});

test("jsonSchemaChild and isRequired follow properties, array items and branches", () => {
  assertEquals(jsonSchemaChild(USERS, "name"), USERS.properties.name);
  assertEquals(jsonSchemaChild(USERS.properties.address, "city"), {
    bsonType: "string",
  });
  assertEquals(jsonSchemaChild(USERS.properties.tags, "label"), {
    bsonType: "string",
  });
  assertEquals(jsonSchemaChild(USERS, "missing"), undefined);
  assertEquals(isRequired(USERS, "name"), true);
  assertEquals(isRequired(USERS, "age"), false);
  assertEquals(isRequired(USERS.properties.address, "city"), true);
  assertEquals(isRequired(USERS, "missing"), undefined);
});

test("jsonSchemaFactDetails carries the pattern and flags keywords MongoDB ignores", () => {
  assertEquals(
    jsonSchemaFactDetails({
      bsonType: "string",
      minLength: 3,
      minItems: 1,
      pattern: "^a+$",
    }),
    [
      { label: "minLength 3" },
      {
        label: "minItems 1",
        inert:
          "MongoDB ignores minItems on a string: it only applies to array values",
      },
      { label: "pattern", detail: "/^a+$/" },
    ],
  );
  assertEquals(
    jsonSchemaFactDetails({ bsonType: ["int", "null"], minimum: 1 }),
    [{ label: "minimum 1" }],
  );
  assertEquals(
    jsonSchemaFactDetails({ bsonType: "string", minimum: 1 })[0].inert,
    "MongoDB ignores minimum on a string: it only applies to numbers",
  );
  assertEquals(jsonSchemaFactDetails({ enum: ["a", 1] }), [
    { label: "enum of 2", detail: '"a", 1' },
  ]);
});

test("an object-valued constraint is named, with its value as the detail", () => {
  const schema = {
    bsonType: "object",
    additionalProperties: { bsonType: "object" },
  };
  assertEquals(jsonSchemaFacts(schema), ["additionalProperties"]);
  assertEquals(jsonSchemaFactDetails(schema), [
    { label: "additionalProperties", detail: '{"bsonType":"object"}' },
  ]);
});

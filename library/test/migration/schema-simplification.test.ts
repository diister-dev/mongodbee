import { assertEquals } from "@std/assert";
import * as v from "../../src/schema.ts";
import { simplifySchema } from "../../src/migration/schema-validation.ts";

Deno.test("Schema simplification - basic types", () => {
  const userSchema = {
    name: v.string(),
    email: v.string(),
    age: v.number(),
    active: v.boolean(),
  };

  const simplified = simplifySchema(userSchema);

  console.log("Simplified schema:", simplified);

  assertEquals(simplified, {
    name: "string",
    email: "string",
    age: "number",
    active: "boolean",
  });
});

Deno.test("Schema simplification - optional fields", () => {
  const schema = {
    name: v.string(),
    email: v.optional(v.string()),
    phone: v.nullable(v.string()),
  };

  const simplified = simplifySchema(schema);

  console.log("Simplified schema:", simplified);

  assertEquals(simplified, {
    name: "string",
    email: "string?",
    phone: "string | null",
  });
});

Deno.test("Schema simplification - nested objects", () => {
  const schema = {
    name: v.string(),
    address: v.object({
      street: v.string(),
      city: v.string(),
      zipcode: v.optional(v.string()),
    }),
  };

  const simplified = simplifySchema(schema);

  console.log("Simplified schema:", simplified);

  assertEquals(simplified, {
    name: "string",
    address: "object",
    "address.street": "string",
    "address.city": "string",
    "address.zipcode": "string?",
  });
});

Deno.test("Schema simplification - arrays", () => {
  const schema = {
    tags: v.array(v.string()),
    scores: v.array(v.number()),
    products: v.array(v.object({
      name: v.string(),
      quantity: v.number(),
    })),
  };

  const simplified = simplifySchema(schema);

  console.log("Simplified schema:", simplified);

  assertEquals(simplified, {
    tags: "string[]",
    scores: "number[]",
    products: "object[]",
  });
});

Deno.test("Schema simplification - picklist/union", () => {
  const schema = {
    status: v.picklist(["active", "inactive", "pending"]),
    data: v.union([v.string(), v.number()]),
  };

  const simplified = simplifySchema(schema);

  console.log("Simplified schema:", simplified);

  assertEquals(simplified, {
    status: '"active" | "inactive" | "pending"',
    data: "string | number",
  });
});

Deno.test("Schema comparison - detect field additions", () => {
  const migrationSchema = {
    name: v.string(),
    email: v.string(),
  };

  const projectSchema = {
    name: v.string(),
    email: v.string(),
    phone: v.string(), // Added field
  };

  const simpleMig = simplifySchema(migrationSchema);
  const simpleProj = simplifySchema(projectSchema);

  const added = Object.keys(simpleProj).filter((k) => !(k in simpleMig));

  console.log("Added fields:", added);

  assertEquals(added, ["phone"]);
});

Deno.test("Schema comparison - detect field removals", () => {
  const migrationSchema = {
    name: v.string(),
    email: v.string(),
    phone: v.string(),
  };

  const projectSchema = {
    name: v.string(),
    email: v.string(),
    // phone removed
  };

  const simpleMig = simplifySchema(migrationSchema);
  const simpleProj = simplifySchema(projectSchema);

  const removed = Object.keys(simpleMig).filter((k) => !(k in simpleProj));

  console.log("Removed fields:", removed);

  assertEquals(removed, ["phone"]);
});

Deno.test("Schema comparison - detect type changes", () => {
  const migrationSchema = {
    name: v.string(),
    age: v.string(), // was string
  };

  const projectSchema = {
    name: v.string(),
    age: v.number(), // changed to number
  };

  const simpleMig = simplifySchema(migrationSchema);
  const simpleProj = simplifySchema(projectSchema);

  const modified: string[] = [];
  for (const key of Object.keys(simpleMig)) {
    if (key in simpleProj && simpleMig[key] !== simpleProj[key]) {
      modified.push(`${key}: ${simpleMig[key]} → ${simpleProj[key]}`);
    }
  }

  console.log("Modified fields:", modified);

  assertEquals(modified, ["age: string → number"]);
});

Deno.test("Schema comparison - complex nested changes", () => {
  const migrationSchema = {
    user: v.object({
      name: v.string(),
      settings: v.object({
        theme: v.string(),
        language: v.string(),
      }),
    }),
  };

  const projectSchema = {
    user: v.object({
      name: v.string(),
      email: v.string(), // Added
      settings: v.object({
        theme: v.string(),
        // language removed
        timezone: v.string(), // Added
      }),
    }),
  };

  const simpleMig = simplifySchema(migrationSchema);
  const simpleProj = simplifySchema(projectSchema);

  console.log("Migration simplified:", simpleMig);
  console.log("Project simplified:", simpleProj);

  const added = Object.keys(simpleProj).filter((k) => !(k in simpleMig)).sort();
  const removed = Object.keys(simpleMig).filter((k) => !(k in simpleProj)).sort();

  console.log("Added:", added);
  console.log("Removed:", removed);

  assertEquals(added, ["user.email", "user.settings.timezone"]);
  assertEquals(removed, ["user.settings.language"]);
});

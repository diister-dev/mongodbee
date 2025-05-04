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

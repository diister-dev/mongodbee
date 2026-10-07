import type * as v from "../schema.ts";
import { toMongoValidator } from "../validator.ts";

export interface MongoValidator {
  $jsonSchema: Record<string, unknown>;
}

export function validatorOf(
  schema: v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>,
): MongoValidator {
  return toMongoValidator(schema);
}

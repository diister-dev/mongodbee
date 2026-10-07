import type * as v from "../schema.ts";

export function isRecord(
  value: unknown,
): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isSchema(
  value: unknown,
): value is v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>> {
  return (
    isRecord(value) && value.kind === "schema" && typeof value.type === "string"
  );
}

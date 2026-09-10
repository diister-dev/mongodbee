import * as v from "./schema.ts";
import { decodeTime, ulid } from "./utils/ulid.ts";

/**
 * The ULID primitives this module builds on.
 *
 * Re-exported because a consumer that already depends on MongoDBee for
 * `newId`/`dbId`/`refId` should not have to add a second ULID implementation
 * to its process just to mint a token or read a timestamp back.
 *
 * `ulid()` returns the canonical uppercase form; `newId()` lowercases it.
 */
export { decodeTime, ulid };

/**
 * Generates a new unique ID using ULID
 *
 * @returns A new ULID string in lowercase
 */
export function newId(): string {
  return ulid().toLowerCase();
}

/**
 * Creates an optional ID field for a document type with automatic generation
 *
 * @param type - The document type identifier to use in the ID prefix
 * @returns A Valibot schema for an ID field with optional auto-generation
 */
export function dbId(
  type: string,
): v.OptionalSchema<
  v.SchemaWithPipe<
    readonly [v.StringSchema<undefined>, v.RegexAction<string, undefined>]
  >,
  () => string
> {
  return v.optional(refId(type), () => `${type}:${newId()}`);
}

/**
 * Creates a reference ID field that must match a specific type prefix
 *
 * @param type - The document type identifier that must prefix the ID
 * @returns A Valibot schema for validating reference IDs
 */
export function refId(
  type: string,
): v.SchemaWithPipe<
  readonly [v.StringSchema<undefined>, v.RegexAction<string, undefined>]
> {
  return v.pipe(v.string(), v.regex(new RegExp(`^${type}:[a-zA-Z0-9]+`)));
}

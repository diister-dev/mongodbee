import * as v from "./schema.ts";

type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

/**
 * A document failed its schema: on write, the value the caller passed; on
 * read, the stored document (carried as `result`). `errors` is Valibot's
 * safe-parse result, `message` stays "Validation error" so existing handlers
 * keep matching it.
 */
export class DocumentValidationError extends Error {
  override readonly name = "DocumentValidationError";
  readonly errors: v.SafeParseResult<AnySchema>;
  readonly result?: unknown;

  constructor(errors: v.SafeParseResult<AnySchema>, result?: unknown) {
    super("Validation error");
    this.errors = errors;
    if (result !== undefined) this.result = result;
  }
}

/** Parses a document read from the database; a mismatch is a DocumentValidationError. */
export function parseStored<S extends AnySchema>(
  schema: S,
  stored: unknown,
): v.InferOutput<S> {
  const parsed = v.safeParse(schema, stored);
  if (!parsed.success) throw new DocumentValidationError(parsed, stored);
  return parsed.output;
}

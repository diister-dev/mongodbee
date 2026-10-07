import { test } from "./+harness.ts";
import { assert, assertEquals, assertThrows } from "./+assert.ts";
import * as v from "../src/schema.ts";
import { unique } from "../src/indexes.ts";
import {
  defineType,
  fieldsOf,
  indexesOf,
  isTypeDefinition,
  withFields,
} from "../src/type-definition.ts";

// A migration that tightens ONE field has to restate the parent's schema with
// that entry changed. Doing it by spread is the trap `withFields` closes: a
// TypeDefinition spreads to its own properties, so the result would declare
// fields literally named `schema` and `indexes` and lose everything real.

const Session = defineType({
  schema: v.object({
    userId: v.string(),
    token: v.string(),
    revoked: v.optional(v.boolean()),
  }),
  indexes: (f) => [unique(f.token)],
});

const PlainJobs = {
  type: v.string(),
  content: v.union([v.object({ kind: v.literal("a") })]),
};

test("withFields keeps a plain field map a plain field map", () => {
  const next = withFields(PlainJobs, {
    content: v.union([v.object({ kind: v.literal("b") })]),
  });

  assertEquals(isTypeDefinition(next), false);
  assertEquals(Object.keys(fieldsOf(next)).sort(), ["content", "type"]);
});

test("withFields keeps a defineType a defineType, indexes included", () => {
  const next = withFields(Session, { revoked: v.boolean() });

  assert(isTypeDefinition(next));
  assertEquals(Object.keys(fieldsOf(next)).sort(), [
    "revoked",
    "token",
    "userId",
  ]);
  // The point of the helper: the declared index survives the rewrite.
  assertEquals(indexesOf(next).length, 1);
  assertEquals(indexesOf(next), indexesOf(Session));
});

test("withFields adds a field as readily as it replaces one", () => {
  const next = withFields(Session, { after: v.optional(v.string()) });
  assertEquals(Object.keys(fieldsOf(next)).sort(), [
    "after",
    "revoked",
    "token",
    "userId",
  ]);
  assertEquals(indexesOf(next).length, 1);
});

// What the naive spread produces, asserted so the difference is on the record
// rather than in a comment.
test("spreading a defineType loses its fields and its indexes", () => {
  const spread = { ...Session, revoked: v.boolean() } as Record<
    string,
    unknown
  >;

  assertEquals(isTypeDefinition(spread), false);
  assert("schema" in spread);
  assert("indexes" in spread);
  assertEquals("userId" in spread, false);
});

// `withFields` only adds or replaces, never removes, so an index can never be
// orphaned by it. Pinned because that is WHY it is safe, not a coincidence.
test("withFields never orphans an index: it cannot remove a field", () => {
  const next = withFields(Session, {
    token: v.pipe(v.string(), v.minLength(8)),
  });
  const indexed = Object.keys(indexesOf(next)[0].key)[0];
  assert(indexed in fieldsOf(next));
});

// The rebuild goes through `defineType`, the canonical constructor, so an index
// pointing nowhere is refused the same way a hand-written type would refuse it.
test("defineType still refuses an index pointing at a missing field", () => {
  const dropped = { ...fieldsOf(Session) } as Record<string, unknown>;
  delete dropped.token;
  assertThrows(
    () =>
      defineType({
        schema: v.object(dropped as never),
        indexes: indexesOf(Session),
      }),
    Error,
    'key path "token" does not exist in the schema',
  );
});

import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { validateLastMigrationMatchesProjectSchema } from "../../src/migration/schema-validation.ts";
import type { SchemasDefinition } from "../../src/migration/types.ts";
import * as v from "../../src/schema.ts";

function gate(snapshot: SchemasDefinition, project: SchemasDefinition) {
  return validateLastMigrationMatchesProjectSchema(
    migrationDefinition("001", "snapshot", {
      parent: null,
      schemas: snapshot,
      migrate: (b) => b.compile(),
    }),
    project,
  );
}

function field(schema: v.GenericSchema): SchemasDefinition {
  return { collections: { users: { _id: v.string(), value: schema } } };
}

function nested(schema: v.GenericSchema): SchemasDefinition {
  return field(v.object({ inner: schema }));
}

function assertSeen(snapshot: SchemasDefinition, project: SchemasDefinition) {
  const result = gate(snapshot, project);
  assertEquals(result.valid, false, "the change passed the gate unseen");
}

function assertRefused(project: SchemasDefinition, ...words: string[]) {
  const result = gate(project, project);
  assertEquals(result.valid, false, "the uncomparable schema was accepted");
  const text = result.errors.join("\n");
  for (const word of words) {
    assert(text.includes(word), `expected "${word}" in: ${text}`);
  }
}

function assertSame(snapshot: SchemasDefinition, project: SchemasDefinition) {
  const result = gate(snapshot, project);
  assertEquals(result.valid, true, result.errors.join("\n"));
}

test("a changed v.regex is seen, at the top level and nested", () => {
  const before = v.pipe(v.string(), v.regex(/^[a-z]+$/));
  const after = v.pipe(v.string(), v.regex(/^[a-z0-9]+$/));
  assertSeen(field(before), field(after));
  assertSeen(nested(before), nested(after));
  assertSeen(
    field(v.pipe(v.string(), v.regex(/^a$/))),
    field(v.pipe(v.string(), v.regex(/^a$/i))),
  );
  assertSame(field(before), field(v.pipe(v.string(), v.regex(/^[a-z]+$/))));
});

test("a v.check is compared by its message, and one without a message is refused", () => {
  const isEven = (n: number) => n % 2 === 0;
  const passthrough = (_n: number) => true;
  assertSeen(
    nested(v.pipe(v.number(), v.check(isEven, "must be even"))),
    nested(v.pipe(v.number(), v.check(isEven, "must be odd"))),
  );
  assertSame(
    field(v.pipe(v.number(), v.check(passthrough, "must be even"))),
    field(v.pipe(v.number(), v.check(isEven, "must be even"))),
  );
  assertRefused(
    nested(v.pipe(v.number(), v.check(isEven))),
    "users.value",
    "v.check",
    "message",
  );
});

test("a v.custom is compared by its message, and one without a message is refused", () => {
  const isText = (input: unknown) => typeof input === "string";
  assertSeen(
    field(v.custom<string>(isText, "must be text")),
    field(v.custom<string>(isText, "must be a code")),
  );
  assertRefused(field(v.custom<string>(isText)), "v.custom", "message");
});

test("a v.rawCheck cannot carry a message and is refused", () => {
  assertRefused(
    field(
      v.pipe(
        v.string(),
        v.rawCheck(() => {}),
      ),
    ),
    "v.rawCheck",
  );
});

test("a changed default value is seen, and a function default is compared by its name", () => {
  assertSeen(
    field(v.optional(v.string(), "draft")),
    field(v.optional(v.string(), "published")),
  );
  assertSeen(
    nested(v.optional(v.array(v.string()), [])),
    nested(v.optional(v.array(v.string()), ["a"])),
  );
  assertSeen(
    field(v.nullish(v.date(), new Date(0))),
    field(v.nullish(v.date(), new Date(1))),
  );
  assertSeen(
    field(v.fallback(v.string(), "a")),
    field(v.fallback(v.string(), "b")),
  );
  assertSame(
    nested(v.optional(v.object({ a: v.number() }), { a: 1, b: [2] })),
    nested(v.optional(v.object({ a: v.number() }), { b: [2], a: 1 })),
  );

  const now = () => new Date();
  const epoch = () => new Date(0);
  assertSeen(
    field(v.optional(v.date(), now)),
    field(v.optional(v.date(), epoch)),
  );
  assertRefused(
    nested(v.optional(v.date(), () => new Date())),
    "users.value",
    "anonymous function",
  );
});

test("the inside of a v.lazy is compared, recursive schemas included", () => {
  assertSeen(field(v.lazy(() => v.string())), field(v.lazy(() => v.number())));

  const tree = (leaf: v.GenericSchema) => {
    const node: v.GenericSchema = v.lazy(() =>
      v.object({ leaf, children: v.array(v.lazy(() => node)) }),
    );
    return node;
  };
  assertSame(field(tree(v.string())), field(tree(v.string())));
  assertSeen(field(tree(v.string())), field(tree(v.number())));

  const pair = (inner: v.GenericSchema) => {
    const a: v.GenericSchema = v.lazy(() =>
      v.object({ a: v.string(), next: b }),
    );
    const b: v.GenericSchema = v.lazy(() => v.object({ b: inner, next: a }));
    return a;
  };
  assertSeen(field(pair(v.string())), field(pair(v.number())));

  const freshEachTime = (): v.GenericSchema =>
    v.object({ children: v.array(v.lazy(freshEachTime)) });
  assertSame(field(freshEachTime()), field(freshEachTime()));
});

test("a v.lazy whose getter needs its input is refused", () => {
  assertRefused(
    field(
      v.lazy((input: any) => (input.kind === "a" ? v.string() : v.number())),
    ),
    "v.lazy",
  );
});

test("the pipe of a v.union is compared", () => {
  const isSet = (input: string | number) => input !== "";
  const union = v.union([v.string(), v.number()]);
  assertSeen(
    nested(v.pipe(union, v.check(isSet, "a"))),
    nested(v.pipe(union, v.check(isSet, "b"))),
  );
});

test("a v.instance is compared by its class", () => {
  assertSeen(field(v.instance(Date)), field(v.instance(Map)));
});

for (const name of ["default", "message", "expects", "async"]) {
  test(`a collection and a field named "${name}" are compared like any other`, () => {
    assertSeen(
      { collections: { [name]: { _id: v.string() } } },
      { collections: { [name]: { _id: v.number() } } },
    );
    assertSeen(
      { collections: { users: { _id: v.string(), [name]: v.string() } } },
      { collections: { users: { _id: v.string(), [name]: v.number() } } },
    );
    assertSeen(
      { multiCollections: { log: { [name]: { _id: v.string() } } } },
      { multiCollections: { log: { [name]: { _id: v.number() } } } },
    );
  });
}

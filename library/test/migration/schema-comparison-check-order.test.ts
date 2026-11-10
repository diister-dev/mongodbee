import { test } from "../+harness.ts";
import { assert, assertEquals, assertThrows } from "../+assert.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import {
  createSchemaComparer,
  simplifySchema,
  validateLastMigrationMatchesProjectSchema,
} from "../../src/migration/schema-validation.ts";
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

function assertSame(snapshot: SchemasDefinition, project: SchemasDefinition) {
  const result = gate(snapshot, project);
  assertEquals(result.valid, true, result.errors.join("\n"));
}

const ANSI_COLOUR = new RegExp(`${String.fromCharCode(27)}\\[\\d+m`, "g");

function differences(
  snapshot: SchemasDefinition,
  project: SchemasDefinition,
): string[] {
  const result = gate(snapshot, project);
  assertEquals(result.valid, false, "the change passed the gate unseen");
  return result.errors
    .join("\n")
    .replace(ANSI_COLOUR, "")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => /^[-+~] /.test(line));
}

function permutations<T>(items: T[]): T[][] {
  if (items.length <= 1) return [items];
  return items.flatMap((item, index) =>
    permutations([...items.slice(0, index), ...items.slice(index + 1)]).map(
      (rest) => [item, ...rest],
    ),
  );
}

const isEven = (n: number) => n % 2 === 0;
const notThirteen = (n: number) => n !== 13;

type Check<T> = v.PipeItem<T, T, v.BaseIssue<unknown>>;

function numberChecks(): Check<number>[] {
  return [
    v.check(isEven, "must be even"),
    v.minValue(0),
    v.maxValue(100),
    v.check(notThirteen, "must not be thirteen"),
  ];
}

test("valibot accepts the same values whatever the order of the checks between two transforms, early aborts included", () => {
  const inputs = [-2, 0, 1, 13, 14, 50, 99, 100, 102, "4", null];
  const configs = [{}, { abortPipeEarly: true }, { abortEarly: true }];
  const reference = v.pipe(v.number(), ...numberChecks());
  for (const order of permutations(numberChecks())) {
    const schema = v.pipe(v.number(), ...order);
    for (const config of configs) {
      for (const input of inputs) {
        assertEquals(
          v.safeParse(schema, input, config).success,
          v.safeParse(reference, input, config).success,
          `${JSON.stringify(input)} under ${JSON.stringify(config)}`,
        );
      }
    }
  }
});

test("with abortPipeEarly a check that throws on what an earlier check rejects throws or is skipped by order, and the value is rejected either way", () => {
  const isObject = (input: unknown) =>
    typeof input === "object" && input !== null;
  const hasName = (input: any) => typeof input.name === "string";
  const guarded = v.pipe(
    v.unknown(),
    v.check(isObject, "must be an object"),
    v.check(hasName, "must have a name"),
  );
  const unguarded = v.pipe(
    v.unknown(),
    v.check(hasName, "must have a name"),
    v.check(isObject, "must be an object"),
  );
  const config = { abortPipeEarly: true };
  assertEquals(v.safeParse(guarded, null, config).success, false);
  assertThrows(() => v.safeParse(unguarded, null, config));
  assertThrows(() => v.safeParse(guarded, null));
  assertEquals(v.safeParse(guarded, { name: "a" }, config).success, true);
  assertEquals(v.safeParse(unguarded, { name: "a" }, config).success, true);
});

test("a run of checks is simplified to one entry per check, sorted by its label, a repeated check counted", () => {
  const simplified = simplifySchema(
    v.pipe(
      v.string(),
      v.regex(/^[a-z]+$/),
      v.minLength(3),
      v.check((s: string) => s !== "admin", "must not be admin"),
      v.minLength(3),
      v.trim(),
      v.maxLength(9),
    ),
  );
  assertEquals(
    simplified.pipe.map((item: any) =>
      item.kind === undefined ? item : item.type,
    ),
    [
      "string",
      {
        'check("must not be admin")': 1,
        "min_length(3)": 2,
        'regex("/^[a-z]+$/")': 1,
      },
      "trim",
      { "max_length(9)": 1 },
    ],
  );
  assertEquals(Object.keys(simplified.pipe[1]), [
    'check("must not be admin")',
    "min_length(3)",
    'regex("/^[a-z]+$/")',
  ]);
});

test("the same checks in another order are the same schema", () => {
  for (const order of permutations(numberChecks())) {
    assertSame(
      field(v.pipe(v.number(), ...numberChecks())),
      field(v.pipe(v.number(), ...order)),
    );
  }
  assertSame(
    field(v.pipe(v.string(), v.regex(/^a/), v.regex(/b$/), v.email())),
    field(v.pipe(v.string(), v.email(), v.regex(/b$/), v.regex(/^a/))),
  );
  assertSame(
    field(v.pipe(v.number(), v.integer(), v.minValue(1))),
    field(v.pipe(v.number(), v.minValue(1), v.integer())),
  );
});

test("a check added, removed or changed in a run is seen, and the difference names the check", () => {
  const base = v.pipe(
    v.number(),
    v.check(isEven, "must be even"),
    v.minValue(0),
  );
  assertEquals(
    differences(
      field(base),
      field(
        v.pipe(
          v.number(),
          v.minValue(0),
          v.check(notThirteen, "must not be thirteen"),
          v.check(isEven, "must be even"),
        ),
      ),
    ),
    ['+ value.pipe[1].check("must not be thirteen") = 1'],
  );
  assertEquals(
    differences(field(base), field(v.pipe(v.number(), v.minValue(0)))),
    ['- value.pipe[1].check("must be even") = 1'],
  );
  assertEquals(
    differences(
      field(base),
      field(v.pipe(v.number(), v.minValue(0), v.check(isEven, "must be odd"))),
    ),
    [
      '- value.pipe[1].check("must be even") = 1',
      '+ value.pipe[1].check("must be odd") = 1',
    ],
  );
  assertEquals(
    differences(
      field(base),
      field(v.pipe(v.number(), v.minValue(1), v.check(isEven, "must be even"))),
    ),
    ["- value.pipe[1].min_value(0) = 1", "+ value.pipe[1].min_value(1) = 1"],
  );
  assertEquals(
    differences(
      field(base),
      field(
        v.pipe(
          v.number(),
          v.check(isEven, "must be even"),
          v.minValue(0),
          v.check(isEven, "must be even"),
        ),
      ),
    ),
    ['~ value.pipe[1].check("must be even")', "- 1", "+ 2"],
  );
});

test("a check moved across a transformation is a difference", () => {
  const before = v.pipe(
    v.string(),
    v.regex(/^[a-z]+$/),
    v.trim(),
    v.minLength(1),
  );
  for (const after of [
    v.pipe(v.string(), v.trim(), v.regex(/^[a-z]+$/), v.minLength(1)),
    v.pipe(v.string(), v.regex(/^[a-z]+$/), v.minLength(1), v.trim()),
    v.pipe(
      v.string(),
      v.regex(/^[a-z]+$/),
      v.transform((s: string) => s.trim()),
      v.minLength(1),
    ),
    v.pipe(v.string(), v.regex(/^[a-z]+$/), v.toLowerCase(), v.minLength(1)),
  ]) {
    differences(field(before), field(after));
  }
});

test("the schema at the head of the pipe is still compared", () => {
  differences(
    field(v.pipe(v.string(), v.minLength(1), v.maxLength(9))),
    field(v.pipe(v.array(v.string()), v.maxLength(9), v.minLength(1))),
  );
});

test("reordered checks are the same schema in object entries, array items, lazy schemas and async pipes", () => {
  const checks = (): Check<string>[] => [
    v.minLength(1),
    v.maxLength(9),
    v.regex(/^\w+$/),
  ];
  const nestings: Array<(pipe: v.GenericSchema) => v.GenericSchema> = [
    (pipe) => v.object({ name: pipe }),
    (pipe) => v.array(pipe),
    (pipe) => v.object({ tags: v.array(v.object({ label: pipe })) }),
    (pipe) => v.lazy(() => pipe),
    (pipe) => v.optional(v.nullable(pipe)),
  ];
  for (const nest of nestings) {
    for (const order of permutations(checks())) {
      assertSame(
        field(nest(v.pipe(v.string(), ...checks()))),
        field(nest(v.pipe(v.string(), ...order))),
      );
    }
    differences(
      field(nest(v.pipe(v.string(), ...checks()))),
      field(
        nest(
          v.pipe(v.string(), v.minLength(2), v.maxLength(9), v.regex(/^\w+$/)),
        ),
      ),
    );
  }
  const isFree = async (name: string) => name !== "taken";
  assertEquals(
    createSchemaComparer().equal(
      v.pipeAsync(
        v.string(),
        v.checkAsync(isFree, "must be free"),
        v.minLength(1),
      ),
      v.pipeAsync(
        v.string(),
        v.minLength(1),
        v.checkAsync(isFree, "must be free"),
      ),
    ),
    true,
  );
});

test("a definition with many named checks compares the same reordered, and one changed check is one line each way", () => {
  const names = Array.from({ length: 21 }, (_, i) => `rule ${i + 1} holds`);
  const definition = (messages: string[]) => ({
    collections: {
      doctype_version: {
        _id: v.string(),
        definition: v.pipe(
          v.object({ fields: v.array(v.string()) }),
          ...messages.map((message) =>
            v.check((d: { fields: string[] }) => d.fields.length >= 0, message),
          ),
        ),
      },
    },
  });
  const shuffled = [...names].reverse();
  [shuffled[3], shuffled[17]] = [shuffled[17], shuffled[3]];
  assertSame(definition(names), definition(shuffled));

  const changed = shuffled.map((name) =>
    name === "rule 13 holds" ? "rule 13 holds, strictly" : name,
  );
  const lines = differences(definition(names), definition(changed));
  assertEquals(lines, [
    '- definition.pipe[1].check("rule 13 holds") = 1',
    '+ definition.pipe[1].check("rule 13 holds, strictly") = 1',
  ]);
});

test("a check without a message in a run is still refused, with the path of its run", () => {
  const project = field(
    v.pipe(v.number(), v.minValue(0), v.check(isEven), v.maxValue(9)),
  );
  const result = gate(project, project);
  assertEquals(result.valid, false);
  const text = result.errors.join("\n");
  assert(text.includes("value.pipe[1]"), text);
  assert(text.includes("v.check has no message"), text);
});

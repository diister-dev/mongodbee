import * as v from "../src/schema.ts";
import { toMongoValidator } from "../src/validator.ts";
import { assertEquals } from "./+assert.ts";
import { test } from "./+harness.ts";
import {
  compareAcceptance,
  type FieldSchema,
  type PipeAction,
  permutations,
  piped,
} from "./+mongo-acceptance.ts";
import { withDatabase } from "./+shared.ts";

interface Labeled {
  label: string;
  action: PipeAction;
}

function pipeOf(schema: FieldSchema, actions: readonly Labeled[]): FieldSchema {
  return piped(schema, ...actions.map(({ action }) => action));
}

const labeled = (label: string, action: PipeAction): Labeled => ({
  label,
  action,
});

const NUMBER_BOUNDS = [
  labeled("minValue(5)", v.minValue(5)),
  labeled("minValue(10)", v.minValue(10)),
  labeled("gtValue(5)", v.gtValue(5)),
  labeled("gtValue(10)", v.gtValue(10)),
  labeled("maxValue(5)", v.maxValue(5)),
  labeled("maxValue(10)", v.maxValue(10)),
  labeled("ltValue(5)", v.ltValue(5)),
  labeled("ltValue(10)", v.ltValue(10)),
];
const NUMBER_PROBES = [0, 4.5, 5, 5.5, 9.5, 10, 10.5, 20];

const LENGTH_BOUNDS = [
  labeled("minLength(2)", v.minLength(2)),
  labeled("minLength(3)", v.minLength(3)),
  labeled("maxLength(3)", v.maxLength(3)),
  labeled("maxLength(4)", v.maxLength(4)),
  labeled("length(3)", v.length(3)),
  labeled("nonEmpty()", v.nonEmpty()),
];
const STRING_PROBES = ["", "a", "ab", "abc", "abcd", "abcde"];
const ARRAY_PROBES = [0, 1, 2, 3, 4, 5].map((size) =>
  Array.from({ length: size }, (_, index) => index),
);

function orderedPairs<T>(items: readonly T[]): [T, T][] {
  return items.flatMap((first, i) =>
    items.flatMap((second, j) => (i === j ? [] : [[first, second] as [T, T]])),
  );
}

function describe(actions: readonly Labeled[]): string {
  return actions.map(({ label }) => label).join(", ");
}

async function expectExactAcceptance(
  t: { name: string },
  base: () => FieldSchema,
  combinations: readonly (readonly Labeled[])[],
  probes: readonly unknown[],
) {
  await withDatabase(t.name, async (db) => {
    const disagreements: string[] = [];
    for (const [index, actions] of combinations.entries()) {
      const { stricter, looser } = await compareAcceptance(
        db,
        `c${index}`,
        pipeOf(base(), actions),
        probes,
      );
      for (const verdict of [...stricter, ...looser]) {
        disagreements.push(
          `[${describe(actions)}] ${JSON.stringify(verdict.probe)}: valibot ${verdict.valibot}, mongo ${verdict.mongo}`,
        );
      }
    }
    assertEquals(disagreements, []);
  });
}

test("gtValue then minValue leaves the inclusive minimum, not the exclusive flag of the looser bound", () => {
  const forward = toMongoValidator(
    v.object({ x: v.pipe(v.number(), v.gtValue(5), v.minValue(10)) }),
  ).$jsonSchema.properties!.x;
  const backward = toMongoValidator(
    v.object({ x: v.pipe(v.number(), v.minValue(10), v.gtValue(5)) }),
  ).$jsonSchema.properties!.x;

  assertEquals(forward, {
    minimum: 10,
    bsonType: "number",
    description: "must be a number",
  });
  assertEquals(backward, forward);
});

test("two bounds at the same value keep the exclusive one, in either order", () => {
  for (const actions of permutations([v.minValue(5), v.gtValue(5)])) {
    const x = toMongoValidator(v.object({ x: piped(v.number(), ...actions) }))
      .$jsonSchema.properties!.x;
    assertEquals(x.minimum, 5);
    assertEquals(x.exclusiveMinimum, true);
  }
  for (const actions of permutations([v.maxValue(5), v.ltValue(5)])) {
    const x = toMongoValidator(v.object({ x: piped(v.number(), ...actions) }))
      .$jsonSchema.properties!.x;
    assertEquals(x.maximum, 5);
    assertEquals(x.exclusiveMaximum, true);
  }
});

test("the order of bound actions never changes the generated validator", () => {
  for (const [bounds, base] of [
    [NUMBER_BOUNDS, () => v.number()],
    [LENGTH_BOUNDS, () => v.string()],
    [LENGTH_BOUNDS, () => v.array(v.number())],
  ] as const) {
    for (const [first, second] of orderedPairs(bounds)) {
      assertEquals(
        toMongoValidator(v.object({ x: pipeOf(base(), [first, second]) })),
        toMongoValidator(v.object({ x: pipeOf(base(), [second, first]) })),
        describe([first, second]),
      );
    }
  }
});

test("a minValue on a string is left to valibot, since $jsonSchema bounds are numeric", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { stricter } = await compareAcceptance(
      db,
      "letters",
      v.pipe(v.string(), v.minValue("b")),
      ["a", "b", "c"],
    );
    assertEquals(stricter, []);
  });
});

test({
  name: "MongoDB accepts exactly what valibot accepts for every pair of number bounds, in both orders",
  timeout: 120_000,
  fn: (t) =>
    expectExactAcceptance(
      t,
      () => v.number(),
      [
        ...NUMBER_BOUNDS.map((bound) => [bound]),
        ...orderedPairs(NUMBER_BOUNDS),
      ],
      NUMBER_PROBES,
    ),
});

test({
  name: "MongoDB accepts exactly what valibot accepts for four number bounds in every order",
  timeout: 120_000,
  fn: (t) =>
    expectExactAcceptance(
      t,
      () => v.number(),
      permutations([
        labeled("gtValue(5)", v.gtValue(5)),
        labeled("minValue(10)", v.minValue(10)),
        labeled("ltValue(20)", v.ltValue(20)),
        labeled("maxValue(15)", v.maxValue(15)),
      ]),
      [4.5, 5, 5.5, 9.5, 10, 10.5, 14.5, 15, 15.5, 19.5, 20, 20.5],
    ),
});

test({
  name: "MongoDB accepts exactly what valibot accepts for an integer between combined bounds",
  timeout: 60_000,
  fn: (t) =>
    expectExactAcceptance(
      t,
      () => v.number(),
      permutations([
        labeled("integer()", v.integer()),
        labeled("gtValue(5)", v.gtValue(5)),
        labeled("minValue(5)", v.minValue(5)),
        labeled("ltValue(8)", v.ltValue(8)),
      ]),
      [4, 5, 5.5, 6, 7, 7.5, 8, 9],
    ),
});

test({
  name: "MongoDB accepts exactly what valibot accepts for every pair of string length bounds, in both orders",
  timeout: 120_000,
  fn: (t) =>
    expectExactAcceptance(
      t,
      () => v.string(),
      [
        ...LENGTH_BOUNDS.map((bound) => [bound]),
        ...orderedPairs(LENGTH_BOUNDS),
      ],
      STRING_PROBES,
    ),
});

test({
  name: "MongoDB accepts exactly what valibot accepts for every pair of array length bounds, in both orders",
  timeout: 120_000,
  fn: (t) =>
    expectExactAcceptance(
      t,
      () => v.array(v.number()),
      [
        ...LENGTH_BOUNDS.map((bound) => [bound]),
        ...orderedPairs(LENGTH_BOUNDS),
      ],
      ARRAY_PROBES,
    ),
});

const ASTRAL_PROBES = ["😀", "a😀", "😀😀", "a😀😀", "😀😀😀"];

test({
  name: "MongoDB never rejects a string valibot accepts when it holds characters outside the BMP",
  timeout: 120_000,
  fn: (t) =>
    withDatabase(t.name, async (db) => {
      const rejected: string[] = [];
      const combinations = [
        ...LENGTH_BOUNDS.map((bound) => [bound]),
        ...orderedPairs(LENGTH_BOUNDS),
      ];
      for (const [index, actions] of combinations.entries()) {
        const { stricter } = await compareAcceptance(
          db,
          `c${index}`,
          pipeOf(v.string(), actions),
          ASTRAL_PROBES,
        );
        for (const verdict of stricter) {
          rejected.push(`[${describe(actions)}] ${verdict.probe}`);
        }
      }
      assertEquals(rejected, []);
    }),
});

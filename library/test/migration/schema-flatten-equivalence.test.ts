/**
 * The single-pass `flattenSchema` and the fingerprint comparison must give
 * exactly what the previous flatten-and-diff gave on the same simplified
 * schemas. Random schemas, built from a seeded generator so a failure
 * replays, mix Valibot schemas with the awkward shapes the flattening has
 * rules for: dotted and bracketed keys, `~standard`, keys the diff skips,
 * index and privacy metadata, type definitions, shared sub-schemas, holes,
 * NaN, functions and bigints. The previous implementation is kept verbatim
 * in `fixtures/schema-flatten-reference.ts`; its simplification is also the
 * oracle for the verdict on collections that use none of the features the
 * comparison has since learnt to see.
 */
import { deepStrictEqual } from "node:assert/strict";
import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { withIndex } from "../../src/indexes.ts";
import { PRIVACY_SYMBOL } from "../../src/privacy/metadata.ts";
import { defineType } from "../../src/type-definition.ts";
import {
  createSchemaComparer,
  diffSchemas,
  flattenSchema,
  simplifySchema,
} from "../../src/migration/schema-validation.ts";
import {
  referenceFlattenSchema,
  referenceSimplifySchema,
} from "../fixtures/schema-flatten-reference.ts";

const CASES = 200;
const MAX_DEPTH = 5;

const KEYS = [
  "name",
  "items",
  "a",
  "b",
  "a.b",
  "x[0]",
  "items[1]",
  "~standard",
  "a.~standard",
  "~standardish",
  "kind",
  "type",
  "message",
  "default",
  "expects",
  "@index",
  "@privacy",
  "@privacy.role",
  "entries",
  "pipe",
  "0",
];

type Random = () => number;

function seeded(seed: number): Random {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * The same draws as `random`, except that draw number `at` is shifted: one
 * decision of the generator goes another way, the rest of the build follows.
 */
function perturbed(random: Random, at: number): Random {
  let draws = 0;
  return () => {
    const value = random();
    return draws++ === at ? (value + 0.5) % 1 : value;
  };
}

class Generator {
  private readonly shared: unknown[] = [];

  private readonly random: Random;
  private readonly shuffle: Random | null;

  constructor(random: Random, shuffle: Random | null) {
    this.random = random;
    this.shuffle = shuffle;
  }

  private pick<T>(values: readonly T[]): T {
    return values[Math.floor(this.random() * values.length)];
  }

  private order<T>(values: T[]): T[] {
    if (this.shuffle === null) return values;
    const shuffled = [...values];
    for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(this.shuffle() * (i + 1));
      [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    return shuffled;
  }

  private keys(): string[] {
    const count = 1 + Math.floor(this.random() * 4);
    const keys = new Set<string>();
    for (let i = 0; i < count; i++) keys.add(this.pick(KEYS));
    return this.order([...keys]);
  }

  leafValue(): unknown {
    return this.pick<() => unknown>([
      () => "text",
      () => "other",
      () => 0,
      () => 1,
      () => Number.NaN,
      () => null,
      () => undefined,
      () => true,
      () => () => 1,
      () => (this.random() < 0.1 ? 10n : 10),
    ])();
  }

  valibot(depth: number): any {
    if (depth >= MAX_DEPTH) {
      return this.pick([
        () => v.string(),
        () => v.number(),
        () => v.pipe(v.string(), v.maxLength(this.random() < 0.5 ? 10 : 20)),
        () =>
          v.pipe(
            v.string(),
            v.check(() => true, "custom message"),
          ),
        () => v.literal(this.pick(["a", "b"])),
      ])();
    }
    if (this.shared.length > 0 && this.random() < 0.15) {
      return this.pick(this.shared);
    }
    const next = depth + 1;
    const built = this.pick<() => any>([
      () => v.string(),
      () => v.pipe(v.number(), v.integer(), v.minValue(0)),
      () => v.optional(this.valibot(next)),
      () => v.nullable(this.valibot(next)),
      () => v.array(this.valibot(next)),
      () => v.union([this.valibot(next), this.valibot(next)]),
      () =>
        v.variant("kind", [
          v.object({ kind: v.literal("x"), value: this.valibot(next) }),
          v.object({ kind: v.literal("y"), other: this.valibot(next) }),
        ]),
      () => v.tuple([this.valibot(next), this.valibot(next)]),
      () => v.record(v.string(), this.valibot(next)),
      () => withIndex(this.valibot(next), { unique: this.random() < 0.5 }),
      () =>
        v.pipe(
          this.valibot(next),
          v.metadata({
            [PRIVACY_SYMBOL]: this.plain(next),
            title: "ignored",
          }),
        ),
      () => v.pipe(this.valibot(next), v.description("ignored")),
      () => v.object(this.entries(next)),
      () => v.strictObject(this.entries(next)),
      () =>
        defineType({ schema: v.object({ field: this.valibot(next) }) }) as any,
    ])();
    if (this.random() < 0.2) this.shared.push(built);
    return built;
  }

  private entries(depth: number): v.ObjectEntries {
    const entries: v.ObjectEntries = {};
    for (const key of this.keys()) entries[key] = this.valibot(depth);
    return entries;
  }

  plain(depth: number): unknown {
    const node: Record<PropertyKey, unknown> = {};
    for (const key of this.keys()) {
      const roll = this.random();
      if (depth >= MAX_DEPTH || roll < 0.4) node[key] = this.leafValue();
      else if (roll < 0.6) node[key] = this.plain(depth + 1);
      else if (roll < 0.75) {
        const array: unknown[] = [this.plain(depth + 1), this.leafValue()];
        if (this.random() < 0.3) array[3] = this.leafValue();
        node[key] = array;
      } else node[key] = this.valibot(depth + 1);
    }
    if (this.random() < 0.15) {
      node[Symbol.for("mongodbee.index")] = { unique: this.random() < 0.5 };
    }
    if (this.random() < 0.15) {
      node[PRIVACY_SYMBOL] = this.plain(depth + 1);
    }
    return node;
  }
}

function build(random: Random, shuffle: Random | null): unknown {
  return new Generator(random, shuffle).plain(0);
}

function outcome<T>(run: () => T): { value: T } | { threw: string } {
  try {
    return { value: run() };
  } catch (error) {
    return { threw: error instanceof Error ? error.constructor.name : "?" };
  }
}

function referenceEqual(a: unknown, b: unknown): boolean {
  return (
    diffSchemas(
      referenceFlattenSchema(simplifySchema(a), simplifySchema),
      referenceFlattenSchema(simplifySchema(b), simplifySchema),
    ).length === 0
  );
}

function previousEqual(a: unknown, b: unknown): boolean {
  return (
    diffSchemas(
      referenceFlattenSchema(referenceSimplifySchema(a)),
      referenceFlattenSchema(referenceSimplifySchema(b)),
    ).length === 0
  );
}

test("flattenSchema writes the same keys, in the same order, with the same values as before", () => {
  for (let seed = 1; seed <= CASES; seed++) {
    const schema = build(seeded(seed), null);
    const simplified = simplifySchema(schema);
    const expected = referenceFlattenSchema(simplified, simplifySchema);
    const actual = flattenSchema(simplified);
    deepStrictEqual(
      Object.entries(actual),
      Object.entries(expected),
      `seed ${seed}`,
    );
    deepStrictEqual(
      Object.entries(flattenSchema(schema)),
      Object.entries(referenceFlattenSchema(schema, simplifySchema)),
      `seed ${seed}, unsimplified`,
    );
  }
});

test("collections without the features the comparison learnt to see keep the verdict they had", () => {
  const collections = (random: Random, shuffle: Random | null) => {
    const generator = new Generator(random, shuffle);
    return {
      users: { _id: generator.valibot(1), name: generator.valibot(1) },
      events: { _id: generator.valibot(1), entries: generator.valibot(1) },
    };
  };
  const cases = CASES / 4;
  let equal = 0;
  let different = 0;
  for (let seed = 1; seed <= cases; seed++) {
    const a = collections(seeded(seed), null);
    for (const b of [
      collections(seeded(seed), null),
      collections(seeded(seed), seeded(seed * 7919)),
      collections(perturbed(seeded(seed), seed % 23), null),
    ]) {
      const expected = outcome(() => previousEqual(a, b));
      deepStrictEqual(
        outcome(() => createSchemaComparer().equal(a, b)),
        expected,
        `seed ${seed}`,
      );
      if ("value" in expected) {
        if (expected.value) equal++;
        else different++;
      }
    }
  }
  assertEquals(equal > cases && different > cases / 4, true);
});

test("the comparison agrees with the previous flatten-and-diff on equal, reordered and perturbed schemas", () => {
  let equal = 0;
  let different = 0;
  for (let seed = 1; seed <= CASES; seed++) {
    const a = build(seeded(seed), null);
    const variants = [
      build(seeded(seed), null),
      build(seeded(seed), seeded(seed * 7919)),
      build(perturbed(seeded(seed), seed % 23), null),
      build(perturbed(seeded(seed), seed % 5), seeded(seed * 31)),
    ];
    for (const b of variants) {
      const expected = outcome(() => referenceEqual(a, b));
      const actual = outcome(() => createSchemaComparer().equal(a, b));
      deepStrictEqual(actual, expected, `seed ${seed}`);
      if ("value" in expected) {
        if (expected.value) equal++;
        else different++;
      }
    }
  }
  assertEquals(equal > CASES && different > CASES / 2, true);
});

test("one comparer reused across many comparisons keeps agreeing", () => {
  const comparer = createSchemaComparer();
  for (let seed = 1; seed <= CASES / 4; seed++) {
    const a = build(seeded(seed), null);
    const b = build(perturbed(seeded(seed), seed % 11), seeded(seed));
    deepStrictEqual(
      outcome(() => comparer.equal(a, b)),
      outcome(() => referenceEqual(a, b)),
      `seed ${seed}`,
    );
  }
});

test("keys whose flattened paths can meet keep their order: the last one written wins, as before", () => {
  const index = Symbol.for("mongodbee.index");
  const pairs: Array<
    [Record<PropertyKey, unknown>, Record<PropertyKey, unknown>]
  > = [
    [
      { "a.b": 1, a: { b: 2 } },
      { a: { b: 2 }, "a.b": 1 },
    ],
    [
      { "x[0]": 1, x: [2] },
      { x: [2], "x[0]": 1 },
    ],
    [
      { "@index": 1, [index]: 2 },
      { [index]: 1, "@index": 2 },
    ],
    [
      { "@privacy": { role: 1 }, [PRIVACY_SYMBOL]: { role: 2 } },
      { "@privacy": { role: 2 }, [PRIVACY_SYMBOL]: { role: 1 } },
    ],
  ];
  for (const [a, b] of pairs) {
    for (const [left, right] of [
      [a, b],
      [{ nested: a }, { nested: b }],
    ]) {
      const expected = referenceEqual(left, right);
      assertEquals(expected, false);
      assertEquals(createSchemaComparer().equal(left, right), expected);
    }
  }
});

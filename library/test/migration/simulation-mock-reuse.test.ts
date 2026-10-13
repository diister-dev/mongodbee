/**
 * The simulation builds one mock generator per schema and reseeds it per
 * document instead of building a seeded generator per document. Locked here:
 * the reseeded generator draws exactly what a fresh `faker: { seed }`
 * generator draws, and reusing it never leaks one invocation's seed or
 * resolve hook into the next.
 */
import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import { createMockGenerator, SKIP } from "@diister/valibot-mock";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import { createSeedableRandomizer } from "../../src/migration/validators/mock/randomizer.ts";
import {
  generateMockDocument,
  generateMockScopeValue,
} from "../../src/migration/validators/mock/generator.ts";

const FIELDS = {
  _id: dbId("user"),
  organizationId: refId("organization"),
  slug: v.pipe(v.string(), v.regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/)),
  email: v.pipe(v.string(), v.email()),
  age: v.pipe(v.number(), v.integer(), v.minValue(0), v.maxValue(120)),
  tags: v.array(v.picklist(["a", "b", "c"])),
  address: v.object({ city: v.string(), zip: v.optional(v.string()) }),
  note: v.nullable(v.string()),
};

const SEEDS = [0, 1, 42, 4_294_967_295, 2 ** 53 - 1, 123_456_789];

const withoutTime = (value: unknown) =>
  JSON.stringify(value)
    // dbId's default mints a time-based ULID, never part of the seeded draw.
    .replace(/user:[0-9a-z]{26}/g, "user:ULID")
    .replace(/\d{4}-\d\d-\d\dT[\d:.]+Z/g, "DATE");

test("mock reuse: a reseeded randomizer draws Faker's own seeded stream", () => {
  const schema = v.object(FIELDS);
  const randomizer = createSeedableRandomizer();
  const reused = createMockGenerator(schema, { faker: { randomizer } });
  for (const seed of SEEDS) {
    const fresh = createMockGenerator(schema, { faker: { seed } });
    randomizer.seed(seed);
    assertEquals(
      withoutTime(reused.generateMany(3)),
      withoutTime(fresh.generateMany(3)),
    );
  }
});

test("mock reuse: a document depends on its seed alone, whatever ran before", () => {
  const first = generateMockDocument(FIELDS, { seed: 7 });
  generateMockDocument(FIELDS, { seed: 8 });
  generateMockDocument(FIELDS, {
    seed: 9,
    resolve: ({ path }) => (path === "slug" ? "injected" : SKIP),
  });
  const again = generateMockDocument(FIELDS, { seed: 7 });
  assertEquals(withoutTime(again), withoutTime(first));
  assertEquals(again.slug === "injected", false);
});

test("mock reuse: a resolve hook generating the same schema gets its own generator", () => {
  const scope = refId("exposition");
  const expected = generateMockScopeValue(scope, { seed: 3 });
  let inner: unknown;
  const outer = generateMockScopeValue(scope, {
    seed: 4,
    resolve: () => {
      inner = generateMockScopeValue(scope, { seed: 3 });
      return SKIP;
    },
  });
  assertEquals(inner, expected);
  assertEquals(outer, generateMockScopeValue(scope, { seed: 4 }));
});

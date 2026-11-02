/**
 * The cost of the chain-versus-project schema comparison follows the number
 * of distinct schema objects, not the size of the expanded tree: a large
 * schema whose types embed the same deep sub-schemas many times compares in
 * work proportional to its types plus its depth, while its flattened form
 * grows with their product. Work is counted (`comparedNodes`), not timed.
 */
import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import {
  flattenSchema,
  simplifySchema,
  validateLastMigrationMatchesProjectSchema,
} from "../../src/migration/schema-validation.ts";
import {
  type LargeSchemaShape,
  largeSchemas,
} from "../fixtures/large-schema.ts";

function compare(shape: LargeSchemaShape) {
  const last = migrationDefinition("001", "snapshot", {
    parent: null,
    schemas: largeSchemas(shape),
    migrate: (b) => b.compile(),
  });
  return validateLastMigrationMatchesProjectSchema(last, largeSchemas(shape));
}

function flattenedKeys(shape: LargeSchemaShape): number {
  return Object.keys(flattenSchema(simplifySchema(largeSchemas(shape)))).length;
}

test("each type added costs the same fixed work, whatever the depth of what it embeds", () => {
  const work = [10, 20, 40].map((types) => compare({ types, depth: 6 }));
  for (const result of work) assertEquals(result.valid, true);
  const [ten, twenty, forty] = work.map((result) => result.comparedNodes);
  assertEquals(forty - twenty, 2 * (twenty - ten));
});

test("each level added to a shared sub-schema costs the same fixed work, whatever the number of types embedding it", () => {
  const work = [2, 4, 6].map((depth) => compare({ types: 30, depth }));
  for (const result of work) assertEquals(result.valid, true);
  const [two, four, six] = work.map((result) => result.comparedNodes);
  assertEquals(six - four, four - two);
});

test("the work stays far below the flattened size, which grows with types times depth", () => {
  const small = { types: 10, depth: 2 };
  const large = { types: 40, depth: 8 };
  const keysGrowth = flattenedKeys(large) / flattenedKeys(small);
  const workGrowth =
    compare(large).comparedNodes / compare(small).comparedNodes;
  assert(keysGrowth > 10, `flattened keys grew ${keysGrowth}x`);
  assert(workGrowth < 4, `comparison work grew ${workGrowth}x`);
  assert(compare(large).comparedNodes * 20 < flattenedKeys(large));
});

test("a drift deep inside a shared sub-schema is still reported", () => {
  const last = migrationDefinition("001", "snapshot", {
    parent: null,
    schemas: largeSchemas({ types: 5, depth: 4 }),
    migrate: (b) => b.compile(),
  });
  const result = validateLastMigrationMatchesProjectSchema(
    last,
    largeSchemas({ types: 5, depth: 5 }),
  );
  assertEquals(result.valid, false);
  assert(result.errors.some((error) => error.includes('type "type_0"')));
});

/**
 * Tests for the migration chain validator.
 *
 * `chain.ts` is 592 lines of public API — `@diister/mongodbee/migration`
 * exports `validateMigrationChain`, `createChainValidator` and
 * `ChainValidator` — that the suite never executed a line of. These cases
 * cover each rule it claims to enforce, plus the shapes most likely to break
 * it: a cycle, a dangling parent reference, and a chain deep enough to
 * exercise the recursive depth walk.
 *
 * @module
 */

import { test } from "../../+harness.ts";
import { assert, assertEquals } from "../../+assert.ts";
import {
  ChainValidator,
  validateMigrationChain,
} from "../../../src/migration/validators/mod.ts";
import type { MigrationDefinition } from "../../../src/migration/types.ts";

/** Builds a migration whose only interesting property is its place in the chain. */
function migration(
  id: string,
  parent: MigrationDefinition | null = null,
): MigrationDefinition {
  return {
    id,
    name: id,
    parent,
    schemas: { collections: {}, multiCollections: {}, multiModels: {} },
    migrate: (m) => m.compile(),
  } as MigrationDefinition;
}

/** A straight chain of `count` migrations, oldest first. */
function linearChain(count: number): MigrationDefinition[] {
  const out: MigrationDefinition[] = [];
  for (let i = 0; i < count; i++) {
    out.push(migration(`m${i}`, i === 0 ? null : out[i - 1]));
  }
  return out;
}

test("chain - a linear chain is valid and reports its shape", () => {
  const chain = linearChain(3);
  const result = validateMigrationChain(chain);

  assertEquals(result.errors, []);
  assert(result.isValid, `expected valid, got ${result.errors.join("; ")}`);
  assertEquals(result.metadata.totalMigrations, 3);
  assertEquals(result.metadata.rootMigrations, 1);
  assertEquals(result.metadata.leafMigrations, 1);
  assertEquals(result.metadata.maxDepth, 2);
  assertEquals(result.metadata.topologicalOrder, ["m0", "m1", "m2"]);
});

test("chain - an empty chain is rejected", () => {
  const result = validateMigrationChain([]);
  assert(!result.isValid);
  assert(
    result.errors.some((e) => e.includes("cannot be empty")),
    `expected an emptiness error, got ${JSON.stringify(result.errors)}`,
  );
});

test("chain - duplicate ids are reported once per duplicate", () => {
  const first = migration("dup");
  const chain = [first, migration("dup", first)];

  const result = validateMigrationChain(chain);
  assert(!result.isValid);
  assertEquals(
    result.errors.filter((e) => e.includes("Duplicate migration ID")).length,
    1,
  );
});

test("chain - a parent that is not in the chain is reported", () => {
  const orphanParent = migration("missing");
  const result = validateMigrationChain([
    migration("m0"),
    migration("m1", orphanParent),
  ]);

  assert(!result.isValid);
  assert(
    result.errors.some((e) => e.includes("non-existent parent")),
    `expected a dangling-parent error, got ${JSON.stringify(result.errors)}`,
  );
});

test("chain - a cycle is detected instead of hanging", () => {
  // Built by hand: `migration()` cannot express a parent that does not exist
  // yet, and a cycle is precisely two migrations naming each other.
  const a = migration("a");
  const b = migration("b", a);
  (a as { parent: MigrationDefinition | null }).parent = b;

  const result = validateMigrationChain([a, b]);
  assert(!result.isValid);
  assert(
    result.errors.some((e) => e.includes("Circular dependency")),
    `expected a cycle error, got ${JSON.stringify(result.errors)}`,
  );
});

test("chain - multiple roots are rejected by default and allowed on request", () => {
  const chain = [migration("r1"), migration("r2")];

  const strict = validateMigrationChain(chain);
  assert(!strict.isValid);
  assert(strict.errors.some((e) => e.includes("Multiple root migrations")));

  const lenient = validateMigrationChain(chain, { allowMultipleRoots: true });
  assertEquals(lenient.errors, []);
  assertEquals(lenient.metadata.rootMigrations, 2);
});

test("chain - branching leaves are allowed by default, refusable on request", () => {
  const root = migration("root");
  const chain = [root, migration("a", root), migration("b", root)];

  assert(validateMigrationChain(chain).isValid);

  const strict = validateMigrationChain(chain, { allowMultipleLeaves: false });
  assert(!strict.isValid);
  assert(strict.errors.some((e) => e.includes("Multiple leaf migrations")));
});

test("chain - maxDepth is enforced against the deepest path", () => {
  const chain = linearChain(5); // depth 4

  assertEquals(validateMigrationChain(chain).metadata.maxDepth, 4);
  assert(validateMigrationChain(chain, { maxDepth: 4 }).isValid);

  const tooDeep = validateMigrationChain(chain, { maxDepth: 3 });
  assert(!tooDeep.isValid);
  assert(tooDeep.errors.some((e) => e.includes("exceeds maximum allowed")));
});

test("chain - strict id format rejects characters a filename cannot carry", () => {
  const chain = [migration("2025_10_09_1445_ABC@init")]; // the real ID shape
  assertEquals(validateMigrationChain(chain).errors, []);

  const bad = validateMigrationChain([migration("has spaces/and-slash")]);
  assert(!bad.isValid);
  assert(bad.errors.some((e) => e.includes("invalid characters")));

  // …and passes when the caller opts out.
  const lenient = new ChainValidator({ strictIdFormat: false }).validateChain([
    migration("has spaces/and-slash"),
  ]);
  assertEquals(lenient.errors, []);
});

test("chain - a migration without a migrate function is rejected", () => {
  const broken = migration("m0");
  (broken as { migrate: unknown }).migrate = undefined;

  const result = validateMigrationChain([broken]);
  assert(!result.isValid);
  assert(
    result.errors.some((e) => e.includes("migrate function")),
    `expected a migrate-function error, got ${JSON.stringify(result.errors)}`,
  );
});

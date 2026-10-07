import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { unique } from "../src/indexes.ts";
import { defineType } from "../src/type-definition.ts";
import { validateLastMigrationMatchesProjectSchema } from "../src/migration/schema-validation.ts";
import type {
  MigrationDefinition,
  SchemasDefinition,
} from "../src/migration/types.ts";

function baseEntries() {
  return {
    userId: refId("user"),
    roleId: refId("role"),
  };
}

function withUnique(entries = baseEntries()) {
  return defineType({
    schema: v.object(entries),
    indexes: (f) => [unique(f.userId, f.roleId)],
  });
}

function schemasWith(types: Record<string, unknown>): SchemasDefinition {
  return {
    scopedMultiCollections: {
      "+expositions": { scope: refId("exposition"), types },
    },
  } as unknown as SchemasDefinition;
}

function migrationWith(schemas: SchemasDefinition): MigrationDefinition {
  return {
    id: "spike",
    name: "spike",
    parent: null,
    schemas,
  } as unknown as MigrationDefinition;
}

function check(
  snapshot: Record<string, unknown>,
  living: Record<string, unknown>,
) {
  return validateLastMigrationMatchesProjectSchema(
    migrationWith(schemasWith(snapshot)),
    schemasWith(living),
  );
}

test("gate: adding a composite index without mirroring it is drift", () => {
  const result = check(
    { user_role: baseEntries() },
    { user_role: withUnique() },
  );
  assertEquals(result.valid, false);
  assert(
    result.errors.join("\n").includes("user_role"),
    result.errors.join("\n"),
  );
});

test("gate: the mirrored snapshot matches the living schema", () => {
  const result = check(
    { user_role: withUnique() },
    { user_role: withUnique() },
  );
  assertEquals(result.errors, []);
  assertEquals(result.valid, true);
});

test("gate: losing the declaration in the living schema is drift", () => {
  const result = check(
    { user_role: withUnique() },
    { user_role: baseEntries() },
  );
  assertEquals(result.valid, false);
});

test("gate: flipping unique off is drift", () => {
  const relaxed = defineType({
    schema: v.object(baseEntries()),
    indexes: [{ key: { userId: 1, roleId: 1 } }],
  });
  const result = check({ user_role: withUnique() }, { user_role: relaxed });
  assertEquals(result.valid, false);
});

test("gate: wrapping an unchanged type in defineType without indexes is not drift", () => {
  const wrapped = defineType({ schema: v.object(baseEntries()) });
  const result = check({ user_role: baseEntries() }, { user_role: wrapped });
  assertEquals(result.errors, []);
  assertEquals(result.valid, true);
});

test("gate: freezing from the parent definition carries the declaration", () => {
  const parentTypes = { user_role: withUnique() };

  const frozen = defineType({
    schema: v.object({ ...parentTypes.user_role.entries, status: v.string() }),
    indexes: (f) => [unique(f.userId, f.roleId)],
  });
  const living = defineType({
    schema: v.object({ ...baseEntries(), status: v.string() }),
    indexes: (f) => [unique(f.userId, f.roleId)],
  });

  const result = check({ user_role: frozen }, { user_role: living });
  assertEquals(result.errors, []);
  assertEquals(result.valid, true);
});

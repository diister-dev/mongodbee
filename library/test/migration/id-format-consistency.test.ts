/**
 * Regression: `generateMigrationId()` produces a documented ID format
 * (`YYYY_MM_DD_HHMM_<ULID>[@<name>]`) that the public-API
 * `validateMigrationChain()` then rejected because it required numeric-only
 * IDs. The two functions must agree on what a valid ID looks like.
 */
import { assert, assertEquals } from "@std/assert";
import {
  generateMigrationId,
  migrationDefinition,
  validateMigrationChain,
} from "../../src/migration/definition.ts";
import * as v from "../../src/schema.ts";

Deno.test("generateMigrationId output is accepted by validateMigrationChain", () => {
  const id1 = generateMigrationId();
  const id2 = generateMigrationId("add-users");
  const id3 = generateMigrationId("complex name with spaces");

  // Sanity checks on the format itself
  assert(/^\d{4}_\d{2}_\d{2}_\d{4}_/.test(id1), `id1 has wrong shape: ${id1}`);
  assert(id2.endsWith("@add-users"), `id2 missing name: ${id2}`);
  assert(
    id3.endsWith("@complex-name-with-spaces"),
    `id3 spaces should be replaced: ${id3}`,
  );

  // ULIDs ensure id1 and id2 differ even when generated back-to-back
  assert(id1 !== id2 && id2 !== id3 && id1 !== id3);

  // Each ID should validate cleanly when used in a single-migration chain
  for (const id of [id1, id2, id3]) {
    const m = migrationDefinition(id, "test", {
      parent: null,
      schemas: { collections: { foo: { _id: v.string() } } },
      migrate: (b) => b.compile(),
    });
    const result = validateMigrationChain([m]);
    assertEquals(
      result.errors,
      [],
      `validateMigrationChain rejected a generated id (${id}): ${result.errors.join(", ")}`,
    );
    assert(result.valid, `result.valid should be true for ${id}`);
  }
});

Deno.test("validateMigrationChain still rejects empty / non-string IDs", () => {
  // The relaxation should NOT make the validator a no-op — invalid IDs
  // (empty, contains pathological chars) should still be caught at
  // migrationDefinition construction time.
  let threw = false;
  try {
    migrationDefinition("", "test", {
      parent: null,
      schemas: { collections: { foo: { _id: v.string() } } },
      migrate: (b) => b.compile(),
    });
  } catch {
    threw = true;
  }
  assert(threw, "empty id should throw at migrationDefinition");
});

Deno.test("validateMigrationChain preserves IDs lexicographic ordering check via parent chain", () => {
  const id1 = generateMigrationId("first");
  // Small delay to make sure id2 > id1 lexically
  const id2 = "9999_99_99_9999_ZZZZZZZZZZ@later";

  const m1 = migrationDefinition(id1, "first", {
    parent: null,
    schemas: { collections: { foo: { _id: v.string() } } },
    migrate: (b) => b.compile(),
  });
  const m2 = migrationDefinition(id2, "second", {
    parent: m1,
    schemas: { collections: { foo: { _id: v.string() } } },
    migrate: (b) => b.compile(),
  });

  const result = validateMigrationChain([m1, m2]);
  assertEquals(
    result.errors,
    [],
    "well-formed chain with timestamp+ULID ids should validate",
  );
});

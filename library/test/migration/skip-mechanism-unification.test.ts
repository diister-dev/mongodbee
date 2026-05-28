/**
 * Regression: the multi-collection instance skip mechanism used two
 * incompatible strategies — timestamp-based (mongodb applier) vs.
 * chain-based (catch-up). They could disagree when migration IDs do
 * not sort the same way as the parent chain (e.g. legacy padded IDs
 * mixed with timestamp+ULID IDs, or out-of-order user-renamed IDs).
 *
 * The chain-based comparison is the source of truth; the timestamp
 * version is unsound in the general case.
 */
import { assert, assertEquals } from "@std/assert";
import {
  isInstanceCreatedAfterMigration,
  shouldInstanceReceiveMigrationByChain,
} from "../../src/migration/multicollection-registry.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import * as v from "../../src/schema.ts";

function makeMigration(id: string, parent: ReturnType<typeof migrationDefinition> | null) {
  return migrationDefinition(id, `m-${id}`, {
    parent,
    schemas: { collections: { foo: { _id: v.string() } } },
    migrate: (b) => b.compile(),
  });
}

Deno.test(
  "chain-based and timestamp-based skip diverge on out-of-order IDs",
  () => {
    // Chain order : m1 -> m2 -> m3, but IDs sort weirdly:
    //   "001"  (m1)         lexically: 001 > 0001
    //   "002"  (m2)
    //   "0001" (m3, latest in chain but smallest lexically)
    const m1 = makeMigration("001", null);
    const m2 = makeMigration("002", m1);
    const m3 = makeMigration("0001", m2);

    // Instance was created at m3 (latest). Now we try to apply m1 (oldest).
    // The correct answer: instance is in the FUTURE relative to m1 — skip.

    // Chain-based: walk m1's parents (none) → m3 not found → instance not in
    // m1's ancestry → should NOT receive → correct.
    assertEquals(
      shouldInstanceReceiveMigrationByChain(m3, m1),
      false,
      "chain-based: instance@m3, current=m1 → false (skip)",
    );

    // Timestamp-based: "0001" > "001"? false → returns "not after" → would
    // report "should receive" → WRONG.
    assertEquals(
      isInstanceCreatedAfterMigration("0001", "001"),
      false,
      "timestamp-based wrongly reports instance was NOT created after m1",
    );
  },
);

Deno.test("chain-based: instance@m1 should receive m2 (older instance, newer migration)", () => {
  const m1 = makeMigration("001", null);
  const m2 = makeMigration("002", m1);
  assertEquals(shouldInstanceReceiveMigrationByChain(m1, m2), true);
});

Deno.test("chain-based: instance@m2 should NOT receive m1 (newer instance, older migration)", () => {
  const m1 = makeMigration("001", null);
  const m2 = makeMigration("002", m1);
  assertEquals(shouldInstanceReceiveMigrationByChain(m2, m1), false);
});

Deno.test("chain-based: instance receives the migration it was created at", () => {
  const m1 = makeMigration("001", null);
  assertEquals(shouldInstanceReceiveMigrationByChain(m1, m1), true);
});

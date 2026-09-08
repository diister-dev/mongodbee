/**
 * Verrou — chain-invariant simulation warnings must be reported ONCE.
 *
 * `deno task db check` on a 12-migration chain printed ~150 lines, 92 of them
 * the same four `⚠ Mock identity correlation:` sentences repeated under every
 * migration. Those findings describe the MODEL (an ambiguous identifier space,
 * a reference nobody mints), so the simulation legitimately re-raises them per
 * migration — but the reporter must fold them. The `✗ Invalid` that mattered
 * was buried at line 131 of 142 because of that repetition.
 *
 * These lock the folding rules, not the validation: the digest never drops a
 * distinct message and never touches a verdict.
 */
import { test } from "../../+harness.ts";
import { assertEquals } from "../../+assert.ts";
import {
  digestWarnings,
  formatWarningDigest,
  warningFamilyKey,
} from "../../../src/migration/cli/utils/warning-digest.ts";

// Verbatim messages from a real `check` run, minus the ANSI colours.
const AMBIGUOUS_SPACE =
  'Mock identity correlation: Identifier space "role" is minted by 2 targets ' +
  "(collections/+roles/, scopedMultiCollections/+expositions/role) — correlated " +
  'references draw from "collections/+roles/" (deterministic tie-break). Declare ' +
  '"role" in uncorrelatedSpaces to silence this if the ambiguity is intended.';

const UNMINTED_NODE =
  'Mock identity correlation: Identifier space "node" is referenced (e.g. field ' +
  '"flowSnapshot.nodes.id" of collections "+flow_sessions") but no _id schema ' +
  'mints it — these references stay uncorrelated random values. Declare "node" ' +
  "in uncorrelatedSpaces to make that assumption explicit.";

const UNMINTED_EDGE = UNMINTED_NODE.replaceAll('"node"', '"edge"').replace(
  "flowSnapshot.nodes.id",
  "flowSnapshot.edges.id",
);

const drawMiss = (space: string, collection: string, field: string) =>
  `Mock identity correlation: Correlated draw found no "${space}" id for ` +
  `collections "${collection}" field "${field}" — an uncorrelated value was ` +
  "generated instead.";

test("warning digest: the same message under 12 migrations folds to one group", () => {
  const sources = Array.from({ length: 12 }, (_, i) => ({
    migrationId: `m${i}`,
    warnings: [AMBIGUOUS_SPACE, UNMINTED_NODE, UNMINTED_EDGE],
  }));

  const digest = digestWarnings(sources);

  assertEquals(digest.occurrences, 36, "every emission is still counted");
  assertEquals(
    digest.groups.length,
    3,
    "three distinct findings, not 36 lines",
  );
  assertEquals(digest.groups[0].representative, AMBIGUOUS_SPACE);
  assertEquals(digest.groups[0].occurrences, 12);
  assertEquals(digest.groups[0].migrations.length, 12);
});

test("warning digest: near-identical draw misses collapse by space, not by site", () => {
  // Same finding ("no role id available"), different collection/field each
  // time — one group. A different SPACE stays its own group.
  const digest = digestWarnings([
    {
      migrationId: "m1",
      warnings: [
        drawMiss("role", "+table_1", "owner"),
        drawMiss("role", "+table_2", "owner"),
        drawMiss("role", "+table_2", "reviewer"),
        drawMiss("exposition", "+table_3", "expo"),
      ],
    },
  ]);

  assertEquals(digest.groups.length, 2);
  assertEquals(digest.groups[0].variants.length, 3, "three sites, one finding");
  assertEquals(digest.groups[1].variants.length, 1);
  assertEquals(digest.distinct, 4, "no distinct message is lost");
});

test("warning family key: the subject is kept, the site is masked", () => {
  // Same space + same shape → same key regardless of where it was observed.
  assertEquals(
    warningFamilyKey(drawMiss("role", "+a", "x")),
    warningFamilyKey(drawMiss("role", "+b", "y")),
  );
  // Different space → different key. Collapsing these would hide a finding.
  const roleKey = warningFamilyKey(drawMiss("role", "+a", "x"));
  const expoKey = warningFamilyKey(drawMiss("exposition", "+a", "x"));
  assertEquals(roleKey === expoKey, false);
  // Different SHAPE on the same space also stays apart.
  assertEquals(
    warningFamilyKey(AMBIGUOUS_SPACE) === warningFamilyKey(UNMINTED_NODE),
    false,
  );
});

test("warning digest: nothing to say prints nothing", () => {
  const digest = digestWarnings([{ migrationId: "m1", warnings: [] }]);
  assertEquals(digest.groups.length, 0);
  assertEquals(formatWarningDigest(digest, { totalMigrations: 1 }), []);
});

test("warning digest formatting: one line per finding, counts attached", () => {
  const sources = Array.from({ length: 12 }, (_, i) => ({
    migrationId: `m${i}`,
    warnings: [AMBIGUOUS_SPACE, UNMINTED_NODE],
  }));
  const lines = formatWarningDigest(digestWarnings(sources), {
    totalMigrations: 12,
  });

  assertEquals(lines[0], "⚠ 2 distinct warnings (24 occurrences)");
  assertEquals(lines[2], `  ⚠ ${AMBIGUOUS_SPACE}`);
  assertEquals(lines[3], "      ×12 · every migration (12)");
  assertEquals(
    lines.filter((line) => line.includes(AMBIGUOUS_SPACE)).length,
    1,
    "the chain-invariant sentence appears exactly once",
  );
});

test("warning digest formatting: --verbose restores every site", () => {
  const digest = digestWarnings([
    {
      migrationId: "m1",
      warnings: [drawMiss("role", "+a", "x"), drawMiss("role", "+b", "y")],
    },
  ]);

  const quiet = formatWarningDigest(digest, { totalMigrations: 1 });
  assertEquals(
    quiet.some((line) => line.includes("+b")),
    false,
    "the second site is summarised, not printed, by default",
  );
  assertEquals(
    quiet.some((line) => line.includes("1 other site")),
    true,
  );

  const verbose = formatWarningDigest(digest, {
    totalMigrations: 1,
    verbose: true,
  });
  assertEquals(
    verbose.some((line) => line.includes("+b")),
    true,
  );
});

test("warning digest formatting: the group cap points at --verbose instead of lying", () => {
  const digest = digestWarnings([
    {
      migrationId: "m1",
      warnings: Array.from({ length: 30 }, (_, i) =>
        drawMiss(`s${i}`, "+a", "x"),
      ),
    },
  ]);

  const capped = formatWarningDigest(digest, {
    totalMigrations: 1,
    maxGroups: 25,
  });
  assertEquals(
    capped.at(-1),
    "  … 5 more warnings (run with --verbose)",
    "hidden groups are announced, never silently dropped",
  );

  const verbose = formatWarningDigest(digest, {
    totalMigrations: 1,
    maxGroups: 25,
    verbose: true,
  });
  assertEquals(
    verbose.filter((line) => line.startsWith("  ⚠ ")).length,
    30,
    "--verbose ignores the cap",
  );
});

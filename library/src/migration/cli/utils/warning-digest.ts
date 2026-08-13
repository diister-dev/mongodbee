/**
 * Chain-wide deduplication of simulation warnings.
 *
 * Most simulation warnings describe the MODEL (an ambiguous identifier space,
 * a reference nobody mints), not the migration being validated — so the same
 * sentence is re-emitted under all twelve migrations of a chain and the one
 * `✗ Invalid` that matters drowns in it. This module folds the per-migration
 * warning arrays into distinct groups so the reporter can print each finding
 * once, with a count, instead of once per migration.
 *
 * It is a PRESENTATION layer: it never decides validity, never drops a
 * finding (every distinct message survives as a variant), and never runs
 * unless the caller asks for a digest.
 *
 * @module
 */

/** One deduplicated finding, with everywhere it was seen. */
export interface WarningGroup {
  /** Grouping identity — see {@link warningFamilyKey}. */
  key: string;
  /** First message of the group, printed verbatim as the headline. */
  representative: string;
  /**
   * Distinct messages folded into this group, in first-seen order. Holds the
   * representative plus every near-identical sibling (same finding, different
   * field/collection/scope), so `--verbose` can restore the full detail.
   */
  variants: string[];
  /** Total emissions across the chain, duplicates included. */
  occurrences: number;
  /** Migration ids the group was seen under, deduplicated, in chain order. */
  migrations: string[];
}

/** Result of {@link digestWarnings}. */
export interface WarningDigest {
  groups: WarningGroup[];
  /** Total warning lines the old reporter would have printed. */
  occurrences: number;
  /** Number of distinct messages (before family grouping). */
  distinct: number;
}

/** One migration's warnings, as fed to {@link digestWarnings}. */
export interface WarningSource {
  migrationId: string;
  warnings: readonly string[];
}

// A parenthesised run, including one nesting level — the messages carry
// `(collections/a, collections/b)` and `(scope "x")` tails that are pure site
// detail.
const PARENTHESISED = /\((?:[^()]|\([^()]*\))*\)/g;
const QUOTED = /"[^"]*"/g;
const NUMBER = /\b\d+\b/g;

/**
 * Identity two warnings must share to be reported as one finding.
 *
 * The convention across the validator's messages is that the SUBJECT comes
 * first (`Identifier space "role" …`, `Correlated draw found no "role" id …`)
 * and everything after it is the site where the subject was observed — the
 * collection, the field path, the scope, a count. Keeping the first quoted
 * token and masking the rest therefore collapses "same finding, other field"
 * while keeping "same shape, other space" apart.
 *
 * @param message - Raw warning message
 * @returns A stable grouping key
 */
export function warningFamilyKey(message: string): string {
  let subjectSeen = false;
  return message
    .replace(PARENTHESISED, "(…)")
    .replace(QUOTED, (match) => {
      if (subjectSeen) return '"…"';
      subjectSeen = true;
      return match;
    })
    .replace(NUMBER, "N");
}

/**
 * Folds per-migration warnings into distinct groups.
 *
 * @param sources - Per-migration warning arrays, in chain order
 * @returns The digest; `groups` keeps first-seen order so the output reads
 *   like the chain does
 */
export function digestWarnings(
  sources: Iterable<WarningSource>,
): WarningDigest {
  const groups = new Map<string, WarningGroup>();
  const distinctMessages = new Set<string>();
  let occurrences = 0;

  for (const source of sources) {
    for (const warning of source.warnings) {
      occurrences++;
      distinctMessages.add(warning);

      const key = warningFamilyKey(warning);
      let group = groups.get(key);
      if (!group) {
        group = {
          key,
          representative: warning,
          variants: [],
          occurrences: 0,
          migrations: [],
        };
        groups.set(key, group);
      }
      group.occurrences++;
      if (!group.variants.includes(warning)) group.variants.push(warning);
      if (group.migrations.at(-1) !== source.migrationId) {
        // Chain order means repeats are adjacent; only a re-entry needs the
        // membership check.
        if (!group.migrations.includes(source.migrationId)) {
          group.migrations.push(source.migrationId);
        }
      }
    }
  }

  return {
    groups: [...groups.values()],
    occurrences,
    distinct: distinctMessages.size,
  };
}

/**
 * Renders the digest as plain, uncoloured lines.
 *
 * @param digest - Output of {@link digestWarnings}
 * @param options.totalMigrations - Chain length, so "seen in 12/12" can read
 *   as "every migration"
 * @param options.verbose - List every variant instead of the representative
 *   alone
 * @param options.maxGroups - Soft cap on printed groups (ignored when
 *   verbose). Defaults to 25.
 * @returns Lines to print, already indented; empty when there is nothing to say
 */
export function formatWarningDigest(
  digest: WarningDigest,
  options: {
    totalMigrations: number;
    verbose?: boolean;
    maxGroups?: number;
  },
): string[] {
  if (digest.groups.length === 0) return [];

  const { totalMigrations, verbose = false } = options;
  const maxGroups = verbose ? digest.groups.length : options.maxGroups ?? 25;
  const lines: string[] = [];

  const plural = (n: number, word: string) =>
    `${n} ${word}${n === 1 ? "" : "s"}`;
  const headline = digest.occurrences > digest.groups.length
    ? `${plural(digest.groups.length, "distinct warning")} (${
      plural(digest.occurrences, "occurrence")
    })`
    : plural(digest.groups.length, "warning");
  lines.push(`⚠ ${headline}`);
  lines.push("");

  for (const group of digest.groups.slice(0, maxGroups)) {
    lines.push(`  ⚠ ${group.representative}`);

    const scope = group.migrations.length >= totalMigrations
      ? `every migration (${totalMigrations})`
      : group.migrations.length === 1
      ? `1 migration`
      : `${group.migrations.length}/${totalMigrations} migrations`;
    const extraVariants = group.variants.length - 1;
    const detail = [
      `×${group.occurrences}`,
      scope,
      extraVariants > 0 ? `${plural(extraVariants, "other site")}` : undefined,
    ].filter(Boolean).join(" · ");
    lines.push(`      ${detail}`);

    if (verbose && extraVariants > 0) {
      for (const variant of group.variants.slice(1)) {
        lines.push(`      · ${variant}`);
      }
    }
  }

  const hidden = digest.groups.length - maxGroups;
  if (hidden > 0) {
    lines.push(`  … ${plural(hidden, "more warning")} (run with --verbose)`);
  }

  return lines;
}

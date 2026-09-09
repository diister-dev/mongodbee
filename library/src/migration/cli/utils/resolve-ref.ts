/**
 * Resolves a user-typed migration reference to exactly one migration.
 *
 * @module
 */

/** The minimum a migration must expose to be addressable on the command line. */
export type MigrationRef = {
  id: string;
  name: string;
};

/**
 * Finds the single migration a reference designates.
 *
 * Ids carry a timestamp and a random suffix (`2026_08_17_2121_8SKVBG31DA`), so
 * asking an operator to retype one in full is how targeting mistakes happen.
 * A substring is accepted, but only when it names ONE migration: a command that
 * picks a "best match" out of several would quietly aim at the wrong point of
 * the chain, and both callers of this function decide what gets written to a
 * real database.
 *
 * @param migrations - The chain, in order.
 * @param ref - An exact id, an exact name, or a substring of either.
 * @returns The single matching migration.
 * @throws When nothing matches, or when the reference is ambiguous.
 */
export function resolveMigrationRef<T extends MigrationRef>(
  migrations: readonly T[],
  ref: string,
): T {
  const needle = ref.trim();
  if (needle === "") {
    throw new Error("Migration reference is empty");
  }

  const exact =
    migrations.find((m) => m.id === needle) ??
    migrations.find((m) => m.name === needle);
  if (exact) return exact;

  const lowered = needle.toLowerCase();
  const partial = migrations.filter(
    (m) =>
      m.id.toLowerCase().includes(lowered) ||
      m.name.toLowerCase().includes(lowered),
  );

  if (partial.length === 1) return partial[0];

  if (partial.length === 0) {
    throw new Error(
      `No migration matches "${ref}".\n` +
        `Known migrations:\n${migrations
          .map((m) => `  ${m.id} (${m.name})`)
          .join("\n")}`,
    );
  }

  throw new Error(
    `"${ref}" is ambiguous, it matches ${partial.length} migrations:\n` +
      `${partial.map((m) => `  ${m.id} (${m.name})`).join("\n")}\n` +
      `Use the full id.`,
  );
}

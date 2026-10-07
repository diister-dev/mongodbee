/**
 * Command-line argument parsing, without a runtime-specific dependency.
 *
 * This replaces `@std/cli/parse-args`, which is JSR-only: a package published
 * to npm cannot depend on `@jsr/*` without forcing every consumer to add an
 * `.npmrc` pointing at `npm.jsr.io`.
 *
 * `node:util`'s `parseArgs` is NOT a drop-in replacement here — it has no
 * notion of negatable flags, so the tri-state `--progress` / `--no-progress` /
 * omitted contract the CLI relies on cannot be expressed with it.
 *
 * The semantics below mirror the std implementation for the surface the CLI
 * uses, including its minimist-inherited quirks that callers depend on:
 * numeric coercion of undeclared values (`--last 5` yields the number `5`,
 * which is what `options.last > 0` reads), short-flag groups, and `--` as a
 * terminator.
 *
 * @module
 */

/** Options accepted by {@linkcode parseArgs}. */
export interface ParseArgsOptions {
  /** Flags to always treat as booleans. */
  boolean?: string[];
  /** Flags to always treat as strings (never numerically coerced). */
  string?: string[];
  /** Flags that also accept a `--no-<flag>` form yielding `false`. */
  negatable?: string[];
  /** Short/long aliases, e.g. `{ v: "version" }`. */
  alias?: Record<string, string>;
  /** Seed values, applied when a flag is absent from the argument vector. */
  default?: Record<string, unknown>;
}

/** Result of {@linkcode parseArgs}: parsed flags plus positional arguments. */
export type ParsedArgs = Record<string, unknown> & { _: (string | number)[] };

const NON_WHITESPACE = /\S/;

/** Matches std's `isNumber`: a numeric-looking, finite, non-blank string. */
function isNumber(value: string): boolean {
  return NON_WHITESPACE.test(value) && Number.isFinite(Number(value));
}

/**
 * Parses an argument vector into flags and positionals.
 *
 * @param args The argument vector, typically `process.argv.slice(2)`.
 * @param options Declarations steering how each flag is interpreted.
 * @returns The parsed flags, with positionals under `_`.
 *
 * @example
 * ```typescript
 * const args = parseArgs(["migrate", "--last", "5", "--no-progress"], {
 *   boolean: ["progress"],
 *   negatable: ["progress"],
 * });
 * // { _: ["migrate"], last: 5, progress: false }
 * ```
 */
export function parseArgs(
  args: string[],
  options: ParseArgsOptions = {},
): ParsedArgs {
  const negatables = new Set(options.negatable ?? []);
  const aliases = options.alias ?? {};

  // Aliases resolve both ways: `-v` and `--version` must set the same keys, and
  // std writes the value to every name in the group rather than to a canonical
  // one, so `args.v` and `args.version` are both readable.
  const groupOf = new Map<string, string[]>();
  for (const [from, to] of Object.entries(aliases)) {
    const group = [from, to];
    groupOf.set(from, group);
    groupOf.set(to, group);
  }

  // Declarations name the long form (`help`, `mode`), but the argument vector
  // carries the short one (`-h`, `-m`). Expanding each declaration over its
  // alias group is what makes `-h fast` leave `fast` positional instead of
  // swallowing it, and `-m` alone yield `""` rather than `true`.
  const expand = (names: string[] | undefined): Set<string> => {
    const out = new Set<string>();
    for (const name of names ?? []) {
      for (const member of groupOf.get(name) ?? [name]) out.add(member);
    }
    return out;
  };
  const booleans = expand(options.boolean);
  const strings = expand(options.string);

  const result = { _: [] as (string | number)[] } as ParsedArgs;
  /** Keys the argument vector set explicitly, which defaults must not clobber. */
  const seen = new Set<string>();

  const setValue = (key: string, value: unknown): void => {
    for (const name of groupOf.get(key) ?? [key]) {
      result[name] = value;
      seen.add(name);
    }
  };

  /** Coerces a raw value the way std does, unless the flag is declared string. */
  const coerce = (key: string, raw: string): unknown => {
    if (strings.has(key)) return raw;
    return isNumber(raw) ? Number(raw) : raw;
  };

  /** True when `--flag value` should consume `value` as the flag's argument. */
  const takesValue = (key: string, next: string | undefined): boolean => {
    if (booleans.has(key)) return false;
    if (next === undefined) return false;
    // A following token that is itself a flag belongs to the next option, not
    // to this one — except a bare negative number, which std treats as a value.
    return !/^-[^\d.]/.test(next);
  };

  /**
   * Value for a flag that consumed nothing. A string-declared flag collapses to
   * the empty string (`--name` alone is `name: ""`); anything else is a bare
   * boolean switch.
   */
  const emptyValue = (key: string): unknown => (strings.has(key) ? "" : true);

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;

    // `--` terminates flag parsing; the remainder is positional verbatim.
    if (arg === "--") {
      for (const rest of args.slice(i + 1)) result._.push(rest);
      break;
    }

    if (arg.startsWith("--")) {
      const body = arg.slice(2);
      const eq = body.indexOf("=");

      if (eq !== -1) {
        const key = body.slice(0, eq);
        const raw = body.slice(eq + 1);
        setValue(key, booleans.has(key) ? raw !== "false" : coerce(key, raw));
        continue;
      }

      // `--no-progress` only negates when `progress` was declared negatable;
      // otherwise `no-progress` is just a flag of that name, as in std.
      if (body.startsWith("no-") && negatables.has(body.slice(3))) {
        setValue(body.slice(3), false);
        continue;
      }

      if (takesValue(body, args[i + 1])) {
        setValue(body, coerce(body, args[++i]!));
      } else {
        setValue(body, emptyValue(body));
      }
      continue;
    }

    if (arg.startsWith("-") && arg !== "-") {
      const body = arg.slice(1);
      const eq = body.indexOf("=");

      if (eq !== -1) {
        const key = body.slice(0, eq);
        const raw = body.slice(eq + 1);
        setValue(key, booleans.has(key) ? raw !== "false" : coerce(key, raw));
        continue;
      }

      // Short groups: every letter but the last is a boolean, and the last may
      // take the following token as its value (`-abc value`).
      for (let c = 0; c < body.length; c++) {
        const key = body[c]!;
        const isLast = c === body.length - 1;
        if (isLast && takesValue(key, args[i + 1])) {
          setValue(key, coerce(key, args[++i]!));
        } else {
          setValue(key, isLast ? emptyValue(key) : true);
        }
      }
      continue;
    }

    result._.push(isNumber(arg) ? Number(arg) : arg);
  }

  // Both fill-in passes test the same snapshot of what the vector actually
  // mentioned. Filling booleans in first must not make them look explicit, or
  // `default: { progress: undefined }` would never apply and `--progress` would
  // collapse from tri-state to a plain `false`.
  const explicit = new Set(seen);
  for (const key of booleans) {
    if (!explicit.has(key)) setValue(key, false);
  }
  for (const [key, value] of Object.entries(options.default ?? {})) {
    if (!explicit.has(key)) setValue(key, value);
  }

  return result;
}

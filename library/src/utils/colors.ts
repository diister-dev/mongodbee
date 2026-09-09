/**
 * ANSI colouring for CLI output, without a runtime-specific dependency.
 *
 * This replaces `@std/fmt/colors`, which is JSR-only: a package published to
 * npm cannot depend on `@jsr/*` without forcing every consumer to add an
 * `.npmrc` pointing at `npm.jsr.io`.
 *
 * Behaviour matches `@std/fmt/colors`: styling is applied unconditionally
 * unless `NO_COLOR` is set to a non-empty value (https://no-color.org). It
 * deliberately does NOT test for a TTY — callers that need that decision make
 * it themselves (see `isInteractive()` in the CLI reporters), and the tests
 * strip the codes rather than expect them to be absent.
 *
 * @module
 */

import process from "node:process";

/** https://no-color.org — any non-empty value disables colouring. */
const noColor = (process.env.NO_COLOR ?? "") !== "";

function wrap(open: number, close: number): (str: string) => string {
  const prefix = `\x1b[${open}m`;
  const suffix = `\x1b[${close}m`;
  return (str: string) => (noColor ? str : `${prefix}${str}${suffix}`);
}

/** Bold text. */
export const bold: (str: string) => string = wrap(1, 22);
/** Dimmed text. */
export const dim: (str: string) => string = wrap(2, 22);
/** Red text. */
export const red: (str: string) => string = wrap(31, 39);
/** Green text. */
export const green: (str: string) => string = wrap(32, 39);
/** Yellow text. */
export const yellow: (str: string) => string = wrap(33, 39);
/** Blue text. */
export const blue: (str: string) => string = wrap(34, 39);
/** Gray (bright black) text. */
export const gray: (str: string) => string = wrap(90, 39);

/**
 * Matches the ANSI escape sequences `@std/fmt/colors` emits and strips.
 *
 * Kept identical to the std implementation so tests that assert on stripped
 * output keep the same meaning.
 */
const ANSI_PATTERN = new RegExp(
  [
    "[\\u001B\\u009B][[\\]()#;?]*(?:(?:(?:(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]+)*",
    "|[a-zA-Z\\d]+(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]*)*)?\\u0007)",
    "|(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PR-TZcf-nq-uy=><~]))",
  ].join(""),
  "g",
);

/** Removes every ANSI escape code from a string. */
export function stripAnsiCode(str: string): string {
  return str.replace(ANSI_PATTERN, "");
}

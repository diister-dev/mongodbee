/**
 * The assertion vocabulary the suite is written in.
 *
 * Most of it delegates straight to `node:assert/strict`. Those are thin
 * wrappers rather than bare re-exports on purpose: Bun's and `@types/node`'s
 * declarations of `node:assert` differ, so re-exporting makes the suite's
 * assertion signatures depend on which type package happens to be in scope.
 *
 * Four functions need real code of their own:
 *
 * - `assertThrows` / `assertRejects` RETURN the error. Node's `throws` and
 *   `rejects` return nothing, and 23 call sites capture the error to make
 *   several separate assertions about it, which a `{ message: /…/ }` matcher
 *   cannot express.
 * - `assertExists` narrows the type (`asserts actual is NonNullable<T>`), which
 *   no `node:assert` function does.
 * - `assertStringIncludes` has no `node:assert` counterpart at all.
 *
 * The module earns its place on those four; the rest are aliases kept so the
 * existing call sites read the same.
 *
 * @module
 */

import {
  deepStrictEqual,
  match,
  notDeepStrictEqual,
  ok,
} from "node:assert/strict";

/** Asserts that `expr` is truthy. */
export function assert(expr: unknown, msg?: string): asserts expr {
  ok(expr, msg);
}

/**
 * Asserts deep structural equality between `actual` and `expected`.
 *
 * Note the absence of node's `asserts actual is T` predicate: node narrows the
 * checked variable, `@std/assert` did not, and adopting the narrowing silently
 * changed inference at call sites — collapsing one to `never` and making two
 * others circular.
 */
export function assertEquals<T>(actual: T, expected: T, msg?: string): void {
  deepStrictEqual(actual, expected, msg);
}

/** Asserts that `actual` and `expected` are NOT deeply equal. */
export function assertNotEquals(
  actual: unknown,
  expected: unknown,
  msg?: string,
): void {
  notDeepStrictEqual(actual, expected, msg);
}

/** Asserts that `actual` matches `expected`. */
export function assertMatch(
  actual: string,
  expected: RegExp,
  msg?: string,
): void {
  match(actual, expected, msg);
}

/** Asserts that `actual` is neither `null` nor `undefined`. */
export function assertExists<T>(
  actual: T,
  msg = "Expected value to exist",
): asserts actual is NonNullable<T> {
  if (actual === null || actual === undefined) throw new Error(msg);
}

/** Asserts that `actual` contains `expected` as a substring. */
export function assertStringIncludes(
  actual: string,
  expected: string,
  msg?: string,
): void {
  if (!actual.includes(expected)) {
    throw new Error(
      msg ??
        `Expected string to contain ${JSON.stringify(expected)}: ${actual}`,
    );
  }
}

/** Constructor of an error, as accepted by {@linkcode assertThrows}. */
type ErrorClass<E extends Error = Error> = new (...args: any[]) => E;

function checkError<E extends Error>(
  error: unknown,
  ErrorType?: ErrorClass<E>,
  msgIncludes?: string,
): E {
  if (ErrorType && !(error instanceof ErrorType)) {
    throw new Error(
      `Expected error to be instance of "${ErrorType.name}", got "${
        (error as { constructor?: { name?: string } })?.constructor?.name
      }": ${String(error)}`,
    );
  }
  if (msgIncludes !== undefined) {
    const message = error instanceof Error ? error.message : String(error);
    if (!message.includes(msgIncludes)) {
      throw new Error(
        `Expected error message to contain ${JSON.stringify(
          msgIncludes,
        )}, got: ${message}`,
      );
    }
  }
  return error as E;
}

/**
 * Asserts that `fn` throws, and returns the thrown error.
 *
 * @param fn The function expected to throw.
 * @param ErrorType Constructor the error must be an instance of.
 * @param msgIncludes Substring the error message must contain.
 */
export function assertThrows<E extends Error = Error>(
  fn: () => unknown,
  ErrorType?: ErrorClass<E>,
  msgIncludes?: string,
  msg?: string,
): E {
  try {
    fn();
  } catch (error) {
    return checkError(error, ErrorType, msgIncludes);
  }
  throw new Error(msg ?? "Expected function to throw, but it did not");
}

/**
 * Asserts that `fn` rejects, and returns the rejection reason.
 *
 * @param fn The function expected to reject.
 * @param ErrorType Constructor the error must be an instance of.
 * @param msgIncludes Substring the error message must contain.
 */
export async function assertRejects<E extends Error = Error>(
  fn: () => Promise<unknown>,
  ErrorType?: ErrorClass<E>,
  msgIncludes?: string,
  msg?: string,
): Promise<E> {
  try {
    await fn();
  } catch (error) {
    return checkError(error, ErrorType, msgIncludes);
  }
  throw new Error(msg ?? "Expected function to reject, but it resolved");
}

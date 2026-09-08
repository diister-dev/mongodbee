/**
 * The suite's test-declaration surface.
 *
 * Wraps `node:test` so a case still receives a context object carrying its own
 * name — 357 cases pass `t.name` straight to `withDatabase()` to derive a
 * unique database, so dropping the parameter would have meant touching every
 * one of them.
 *
 * @module
 */

import { test as nodeTest } from "node:test";

/** What a test case receives. Named for the database each case isolates into. */
export interface TestContext {
  /** The case's own name. */
  readonly name: string;
}

/** A test case body. */
export type TestFn = (t: TestContext) => void | Promise<void>;

/** Object form of a test declaration. */
export interface TestDefinition {
  /** The case's name. */
  name: string;
  /** The case body. */
  fn: TestFn;
  /** Skip this case. */
  ignore?: boolean;
  /** Run only this case. */
  only?: boolean;
  /**
   * Accepted and ignored.
   *
   * These asked Deno's leak detector to stand down for a case that leaks on
   * purpose (concurrent retries leaving timers behind). `bun test` has no
   * equivalent detector, so there is nothing to stand down — the fields are
   * kept so the declarations still read as the deliberate opt-outs they are.
   * The leak checking itself is genuinely gone; `deno test --trace-leaks` was
   * the only thing that ever performed it.
   */
  sanitizeOps?: boolean;
  /** Accepted and ignored. See {@link TestDefinition.sanitizeOps}. */
  sanitizeResources?: boolean;
}

/**
 * Declares a test case.
 *
 * @example
 * ```typescript
 * test("Collection: inserts", async (t) => {
 *   await withDatabase(t.name, async (db) => { ... });
 * });
 * ```
 */
export function test(name: string, fn: TestFn): void;
export function test(definition: TestDefinition): void;
export function test(
  nameOrDefinition: string | TestDefinition,
  maybeFn?: TestFn,
): void {
  const { name, fn, ignore, only } =
    typeof nameOrDefinition === "string"
      ? {
          name: nameOrDefinition,
          fn: maybeFn as TestFn,
          ignore: false,
          only: false,
        }
      : nameOrDefinition;

  const run = ignore ? nodeTest.skip : only ? nodeTest.only : nodeTest;
  run(name, () => fn({ name }));
}

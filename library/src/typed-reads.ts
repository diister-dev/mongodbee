/**
 * Read helpers shared by `multiCollection` and the scoped views: the options
 * of a validated `find`, and the typing of `distinct`.
 *
 * @module
 */

import type * as m from "mongodb";
import type { FlatType } from "../types/flat.ts";
import type { WithReadPreferenceInput } from "./read-preference.ts";

/**
 * Options of a validated read. `projection` is left out: a projected
 * document fails validation and would be dropped without a word.
 */
export type ValidatedFindOptions = Omit<
  WithReadPreferenceInput<m.FindOptions>,
  "projection"
>;

/** A dot path of `X`, as `distinct` takes it. */
export type DistinctField<X> = keyof FlatType<X> & string;

/** The values `distinct` returns for `field`: an array contributes its items. */
export type DistinctValue<X, F extends DistinctField<X>> = Exclude<
  m.Flatten<FlatType<X>[F]>,
  undefined
>;

type Depth = [never, 0, 1, 2, 3, 4, 5, 6];

/**
 * A field of `X` or a dot path into its nested objects, as a projection
 * takes it: arrays are crossed without an index (`"lines.sku"`).
 */
export type ProjectionPath<X, D extends number = 7> = [D] extends [never]
  ? never
  : X extends readonly (infer I)[]
    ? ProjectionPath<I, D>
    : X extends Record<string, unknown>
      ? {
          [K in keyof X & string]:
            | K
            | (ProjectionPath<NonNullable<X[K]>, Depth[D]> extends infer S
                ? S extends string
                  ? `${K}.${S}`
                  : never
                : never);
        }[keyof X & string]
      : never;

type ShapeAt<V, P extends string> = V extends readonly (infer I)[]
  ? ShapeAt<I, P>[]
  : V extends Record<string, unknown>
    ? PathShape<V, P>
    : V;

type PathShape<X, P extends string> = P extends `${infer H}.${infer R}`
  ? H extends keyof X
    ? { [K in keyof Pick<X, H>]: ShapeAt<X[K], R> }
    : never
  : P extends keyof X
    ? Pick<X, P>
    : never;

type Intersect<U> = (U extends unknown ? (u: U) => void : never) extends (
  i: infer I,
) => void
  ? I
  : never;

type Merge<X> = X extends readonly (infer I)[]
  ? Merge<I>[]
  : X extends Date
    ? X
    : X extends Record<string, unknown>
      ? { [K in keyof X]: Merge<X[K]> }
      : X;

/** What a projection on `paths` returns: the nested shape of each path. */
export type ProjectedShape<X, P extends string> = Merge<
  Intersect<P extends unknown ? PathShape<X, P> : never>
>;

/**
 * Refuses a `projection` passed to a read that validates its documents, and
 * says what to use instead.
 */
export function refuseProjection(
  operation: string,
  options: object | undefined,
  instead: string,
): void {
  if (
    (options as { projection?: unknown } | undefined)?.projection === undefined
  )
    return;
  throw new Error(
    `${operation}: \`projection\` is not supported, a projected document fails validation and would be dropped; use ${instead}`,
  );
}

/**
 * Internal type helper for handling array flattening with dot notation
 *
 * @template T - The array element type
 * @template K - The current path prefix
 * @template MAX - Maximum recursion depth counter
 * @internal
 */
type HandleArray<T, K extends string = "", MAX extends keyof DECREMENT = 10> =
  MAX extends 0 ? never
    : T extends unknown[] ?
        | { path: K; value: T }
        | { path: `${K}.${number}`; value: T[number] }
        | { path: `${K}.$[]`; value: T[number] }
        | NodesType<T[number], `${K}.$[]`, DECREMENT[MAX]>
        | NodesType<T[number], `${K}.${number}`, DECREMENT[MAX]>
    : never;

/**
 * Internal type helper for handling object flattening with dot notation
 *
 * @template T - The object type
 * @template K - The current path prefix
 * @template MAX - Maximum recursion depth counter
 * @internal
 */
type HandleRecord<T, K extends string = "", MAX extends keyof DECREMENT = 10> =
  MAX extends 0 ? never
    : T extends Record<string, unknown> ?
        | (K extends "" ? never : { path: K; value: T })
        | {
          [k in keyof T]: NodesType<
            T[k],
            `${K}${K extends "" ? "" : "."}${k & string}`,
            DECREMENT[MAX]
          >;
        }[keyof T] extends infer U ? U extends undefined ? never : U : never
    : never;

/**
 * Recursive type that represents all possible paths and their values in a nested structure
 *
 * @template V - The value type to analyze
 * @template K - The current path prefix
 * @template MAX - Maximum recursion depth counter
 * @internal
 */
export type NodesType<
  V,
  K extends string = "",
  MAX extends keyof DECREMENT = 10,
> = MAX extends 0 ? never
  : V extends (infer T)[] ? HandleArray<T[], K, DECREMENT[MAX]>
  : V extends Record<string, unknown> ? HandleRecord<V, K, DECREMENT[MAX]>
  : { path: K; value: V };

/**
 * Extracts all possible dot notation keys from a type
 *
 * This type utility extracts all possible dot notation paths from a nested object type,
 * which are used when working with MongoDB update operations and queries.
 *
 * @template T - The object type to extract paths from
 */
export type FlatKey<T extends Record<string, unknown>> = NodesType<T> extends
  infer U ? U extends { path: infer P } ? P : never : never;

/**
 * Converts a nested type into a flattened type with dot notation paths as keys
 *
 * This type utility is essential for MongoDB dot notation operations, creating a
 * type mapping from dot notation paths to their respective value types.
 *
 * @template T - The object type to flatten
 * @example
 * ```typescript
 * type User = {
 *   name: string;
 *   address: {
 *     city: string;
 *     zipCode: number;
 *   };
 * };
 *
 * // Results in:
 * // {
 * //   "name": string;
 * //   "address": { city: string; zipCode: number };
 * //   "address.city": string;
 * //   "address.zipCode": number;
 * // }
 * type FlatUser = FlatType<User>;
 * ```
 */
export type FlatType<T> = T extends Record<string, unknown>
  ? NodesType<T> extends infer U ? {
      [k in FlatKey<T>]: U extends { path: k; value: infer V } ? V : never;
    }
  : never
  : never;

// System to prevent infinite recursion
// DECREMENT is a type that maps numbers to their decremented values
type DECREMENT = {
  0: 0;
  1: 0;
  2: 1;
  3: 2;
  4: 3;
  5: 4;
  6: 5;
  7: 6;
  8: 7;
  9: 8;
  10: 9;
  11: 10;
  12: 11;
  13: 12;
  14: 13;
  15: 14;
  16: 15;
  17: 16;
  18: 17;
  19: 18;
  20: 19;
  21: 20;
  22: 21;
  23: 22;
  24: 23;
  25: 24;
  26: 25;
  27: 26;
  28: 27;
  29: 28;
  30: 29;
};

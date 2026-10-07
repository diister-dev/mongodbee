import type { ComputedSourceRef, ComputedWhere } from "./computed.ts";
import type * as v from "./schema.ts";

type Scalar = string | number | boolean | bigint | Date | null | undefined;

type BsonValue = { readonly _bsontype: string };

export type FrozenDate = Omit<Date, Extract<keyof Date, `set${string}`>>;

export type ReaderArgument =
  | string
  | number
  | boolean
  | bigint
  | null
  | undefined
  | Date
  | BsonValue
  | readonly ReaderArgument[]
  | { readonly [key: string]: ReaderArgument };

export type DeepReadonly<T> = unknown extends T ? T : DeepReadonlyKnown<T>;

type DeepReadonlyKnown<T> = T extends Date
  ? FrozenDate
  : T extends Scalar | BsonValue | ((...args: never[]) => unknown)
    ? T
    : T extends ReadonlyMap<infer K, infer V>
      ? ReadonlyMap<K, DeepReadonly<V>>
      : T extends ReadonlySet<infer V>
        ? ReadonlySet<DeepReadonly<V>>
        : T extends readonly unknown[]
          ? { readonly [I in keyof T]: DeepReadonly<T[I]> }
          : { readonly [K in keyof T]: DeepReadonly<T[K]> };

export type Plain<T> = unknown extends T ? T : PlainKnown<T>;

type PlainKnown<T> = T extends Scalar | BsonValue
  ? T
  : T extends ((...args: never[]) => unknown) | Promise<unknown>
    ? never
    : T extends ReadonlyMap<infer K, infer V>
      ? ReadonlyMap<K, Plain<V>>
      : T extends ReadonlySet<infer V>
        ? ReadonlySet<Plain<V>>
        : T extends readonly unknown[]
          ? { [I in keyof T]: Plain<T[I]> }
          : { [K in keyof T]: Plain<T[K]> };

export type KeysOfAny<T> = T extends unknown ? keyof T & string : never;

export type SelectedRow<T, P extends string> = T extends unknown
  ? DeepReadonly<Pick<T, Extract<P | "_id", keyof T>>>
  : never;

export type ScopeArgs<S> = [S] extends [never] ? [] : [scope: S];

export type KeyArgs<K> = [K] extends [never] ? [] : [key: K];

export type KeyOf<V> =
  NonNullable<V> extends readonly (infer E)[] ? E : NonNullable<V>;

export interface ReaderQueryDescriptor {
  readonly source: ComputedSourceRef;
  readonly scope:
    | v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
    | undefined;
  readonly by?: string;
  readonly where: ComputedWhere;
  readonly select: readonly string[];
  readonly one: boolean;
}

declare const QUERY_ARGS: unique symbol;
declare const QUERY_VALUE: unique symbol;
declare const QUERY_SCOPE: unique symbol;
declare const QUERY_KEY: unique symbol;

export class ReaderQuery<A extends readonly unknown[], V, S, K> {
  declare readonly [QUERY_ARGS]?: A;
  declare readonly [QUERY_VALUE]?: V;
  declare readonly [QUERY_SCOPE]?: S;
  declare readonly [QUERY_KEY]?: K;
  readonly descriptor: ReaderQueryDescriptor;

  constructor(descriptor: ReaderQueryDescriptor) {
    this.descriptor = descriptor;
  }
}

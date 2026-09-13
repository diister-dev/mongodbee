import type * as m from "mongodb";
import type { CompositeIndexDescriptor } from "./indexes.ts";

export const PATH_SYMBOL: symbol = Symbol.for("mongodbee.path");

export interface FieldRef {
  readonly [PATH_SYMBOL]: string;
}

type Scalar = string | number | boolean | bigint | Date | null | undefined;

export type FieldProxy<T> = FieldRef &
  (T extends Scalar
    ? unknown
    : T extends ReadonlyArray<unknown>
      ? unknown
      : { readonly [K in keyof T]-?: FieldProxy<NonNullable<T[K]>> });

export type FieldsOf<T> = FieldProxy<T> & {
  readonly _id: FieldRef;
};

export function fieldPath(ref: FieldRef | string): string {
  return typeof ref === "string" ? ref : ref[PATH_SYMBOL];
}

export function createFieldProxy<T>(prefix = ""): FieldsOf<T> {
  return new Proxy(Object.create(null), {
    get(_target, property) {
      if (property === PATH_SYMBOL) return prefix;
      if (typeof property === "symbol") return undefined;
      const next = prefix ? `${prefix}.${property}` : property;
      return createFieldProxy(next);
    },
  }) as FieldsOf<T>;
}

export interface KeyPart {
  ref: FieldRef | string;
  direction: 1 | -1;
}

export type KeyInput = FieldRef | string | KeyPart;

function toKeyPart(input: KeyInput): KeyPart {
  if (typeof input === "string") return { ref: input, direction: 1 };
  if ("direction" in input && "ref" in input) return input as KeyPart;
  return { ref: input as FieldRef, direction: 1 };
}

export function desc(ref: FieldRef | string): KeyPart {
  return { ref, direction: -1 };
}

export function asc(ref: FieldRef | string): KeyPart {
  return { ref, direction: 1 };
}

export class IndexDeclaration {
  #unique: boolean;
  #parts: KeyPart[];
  #name?: string;
  #filters: m.Document[] = [];
  #global = false;
  #collation?: m.CollationOptions;
  #expireAfterSeconds?: number;

  constructor(unique: boolean, keys: KeyInput[]) {
    if (keys.length === 0) {
      throw new Error("an index declaration needs at least one key");
    }
    this.#unique = unique;
    this.#parts = keys.map(toKeyPart);
  }

  named(name: string): this {
    this.#name = name;
    return this;
  }

  where(field: FieldRef | string, value: unknown): this;
  where(filter: m.Document): this;
  where(fieldOrFilter: FieldRef | string | m.Document, value?: unknown): this {
    if (
      value === undefined &&
      typeof fieldOrFilter === "object" &&
      !(PATH_SYMBOL in (fieldOrFilter as object))
    ) {
      this.#filters.push(fieldOrFilter as m.Document);
      return this;
    }
    this.#filters.push({
      [fieldPath(fieldOrFilter as FieldRef | string)]: value,
    });
    return this;
  }

  exists(field: FieldRef | string): this {
    this.#filters.push({ [fieldPath(field)]: { $exists: true } });
    return this;
  }

  acrossScopes(): this {
    this.#global = true;
    return this;
  }

  insensitive(): this {
    this.#collation = { locale: "en", strength: 2 };
    return this;
  }

  collated(collation: m.CollationOptions): this {
    this.#collation = collation;
    return this;
  }

  expiresAfter(seconds: number): this {
    this.#expireAfterSeconds = seconds;
    return this;
  }

  toDescriptor(): CompositeIndexDescriptor {
    const key: Record<string, 1 | -1> = {};
    for (const part of this.#parts) {
      const path = fieldPath(part.ref);
      if (!path) {
        throw new Error("an index key must reference a field, not the root");
      }
      key[path] = part.direction;
    }

    const descriptor: CompositeIndexDescriptor = { key };
    if (this.#name) descriptor.name = this.#name;
    if (this.#unique) descriptor.unique = true;
    if (this.#global) descriptor.global = true;
    if (this.#collation) descriptor.collation = this.#collation;
    if (this.#expireAfterSeconds !== undefined) {
      descriptor.expireAfterSeconds = this.#expireAfterSeconds;
    }
    if (this.#filters.length === 1) {
      descriptor.partialFilterExpression = this.#filters[0];
    } else if (this.#filters.length > 1) {
      descriptor.partialFilterExpression = { $and: this.#filters };
    }
    return descriptor;
  }
}

export function index(...keys: KeyInput[]): IndexDeclaration {
  return new IndexDeclaration(false, keys);
}

export function unique(...keys: KeyInput[]): IndexDeclaration {
  return new IndexDeclaration(true, keys);
}

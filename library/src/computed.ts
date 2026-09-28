import * as v from "./schema.ts";
import {
  createFieldProxy,
  type FieldRef,
  type FieldsOf,
  fieldPath,
} from "./index-builder.ts";
import { findSchemaAtPath } from "./schema-navigator.ts";
import { isRecord, isSchema } from "./utils/guards.ts";
import {
  fieldsOf,
  type FieldsOfInput,
  type TypeInput,
} from "./type-definition.ts";

type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

import { COMPUTED_ROOT } from "./computed-guard.ts";

export {
  COMPUTED_ROOT,
  ComputedFieldWriteError,
  refuseComputedWrite,
} from "./computed-guard.ts";

export type ComputedLiteral = string | number | boolean | null;

export type ComputedWhere = Readonly<
  Record<string, ComputedLiteral | readonly ComputedLiteral[]>
>;

export interface ComputedSourceRef {
  readonly model?: string;
  readonly type: string;
}

export type ComputedAggregate =
  | {
      readonly kind: "collect";
      readonly path: string;
      readonly distinct: boolean;
      readonly maxEntries?: number;
    }
  | { readonly kind: "count" };

export interface ComputedThrough {
  readonly source: ComputedSourceRef;
  readonly via: string;
  readonly where: ComputedWhere;
}

export interface ComputedDescriptor {
  readonly source: ComputedSourceRef;
  readonly by: string;
  readonly sameScope: boolean;
  readonly where: ComputedWhere;
  readonly through?: ComputedThrough;
  readonly aggregate: ComputedAggregate;
}

export class ComputedDefinitionError extends Error {
  override readonly name = "ComputedDefinitionError";
}

export type SourceDocument<I extends TypeInput> =
  FieldsOfInput<I> extends infer E extends v.ObjectEntries
    ? v.InferOutput<v.ObjectSchema<E, undefined>> & { _id: string }
    : never;

interface ModelLike {
  readonly name: string;
  readonly schema: Record<string, TypeInput>;
}

interface ResolvedSource {
  readonly ref: ComputedSourceRef;
  readonly entries: Record<string, AnySchema>;
}

interface BuilderState {
  readonly near: ResolvedSource;
  readonly by?: string;
  readonly sameScope: boolean;
  readonly where: Record<string, ComputedLiteral | readonly ComputedLiteral[]>;
  readonly through?: {
    readonly far: ResolvedSource;
    readonly via: string;
    readonly where: Record<
      string,
      ComputedLiteral | readonly ComputedLiteral[]
    >;
  };
}

declare const DECLARED_VALUE: unique symbol;

export class ComputedDeclaration<V> {
  declare readonly [DECLARED_VALUE]?: V;
  readonly descriptor: ComputedDescriptor;
  readonly valueSchema: AnySchema;

  constructor(descriptor: ComputedDescriptor, valueSchema: AnySchema) {
    this.descriptor = descriptor;
    this.valueSchema = valueSchema;
  }
}

export type DeclaredValue<D> =
  D extends ComputedDeclaration<infer V> ? V : never;

export type ComputedDeclarations = Readonly<
  Record<string, ComputedDeclaration<unknown>>
>;

const COUNT_SCHEMA: AnySchema = v.pipe(v.number(), v.integer(), v.minValue(0));

function isLiteral(value: unknown): value is ComputedLiteral {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  );
}

function whereEntry(
  value: unknown,
): ComputedLiteral | readonly ComputedLiteral[] | undefined {
  if (isLiteral(value)) return value;
  if (!Array.isArray(value)) return undefined;
  const literals: ComputedLiteral[] = [];
  for (const item of value) {
    if (!isLiteral(item)) return undefined;
    literals.push(item);
  }
  return literals;
}

function isModel(value: unknown): value is ModelLike {
  return (
    isRecord(value) &&
    typeof value.name === "string" &&
    isRecord(value.schema) &&
    !("entries" in value)
  );
}

function resolveSource(
  first: string | ModelLike,
  second: TypeInput | string,
): ResolvedSource {
  if (isModel(first)) {
    if (typeof second !== "string") {
      throw new ComputedDefinitionError(
        `from(model "${first.name}", type) takes the type name as its second argument`,
      );
    }
    const input = first.schema[second];
    if (input === undefined)
      throw new ComputedDefinitionError(
        `model "${first.name}" has no type "${second}"`,
      );
    return {
      ref: { model: first.name, type: second },
      entries: fieldsOf(input),
    };
  }
  if (typeof second === "string") {
    throw new ComputedDefinitionError(
      `from("${first}", fields) takes the source fields as its second argument`,
    );
  }
  return { ref: { type: first }, entries: fieldsOf(second) };
}

function schemaAt(source: ResolvedSource, path: string): AnySchema {
  if (path === "_id") return v.string();
  const found = findSchemaAtPath(v.object(source.entries), path.split("."));
  if (found === undefined || !("kind" in found) || found.kind !== "schema") {
    throw new ComputedDefinitionError(
      `"${path}" does not exist on source type "${source.ref.type}"`,
    );
  }
  return unwrapOptional(found);
}

function unwrapOptional(schema: AnySchema): AnySchema {
  if (
    (schema.type === "optional" ||
      schema.type === "nullish" ||
      schema.type === "exact_optional") &&
    "wrapped" in schema &&
    isSchema(schema.wrapped)
  ) {
    return unwrapOptional(schema.wrapped);
  }
  return schema;
}

function pathOf(source: ResolvedSource, ref: FieldRef<unknown>): string {
  const path = fieldPath(ref);
  schemaAt(source, path);
  return path;
}

function freezeWhere(
  where: Record<string, ComputedLiteral | readonly ComputedLiteral[]>,
): ComputedWhere {
  return Object.freeze(
    Object.fromEntries(
      Object.entries(where).map(([path, value]) => [
        path,
        Array.isArray(value) ? Object.freeze([...value]) : value,
      ]),
    ),
  );
}

export class ComputedFrom<T> {
  readonly #state: BuilderState;

  constructor(state: BuilderState) {
    this.#state = state;
  }

  #current(): ResolvedSource {
    return this.#state.through?.far ?? this.#state.near;
  }

  by(pick: (source: FieldsOf<T>) => FieldRef<unknown>): ComputedFrom<T> {
    if (this.#state.through)
      throw new ComputedDefinitionError(
        "by() names the subject on the near source; declare it before through()",
      );
    return new ComputedFrom<T>({
      ...this.#state,
      by: pathOf(this.#state.near, pick(createFieldProxy<T>())),
    });
  }

  sameScope(): ComputedFrom<T> {
    return new ComputedFrom<T>({ ...this.#state, sameScope: true });
  }

  where<V>(
    pick: (source: FieldsOf<T>) => readonly [FieldRef<V>, V | readonly V[]],
  ): ComputedFrom<T> {
    const [ref, value] = pick(createFieldProxy<T>());
    const current = this.#current();
    const path = pathOf(current, ref);
    const entry = whereEntry(value);
    if (entry === undefined) {
      throw new ComputedDefinitionError(
        `where("${path}") takes string, number, boolean or null values only`,
      );
    }
    if (this.#state.through) {
      return new ComputedFrom<T>({
        ...this.#state,
        through: {
          ...this.#state.through,
          where: { ...this.#state.through.where, [path]: entry },
        },
      });
    }
    return new ComputedFrom<T>({
      ...this.#state,
      where: { ...this.#state.where, [path]: entry },
    });
  }

  through<I extends TypeInput>(
    type: string,
    source: I,
    via: (near: FieldsOf<T>) => FieldRef<unknown>,
  ): ComputedFrom<SourceDocument<I>>;
  through<M extends ModelLike, K extends keyof M["schema"] & string>(
    model: M,
    type: K,
    via: (near: FieldsOf<T>) => FieldRef<unknown>,
  ): ComputedFrom<SourceDocument<M["schema"][K]>>;
  through(
    first: string | ModelLike,
    second: TypeInput | string,
    via: (near: FieldsOf<T>) => FieldRef<unknown>,
  ): ComputedFrom<unknown> {
    if (this.#state.through)
      throw new ComputedDefinitionError(
        "a computed field follows at most one extra hop",
      );
    const far = resolveSource(first, second);
    const viaPath = pathOf(this.#state.near, via(createFieldProxy<T>()));
    return new ComputedFrom<unknown>({
      ...this.#state,
      through: { far, via: viaPath, where: {} },
    });
  }

  collect<V>(pick: (source: FieldsOf<T>) => FieldRef<V>): ComputedCollect<V> {
    const current = this.#current();
    const path = fieldPath(pick(createFieldProxy<T>()));
    const entry = schemaAt(current, path);
    return new ComputedCollect<V>(
      this.#finish({ kind: "collect", path, distinct: false }),
      entry,
      undefined,
    );
  }

  count(): ComputedDeclaration<number> {
    return new ComputedDeclaration<number>(
      this.#finish({ kind: "count" }),
      COUNT_SCHEMA,
    );
  }

  #finish(aggregate: ComputedAggregate): ComputedDescriptor {
    const { near, by, sameScope, where, through } = this.#state;
    if (by === undefined)
      throw new ComputedDefinitionError(
        `a computed field over "${near.ref.type}" needs by() to name its subject`,
      );
    return Object.freeze({
      source: Object.freeze({ ...near.ref }),
      by,
      sameScope,
      where: freezeWhere(where),
      ...(through && {
        through: Object.freeze({
          source: Object.freeze({ ...through.far.ref }),
          via: through.via,
          where: freezeWhere(through.where),
        }),
      }),
      aggregate: Object.freeze(aggregate),
    });
  }
}

export class ComputedCollect<V> extends ComputedDeclaration<V[]> {
  readonly #entry: AnySchema;

  constructor(
    descriptor: ComputedDescriptor,
    entry: AnySchema,
    maxEntries: number | undefined,
  ) {
    const array = v.array(entry);
    super(
      descriptor,
      maxEntries === undefined ? array : v.pipe(array, v.maxLength(maxEntries)),
    );
    this.#entry = entry;
  }

  #aggregate(): Extract<ComputedAggregate, { kind: "collect" }> {
    const { aggregate } = this.descriptor;
    if (aggregate.kind !== "collect") {
      throw new ComputedDefinitionError(
        `a collect declaration carries a "${aggregate.kind}" aggregate`,
      );
    }
    return aggregate;
  }

  distinct(): ComputedCollect<V> {
    const aggregate = this.#aggregate();
    return new ComputedCollect<V>(
      Object.freeze({
        ...this.descriptor,
        aggregate: Object.freeze({ ...aggregate, distinct: true }),
      }),
      this.#entry,
      aggregate.maxEntries,
    );
  }

  maxEntries(limit: number): ComputedCollect<V> {
    if (!Number.isInteger(limit) || limit <= 0)
      throw new ComputedDefinitionError(
        `maxEntries takes a positive integer, got ${limit}`,
      );
    const aggregate = this.#aggregate();
    return new ComputedCollect<V>(
      Object.freeze({
        ...this.descriptor,
        aggregate: Object.freeze({ ...aggregate, maxEntries: limit }),
      }),
      this.#entry,
      limit,
    );
  }
}

export function from<I extends TypeInput>(
  type: string,
  source: I,
): ComputedFrom<SourceDocument<I>>;
export function from<M extends ModelLike, K extends keyof M["schema"] & string>(
  model: M,
  type: K,
): ComputedFrom<SourceDocument<M["schema"][K]>>;
export function from(
  first: string | ModelLike,
  second: TypeInput | string,
): ComputedFrom<unknown> {
  return new ComputedFrom<unknown>({
    near: resolveSource(first, second),
    sameScope: false,
    where: {},
  });
}

export type ComputedEntries<C extends ComputedDeclarations> = {
  readonly [K in keyof C]: v.OptionalSchema<
    v.GenericSchema<DeclaredValue<C[K]>>,
    undefined
  >;
};

export function computedRootSchema(
  declarations: ComputedDeclarations,
): AnySchema {
  return v.optional(
    v.object(
      Object.fromEntries(
        Object.entries(declarations).map(([name, declaration]) => [
          name,
          v.optional(declaration.valueSchema),
        ]),
      ),
    ),
  );
}

export function computedDescriptors(
  declarations: ComputedDeclarations,
): Readonly<Record<string, ComputedDescriptor>> {
  return Object.freeze(
    Object.fromEntries(
      Object.entries(declarations).map(([name, declaration]) => [
        name,
        declaration.descriptor,
      ]),
    ),
  );
}

export function assertComputedDeclarations(
  entries: Record<string, unknown>,
  declarations: ComputedDeclarations,
): void {
  if (Object.hasOwn(entries, COMPUTED_ROOT)) {
    throw new ComputedDefinitionError(
      `"${COMPUTED_ROOT}" is generated from the computed declarations; do not declare it in the schema`,
    );
  }
  for (const [name, declaration] of Object.entries(declarations)) {
    if (!(declaration instanceof ComputedDeclaration)) {
      throw new ComputedDefinitionError(
        `computed field "${name}" must be built with from(...).collect() or from(...).count()`,
      );
    }
    if (!/^[A-Za-z][A-Za-z0-9]*$/.test(name)) {
      throw new ComputedDefinitionError(
        `computed field "${name}" must be an identifier: letters and digits, starting with a letter`,
      );
    }
  }
}

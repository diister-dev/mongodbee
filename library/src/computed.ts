import * as v from "./schema.ts";
import {
  createFieldProxy,
  type FieldRef,
  type FieldsOf,
  fieldPath,
} from "./index-builder.ts";
import {
  findSchemaAtPath,
  findSchemasAtFieldPath,
} from "./schema-navigator.ts";
import { isRecord, isSchema } from "./utils/guards.ts";
import {
  fieldsOf,
  type FieldsOfInput,
  type TypeInput,
} from "./type-definition.ts";
import {
  type KeyArgs,
  type KeyOf,
  type KeysOfAny,
  ReaderQuery,
  type ReaderQueryDescriptor,
  type ScopeArgs,
  type SelectedRow,
} from "./reader-query.ts";

type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

import { COMPUTED_REVISION, COMPUTED_ROOT } from "./computed-guard.ts";

export {
  COMPUTED_REVISION,
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

export interface ScopedModel<M extends ModelLike, S extends AnySchema> {
  readonly name: M["name"];
  readonly schema: M["schema"];
  readonly scope: S;
}

export function scoped<M extends ModelLike, S extends AnySchema>(
  model: M,
  scope: S,
): ScopedModel<M, S> {
  return Object.freeze({ name: model.name, schema: model.schema, scope });
}

interface ResolvedSource {
  readonly ref: ComputedSourceRef;
  readonly entries: Record<string, AnySchema>;
  readonly scope: AnySchema | undefined;
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
      scope:
        "scope" in first && isSchema(first.scope) ? first.scope : undefined,
    };
  }
  if (typeof second === "string") {
    throw new ComputedDefinitionError(
      `from("${first}", fields) takes the source fields as its second argument`,
    );
  }
  return { ref: { type: first }, entries: fieldsOf(second), scope: undefined };
}

function schemaAt(source: ResolvedSource, path: string): AnySchema {
  if (path === "_id") return v.string();
  const root = v.object(source.entries);
  const found = findSchemaAtPath(root, path.split("."));
  if (found !== undefined && "kind" in found && found.kind === "schema") {
    return unwrapOptional(found);
  }
  const alternatives = findSchemasAtFieldPath(root, path.split(".")).map(
    unwrapOptional,
  );
  if (alternatives.length === 0) {
    throw new ComputedDefinitionError(
      `"${path}" does not exist on source type "${source.ref.type}"`,
    );
  }
  return alternatives.length === 1 ? alternatives[0]! : v.union(alternatives);
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

type QueryArgs<S, K> = [...ScopeArgs<S>, ...KeyArgs<K>];

declare const QUERYABLE: unique symbol;

export class ComputedFrom<T, S = never, K = never, Q extends boolean = true> {
  declare readonly [QUERYABLE]?: Q;
  readonly #state: BuilderState;

  constructor(state: BuilderState) {
    this.#state = state;
  }

  #current(): ResolvedSource {
    return this.#state.through?.far ?? this.#state.near;
  }

  by<V>(
    pick: (source: FieldsOf<T>) => FieldRef<V>,
  ): ComputedFrom<T, S, KeyOf<V>, Q> {
    if (this.#state.through)
      throw new ComputedDefinitionError(
        "by() names the subject on the near source; declare it before through()",
      );
    return new ComputedFrom<T, S, KeyOf<V>, Q>({
      ...this.#state,
      by: pathOf(this.#state.near, pick(createFieldProxy<T>())),
    });
  }

  sameScope(): ComputedFrom<T, S, K, false> {
    return new ComputedFrom<T, S, K, false>({
      ...this.#state,
      sameScope: true,
    });
  }

  one(this: ComputedFrom<T, S, K, true>): ReaderOne<T, S, K> {
    return new ReaderOne<T, S, K>((fields) => this.#query(fields, true));
  }

  select<const P extends KeysOfAny<T>>(
    this: ComputedFrom<T, S, K, true>,
    fields: readonly P[],
  ): ReaderQuery<QueryArgs<S, K>, readonly SelectedRow<T, P>[], S, K> {
    return new ReaderQuery(this.#query(fields, false));
  }

  #query(fields: readonly string[], one: boolean) {
    const { near, by, where, through, sameScope } = this.#state;
    if (through || sameScope)
      throw new ComputedDefinitionError(
        "a reader reads one source: through() and sameScope() belong to computed fields",
      );
    if (fields.length === 0)
      throw new ComputedDefinitionError(
        `a reader over "${near.ref.type}" selects at least one field`,
      );
    for (const field of fields) {
      if (field.includes("."))
        throw new ComputedDefinitionError(
          `select() takes top-level fields, got "${field}"`,
        );
      schemaAt(near, field);
    }
    return Object.freeze({
      source: Object.freeze({ ...near.ref }),
      scope: near.scope,
      ...(by !== undefined && { by }),
      where: freezeWhere(where),
      select: Object.freeze([...new Set(fields)]),
      one,
    });
  }

  where<V, const W extends (V | null) & ComputedLiteral>(
    pick: (source: FieldsOf<T>) => readonly [FieldRef<V>, W | readonly W[]],
  ): ComputedFrom<T, S, K, Q> {
    const [ref, value] = pick(createFieldProxy<T>());
    const current = this.#current();
    const path = pathOf(current, ref);
    const entry = whereEntry(value);
    if (entry === undefined) {
      throw new ComputedDefinitionError(
        `where("${path}") takes string, number, boolean or null values only`,
      );
    }
    const declared = this.#state.through?.where ?? this.#state.where;
    if (Object.hasOwn(declared, path)) {
      throw new ComputedDefinitionError(
        `where("${path}") is declared twice; give one where() the list of accepted values`,
      );
    }
    if (this.#state.through) {
      return new ComputedFrom<T, S, K, Q>({
        ...this.#state,
        through: {
          ...this.#state.through,
          where: { ...this.#state.through.where, [path]: entry },
        },
      });
    }
    return new ComputedFrom<T, S, K, Q>({
      ...this.#state,
      where: { ...this.#state.where, [path]: entry },
    });
  }

  through<I extends TypeInput>(
    type: string,
    source: I,
    via: (near: FieldsOf<T>) => FieldRef<unknown>,
  ): ComputedFrom<SourceDocument<I>, S, K, false>;
  through<M extends ModelLike, N extends keyof M["schema"] & string>(
    model: M,
    type: N,
    via: (near: FieldsOf<T>) => FieldRef<unknown>,
  ): ComputedFrom<SourceDocument<M["schema"][N]>, S, K, false>;
  through(
    first: string | ModelLike,
    second: TypeInput | string,
    via: (near: FieldsOf<T>) => FieldRef<unknown>,
  ): ComputedFrom<unknown, S, K, false> {
    if (this.#state.through)
      throw new ComputedDefinitionError(
        "a computed field follows at most one extra hop",
      );
    const far = resolveSource(first, second);
    const viaPath = pathOf(this.#state.near, via(createFieldProxy<T>()));
    return new ComputedFrom<unknown, S, K, false>({
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

export class ReaderOne<T, S, K> {
  readonly #finish: (fields: readonly string[]) => ReaderQueryDescriptor;

  constructor(finish: (fields: readonly string[]) => ReaderQueryDescriptor) {
    this.#finish = finish;
  }

  select<const P extends KeysOfAny<T>>(
    fields: readonly P[],
  ): ReaderQuery<QueryArgs<S, K>, SelectedRow<T, P> | null, S, K> {
    return new ReaderQuery(this.#finish(fields));
  }
}

export function from<
  M extends ScopedModel<ModelLike, AnySchema>,
  N extends keyof M["schema"] & string,
>(
  model: M,
  type: N,
): ComputedFrom<SourceDocument<M["schema"][N]>, v.InferOutput<M["scope"]>>;
export function from<I extends TypeInput>(
  type: string,
  source: I,
): ComputedFrom<SourceDocument<I>>;
export function from<M extends ModelLike, N extends keyof M["schema"] & string>(
  model: M,
  type: N,
): ComputedFrom<SourceDocument<M["schema"][N]>>;
export function from(
  first: string | ModelLike,
  second: TypeInput | string,
): ComputedFrom<unknown, unknown> | ComputedFrom<unknown> {
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
} & {
  readonly [COMPUTED_REVISION]: v.OptionalSchema<
    v.GenericSchema<number>,
    undefined
  >;
};

const REVISION_SCHEMA: AnySchema = v.pipe(
  v.number(),
  v.integer(),
  v.minValue(0),
);

export function computedRootSchema(
  declarations: ComputedDeclarations,
): AnySchema {
  return v.optional(
    v.object({
      ...Object.fromEntries(
        Object.entries(declarations).map(([name, declaration]) => [
          name,
          v.optional(declaration.valueSchema),
        ]),
      ),
      [COMPUTED_REVISION]: v.optional(REVISION_SCHEMA),
    }),
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

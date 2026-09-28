import * as v from "./schema.ts";
import {
  createFieldProxy,
  type FieldsOf,
  IndexDeclaration,
} from "./index-builder.ts";
import type { CompositeIndexDescriptor } from "./indexes.ts";
import {
  assertComputedDeclarations,
  COMPUTED_ROOT,
  type ComputedDeclarations,
  type ComputedDescriptor,
  computedDescriptors,
  type ComputedEntries,
  computedRootSchema,
} from "./computed.ts";

type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

export type ObjectLikeSchema = AnySchema & {
  entries: Record<string, AnySchema>;
};

export type IndexBuild<TOutput> = (
  f: FieldsOf<TOutput>,
) => ReadonlyArray<IndexDeclaration | CompositeIndexDescriptor>;

const BRAND: symbol = Symbol.for("mongodbee.type-definition");

const NO_COMPUTED: ComputedDeclarations = Object.freeze({});

export class TypeDefinition<S extends ObjectLikeSchema = ObjectLikeSchema> {
  readonly schema: S;
  readonly indexes: readonly CompositeIndexDescriptor[];
  readonly computed: Readonly<Record<string, ComputedDescriptor>>;
  readonly computedDeclarations: ComputedDeclarations;

  constructor(
    schema: S,
    indexes: readonly CompositeIndexDescriptor[],
    computedDeclarations: ComputedDeclarations = NO_COMPUTED,
  ) {
    this.schema = schema;
    this.indexes = indexes;
    this.computedDeclarations = computedDeclarations;
    this.computed = computedDescriptors(computedDeclarations);
    Object.defineProperty(this, BRAND, { value: true, enumerable: false });
  }

  get entries(): S["entries"] {
    return this.schema.entries;
  }
}

export type WithComputed<
  S extends ObjectLikeSchema,
  C extends ComputedDeclarations,
> = keyof C extends never
  ? S
  : v.ObjectSchema<
      S["entries"] & {
        readonly [COMPUTED_ROOT]: v.OptionalSchema<
          v.ObjectSchema<ComputedEntries<C>, undefined>,
          undefined
        >;
      },
      undefined
    >;

export interface TypeDefinitionInput<
  S extends ObjectLikeSchema,
  C extends ComputedDeclarations = Record<never, never>,
> {
  schema: S;
  computed?: C;
  indexes?:
    | IndexBuild<v.InferOutput<WithComputed<S, C>>>
    | ReadonlyArray<IndexDeclaration | CompositeIndexDescriptor>;
}

function withEntries(
  schema: ObjectLikeSchema,
  entries: Record<string, AnySchema>,
): ObjectLikeSchema {
  const shaped = schema as ObjectLikeSchema & {
    rest?: AnySchema;
    pipe?: readonly unknown[];
  };
  const rebuilt =
    schema.type === "loose_object"
      ? v.looseObject(entries)
      : schema.type === "strict_object"
        ? v.strictObject(entries)
        : schema.type === "object_with_rest" && shaped.rest
          ? v.objectWithRest(entries, shaped.rest)
          : v.object(entries);
  if (!shaped.pipe || shaped.pipe.length < 2)
    return rebuilt as ObjectLikeSchema;
  return v.pipe(
    rebuilt,
    ...(shaped.pipe.slice(1) as [never]),
  ) as unknown as ObjectLikeSchema;
}

function assertKeyPaths(
  entries: Record<string, unknown>,
  descriptors: readonly CompositeIndexDescriptor[],
): void {
  for (const descriptor of descriptors) {
    const paths = Object.keys(descriptor.key);
    if (paths.length === 0) {
      throw new Error("defineType: an index needs at least one key");
    }
    for (const path of paths) {
      const root = path.split(".")[0];
      if (root === "_id" || root === "_type" || root === "_scope") continue;
      if (!Object.hasOwn(entries, root)) {
        throw new Error(
          `defineType: key path "${path}" does not exist in the schema`,
        );
      }
    }
  }
}

export function defineType<
  const S extends ObjectLikeSchema,
  const C extends ComputedDeclarations = Record<never, never>,
>(input: TypeDefinitionInput<S, C>): TypeDefinition<WithComputed<S, C>> {
  const declarations: ComputedDeclarations = input.computed ?? NO_COMPUTED;
  assertComputedDeclarations(input.schema.entries, declarations);
  const schema =
    Object.keys(declarations).length === 0
      ? input.schema
      : withEntries(input.schema, {
          ...input.schema.entries,
          [COMPUTED_ROOT]: computedRootSchema(declarations),
        });
  const declared = input.indexes ?? [];
  const raw =
    typeof declared === "function"
      ? declared(createFieldProxy<v.InferOutput<WithComputed<S, C>>>())
      : declared;
  const descriptors = raw.map((item) =>
    item instanceof IndexDeclaration ? item.toDescriptor() : item,
  );
  assertKeyPaths(schema.entries, descriptors);
  return new TypeDefinition(
    schema as WithComputed<S, C>,
    descriptors,
    declarations,
  );
}

export function isTypeDefinition(value: unknown): value is TypeDefinition {
  if (value instanceof TypeDefinition) return true;
  return (
    typeof value === "object" &&
    value !== null &&
    (value as Record<PropertyKey, unknown>)[BRAND] === true
  );
}

export type TypeInput = Record<string, AnySchema> | TypeDefinition;

export type FieldsOfInput<I> =
  I extends TypeDefinition<infer S> ? S["entries"] : I;

export type ResolveTypes<M> = {
  [K in keyof M]: FieldsOfInput<M[K]>;
};

export function fieldsOf<I extends TypeInput>(input: I): FieldsOfInput<I> {
  return (
    isTypeDefinition(input) ? input.schema.entries : input
  ) as FieldsOfInput<I>;
}

export function indexesOf(
  input: TypeInput,
): readonly CompositeIndexDescriptor[] {
  return isTypeDefinition(input) ? input.indexes : [];
}

export interface NormalizedTypes<M> {
  fields: ResolveTypes<M>;
  indexes: Record<string, readonly CompositeIndexDescriptor[]>;
}

export function normalizeTypes<M extends Record<string, TypeInput>>(
  types: M,
): NormalizedTypes<M> {
  const fields: Record<string, unknown> = {};
  const indexes: Record<string, readonly CompositeIndexDescriptor[]> = {};
  for (const [name, input] of Object.entries(types)) {
    fields[name] = fieldsOf(input);
    const declared = indexesOf(input);
    if (declared.length > 0) indexes[name] = declared;
  }
  return { fields: fields as ResolveTypes<M>, indexes };
}

/**
 * Rebuild a type source with fields added or replaced, keeping what it is: a
 * plain field map stays one, a `defineType` keeps its indexes.
 *
 * Spreading the source instead is the trap: a {@link TypeDefinition} spreads to
 * its own properties, so the result declares fields named `schema` and
 * `indexes` and drops the real ones.
 *
 * Goes through {@link defineType}, so dropping a field an index points at is
 * refused rather than carried over as a dangling index.
 *
 * @example
 * ```typescript
 * const tightened = withFields(parent.schemas.collections["+jobs"], {
 *   content: v.union(variants),
 * });
 * ```
 */
export function withFields<I extends TypeInput>(
  source: I,
  fields: Record<string, AnySchema>,
): TypeInput {
  const { [COMPUTED_ROOT]: _generated, ...own } = fieldsOf(source) as Record<
    string,
    AnySchema
  >;
  const merged: Record<string, AnySchema> = { ...own, ...fields };
  if (!isTypeDefinition(source)) return merged;
  return defineType({
    schema: v.object(merged),
    indexes: source.indexes,
    computed: source.computedDeclarations,
  });
}

export function computedOf(
  input: TypeInput,
): Readonly<Record<string, ComputedDescriptor>> {
  return isTypeDefinition(input) ? input.computed : {};
}

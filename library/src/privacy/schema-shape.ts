export type SchemaNode = Record<string, unknown>;

export const WRAPPER_TYPES: ReadonlySet<string> = new Set([
  "optional",
  "nullable",
  "nullish",
  "non_optional",
  "non_nullable",
  "non_nullish",
  "undefinedable",
  "exact_optional",
]);

const OPTIONAL_WRAPPERS: ReadonlySet<string> = new Set([
  "optional",
  "nullish",
  "undefinedable",
  "exact_optional",
]);

const NULLABLE_WRAPPERS: ReadonlySet<string> = new Set(["nullable", "nullish"]);

export const PLAIN_OBJECT_TYPES: ReadonlySet<string> = new Set([
  "object",
  "loose_object",
  "strict_object",
]);

export const OBJECT_TYPES: ReadonlySet<string> = new Set([
  ...PLAIN_OBJECT_TYPES,
  "object_with_rest",
]);

export const TUPLE_TYPES: ReadonlySet<string> = new Set([
  "tuple",
  "loose_tuple",
  "strict_tuple",
  "tuple_with_rest",
]);

export const UNION_TYPES: ReadonlySet<string> = new Set(["union", "variant"]);

export interface Unwrapped {
  readonly schema: SchemaNode;
  readonly optional: boolean;
  readonly nullable: boolean;
}

export function unwrap(schema: unknown): Unwrapped {
  let current = schema as SchemaNode;
  const chain: string[] = [];
  while (current && WRAPPER_TYPES.has(current.type as string)) {
    chain.push(current.type as string);
    current = current.wrapped as SchemaNode;
  }
  let optional = false;
  let nullable = false;
  for (const type of chain.reverse()) {
    if (OPTIONAL_WRAPPERS.has(type)) optional = true;
    if (NULLABLE_WRAPPERS.has(type)) nullable = true;
    if (type === "non_optional" || type === "non_nullish") optional = false;
    if (type === "non_nullable" || type === "non_nullish") nullable = false;
  }
  return { schema: current, optional, nullable };
}

export function unwrapSchema(schema: unknown): SchemaNode | undefined {
  return unwrap(schema).schema;
}

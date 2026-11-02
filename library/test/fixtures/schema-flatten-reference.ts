/**
 * The schema simplification and flattening as they stood before the
 * single-pass rewrite, kept verbatim as the oracle the equivalence test
 * compares the current implementation against.
 *
 * @module
 */

import { INDEX_SYMBOL, indexMetadataOf } from "../../src/indexes.ts";
import { PRIVACY_SYMBOL } from "../../src/privacy/metadata.ts";
import { isTypeDefinition } from "../../src/type-definition.ts";

/**
 * Properties to remove from Valibot schemas during simplification
 */
const CLEANUP_PROPERTIES = [
  "~standard",
  "async",
  "expects",
  "message",
  "default",
];

/**
 * Schema kinds to ignore during simplification
 */
const CLEANUP_KINDS = ["transformation", "metadata"];

/**
 * Type-specific handlers for simplifying Valibot schemas
 */
const simplifyHandlers: Record<string, (schema: any) => any> = {
  map: (schema: any) => ({
    ...schema,
    key: referenceSimplifySchema(schema.key),
    value: referenceSimplifySchema(schema.value),
  }),
  record: (schema: any) => ({
    ...schema,
    key: referenceSimplifySchema(schema.key),
    value: referenceSimplifySchema(schema.value),
  }),
  set: (schema: any) => ({
    ...schema,
    value: referenceSimplifySchema(schema.value),
  }),
  object: (schema: any) => ({
    ...schema,
    entries: Object.fromEntries(
      Object.entries(schema.entries).map(([key, value]) => [
        key,
        referenceSimplifySchema(value),
      ]),
    ),
  }),
  loose_object: (schema: any) => simplifyHandlers["object"](schema),
  object_with_rest: (schema: any) => ({
    ...simplifyHandlers["object"](schema),
    rest: referenceSimplifySchema(schema.rest),
  }),
  strict_object: (schema: any) => simplifyHandlers["object"](schema),
  array: (schema: any) => ({
    ...schema,
    item: referenceSimplifySchema(schema.item),
  }),
  tuple: (schema: any) => ({
    ...schema,
    items: schema.items.map((s: any) => referenceSimplifySchema(s)),
  }),
  loose_tuple: (schema: any) => simplifyHandlers["tuple"](schema),
  strict_tuple: (schema: any) => simplifyHandlers["tuple"](schema),
  tuple_with_rest: (schema: any) => ({
    ...simplifyHandlers["tuple"](schema),
    rest: referenceSimplifySchema(schema.rest),
  }),
  union: (schema: any) =>
    schema.options.map((s: any) => referenceSimplifySchema(s)),
  intersect: (schema: any) => ({
    ...schema,
    options: schema.options.map((s: any) => referenceSimplifySchema(s)),
  }),
  variant: (schema: any) => ({
    ...schema,
    options: schema.options.map((s: any) => referenceSimplifySchema(s)),
  }),
  "#wrapped": (schema: any) => ({
    ...schema,
    wrapped: referenceSimplifySchema(schema.wrapped),
  }),
  optional: (schema: any) => simplifyHandlers["#wrapped"](schema),
  non_optional: (schema: any) => simplifyHandlers["#wrapped"](schema),
  undefinedable: (schema: any) => simplifyHandlers["#wrapped"](schema),
  nullable: (schema: any) => simplifyHandlers["#wrapped"](schema),
  non_nullable: (schema: any) => simplifyHandlers["#wrapped"](schema),
  nullish: (schema: any) => simplifyHandlers["#wrapped"](schema),
  non_nullish: (schema: any) => simplifyHandlers["#wrapped"](schema),
  exact_optional: (schema: any) => simplifyHandlers["#wrapped"](schema),
};

/**
 * Simplifies a Valibot schema by removing internal metadata and recursively
 * processing nested schemas
 *
 * @param schema - Valibot schema object
 * @returns Simplified schema with metadata removed
 */
export function referenceSimplifySchema(schema: any): any {
  if (!schema || typeof schema !== "object") {
    return schema;
  }

  if (isTypeDefinition(schema)) {
    const simplified = referenceSimplifySchema(schema.schema.entries);
    return schema.indexes.length > 0
      ? { ...simplified, "@indexes": schema.indexes }
      : simplified;
  }

  // Skip schemas marked for cleanup — EXCEPT index metadata (withIndex):
  // it drives real MongoDB indexes (unique, TTL, collation, partial filters),
  // so it is structural and must survive simplification to participate in
  // snapshot comparisons. Other metadata (title, description, i18n) stays noise.
  if (CLEANUP_KINDS.includes(schema.kind)) {
    const kept: Record<PropertyKey, unknown> = {};
    const indexMetadata = indexMetadataOf(schema.metadata);
    if (indexMetadata !== undefined) kept[INDEX_SYMBOL] = indexMetadata;
    const privacyMetadata = schema.metadata?.[PRIVACY_SYMBOL];
    if (privacyMetadata !== undefined) kept[PRIVACY_SYMBOL] = privacyMetadata;
    if (Object.getOwnPropertySymbols(kept).length > 0) {
      return { kind: schema.kind, type: schema.type, metadata: kept };
    }
    return undefined;
  }

  // Create a shallow copy to avoid mutating the original
  const simplified = { ...schema };

  // Remove cleanup properties
  for (const prop of CLEANUP_PROPERTIES) {
    delete simplified[prop];
  }

  // Process pipe array if present
  if ("pipe" in simplified && Array.isArray(simplified.pipe)) {
    simplified.pipe = simplified.pipe
      .map((s: any) => referenceSimplifySchema(s))
      .filter((s: any) => s !== undefined);
  }

  // Use type-specific handler if available
  const handler = simplifyHandlers[schema.type];
  if (handler) {
    return handler(simplified);
  }

  return simplified;
}

/**
 * Flattens a simplified schema into dot-notation keys
 *
 * @param schema - Simplified schema object
 * @returns Flattened object with dot-notation paths as keys
 */
export function referenceFlattenSchema(schema: any): Record<string, any> {
  const result: Record<string, any> = {};

  if (!schema || typeof schema !== "object") {
    return result;
  }

  if (isTypeDefinition(schema)) {
    return referenceFlattenSchema(referenceSimplifySchema(schema));
  }

  // Symbol-keyed index metadata (withIndex) is invisible to Object.keys and
  // JSON.stringify — surface it under a stable string key so schema diffs
  // can see index changes (unique/TTL/collation/partialFilter/global).
  const indexMetadata = (schema as Record<PropertyKey, any>)[INDEX_SYMBOL];
  if (indexMetadata !== undefined) {
    result["@index"] = indexMetadata;
  }
  const privacyMetadata = (schema as Record<PropertyKey, any>)[PRIVACY_SYMBOL];
  if (privacyMetadata !== undefined) {
    for (const [k, val] of Object.entries(
      referenceFlattenSchema(privacyMetadata),
    )) {
      result[`@privacy.${k}`] = val;
    }
  }

  const keys = Object.keys(schema);
  for (const key of keys) {
    // Skip ~standard properties as they add noise
    if (key === "~standard" || key.endsWith(".~standard")) {
      continue;
    }

    const value = schema[key];

    if (Array.isArray(value)) {
      for (const [index, subValue] of value.entries()) {
        if (typeof subValue === "object" && subValue !== null) {
          for (const [subKey, subSubValue] of Object.entries(
            referenceFlattenSchema(subValue),
          )) {
            // Skip ~standard in nested paths
            if (subKey.includes(".~standard") || subKey === "~standard") {
              continue;
            }
            result[`${key}[${index}].${subKey}`] = subSubValue;
          }
        } else {
          result[`${key}[${index}]`] = subValue;
        }
      }
    } else if (typeof value === "object" && value !== null) {
      const nested = isTypeDefinition(value)
        ? referenceSimplifySchema(value)
        : value;
      for (const [subKey, subValue] of Object.entries(
        referenceFlattenSchema(nested),
      )) {
        // Skip ~standard in nested paths
        if (subKey.includes(".~standard") || subKey === "~standard") {
          continue;
        }
        result[`${key}.${subKey}`] = subValue;
      }
    } else {
      result[key] = value;
    }
  }

  return result;
}

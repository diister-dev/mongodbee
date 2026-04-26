import {
  computePath,
  createSimpleVisitor,
  SchemaNavigator,
} from "./schema-navigator.ts";
import * as v from "./schema.ts";
import type * as m from "mongodb";

/**
 * Symbol used to mark a field as requiring a unique index
 * @internal
 */
export const INDEX_SYMBOL = Symbol("mongodbee.index");

/**
 * Metadata for unique index validation
 */
export type IndexMetadata = {
  unique?: boolean;
  insensitive?: boolean;
  collation?: m.CollationOptions;
  /**
   * Number of seconds after the indexed field's value at which MongoDB will
   * purge the document (TTL index). Requires the indexed field to be a BSON
   * Date — on non-Date fields MongoDB silently no-ops the TTL behavior. `0`
   * is a valid value meaning "expire when current time >= the field's value".
   */
  expireAfterSeconds?: number;
  /**
   * MongoDB partial-filter expression. The index only covers documents
   * matching this filter. Combine with TTL to scope purges (e.g.
   * `{ status: "SENT" }`), or with `unique` to scope uniqueness. In
   * multi-collections this filter is automatically AND-merged with the
   * `_type` scoping filter.
   */
  partialFilterExpression?: m.Document;
};

export type IndexDatabase = {
  unique?: boolean;
  collation?: m.CollationOptions;
  expireAfterSeconds?: number;
  partialFilterExpression?: m.Document;
};

/**
 * Creates a unique index validation action for Valibot schemas
 *
 * This action marks a field as requiring a unique index in MongoDB.
 * The uniqueness can be enforced at collection level or across multi-collection types.
 *
 * @param schema - The base schema to apply unique index validation to
 * @param options - Configuration options for the unique index
 * @returns A schema with unique index metadata attached
 *
 * @example
 * ```typescript
 * import * as v from "mongodbee/schema";
 * import { uniqueIndex } from "mongodbee";
 *
 * const userSchema = {
 *   email: uniqueIndex(v.string()),
 *   username: uniqueIndex(v.pipe(v.string(), v.minLength(3)))
 * };
 * ```
 */
export function withIndex<
  T extends v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>,
>(
  schema: T,
  options: IndexMetadata = {},
): v.SchemaWithPipe<
  readonly [
    T,
    v.MetadataAction<v.InferOutput<T>, {
      readonly [INDEX_SYMBOL]: IndexDatabase;
    }>,
  ]
> {
  // Build the index metadata based on provided options
  // We construct this object in this way to ensure key are not defined if not provided
  // MongoDB driver is sensitive to key with undefined values
  const indexDatabase: IndexDatabase = {};
  if (options.unique) {
    indexDatabase.unique = true;
  }
  if (options.collation) {
    indexDatabase.collation = options.collation;
  } else if (options.insensitive) {
    indexDatabase.collation = { locale: "en", strength: 2 };
  }
  if (options.expireAfterSeconds !== undefined) {
    indexDatabase.expireAfterSeconds = options.expireAfterSeconds;
  }
  if (options.partialFilterExpression) {
    indexDatabase.partialFilterExpression = options.partialFilterExpression;
  }

  return v.pipe(
    schema,
    v.metadata({
      [INDEX_SYMBOL]: indexDatabase,
    }),
  );
}

/**
 * Extracts all unique index field paths from an object schema
 * @param schema - Object schema to analyze
 * @param prefix - Field path prefix for nested objects
 * @returns Array of unique index specifications
 * @internal
 */
export function extractIndexes(
  schema: v.ObjectSchema<any, any>,
): Array<{ path: string; metadata: IndexMetadata }> {
  const uniqueIndexes: Array<{ path: string; metadata: IndexMetadata }> = [];

  const structureVisitor = createSimpleVisitor({
    onNode: (node) => {
      // Skip metadata pipe nodes to avoid duplicates
      if (node.schema.type === "metadata") {
        return false;
      }

      const fullPath = computePath(node.context, (element) => {
        if (element.toString().startsWith("$")) return false;
        return true;
      }).join(".");

      // Check for index metadata on the schema itself
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let indexMetadata = (node.schema as any).metadata?.[INDEX_SYMBOL];

      // If not found on schema, check in pipe validations (for withIndex on unions, etc.)
      if (!indexMetadata) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const pipes = (node.schema as any).pipe;
        if (pipes && Array.isArray(pipes)) {
          for (const pipe of pipes) {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const pipeMetadata = (pipe as any).metadata?.[INDEX_SYMBOL];
            if (pipeMetadata) {
              indexMetadata = pipeMetadata;
              break; // Only need one index per field
            }
          }
        }
      }

      if (indexMetadata) {
        uniqueIndexes.push({
          path: fullPath,
          metadata: indexMetadata,
        });
        return true; // Stop navigation here, index found
      }

      return true;
    },
  });

  const navigator = new SchemaNavigator();
  navigator.navigate(schema, structureVisitor);

  return uniqueIndexes;
}

/**
 * Compare two index key specs for equality in a safe way.
 * Uses JSON.stringify as a deterministic fallback for nested keys.
 */
export function keyEqual(
  a: Record<string, unknown> | undefined,
  b: Record<string, unknown> | undefined,
): boolean {
  try {
    return JSON.stringify(a ?? {}) === JSON.stringify(b ?? {});
  } catch {
    return false;
  }
}

/**
 * Normalize collation options for comparison by extracting only significant fields.
 * MongoDB adds many default fields (version, caseLevel, etc.) that we should ignore.
 */
function normalizeCollation(collation: unknown): Record<string, unknown> | undefined {
  if (!collation || typeof collation !== "object") return undefined;

  const obj = collation as Record<string, unknown>;
  const normalized: Record<string, unknown> = {};

  // Only include significant fields that affect index behavior
  // Ignore MongoDB defaults like version, caseLevel, caseFirst, numericOrdering, etc.
  if (obj.locale !== undefined) normalized.locale = obj.locale;
  if (obj.strength !== undefined) normalized.strength = obj.strength;

  return Object.keys(normalized).length > 0 ? normalized : undefined;
}

/**
 * Normalize index options for comparison purposes.
 */
export function normalizeIndexOptions(
  opts: unknown,
): {
  unique: boolean;
  collation?: string;
  partialFilterExpression?: string;
  expireAfterSeconds?: number;
} {
  const obj = (opts as Record<string, unknown> | undefined) ?? {};
  const objTyped = obj as Record<string, unknown>;
  const hasUnique = Object.prototype.hasOwnProperty.call(objTyped, "unique")
    ? Boolean(objTyped["unique"])
    : false;
  const collationVal =
    Object.prototype.hasOwnProperty.call(objTyped, "collation")
      ? objTyped["collation"]
      : undefined;
  const pfeVal =
    Object.prototype.hasOwnProperty.call(objTyped, "partialFilterExpression")
      ? objTyped["partialFilterExpression"]
      : undefined;
  const ttlVal =
    Object.prototype.hasOwnProperty.call(objTyped, "expireAfterSeconds")
      ? objTyped["expireAfterSeconds"]
      : undefined;
  return {
    unique: hasUnique,
    collation: collationVal ? JSON.stringify(normalizeCollation(collationVal)) : undefined,
    partialFilterExpression: pfeVal ? JSON.stringify(pfeVal) : undefined,
    expireAfterSeconds: typeof ttlVal === "number" ? ttlVal : undefined,
  };
}

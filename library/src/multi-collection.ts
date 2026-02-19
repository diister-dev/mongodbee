import * as v from "./schema.ts";
import type * as m from "mongodb";
import { toMongoValidator } from "./validator.ts";
import { dbId, newId } from "./ids.ts";
import { extractFieldsToRemove, sanitizeForMongoDB } from "./sanitizer.ts";
import { createDotNotationSchema, getNestedValue } from "./dot-notation.ts";
import { getSessionContext } from "./session.ts";
import { withIndex } from "./indexes.ts";
import type { FlatType } from "../types/flat.ts";
import type { Db } from "./mongodb.ts";
import { dirtyEquivalent } from "./utils/object.ts";
import { mongoOperationQueue } from "./operation.ts";
import type { MultiCollectionModel } from "./multi-collection-model.ts";
import { retryOnWriteConflict } from "./utils/retry.ts";
import {
  createMultiCollectionInfo,
  createMetadataSchemas,
  multiCollectionInstanceExists,
} from "./migration/multicollection-registry.ts";
import { getLastAppliedMigration } from "./migration/state.ts";
import { applyMultiCollectionIndexes } from "./indexes-applier.ts";
import { isSchemaManaged } from "./runtime-config.ts";
import { createStageBuilder } from "./stage-builder.ts";
import type { AggregationStage, StageBuilder, MultiCollectionSchema } from "./stage-builder.ts";

// Re-export dbId and refId for backwards compatibility
export { dbId, refId } from "./ids.ts";
// Re-export StageBuilder types for consumers
export type { AggregationStage, StageBuilder } from "./stage-builder.ts";

type CollectionOptions = {
  safeDelete?: boolean;
  enableWatching?: boolean;
  /** How to handle undefined values in updates: 'remove' | 'ignore' | 'error' */
  undefinedBehavior?: "remove" | "ignore" | "error";
  /**
   * Override global schema management for this collection
   * - "auto": Apply validators/indexes automatically
   * - "managed": Skip auto-apply (migrations handle this)
   * - "inherit": Use global runtime config (default)
   */
  schemaManagement?: "auto" | "managed" | "inherit";
};

// Use _id if the schema is a literal schema, otherwise use dbId
type DynId<T> = T extends v.LiteralSchema<any, AnyMessage> ? T
  : ReturnType<typeof dbId>;

type AnyMessage = any;

type Elements<T extends Record<string, any>> = {
  [key in keyof T]: {
    _id: DynId<T[key]["_id"]>;
    _type: v.LiteralSchema<key, AnyMessage>;
  } & T[key];
}[keyof T];

type OutputElementSchema<T extends Record<string, any>, K extends keyof T> =
  v.ObjectSchema<
    {
      _id: DynId<T[K]["_id"]>;
      _type: v.LiteralSchema<K, AnyMessage>;
    } & T[K],
    any
  >;

type ElementSchema<T extends Record<string, any>, K extends keyof T> =
  v.ObjectSchema<
    {
      _id: DynId<T[K]["_id"]>;
    } & T[K],
    any
  >;

type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;
type MultiSchema<T extends Record<string, any>> = Elements<T>;

/**
 * Type helper to allow field removal with removeField()
 * Makes all fields accept either their original type or a symbol (for removeField())
 * Recursively applies to nested objects
 */
type DeepWithRemovable<T> = T extends Record<string, unknown>
  ? { [K in keyof T]: DeepWithRemovable<T[K]> | symbol }
  : T;

type WithRemovable<T> = {
  [K in keyof T]: DeepWithRemovable<T[K]> | symbol;
};

type Input<T extends MultiCollectionSchema> = v.InferInput<
  v.UnionSchema<[v.ObjectSchema<MultiSchema<T>, any>], any>
>;
type Output<T extends MultiCollectionSchema> = v.InferOutput<
  v.UnionSchema<[v.ObjectSchema<MultiSchema<T>, any>], any>
>;

/**
 * Type helper to extract union members that match specific _type values
 * This creates a proper discriminated union based on _type by distributing over K
 */
type ExtractByType<T extends MultiCollectionSchema, K extends keyof T> =
  K extends K ? v.InferOutput<OutputElementSchema<T, K>> : never;

// ─── Internal helpers ───

/** Builds { _type: "x" } or { _type: { $in: ["x", "y"] } } */
function buildTypeFilter(keys: string | string[]): Record<string, unknown> {
  const keyArray = Array.isArray(keys) ? keys : [keys];
  return keyArray.length === 1
    ? { _type: keyArray[0] }
    : { _type: { $in: keyArray } };
}

/** Builds { $and: [typeFilter, filter?] } */
function buildTypeQuery(keys: string | string[], filter?: Record<string, unknown>): Record<string, unknown> {
  const typeFilter = buildTypeFilter(keys);
  return filter
    ? { $and: [typeFilter, filter] }
    : typeFilter;
}

/**
 * Type representing the enhanced MongoDB collection for storing multiple document types
 * @template T - Record mapping document type names to their schemas
 */
type MultiCollectionResult<T extends MultiCollectionSchema> = {
  withSession: Awaited<ReturnType<typeof getSessionContext>>["withSession"];

  // === INSERT ===
  insertOne<E extends keyof T>(
    key: E,
    doc: v.InferInput<ElementSchema<T, E>>,
  ): Promise<string>;
  insertMany<E extends keyof T>(
    key: E,
    docs: v.InferInput<ElementSchema<T, E>>[],
  ): Promise<(string)[]>;

  // === READ ===
  getById<E extends keyof T>(
    key: E,
    id: string,
  ): Promise<v.InferOutput<OutputElementSchema<T, E>>>;

  /** Find documents of a single type - returns a Cursor */
  find<E extends keyof T>(
    key: E,
    filter?: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
    options?: m.FindOptions,
  ): m.FindCursor<v.InferOutput<OutputElementSchema<T, E>>>;
  /** Find documents across multiple types - returns a Cursor */
  find<E extends (keyof T)[]>(
    keys: E,
    filter?: m.Filter<v.InferInput<OutputElementSchema<T, E[number]>>>,
    options?: m.FindOptions,
  ): m.FindCursor<ExtractByType<T, E[number]>>;
  /** Find documents of any type - returns a Cursor */
  findAny(
    filter?: m.Filter<Input<T>>,
    options?: m.FindOptions,
  ): m.FindCursor<Output<T>>;

  /** Find one document of a single type */
  findOne<E extends keyof T>(
    key: E,
    filter: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
  ): Promise<v.InferOutput<OutputElementSchema<T, E>> | null>;
  /** Find one document across multiple types */
  findOne<E extends (keyof T)[]>(
    keys: E,
    filter: m.Filter<v.InferInput<OutputElementSchema<T, E[number]>>>,
  ): Promise<ExtractByType<T, E[number]> | null>;
  /** Find one document of any type */
  findOneAny(
    filter: m.Filter<Input<T>>,
  ): Promise<Output<T> | null>;

  // === PAGINATE ===
  paginate<E extends keyof T, EN = v.InferOutput<OutputElementSchema<T, E>>, R = EN>(
    key: E,
    filter?: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
    options?: {
      limit?: number;
      afterId?: string;
      beforeId?: string;
      sort?: m.Sort | m.SortDirection;
      /** Pipeline stages to execute server-side before pagination (lookups, addFields, etc.) */
      pipeline?: (stage: StageBuilder<T>) => AggregationStage[];
      prepare?: (
        doc: v.InferOutput<OutputElementSchema<T, E>>,
      ) => Promise<EN> | EN;
      filter?: (doc: EN) => Promise<boolean> | boolean;
      format?: (doc: EN) => Promise<R> | R;
    },
  ): Promise<{
    total: number;
    position: number;
    data: R[];
  }>;
  /**
   * Cross-pagination: paginate across multiple types simultaneously.
   * Documents from all specified types are merged and sorted together.
   */
  paginate<E extends (keyof T)[], EN = ExtractByType<T, E[number]>, R = EN>(
    keys: E,
    filter?: m.Filter<v.InferInput<OutputElementSchema<T, E[number]>>>,
    options?: {
      limit?: number;
      afterId?: string;
      beforeId?: string;
      sort?: m.Sort | m.SortDirection;
      /**
       * When true, sorts by the ULID part of _id (after the type prefix),
       * giving chronological ordering across different types.
       */
      naturalIdSort?: boolean;
      /** Pipeline stages to execute server-side before pagination (lookups, addFields, etc.) */
      pipeline?: (stage: StageBuilder<T>) => AggregationStage[];
      prepare?: (
        doc: ExtractByType<T, E[number]>,
      ) => Promise<EN> | EN;
      filter?: (doc: EN) => Promise<boolean> | boolean;
      format?: (doc: EN) => Promise<R> | R;
    },
  ): Promise<{
    total: number;
    position: number;
    data: R[];
  }>;

  // === COUNT ===
  /** Count documents of a single type */
  countDocuments<E extends keyof T>(
    key: E,
    filter?: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
    options?: m.CountDocumentsOptions,
  ): Promise<number>;
  /** Count documents across multiple types */
  countDocuments<E extends (keyof T)[]>(
    keys: E,
    filter?: m.Filter<v.InferInput<OutputElementSchema<T, E[number]>>>,
    options?: m.CountDocumentsOptions,
  ): Promise<number>;
  /** Count documents of any type */
  countAny(
    filter?: m.Filter<Input<T>>,
    options?: m.CountDocumentsOptions,
  ): Promise<number>;

  // === UPDATE ===
  /** Update a single document by ID */
  updateById<E extends keyof T>(
    key: E,
    id: string,
    doc: Omit<
      WithRemovable<Partial<FlatType<v.InferInput<ElementSchema<T, E>>>>>,
      "_id" | "type"
    >,
  ): Promise<number>;
  /** Update a single document by filter */
  updateOne<E extends keyof T>(
    key: E,
    filter: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
    doc: Omit<
      WithRemovable<Partial<FlatType<v.InferInput<ElementSchema<T, E>>>>>,
      "_id" | "type"
    >,
  ): Promise<number>;
  /** Update multiple documents of a type by filter */
  updateMany<E extends keyof T>(
    key: E,
    filter: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
    doc: Omit<
      WithRemovable<Partial<FlatType<v.InferInput<ElementSchema<T, E>>>>>,
      "_id" | "type"
    >,
  ): Promise<number>;
  /** Bulk update multiple documents by IDs across types */
  updateManyByIds(
    operation: {
      [key in keyof T]?: {
        [id: string]: Omit<
          WithRemovable<Partial<FlatType<v.InferInput<ElementSchema<T, key>>>>>,
          "_id" | "type"
        >;
      };
    },
  ): Promise<number>;

  // === DELETE ===
  /** Delete a single document by ID */
  deleteById<E extends keyof T>(key: E, id: string): Promise<number>;
  /** Delete multiple documents by IDs */
  deleteByIds<E extends keyof T>(key: E, ids: string[]): Promise<number>;
  /** Delete documents of a single type by filter */
  deleteMany<E extends keyof T>(
    key: E,
    filter: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
  ): Promise<number>;
  /** Delete documents across multiple types by filter */
  deleteMany<E extends (keyof T)[]>(
    keys: E,
    filter: m.Filter<v.InferInput<OutputElementSchema<T, E[number]>>>,
  ): Promise<number>;
  /** Delete documents of any type by filter */
  deleteAny(filter: m.Filter<Input<T>>): Promise<number>;

  // === AGGREGATE ===
  aggregate(
    stageBuilder: (stage: StageBuilder<T>) => AggregationStage[],
  ): Promise<any[]>;

  // === OTHER ===
  drop(options: { force: true }): Promise<boolean>;
};

/**
 * Creates a single MongoDB collection that can store multiple document types with validation
 *
 * This function creates or updates a MongoDB collection that can store different document types
 * in a single collection while maintaining type safety and validation for each type.
 *
 * @param db - MongoDB database instance
 * @param collectionName - Name of the collection to create or use
 * @param model - MultiCollectionModel defining the schema
 * @param options - Additional options for the collection
 * @returns A Promise resolving to an enhanced MongoDB collection with multi-document-type support
 *
 * @example
 * ```typescript
 * const catalogModel = defineModel("catalog", {
 *   product: {
 *     name: v.string(),
 *     price: v.number(),
 *   },
 *   category: {
 *     name: v.string(),
 *   }
 * });
 *
 * const catalog = await multiCollection(db, "catalog_louvre", catalogModel);
 *
 * const categoryId = await catalog.insertOne("category", { name: "Electronics" });
 * await catalog.insertOne("product", { name: "Phone", price: 499, category: categoryId });
 * ```
 */
export async function multiCollection<const T extends MultiCollectionSchema>(
  db: Db,
  collectionName: string,
  model: (T | MultiCollectionModel<T>),
  options?: (m.CollectionOptions & CollectionOptions),
): Promise<MultiCollectionResult<T>> {
  // Extract schema from model
  const useModel = (model && (model as MultiCollectionModel<T>).schema && (typeof model.expose === "function"));
  const collectionSchema = (useModel
    ? (model as MultiCollectionModel<T>).schema
    : model) as T;
  type TInput = Input<T>;
  type TOutput = Output<T>;

  const schemaWithId = Object.entries(collectionSchema).reduce(
    (acc, [key, value]) => {
      return {
        ...acc,
        [key]: {
          _id: dbId(key),
          _type: withIndex(v.optional(v.literal(key), () => key)),
          ...value,
        },
      };
    },
    {} as { [key in keyof T]: Elements<T> },
  );

  const schemaElements = Object.entries(schemaWithId).reduce(
    (acc, [key, value]) => {
      return {
        ...acc,
        [key]: v.object(value),
      };
    },
    {} as { [key in keyof T]: ElementSchema<T, key> },
  );

  const dotSchemaElements = Object.entries(schemaElements).reduce(
    (acc, [key, value]) => {
      return {
        ...acc,
        [key]: createDotNotationSchema(value),
      };
    },
    {} as Record<keyof T, v.BaseSchema<any, any, any>>,
  );

  const schema = v.union([
    ...Object.values(schemaElements),
  ]);

  const opts: m.CollectionOptions & CollectionOptions = {
    ...{
      safeDelete: true,
      undefinedBehavior: "remove", // Default behavior
    },
    ...options,
  };

  async function applyValidator() {
    const collections = await db.listCollections({ name: collectionName })
      .toArray();

    const modelValidators = createMetadataSchemas();

    const validator = toMongoValidator(
      v.union([
        // User-defined types
        ...Object.entries(schemaWithId).map(([key, value]) => {
          return v.object({
            ...value,
            _type: v.literal(key as string),
          });
        }),
        ...(useModel ? modelValidators : [])
      ]),
    );

    if (collections.length === 0) {
      // Create the collection with the validator
      await db.createCollection(collectionName, {
        validator,
      });
    } else {
      // Check collection options
      const existingOptions = await db.command({
        listCollections: 1,
        filter: { name: collectionName },
      });
      const currentSchema =
        existingOptions.cursor?.firstBatch?.[0]?.options?.validator || {};

      const sameSchema = dirtyEquivalent(currentSchema, validator);

      if (sameSchema) {
        return; // No need to update
      }

      // Update the collection with the validator
      await db.command({
        collMod: collectionName,
        validator,
      });
    }
  }

  async function applyIndexes() {
    await applyMultiCollectionIndexes(collection, schemaElements, {
      queue: mongoOperationQueue,
    })
  }

  let sessionContext: Awaited<ReturnType<typeof getSessionContext>>;

  async function init() {
    sessionContext = getSessionContext(db.client);

    // Determine if we should auto-apply schema and indexes
    const shouldAutoApply = (() => {
      // Local option takes precedence
      if (opts.schemaManagement === "auto") return true;
      if (opts.schemaManagement === "managed") return false;
      // Default to "inherit" - use global config
      return !isSchemaManaged();
    })();

    // Prevent add validator if a session is active
    const insideSession = !!sessionContext.getSession();

    if (shouldAutoApply && !insideSession) {
      await applyValidator();
      await applyIndexes();

      // Auto-initialize metadata for multi-collection model (only in auto mode)
      // In managed mode, migrations handle metadata creation
      // Check if metadata already exists
      const exists = await multiCollectionInstanceExists(db, collectionName);

      if (!exists) {
        // Get the current migration version
        const lastMigration = await getLastAppliedMigration(db);
        const currentMigrationId = lastMigration?.id || "current";

        // Create metadata for this instance
        if(useModel) {
          await createMultiCollectionInfo(
            db,
            collectionName,
            (model as MultiCollectionModel<T>).name,
            currentMigrationId,
          );
        }
      }
    }
  }

  const collection = db.collection<TOutput>(collectionName, opts);
  await init();

  return {
    withSession: sessionContext!.withSession,
        async insertOne(key, doc) {
            const _id = doc._id ?? `${key as string}:${newId()}`;
            const schema = schemaElements[key];
            const validation = v.parse(schema, {
                ...doc,
                _id,
            });

            // Apply sanitization based on configuration
            const safeDoc = sanitizeForMongoDB(validation, {
                undefinedBehavior: opts.undefinedBehavior || 'remove',
                deep: true
            }) as any;

            const session = sessionContext.getSession();
            const result = await collection.insertOne(safeDoc, { session });
            if(!result.acknowledged) {
                throw new Error("Insert failed");
            }

            return result.insertedId as unknown as string;
        },
        async insertMany(key, docs) {
            const validation = docs.map((doc) => {
                const _id = doc._id ?? `${key as string}:${newId()}`;
                return v.parse(schema, {
                    ...doc,
                    _id,
                });
            });

            // Apply sanitization based on configuration
            const safeDocs = validation.map(doc => sanitizeForMongoDB(doc, {
                undefinedBehavior: opts.undefinedBehavior || 'remove',
                deep: true
            }) as any);

            const session = sessionContext.getSession();
            const result = await collection.insertMany(safeDocs, { session });
            if(!result.acknowledged) {
                throw new Error("Insert failed");
            }

            return Object.values(result.insertedIds) as unknown as string[];
        },
        async getById(key, id) {
            const session = sessionContext.getSession();
            const query = buildTypeQuery(key as string, { _id: id });
            const result = await collection.findOne(query as any, { session });

            if (!result) {
                throw new Error("No element found");
            }

            return v.parse(schema, result);
        },
        findOne(keyOrKeys: any, filter: any) {
            const keys = Array.isArray(keyOrKeys) ? keyOrKeys as string[] : [keyOrKeys as string];
            const session = sessionContext.getSession();
            const query = buildTypeQuery(keys, filter);

            const doFind = async () => {
                const result = await collection.findOne(query as any, { session });
                if (!result) {
                    return null;
                }
                return v.parse(schema, result);
            };
            return doFind();
        },
        findOneAny(filter: any) {
            const session = sessionContext.getSession();
            const doFind = async () => {
                const result = await collection.findOne(filter as any, { session });
                if (!result) {
                    return null;
                }
                return v.parse(schema, result);
            };
            return doFind();
        },
        find(keyOrKeys: any, filter?: any, options?: any) {
            const keys = Array.isArray(keyOrKeys) ? keyOrKeys as string[] : [keyOrKeys as string];
            const query = buildTypeQuery(keys, filter);
            const session = sessionContext.getSession();
            const cursor = collection.find(query as any, { session, ...options });

            // Wrap toArray to add validation
            const originalToArray = cursor.toArray.bind(cursor);
            cursor.toArray = async () => {
                const results = await originalToArray();
                let invalidsCount = 0;

                const output = results.map((item) => {
                    const parsed = v.safeParse(schema, item);
                    if (!parsed.success) {
                        invalidsCount++;
                        return null;
                    }
                    return parsed.output;
                }).filter((item): item is NonNullable<typeof item> => item !== null);

                if (invalidsCount > 0) {
                    console.warn(
                        `Warning: ${invalidsCount} invalid documents were ignored during find operation`,
                    );
                }

                return output;
            };

            // Wrap next to add validation
            const originalNext = cursor.next.bind(cursor);
            cursor.next = async () => {
                while (true) {
                    const doc = await originalNext();
                    if (!doc) return null;
                    const result = v.safeParse(schema, doc);
                    if (result.success) return result.output as any;
                    // Skip invalid, try next
                }
            };

            return cursor as any;
        },
        findAny(filter?: any, options?: any) {
            const session = sessionContext.getSession();
            const cursor = collection.find((filter || {}) as any, { session, ...options });

            // Wrap toArray to add validation
            const originalToArray = cursor.toArray.bind(cursor);
            cursor.toArray = async () => {
                const results = await originalToArray();
                let invalidsCount = 0;

                const output = results.map((item) => {
                    const parsed = v.safeParse(schema, item);
                    if (!parsed.success) {
                        invalidsCount++;
                        return null;
                    }
                    return parsed.output;
                }).filter((item): item is NonNullable<typeof item> => item !== null);

                if (invalidsCount > 0) {
                    console.warn(
                        `Warning: ${invalidsCount} invalid documents were ignored during findAny operation`,
                    );
                }

                return output;
            };

            // Wrap next to add validation
            const originalNext = cursor.next.bind(cursor);
            cursor.next = async () => {
                while (true) {
                    const doc = await originalNext();
                    if (!doc) return null;
                    const result = v.safeParse(schema, doc);
                    if (result.success) return result.output as any;
                }
            };

            return cursor as any;
        },
        // Implementation supports both single key and array of keys
        // Type safety is enforced through the overload signatures above
        async paginate(
            keyOrKeys: keyof T | (keyof T)[],
            filter?: m.Filter<any>,
            options?: {
                limit?: number,
                afterId?: string,
                beforeId?: string,
                sort?: m.Sort | m.SortDirection,
                naturalIdSort?: boolean,
                pipeline?: (stage: StageBuilder<T>) => AggregationStage[],
                prepare?: (doc: any) => Promise<any> | any,
                filter?: (doc: any) => Promise<boolean> | boolean,
                format?: (doc: any) => Promise<any> | any,
            }
        ) {
            let { limit = 100, afterId, beforeId, sort, naturalIdSort, pipeline: pipelineBuilder, prepare, filter: customFilter, format } = options || {};
            const session = sessionContext.getSession();

            // Support both single key and array of keys for cross-pagination
            const keys = Array.isArray(keyOrKeys) ? keyOrKeys as (keyof T)[] : [keyOrKeys as keyof T];
            const isCrossPagination = Array.isArray(keyOrKeys);

            // Build type filter and base query using helpers
            const typeFilter = buildTypeFilter(keys as string[]);
            const baseQuery = filter ? [typeFilter, filter] : [typeFilter];
            let query: Record<string, unknown> = { $and: baseQuery };

            // Normalize sort to object format
            sort = sort || { _id: 1 };
            const sortObj: Record<string, 1 | -1> = typeof sort === 'object' && !Array.isArray(sort)
                ? { ...sort as Record<string, 1 | -1> }
                : { _id: sort === 1 || sort === 'asc' || sort === 'ascending' ? 1 : -1 };

            // Always add _id as tie-breaker if not already in sort (ensures stable ordering for duplicate values)
            if (!('_id' in sortObj)) {
                sortObj._id = 1;
            }

            // For naturalIdSort, replace _id with _ulid in sort (extracts ULID part after "type:")
            // This gives chronological ordering across different types
            const useNaturalIdSort = naturalIdSort && '_id' in sortObj;
            const effectiveSortObj = useNaturalIdSort
                ? Object.fromEntries(
                    Object.entries(sortObj).map(([k, v]) => [k === '_id' ? '_ulid' : k, v])
                  ) as Record<string, 1 | -1>
                : sortObj;

            // Update sort to include _id tie-breaker (or _ulid for naturalIdSort)
            sort = effectiveSortObj;

            // Helper to extract ULID part from an ID (after "type:")
            const extractUlid = (id: string): string => {
                const colonIndex = id.indexOf(':');
                return colonIndex >= 0 ? id.substring(colonIndex + 1) : id;
            };

            // Helper to build cursor-based pagination filter for multi-collection
            const buildCursorFilter = async (anchorId: string, direction: 'after' | 'before') => {
                // Fetch the anchor document to get its sort field values
                const anchorDoc = await collection.findOne({ _id: anchorId } as never, { session });
                if (!anchorDoc) return null;

                // Add _ulid to anchor doc if using naturalIdSort
                const enrichedAnchorDoc = useNaturalIdSort
                    ? { ...anchorDoc, _ulid: extractUlid(anchorId) }
                    : anchorDoc;

                const sortFields = Object.keys(effectiveSortObj);

                // If sorting only by _id (or _ulid), use simple comparison
                if (sortFields.length === 1 && (sortFields[0] === '_id' || sortFields[0] === '_ulid')) {
                    const sortField = sortFields[0];
                    const sortDir = effectiveSortObj[sortField];
                    const op = direction === 'after'
                        ? (sortDir === 1 ? '$gt' : '$lt')
                        : (sortDir === 1 ? '$lt' : '$gt');

                    // For _ulid, we compare the ULID part directly
                    const compareValue = sortField === '_ulid' ? extractUlid(anchorId) : anchorId;
                    return { [sortField]: { [op]: compareValue } };
                }

                // Build compound cursor filter for custom sort
                const conditions: Record<string, unknown>[] = [];

                for (let i = 0; i < sortFields.length; i++) {
                    const field = sortFields[i];
                    const sortDir = effectiveSortObj[field];
                    const anchorValue = getNestedValue(enrichedAnchorDoc as Record<string, unknown>, field);

                    const condition: Record<string, unknown> = {};

                    // All previous fields must be equal
                    for (let j = 0; j < i; j++) {
                        const prevField = sortFields[j];
                        condition[prevField] = getNestedValue(enrichedAnchorDoc as Record<string, unknown>, prevField);
                    }

                    // Current field uses comparison based on sort direction and pagination direction
                    const isForward = direction === 'after';
                    const op = (sortDir === 1) === isForward ? '$gt' : '$lt';
                    condition[field] = { [op]: anchorValue };

                    conditions.push(condition);
                }

                return { $or: conditions };
            };

            // Helper to validate ID format matches one of the allowed types
            const isValidIdForTypes = (id: string, allowedTypes: (keyof T)[]): boolean => {
                return allowedTypes.some(type => id.startsWith(`${type as string}:`));
            };

            // Add pagination filters
            // Keep cursorFilter separate for use in aggregation pipeline with naturalIdSort
            let cursorFilterResult: Record<string, unknown> | null = null;

            if (afterId) {
              if (!isValidIdForTypes(afterId, keys)) {
                  const typesStr = keys.map(k => String(k)).join(', ');
                  throw new Error(`Invalid afterId format for type(s) ${typesStr}`);
              }
              cursorFilterResult = await buildCursorFilter(afterId, 'after');
              if (cursorFilterResult) {
                  query = {
                      $and: [
                          ...baseQuery,
                          cursorFilterResult
                      ]
                  };
              }
            } else if (beforeId) {
              if (!isValidIdForTypes(beforeId, keys)) {
                  const typesStr = keys.map(k => String(k)).join(', ');
                  throw new Error(`Invalid beforeId format for type(s) ${typesStr}`);
              }
              cursorFilterResult = await buildCursorFilter(beforeId, 'before');
              if (cursorFilterResult) {
                  query = {
                      $and: [
                          ...baseQuery,
                          cursorFilterResult
                      ]
                  };
              }
              // Reverse the sort for beforeId to get items in reverse order
              const reversedSort: Record<string, 1 | -1> = {};
              for (const [field, dir] of Object.entries(sortObj)) {
                  reversedSort[field] = (dir === 1 ? -1 : 1) as 1 | -1;
              }
              sort = reversedSort;
            }

            let total: number | undefined;
            let position: number | undefined;
            {
                const baseCountQuery = { $and: baseQuery };
                total = await collection.countDocuments(baseCountQuery as never, { session });

                if (afterId) {
                    const afterFilter = await buildCursorFilter(afterId, 'after');
                    if (afterFilter) {
                        const afterCount = await collection.countDocuments({ $and: [...baseQuery, afterFilter] } as never, { session });
                        position = total - afterCount;
                    } else {
                        position = 1;
                    }
                } else if (beforeId) {
                    // For beforeId, we need to calculate position after we know how many items will be returned
                    position = -1; // Marker for "needs calculation"
                } else {
                    position = 0;
                }
            }

            const stageBuilderInstance = createStageBuilder<T>(collectionName);

            // Build cursor - use aggregate if pipeline is provided or naturalIdSort is enabled
            // deno-lint-ignore no-explicit-any
            let cursor: m.FindCursor<any> | m.AggregationCursor<any>;

            // Stage to extract ULID part from _id for natural sorting across types
            const ulidExtractStage: AggregationStage = {
                $addFields: {
                    _ulid: {
                        $substr: [
                            "$_id",
                            { $add: [{ $indexOfCP: ["$_id", ":"] }, 1] },
                            -1
                        ]
                    }
                }
            };

            if (pipelineBuilder || useNaturalIdSort) {
                // Build aggregation pipeline with user stages
                const userPipeline = pipelineBuilder ? pipelineBuilder(stageBuilderInstance) : [];

                // For naturalIdSort, we need to add _ulid BEFORE the cursor filter can use it
                // So we split the query: base type filter first, then add _ulid, then cursor filter
                const aggregatePipeline: AggregationStage[] = useNaturalIdSort
                    ? [
                        // First: match the base type filter
                        { $match: { $and: baseQuery } },
                        // Add _ulid field before cursor filter needs it
                        ulidExtractStage,
                        // Apply cursor filter if present (afterId/beforeId)
                        ...(cursorFilterResult ? [{ $match: cursorFilterResult }] : []),
                        ...userPipeline,
                        { $sort: sort as Record<string, 1 | -1> },
                    ]
                    : [
                        { $match: query },
                        ...userPipeline,
                        { $sort: sort as Record<string, 1 | -1> },
                    ];
                cursor = collection.aggregate(aggregatePipeline, { session });
            } else {
                cursor = collection.find(query as never, { session }).sort(sort as m.Sort);
            }

            let hardLimit = 10_000;
            const elements: unknown[] = [];

            try {
                while(hardLimit-- > 0 && limit > 0) {
                    const doc = await cursor.next();
                    if (!doc) break;

                    // Validate document with schema
                    const validation = v.safeParse(schema, doc);
                    if (!validation.success) {
                        continue; // Skip invalid documents
                    }

                    // When pipeline is used, merge validated doc with original doc to preserve
                    // additional fields added by pipeline stages (like $lookup results)
                    const validatedDoc = pipelineBuilder
                        ? { ...doc, ...validation.output }
                        : validation.output;

                    // Step 1: Prepare - enrich document with external data
                    const enrichedDoc = prepare ? await prepare(validatedDoc) : validatedDoc;

                    // Step 2: Filter - apply custom filtering logic
                    const isValid = await customFilter?.(enrichedDoc) ?? true;
                    if (!isValid) continue;

                    // Step 3: Format - transform document to final output format
                    const finalDoc = format ? await format(enrichedDoc) : enrichedDoc;

                    elements.push(finalDoc);
                    limit--;
                }
            } finally {
                await cursor.close();
            }

            // If paginating backwards (beforeId), reverse to maintain consistent order with forward pagination
            if (beforeId) {
                elements.reverse();
                // Calculate position: count of elements before the first returned element
                const beforeFilter = await buildCursorFilter(beforeId, 'before');
                if (beforeFilter) {
                    const beforeCount = await collection.countDocuments({ $and: [...baseQuery, beforeFilter] } as never, { session });
                    position = Math.max(0, beforeCount - elements.length);
                } else {
                    position = 0;
                }
            }

            return {
                total,
                position: position as number,
                data: elements,
            };
        },
        countDocuments(keyOrKeys: any, filter?: any, options?: any) {
            const keys = Array.isArray(keyOrKeys) ? keyOrKeys as string[] : [keyOrKeys as string];
            const session = sessionContext.getSession();
            const query = buildTypeQuery(keys, filter);
            return collection.countDocuments(query as never, { session, ...options });
        },
        countAny(filter?: any, options?: any) {
            const session = sessionContext.getSession();
            return collection.countDocuments((filter || {}) as never, { session, ...options });
        },
        async deleteById(key, id) {
            const schema = schemaWithId[key];
            v.parse(schema._id, id);

            if(!id.startsWith(`${key as string}:`)) {
                throw new Error(`Invalid id format`);
            }

            const session = sessionContext.getSession();

            const result = await collection.deleteOne({
                _id: id,
            } as any, { session });

            if(!result.acknowledged) {
                throw new Error("Delete failed");
            }

            if (result.deletedCount === 0) {
                throw new Error("No element that match the filter to delete");
            }

            return result.deletedCount;
        },
        async deleteByIds(key, ids) {
            const schema = schemaWithId[key];
            ids.forEach((id) => {
                v.parse(schema._id, id);
            });

            const session = sessionContext.getSession();

            const result = await collection.deleteMany({
                _id: {
                    $in: ids,
                },
                _type: key as string,
            } as any, { session });

            if(!result.acknowledged) {
                throw new Error("Delete failed");
            }

            if (result.deletedCount === 0) {
                throw new Error("No element that match the filter to delete");
            }

            return result.deletedCount;
        },
        async deleteMany(keyOrKeys: any, filter: any) {
            const keys = Array.isArray(keyOrKeys) ? keyOrKeys as string[] : [keyOrKeys as string];
            const session = sessionContext.getSession();
            const query = buildTypeQuery(keys, filter);

            const result = await collection.deleteMany(query as any, { session });

            if (!result.acknowledged) {
                throw new Error("Delete failed");
            }

            return result.deletedCount;
        },
        async deleteAny(filter) {
            const session = sessionContext.getSession();

            const result = await collection.deleteMany(filter as any, { session });

            if (!result.acknowledged) {
                throw new Error("Delete failed");
            }

            return result.deletedCount;
        },
        async updateById(key, id, doc) {
            // Validation happens outside retry - no need to retry validation errors
            const dotSchema = dotSchemaElements[key];
            if(!dotSchema) {
                throw new Error(`Invalid element type`);
            }

            // Extract fields to remove before validation (symbols would fail validation)
            const { set, unset } = extractFieldsToRemove(doc as Record<string, unknown>);

            // Validate only the fields that will be set (not the removed ones)
            if (Object.keys(set).length > 0) {
                v.parse(dotSchema, set);
            }

            return retryOnWriteConflict(async () => {
                const session = sessionContext.getSession();

                // Sanitize the remaining fields
                const sanitizedDoc = sanitizeForMongoDB(set, {
                    undefinedBehavior: opts.undefinedBehavior || "remove",
                    deep: true,
                });

                // Build update operations
                const updateOps: Record<string, unknown> = {};
                if (Object.keys(sanitizedDoc as Record<string, unknown>).length > 0) {
                    updateOps.$set = sanitizedDoc;
                }
                if (Object.keys(unset).length > 0) {
                    updateOps.$unset = unset;
                }

                // If no operations, return early
                if (Object.keys(updateOps).length === 0) {
                    return 0; // No modifications
                }

                const result = await collection.updateOne({
                    _id: id,
                    _type: key as string,
                } as m.Filter<TOutput>, updateOps as m.UpdateFilter<TOutput>, { session });

                if(!result.acknowledged) {
                    throw new Error("Update failed");
                }

                if (result.matchedCount === 0) {
                    throw new Error("No element that match the filter to update");
                }

                // Note: modifiedCount can be 0 if the values didn't actually change
                // This is not an error condition
                return result.modifiedCount;
            });
        },
        async updateOne(key, filter, doc) {
            const dotSchema = dotSchemaElements[key];
            if(!dotSchema) {
                throw new Error(`Invalid element type`);
            }

            const { set, unset } = extractFieldsToRemove(doc as Record<string, unknown>);

            if (Object.keys(set).length > 0) {
                v.parse(dotSchema, set);
            }

            return retryOnWriteConflict(async () => {
                const session = sessionContext.getSession();

                const sanitizedDoc = sanitizeForMongoDB(set, {
                    undefinedBehavior: opts.undefinedBehavior || "remove",
                    deep: true,
                });

                const updateOps: Record<string, unknown> = {};
                if (Object.keys(sanitizedDoc as Record<string, unknown>).length > 0) {
                    updateOps.$set = sanitizedDoc;
                }
                if (Object.keys(unset).length > 0) {
                    updateOps.$unset = unset;
                }

                if (Object.keys(updateOps).length === 0) {
                    return 0;
                }

                const query = buildTypeQuery(key as string, filter as Record<string, unknown>);
                const result = await collection.updateOne(
                    query as m.Filter<TOutput>,
                    updateOps as m.UpdateFilter<TOutput>,
                    { session },
                );

                if(!result.acknowledged) {
                    throw new Error("Update failed");
                }

                if (result.matchedCount === 0) {
                    throw new Error("No element that match the filter to update");
                }

                return result.modifiedCount;
            });
        },
        async updateMany(key, filter, doc) {
            const dotSchema = dotSchemaElements[key];
            if(!dotSchema) {
                throw new Error(`Invalid element type`);
            }

            const { set, unset } = extractFieldsToRemove(doc as Record<string, unknown>);

            if (Object.keys(set).length > 0) {
                v.parse(dotSchema, set);
            }

            return retryOnWriteConflict(async () => {
                const session = sessionContext.getSession();

                const sanitizedDoc = sanitizeForMongoDB(set, {
                    undefinedBehavior: opts.undefinedBehavior || "remove",
                    deep: true,
                });

                const updateOps: Record<string, unknown> = {};
                if (Object.keys(sanitizedDoc as Record<string, unknown>).length > 0) {
                    updateOps.$set = sanitizedDoc;
                }
                if (Object.keys(unset).length > 0) {
                    updateOps.$unset = unset;
                }

                if (Object.keys(updateOps).length === 0) {
                    return 0;
                }

                const query = buildTypeQuery(key as string, filter as Record<string, unknown>);
                const result = await collection.updateMany(
                    query as m.Filter<TOutput>,
                    updateOps as m.UpdateFilter<TOutput>,
                    { session },
                );

                if(!result.acknowledged) {
                    throw new Error("Update failed");
                }

                return result.modifiedCount;
            });
        },
        async updateManyByIds(operation) {
            return retryOnWriteConflict(async () => {
                const bulkOps: any[] = [];
                for(const type in operation) {
                    const elements = operation[type];
                    for(const id in elements) {
                        const element = elements[id];
                        const dotSchema = dotSchemaElements[type];
                        if(!id.startsWith(`${type}:`)) {
                            throw new Error(`Invalid id format`);
                        }
                        if(!dotSchema) {
                            throw new Error(`Invalid element type`);
                        }

                        // Extract fields to remove before validation (symbols would fail validation)
                        const { set, unset } = extractFieldsToRemove(element as Record<string, unknown>);

                        // Validate only the fields that will be set (not the removed ones)
                        if (Object.keys(set).length > 0) {
                            v.parse(dotSchema, set);
                        }

                        // Sanitize the remaining fields
                        const sanitizedElement = sanitizeForMongoDB(set, {
                            undefinedBehavior: opts.undefinedBehavior || "remove",
                            deep: true,
                        });

                        // Build update operations
                        const updateOps: Record<string, unknown> = {};
                        if (Object.keys(sanitizedElement as Record<string, unknown>).length > 0) {
                            updateOps.$set = sanitizedElement;
                        }
                        if (Object.keys(unset).length > 0) {
                            updateOps.$unset = unset;
                        }

                        // Skip if no operations
                        if (Object.keys(updateOps).length === 0) {
                            continue;
                        }

                        bulkOps.push({
                            updateOne: {
                                filter: { _id: id },
                                update: updateOps,
                            }
                        });
                    }
                }

                if (bulkOps.length === 0) {
                    throw new Error("No element to update");
                }

                const session = sessionContext.getSession();

                const result = await collection.bulkWrite(bulkOps, { session });

                if (result.matchedCount === 0) {
                    throw new Error("No element that match the filter to update");
                }

                // Note: modifiedCount can be 0 if the values didn't actually change
                // This is not an error condition
                return result.modifiedCount;
            });
        },
        async aggregate(stageBuilder) {
            const stage = createStageBuilder<T>(collectionName);
            const session = sessionContext.getSession();

            const pipeline = stageBuilder(stage);
            const cursor = collection.aggregate(pipeline, { session });

            return await cursor.toArray();
        },
        async drop(options) {
            if (!options?.force) {
                throw new Error("Must provide { force: true } to drop the collection");
            }

            const session = sessionContext.getSession();
            return await collection.drop({ session });
        },
  };
}

/**
 * Creates a NEW multi-collection with a raw schema
 *
 * This creates a physical collection with validators and indexes applied immediately.
 * No metadata tracking is created - use this for quick prototyping or simple collections.
 *
 * For production collections that need to be tracked by the migration system,
 * use createMultiCollectionInstance() with a formal model instead.
 *
 * @param db - MongoDB database instance
 * @param collectionName - Name of the collection to create
 * @param schema - The raw schema object defining document types
 * @param options - Additional options for the collection
 * @returns A Promise resolving to an enhanced MongoDB collection
 *
 * @throws {Error} If called within an active session/transaction (DDL operations incompatible)
 * @throws {Error} If collection already exists
 *
 * @example
 * ```typescript
 * import { newMultiCollection } from "@diister/mongodbee";
 * import * as v from "valibot";
 *
 * // ✅ Create collection outside session with raw schema
 * const catalog = await newMultiCollection(db, "catalog_temp", {
 *   product: {
 *     name: v.string(),
 *     price: v.number(),
 *   },
 *   category: {
 *     name: v.string(),
 *   }
 * });
 *
 * // ✓ Then use it inside session for data operations
 * await catalog.withSession(async () => {
 *   await catalog.insertOne("category", { name: "Electronics" });
 * });
 * ```
 */
export async function newMultiCollection<const T extends MultiCollectionSchema>(
  db: Db,
  collectionName: string,
  schema: T,
  options?: m.CollectionOptions & CollectionOptions,
): Promise<MultiCollectionResult<T>> {
  // Check if we're in a session - DDL operations are incompatible with transactions
  const { getSession } = getSessionContext(db.client);
  const activeSession = getSession();

  if (activeSession) {
    throw new Error(
      `Cannot call newMultiCollection() within an active session/transaction.\n` +
      `This function applies validators and indexes using DDL operations (createCollection, collMod, createIndex)\n` +
      `which are incompatible with transactions.\n\n` +
      `Solution: Call newMultiCollection() BEFORE entering a session:\n\n` +
      `  // ✅ Create collection outside session\n` +
      `  const catalog = await newMultiCollection(db, "catalog_temp", schema);\n` +
      `  \n` +
      `  // ✓ Then use it inside session for data operations\n` +
      `  await catalog.withSession(async () => {\n` +
      `    await catalog.insertOne("product", { name: "Item", price: 100 });\n` +
      `  });`
    );
  }

  // Check if collection already exists
  const collections = await db.listCollections({ name: collectionName }).toArray();
  if (collections.length > 0) {
    throw new Error(
      `Collection "${collectionName}" already exists.\n` +
      `Use multiCollection() to connect to an existing collection.`
    );
  }

  // Create the collection with validators and indexes applied immediately
  // No metadata needed - raw schema usage
  return await multiCollection(db, collectionName, schema, {
    ...options,
    schemaManagement: "auto", // Force immediate application of schema/indexes
  });
}

/**
 * Creates a multi-collection instance from a formal model WITH metadata tracking
 *
 * Use this to create production instances of a model that need to be tracked
 * by the migration system. The model provides the schema, and metadata documents
 * are created to track the instance.
 *
 * This function will:
 * 1. Create the physical MongoDB collection
 * 2. Apply validators and indexes from the model schema
 * 3. Create metadata documents (_information, _migrations) for tracking
 *
 * The migration ID is automatically determined from the current migration state.
 *
 * @param db - MongoDB database instance
 * @param collectionName - Name of the collection instance to create
 * @param model - The multi-collection model defining the schema
 * @param options - Additional collection options
 * @returns A Promise that resolves when the instance is created
 *
 * @throws {Error} If called within an active session/transaction (DDL operations incompatible)
 * @throws {Error} If instance already exists
 *
 * @example
 * ```typescript
 * import { defineModel, createMultiCollectionInstance } from "@diister/mongodbee";
 * import * as v from "valibot";
 *
 * // Define a reusable model
 * const catalogModel = defineModel("catalog", {
 *   schema: {
 *     product: {
 *       name: v.string(),
 *       price: v.number(),
 *     },
 *     category: {
 *       name: v.string(),
 *     }
 *   }
 * });
 *
 * // Create a tracked instance in production
 * await createMultiCollectionInstance(db, "catalog_paris", catalogModel);
 *
 * // Later, connect to the instance
 * const catalog = await multiCollection(db, "catalog_paris", catalogModel);
 * ```
 */
export async function createMultiCollectionInstance<const T extends MultiCollectionSchema>(
  db: Db,
  collectionName: string,
  model: MultiCollectionModel<T>,
  options?: m.CollectionOptions & CollectionOptions,
): Promise<MultiCollectionResult<T>> {
  // Check if we're in a session - DDL operations are incompatible with transactions
  const { getSession } = getSessionContext(db.client);
  const activeSession = getSession();

  if (activeSession) {
    throw new Error(
      `Cannot call createMultiCollectionInstance() within an active session/transaction.\n` +
      `This function creates the collection and applies validators/indexes using DDL operations\n` +
      `which are incompatible with transactions.\n\n` +
      `Solution: Call this function OUTSIDE of any session/transaction.`
    );
  }

  // Check if already exists
  const exists = await multiCollectionInstanceExists(db, collectionName);
  if (exists) {
    throw new Error(
      `Multi-collection instance "${collectionName}" already exists.`
    );
  }

  // Create collection with schema from model
  // This will apply validators, indexes, AND create metadata automatically
  // because we're using schemaManagement: "auto" with a model
  // The migration ID is determined from the current migration state
  return await multiCollection(db, collectionName, model, {
    ...options,
    schemaManagement: "auto", // Force immediate application + metadata creation
  });
}

// Re-export utility functions from multicollection-registry for public use
export {
  discoverMultiCollectionInstances,
  getMultiCollectionInfo,
  getMultiCollectionMigrations,
  markAsMultiCollection,
  multiCollectionInstanceExists,
  type MultiCollectionInfo,
  type MultiCollectionMigrations,
} from "./migration/multicollection-registry.ts";
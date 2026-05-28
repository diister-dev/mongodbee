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
import { createLogger } from "./utils/logger.ts";

const log = createLogger("multi-collection");

// Re-export dbId and refId for backwards compatibility
export { dbId, refId } from "./ids.ts";

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

type MultiCollectionSchema = Record<string, Record<string, AnySchema>>;

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

// Type for aggregation pipeline stages
type AggregationStage = Record<string, unknown>;
type StageBuilder<T extends MultiCollectionSchema> = {
  match: <E extends keyof T>(
    key: E,
    filter: Record<string, unknown>,
  ) => AggregationStage;
  unwind: <E extends keyof T>(key: E, field: string) => AggregationStage;
  lookup: <E extends keyof T>(
    key: E,
    localField: string,
    foreignField: string,
    asOrOptions?: string | {
      as?: string;
      pipeline?: (stage: StageBuilder<T>) => AggregationStage[];
      let?: Record<string, unknown>;
    },
  ) => AggregationStage;
  /** 
   * Lookup without _type constraint - useful for polymorphic references 
   * where the ID prefix (e.g., "collaborator:xxx") already guarantees uniqueness.
   * Returns documents from any type in the collection.
   */
  anyLookup: (
    localField: string,
    foreignField: string,
    asOrOptions?: string | {
      as?: string;
      pipeline?: (stage: StageBuilder<T>) => AggregationStage[];
      let?: Record<string, unknown>;
    },
  ) => AggregationStage;
  /**
   * Lookup into an external collection (outside this multi-collection).
   * Useful for joining with other MongoDB collections or other multi-collections.
   */
  externalLookup: (
    fromCollection: string,
    localField: string,
    foreignField: string,
    asOrOptions?: string | {
      as?: string;
      pipeline?: AggregationStage[];
      let?: Record<string, unknown>;
    },
  ) => AggregationStage;
  project: (projection: Record<string, 1 | 0 | string | Record<string, unknown>>) => AggregationStage;
  addFields: (fields: Record<string, unknown>) => AggregationStage;
  group: (grouping: Record<string, unknown>) => AggregationStage;
  sort: (sort: Record<string, 1 | -1>) => AggregationStage;
  limit: (limit: number) => AggregationStage;
  skip: (skip: number) => AggregationStage;
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

/**
 * Type representing the enhanced MongoDB collection for storing multiple document types
 * @template T - Record mapping document type names to their schemas
 */
type MultiCollectionResult<T extends MultiCollectionSchema> = {
  withSession: Awaited<ReturnType<typeof getSessionContext>>["withSession"];
  insertOne<E extends keyof T>(
    key: E,
    doc: v.InferInput<ElementSchema<T, E>>,
  ): Promise<string>;
  insertMany<E extends keyof T>(
    key: E,
    docs: v.InferInput<ElementSchema<T, E>>[],
  ): Promise<(string)[]>;
  getById<E extends keyof T>(
    key: E,
    id: string,
  ): Promise<v.InferOutput<OutputElementSchema<T, E>>>;
  findOne<E extends keyof T>(
    key: E,
    filter: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
  ): Promise<v.InferOutput<OutputElementSchema<T, E>> | null>;
  find<E extends keyof T>(
    key: E,
    filter?: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
    options?: m.FindOptions,
  ): Promise<v.InferOutput<OutputElementSchema<T, E>>[]>;
  /**
   * Find the first document matching a cross-type filter — no `_type`
   * constraint injected. Symmetric to `deleteAny` ; useful when the
   * caller branches on `_type` inside the filter (e.g. polymorphic
   * existence check with a `$or` across distinct types).
   *
   * Returns `null` if no doc matches. The result is validated against the
   * union schema and typed as the union of all element shapes.
   */
  findOneAny(
    filter: m.Filter<Input<T>>,
  ): Promise<Output<T> | null>;
  /**
   * Find all documents matching a cross-type filter — no `_type` constraint
   * injected. Symmetric to `deleteAny`. Each result is validated against
   * the union schema ; invalid docs are silently dropped (same posture as
   * `find`).
   */
  findAny(
    filter: m.Filter<Input<T>>,
    options?: m.FindOptions,
  ): Promise<Output<T>[]>;
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
      /** Skip the countDocuments call(s); total/position will be undefined. */
      skipTotal?: boolean;
      /** Fetch one extra document to set hasMore cheaply; the extra row is dropped. */
      peek?: boolean;
    },
  ): Promise<{
    total?: number;
    position?: number;
    data: R[];
    hasMore?: boolean;
  }>;
  /**
   * Cross-pagination: paginate across multiple types simultaneously.
   * Documents from all specified types are merged and sorted together.
   *
   * @example
   * ```typescript
   * // Paginate collaborators and visitors together
   * const result = await catalog.paginate(
   *   ["collaborator", "visitor"],
   *   { active: true },
   *   { limit: 10, sort: { createdAt: -1 } }
   * );
   * // result.data is (Collaborator | Visitor)[]
   *
   * // With naturalIdSort to sort by ULID creation time (ignoring type prefix)
   * const result2 = await catalog.paginate(
   *   ["collaborator", "visitor"],
   *   {},
   *   { naturalIdSort: true }
   * );
   * ```
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
       * Only applies when sort includes _id or when using default sort.
       */
      naturalIdSort?: boolean;
      /** Pipeline stages to execute server-side before pagination (lookups, addFields, etc.) */
      pipeline?: (stage: StageBuilder<T>) => AggregationStage[];
      prepare?: (
        doc: ExtractByType<T, E[number]>,
      ) => Promise<EN> | EN;
      filter?: (doc: EN) => Promise<boolean> | boolean;
      format?: (doc: EN) => Promise<R> | R;
      /** Skip the countDocuments call(s); total/position will be undefined. */
      skipTotal?: boolean;
      /** Fetch one extra document to set hasMore cheaply; the extra row is dropped. */
      peek?: boolean;
    },
  ): Promise<{
    total?: number;
    position?: number;
    data: R[];
    hasMore?: boolean;
  }>;
  countDocuments<E extends keyof T>(
    key: E,
    filter?: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
    options?: m.CountDocumentsOptions,
  ): Promise<number>;
  deleteId<E extends keyof T>(key: E, id: string): Promise<number>;
  deleteIds<E extends keyof T>(key: E, ids: string[]): Promise<number>;
  deleteMany<E extends keyof T>(
    key: E,
    filter: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
  ): Promise<number>;
  deleteAny(filter: m.Filter<Input<T>>): Promise<number>;
  updateOne<E extends keyof T>(
    key: E,
    id: string,
    doc: Omit<
      WithRemovable<Partial<FlatType<v.InferInput<ElementSchema<T, E>>>>>,
      "_id" | "type"
    >,
  ): Promise<number>;
  updateMany(
    operation: {
      [key in keyof T]?: {
        [id: string]: Omit<
          WithRemovable<Partial<FlatType<v.InferInput<ElementSchema<T, key>>>>>,
          "_id" | "type"
        >;
      };
    },
  ): Promise<number>;
  aggregate(
    stageBuilder: (stage: StageBuilder<T>) => AggregationStage[],
  ): Promise<any[]>;
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
    log.debug(`applyValidator(${collectionName}): listCollections`);
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
      log.debug(`applyValidator(${collectionName}): createCollection`);
      // Create the collection with the validator
      await db.createCollection(collectionName, {
        validator,
      });
      log.debug(`applyValidator(${collectionName}): createCollection done`);
    } else {
      log.debug(`applyValidator(${collectionName}): exists, comparing validator`);
      // Check collection options
      const existingOptions = await db.command({
        listCollections: 1,
        filter: { name: collectionName },
      });
      const currentSchema =
        existingOptions.cursor?.firstBatch?.[0]?.options?.validator || {};

      const sameSchema = dirtyEquivalent(currentSchema, validator);

      if (sameSchema) {
        log.debug(`applyValidator(${collectionName}): validator unchanged, skipping`);
        return; // No need to update
      }

      log.debug(`applyValidator(${collectionName}): collMod (updating validator)`);
      // Update the collection with the validator
      await db.command({
        collMod: collectionName,
        validator,
      });
      log.debug(`applyValidator(${collectionName}): collMod done`);
    }
  }

  async function applyIndexes() {
    log.debug(`applyIndexes(${collectionName}): start`);
    await applyMultiCollectionIndexes(collection, schemaElements, {
      queue: mongoOperationQueue,
    });
    log.debug(`applyIndexes(${collectionName}): done`);
  }

  let sessionContext: Awaited<ReturnType<typeof getSessionContext>>;

  async function init() {
    log.debug(`init(${collectionName}): begin`);
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
    log.debug(
      `init(${collectionName}): shouldAutoApply=${shouldAutoApply} insideSession=${insideSession} useModel=${useModel}`,
    );

    if (shouldAutoApply && !insideSession) {
      await applyValidator();
      await applyIndexes();

      // Auto-initialize metadata for multi-collection model (only in auto mode)
      // In managed mode, migrations handle metadata creation
      // Check if metadata already exists
      log.debug(`init(${collectionName}): check multiCollectionInstanceExists`);
      const exists = await multiCollectionInstanceExists(db, collectionName);
      log.debug(`init(${collectionName}): exists=${exists}`);

      if (!exists) {
        // Get the current migration version
        log.debug(`init(${collectionName}): getLastAppliedMigration`);
        const lastMigration = await getLastAppliedMigration(db);
        const currentMigrationId = lastMigration?.id || "current";
        log.debug(`init(${collectionName}): migrationId=${currentMigrationId}`);

        // Create metadata for this instance
        if(useModel) {
          log.debug(`init(${collectionName}): createMultiCollectionInfo`);
          await createMultiCollectionInfo(
            db,
            collectionName,
            (model as MultiCollectionModel<T>).name,
            currentMigrationId,
          );
          log.debug(`init(${collectionName}): createMultiCollectionInfo done`);
        }
      }
    }
    log.debug(`init(${collectionName}): end`);
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
            const result = await collection.findOne({
                $and: [
                    { _type: key as string },
                    { _id: id },
                ]
            } as any, { session });

            if (!result) {
                throw new Error("No element found");
            }
            
            return v.parse(schema, result);
        },
        async findOne(key, filter) {
            const session = sessionContext.getSession();
            const result = await collection.findOne({
                $and: [
                    { _type: key as string },
                    filter,
                ]
            } as any, { session });

            if (!result) {
                return null;
            }
            
            return v.parse(schema, result);
        },
        async find(key, filter, options) {
            const typeChecker = {
                _type: key as string,
            };

            const session = sessionContext.getSession();
            const cursor = collection.find({
                $and: filter ? [typeChecker, filter] : [typeChecker],
            } as any, { session, ...options });
            
            const result = await cursor.toArray();
            let invalidsCount = 0;

            const output = result.map((item) => {
                const parsed = v.safeParse(schema, item);
                if(!parsed.success) {
                    invalidsCount++;
                    return null;
                }
                return parsed.output;
            }).filter((item): item is v.InferOutput<OutputElementSchema<T, typeof key>> => item !== null);
            
            return output;
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
                skipTotal?: boolean,
                peek?: boolean,
            }
        ) {
            const { skipTotal = false, peek = false } = options || {};
            const requestedLimit = options?.limit ?? 100;
            let limit = peek ? requestedLimit + 1 : requestedLimit;
            let { afterId, beforeId, sort, naturalIdSort, pipeline: pipelineBuilder, prepare, filter: customFilter, format } = options || {};
            const session = sessionContext.getSession();

            // Support both single key and array of keys for cross-pagination
            const keys = Array.isArray(keyOrKeys) ? keyOrKeys as (keyof T)[] : [keyOrKeys as keyof T];

            // Build type checker: single type or $in for multiple types
            const typeChecker = keys.length === 1
                ? { _type: keys[0] as string }
                : { _type: { $in: keys as string[] } };

            // Build the base query with type filter
            const baseQuery = filter ? [typeChecker, filter] : [typeChecker];
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

            // Create StageBuilder for pipeline support
            const createStageBuilder = (): StageBuilder<T> => ({
                match: (matchKey, matchFilter) => ({
                    $match: {
                        _type: matchKey as string,
                        ...matchFilter,
                    },
                }),
                unwind: (_unwindKey, field) => ({
                    $unwind: `$${field}`,
                }),
                lookup: (lookupKey, localField, foreignField, asOrOptions) => {
                    // Simple case: string parameter is the 'as' field name
                    // Automatically filter by _type for multi-collection support
                    if (typeof asOrOptions === 'string') {
                        return {
                            $lookup: {
                                from: collectionName,
                                let: { localValue: `$${localField}` },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $and: [
                                                    { $eq: [`$${foreignField}`, "$$localValue"] },
                                                    { $eq: ["$_type", lookupKey as string] },
                                                ],
                                            },
                                        },
                                    },
                                ],
                                as: asOrOptions,
                            },
                        };
                    }

                    // Advanced case: object with options
                    const lookupOptions = asOrOptions || {};
                    const as = lookupOptions.as || localField;
                    
                    // Build the lookup with automatic _type filter
                    const lookupStage: Record<string, unknown> = {
                        from: collectionName,
                        let: { localValue: `$${localField}`, ...(lookupOptions.let || {}) },
                        as,
                    };

                    // Build pipeline: start with _type match, then add user pipeline if provided
                    const basePipeline: AggregationStage[] = [
                        {
                            $match: {
                                $expr: {
                                    $and: [
                                        { $eq: [`$${foreignField}`, "$$localValue"] },
                                        { $eq: ["$_type", lookupKey as string] },
                                    ],
                                },
                            },
                        },
                    ];

                    // Add user-provided pipeline stages after the base filter
                    if (lookupOptions.pipeline) {
                        const userPipeline = lookupOptions.pipeline(createStageBuilder());
                        basePipeline.push(...userPipeline);
                    }

                    lookupStage.pipeline = basePipeline;

                    return { $lookup: lookupStage };
                },
                anyLookup: (localField, foreignField, asOrOptions) => {
                    // Simple case: string parameter is the 'as' field name
                    // No _type filter - matches any document type
                    if (typeof asOrOptions === 'string') {
                        return {
                            $lookup: {
                                from: collectionName,
                                localField,
                                foreignField,
                                as: asOrOptions,
                            },
                        };
                    }

                    // Advanced case: object with options
                    const anyLookupOptions = asOrOptions || {};
                    const as = anyLookupOptions.as || localField;
                    const anyLookupStage: Record<string, unknown> = {
                        from: collectionName,
                        localField,
                        foreignField,
                        as,
                    };

                    // Add let variables if provided
                    if (anyLookupOptions.let) {
                        anyLookupStage.let = anyLookupOptions.let;
                    }

                    // Add pipeline if provided (execute the builder function)
                    if (anyLookupOptions.pipeline) {
                        anyLookupStage.pipeline = anyLookupOptions.pipeline(createStageBuilder());
                    }

                    return { $lookup: anyLookupStage };
                },
                externalLookup: (fromCollection, localField, foreignField, asOrOptions) => {
                    // Simple case: string parameter is the 'as' field name
                    if (typeof asOrOptions === 'string') {
                        return {
                            $lookup: {
                                from: fromCollection,
                                localField,
                                foreignField,
                                as: asOrOptions,
                            },
                        };
                    }

                    // Advanced case: object with options
                    const extLookupOptions = asOrOptions || {};
                    const as = extLookupOptions.as || localField;
                    const extLookupStage: Record<string, unknown> = {
                        from: fromCollection,
                        localField,
                        foreignField,
                        as,
                    };

                    // Add let variables if provided
                    if (extLookupOptions.let) {
                        extLookupStage.let = extLookupOptions.let;
                    }

                    // Add pipeline if provided (raw pipeline, not using StageBuilder)
                    if (extLookupOptions.pipeline) {
                        extLookupStage.pipeline = extLookupOptions.pipeline;
                    }

                    return { $lookup: extLookupStage };
                },
                project: (projection) => ({
                    $project: projection,
                }),
                addFields: (fields) => ({
                    $addFields: fields,
                }),
                group: (grouping) => ({
                    $group: grouping,
                }),
                sort: (sortSpec) => ({
                    $sort: sortSpec,
                }),
                limit: (limitVal) => ({
                    $limit: limitVal,
                }),
                skip: (skipVal) => ({
                    $skip: skipVal,
                })
            });

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

            // Build user pipeline once — used both for the data fetch
            // below AND for the count above (so `total` reflects docs
            // that survive the pipeline's $match stages, not just the
            // base type filter).
            const userPipeline = pipelineBuilder ? pipelineBuilder(createStageBuilder()) : [];

            // Count total + position. When a user pipeline is present,
            // the count must reflect docs that survive the WHOLE
            // pipeline (e.g. $lookup-based JOIN filters), not just the
            // base $match — otherwise the UI shows misleading counts
            // when a pipeline narrows the result set.
            let total: number | undefined;
            let position: number | undefined;
            if (!skipTotal) {
                if (userPipeline.length > 0) {
                    const countPipeline: AggregationStage[] = [
                        { $match: { $and: baseQuery } },
                        ...userPipeline,
                        { $count: "total" },
                    ];
                    const totalResult = await collection.aggregate(countPipeline, { session }).toArray();
                    total = (totalResult[0]?.total as number | undefined) ?? 0;

                    if (afterId) {
                        const afterFilter = await buildCursorFilter(afterId, 'after');
                        if (afterFilter) {
                            const afterPipeline: AggregationStage[] = [
                                { $match: { $and: [...baseQuery, afterFilter] } },
                                ...userPipeline,
                                { $count: "total" },
                            ];
                            const afterResult = await collection.aggregate(afterPipeline, { session }).toArray();
                            const afterCount = (afterResult[0]?.total as number | undefined) ?? 0;
                            position = total - afterCount;
                        } else {
                            position = 1;
                        }
                    } else if (beforeId) {
                        position = -1;
                    } else {
                        position = 0;
                    }
                } else {
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
                        position = -1;
                    } else {
                        position = 0;
                    }
                }
            }

            if (pipelineBuilder || useNaturalIdSort) {
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

            // If peek was requested, pop the extra row (cursor's natural order, before any beforeId reverse)
            let hasMore: boolean | undefined;
            if (peek) {
                if (elements.length > requestedLimit) {
                    hasMore = true;
                    elements.pop();
                } else {
                    hasMore = false;
                }
            }

            // If paginating backwards (beforeId), reverse to maintain consistent order with forward pagination
            if (beforeId) {
                elements.reverse();
                // Calculate position: count of elements before the first returned element
                if (!skipTotal) {
                    const beforeFilter = await buildCursorFilter(beforeId, 'before');
                    if (beforeFilter) {
                        const beforeCount = await collection.countDocuments({ $and: [...baseQuery, beforeFilter] } as never, { session });
                        position = Math.max(0, beforeCount - elements.length);
                    } else {
                        position = 0;
                    }
                }
            }

            return {
                total,
                position,
                data: elements,
                ...(peek ? { hasMore } : {}),
            };
        },
        countDocuments(key, filter, options?) {
            const session = sessionContext.getSession();
            
            const typeChecker = {
                _type: key as string,
            };
            
            // Build the query using the same logic as find()
            const query = {
                $and: filter ? [typeChecker, filter] : [typeChecker],
            };
            
            return collection.countDocuments(query as never, { session, ...options });
        },
        async deleteId(key, id) {
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
        async deleteIds(key, ids) {
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
        async deleteMany(key, filter) {
            const session = sessionContext.getSession();

            // Combine the user filter with the type filter
            const combinedFilter = {
                ...filter,
                _type: key as string,
            } as any;

            const result = await collection.deleteMany(combinedFilter, { session });

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
        async findOneAny(filter) {
            const session = sessionContext.getSession();
            const result = await collection.findOne(filter as any, { session });
            if (!result) {
                return null;
            }
            return v.parse(schema, result);
        },
        async findAny(filter, options) {
            const session = sessionContext.getSession();
            const cursor = collection.find(filter as any, { session, ...options });
            const result = await cursor.toArray();

            const output = result.map((item) => {
                const parsed = v.safeParse(schema, item);
                if (!parsed.success) return null;
                return parsed.output;
            }).filter((item): item is Output<T> => item !== null);

            return output;
        },
        async updateOne(key, id, doc) {
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
                } as unknown as m.Filter<TOutput>, updateOps as m.UpdateFilter<TOutput>, { session });

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
        async updateMany(operation) {
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
            const stage: StageBuilder<T> = {
                match: (key, filter) => ({
                    $match: {
                        _type: key as string,
                        ...filter,
                    },
                }),
                unwind: (_key, field) => ({
                    $unwind: `$${field}`,
                }),
                lookup: (lookupKey, localField, foreignField, asOrOptions) => {
                    // Simple case: string parameter is the 'as' field name
                    // Automatically filter by _type for multi-collection support
                    if (typeof asOrOptions === 'string') {
                        return {
                            $lookup: {
                                from: collectionName,
                                let: { localValue: `$${localField}` },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $and: [
                                                    { $eq: [`$${foreignField}`, "$$localValue"] },
                                                    { $eq: ["$_type", lookupKey as string] },
                                                ],
                                            },
                                        },
                                    },
                                ],
                                as: asOrOptions,
                            },
                        };
                    }

                    // Advanced case: object with options
                    const options = asOrOptions || {};
                    const as = options.as || localField;
                    
                    // Build the lookup with automatic _type filter
                    const lookupStage: Record<string, unknown> = {
                        from: collectionName,
                        let: { localValue: `$${localField}`, ...(options.let || {}) },
                        as,
                    };

                    // Build pipeline: start with _type match, then add user pipeline if provided
                    const basePipeline: AggregationStage[] = [
                        {
                            $match: {
                                $expr: {
                                    $and: [
                                        { $eq: [`$${foreignField}`, "$$localValue"] },
                                        { $eq: ["$_type", lookupKey as string] },
                                    ],
                                },
                            },
                        },
                    ];

                    // Add user-provided pipeline stages after the base filter
                    if (options.pipeline) {
                        const userPipeline = options.pipeline(stage);
                        basePipeline.push(...userPipeline);
                    }

                    lookupStage.pipeline = basePipeline;

                    return { $lookup: lookupStage };
                },
                anyLookup: (localField, foreignField, asOrOptions) => {
                    // Simple case: string parameter is the 'as' field name
                    // No _type filter - matches any document type
                    if (typeof asOrOptions === 'string') {
                        return {
                            $lookup: {
                                from: collectionName,
                                localField,
                                foreignField,
                                as: asOrOptions,
                            },
                        };
                    }

                    // Advanced case: object with options
                    const anyLookupOptions = asOrOptions || {};
                    const as = anyLookupOptions.as || localField;
                    const anyLookupStage: Record<string, unknown> = {
                        from: collectionName,
                        localField,
                        foreignField,
                        as,
                    };

                    // Add let variables if provided
                    if (anyLookupOptions.let) {
                        anyLookupStage.let = anyLookupOptions.let;
                    }

                    // Add pipeline if provided (execute the builder function)
                    if (anyLookupOptions.pipeline) {
                        anyLookupStage.pipeline = anyLookupOptions.pipeline(stage);
                    }

                    return { $lookup: anyLookupStage };
                },
                externalLookup: (fromCollection, localField, foreignField, asOrOptions) => {
                    // Simple case: string parameter is the 'as' field name
                    if (typeof asOrOptions === 'string') {
                        return {
                            $lookup: {
                                from: fromCollection,
                                localField,
                                foreignField,
                                as: asOrOptions,
                            },
                        };
                    }

                    // Advanced case: object with options
                    const extLookupOptions = asOrOptions || {};
                    const as = extLookupOptions.as || localField;
                    const extLookupStage: Record<string, unknown> = {
                        from: fromCollection,
                        localField,
                        foreignField,
                        as,
                    };

                    // Add let variables if provided
                    if (extLookupOptions.let) {
                        extLookupStage.let = extLookupOptions.let;
                    }

                    // Add pipeline if provided (raw pipeline, not using StageBuilder)
                    if (extLookupOptions.pipeline) {
                        extLookupStage.pipeline = extLookupOptions.pipeline;
                    }

                    return { $lookup: extLookupStage };
                },
                project: (projection) => ({
                    $project: projection,
                }),
                addFields: (fields) => ({
                    $addFields: fields,
                }),
                group: (grouping) => ({
                    $group: grouping,
                }),
                sort: (sort) => ({
                    $sort: sort,
                }),
                limit: (limit) => ({
                    $limit: limit,
                }),
                skip: (skip) => ({
                    $skip: skip,
                })
            };

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
  log.debug(`createMultiCollectionInstance(${collectionName}): begin model=${model?.name}`);
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
  log.debug(`createMultiCollectionInstance(${collectionName}): multiCollectionInstanceExists`);
  const exists = await multiCollectionInstanceExists(db, collectionName);
  log.debug(`createMultiCollectionInstance(${collectionName}): exists=${exists}`);
  if (exists) {
    throw new Error(
      `Multi-collection instance "${collectionName}" already exists.`
    );
  }

  // Create collection with schema from model
  // This will apply validators, indexes, AND create metadata automatically
  // because we're using schemaManagement: "auto" with a model
  // The migration ID is determined from the current migration state
  log.debug(`createMultiCollectionInstance(${collectionName}): delegating to multiCollection()`);
  const result = await multiCollection(db, collectionName, model, {
    ...options,
    schemaManagement: "auto", // Force immediate application + metadata creation
  });
  log.debug(`createMultiCollectionInstance(${collectionName}): end`);
  return result;
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
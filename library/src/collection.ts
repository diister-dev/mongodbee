import * as v from "./schema.ts";
import { toMongoValidator } from "./validator.ts";
import { extractFieldsToRemove, sanitizeForMongoDB } from "./sanitizer.ts";
import { EventEmitter } from "./events.ts";
import { watchEvent } from "./change-stream.ts";
import { getSessionContext } from "./session.ts";
import { dirtyEquivalent } from "./utils/object.ts";
import { mongoOperationQueue } from "./operation.ts";
import { applyCollectionIndexes } from "./indexes-applier.ts";
import { retryOnWriteConflict } from "./utils/retry.ts";
import { isSchemaManaged } from "./runtime-config.ts";
import {
  assertSortResolvableBeforePipeline,
  buildCursorLadderBranches,
  buildExprCursorFilter,
  buildSortMachinery,
  buildSortPaginateStages,
  composeCursorQuery,
  normalizePaginateSort,
  type SortMachinery,
} from "./paginate-sort.ts";
import {
  createOperationTracer,
  filterKeys,
  type OpContext,
  registerClientTelemetry,
  TELEMETRY_ATTRIBUTES as TA,
  type TelemetryOptions,
  traced,
  updateOperators,
} from "./telemetry.ts";
import type { Db } from "./mongodb.ts";
import type * as m from "mongodb";

// Type for aggregation pipeline stages in simple collections
type AggregationStage = Record<string, unknown>;

// `_id` always exists — its cursor rungs stay raw comparisons (no null branch).
const NON_NULLABLE_SORT_FIELDS: ReadonlySet<string> = new Set(["_id"]);

/**
 * Stage builder for simple collections (not multi-collection)
 * Provides helpers for building aggregation pipeline stages
 */
type SimpleStageBuilder = {
  /** Match documents by filter */
  match: (filter: Record<string, unknown>) => AggregationStage;
  /** Unwind an array field */
  unwind: (field: string) => AggregationStage;
  /**
   * Lookup into the same collection
   * Useful for self-referential documents
   */
  lookup: (
    localField: string,
    foreignField: string,
    asOrOptions?: string | {
      as?: string;
      pipeline?: AggregationStage[];
      let?: Record<string, unknown>;
    },
  ) => AggregationStage;
  /**
   * Lookup into an external collection
   * Useful for joining with other MongoDB collections
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
  /** Project specific fields */
  project: (
    projection: Record<string, 1 | 0 | string | Record<string, unknown>>,
  ) => AggregationStage;
  /** Add computed fields */
  addFields: (fields: Record<string, unknown>) => AggregationStage;
  /** Group documents */
  group: (grouping: Record<string, unknown>) => AggregationStage;
  /** Sort documents */
  sort: (sort: Record<string, 1 | -1>) => AggregationStage;
  /** Limit number of documents */
  limit: (limit: number) => AggregationStage;
  /** Skip documents */
  skip: (skip: number) => AggregationStage;
};

type CollectionOptions = {
  safeDelete?: boolean;
  enableWatching?: boolean;
  /** How to handle undefined values in updates: 'remove' | 'ignore' | 'error' */
  undefinedBehavior?: "remove" | "ignore" | "error";
  // Initialization options
  noInit?: boolean; // If true, skip all initialization (validator, indexes, watching)
  /**
   * Override global schema management for this collection
   * - "auto": Apply validators/indexes automatically
   * - "managed": Skip auto-apply (migrations handle this)
   * - "inherit": Use global runtime config (default)
   */
  schemaManagement?: "auto" | "managed" | "inherit";
  /** Opt-in OpenTelemetry tracing for this collection's operations. */
  telemetry?: TelemetryOptions;
};

type WithId<T> = T extends { _id: unknown } ? T
  : m.WithId<T> | { _id: string } & T;

/**
 * Helper type that recursively allows symbol values (for removeField()) in nested objects
 */
type DeepWithRemovable<T> = T extends Record<string, unknown>
  ? { [K in keyof T]?: DeepWithRemovable<T[K]> | symbol }
  : T;

/**
 * Helper type that allows symbol values (for removeField()) in update operations
 * This makes all field values accept either their original type or symbol
 * Also accepts string keys for MongoDB dot notation (e.g., "items.0.price")
 * Recursively applies to nested objects
 */
type WithRemovableFields<T> =
  & {
    [K in keyof T]?: DeepWithRemovable<T[K]> | symbol;
  }
  & {
    [key: string]: unknown;
  };

/**
 * Update filter type that supports removeField() symbols in $set and other operators
 */
type UpdateFilterWithRemovable<T> =
  & Omit<m.UpdateFilter<T>, "$set" | "$setOnInsert">
  & {
    $set?: WithRemovableFields<T>;
    $setOnInsert?: WithRemovableFields<T>;
  };

/**
 * Process update filter to extract removeField() symbols from $set and convert to $unset
 */
function processUpdateWithRemoveField(
  update: Record<string, unknown>,
): Record<string, unknown> {
  const result = { ...update };

  // Process $set to extract removeField() symbols
  if (result.$set && typeof result.$set === "object") {
    const { set, unset } = extractFieldsToRemove(
      result.$set as Record<string, unknown>,
    );

    if (Object.keys(set).length > 0) {
      result.$set = set;
    } else {
      delete result.$set;
    }

    if (Object.keys(unset).length > 0) {
      result.$unset = {
        ...(result.$unset as Record<string, 1> || {}),
        ...unset,
      };
    }
  }

  return result;
}

type TInput<
  T extends Record<
    string,
    v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
  >,
> = v.InferInput<v.ObjectSchema<T, undefined>>;
type TOutput<
  T extends Record<
    string,
    v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
  >,
> = WithId<v.InferOutput<v.ObjectSchema<T, undefined>>>;

type Events<
  T extends Record<
    string,
    v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
  >,
> = {
  insert: (insertEvent: m.ChangeStreamInsertDocument<TOutput<T>>) => void;
  update: (updateEvent: m.ChangeStreamUpdateDocument<TOutput<T>>) => void;
  replace: (replaceEvent: m.ChangeStreamReplaceDocument<TOutput<T>>) => void;
  delete: (deleteEvent: m.ChangeStreamDeleteDocument<TOutput<T>>) => void;
};

/**
 * Type representing the enhanced MongoDB collection with validation and type safety
 * @template T - Schema type containing Valibot schemas for document fields
 */
export type CollectionResult<
  T extends Record<
    string,
    v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
  >,
> =
  & Omit<
    m.Collection<TInput<T>>,
    | "findOne"
    | "find"
    | "insertOne"
    | "updateOne"
    | "updateMany"
    | "distinct"
    | "findOneAndDelete"
    | "findOneAndReplace"
    | "findOneAndUpdate"
    | "indexInformation"
    | "listSearchIndexes"
    | "count"
  >
  & {
    collection: m.Collection<TInput<T>>;
    schema: v.ObjectSchema<
      { readonly _id: v.OptionalSchema<v.AnySchema, undefined> } & T,
      undefined
    >;
    on: ReturnType<typeof EventEmitter<Events<T>>>["on"];
    off: ReturnType<typeof EventEmitter<Events<T>>>["off"];
    insertOne: (
      doc: m.OptionalUnlessRequiredId<TInput<T>>,
      options?: m.InsertOneOptions,
    ) => Promise<WithId<TOutput<T>>["_id"]>;
    findOne: (
      filter: m.Filter<WithId<TInput<T>>>,
      options?: Omit<m.FindOptions, "timeoutMode"> & m.Abortable,
    ) => Promise<WithId<TOutput<T>> | null>;
    getById: (id: string | m.ObjectId) => Promise<WithId<TOutput<T>>>;
    find: (
      filter: m.Filter<TInput<T>>,
      options?: m.FindOptions & m.Abortable,
    ) => m.AbstractCursor<TOutput<T>>;
    findInvalid: (
      filter: m.Filter<TInput<T>>,
      options?: m.FindOptions & m.Abortable,
    ) => m.AbstractCursor<WithId<TInput<T>>>;
    withSession: Awaited<ReturnType<typeof getSessionContext>>["withSession"];

    // Utilities
    paginate: <E = WithId<TOutput<T>>, R = E>(
      filter: m.Filter<TInput<T>>,
      options?: {
        limit?: number;
        afterId?: string | m.ObjectId;
        beforeId?: string | m.ObjectId;
        sort?: m.Sort | m.SortDirection;
        prepare?: (doc: WithId<TOutput<T>>) => Promise<E> | E;
        filter?: (doc: E) => Promise<boolean> | boolean;
        format?: (doc: E) => Promise<R> | R;
        pipeline?: (stage: SimpleStageBuilder) => AggregationStage[];
        /**
         * Stages that run BEFORE the cursor match and the `$sort`, so `sort`
         * may reference fields they compute (e.g. sort by a `$lookup`ed
         * document's field). Unlike `pipeline` (which runs after the sort,
         * lazily over ~`limit` docs), these stages run over the whole
         * filtered set — keep them lean (join just what the sort needs).
         * Fields they add survive into the returned docs. Sort keys must be
         * scalar (`$first` a lookup result before sorting on it).
         */
        sortPipeline?: (stage: SimpleStageBuilder) => AggregationStage[];
        /**
         * Skip the `countDocuments` call(s). `total` and `position` will be
         * `undefined` in the result. Useful when the caller only needs the
         * page data and doesn't care about absolute position in the result set.
         */
        skipTotal?: boolean;
        /**
         * Fetch one extra document past `limit` to set `hasMore` cheaply
         * (no second count). The extra row is dropped before the result is
         * returned. Combine with `skipTotal: true` for fully count-free
         * pagination.
         */
        peek?: boolean;
      },
    ) => Promise<{
      total?: number;
      position?: number;
      data: R[];
      hasMore?: boolean;
    }>;

    // From mongodb.Collection
    updateOne(
      filter: m.Filter<WithId<TInput<T>>>,
      update: UpdateFilterWithRemovable<TInput<T>> | m.Document[],
      options?: m.UpdateOptions,
    ): Promise<m.UpdateResult<TInput<T>>>;
    updateMany(
      filter: m.Filter<TInput<T>>,
      update: UpdateFilterWithRemovable<TInput<T>> | m.Document[],
      options?: m.UpdateOptions,
    ): Promise<m.UpdateResult<TInput<T>>>;
    distinct<Key extends keyof WithId<TInput<T>>>(
      key: Key,
      filter: m.Filter<TInput<T>>,
      options?: m.DistinctOptions,
    ): Promise<Array<m.Flatten<WithId<TInput<T>>[Key]>>>;
    findOneAndDelete(
      filter: m.Filter<TInput<T>>,
      options?: m.FindOneAndDeleteOptions & { includeResultMetadata: boolean },
    ): Promise<WithId<TInput<T>> | null>;
    findOneAndReplace(
      filter: m.Filter<TInput<T>>,
      replacement: m.WithoutId<TInput<T>>,
      options?: m.FindOneAndReplaceOptions & { includeResultMetadata: boolean },
    ): Promise<m.ModifyResult<TInput<T>> | null>;
    findOneAndUpdate(
      filter: m.Filter<TInput<T>>,
      update: UpdateFilterWithRemovable<TInput<T>> | m.Document[],
      options?: m.FindOneAndUpdateOptions & { includeResultMetadata: boolean },
    ): Promise<m.WithId<TInput<T>> | null>;
    indexInformation(
      options: m.IndexInformationOptions & { full: true },
    ): Promise<m.IndexDescriptionInfo[]>;
    listSearchIndexes(
      name: string,
      options?: m.ListSearchIndexesOptions,
    ): m.ListSearchIndexesCursor;
  };

/**
 * Utility type for extracting the schema type from a collection
 * @template T - The CollectionResult type to extract the schema from
 */
export type CollectionSchema<T> = T extends CollectionResult<infer U>
  ? WithId<v.InferOutput<v.ObjectSchema<U, undefined>>>
  : never;

// Roadmap
//
// Create a collection with a validator from a valibot schema
//
// Objectives:
// 1. Support CRUD operations with validation
// 2. Support MongoDB json schema validation
// 3. Support aggregation strong typing : https://www.mongodb.com/docs/manual/reference/operator/aggregation/
// 4. Prevent insecure delete operations
//
// Details:
// 1. Support CRUD operations with validation
//    Create:
//        - [x] insertOne
//        - [x] insertMany
//    Read:
//        - [x] findOne
//        - [x] find
//    Update:
//        - [-] updateOne
//        - [x] replaceOne
//        - [-] updateMany
//    Delete:
//        - [x] deleteOne
//        - [x] deleteMany
//    Compound:
//        - [ ] findOneAndUpdate
//        - [ ] findOneAndReplace
//        - [ ] findOneAndDelete
//    Aggregate:
//        - [ ] aggregate
//        - [ ] bulkWrite
// 2. Support MongoDB json schema validation
//    - [x] Create a collection with a validator
//    - [x] Update a collection with a validator
//    - [ ] Validate a collection with a validator
//    - [ ] Validate a document with a validator
// 3. Support deep key validation (e.g. "a.b.c")
/**
 * Creates a type-safe MongoDB collection with schema validation
 *
 * This function creates or updates a MongoDB collection with built-in validation
 * based on the provided Valibot schema. It adds type safety and validation to
 * standard MongoDB operations.
 *
 * @param db - MongoDB database instance
 * @param collectionName - Name of the collection to create or use
 * @param collectionSchema - Valibot schema describing the document structure
 * @param options - Additional options for the collection
 * @returns A Promise resolving to an enhanced MongoDB collection with validation
 *
 * @example
 * ```typescript
 * const users = await collection(db, "users", {
 *   username: v.string(),
 *   email: v.pipe(v.string(), v.email()),
 *   age: v.pipe(v.number(), v.minValue(0))
 * });
 *
 * const userId = await users.insertOne({
 *   username: "john",
 *   email: "john@example.com",
 *   age: 30
 * });
 * ```
 */
export async function collection<
  const T extends Record<
    string,
    v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
  >,
>(
  db: Db,
  collectionName: string,
  collectionSchema: T,
  options?: m.CollectionOptions & CollectionOptions,
): Promise<CollectionResult<T>> {
  type TInput = v.InferInput<v.ObjectSchema<T, undefined>>;
  type TOutput = WithId<v.InferOutput<v.ObjectSchema<T, undefined>>>;

  const schema = v.object({
    _id: v.optional(v.any()),
    ...collectionSchema,
  });

  const opts: m.CollectionOptions & CollectionOptions = {
    ...{
      safeDelete: true,
      undefinedBehavior: "remove", // Default behavior
    },
    ...options,
  };

  const events = EventEmitter<Events<T>>();
  const validator = toMongoValidator(schema);
  const invalidValidation = { $nor: [validator] };

  async function applyValidator() {
    const collections = await db.listCollections({ name: collectionName })
      .toArray();

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
    await applyCollectionIndexes(collection, schema, {
      queue: mongoOperationQueue,
    });
  }

  async function startWatching() {
    watchEvent(db, collection, (change) => {
      switch (change.operationType) {
        case "insert":
          events.call(
            "insert",
            change as m.ChangeStreamInsertDocument<TOutput>,
          );
          break;
        case "update":
          events.call(
            "update",
            change as m.ChangeStreamUpdateDocument<TOutput>,
          );
          break;
        case "replace":
          events.call(
            "replace",
            change as m.ChangeStreamReplaceDocument<TOutput>,
          );
          break;
        case "delete":
          events.call(
            "delete",
            change as m.ChangeStreamDeleteDocument<TOutput>,
          );
          break;
        // Special case, watch will be closed (drop, dropDatabase)
        case "drop":
        case "dropDatabase":
          break;
        default:
          // Not handled yet
          break;
      }
    });

    // Prevent issue with MongoDB change stream not being ready
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  let sessionContext: Awaited<ReturnType<typeof getSessionContext>>;

  async function init() {
    sessionContext = getSessionContext(db.client);

    // If raw mode, skip all initialization
    if (opts.noInit) return;

    // Determine if we should auto-apply schema and indexes
    const shouldAutoApply = (() => {
      // Local option takes precedence
      if (opts.schemaManagement === "auto") return true;
      if (opts.schemaManagement === "managed") return false;
      // Default to "inherit" - use global config
      return !isSchemaManaged();
    })();

    // Prevent applying validator/indexes if a session is active
    const insideSession = !!sessionContext.getSession();

    if (shouldAutoApply && !insideSession) {
      await applyValidator();
      await applyIndexes();
    }

    // Only start watching if explicitly enabled
    if (opts.enableWatching) {
      await startWatching();
    }
  }

  const collection = db.collection<TInput>(collectionName, opts);
  await init();

  const tele = createOperationTracer(opts.telemetry, {
    dbName: db.databaseName,
    collectionName,
    getSession: () => sessionContext.getSession(),
  });
  registerClientTelemetry(db.client, opts.telemetry);

  return {
    // Raw collection
    collection,

    // Schema
    schema,

    // Events
    on: events.on,
    off: events.off,
    withSession: sessionContext!.withSession,

    get bsonOptions() {
      return collection.bsonOptions;
    },
    get collectionName() {
      return collection.collectionName;
    },
    get dbName() {
      return collection.dbName;
    },
    get hint() {
      return collection.hint;
    },
    get namespace() {
      return collection.namespace;
    },
    get readConcern() {
      return collection.readConcern;
    },
    get readPreference() {
      return collection.readPreference;
    },
    get timeoutMS() {
      return collection.timeoutMS;
    },
    get writeConcern() {
      return collection.writeConcern;
    },
    // Document creation operations with validation
    async insertOne(doc, options?) {
      const run = async () => {
        const validatedDoc = v.parse(schema, doc) as m.OptionalUnlessRequiredId<
          TInput
        >;

        // Apply sanitization based on configuration
        const safeDoc = sanitizeForMongoDB(validatedDoc, {
          undefinedBehavior: opts.undefinedBehavior || "remove",
          deep: true,
        }) as unknown as m.OptionalUnlessRequiredId<TInput>;

        const session = sessionContext.getSession();
        const inserted = await collection.insertOne(safeDoc, {
          session,
          ...options,
        });
        if (!inserted.acknowledged) {
          throw new Error("Insert failed");
        }
        return inserted.insertedId as WithId<TOutput>["_id"];
      };
      return traced(
        tele,
        "insertOne",
        undefined,
        run,
        () => ({ [TA.INSERTED_COUNT]: 1 }),
      );
    },
    async insertMany(docs, options?) {
      const run = async () => {
        const validatedDocs = docs.map((doc) => v.parse(schema, doc));

        // Apply sanitization based on configuration
        const safeDocs = validatedDocs.map((doc) =>
          sanitizeForMongoDB(doc, {
            undefinedBehavior: opts.undefinedBehavior || "remove",
            deep: true,
          }) as unknown as m.OptionalUnlessRequiredId<TInput>
        );

        const session = sessionContext.getSession();
        const inserted = await collection.insertMany(safeDocs, {
          session,
          ...options,
        });
        if (!inserted.acknowledged) {
          throw new Error("Insert failed");
        }
        return inserted;
      };
      return traced(
        tele,
        "insertMany",
        () => ({ [TA.BATCH_SIZE]: docs.length }),
        run,
        (r) => ({ [TA.INSERTED_COUNT]: r.insertedCount }),
      );
    },

    // Document read operations with validation
    async findOne(filter, options?) {
      const run = async () => {
        const session = sessionContext.getSession();
        const result = await collection.findOne({
          ...validator, // Prevent returning invalid documents
          ...filter as unknown as m.Filter<TInput>,
        }, { session, ...options });

        if (!result) {
          return null;
        }

        const validation = v.safeParse(schema, result);
        if (validation.success) {
          return validation.output as WithId<TOutput>;
        }

        throw {
          message: "Validation error",
          errors: validation,
          result,
        };
      };
      return traced(
        tele,
        "findOne",
        () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
        run,
        (r) => ({ [TA.RETURNED_ROWS]: r ? 1 : 0 }),
      );
    },
    async getById(id) {
      const run = async () => {
        const session = sessionContext.getSession();
        const result = await collection.findOne({ _id: id } as any, {
          session,
        });

        if (!result) {
          throw new Error("No element found");
        }

        const validation = v.safeParse(schema, result);
        if (validation.success) {
          return validation.output as WithId<TOutput>;
        }

        throw {
          message: "Validation error",
          errors: validation,
          result,
        };
      };
      return traced(
        tele,
        "getById",
        () => ({ [TA.FILTER_KEYS]: "_id" }),
        run,
        () => ({ [TA.RETURNED_ROWS]: 1 }),
      );
    },
    find(
      filter: m.Filter<TInput>,
      options?: m.FindOptions & m.Abortable,
    ): m.AbstractCursor<TOutput> {
      const session = sessionContext.getSession();
      const cursor = collection.find(filter, { session, ...options });
      const originalToArray = cursor.toArray;
      // Override toArray
      cursor.toArray = async function () {
        const results = await originalToArray.call(cursor);
        let invalidsCount = 0;

        const output = results.map((item) => {
          const validation = v.safeParse(schema, item);
          if (!validation.success) {
            invalidsCount++;
            return null;
          }
          return validation.output as m.WithId<TInput>;
        }).filter((item): item is m.WithId<TInput> => item !== null);

        if (invalidsCount > 0) {
          console.warn(
            `Warning: ${invalidsCount} invalid documents were ignored during find operation`,
          );
        }

        return output;
      };

      if (tele) {
        cursor.toArray = tele.wrapToArray("find", {
          [TA.FILTER_KEYS]: filterKeys(filter),
        }, cursor.toArray.bind(cursor));
      }

      return cursor as unknown as m.AbstractCursor<TOutput>;
    },
    findInvalid(
      filter: m.Filter<TInput>,
      options?: m.FindOptions & m.Abortable,
    ): m.AbstractCursor<TOutput> {
      const session = sessionContext.getSession();
      const cursor = collection.find({
        $and: [
          filter as any,
          invalidValidation,
        ],
      }, { session, ...options });

      const originalToArray = cursor.toArray;
      // Override toArray
      cursor.toArray = async function () {
        const results = await originalToArray.call(cursor);
        let invalidsCount = 0;
        const output = results.map((item) => {
          const validation = v.safeParse(schema, item);
          if (!validation.success) {
            invalidsCount++;
            return item as m.WithId<TInput>;
          }
          return null;
        }).filter((item): item is m.WithId<TInput> => item !== null);

        if (invalidsCount > 0) {
          console.warn(
            `Warning: ${invalidsCount} invalid documents were found during findInvalid operation`,
          );
        }
        return output;
      };

      if (tele) {
        cursor.toArray = tele.wrapToArray("findInvalid", {
          [TA.FILTER_KEYS]: filterKeys(filter),
        }, cursor.toArray.bind(cursor));
      }

      return cursor as unknown as m.AbstractCursor<TOutput>;
    },
    async paginate<E = WithId<TOutput>, R = E>(
      filter: m.Filter<TInput>,
      options?: {
        limit?: number;
        afterId?: string | m.ObjectId;
        beforeId?: string | m.ObjectId;
        sort?: m.Sort | m.SortDirection;
        prepare?: (doc: WithId<TOutput>) => Promise<E>;
        filter?: (doc: E) => Promise<boolean> | boolean;
        format?: (doc: E) => Promise<R>;
        pipeline?: (stage: SimpleStageBuilder) => AggregationStage[];
        sortPipeline?: (stage: SimpleStageBuilder) => AggregationStage[];
        skipTotal?: boolean;
        peek?: boolean;
      },
    ): Promise<{
      total?: number;
      position?: number;
      data: R[];
      hasMore?: boolean;
    }> {
      const run = async () => {
        const { skipTotal = false, peek = false } = options || {};
        const requestedLimit = options?.limit ?? 100;
        let limit = peek ? requestedLimit + 1 : requestedLimit;
        let {
          afterId,
          beforeId,
          sort,
          prepare,
          filter: customFilter,
          format,
          pipeline: pipelineBuilder,
          sortPipeline: sortPipelineBuilder,
        } = options || {};
        const session = sessionContext.getSession();
        const baseQuery: m.Filter<TInput> = { ...filter };
        let query: m.Filter<TInput> = { ...filter };

        // Normalize sort + direction-following `_id` tie-break (see
        // normalizePaginateSort for why the tie-break is not a fixed `1`).
        const sortObj = normalizePaginateSort(sort);
        sort = sortObj;

        // Resolve the anchor and build the flat cursor DNF branches — see
        // paginate-sort.ts for the null-boundary and rooted-$or reasoning.
        const buildCursorBranches = async (
          anchorId: string | m.ObjectId,
          direction: "after" | "before",
        ): Promise<Record<string, unknown>[] | null> => {
          const anchorDoc = await collection.findOne(
            { _id: anchorId } as m.Filter<TInput>,
            { session },
          );
          if (!anchorDoc) return null;
          return buildCursorLadderBranches({
            sortObj,
            anchorDoc: anchorDoc as Record<string, unknown>,
            direction,
            nonNullable: NON_NULLABLE_SORT_FIELDS,
          });
        };

        // Stage builder for simple collections
        const stageBuilder: SimpleStageBuilder = {
          match: (matchFilter: Record<string, unknown>) => ({
            $match: matchFilter,
          }),
          unwind: (field: string) => ({
            $unwind: field.startsWith("$") ? field : `$${field}`,
          }),
          lookup: (localField, foreignField, asOrOptions) => {
            const baseAs = typeof asOrOptions === "string"
              ? asOrOptions
              : asOrOptions?.as ?? localField;
            if (
              typeof asOrOptions === "object" &&
              (asOrOptions.pipeline || asOrOptions.let)
            ) {
              return {
                $lookup: {
                  from: collectionName,
                  localField,
                  foreignField,
                  as: baseAs,
                  ...(asOrOptions.let ? { let: asOrOptions.let } : {}),
                  ...(asOrOptions.pipeline
                    ? { pipeline: asOrOptions.pipeline }
                    : {}),
                },
              };
            }
            return {
              $lookup: {
                from: collectionName,
                localField,
                foreignField,
                as: baseAs,
              },
            };
          },
          externalLookup: (
            fromCollection,
            localField,
            foreignField,
            asOrOptions,
          ) => {
            const baseAs = typeof asOrOptions === "string"
              ? asOrOptions
              : asOrOptions?.as ?? localField;
            if (
              typeof asOrOptions === "object" &&
              (asOrOptions.pipeline || asOrOptions.let)
            ) {
              return {
                $lookup: {
                  from: fromCollection,
                  localField,
                  foreignField,
                  as: baseAs,
                  ...(asOrOptions.let ? { let: asOrOptions.let } : {}),
                  ...(asOrOptions.pipeline
                    ? { pipeline: asOrOptions.pipeline }
                    : {}),
                },
              };
            }
            return {
              $lookup: {
                from: fromCollection,
                localField,
                foreignField,
                as: baseAs,
              },
            };
          },
          project: (projection) => ({ $project: projection }),
          addFields: (fields) => ({ $addFields: fields }),
          group: (grouping) => ({ $group: grouping }),
          sort: (sortSpec) => ({ $sort: sortSpec }),
          limit: (limitVal) => ({ $limit: limitVal }),
          skip: (skipVal) => ({ $skip: skipVal }),
        };

        // Build aggregation pipeline if provided
        const customPipeline = pipelineBuilder
          ? pipelineBuilder(stageBuilder)
          : [];
        const sortStages = sortPipelineBuilder
          ? sortPipelineBuilder(stageBuilder)
          : [];
        assertSortResolvableBeforePipeline(
          Object.keys(sortObj),
          sortStages,
          customPipeline,
        );
        const sortMachinery = sortStages.length > 0
          ? buildSortMachinery(sortObj)
          : null;

        // Resolve the cursor anchor THROUGH the sort pipeline: the sort key
        // may only exist after those stages run (e.g. a $lookup'ed field), so
        // a raw findOne would yield `undefined` anchor values and a cursor
        // that restarts at page 1. Fail loud on a missing/dropped anchor —
        // this API is new, no silent page-1 restart to preserve.
        const resolveSortAnchor = async (
          anchorId: string | m.ObjectId,
          label: "afterId" | "beforeId",
        ): Promise<Record<string, unknown>> => {
          const rows = await collection.aggregate(
            [{ $match: { _id: anchorId } }, ...sortStages, { $limit: 1 }],
            { session },
          ).toArray();
          if (rows[0]) return rows[0] as Record<string, unknown>;
          const exists = await collection.findOne(
            { _id: anchorId } as m.Filter<TInput>,
            { session },
          );
          throw new Error(
            exists
              ? `paginate: ${label} was dropped by \`sortPipeline\` — cannot ` +
                `anchor the page (the anchor must survive the sort pipeline)`
              : `paginate: ${label} was not found — cannot anchor the page`,
          );
        };

        // Add pagination filters. With a sortPipeline, the cursor is an $expr
        // ladder over the hidden normalized sort keys (see paginate-sort.ts) —
        // query operators would silently drop docs whose sort key is missing.
        // Otherwise the cursor DNF is composed as a ROOTED $or with the user
        // filter folded per branch (see composeCursorQuery) — the composed
        // query REPLACES `query`, never merges into it: a spread once let the
        // cursor's $or silently overwrite a user filter's own $or.
        // Anchor-not-found is a DIVERGENT contract, deliberately: here (and
        // on multiCollection) a ghost anchor silently RESTARTS — afterId
        // yields page 1 with position 1, beforeId yields the LAST page with
        // position 0 — because the pinned consumer relies on the restart.
        // scopedMultiCollection throws instead, and the sortPipeline path
        // throws on every surface. Pinned by paginate-anchor-not-found.test.
        let exprCursor: AggregationStage | null = null;
        let cursorBranches: Record<string, unknown>[] | null = null;
        if (afterId) {
          if (sortMachinery) {
            const anchor = await resolveSortAnchor(afterId, "afterId");
            exprCursor = buildExprCursorFilter(sortMachinery, anchor, "after");
          } else {
            cursorBranches = await buildCursorBranches(afterId, "after");
            if (cursorBranches) {
              query = composeCursorQuery(
                [baseQuery as Record<string, unknown>],
                cursorBranches,
              ) as m.Filter<TInput>;
            }
          }
        } else if (beforeId) {
          if (sortMachinery) {
            const anchor = await resolveSortAnchor(beforeId, "beforeId");
            exprCursor = buildExprCursorFilter(sortMachinery, anchor, "before");
          } else {
            cursorBranches = await buildCursorBranches(beforeId, "before");
            if (cursorBranches) {
              query = composeCursorQuery(
                [baseQuery as Record<string, unknown>],
                cursorBranches,
              ) as m.Filter<TInput>;
            }
          }
          // Reverse the sort for beforeId to get items in reverse order
          const reversedSort: Record<string, 1 | -1> = {};
          for (const [field, dir] of Object.entries(sortObj)) {
            reversedSort[field] = (dir === 1 ? -1 : 1) as 1 | -1;
          }
          sort = reversedSort;
        }

        // Count helper for the sortPipeline path. Counts MUST mirror the data
        // assembly (base → sortPipeline → normalize → cursor): applying the
        // $expr cursor to a pipeline that never ran the sort stages would
        // compare against fields that don't exist and corrupt `position`.
        const countViaSortPipeline = async (
          machinery: SortMachinery,
          cursor: AggregationStage | null,
        ): Promise<number> => {
          const rows = await collection.aggregate(
            buildSortPaginateStages({
              baseMatch: baseQuery as Record<string, unknown>,
              sortStages,
              machinery,
              cursorFilter: cursor,
              pipeline: customPipeline,
              count: true,
            }),
            { session },
          ).toArray();
          return (rows[0]?.total as number | undefined) ?? 0;
        };

        // Count total + position. When a custom pipeline is present, the
        // count must reflect docs that pass through the WHOLE pipeline
        // (e.g. $lookup-based JOIN filters), not just the base $match —
        // otherwise the UI shows misleading "2 / 15" when only 2 docs
        // survive the pipeline. We run a sibling aggregation that mirrors
        // the data pipeline up to the matching stages and appends $count.
        let total: number | undefined;
        let position: number | undefined;
        if (!skipTotal) {
          if (sortMachinery) {
            total = await countViaSortPipeline(sortMachinery, null);
            if (afterId) {
              position = total -
                (await countViaSortPipeline(sortMachinery, exprCursor));
            } else if (beforeId) {
              position = -1;
            } else {
              position = 0;
            }
          } else if (customPipeline.length > 0) {
            const countPipeline: m.Document[] = [
              { $match: baseQuery },
              ...customPipeline,
              { $count: "total" },
            ];
            const totalResult = await collection.aggregate(countPipeline, {
              session,
            }).toArray();
            total = (totalResult[0]?.total as number | undefined) ?? 0;

            if (afterId) {
              if (cursorBranches) {
                const afterPipeline: m.Document[] = [
                  {
                    $match: composeCursorQuery(
                      [baseQuery as Record<string, unknown>],
                      cursorBranches,
                    ),
                  },
                  ...customPipeline,
                  { $count: "total" },
                ];
                const afterResult = await collection.aggregate(afterPipeline, {
                  session,
                }).toArray();
                const afterCount =
                  (afterResult[0]?.total as number | undefined) ?? 0;
                position = total - afterCount;
              } else {
                position = 1;
              }
            } else if (beforeId) {
              position = -1; // computed post-fetch (same marker as the find-path)
            } else {
              position = 0;
            }
          } else {
            // Find-style fast path: countDocuments is cheaper than aggregate.
            total = await collection.countDocuments(baseQuery, { session });

            if (afterId) {
              if (cursorBranches) {
                const afterCount = await collection.countDocuments(
                  composeCursorQuery(
                    [baseQuery as Record<string, unknown>],
                    cursorBranches,
                  ) as m.Filter<TInput>,
                  { session },
                );
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

        let hardLimit = 10_000;
        const elements: R[] = [];

        // Use aggregation pipeline when custom pipeline is provided
        if (sortMachinery || customPipeline.length > 0) {
          const aggregationPipeline: m.Document[] = sortMachinery
            // sortPipeline path: the stages the sort depends on run over the
            // whole filtered set (the sort needs every value); the after-sort
            // `pipeline` stays lazy over the ~`limit` docs the loop consumes.
            ? buildSortPaginateStages({
              baseMatch: baseQuery as Record<string, unknown>,
              sortStages,
              machinery: sortMachinery,
              cursorFilter: exprCursor,
              pipeline: customPipeline,
              reverse: Boolean(beforeId),
            })
            : [
              { $match: query },
              { $sort: sort },
              ...customPipeline,
            ];

          const cursor = collection.aggregate(aggregationPipeline, { session });

          try {
            while (hardLimit-- > 0 && limit > 0) {
              const doc = await cursor.next() as WithId<TOutput> | null;
              if (!doc) break;

              // Validate document with schema (only original fields, not lookup fields)
              const validation = v.safeParse(schema, doc);
              if (!validation.success) {
                continue; // Skip invalid documents
              }

              // Merge original doc (with lookup fields) with validated output
              const validatedDoc = { ...doc, ...validation.output } as WithId<
                TOutput
              >;

              // Step 1: Prepare - enrich document with external data
              const enrichedDoc = prepare
                ? await prepare(validatedDoc)
                : validatedDoc as unknown as E;

              // Step 2: Filter - apply custom filtering logic
              const isValid = await customFilter?.(enrichedDoc) ?? true;
              if (!isValid) continue;

              // Step 3: Format - transform document to final output format
              const finalDoc = format
                ? await format(enrichedDoc)
                : enrichedDoc as unknown as R;

              elements.push(finalDoc);
              limit--;
            }
          } finally {
            await cursor.close();
          }
        } else {
          // Use simple find for non-pipeline queries
          const cursor = collection.find(query, { session }).sort(
            sort as m.Sort,
          );

          try {
            while (hardLimit-- > 0 && limit > 0) {
              const doc = await cursor.next() as WithId<TOutput> | null;
              if (!doc) break;

              // Validate document with schema
              const validation = v.safeParse(schema, doc);
              if (!validation.success) {
                continue; // Skip invalid documents
              }

              const validatedDoc = validation.output as WithId<TOutput>;

              // Step 1: Prepare - enrich document with external data
              const enrichedDoc = prepare
                ? await prepare(validatedDoc)
                : validatedDoc as unknown as E;

              // Step 2: Filter - apply custom filtering logic
              const isValid = await customFilter?.(enrichedDoc) ?? true;
              if (!isValid) continue;

              // Step 3: Format - transform document to final output format
              const finalDoc = format
                ? await format(enrichedDoc)
                : enrichedDoc as unknown as R;

              elements.push(finalDoc);
              limit--;
            }
          } finally {
            await cursor.close();
          }
        }

        // If peek was requested, pop the extra row (fetched in cursor's natural order, before any beforeId reverse)
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
          // After reverse, elements[0] is the earliest in the sorted order
          // Position = total elements before anchor - elements returned
          if (!skipTotal) {
            if (sortMachinery) {
              // exprCursor is always set here — resolveSortAnchor throws
              // instead of returning null.
              const beforeCount = exprCursor
                ? await countViaSortPipeline(sortMachinery, exprCursor)
                : 0;
              position = Math.max(0, beforeCount - elements.length);
            } else if (cursorBranches) {
              // Counted through the SAME shape as `total`: with a filtering
              // `pipeline`, a bare countDocuments would count dropped docs
              // too and `position` would overshoot (multi and scoped already
              // count through the pipeline).
              const beforeQuery = composeCursorQuery(
                [baseQuery as Record<string, unknown>],
                cursorBranches,
              );
              let beforeCount: number;
              if (customPipeline.length > 0) {
                const rows = await collection.aggregate([
                  { $match: beforeQuery },
                  ...customPipeline,
                  { $count: "total" },
                ], { session }).toArray();
                beforeCount = (rows[0]?.total as number | undefined) ?? 0;
              } else {
                beforeCount = await collection.countDocuments(
                  beforeQuery as m.Filter<TInput>,
                  { session },
                );
              }
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
      };
      return traced(
        tele,
        "paginate",
        () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
        run,
        (r) => ({ [TA.RETURNED_ROWS]: r.data.length }),
      );
    },
    countDocuments(filter, options?) {
      const run = () => {
        const session = sessionContext.getSession();
        return collection.countDocuments(filter, { session, ...options });
      };
      return traced(
        tele,
        "countDocuments",
        () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
        run,
      );
    },
    estimatedDocumentCount(options?) {
      const run = () => {
        const session = sessionContext.getSession();
        return collection.estimatedDocumentCount({ session, ...options });
      };
      return traced(tele, "estimatedDocumentCount", undefined, run);
    },
    distinct(key, filter, options?) {
      const run = () => {
        const session = sessionContext.getSession();
        return collection.distinct(key as string, filter, {
          session,
          ...options,
        });
      };
      return traced(
        tele,
        "distinct",
        () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
        run,
        (r) => ({ [TA.RETURNED_ROWS]: r.length }),
      );
    },

    // Document update operations
    replaceOne(filter, replacement, options?) {
      const run = () => {
        const validation = v.safeParse(schema, replacement);
        if (!validation.success) {
          throw {
            message: "Validation error",
            errors: validation,
          };
        }

        const sanitizedReplacement = sanitizeForMongoDB(validation.output, {
          undefinedBehavior: opts.undefinedBehavior || "remove",
          deep: true,
        }) as unknown as TInput;
        const session = sessionContext.getSession();
        return collection.replaceOne(filter, sanitizedReplacement, {
          session,
          ...options,
        });
      };
      return traced(
        tele,
        "replaceOne",
        () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
        run,
        (r) => ({
          [TA.MATCHED_COUNT]: r.matchedCount,
          [TA.MODIFIED_COUNT]: r.modifiedCount,
          [TA.UPSERTED_COUNT]: r.upsertedCount,
        }),
      );
    },
    updateOne(filter, update, options?) {
      // @TODO: check if update is valid
      const run = (op?: OpContext) =>
        retryOnWriteConflict(async () => {
          // Process removeField() symbols in $set before sanitization
          const processedUpdate = processUpdateWithRemoveField(
            update as Record<string, unknown>,
          );
          const sanitizedUpdate = sanitizeForMongoDB(processedUpdate, {
            undefinedBehavior: opts.undefinedBehavior || "remove",
            deep: true,
          });
          const session = sessionContext.getSession();
          return await collection.updateOne(
            filter as any,
            sanitizedUpdate as any,
            {
              session,
              ...options,
            },
          );
        }, op ? { onRetry: op.onRetry } : undefined);
      return traced(
        tele,
        "updateOne",
        () => ({
          [TA.FILTER_KEYS]: filterKeys(filter),
          [TA.UPDATE_OPERATORS]: updateOperators(update),
        }),
        run,
        (r) => ({
          [TA.MATCHED_COUNT]: r.matchedCount,
          [TA.MODIFIED_COUNT]: r.modifiedCount,
          [TA.UPSERTED_COUNT]: r.upsertedCount,
        }),
      );
    },
    updateMany(filter, update, options?) {
      // @TODO: check if update is valid
      const run = (op?: OpContext) =>
        retryOnWriteConflict(async () => {
          // Process removeField() symbols in $set before sanitization
          const processedUpdate = processUpdateWithRemoveField(
            update as Record<string, unknown>,
          );
          const sanitizedUpdate = sanitizeForMongoDB(processedUpdate, {
            undefinedBehavior: opts.undefinedBehavior || "remove",
            deep: true,
          });
          const session = sessionContext.getSession();
          return await collection.updateMany(filter, sanitizedUpdate as any, {
            session,
            ...options,
          });
        }, op ? { onRetry: op.onRetry } : undefined);
      return traced(
        tele,
        "updateMany",
        () => ({
          [TA.FILTER_KEYS]: filterKeys(filter),
          [TA.UPDATE_OPERATORS]: updateOperators(update),
        }),
        run,
        (r) => ({
          [TA.MATCHED_COUNT]: r.matchedCount,
          [TA.MODIFIED_COUNT]: r.modifiedCount,
          [TA.UPSERTED_COUNT]: r.upsertedCount,
        }),
      );
    },

    // Document delete operations
    deleteOne(filter, options?) {
      const run = () => {
        const session = sessionContext.getSession();
        return collection.deleteOne(filter, { session, ...options });
      };
      return traced(
        tele,
        "deleteOne",
        () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
        run,
        (r) => ({ [TA.DELETED_COUNT]: r.deletedCount }),
      );
    },
    deleteMany(filter, options?) {
      const run = () => {
        if (opts.safeDelete) {
          const filterSize = Object.keys(filter ?? {}).length;
          if (filterSize === 0) throw new Error("Filter is empty");

          let anyValidFilter = false;
          for (const key in filter) {
            if (key === "_id") continue;
            const value = filter[key];
            if (value !== undefined) {
              anyValidFilter = true;
              break;
            }
          }

          if (!anyValidFilter) {
            throw new Error("Filter is empty or only contains _id");
          }
        }

        const session = sessionContext.getSession();
        return collection.deleteMany(filter, { session, ...options });
      };
      return traced(
        tele,
        "deleteMany",
        () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
        run,
        (r) => ({ [TA.DELETED_COUNT]: r.deletedCount }),
      );
    },

    // Compound operations
    findOneAndDelete(filter, options?) {
      const run = () => {
        const session = sessionContext.getSession();
        return collection.findOneAndDelete(filter, { session, ...options });
      };
      return traced(
        tele,
        "findOneAndDelete",
        () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
        run,
      );
    },
    findOneAndReplace(filter, replacement, options?) {
      const run = () => {
        const validation = v.safeParse(schema, replacement);
        if (!validation.success) {
          throw {
            message: "Validation error",
            errors: validation,
          };
        }

        const sanitizedReplacement = sanitizeForMongoDB(validation.output, {
          undefinedBehavior: opts.undefinedBehavior || "remove",
          deep: true,
        }) as unknown as TInput;

        const session = sessionContext.getSession();
        return collection.findOneAndReplace(filter, sanitizedReplacement, {
          session,
          ...options,
        });
      };
      return traced(
        tele,
        "findOneAndReplace",
        () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
        run,
      );
    },
    findOneAndUpdate(filter, update, options?) {
      const run = () => {
        // Process removeField() symbols in $set before sanitization
        const processedUpdate = processUpdateWithRemoveField(
          update as Record<string, unknown>,
        );
        const sanitizedUpdate = sanitizeForMongoDB(processedUpdate, {
          undefinedBehavior: opts.undefinedBehavior || "remove",
          deep: true,
        });
        const session = sessionContext.getSession();
        return collection.findOneAndUpdate(filter, sanitizedUpdate as any, {
          session,
          ...options,
        });
      };
      return traced(
        tele,
        "findOneAndUpdate",
        () => ({
          [TA.FILTER_KEYS]: filterKeys(filter),
          [TA.UPDATE_OPERATORS]: updateOperators(update),
        }),
        run,
      );
    },

    // Bulk operations
    aggregate(pipeline, options?) {
      const session = sessionContext.getSession();
      return collection.aggregate(pipeline, { session, ...options });
    },
    bulkWrite(operations, options?) {
      const run = () => {
        const session = sessionContext.getSession();
        return collection.bulkWrite(operations, { session, ...options });
      };
      return traced(
        tele,
        "bulkWrite",
        () => ({ [TA.BATCH_SIZE]: operations.length }),
        run,
        (r) => ({
          [TA.INSERTED_COUNT]: r.insertedCount,
          [TA.MATCHED_COUNT]: r.matchedCount,
          [TA.MODIFIED_COUNT]: r.modifiedCount,
          [TA.UPSERTED_COUNT]: r.upsertedCount,
          [TA.DELETED_COUNT]: r.deletedCount,
        }),
      );
    },
    initializeOrderedBulkOp(options?) {
      const session = sessionContext.getSession();
      return collection.initializeOrderedBulkOp({ session, ...options });
    },
    initializeUnorderedBulkOp(options?) {
      const session = sessionContext.getSession();
      return collection.initializeUnorderedBulkOp({ session, ...options });
    },

    // Index operations
    createIndex(indexSpec, options?) {
      const session = sessionContext.getSession();
      return collection.createIndex(indexSpec, { session, ...options });
    },
    createIndexes(indexSpecs, options?) {
      const session = sessionContext.getSession();
      return collection.createIndexes(indexSpecs, { session, ...options });
    },
    dropIndex(indexName, options?) {
      const session = sessionContext.getSession();
      return collection.dropIndex(indexName, { session, ...options });
    },
    dropIndexes(options?) {
      const session = sessionContext.getSession();
      return collection.dropIndexes({ session, ...options });
    },
    indexes(options?) {
      const session = sessionContext.getSession();
      return collection.indexes({ session, ...options });
    },
    listIndexes(options?) {
      const session = sessionContext.getSession();
      return collection.listIndexes({ session, ...options });
    },
    indexExists(indexes, options?) {
      const session = sessionContext.getSession();
      return collection.indexExists(indexes, { session, ...options });
    },
    indexInformation(options) {
      const session = sessionContext.getSession();
      return collection.indexInformation({ session, ...options });
    },

    // Search operations
    createSearchIndex(description) {
      return collection.createSearchIndex(description);
    },
    createSearchIndexes(descriptions) {
      return collection.createSearchIndexes(descriptions);
    },
    dropSearchIndex(name) {
      return collection.dropSearchIndex(name);
    },
    listSearchIndexes(name, options?) {
      const session = sessionContext.getSession();
      return collection.listSearchIndexes(name, { session, ...options });
    },
    updateSearchIndex(name, indexSpec) {
      return collection.updateSearchIndex(name, indexSpec);
    },

    // Collection operations
    drop(options?) {
      const session = sessionContext.getSession();
      return collection.drop({ session, ...options });
    },
    isCapped(options?) {
      const session = sessionContext.getSession();
      return collection.isCapped({ session, ...options });
    },
    options(options?) {
      const session = sessionContext.getSession();
      return collection.options({ session, ...options });
    },
    rename(newName, options?) {
      const session = sessionContext.getSession();
      return collection.rename(newName, { session, ...options });
    },
    watch(pipeline, options?) {
      const session = sessionContext.getSession();
      return collection.watch(pipeline, { session, ...options });
    },
  } as CollectionResult<T>;
}

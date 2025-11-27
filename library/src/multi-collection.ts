import * as v from "./schema.ts";
import * as m from "mongodb";
import { ulid } from "@std/ulid";
import { toMongoValidator } from "./validator.ts";
import { extractFieldsToRemove, sanitizeForMongoDB } from "./sanitizer.ts";
import { createDotNotationSchema } from "./dot-notation.ts";
import { getSessionContext } from "./session.ts";
import {
  extractIndexes,
  keyEqual,
  normalizeIndexOptions,
  withIndex,
} from "./indexes.ts";
import { sanitizePathName } from "./schema-navigator.ts";
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

/**
 * Generates a new unique ID using ULID
 *
 * @returns A new ULID string in lowercase
 */
function newId() {
  return ulid().toLowerCase();
}

/**
 * Creates an optional ID field for a document type with automatic generation
 *
 * @param type - The document type identifier to use in the ID prefix
 * @returns A Valibot schema for an ID field with optional auto-generation
 */
export function dbId(
  type: string,
): v.OptionalSchema<
  v.SchemaWithPipe<
    readonly [v.StringSchema<undefined>, v.RegexAction<string, undefined>]
  >,
  () => string
> {
  return v.optional(refId(type), () => `${type}:${newId()}`);
}

/**
 * Creates a reference ID field that must match a specific type prefix
 *
 * @param type - The document type identifier that must prefix the ID
 * @returns A Valibot schema for validating reference IDs
 */
export function refId(
  type: string,
): v.SchemaWithPipe<
  readonly [v.StringSchema<undefined>, v.RegexAction<string, undefined>]
> {
  return v.pipe(v.string(), v.regex(new RegExp(`^${type}:[a-zA-Z0-9]+`)));
}

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
  paginate<E extends keyof T, EN = v.InferOutput<OutputElementSchema<T, E>>, R = EN>(
    key: E,
    filter?: m.Filter<v.InferInput<OutputElementSchema<T, E>>>,
    options?: {
      limit?: number;
      afterId?: string;
      beforeId?: string;
      sort?: m.Sort | m.SortDirection;
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
    // Determine if we should auto-apply schema and indexes
    const shouldAutoApply = (() => {
      // Local option takes precedence
      if (opts.schemaManagement === "auto") return true;
      if (opts.schemaManagement === "managed") return false;
      // Default to "inherit" - use global config
      return !isSchemaManaged();
    })();

    if (shouldAutoApply) {
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

    sessionContext = getSessionContext(db.client);
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
        async paginate<E extends keyof T, EN = v.InferOutput<OutputElementSchema<T, E>>, R = EN>(
            key: E, 
            filter?: m.Filter<v.InferInput<OutputElementSchema<T, E>>>, 
            options?: {
                limit?: number,
                afterId?: string,
                beforeId?: string,
                sort?: m.Sort | m.SortDirection,
                prepare?: (doc: v.InferOutput<OutputElementSchema<T, E>>) => Promise<EN>,
                filter?: (doc: EN) => Promise<boolean> | boolean,
                format?: (doc: EN) => Promise<R>,
            }
        ) {
            let { limit = 100, afterId, beforeId, sort, prepare, filter: customFilter, format } = options || {};
            const session = sessionContext.getSession();
            
            const typeChecker = {
                _type: key as string,
            };
            
            // Build the base query with type filter
            const baseQuery = filter ? [typeChecker, filter] : [typeChecker];
            let query: Record<string, unknown> = { $and: baseQuery };

            // Add pagination filters
            if (afterId) {
              if (!afterId.startsWith(`${key as string}:`)) {
                  throw new Error(`Invalid afterId format for type ${key as string}`);
              }
              query = {
                  $and: [
                      ...baseQuery,
                      { _id: { $gt: afterId } }
                  ]
              };
              sort = sort || { _id: 1 };
            } else if (beforeId) {
              if (!beforeId.startsWith(`${key as string}:`)) {
                  throw new Error(`Invalid beforeId format for type ${key as string}`);
              }
              query = {
                  $and: [
                      ...baseQuery,
                      { _id: { $lt: beforeId } }
                  ]
              };
              sort = sort || { _id: -1 };
            } else {
              sort = sort || { _id: 1 };
            }

            let total: number | undefined;
            let position: number | undefined;
            {
                const baseCountQuery = { $and: baseQuery };
                total = await collection.countDocuments(baseCountQuery as never, { session });
                
                if (afterId) {
                    const positionQuery = {
                        $and: [
                            ...baseQuery,
                            { _id: { $lte: afterId } }
                        ]
                    };
                    position = await collection.countDocuments(positionQuery as never, { session });
                } else if (beforeId) {
                    const positionQuery = {
                        $and: [
                            ...baseQuery,
                            { _id: { $gte: beforeId } }
                        ]
                    };
                    const remainingCount = await collection.countDocuments(positionQuery as never, { session });
                    position = total - remainingCount;
                } else {
                    position = 0;
                }
            }

            const cursor = collection.find(query as never, { session }).sort(sort as m.Sort);
            let hardLimit = 10_000;
            const elements: R[] = [];
            
            while(hardLimit-- > 0 && limit > 0) {
                const doc = await cursor.next();
                if (!doc) break;

                // Validate document with schema
                const validation = v.safeParse(schema, doc);
                if (!validation.success) {
                    continue; // Skip invalid documents
                }

                const validatedDoc = validation.output as v.InferOutput<OutputElementSchema<T, E>>;
                
                // Step 1: Prepare - enrich document with external data
                const enrichedDoc = prepare ? await prepare(validatedDoc) : validatedDoc as unknown as EN;
                
                // Step 2: Filter - apply custom filtering logic
                const isValid = await customFilter?.(enrichedDoc) ?? true;
                if (!isValid) continue;
                
                // Step 3: Format - transform document to final output format
                const finalDoc = format ? await format(enrichedDoc) : enrichedDoc as unknown as R;
                
                elements.push(finalDoc);
                limit--;
            }

            // If paginating backwards (beforeId) and no explicit sort was provided, reverse the results to maintain chronological order
            if(beforeId) {
                elements.reverse();
                position = (position || 0) - elements.length;
                position = position < 0 ? 0 : position;
            }

            return {
                total,
                position,
                data: elements,
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
                unwind: (key, field) => ({
                    $unwind: `$${field}`,
                }),
                lookup: (key, localField, foreignField, asOrOptions) => {
                    // Simple case: string parameter is the 'as' field name
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
                    const options = asOrOptions || {};
                    const as = options.as || localField;
                    const lookupStage: Record<string, unknown> = {
                        from: collectionName,
                        localField,
                        foreignField,
                        as,
                    };

                    // Add let variables if provided
                    if (options.let) {
                        lookupStage.let = options.let;
                    }

                    // Add pipeline if provided (execute the builder function)
                    if (options.pipeline) {
                        lookupStage.pipeline = options.pipeline(stage);
                    }

                    return { $lookup: lookupStage };
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
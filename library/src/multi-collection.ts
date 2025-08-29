import * as v from './schema.ts';
import * as m from "mongodb";
import { ulid } from "@std/ulid";
import { toMongoValidator } from "./validator.ts";
import { sanitizeForMongoDB } from "./sanitizer.ts";
import { createDotNotationSchema } from "./dot-notation.ts";
import { getSessionContext } from "./session.ts";
import { extractIndexes, withIndex } from "./indexes.ts";
import { sanitizePathName } from "./schema-navigator.ts";
import type { FlatType } from "../types/flat.ts";
import type { Db } from "./mongodb.ts";

type CollectionOptions = {
    safeDelete?: boolean,
    enableWatching?: boolean,
    /** How to handle undefined values in updates: 'remove' | 'ignore' | 'error' */
    undefinedBehavior?: 'remove' | 'ignore' | 'error',
}

// Use _id if the schema is a literal schema, otherwise use dbId
type DynId<T> = T extends v.LiteralSchema<any, any> ? T : ReturnType<typeof dbId>;

type Elements<T extends Record<string, any>> = {
    [key in keyof T]: {
        _id: DynId<T[key]["_id"]>,
        _type: v.LiteralSchema<key, any>,
    } & T[key]
}[keyof T];

type OutputElementSchema<T extends Record<string, any>, K extends keyof T> = v.ObjectSchema<{
    _id: DynId<T[K]["_id"]>,
    _type: v.LiteralSchema<K, any>,
} & T[K], any>;

type ElementSchema<T extends Record<string, any>, K extends keyof T> = v.ObjectSchema<{
    _id: DynId<T[K]["_id"]>,
} & T[K], any>;

type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;
type MultiSchema<T extends Record<string, any>> = Elements<T>;

type MultiCollectionSchema = Record<string, Record<string, AnySchema>>;

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
export function dbId(type: string): v.OptionalSchema<v.SchemaWithPipe<readonly [v.StringSchema<undefined>, v.RegexAction<string, undefined>]>, () => string> {
    return v.optional(refId(type), () => `${type}:${newId()}`);
}

/**
 * Creates a reference ID field that must match a specific type prefix
 * 
 * @param type - The document type identifier that must prefix the ID
 * @returns A Valibot schema for validating reference IDs
 */
export function refId(type: string): v.SchemaWithPipe<readonly [v.StringSchema<undefined>, v.RegexAction<string, undefined>]> {
    return v.pipe(v.string(), v.regex(new RegExp(`^${type}:`)));
}

// Type for aggregation pipeline stages
type AggregationStage = Record<string, unknown>;
type StageBuilder<T extends MultiCollectionSchema> = {
    match: <E extends keyof T>(key: E, filter: Record<string, unknown>) => AggregationStage;
    unwind: <E extends keyof T>(key: E, field: string) => AggregationStage;
    lookup: <E extends keyof T>(key: E, localField: string, foreignField: string) => AggregationStage;
};

type Input<T extends MultiCollectionSchema> = v.InferInput<v.UnionSchema<[v.ObjectSchema<MultiSchema<T>, any>], any>>;
type Output<T extends MultiCollectionSchema> = v.InferOutput<v.UnionSchema<[v.ObjectSchema<MultiSchema<T>, any>], any>>;

/**
 * Type representing the enhanced MongoDB collection for storing multiple document types
 * @template T - Record mapping document type names to their schemas
 */
type MultiCollectionResult<T extends MultiCollectionSchema> = {
    withSession: Awaited<ReturnType<typeof getSessionContext>>["withSession"],
    insertOne<E extends keyof T>(key: E, doc: v.InferInput<ElementSchema<T, E>>): Promise<string>;
    insertMany<E extends keyof T>(key: E, docs: v.InferInput<ElementSchema<T, E>>[]): Promise<(string)[]>;
    findOne<E extends keyof T>(key: E, filter: m.Filter<v.InferInput<OutputElementSchema<T, E>>>): Promise<v.InferOutput<OutputElementSchema<T, E>> | null>;
    find<E extends keyof T>(key: E, filter?: m.Filter<v.InferInput<OutputElementSchema<T, E>>>, options?: m.FindOptions): Promise<v.InferOutput<OutputElementSchema<T, E>>[]>;
    paginate<E extends keyof T, EN = v.InferOutput<OutputElementSchema<T, E>>, R = EN>(key: E, filter?: m.Filter<v.InferInput<OutputElementSchema<T, E>>>, options?: {
        limit?: number,
        afterId?: string,
        beforeId?: string,
        sort?: m.Sort | m.SortDirection,
        prepare?: (doc: v.InferOutput<OutputElementSchema<T, E>>) => Promise<EN> | EN,
        filter?: (doc: EN) => Promise<boolean> | boolean,
        format?: (doc: EN) => Promise<R> | R,
    }): Promise<R[]>;
    countDocuments<E extends keyof T>(key: E, filter?: m.Filter<v.InferInput<OutputElementSchema<T, E>>>, options?: m.CountDocumentsOptions): Promise<number>;
    deleteId<E extends keyof T>(key: E, id: string): Promise<number>;
    deleteIds<E extends keyof T>(key: E, ids: string[]): Promise<number>;
    deleteMany<E extends keyof T>(key: E, filter: m.Filter<v.InferInput<OutputElementSchema<T, E>>>): Promise<number>;
    deleteAny(filter: m.Filter<Input<T>>): Promise<number>;
    updateOne<E extends keyof T>(key: E, id: string, doc: Omit<Partial<FlatType<v.InferInput<ElementSchema<T, E>>>>, "_id" | "type">): Promise<number>;
    updateMany(operation: {
        [key in keyof T]?: {
            [id: string]: Omit<Partial<FlatType<v.InferInput<ElementSchema<T, key>>>>, "_id" | "type">;
        }
    }): Promise<number>;
    aggregate(stageBuilder: (stage: StageBuilder<T>) => AggregationStage[]): Promise<any[]>;
}

/**
 * Creates a single MongoDB collection that can store multiple document types with validation
 * 
 * This function creates or updates a MongoDB collection that can store different document types
 * in a single collection while maintaining type safety and validation for each type.
 * 
 * @param db - MongoDB database instance
 * @param collectionName - Name of the collection to create or use
 * @param collectionSchema - Record mapping document type names to their Valibot schemas
 * @param options - Additional options for the collection
 * @returns A Promise resolving to an enhanced MongoDB collection with multi-document-type support
 * 
 * @example
 * ```typescript
 * const catalog = await multiCollection(db, "catalog", {
 *   product: {
 *     name: v.string(),
 *     price: v.number(),
 *     category: v.string()
 *   },
 *   category: {
 *     name: v.string(),
 *     parentId: v.optional(v.string())
 *   }
 * });
 * 
 * const categoryId = await catalog.insertOne("category", { name: "Electronics" });
 * await catalog.insertOne("product", { name: "Phone", price: 499, category: categoryId });
 * ```
 */
export async function multiCollection<const T extends MultiCollectionSchema>(
    db: Db,
    collectionName: string,
    collectionSchema: T,
    options?: m.CollectionOptions & CollectionOptions
): Promise<MultiCollectionResult<T>> {
    type TInput = Input<T>;
    type TOutput = Output<T>;

    const schemaWithId = Object.entries(collectionSchema).reduce((acc, [key, value]) => {
        return {
            ...acc,
            [key]: {
                _id: dbId(key),
                _type: withIndex(v.optional(v.literal(key), () => key)),
                ...value,
            }
        }
    }, {} as { [key in keyof T]: Elements<T> });

    const schemaElements = Object.entries(schemaWithId).reduce((acc, [key, value]) => {
        return {
            ...acc,
            [key]: v.object(value),
        }
    }, {} as  { [key in keyof T]: ElementSchema<T, key> });

    const dotSchemaElements = Object.entries(schemaElements).reduce((acc, [key, value]) => {
        return {
            ...acc,
            [key]: createDotNotationSchema(value),
        }
    }, {} as Record<keyof T, v.BaseSchema<any, any, any>>);

    const schema = v.union([
        ...Object.values(schemaElements),
    ]);
    
    const opts: m.CollectionOptions & CollectionOptions = {
        ...{
            safeDelete: true,
            undefinedBehavior: 'remove', // Default behavior
        },
        ...options,
    }

    async function applyValidator() {
        const collections = await db.listCollections({ name: collectionName }).toArray();
        const validator = toMongoValidator(
            v.union([
                ...Object.entries(schemaWithId).map(([key, value]) => {
                    return v.object({
                        ...value,
                        _type: v.literal(key as string)
                    })
                })
            ])
        );

        if (collections.length === 0) {
            // Create the collection with the validator
            await db.createCollection(collectionName, {
                validator,
            });
        } else {
            // Update the collection with the validator
            await db.command({
                collMod: collectionName,
                validator,
            });
        }
    }

    async function applyIndexes() {
        const currentIndexes = await collection.indexes();
        const allIndexes = Object.entries(schemaElements).map(([key, value]) => {
            const indexes = extractIndexes(value);
            return {
                type: key,
                indexes,
            };
        });
        
        for (const { type, indexes } of allIndexes) {
            for (const index of indexes) {
                const indexName = sanitizePathName(`${type}_${index.path}`);
                const existingIndex = currentIndexes.find(i => i.name === indexName);
                
                if(existingIndex) {
                    await collection.dropIndex(indexName).catch((e) => {
                        // https://www.mongodb.com/docs/manual/reference/error-codes/
                        if (e instanceof m.MongoServerError && e.codeName === "IndexNotFound") {
                            return;
                        }

                        throw e;
                    });
                }

                await collection.createIndex(
                    { [index.path]: 1 },
                    {
                        ...index.metadata,
                        partialFilterExpression: {
                            _type: { $eq: type },
                        },
                        name: indexName,
                    }
                );
            }
        }
    }

    let sessionContext: Awaited<ReturnType<typeof getSessionContext>>;
    
    async function init() {
        await applyValidator();
        await applyIndexes();
        sessionContext = await getSessionContext(db.client);
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
        ): Promise<R[]> {
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
            }
            if (beforeId) {
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

            return elements;
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
            const dotSchema = dotSchemaElements[key];
            if(!dotSchema) {
                throw new Error(`Invalid element type`);
            }

            v.parse(dotSchema, doc);

            const session = sessionContext.getSession();

            const result = await collection.updateOne({
                _id: id,
                _type: key as string,
            } as any, {
                $set: doc,
            } as any, { session });

            if(!result.acknowledged) {
                throw new Error("Update failed");
            }

            if (result.matchedCount === 0) {
                throw new Error("No element that match the filter to update");
            }

            if (result.modifiedCount === 0) {
                throw new Error("No element that match the filter to update");
            }

            return result.modifiedCount;
        },
        async updateMany(operation) {
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
                    v.parse(dotSchema, element);

                    bulkOps.push({
                        updateOne: {
                            filter: { _id: id },
                            update: { $set: element },
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

            if (result.modifiedCount === 0) {
                throw new Error("No element that match the filter to update");
            }

            return result.modifiedCount;
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
                lookup: (key, localField, foreignField) => ({
                    $lookup: {
                        from: collectionName,
                        localField,
                        foreignField,
                        as: localField,
                    },
                }),
            };

            const session = sessionContext.getSession();
            
            const pipeline = stageBuilder(stage);
            const cursor = collection.aggregate(pipeline, { session });
            
            return await cursor.toArray();
        },
    }
}
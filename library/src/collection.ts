import * as v from './schema.ts';
import { toMongoValidator } from "./validator.ts";
import { sanitizeForMongoDB } from "./sanitizer.ts";
import * as m from "mongodb";
import { EventEmitter } from "./events.ts";
import { watchEvent } from "./change-stream.ts";
import { getSessionContext } from "./session.ts"
import type { Db } from "./mongodb.ts";
import { extractIndexes, keyEqual, normalizeIndexOptions } from "./indexes.ts";
import { sanitizePathName } from "./schema-navigator.ts";
import { dirtyEquivalent } from "./utils/object.ts";
import { mongoOperationQueue } from "./operation.ts";

type CollectionOptions = {
    safeDelete?: boolean,
    enableWatching?: boolean,
    /** How to handle undefined values in updates: 'remove' | 'ignore' | 'error' */
    undefinedBehavior?: 'remove' | 'ignore' | 'error',
    // Initialization options
    noInit?: boolean, // If true, skip all initialization (validator, indexes, watching)
}

type WithId<T> = T extends { _id: infer U } ? T : m.WithId<T> | { _id: string } & T;

type TInput<T extends Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>> = v.InferInput<v.ObjectSchema<T, undefined>>;
type TOutput<T extends Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>> = WithId<v.InferOutput<v.ObjectSchema<T, undefined>>>;

type Events<T extends Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>> = {
    insert: (insertEvent: m.ChangeStreamInsertDocument<TOutput<T>>) => void,
    update: (updateEvent: m.ChangeStreamUpdateDocument<TOutput<T>>) => void,
    replace: (replaceEvent: m.ChangeStreamReplaceDocument<TOutput<T>>) => void,
    delete: (deleteEvent: m.ChangeStreamDeleteDocument<TOutput<T>>) => void,
}

/**
 * Type representing the enhanced MongoDB collection with validation and type safety
 * @template T - Schema type containing Valibot schemas for document fields
 */
export type CollectionResult<T extends Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>> =
    Omit<m.Collection<TInput<T>>, "findOne" | "find" | "insertOne" | "distinct" | "findOneAndDelete" | "findOneAndReplace" | "findOneAndUpdate" | "indexInformation" | "listSearchIndexes" | "count"> & {
        collection: m.Collection<TInput<T>>,
        schema: v.ObjectSchema<{readonly _id: v.OptionalSchema<v.AnySchema, undefined>} & T, undefined>,
        on: ReturnType<typeof EventEmitter<Events<T>>>["on"],
        off: ReturnType<typeof EventEmitter<Events<T>>>["off"],
        insertOne: (doc: m.OptionalUnlessRequiredId<TInput<T>>, options?: m.InsertOneOptions) => Promise<WithId<TOutput<T>>["_id"]>,
        findOne: (filter: m.Filter<WithId<TInput<T>>>, options?: Omit<m.FindOptions, 'timeoutMode'> & m.Abortable) => Promise<WithId<TOutput<T>> | null>,
        getById: (id: string | m.ObjectId) => Promise<WithId<TOutput<T>>>,
        find: (filter: m.Filter<TInput<T>>, options?: m.FindOptions & m.Abortable) => m.AbstractCursor<TOutput<T>>,
        findInvalid: (filter: m.Filter<TInput<T>>, options?: m.FindOptions & m.Abortable) => m.AbstractCursor<WithId<TInput<T>>>,
        withSession: Awaited<ReturnType<typeof getSessionContext>>["withSession"],

        // Utilities
        paginate: <E = WithId<TOutput<T>>, R = E>(filter: m.Filter<TInput<T>>, options?: {
            limit?: number,
            afterId?: string | m.ObjectId,
            beforeId?: string | m.ObjectId,
            sort?: m.Sort | m.SortDirection,
            prepare?: (doc: WithId<TOutput<T>>) => Promise<E> | E,
            filter?: (doc: E) => Promise<boolean> | boolean,
            format?: (doc: E) => Promise<R> | R,
        }) => Promise<{
            total: number,
            position: number,
            data: R[],
        }>,

        // From mongodb.Collection
        updateOne(filter: m.Filter<WithId<TInput<T>>>, update: m.UpdateFilter<TInput<T>> | m.Document[], options?: m.UpdateOptions): Promise<m.UpdateResult<TInput<T>>>;
        distinct<Key extends keyof WithId<TInput<T>>>(key: Key, filter: m.Filter<TInput<T>>, options?: m.DistinctOptions): Promise<Array<m.Flatten<WithId<TInput<T>>[Key]>>>;
        findOneAndDelete(filter: m.Filter<TInput<T>>, options?: m.FindOneAndDeleteOptions & { includeResultMetadata: boolean; }): Promise<WithId<TInput<T>> | null>;
        findOneAndReplace(filter: m.Filter<TInput<T>>, replacement: m.WithoutId<TInput<T>>, options?: m.FindOneAndReplaceOptions & { includeResultMetadata: boolean; }): Promise<m.ModifyResult<TInput<T>> | null>;
        findOneAndUpdate(filter: m.Filter<TInput<T>>, update: m.UpdateFilter<TInput<T>> | m.Document[], options?: m.FindOneAndUpdateOptions & { includeResultMetadata: boolean; }): Promise<m.WithId<TInput<T>> | null>;
        indexInformation(options: m.IndexInformationOptions & { full: true; }): Promise<m.IndexDescriptionInfo[]>;
        listSearchIndexes(name: string, options?: m.ListSearchIndexesOptions): m.ListSearchIndexesCursor;
    }

/**
 * Utility type for extracting the schema type from a collection
 * @template T - The CollectionResult type to extract the schema from
 */
export type CollectionSchema<T> = T extends CollectionResult<infer U> ? WithId<v.InferOutput<v.ObjectSchema<U, undefined>>> : never;

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
export async function collection<const T extends Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>>(
    db: Db,
    collectionName: string,
    collectionSchema: T,
    options?: m.CollectionOptions & CollectionOptions
) : Promise<CollectionResult<T>> {
    type TInput = v.InferInput<v.ObjectSchema<T, undefined>>;
    type TOutput = WithId<v.InferOutput<v.ObjectSchema<T, undefined>>>;

    const schema = v.object({
        _id: v.optional(v.any()),
        ...collectionSchema,
    });

    const opts: m.CollectionOptions & CollectionOptions = {
        ...{
            safeDelete: true,
            undefinedBehavior: 'remove', // Default behavior
        },
        ...options,
    }

    const events = EventEmitter<Events<T>>();
    const validator = toMongoValidator(schema);
    const invalidValidation = { $nor: [validator] };

    async function applyValidator() {
        const collections = await db.listCollections({ name: collectionName }).toArray();

        if (collections.length === 0) {
            // Create the collection with the validator
            await db.createCollection(collectionName, {
                validator,
            });
        } else {
            // Check collection options
            const existingOptions = await db.command({ listCollections: 1, filter: { name: collectionName } });
            const currentSchema = existingOptions.cursor?.firstBatch?.[0]?.options?.validator || {};
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
        const currentIndexes = await collection.indexes();
        const indexes = extractIndexes(schema);

        // Collect all indexes that need to be created or recreated
        const indexesToCreate: Array<{
            key: Record<string, number>;
            options: m.CreateIndexesOptions;
        }> = [];
        const indexesToDrop: string[] = [];

        // Collect all expected index names from the current schema
        const expectedIndexNames = new Set<string>();
        for (const index of indexes) {
            const indexPath = sanitizePathName(index.path);
            expectedIndexNames.add(indexPath);
        }

        // Get all possible field paths from the current schema to detect potential mongodbee indexes
        const allSchemaPaths = new Set<string>();
        function collectPaths(obj: any, prefix = '') {
            for (const [key, value] of Object.entries(obj)) {
                const fullPath = prefix ? `${prefix}.${key}` : key;
                const sanitizedPath = sanitizePathName(fullPath);
                allSchemaPaths.add(sanitizedPath);
                
                if (value && typeof value === 'object' && 'entries' in value) {
                    collectPaths((value as any).entries, fullPath);
                }
            }
        }
        collectPaths(schema.entries);
        
        // Find orphaned indexes that were created by mongodbee but are no longer in the schema
        for (const existingIndex of currentIndexes) {
            const indexName = existingIndex.name;
            if (!indexName || indexName === '_id_') continue; // Skip default _id index
            
            // Check if this index name matches any field in our schema
            const isSchemaField = allSchemaPaths.has(indexName);
            
            if (isSchemaField && !expectedIndexNames.has(indexName)) {
                // This is an orphaned mongodbee index that should be removed
                indexesToDrop.push(indexName);
            }
        }

        for (const index of indexes) {
            const keySpec = { [index.path]: 1 };
            const indexPath = sanitizePathName(index.path);

            // Try to find an existing index: prefer exact name, fallback to key match
            const existingIndex = currentIndexes.find(i => i.name === indexPath) || currentIndexes.find(i => keyEqual(i.key || {}, keySpec));

            const desiredOptions = {
                ...index.metadata,
                name: indexPath,
            };
            
            let needsRecreate = true;
            if (existingIndex) {
                const existingNorm = normalizeIndexOptions(existingIndex);
                const desiredNorm = normalizeIndexOptions(desiredOptions);
                if (existingNorm.unique === desiredNorm.unique
                    && existingNorm.collation === desiredNorm.collation
                    && existingNorm.partialFilterExpression === desiredNorm.partialFilterExpression) {
                    needsRecreate = false;
                }
            }

            if (!needsRecreate) continue;

            if (existingIndex) {
                indexesToDrop.push(existingIndex.name!);
            }

            indexesToCreate.push({
                key: keySpec,
                options: desiredOptions,
            });
        }

        // Use the queue system to manage index operations
        if (indexesToDrop.length > 0) {
            await Promise.all(indexesToDrop.map(indexName => {
                return mongoOperationQueue.add(() => {
                    return collection.dropIndex(indexName).catch((err: unknown) => {
                        // tolerate race / already dropped
                        if (err instanceof m.MongoServerError && err.codeName === "IndexNotFound") {
                            // ignore
                            return;
                        }
                        const maybe = err as { code?: number };
                        if (maybe.code === 27) {
                            // legacy IndexNotFound code
                            return;
                        }
                        throw err;
                    });
                });
            }));
        }

        if (indexesToCreate.length > 0) {
            await Promise.all(indexesToCreate.map(indexSpec => {
                return mongoOperationQueue.add(() => {
                    return collection.createIndex(
                        indexSpec.key,
                        indexSpec.options
                    );
                });
            }));
        }
    }
    
    async function startWatching() {
        watchEvent(db, collection, (change) => {
            switch (change.operationType) {
                case "insert":
                    events.call("insert", change as m.ChangeStreamInsertDocument<TOutput>);
                    break;
                case "update":
                    events.call("update", change as m.ChangeStreamUpdateDocument<TOutput>);
                    break;
                case "replace":
                    events.call("replace", change as m.ChangeStreamReplaceDocument<TOutput>);
                    break;
                case "delete":
                    events.call("delete", change as m.ChangeStreamDeleteDocument<TOutput>);
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
        await new Promise(resolve => setTimeout(resolve, 100));
    }

    let sessionContext: Awaited<ReturnType<typeof getSessionContext>>;

    async function init() {
        sessionContext = getSessionContext(db.client);

        // If raw mode, skip all initialization
        if(opts.noInit) return;

        await applyValidator();
        await applyIndexes();
        
        // Only start watching if explicitly enabled
        if (opts.enableWatching) {
            await startWatching();
        }
    }

    const collection = db.collection<TInput>(collectionName, opts);
    await init();

    return {
        // Raw collection
        collection,

        // Schema
        schema,

        // Events
        on: events.on,
        off: events.off,
        withSession: sessionContext!.withSession,

        get bsonOptions() { return collection.bsonOptions },
        get collectionName() { return collection.collectionName },
        get dbName() { return collection.dbName },
        get hint() { return collection.hint },
        get namespace() { return collection.namespace },
        get readConcern() { return collection.readConcern },
        get readPreference() { return collection.readPreference },
        get timeoutMS() { return collection.timeoutMS },
        get writeConcern() { return collection.writeConcern },
        // Document creation operations with validation
        async insertOne(doc, options?) {
            const validatedDoc = v.parse(schema, doc) as m.OptionalUnlessRequiredId<TInput>;
            
            // Apply sanitization based on configuration
            const safeDoc = sanitizeForMongoDB(validatedDoc, {
                undefinedBehavior: opts.undefinedBehavior || 'remove',
                deep: true
            }) as m.OptionalUnlessRequiredId<TInput>;
            
            const session = sessionContext.getSession();
            const inserted = await collection.insertOne(safeDoc, { session, ...options });
            if(!inserted.acknowledged) {
                throw new Error("Insert failed");
            }
            return inserted.insertedId as WithId<TOutput>["_id"];
        },
        async insertMany(docs, options?) {
            const validatedDocs = docs.map(doc => v.parse(schema, doc));
            
            // Apply sanitization based on configuration
            const safeDocs = validatedDocs.map(doc => sanitizeForMongoDB(doc, {
                undefinedBehavior: opts.undefinedBehavior || 'remove',
                deep: true
            }) as m.OptionalUnlessRequiredId<TInput>);
            
            const session = sessionContext.getSession();
            const inserted = await collection.insertMany(safeDocs, { session, ...options });
            if(!inserted.acknowledged) {
                throw new Error("Insert failed");
            }
            return inserted;
        },
        
        // Document read operations with validation
        async findOne(filter, options?) {
            const session = sessionContext.getSession();
            const result = await collection.findOne({
                ...validator, // Prevent returning invalid documents
                ...filter as any,
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
            }
        },
        async getById(id) {
            const session = sessionContext.getSession();
            const result = await collection.findOne({ _id: id } as any, { session });

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
            }
        },
        find(filter: m.Filter<TInput>, options?: m.FindOptions & m.Abortable): m.AbstractCursor<TOutput> {
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
                    console.warn(`Warning: ${invalidsCount} invalid documents were ignored during find operation`);
                }

                return output;
            };

            return cursor as unknown as m.AbstractCursor<TOutput>;
        },
        findInvalid(filter: m.Filter<TInput>, options?: m.FindOptions & m.Abortable): m.AbstractCursor<TOutput> {
            const session = sessionContext.getSession();
            const cursor = collection.find({
                $and: [
                    filter as any,
                    invalidValidation
                ]
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
                    console.warn(`Warning: ${invalidsCount} invalid documents were found during findInvalid operation`);
                }
                return output;
            };

            return cursor as unknown as m.AbstractCursor<TOutput>;
        },
        async paginate<E = WithId<TOutput>, R = E>(filter: m.Filter<TInput>, options?: {
            limit?: number,
            afterId?: string | m.ObjectId,
            beforeId?: string | m.ObjectId,
            sort?: m.Sort | m.SortDirection,
            prepare?: (doc: WithId<TOutput>) => Promise<E>,
            filter?: (doc: E) => Promise<boolean> | boolean,
            format?: (doc: E) => Promise<R>,
        }): Promise<{
            total: number,
            position: number,
            data: R[],
        }> {
            let { limit = 100, afterId, beforeId, sort, prepare, filter: customFilter, format } = options || {};
            const session = sessionContext.getSession();
            const baseQuery: m.Filter<TInput> = { ...filter };
            let query: m.Filter<TInput> = { ...filter };

            // Add pagination filters
            if (afterId) {
                (query as Record<string, unknown>)._id = { $gt: afterId };
                sort = sort || { _id: 1 };
            }
            if (beforeId) {
                (query as Record<string, unknown>)._id = { $lt: beforeId };
                sort = sort || { _id: -1 };
            }

            let total: number | undefined;
            let position: number | undefined;
            {
                // Count total documents matching the base filter
                total = await collection.countDocuments(baseQuery, { session });
                
                if (afterId) {
                    const positionQuery = { 
                        ...baseQuery,
                        _id: { $lte: afterId }
                    } as m.Filter<TInput>;
                    position = await collection.countDocuments(positionQuery, { session });
                } else if (beforeId) {
                    const positionQuery = { 
                        ...baseQuery,
                        _id: { $gte: beforeId }
                    } as m.Filter<TInput>;
                    const remainingCount = await collection.countDocuments(positionQuery, { session });
                    position = total - remainingCount;
                } else {
                    position = 0;
                }
            }

            const cursor = collection.find(query, { session }).sort(sort as m.Sort);
            let hardLimit = 10_000;
            const elements: R[] = [];
            
            while(hardLimit-- > 0 && limit > 0) {
                const doc = await cursor.next() as WithId<TOutput> | null;
                if (!doc) break;

                // Validate document with schema
                const validation = v.safeParse(schema, doc);
                if (!validation.success) {
                    continue; // Skip invalid documents
                }

                const validatedDoc = validation.output as WithId<TOutput>;
                
                // Step 1: Prepare - enrich document with external data
                const enrichedDoc = prepare ? await prepare(validatedDoc) : validatedDoc as unknown as E;
                
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
        countDocuments(filter, options?) {
            const session = sessionContext.getSession();
            return collection.countDocuments(filter, { session, ...options });
        },
        estimatedDocumentCount(options?) {
            const session = sessionContext.getSession();
            return collection.estimatedDocumentCount({ session, ...options });
        },
        distinct(key, filter, options?) {
            const session = sessionContext.getSession();
            return collection.distinct(key as string, filter, { session, ...options });
        },
        
        // Document update operations
        replaceOne(filter, replacement, options?) {
            const validation = v.safeParse(schema, replacement);
            if (!validation.success) {
                throw {
                    message: "Validation error",
                    errors: validation,
                }
            }

            const sanitizedReplacement = sanitizeForMongoDB(validation.output, {
                undefinedBehavior: opts.undefinedBehavior || 'remove',
                deep: true
            }) as TInput;
            const session = sessionContext.getSession();
            return collection.replaceOne(filter, sanitizedReplacement, { session, ...options });
        },
        updateOne(filter, update, options?) {
            // @TODO: check if update is valid
            const session = sessionContext.getSession();
            return collection.updateOne(filter as any, update, { session, ...options });
        },
        updateMany(filter, update, options?) {
            // @TODO: check if update is valid
            const session = sessionContext.getSession();
            return collection.updateMany(filter, update, { session, ...options });
        },
        
        // Document delete operations
        deleteOne(filter, options?) {
            const session = sessionContext.getSession();
            return collection.deleteOne(filter, { session, ...options });
        },
        deleteMany(filter, options?) {
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
        },
        
        // Compound operations
        findOneAndDelete(filter, options?) {
            const session = sessionContext.getSession();
            return collection.findOneAndDelete(filter, { session, ...options });
        },
        findOneAndReplace(filter, replacement, options?) {
            const session = sessionContext.getSession();
            return collection.findOneAndReplace(filter, replacement, { session, ...options });
        },
        findOneAndUpdate(filter, update, options?) {
            const session = sessionContext.getSession();
            return collection.findOneAndUpdate(filter, update, { session, ...options });
        },
        
        // Bulk operations
        aggregate(pipeline, options?) {
            const session = sessionContext.getSession();
            return collection.aggregate(pipeline, { session, ...options });
        },
        bulkWrite(operations, options?) {
            const session = sessionContext.getSession();
            return collection.bulkWrite(operations, { session, ...options });
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
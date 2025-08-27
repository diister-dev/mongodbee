import * as v from './schema.ts';
import { toMongoValidator } from "./validator.ts";
import type * as m from "mongodb";
import { EventEmitter } from "./events.ts";
import { watchEvent } from "./change-stream.ts";
import { getSessionContext } from "./session.ts"
import type { Db } from "./mongodb.ts";
import { extractIndexes } from "./indexes.ts";
import { sanitizePathName } from "./schema-navigator.ts";

type CollectionOptions = {
    safeDelete?: boolean,
    enableWatching?: boolean,
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
        findOne: (filter: m.Filter<WithId<TInput<T>>>, options?: Omit<m.FindOptions, 'timeoutMode'> & m.Abortable) => Promise<WithId<TOutput<T>>>,
        find: (filter: m.Filter<TInput<T>>, options?: m.FindOptions & m.Abortable) => m.AbstractCursor<TOutput<T>>,
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
        }) => Promise<R[]>,

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
        },
        ...options,
    }

    const events = EventEmitter<Events<T>>();

    async function applyValidator() {
        const collections = await db.listCollections({ name: collectionName }).toArray();
        const validator = toMongoValidator(schema);

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
        const indexes = extractIndexes(schema);
        
        for (const index of indexes) {
            const indexPath = sanitizePathName(index.path);
            
            const existingIndex = currentIndexes.find(i => {
                return i.name === indexPath || i.key[index.path] !== undefined;
            });
            
            if (existingIndex) {
                await collection.dropIndex(existingIndex.name!);
            }

            await collection.createIndex(
                { [index.path]: 1 },
                {
                    ...index.metadata,
                    name: indexPath,
                }
            );
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
        await applyValidator();
        await applyIndexes();
        
        // Only start watching if explicitly enabled
        if (opts.enableWatching) {
            await startWatching();
        }

        sessionContext = await getSessionContext(db.client);
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
            const safeDoc = v.parse(schema, doc) as m.OptionalUnlessRequiredId<TInput>;
            const session = sessionContext.getSession();
            const inserted = await collection.insertOne(safeDoc, { session, ...options });
            if(!inserted.acknowledged) {
                throw new Error("Insert failed");
            }
            return inserted.insertedId as WithId<TOutput>["_id"];
        },
        async insertMany(docs, options?) {
            const safeDocs = docs.map(doc => v.parse(schema, doc)) as unknown as typeof docs;
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
            const result = await collection.findOne(filter as any, { session, ...options });

            if (!result) {
                throw new Error("Document not found");
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
                const valids = [];
                const invalids = [];
                for (const doc of results) {
                    const validation = v.safeParse(schema, doc);
                    if (validation.success) {
                        valids.push(validation.output as m.WithId<TInput>);
                    } else {
                        invalids.push({
                            errors: validation,
                            result: doc,
                        });
                    }
                }

                if (invalids.length > 0) {
                    throw {
                        message: "Validation error",
                        valids,
                        invalids,
                    }
                }

                return valids;
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
        }): Promise<R[]> {
            let { limit = 100, afterId, beforeId, sort, prepare, filter: customFilter, format } = options || {};
            const session = sessionContext.getSession();
            const query: m.Filter<TInput> = { ...filter };

            if (afterId) {
                (query as Record<string, unknown>)._id = { $gt: afterId };
                sort = sort || { _id: 1 };
            }
            if (beforeId) {
                (query as Record<string, unknown>)._id = { $lt: beforeId };
                sort = sort || { _id: -1 };
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

            return elements;
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

            const session = sessionContext.getSession();
            return collection.replaceOne(filter, validation.output as TInput, { session, ...options });
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
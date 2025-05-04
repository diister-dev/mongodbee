import * as v from './schema.ts';
import { toMongoValidator } from "./validator.ts";
import type * as m from "mongodb";

type CollectionOptions = {
    safeDelete?: boolean,
}

type WithId<T> = T extends { _id: infer U } ? T : m.WithId<T>;

type TInput<T extends Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>> = v.InferInput<v.ObjectSchema<T, undefined>>;
type TOutput<T extends Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>> = WithId<v.InferOutput<v.ObjectSchema<T, undefined>>>;

export type CollectionResult<T extends Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>> = Omit<m.Collection<TInput<T>>, "findOne" | "find" | "insertOne"> & {
    collection: m.Collection<TInput<T>>,
    insertOne: (doc: m.OptionalUnlessRequiredId<TInput<T>>, options?: m.InsertOneOptions) => Promise<WithId<TOutput<T>>["_id"]>,
    findOne: (filter: m.Filter<TInput<T>>, options?: Omit<m.FindOptions, 'timeoutMode'> & m.Abortable) => Promise<WithId<TOutput<T>>>,
    find: (filter: m.Filter<TInput<T>>, options?: m.FindOptions & m.Abortable) => m.AbstractCursor<TOutput<T>>,
}

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
export async function collection<const T extends Record<string, v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>>>(db: m.Db, collectionName: string, collectionSchema: T, options?: m.CollectionOptions & CollectionOptions) : Promise<CollectionResult<T>> {
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

    await applyValidator();

    const collection = db.collection<TInput>(collectionName, opts);

    return {
        collection,
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
            const inserted = await collection.insertOne(safeDoc, options);
            if(!inserted.acknowledged) {
                throw new Error("Insert failed");
            }
            return inserted.insertedId;
        },
        async insertMany(docs, options?) {
            const safeDocs = docs.map(doc => v.parse(schema, doc)) as unknown as typeof docs;
            const inserted = await collection.insertMany(safeDocs, options);
            if(!inserted.acknowledged) {
                throw new Error("Insert failed");
            }
            return inserted;
        },
        
        // Document read operations with validation
        async findOne(filter, options?) {
            const result = await collection.findOne(filter, options);

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
            const cursor = collection.find(filter, options);
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
        countDocuments(...params) { return collection.countDocuments(...params) },
        estimatedDocumentCount(...params) { return collection.estimatedDocumentCount(...params) },
        distinct: collection.distinct,
        
        // Document update operations
        replaceOne(filter, replacement, options?) {
            const validation = v.safeParse(schema, replacement);
            if (!validation.success) {
                throw {
                    message: "Validation error",
                    errors: validation,
                }
            }
            return collection.replaceOne(filter, validation.output as TInput, options);
        },
        updateOne(filter, update, options?) {
            // @TODO: check if update is valid
            return collection.updateOne(filter, update, options);
        },
        updateMany(filter, update, options?) {
            // @TODO: check if update is valid
            return collection.updateMany(filter, update, options);
        },
        
        // Document delete operations
        deleteOne(...params) { return collection.deleteOne(...params) },
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

            return collection.deleteMany(filter, options);
        },
        
        // Compound operations
        findOneAndDelete: collection.findOneAndDelete,
        findOneAndReplace: collection.findOneAndReplace,
        findOneAndUpdate: collection.findOneAndUpdate,
        
        // Bulk operations
        aggregate(...params) { return collection.aggregate(...params) },
        bulkWrite(...params) { return collection.bulkWrite(...params) },
        initializeOrderedBulkOp(...params) { return collection.initializeOrderedBulkOp(...params) },
        initializeUnorderedBulkOp(...params) { return collection.initializeUnorderedBulkOp(...params) },
        
        // Index operations
        createIndex(...params) { return collection.createIndex(...params) },
        createIndexes(...params) { return collection.createIndexes(...params) },
        dropIndex(...params) { return collection.dropIndex(...params) },
        dropIndexes(...params) { return collection.dropIndexes(...params) },
        indexes(...params) { return collection.indexes(...params) },
        listIndexes(...params) { return collection.listIndexes(...params) },
        indexExists(...params) { return collection.indexExists(...params) },
        indexInformation: collection.indexInformation,
        
        // Search operations
        createSearchIndex(...params) { return collection.createSearchIndex(...params) },
        createSearchIndexes(...params) { return collection.createSearchIndexes(...params) },
        dropSearchIndex(...params) { return collection.dropSearchIndex(...params) },
        listSearchIndexes: collection.listSearchIndexes,
        updateSearchIndex(...params) { return collection.updateSearchIndex(...params) },
        
        // Collection operations
        drop(...params) { return collection.drop(...params) },
        isCapped(...params) { return collection.isCapped(...params) },
        options(...params) { return collection.options(...params) },
        rename(...params) { return collection.rename(...params) },
        watch(...params) { return collection.watch(...params) },
    } as CollectionResult<T>;
}
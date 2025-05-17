import * as v from './schema.ts';
import type * as m from "mongodb";
import { ulid } from "@std/ulid";
import { toMongoValidator } from "./validator.ts";
import { FlatType } from "../types/flat.ts";
import { createDotNotationSchema } from "./dot-notation.ts";

type CollectionOptions = {
    safeDelete?: boolean,
}

type Elements<T extends Record<string, any>> = {
    [key in keyof T]: {
        _id: ReturnType<typeof dbId>;
    } & T[key]
}[keyof T];

type ElementSchema<T extends Record<string, any>, K extends keyof T> = v.ObjectSchema<{
    _id: ReturnType<typeof dbId>,
} & T[K], any>;

type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;
type MultiSchema<T extends Record<string, any>> = Elements<T>;

type MultiCollectionSchema = Record<string, Record<string, AnySchema>>;

export function dbId(type: string): v.OptionalSchema<v.SchemaWithPipe<readonly [v.StringSchema<undefined>, v.RegexAction<string, undefined>]>, () => string> {
    return v.optional(refId(type), () => `${type}:${ulid()}`);
}

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

type MultiCollectionResult<T extends MultiCollectionSchema> = {
    insertOne<E extends keyof T>(key: E, doc: v.InferInput<ElementSchema<T, E>>): Promise<string>;
    insertMany<E extends keyof T>(key: E, docs: v.InferInput<ElementSchema<T, E>>[]): Promise<(string)[]>;
    findOne<E extends keyof T>(key: E, filter: Partial<Input<T>>): Promise<v.InferOutput<ElementSchema<T, E>>>;
    find<E extends keyof T>(key: E, filter?: Partial<Input<T>>): Promise<v.InferOutput<v.UnionSchema<[v.ObjectSchema<MultiSchema<T>, any>], any>>[]>;
    deleteId<E extends keyof T>(key: E, id: string): Promise<number>;
    deleteIds<E extends keyof T>(key: E, ids: string[]): Promise<number>;
    updateOne<E extends keyof T>(key: E, id: string, doc: Omit<Partial<FlatType<v.InferInput<ElementSchema<T, E>>>>, "_id">): Promise<number>;
    updateMany(operation: {
        [key in keyof T]?: {
            [id: string]: Omit<Partial<FlatType<v.InferInput<ElementSchema<T, key>>>>, "_id">;
        }
    }): Promise<number>;
    aggregate(stageBuilder: (stage: StageBuilder<T>) => AggregationStage[]): Promise<any[]>;
}

// Objective
// - Support many aggregation stages (https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline)
export async function multiCollection<const T extends MultiCollectionSchema>(
    db: m.Db,
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

    async function init() {
        await applyValidator();
    }

    const collection = db.collection<TOutput>(collectionName, opts);
    await init();

    return {
        async insertOne(key, doc) {
            const _id = doc._id ?? `${key as string}:${ulid()}`;
            const schema = schemaElements[key];
            const validation = v.parse(schema, {
                ...doc,
                _id,
            });

            const result = await collection.insertOne(validation as any);
            if(!result.acknowledged) {
                throw new Error("Insert failed");
            }

            return result.insertedId as unknown as string;
        },
        async insertMany(key, docs) {
            const validation = docs.map((doc) => {
                const _id = doc._id ?? `${key as string}:${ulid()}`;
                return v.parse(schema, {
                    ...doc,
                    _id,
                });
            });

            const result = await collection.insertMany(validation as any);
            if(!result.acknowledged) {
                throw new Error("Insert failed");
            }

            return Object.values(result.insertedIds) as unknown as string[];
        },
        async findOne(key, filter) {
            const result = await collection.findOne({
                $and: [
                    { _id: { $regex: new RegExp(`^${key as string}:`) } },
                    filter,
                ]
            } as any);

            if (!result) {
                throw new Error("Not found");
            }
            
            return v.parse(schema, result);
        },
        async find(key, filter) {
            const idChecker = { _id: { $regex: new RegExp(`^${key as string}:`) } };

            const cursor = collection.find({
                $and: filter ? [idChecker, filter] : [idChecker],
            } as any);
            
            const result = await cursor.toArray();

            return result.map((item) => v.parse(schema, item));
        },
        async deleteId(key, id) {
            const schema = schemaWithId[key];
            v.parse(schema._id, id);

            const result = await collection.deleteOne({
                _id: { $regex: new RegExp(`^${key as string}:`) },
            } as any);

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

            const result = await collection.deleteMany({
                _id: {
                    $in: ids,
                }
            } as any);

            if(!result.acknowledged) {
                throw new Error("Delete failed");
            }

            if (result.deletedCount === 0) {
                throw new Error("No element that match the filter to delete");
            }

            return result.deletedCount;
        },
        async updateOne(key, id, doc) {
            const dotSchema = dotSchemaElements[key];
            if(!dotSchema) {
                throw new Error(`Invalid element type`);
            }

            v.parse(dotSchema, doc);

            const result = await collection.updateOne({
                _id: id,
            } as any, {
                $set: doc,
            } as any);

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

            const result = await collection.bulkWrite(bulkOps);

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
                        _id: { $regex: new RegExp(`^${key as string}:`) },
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
            
            const pipeline = stageBuilder(stage);
            const cursor = collection.aggregate(pipeline);
            
            return await cursor.toArray();
        },
    }
}
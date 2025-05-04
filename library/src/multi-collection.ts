import * as v from './schema.ts';
import type * as m from "mongodb";
import { ulid } from "@std/ulid";
import { toMongoValidator } from "./validator.ts";

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

function dbId(type: string) {
    return v.optional(v.pipe(v.string(), v.regex(new RegExp(`^${type}:`))), () => `${type}:${ulid()}`);
}

export async function multiCollection<const T extends MultiCollectionSchema>(
    db: m.Db,
    collectionName: string,
    collectionSchema: T,
    options?: m.CollectionOptions & CollectionOptions
) {
    type TInput = v.InferInput<v.UnionSchema<[v.ObjectSchema<MultiSchema<T>, any>], any>>;
    type TOutput = v.InferOutput<v.UnionSchema<[v.ObjectSchema<MultiSchema<T>, any>], any>>;

    const schema = v.union([
        ...Object.entries(collectionSchema).map(([key, value]) => {
            return v.object({
                _id: dbId(key),
                ...value,
            })
        })
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
        async insertOne<E extends keyof T>(key: E, doc: v.InferInput<ElementSchema<T, E>>) {
            const _id = doc._id ?? `${key as string}:${ulid()}`;
            const validation = v.parse(schema, {
                ...doc,
                _id,
            });

            const result = await collection.insertOne(validation as any);
            if(!result.acknowledged) {
                throw new Error("Insert failed");
            }

            return result.insertedId;
        },
        async findOne<E extends keyof T>(key: E, filter: Partial<TInput>) {
            const result = await collection.findOne({
                _id: { $regex: new RegExp(`^${key as string}:`) },
                ...filter,
            } as any);

            if (!result) {
                throw new Error("Not found");
            }
            
            return v.parse(schema, result) as v.InferOutput<ElementSchema<T, E>>;
        },
        async find<E extends keyof T>(key: E, filter: Partial<TInput> = {}) {
            const cursor = collection.find({
                _id: { $regex: new RegExp(`^${key as string}:`) },
                ...filter,
            } as any);
            
            const result = await cursor.toArray();

            return result.map((item) => v.parse(schema, item));
        },
        async deleteOne<E extends keyof T>(key: E, filter: Partial<TInput>) {
            if (opts.safeDelete) {
                const filterSize = Object.keys(filter ?? {}).length;
                if (filterSize === 0) throw new Error("Filter is empty");

                let anyValidFilter = false;
                for (const key in filter) {
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

            const result = await collection.deleteOne({
                _id: { $regex: new RegExp(`^${key as string}:`) },
                ...filter,
            } as any, options);

            if(!result.acknowledged) {
                throw new Error("Delete failed");
            }

            if (result.deletedCount === 0) {
                throw new Error("No element that match the filter to delete");
            }

            return result.deletedCount;
        },
    }
}
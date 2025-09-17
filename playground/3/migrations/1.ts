/**
 * mongodbee migration
 * ---
 * This is an empty initial migration file.
 * You can use it as a starting point for your own migrations.
 */

import * as v from "valibot";
import { collection, dbId } from "mongodbee";
import parentMigration from "./0.ts";
import { deepEqual } from "../utilities.ts";

export const name = "Add lastname to user";
export const id = "0000000000001";

export const parent = parentMigration;

const schema = {
    ...parent.schema,
    "+users": {
        _id: dbId("user"),
        fullname: v.string(),
    },
    "+articles": {
        _id: dbId("article"),
        title: v.string(),
        content: v.string(),
    }
}

const userMigration = {
    collection: "+users",
    fromSchema: parent.schema["+users"],
    toSchema: schema["+users"],
    up: (oldUser: any) => {
        const fullname = `${oldUser.firstname} ${oldUser.lastname}`;
        const obj = {
            ...oldUser,
            fullname,
        };
        delete obj.firstname;
        delete obj.lastname;
        return obj;
    },
    down: (newUser: any) => {
        const [firstname, ...rest] = newUser.fullname.split(" ");
        const lastname = rest.join(" ");
        const obj = {
            ...newUser,
            firstname,
            lastname,
        };
        delete obj.fullname;
        return obj;
    }
}

async function applyMigrationUp(migration: any, rule: any) {
    const { db } = migration;
    const coll = await collection(db, rule.collection, rule.toSchema, { noInit: true });
    const cursor = coll.find({});
    for await (const doc of cursor) {
        const newDoc = rule.up(doc);
        v.parse(v.object(rule.toSchema), newDoc);
        const downDoc = rule.down(newDoc);
        v.parse(v.object(rule.fromSchema), downDoc);

        // Ensure that the down migration returns the original document
        if (deepEqual(doc, downDoc) === false) {
            throw new Error(`Migration up/down mismatch for document with _id=${doc._id}`);
        }

        await coll.replaceOne({ _id: doc._id as any }, newDoc);
    }
}

export async function up(migration: any) {
    await applyMigrationUp(migration, userMigration);

    // Add lastname field to user collection
    const { db } = migration;

    const articleCollection = await collection(db, "+articles", schema['+articles'], { noInit: true });
    await articleCollection.insertMany([
        { title: "First Article", content: "This is the content of the first article." },
        { title: "Second Article", content: "This is the content of the second article." },
    ]);
}

export async function down(migration: any) {
}

export default {
    name,
    id,
    parent,
    schema,
    up,
    down,
}
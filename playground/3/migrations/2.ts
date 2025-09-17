/**
 * mongodbee migration
 * ---
 * This is an empty initial migration file.
 * You can use it as a starting point for your own migrations.
 */

import * as v from "valibot";
import { collection, dbId } from "mongodbee";
import parentMigration from "./1.ts";
import { deepEqual } from "../utilities.ts";

export const name = "Add Groups collection";
export const id = "0000000000002";

export const parent = parentMigration;

const schema = {
    ...parent.schema,
    "+groups": {
        _id: dbId("group"),
        name: v.string(),
    }
}

export async function up(migration: any) {
    const { db } = migration;

    const articleCollection = await collection(db, "+groups", schema['+groups'], { noInit: true });
    await articleCollection.insertMany([
        { name: "Admins" },
        { name: "Editors" },
        { name: "Guests" },
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
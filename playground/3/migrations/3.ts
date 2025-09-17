/**
 * mongodbee migration
 * ---
 * This is an empty initial migration file.
 * You can use it as a starting point for your own migrations.
 */

import * as v from "valibot";
import parentMigration from "./2.ts";

export const name = "Add Groups collection";
export const id = "0000000000002";

export const parent = parentMigration;

const schema = {
    ...parent.schema,
    "+users": {
        ...parent.schema['+users'],
        fullname: undefined, // Remove fullname field
        firstname: v.string(), // Add firstname field
        lastname: v.string(),  // Add lastname field
    }
}

export async function up(migration: any) {
    const { db } = migration;
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
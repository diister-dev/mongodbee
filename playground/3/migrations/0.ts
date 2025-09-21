/**
 * mongodbee migration
 * ---
 * This is an empty initial migration file.
 * You can use it as a starting point for your own migrations.
 */

import * as v from "valibot";
import { dbId } from "mongodbee";
import { migrationDefinition } from "../migration/migration.ts";

const schemas = {
    collections: {
        "+users": {
            _id: dbId("user"),
            firstname: v.string(),
            lastname: v.string(),
        }
    }
}

export default migrationDefinition("0000000000000", "Initial Migration", {
    schemas,
    parent: null, // No parent for the initial migration
    migrate(migration) {
        migration.createCollection("+users")
            .seed([
                { firstname: "John", lastname: "Doe" },
                { firstname: "Jane", lastname: "Smith" },
                { firstname: "Alice", lastname: "Johnson" },
                { firstname: "Bob", lastname: "Brown" },
            ]);

        return migration.compile();
    },
});
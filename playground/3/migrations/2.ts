/**
 * mongodbee migration
 */

import * as v from "valibot";
import { dbId } from "mongodbee";
import parent from "./1.ts";
import { migrationDefinition } from "../migration/migration.ts";

const schemas = {
    collections: {
        ...parent.schemas.collections,
        "+groups": {
            _id: dbId("group"),
            name: v.string(),
        }
    }
}

export default migrationDefinition("0000000000002", "Add Groups collection", {
    parent,
    schemas,
    migrate(migration) {
        migration.createCollection("+groups")
            .seed([
                { name: "Admins" },
                { name: "Editors" },
                { name: "Guests" },
            ])
            .done();

        return migration.compile();
    },
});
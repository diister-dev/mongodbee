/**
 * mongodbee migration
 */

import * as v from "valibot";
import { dbId } from "mongodbee";
import parent from "./0.ts";
import { migrationDefinition } from "../migration/migration.ts";

const schemas = {
    collections: {
        ...parent.schemas.collections,
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
}

export default migrationDefinition("0000000000001", "Add lastname to user", {
    parent,
    schemas,
    migrate(migration) {
        migration.collection("+users")
            .transform({
                up: (doc) => {
                    const { firstname, lastname, ...rest } = doc;
                    return { ...rest, fullname: `${firstname} ${lastname}` };
                },
                down: (doc) => {
                    const { fullname, ...rest } = doc;
                    const [firstname, ...lastnameParts] = fullname.split(" ");
                    const lastname = lastnameParts.join(" ");
                    return { ...rest, firstname, lastname };
                }
            })
            .seed([
                { fullname: "John Doe" },
                { fullname: "Jane Smith" },
                { fullname: "Alice Johnson" },
                { fullname: "Bob Brown" },
            ])

        migration.createCollection("+articles")
            .seed([
                { title: "First Article", content: "This is the content of the first article." },
                { title: "Second Article", content: "This is the content of the second article." },
            ]);

        return migration.compile();
    },
});
/**
 * mongodbee migration
 */

import * as v from "valibot";
import parent from "./2.ts";
import { migrationDefinition } from "../migration/migration.ts";

const schemas = {
    collections: {
        ...parent.schemas.collections,
        "+users": {
            _id: parent.schemas.collections['+users']._id,
            firstname: v.string(),
            lastname: v.string(),
        },
    }
}

export default migrationDefinition("0000000000003", "Replace fullname with firstname and lastname", {
    parent,
    schemas,
    migrate(migration) {
        migration.collection("+users")
            .transform({
                up: (doc) => {
                    const { fullname, ...rest } = doc;
                    const [firstname, ...lastnameParts] = fullname.split(" ");
                    const lastname = lastnameParts.join(" ");
                    return { ...rest, firstname, lastname };
                },
                down: (doc) => {
                    const { firstname, lastname, ...rest } = doc;
                    const fullnames = [];
                    if (firstname) fullnames.push(firstname);
                    if (lastname) fullnames.push(lastname);
                    const fullname = fullnames.join(" ");
                    return { ...rest, fullname };
                }
            })
            .done();

        return migration.compile();
    },
});
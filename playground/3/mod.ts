import { collection, dbId, MongoClient } from "mongodbee"
import * as v from "valibot"

import { migrationOrder } from "./migrations/mod.ts"
import { getSessionContext } from "mongodbee/session.ts";
import { deepEqual } from "./utilities.ts";
import { flattenEquals, flattenObject } from "./utilities/objectPath.ts";
import { schemas } from "./schemas/mod.ts";
import { toMongoValidator } from "mongodbee/validator.ts";

const DATABASE_MONGO_URL = 'mongodb://localhost:27017';
const DATABASE_MONGO_DB = "test_mongodbee";

export const client = new MongoClient(DATABASE_MONGO_URL);
export const database = client.db(DATABASE_MONGO_DB);

// Delete database for reset
await database.dropDatabase();

// const userCollection = await collection(database, "users", {
//     _id: dbId("user"),
//     firstname: v.string(),
// });

const finalSchema = schemas.collections;

console.debug("Ensure migrations are linked correctly...");
for (let i = 0; i < migrationOrder.length; i++) {
  const migration = migrationOrder[i];
  const migrationParent = migration.parent;
  if(i > 0) {
    const expectedParent = migrationOrder[i - 1];
    if(migrationParent?.id !== expectedParent.id) {
      throw new Error(`Migration ${migration.name} (${migration.id}) parent is not the previous migration ${expectedParent.name} (${expectedParent.id})`);
    }
  }
}

console.debug("Ensure final schema is the same as the last migration schema...");
const lastMigration = migrationOrder[migrationOrder.length - 1];
if (!lastMigration?.schema) throw new Error("Last migration has no schema");
if (!flattenEquals(finalSchema, lastMigration.schema)) {
  throw new Error("Final schema does not match last migration schema");
}

console.debug("Starting migrations...");

const sessionContext = await getSessionContext(client);
for (const migration of migrationOrder) {
  // await sessionContext.withSession(async (session) => {
    await migration.up({ db: database, client });
    console.debug("Verifying schema after migration...");
    for (const [collectionName, schema] of Object.entries(migration.schema)) {
      const coll = database.collection(collectionName);
      const jsonSchema = toMongoValidator(v.object(schema));
      // Find invalid documents
      const invalidDocs = await coll.find({ $nor: [jsonSchema] }).toArray();
      if (invalidDocs.length > 0) {
        console.log(jsonSchema);
        console.log(invalidDocs);
        throw new Error(`Migration ${migration.name} (${migration.id}) failed: Collection ${collectionName} has ${invalidDocs.length} invalid documents after migration.`);
      }
    }

    console.debug(`Migration ${migration.name} (${migration.id}) applied.`);
  // });
}

console.debug("All migrations applied.");
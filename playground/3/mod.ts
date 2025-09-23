import { collection, dbId, MongoClient } from "mongodbee"
import * as v from "valibot"
import { createMockGenerator, fake, locales } from "@diister/valibot-mock"

import { migrationOrder } from "./migrations/mod.ts"
import { getSessionContext } from "mongodbee/session.ts";
import { deepEqual } from "./utilities.ts";
import { flattenEquals, flattenObject } from "./utilities/objectPath.ts";
import { schemas } from "./schemas/mod.ts";
import { toMongoValidator } from "mongodbee/validator.ts";
import { migrationBuilder } from "./migration/migration.ts";

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

// console.debug("Ensure final schema is the same as the last migration schema...");
// const lastMigration = migrationOrder[migrationOrder.length - 1];
// if (!lastMigration?.schema) throw new Error("Last migration has no schema");
// if (!flattenEquals(finalSchema, lastMigration.schema)) {
//   throw new Error("Final schema does not match last migration schema");
// }

const migrationSimulation = {
  applyOperation(databaseContent: any, operation: any) {
    switch(operation.type) {
      case "create_collection" : {
        if(databaseContent.collections[operation.collectionName]) {
          throw new Error(`Collection ${operation.collectionName} already exists`);
        }
        databaseContent.collections[operation.collectionName] = {
          content: [],
        };
        break;
      }
      case "seed_collection" : {
        if(!databaseContent.collections[operation.collectionName]) {
          throw new Error(`Collection ${operation.collectionName} does not exist for seeding`);
        }
        databaseContent.collections[operation.collectionName].content.push(...operation.documents);
        break;
      }
      case "transform_collection" : {
        if(!databaseContent.collections[operation.collectionName]) {
          throw new Error(`Collection ${operation.collectionName} does not exist for transforming`);
        }
        databaseContent.collections[operation.collectionName].content = databaseContent.collections[operation.collectionName].content.map((doc: any) => operation.up(doc));
        break;
      }
    }
    return databaseContent;
  },
  applyReverseOperation(databaseContent: any, operation: any) {
    switch(operation.type) {
      case "create_collection" : {
        if(!databaseContent.collections[operation.collectionName]) {
          throw new Error(`Collection ${operation.collectionName} does not exist for dropping`);
        }
        delete databaseContent.collections[operation.collectionName];
        break;
      }
      case "seed_collection" : {
        if(!databaseContent.collections[operation.collectionName]) {
          throw new Error(`Collection ${operation.collectionName} does not exist for unseeding`);
        }
        const seededIds = new Set(operation.documents.map((doc: any) => doc._id));
        databaseContent.collections[operation.collectionName].content = databaseContent.collections[operation.collectionName].content.filter((doc: any) => !seededIds.has(doc._id));
        break;
      }
      case "transform_collection" : {
        if(!databaseContent.collections[operation.collectionName]) {
          throw new Error(`Collection ${operation.collectionName} does not exist for reverse transforming`);
        }
        databaseContent.collections[operation.collectionName].content = databaseContent.collections[operation.collectionName].content.map((doc: any) => operation.down(doc));
        break;
      }
    }
    return databaseContent;
  }
}


async function verifyMigrationIntegrity(migrationDeclaration: any) {
  const builder = migrationBuilder({
    schemas: migrationDeclaration.schemas,
  });
  const migrationResult = migrationDeclaration.migrate(builder);
  let databaseContent: any = {
    collections: {},
    // multiCollections: {}, // @TODO
  };

  // Simulate existing data from parent migration
  if(migrationDeclaration.parent) {
    const generator = Object.fromEntries(Object.entries(migrationDeclaration.parent.schemas.collections).map(([collectionName, schema]) => {
      return [collectionName, createMockGenerator(v.object(schema as any), {
        faker: {
          locale: [locales.en],
          // seed: 1234,
        },
        defaultStringMaxLength: 32,
      })];
    }));
    
    for(const collectionName in generator) {
      databaseContent.collections[collectionName] = {
        content: generator[collectionName].generateMany(5),
      }
    }
  }

  const initialDatabaseContent = JSON.parse(JSON.stringify(databaseContent));
  for(const operation of migrationResult.operations) {
    const cloneState = JSON.parse(JSON.stringify(databaseContent));
    databaseContent = migrationSimulation.applyOperation(cloneState, operation);
  }

  // Now reverse the operations to see if we get back to the initial state
  if(!migrationResult.hasProperty('irreversible')) {
    let start = JSON.parse(JSON.stringify(databaseContent));
    for(let i = migrationResult.operations.length - 1; i >= 0; i--) {
      const operation = migrationResult.operations[i];
      const cloneState = JSON.parse(JSON.stringify(databaseContent));
      start = migrationSimulation.applyReverseOperation(cloneState, operation);
    }

    if(!deepEqual(start, initialDatabaseContent)) {
      console.log("Initial Database Content:", JSON.stringify(initialDatabaseContent, null, 2));
      console.log("Final Database Content:", JSON.stringify(start, null, 2));
      throw new Error(`Migration ${migrationDeclaration.name} (${migrationDeclaration.id}) is not reversible`);
    }
  }

  const validCollections = [];
  const invalidCollections = [];

  // Check the databaseContent against the schema
  for(const [collectionName, docSchema] of Object.entries(migrationDeclaration.schemas.collections)) {
    const docs = databaseContent.collections[collectionName];
    if(!docs) {
      throw new Error(`Migration ${migrationDeclaration.name} (${migrationDeclaration.id}) is missing collection ${collectionName} in the database content`);
    }
    const allOks = docs.content.every((doc: any) => {
      return v.safeParse(v.object(docSchema), doc).success;
    });
    if(allOks) {
      validCollections.push(collectionName);
    } else {
      invalidCollections.push(collectionName);
    }
  }

  if(invalidCollections.length > 0) {
    throw new Error(`Migration ${migrationDeclaration.name} (${migrationDeclaration.id}) has invalid documents in collections: ${invalidCollections.join(", ")}`);
  }

  return {
    ok: true,
    databaseContent,
  }
}

for(const migration of migrationOrder) {
  console.debug(`Verifying migration integrity: ${migration.name} (${migration.id})`);
  await verifyMigrationIntegrity(migration);
}

console.debug("✅ All migrations verified successfully.");

function createMongoMigrationApplier(client: MongoClient, databaseName: string) {
  const db = client.db(databaseName);

  return {
    async applyOperation(operation: any) {
      switch(operation.type) {
        case "create_collection" : {
          const collections = await db.listCollections({ name: operation.collectionName }).toArray();
          if(collections.length > 0) {
            throw new Error(`Collection ${operation.collectionName} already exists`);
          }
          await db.createCollection(operation.collectionName);
          break;
        }
        case "seed_collection" : {
          const coll = db.collection(operation.collectionName);
          await coll.insertMany(operation.documents);
          break;
        }
        case "transform_collection" : {
          const coll = db.collection(operation.collectionName);
          const allDocs = await coll.find().toArray();
          const transformedDocs = allDocs.map((doc) => operation.up(doc));
          for(const doc of transformedDocs) {
            await coll.updateOne({ _id: doc._id }, { $set: doc });
          }
          break;
        }
      }
    },
    async applyReverseOperation(operation: any) {
      switch(operation.type) {
        case "create_collection" : {
          const collections = await db.listCollections({ name: operation.collectionName }).toArray();
          if(collections.length === 0) {
            throw new Error(`Collection ${operation.collectionName} does not exist for dropping`);
          }
          await db.collection(operation.collectionName).drop();
          break;
        }
        case "seed_collection" : {
          const coll = db.collection(operation.collectionName);
          const seededIds = new Set(operation.documents.map((doc: any) => doc._id));
          await coll.deleteMany({ _id: { $in: Array.from(seededIds) } });
          break;
        }
        case "transform_collection" : {
          const coll = db.collection(operation.collectionName);
          const allDocs = await coll.find().toArray();
          const transformedDocs = allDocs.map((doc) => operation.down(doc));
          for(const doc of transformedDocs) {
            await coll.updateOne({ _id: doc._id }, { $set: doc });
          }
          break;
        }
      }
    }
  }
}

const applier = createMongoMigrationApplier(client, DATABASE_MONGO_DB);

console.debug("Applying migrations to MongoDB...");
for(const migration of migrationOrder) {
  const builder = migrationBuilder({
    schemas: migration.schemas,
  });
  const migrationResult = migration.migrate(builder);
  for(const operation of migrationResult.operations) {
    await applier.applyOperation(operation);
  }
  console.debug(`Migration ${migration.name} (${migration.id}) applied.`);
}

console.debug("Verifying final database schema...");
const lastSchema = migrationOrder[migrationOrder.length - 1].schemas;
for(const [collectionName, schema] of Object.entries(lastSchema.collections)) {
  const collection = database.collection(collectionName);
  const items = await collection.find();
  for await (const item of items) {
    const parseResult = v.safeParse(v.object(schema), item);
    if(!parseResult.success) {
      console.error(`Document in collection ${collectionName} failed schema validation:`, item, parseResult.issues);
      throw new Error(`Document in collection ${collectionName} failed schema validation`);
    }
  }
}

console.debug("--- All migrations applied.");
client.close();
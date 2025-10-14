# Protocole de test pour vérifier le système de migration de Mongodbee

1. Create a new directory named `test-{n}` (ex: `test-1`, `test-2`, etc.), this will be your test case directory.
   The `{n}` should be incremented for each new test case you create.
   And must be placed under `playground/testing-migration`.
2. Copy the `template` directory into your newly created test case directory.
   It contain a basic structure to start with.
3. Update the .env file with a unique `MONGODB_DATABASE` name for your test case. ex: `mongodbee_test_db_{n}`.
4. Inside the test directory, run `deno task mongodbee init` to initialize the database with the initial migration.
   There will be a `mongodbee.config.ts` file created in your test directory, update it with
   ```ts
   database: {
    connection: {
      uri: Deno.env.get("MONGODB_URI"),
    },
    name: Deno.env.get("MONGODB_DATABASE"),
  },...
   ```

After this steps, you are ready to start your test journey.

## Test case structure
```
- test-{n}
  - app.ts // Your application code, can be empty if not needed for the test
  - migrations // Directory containing your migration files
    - V1__initial_migration.ts
    - V2__second_migration.ts
    - ...
  - .env // Environment variables for your test case
  - mongodbee.config.ts // Mongodbee configuration file
  - schemas.ts // Schema definitions, represent the current state of the database schemas
```
The goal: You must simulate the creation of an application that will evolve over time.

- Start with an initial goal (ex: a blog application with users and posts).
- Update the `schemas.ts` file to represent the current state of your database schemas.
- Create migration files in the `migrations` directory to evolve your database schema over time.
- Update your application code in `app.ts` if needed to reflect the changes in your database schema.
- Try to break the migration system with differents scenarios, like invalidad migration, or missing writing migration elements files, or invalid rollback etc...
- Continue update your `schemas.ts`, create new migration files, and update your application code as needed.

## The commands to use
- `deno task mongodbee init`: Initialize the database with the initial migration.
- `deno task mongodbee migrate`: Apply all pending migrations to the database.
- `deno task mongodbee rollback`: Rollback the last applied migration.
- `deno task mongodbee status`: Show the current status of migrations (applied and pending).
- `deno task mongodbee check`: Check your migration files are valid (simulate the migration without applying it in memory).
- `deno task mongodbee generate --name "<migration-name>"`: Generate a new migration file with the given name.

## Schemas content

The mongodbee proejct use a lot `valibot` for schema definition.
A schema can define collections, multi-collections and multi-models.
collections: single collection with a fixed schema.
multi-collections: single collection that contain multiple document types with different schemas.
multi-models: single collection that contain multiple document types, but we can't know the name of the collection in advance.
    This is used for a set of collections that share the same schema.
    (ex: a "drive" model that can be used for multiple users, each user have its own collection)
    (ex: a "exposition" model that can be used for multiple exhibitions, each exhibition have its own collection)

Here is a simple example of `schemas.ts` file content:
```ts
import * as v from "valibot";
import { dbId, defineModel, refId } from "@diister/mongodbee";
import { SchemasDefinition } from "@diister/mongodbee/migration";

export const driveModel = defineModel("drive", {
  schema: {
    info: {
      _id: v.literal("info:0"), // constants can be used as _id to have a single document in the collection
      owner: refId("user"),
    },
    file: {
      _id: dbId("file"), // dbId is used to generate a unique id for each document (like: file:<ulid> here)
      name: v.string(),
    }
  }
});

export const schemas = {
  collections: {
    // Name of the collection is "users"
    "users": {
      // User schema
      _id: dbId("user"),
      name: v.string(),
      email: v.string(),
    }
  },
  multiCollections: {
    // Name of the collection is "original-drive"
    "original-drive": driveModel.schema,
    otherMultiCollection: {
        // Name of the collection is "otherMultiCollection"
        typeA: {
            // No _id is defined, the _id will be `typeA:<ulid>` here
            fieldA: v.string(),
        },
        typeB: {
            // No _id is defined, the _id will be `typeB:<ulid>` here
            fieldB: v.number(),
        }
    }
  },
  multiModels: {
    ...driveModel.expose(),
  }
} satisfies SchemasDefinition;
```

The rule is you CANNOT INCLUDE `schemas.ts` in your migration files.
This is to ensure that the migration files are independent and can be executed without relying on the current state of the schemas.

## Migration files content

Migration files are TypeScript files that declare a migration using the `defineMigration` function from Mongodbee.

For example, here is a simple migration file that create an initial collection:
```ts
/**
 * This migration was generated using MongoDBee CLI
 * Please edit the migration logic in the migrate() function.
 * @module
 */

import { migrationDefinition } from "@diister/mongodbee/migration";
import { dbId } from "@diister/mongodbee";
import * as v from "valibot";

const id = "2025_10_09_1445_4G3198R0CE@init";
const name = "init";

export default migrationDefinition(id, name, {
  parent: null, // No parent migration, this is the first migration
  // Define the schemas as they should be after this migration is applied
  schemas: {
    collections: {
      user: {
        _id: dbId("user"),
        name: v.string(),
        age: v.number(),
      }
    },
    multiCollections: {
      "original-drive": {
        info: {
          _id: v.literal("info:0"),
          owner: dbId("user"),
        },
        file: {
          _id: dbId("file"),
          name: v.string(),
        }
      },
      hello: {
        hello1: {
          content: v.string(),
        },
        hello2: {
          content: v.string(),
        }
      }
    },
    multiModels: {
      drive: {
        info: {
          _id: v.literal("info:0"),
          owner: dbId("user"),
        },
        file: {
          _id: dbId("file"),
          name: v.string(),
        }
      }
    }
  },
  migrate(migration) {
    migration.createCollection("user")
      .seed([
        { name: "Alice", age: 30 },
        { name: "Bob", age: 25 },
      ])
      
    migration.createMultiCollection("original-drive")
      .type("info")
        .seed([
          { _id: "info:0", owner: "user:1" },
        ]).end()
      .type("file")
        .seed([
          { name: "file1.txt" },
          { name: "file2.txt" },
        ]).end()

    // During your exploration, you can forget to add this part, to check if the system detect it for example
    migration.createMultiCollection("hello")
      .type("hello1")
        .seed([
          { content: "hello" },
          { content: "hello" },
        ]).end()
      .type("hello2")
        .seed([
          { content: "world" },
          { content: "world" },
        ]).end()

    migration.createMultiModelInstance("drive1", "drive")
      .type("info")
        .seed([
          { _id: "info:0", owner: "user:1" },
        ]).end()
      .type("file")
        .seed([
          { name: "file1.txt" },
          { name: "file2.txt" },
        ]).end()

    // At the end of your migration, always return the compiled migration
    return migration.compile();
  },
})
```
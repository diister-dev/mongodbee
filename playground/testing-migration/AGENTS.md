# Testing Protocol for MongoDBee Migration System

## Goal

The goal is to **simulate a complete development day** by creating an application that evolves progressively through multiple successive migrations. The objective is to **push the system to its limits** to verify it works correctly in real-world usage scenarios.

**Important**: Create **ONE SINGLE test case** that will evolve over time, not multiple separate tests!

## Initial Setup

1. Copy the `template` directory to a new test directory:
   ```bash
   cp -r playground/testing-migration/template playground/testing-migration/test-migration-{n}
   ```
   The `{n}` should be incremented for each new test case you create (ex: `test-migration-1`, `test-migration-2`, etc.)

2. Navigate to your test directory:
   ```bash
   cd playground/testing-migration/test-migration-{n}
   ```

3. Create a `.env` file
   ```bash
   # .env content:
   MONGODB_URI=mongodb://localhost:27017
   MONGODB_DATABASE=mongodbee_test_db_{n}
   ```

4. Run `deno task mongodbee init` to initialize the database with the initial migration.
   This will create a `mongodbee.config.ts` file and a `migrations` directory.

5. Update the generated `mongodbee.config.ts` file to use environment variables:
   ```ts
   database: {
    connection: {
      uri: Deno.env.get("MONGODB_URI"),
    },
    name: Deno.env.get("MONGODB_DATABASE"),
  },...
   ```

After these steps, you are ready to start your test journey.

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
## What to do

You must simulate the creation of an application that evolves over time through a **typical development day**:

1. **Start with an initial goal** (ex: a social media platform, e-commerce site, project management tool, etc.)
2. **Create the first migration** with your initial schema (collections, multi-collections, multi-models)
3. **Update `schemas.ts`** to reflect the current state after migration
4. **Update `app.ts`** to use the database (read/write operations)
5. **Iterate multiple times** by:
   - Adding new features (new collections, new fields)
   - Modifying existing schemas (rename fields, change types, add constraints)
   - Creating complex data migrations (transforming existing data)
   - Adding multi-model instances dynamically
   - Testing edge cases and error scenarios

## Important Notes for Testing

### 💡 Utility Scripts

The template includes helpful utility scripts in the `scripts/` directory:

**scripts/drop_database.ts** - Quickly reset your test database:
```bash
deno run -A --env-file=.env scripts/drop_database.ts
```

**scripts/check_indexes.ts** - Inspect indexes on a collection:
```bash
deno run -A --env-file=.env scripts/check_indexes.ts users
```

**scripts/list_collections.ts** - List all collections with document counts:
```bash
deno run -A --env-file=.env scripts/list_collections.ts
```

See `scripts/README.md` for more details and usage examples.

### ⚠️ Common Pitfalls & Solutions

**Pitfall #1: Testing with duplicate data when creating unique indexes**
- Problem: If you seed data with duplicate values and then try to evolve an index to be unique, MongoDB will reject it
- Solution: Always ensure your seed data has unique values for fields that will have unique indexes

**Pitfall #2: Multi-model discovery**
- Problem: There's NO external `__mongodbee_multi_collection_metadata` collection
- Reality: MongoDBee discovers multi-model instances by scanning ALL collections and checking for the `_information` document inside each one
- Use: `discoverMultiCollectionInstances(db, "modelType")` to find instances

**Pitfall #3: Testing index evolution**
- Tip: Create test scenarios where you evolve indexes (simple → unique, add/remove options)
- Use `migration.updateIndexes("collectionName")` to trigger index synchronization
- Test rollback to ensure indexes are properly restored

**Pitfall #4: Generating multiple migrations quickly**
- Problem: If you generate migrations too fast (same second), they might get the same timestamp
- Solution: The ULID part ensures ordering even within the same second, but it's safer to add a small delay (`sleep 3`) between generation commands

## Testing Methodology: Be Exploratory!

The goal is to **break things on purpose** to verify the system catches errors. Think like a developer who:
- Makes mistakes
- Forgets things
- Tries shortcuts
- Doesn't read the docs carefully

### 🔍 After EVERY change, test the system state

Don't wait to accumulate multiple migrations before testing. After each action:

```bash
# After creating/editing a migration
deno task mongodbee check   # Does it validate correctly?
deno task mongodbee status  # What's the current state?

# After applying a migration
deno task mongodbee status  # Was it applied?
deno run -A --env-file=.env app.ts  # Does the app still work?
```

### 🧪 Things to test

**1. Normal scenarios** (to verify it works):
- Creating collections, adding fields, seeding data
- Complex migrations: transforming data, renaming fields
- Multi-models: creating instances dynamically

**2. Error scenarios** (try to break it!):
- **Forget to call `.end()`** in a migration builder chain
- **Use wrong API**: try `migration.updateCollection()` instead of `migration.collection()`
- **Missing schema definition**: declare a collection in schema but forget to create it in migration
- **Inconsistent schemas**: make parent schema and current schema incompatible
- **Invalid transform**: write a transform that throws an error
- **Wrong type names**: reference a multi-collection type that doesn't exist
- **Forget `.compile()`**: don't return `migration.compile()` at the end

**3. Rollback scenarios**:
- Apply a migration, then immediately rollback
- Try to rollback a "lossy" migration
- Rollback then re-apply to see if it's idempotent

**4. Application evolution** (simulate real development):
- **After each migration**, update `app.ts` to use the new schema
- Add code that reads/writes to new collections
- Remove code that used old fields after a transform
- Test that your app crashes if migrations aren't applied

**5. Edge cases**:
- Empty collections (no seed data)
- Very large seed datasets (100+ documents)
- Circular references between collections
- Rename a collection (requires data migration)

### 📝 Recommended Testing Flow

```
1. Write migration → check → Fix errors → check again
2. Once valid → migrate → status → verify applied
3. Update app.ts → run app → verify it works
4. Make app.ts do something with the new data
5. (Optional) Test rollback → run app → verify old behavior
6. Repeat with next migration
```

**Remember**: The more you try to break it, the more confident you'll be that it works!

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

## Using MongoDBee in app.ts

Here's how to use MongoDBee collections in your application code:

### Basic Setup

```ts
import { MongoClient, collection, multiCollection } from "@diister/mongodbee";
import { checkMigrationStatus, discoverMultiCollectionInstances } from "@diister/mongodbee/migration";
import { schemas } from "./schemas.ts";

const client = new MongoClient(Deno.env.get("MONGODB_URI")!);
await client.connect();
const db = client.db(Deno.env.get("MONGODB_DATABASE")!);

// Check migrations are up to date
const migrationState = await checkMigrationStatus({ db });
if (!migrationState.ok) {
  console.error("Migration check failed:", migrationState.message);
  Deno.exit(1);
}
```

### Working with Collections

```ts
// Get a collection (NOTE: collection() returns a Promise, use await!)
const users = await collection(db, "users", schemas.collections.users);

// Count documents
const count = await users.countDocuments({});

// Find all documents (NOTE: find() requires at least an empty object {})
for await (const user of users.find({})) {
  console.log(user.name, user.email);
}

// Find one document
const user = await users.findOne({ _id: "user:123" });

// Insert a document
await users.insertOne({ name: "John", email: "john@example.com" });
```

### Working with Multi-Collections

```ts
// Get a multi-collection (NOTE: also returns a Promise, use await!)
const analytics = await multiCollection(db, "analytics", schemas.multiCollections.analytics);

// Find documents of a specific type (NOTE: API is find(type, query))
const dailyStats = await analytics.find("dailyStats", {});

// Find one document of a specific type
const stat = await analytics.findOne("dailyStats", { _id: "2025-10-14" });
```

### Working with Multi-Models

```ts
// Discover all instances of a multi-model
const instances = await discoverMultiCollectionInstances(db, "visitor");
console.log(`Found ${instances.length} visitor collections`);

// Access each instance
for (const instanceName of instances) {
  const visitorCol = await multiCollection(db, instanceName, schemas.multiModels.visitor);
  const entries = await visitorCol.find("entry", {});
  console.log(`Instance ${instanceName} has ${entries.length} entries`);
}
```

**Important Notes:**
- `collection()` and `multiCollection()` are **async** - always use `await`
- `find()` requires at least an empty object `{}` as parameter
- For multi-collections: use `find(type, query)` NOT `find().type()`

## Migration Builder API Reference

The `migration` object in the `migrate()` function provides these methods:

### Creating new collections/multi-collections

```ts
// Create a regular collection
migration.createCollection("collectionName")
  .seed([{ field1: "value", field2: 123 }])
  .end();

// Create a multi-collection (single collection with multiple document types)
migration.createMultiCollection("multiCollectionName")
  .type("typeA")
    .seed([{ fieldA: "value" }])
    .end()
  .type("typeB")
    .seed([{ fieldB: 123 }])
    .end();

// Create a multi-model instance (separate collection for this instance)
migration.createMultiModelInstance("instanceName", "modelType")
  .type("typeA")
    .seed([{ field: "value" }])
    .end();
```

### Modifying existing collections

```ts
// Transform documents in an existing collection
migration.collection("collectionName")
  .transform({
    up: (doc) => ({ ...doc, newField: "defaultValue" }),
    down: (doc) => {
      const { newField, ...rest } = doc;
      return rest;
    },
    lossy: true, // optional: marks that rollback will lose data
  })
  .end();

// Another transform example: rename field
migration.collection("collectionName")
  .transform({
    up: (doc) => ({ ...doc, newName: doc.oldName }),
    down: (doc) => {
      const { newName, ...rest } = doc;
      return { ...rest, oldName: newName };
    },
  })
  .end();

// Update indexes (when you change index options in the schema)
migration.updateIndexes("collectionName");
```

### Working with Indexes

You can add indexes to fields using `withIndex()`:

```ts
import { dbId, withIndex } from "@diister/mongodbee";
import * as v from "valibot";

// Simple index (non-unique)
email: withIndex(v.string())

// Unique index (case-insensitive)
email: withIndex(v.string(), { unique: true, insensitive: true })

// Index on a reference field
userId: withIndex(refId("user"))
```

**Evolving Indexes:**
When you change index options between migrations (e.g., making an index unique), use `migration.updateIndexes()`:

```ts
// Migration 1: Simple index
schemas: {
  collections: {
    users: {
      email: withIndex(v.string())  // Non-unique
    }
  }
}

// Migration 2: Make it unique
schemas: {
  collections: {
    users: {
      email: withIndex(v.string(), { unique: true })  // Now unique!
    }
  }
}
migrate(migration) {
  migration.updateIndexes("users");  // This will drop and recreate the index
  return migration.compile();
}
```

**Important Notes:**
- There is NO `migration.updateCollection()` method
- Use `migration.collection()` with `.transform()` to modify existing documents
- Use `migration.updateIndexes()` to synchronize index changes
- Always call `.end()` to finish a builder chain
- Always `return migration.compile()` at the end of `migrate()`

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
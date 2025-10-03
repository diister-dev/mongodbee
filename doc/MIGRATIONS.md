# MongoDBee Migration System

## Overview

MongoDBee uses a migration system to manage database schema evolution in a versioned, traceable, and reversible way. Migrations are TypeScript files that describe the transformations to apply to the database.

## Core Concepts

### 1. Frozen-in-Time Migrations

**CRITICAL RULE**: A migration must be **completely autonomous and frozen in time**.

❌ **BAD** - Never reference `schemas.ts`:
```typescript
import { schemas } from "../schemas.ts";  // ❌ NO!

export default migrationDefinition(id, name, {
  parent: null,
  schemas,  // ❌ The schema can evolve!
  migrate(migration) { ... }
})
```

✅ **GOOD** - Define the schema directly in the migration:
```typescript
import * as v from "valibot";

export default migrationDefinition(id, name, {
  parent: null,
  schemas: {
    collections: {},
    multiCollections: {
      exposition: {
        artwork: {
          title: v.string(),
          artist: v.string(),
          year: v.number()
        }
      }
    }
  },
  migrate(migration) { ... }
})
```

**Why?** If `schemas.ts` evolves, your migration will no longer represent the schema state at the time it was created. The migration chain must be reproducible identically.

### 2. Migration Chain

Migrations form a **linear chain** where each migration has a parent (except the first one).

```
M1 (initial) → M2 (add_field) → M3 (transform) → ...
```

Each child migration **inherits and extends** its parent's schema:

```typescript
import parent from "./2025_10_01_1000_XXXXX@initial.ts";

export default migrationDefinition(id, name, {
  parent: parent,  // Reference the parent module
  schemas: {
    collections: {
      ...parent.schemas.collections,  // Inherit from parent
      // Modifications...
    },
    multiCollections: {
      ...parent.schemas.multiCollections,  // Inherit from parent
      // Modifications...
    }
  },
  migrate(migration) { ... }
})
```

### 3. The `schemas.ts` File

`schemas.ts` represents the **final state** of the schema after applying **ALL** migrations.

**Workflow**:
1. Create a migration → it defines its own schema
2. Apply the migration → it transforms the DB
3. Update `schemas.ts` → it reflects the new final state

`schemas.ts` is the **source of truth** for the current schema, but **never referenced** by migrations.

### 4. Multi-Collections

A **multi-collection** is a single MongoDB collection that stores multiple document types, identified by a `_type` field.

**Structure**:
```typescript
multiCollections: {
  exposition: {           // Multi-collection type/model name
    artwork: {           // Document type
      title: v.string(),
      artist: v.string()
    },
    visitor: {           // Another type
      name: v.string()
    }
  }
}
```

**In MongoDB**:
```javascript
// Collection "exposition_louvre"
{ _type: "artwork", title: "Mona Lisa", artist: "Da Vinci" }
{ _type: "visitor", name: "John Doe" }

// Special metadata documents (managed automatically)
{ _id: "_information", _type: "_information", collectionType: "exposition", createdAt: Date }
{ _id: "_migrations", _type: "_migrations", fromMigrationId: "...", appliedMigrations: [...] }
```

**Multi-Collection Model**:
To avoid duplication, use `createMultiCollectionModel()` in `models.ts`:

```typescript
// models.ts
export const expositionModel = createMultiCollectionModel("exposition", {
  schema: {
    artwork: { ... },
    visitor: { ... }
  },
  version: "1.0.0"
});

// schemas.ts
import { expositionModel } from "./models.ts";

export const schemas = {
  collections: {},
  multiCollections: {
    ...expositionModel.expose()
  }
};
```

**Multi-Collection Instances**:
- `exposition` is the **model/type** (defined in schema)
- Collections like `exposition_louvre`, `my_gallery`, or any name you choose are **instances** (physical MongoDB collections)

**Important**: You have **full control** over collection names - there's no automatic prefixing!

Creating an instance:
```typescript
import { newMultiCollection } from "@diister/mongodbee";

// Creates the MongoDB collection "exposition_louvre" with metadata
const louvre = await newMultiCollection(db, "exposition_louvre", expositionModel);

// Or any name you want:
const myGallery = await newMultiCollection(db, "my_awesome_gallery", expositionModel);
```

### 5. Multi-Collection Metadata & Version Tracking

Each multi-collection instance stores two special metadata documents:

#### `_information` Document
```javascript
{
  _id: "_information",
  _type: "_information",
  collectionType: "exposition",  // The model/type name
  createdAt: Date
}
```

#### `_migrations` Document
```javascript
{
  _id: "_migrations",
  _type: "_migrations",
  fromMigrationId: "2025_10_01_1000_H0XWCWC4E6@initial",  // When created
  mongodbeeVersion: "0.13.0",  // MongoDBee version at creation
  appliedMigrations: [
    { 
      id: "2025_10_01_1000_H0XWCWC4E6@initial", 
      appliedAt: Date,
      mongodbeeVersion: "0.13.0"  // Version that applied this migration
    },
    { 
      id: "2025_10_02_1100_YYYYYYY@add_field", 
      appliedAt: Date,
      mongodbeeVersion: "0.13.0"  // Version that applied this migration
    }
  ]
}
```

**Importance**: During transformations via `.multiCollection().type().transform()`, the system:
1. Discovers all instances by querying for `_information` documents with matching `collectionType`
2. Compares the instance's `fromMigrationId` with the current migration ID
3. **Skips instances created AFTER** the migration
4. Applies only to instances created BEFORE

**Example**:
```
Timeline:
- M1: Creates "exposition" model schema
- Louvre created (fromMigrationId = M1)
- M2: Adds "description" field via transform
- Pompidou created (fromMigrationId = M2)

During M2.apply():
- Louvre: receives the transform (created BEFORE M2)
- Pompidou: SKIPPED (created AFTER M2, already has correct schema)
```

### 6. Validators and Indexes

**Automatic Application**: When you apply a migration, the system automatically:
1. Creates MongoDB JSON Schema validators for all collections
2. Creates indexes defined with `withIndex()` in schemas
3. Updates validators and indexes on existing collections to match the current schema

**Manual Control**: If you need to manually apply validators/indexes:
```typescript
import { applySecurityToCollection, applySecurityToMultiCollection } from "@diister/mongodbee";

// For regular collections
await applySecurityToCollection(db, "users", usersSchema);

// For multi-collections
await applySecurityToMultiCollection(db, "comments_main", commentsMultiCollectionSchema);
```

**Updating Indexes**: If you add new indexes to your schema:
```typescript
// In migration
migrate(migration) {
  return migration
    .updateIndexes("users")  // Updates indexes to match schema
    .compile();
}
```

### 7. MongoDBee Version Tracking

**Global Migration History**: Every migration operation (apply, revert, fail) is recorded in the `__dbee_migration__` collection with the MongoDBee version that executed it:

```javascript
{
  migrationId: "2025_10_01_1000_H0XWCWC4E6@initial",
  migrationName: "initial",
  operation: "applied",  // 'applied' | 'reverted' | 'failed'
  executedAt: Date,
  duration: 1500,  // milliseconds
  status: "success",  // 'success' | 'failure'
  mongodbeeVersion: "0.13.0"  // ✨ Version that executed this operation
}
```

**Multi-Collection Instance Tracking**: Each multi-collection instance also tracks versions:
- `mongodbeeVersion` at creation time
- `mongodbeeVersion` for each applied migration

**Why This Matters**:
- 📊 **Audit Trail**: Know exactly which version performed each operation
- 🐛 **Debugging**: "This bug appeared after upgrading to v0.14" → check which migrations ran with that version
- 🔄 **Rollback Safety**: Understand compatibility when rolling back to older versions
- 📈 **Analytics**: Track MongoDBee adoption and upgrade patterns across your infrastructure

**Accessing Version Information**:
```typescript
import { getAllOperations } from "@diister/mongodbee/migration";

const operations = await getAllOperations(db);
operations.forEach(op => {
  console.log(`${op.migrationName} executed with MongoDBee v${op.mongodbeeVersion}`);
});
```

## CLI Commands

### Configuration File

MongoDBee uses a configuration file (`mongodbee.config.ts`) to specify database connection and file paths. This file is automatically created by `mongodbee init`.

#### Basic Configuration Structure

```typescript
// mongodbee.config.ts
export default {
  database: {
    connection: {
      uri: "mongodb://localhost:27017"
    },
    name: "myapp"
  },
  paths: {
    migrations: "./migrations",
    schemas: "./schemas.ts"
  }
};
```

#### Using `defineConfig()` for Type Safety

For better TypeScript support and autocomplete, you can use the `defineConfig()` helper:

```typescript
import { defineConfig } from "@diister/mongodbee";

export default defineConfig({
  database: {
    connection: {
      uri: process.env.MONGODB_URI || "mongodb://localhost:27017",
      options: {
        maxPoolSize: 10,
        connectTimeoutMS: 5000
      }
    },
    name: process.env.MONGODB_DATABASE || "myapp"
  },
  paths: {
    migrations: "./migrations",
    schemas: "./schemas.ts"
  }
});
```

#### Configuration Options

**Database Configuration**:
- `database.connection.uri` - MongoDB connection URI (required)
- `database.connection.options` - MongoDB driver options (optional)
- `database.name` - Target database name (required)

**Paths Configuration**:
- `paths.migrations` - Directory containing migration files (default: `"./migrations"`)
- `paths.schemas` - Path to schemas file (default: `"./schemas.ts"`)

#### Environment Variables

You can use environment variables in your configuration:

```typescript
// mongodbee.config.ts
export default {
  database: {
    connection: {
      uri: Deno.env.get("MONGODB_URI") || "mongodb://localhost:27017"
    },
    name: Deno.env.get("MONGODB_DATABASE") || "myapp"
  },
  paths: {
    migrations: "./migrations",
    schemas: "./schemas.ts"
  }
};
```

With a `.env` file:
```env
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=myapp
```

#### Advanced Configuration

The CLI internally uses a more comprehensive configuration system (`MigrationSystemConfig`) with additional options for:
- Migration execution settings (dry-run, backup, batch size)
- Logging configuration (level, format, console/file output)
- CLI behavior (colors, progress indicators, confirmations)
- Environment-specific overrides

These advanced options are automatically applied with sensible defaults. Users typically only need to configure `database` and `paths` in their `mongodbee.config.ts` file.

**Note**: The simple `MongodbeeConfig` type (used in user config files) is a subset of the internal `MigrationSystemConfig` type. This keeps the user-facing API simple while allowing the system to have rich internal configuration.

### Complete Workflow

```bash
# 1. Initialize MongoDBee
deno task mongodbee init

# This creates:
# - migrations/
# - schemas.ts
# - mongodbee.config.ts

# 2. Define models (optional but recommended)
# Create models.ts with createMultiCollectionModel()

# 3. Update schemas.ts
# Use model.expose() to inject the schema

# 4. Generate a migration with a descriptive name
deno task mongodbee generate --name "initial_setup"
deno task mongodbee generate --name "add_user_bio"
deno task mongodbee generate --name "transform_comments"

# ⚠️ IMPORTANT: Use --name flag for named migrations
# Format will be: YYYY_MM_DD_HHMM_ULID@your_name

# 5. Edit the generated migration
# - Define the schema (copy from schemas.ts)
# - Define operations in migrate()

# 6. Check status
deno task mongodbee status

# 7. Apply the migration
deno task mongodbee apply

# 8. Rollback if needed
deno task mongodbee rollback

# 9. View history
deno task mongodbee status
```

### deno.json Configuration

```json
{
  "tasks": {
    "mongodbee": "deno run --allow-read --allow-write --allow-net --allow-env --allow-sys --env-file=.env ../../library/src/migration/cli/bin.ts"
  },
  "imports": {
    "@diister/mongodbee": "../../library/mod.ts",
    "@diister/mongodbee/migration": "../../library/src/migration/mod.ts",
    "mongodb": "npm:mongodb@^6.11.0",
    "valibot": "npm:valibot@^0.42.1"
  }
}
```

### .env Configuration

```env
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=myapp
```

## Creating Multi-Collection Instances

**⚠️ CRITICAL**: Always use the proper functions to create multi-collection instances!

### ✅ Best Practice: Use `newMultiCollection()`

```typescript
import { newMultiCollection } from "@diister/mongodbee";
import { commentsModel } from "./models.ts";

// You control the collection name - no automatic prefixing!
const mainComments = await newMultiCollection(db, "comments_main", commentsModel);
const blogComments = await newMultiCollection(db, "blog_comments", commentsModel);
const forumComments = await newMultiCollection(db, "forum_posts", commentsModel);
```

**Why?** This function:
1. ✅ Automatically detects the last applied migration
2. ✅ Creates metadata documents (`_information` and `_migrations`)
3. ✅ Marks the instance with `fromMigrationId` for version tracking
4. ✅ Applies JSON Schema validators and indexes
5. ✅ Returns a ready-to-use multi-collection instance

### ❌ Wrong: Manual Creation

```typescript
// ❌ DON'T DO THIS!
await db.createCollection("comments_main");
await db.collection("comments_main").insertMany([
  { _type: "user_comment", content: "Hello", ... }
]);
```

**Problem**: The system can't track when this instance was created, so:
- ❌ Migrations won't know if they should apply transforms
- ❌ No version tracking
- ❌ Discovery won't find it

### 🔧 Retroactive Fix: Use `markAsMultiCollection()`

If you already have a collection created manually, you can adopt it:

```typescript
import { markAsMultiCollection } from "@diister/mongodbee/migration";

// Mark an existing collection as a multi-collection instance
await markAsMultiCollection(
  db,
  "comments_main",                       // Full collection name
  "comments",                            // Collection type/model name
  "2025_10_02_0201_XXX@initial"         // Migration ID (when it was "created")
);
```

**Use case**: Adopting legacy collections or fixing collections created incorrectly.

⚠️ **Warning**: `markAsMultiCollection()` does NOT validate structure! Make sure your documents already have `_type` fields.

---

## Migration Builder API

The builder provided in `migrate()` offers several operations:

### Regular Collections

```typescript
migrate(migration) {
  return migration
    // Create a collection (automatically applies JSON Schema validator + indexes)
    .createCollection("users")
      .done()

    // Transform existing documents in a collection
    .collection("users")
      .transform({
        up: (doc) => ({
          ...doc,
          bio: "No bio yet"  // Add new field with default
        }),
        down: (doc) => {
          const { bio, ...rest } = doc;
          return rest;
        }
      })
      .done()

    // Update indexes on existing collection
    .updateIndexes("users")

    .compile();
}
```

**Important Notes**:
- `createCollection()` uses the schema from `migration.schemas.collections[name]`
- Validators and indexes are automatically applied after migration operations
- Use `.done()` to return from collection builder to main migration builder

### Multi-Collections

The builder uses a **fluent API** with nested builders:

```typescript
migrate(migration) {
  return migration
    // Create a new multi-collection instance in the migration
    .newMultiCollection("comments_blog", "comments")
      .seedType("user_comment", [
        { content: "First comment", ... }
      ])
      .end()

    // Access existing multi-collection instance
    .multiCollectionInstance("comments_blog")
      .seedType("admin_comment", [
        { content: "Pinned", ... }
      ])
      .end()

    // Transform a type across ALL instances of a model
    .multiCollection("comments")
      .type("user_comment")
        .transform({
          up: (doc) => ({
            ...doc,
            likes: 0
          }),
          down: (doc) => {
            const { likes, ...rest } = doc;
            return rest;
          }
        })
        .end()
      .end()

    .compile();
}
```

**Builder Hierarchy**:
1. `MigrationBuilder` (main)
   - `.newMultiCollection(collectionName, collectionType)` → `MultiCollectionInstanceBuilder`
   - `.multiCollectionInstance(collectionName)` → `MultiCollectionInstanceBuilder`
   - `.multiCollection(collectionType)` → `MultiCollectionBuilder`
   - `.compile()` → finalize

2. `MultiCollectionBuilder`
   - `.type(typeName)` → `MultiCollectionTypeBuilder`
   - `.end()` → back to `MigrationBuilder`

3. `MultiCollectionTypeBuilder`
   - `.transform({ up, down })` → transform documents (with version tracking)
   - `.end()` → back to `MultiCollectionBuilder`

4. `MultiCollectionInstanceBuilder`
   - `.seedType(typeName, docs)` → seed data for a specific type
   - `.end()` → back to `MigrationBuilder`

**Transform Rules**:
- `up`: function to migrate from old to new format
- `down`: function to rollback from new to old format
- Both are required for bidirectional migrations

### Marking Existing Collections as Multi-Collections

In some migration scenarios, you may have an **existing collection** that already follows the multi-collection structure (documents with `_type` fields) but is missing the metadata documents that identify it as a multi-collection instance.

The `.markAsMultiCollection()` method allows you to convert such collections during a migration:

```typescript
migrate(migration) {
  return migration
    // Mark an existing collection as a multi-collection instance
    .markAsMultiCollection("legacy_catalog", "catalog")
    .compile();
}
```

**What it does**:
- **Apply**: Creates the required metadata documents (`_information` and `_migrations`) in the collection
- **Reverse**: Removes the metadata documents, effectively unmarking the collection

**Use Cases**:
1. **Data Migration**: You manually created a collection with multi-collection structure before migrations existed
2. **Legacy Import**: Importing data from another system that uses a similar pattern
3. **Manual Fixes**: Recovering from accidental metadata deletion

**Important Notes**:
- The operation is **idempotent** - running it multiple times won't cause errors
- The collection must exist before marking (will throw an error if it doesn't)
- Only marks the collection; doesn't validate or transform existing documents
- After marking, the collection can be used with `newMultiCollection()` in your application code

**Example Scenario**:

```typescript
// Before migration: Collection "user_events" exists with documents like:
// { _type: "login", userId: "123", timestamp: "..." }
// { _type: "purchase", userId: "456", amount: 100 }
// But missing metadata documents

export async function migrate(migration: MigrationBuilder) {
  return migration
    .markAsMultiCollection("user_events", "events")
    .compile();
}

// After migration: Now you can use it in your app:
import { newMultiCollection } from "@diister/mongodbee";

const events = await newMultiCollection(db, "user_events", eventsModel);
// Works correctly because metadata now exists
```

### Advanced Operations

```typescript
migrate(migration) {
  return migration
    .customOperation({
      apply: async (db) => {
        // Custom code for apply
        await db.collection("users").updateMany(...);
      },
      reverse: async (db) => {
        // Custom code for rollback
        await db.collection("users").updateMany(...);
      }
    })
    .compile();
}
```

## Important Notes on Simulation

The migration system includes a **simulation step** that validates migrations before applying them.

### How Simulation Works

The simulation system validates migrations by running them in an **in-memory environment** that simulates the database state:

1. **Parent migrations are simulated first**: The simulator walks up the migration chain and applies all parent migrations to build the correct initial database state
2. **Current migration is simulated**: Operations from the current migration are applied to this state
3. **Reversibility is checked**: All operations are reversed to verify the migration can be rolled back
4. **State is validated**: The final state after reversal should match the initial state

**Example simulation flow for M3:**
```
Empty State → Apply M1 → Apply M2 → Apply M3 → Reverse M3 → Reverse M2 → Reverse M1 → Compare with Empty State
```

This ensures that:
- ✅ Collections/multi-collections created by parent migrations exist when needed
- ✅ Transforms have the correct data structures to work with
- ✅ Schema inheritance is validated
- ✅ The migration chain is consistent

### Simulation with Mock Data Generation

When transforming a multi-collection type, the simulation validates that your `up` and `down` functions work correctly. If no instances exist yet, the simulation **automatically generates mock test data** using `@diister/valibot-mock`.

**How it works**:

1. The builder passes the Valibot schema to the transform operation
2. If no instances are found during simulation, realistic mock data is generated from the schema
3. The `up` and `down` functions are tested with this mock data
4. The simulation verifies reversibility (that applying then reversing returns to the original state)

**Benefits**:
- ✅ No need to create seed instances just for simulation
- ✅ Transform logic is validated even if instances are created in application code
- ✅ Reversibility is guaranteed before applying to production
- ✅ Mock data generation uses realistic values from Faker.js

**What happens in production**:
- If an instance was created **before** the migration: the transform is applied
- If an instance was created **after** the migration: it's skipped (already has the correct schema)
- If no instances exist yet: a warning is shown, but the migration succeeds

## Generated Migration Structure

```typescript
import { migrationDefinition } from "@diister/mongodbee/migration";
import parent from "./2025_XX_XX_XXXX_PARENT@name.ts";  // If not the first
import * as v from "valibot";

const id = "2025_10_01_0115_H0XWCWC4E6@initial";  // Format: DATE_TIME_ULID@name
const name = "initial";

export default migrationDefinition(id, name, {
  parent: null,  // or `parent` if child migration

  // ⚠️ ALWAYS define the schema here, NEVER import schemas.ts
  schemas: {
    collections: {
      // ...parent?.schemas.collections (if child)
      // Define collections
    },
    multiCollections: {
      // ...parent?.schemas.multiCollections (if child)
      // Define multi-collections
    }
  },

  migrate(migration) {
    // Define migration operations
    return migration
      .createCollection("users")
      .compile();
  }
})
```

## Best Practices

### ✅ DO

1. **Freeze the schema in the migration**
   - Copy the schema from `schemas.ts` into the migration
   - Never import `schemas.ts` in a migration

2. **Use models for DRY**
   - Create models in `models.ts`
   - Expose them in `schemas.ts` via `.expose()`
   - Use them in application code with `newMultiCollection()`

3. **Version instances**
   - Always use `newMultiCollection()` to create instances
   - Never create manually in MongoDB

4. **Control your collection names**
   - Choose meaningful names: `blog_comments`, `forum_posts`, `main_comments`
   - No automatic prefixing - you decide the exact name

5. **Name migrations clearly**
   ```bash
   deno task mongodbee generate --name add_user_email
   deno task mongodbee generate --name transform_artwork_description
   ```

6. **Test migrations**
   - Apply → verify → Rollback → verify

7. **Document complex transformations**
   ```typescript
   migrate(migration) {
     return migration
       // Adds description field by combining title and artist
       // Format: "Title by Artist (Year)"
       .multiCollection("exposition")
         .type("artwork")
         .transform({ ... })
         .end()
       .end()
       .compile();
   }
   ```

### ❌ DON'T

1. **Never import `schemas.ts` in a migration**
   ```typescript
   import { schemas } from "../schemas.ts";  // ❌ NO!
   ```

2. **Never modify an already applied migration**
   - If changes are needed: create a new migration

3. **Never create instances without `newMultiCollection()`**
   ```typescript
   // ❌ NO
   await db.createCollection("comments_blog");

   // ✅ YES
   await newMultiCollection(db, "comments_blog", commentsModel);
   ```

4. **Never skip version tracking**
   - Transform operations automatically handle version tracking
   - Don't try to circumvent this mechanism

5. **Never modify the DB directly in production**
   - Always go through a migration
   - Even for "small" changes

## Debugging

### Check migration status

```bash
deno task mongodbee status
```

Shows:
- Applied migrations
- Pending migrations
- Chain validation

### Check multi-collection metadata

```typescript
import { getMultiCollectionInfo, getMultiCollectionMigrations } from "@diister/mongodbee/migration";

const info = await getMultiCollectionInfo(db, "comments_blog");
console.log(info.collectionType);  // "comments"
console.log(info.createdAt);

const migrations = await getMultiCollectionMigrations(db, "comments_blog");
console.log(migrations.fromMigrationId);  // Creation version
console.log(migrations.appliedMigrations);  // All applied migrations
```

### Discover multi-collection instances

```typescript
import { discoverMultiCollectionInstances } from "@diister/mongodbee/migration";

// Find all instances of a specific collection type
const instances = await discoverMultiCollectionInstances(db, "comments");
// Returns: ["comments_blog", "comments_forum", "comments_main", ...]
```

### Inspect MongoDB directly

```javascript
// View metadata in any multi-collection
db.comments_blog.find({ _type: "_information" })
db.comments_blog.find({ _type: "_migrations" })

// Global migrations tracking
db.__mongodbee_migrations.find()
```

## Migration ID Format

Format: `YYYY_MM_DD_HHMM_ULID@name`

Example: `2025_10_01_0115_H0XWCWC4E6@initial`

- `YYYY_MM_DD_HHMM`: Timestamp for lexicographic ordering
- `ULID`: Unique identifier
- `@name`: Descriptive name

This format allows:
- Automatic chronological sorting
- Version comparison (for version tracking)
- Easy identification

## Common Pitfalls

### 1. Importing `schemas.ts` in Migrations

❌ **Wrong**:
```typescript
import { schemas } from "../schemas.ts";

export default migrationDefinition(id, name, {
  parent: null,
  schemas,  // This will break when schemas.ts changes!
  // ...
})
```

✅ **Correct**: Always copy the schema definition into the migration file.

### 2. Wrong Builder API Usage

❌ **Wrong** (old API):
```typescript
.createMultiCollectionInstance("comments", "blog")  // Old signature
```

✅ **Correct** (new API):
```typescript
.newMultiCollection("comments_blog", "comments")  // collectionName, collectionType
```

### 3. Forgetting to Update `schemas.ts`

After creating a migration with schema changes:
1. ✅ Update the migration file with the new schema
2. ✅ Update `models.ts` with the new fields
3. ✅ Update the model version
4. ✅ Apply the migration

The CLI validates that the **last migration schema** matches `schemas.ts`.

### 4. Missing Transform Down Function

❌ **Wrong**:
```typescript
.transform({
  up: (doc) => ({ ...doc, newField: "value" })
  // Missing down!
})
```

✅ **Correct**:
```typescript
.transform({
  up: (doc) => ({ ...doc, newField: "value" }),
  down: (doc) => {
    const { newField, ...rest } = doc;
    return rest;
  }
})
```

### 5. Creating Instances Without Version Tracking

❌ **Wrong**:
```typescript
await db.createCollection("comments_blog");
```

✅ **Correct**:
```typescript
import { newMultiCollection } from "@diister/mongodbee";

await newMultiCollection(db, "comments_blog", commentsModel);
```

This ensures the instance is tracked with `fromMigrationId` for proper version tracking.

### 6. Understanding Simulation Warnings

You may see warnings like:
```
Warning: No instances found for multi-collection type comments. Transform operation will have no effect.
```

**This is normal** when:
- Instances are created in application code (not migrations)
- You're running migrations on a fresh database
- The migration defines transforms for types that will be instantiated later

**What happens**:
- ✅ The simulation generates mock data to validate your transform logic works
- ✅ The migration applies successfully
- ✅ When instances are created later in your application, they'll have the correct schema
- ✅ If you later add instances created at an earlier migration, transforms will be applied correctly based on version tracking

**No action needed** - this warning is informational only.

## Test Coverage

The migration system is thoroughly tested with **165+ automated tests** covering:

### Core Functionality ✅
- Migration chain validation and discovery
- Builder API (collections, multi-collections, transforms)
- Forward and reverse operations
- Schema inheritance between parent-child migrations
- CLI commands (init, generate, apply, status)

### Multi-Collection Features ✅
- **Real MongoDB transforms** across multiple instances
- **Version tracking** - instances created before/after migrations
- **Rollback operations** - reverse transforms on real databases
- **Metadata management** - `_information` and `_migrations` documents
- **Instance discovery** - finding all instances of a collection type
- **Automatic _type injection** - seeding documents with correct type field

### Edge Cases & Error Handling ✅
- Transform on non-existent types (graceful handling)
- Empty instance collections
- Metadata preservation during transforms
- Type isolation (only specified types transform)
- Multiple documents per instance
- Mixed version scenarios

### Validation & Safety ✅
- Simulation with mock data generation
- Chain integrity validation
- Operation reversibility checks
- Timeout and retry logic
- Error handling with automatic rollback

**Test Files**: See `library/test/migration/` for the complete test suite, including:
- `multicollection-advanced.test.ts` - Comprehensive multi-collection scenarios with real MongoDB
- `mongodb-applier.test.ts` - Database operation tests
- `simulation-applier.test.ts` - In-memory simulation tests
- `runners.test.ts` - Migration execution and rollback
- `validators.test.ts` - Chain and integrity validation

For test coverage details, see `library/test/migration/TEST_COVERAGE_REPORT.md`.

## Application Startup Validation

### Why Validate Migrations at Startup?

In production, it's critical to ensure that:
1. All pending migrations have been applied
2. The database schema matches your application code
3. You catch issues early, before users experience errors

MongoDBee provides validation helpers specifically designed for application startup checks.

### Available Functions

#### `checkMigrationStatus(db, migrationsDir?)`

Returns detailed migration status information:

```typescript
import { checkMigrationStatus } from "@diister/mongodbee/migration";

const db = client.db("myapp");
const status = await checkMigrationStatus(db);

console.log(status);
// {
//   isUpToDate: false,
//   pendingMigrations: ["mig_003", "mig_004"],
//   totalMigrations: 4,
//   appliedCount: 2,
//   lastAppliedMigration: "mig_002",
//   message: "⚠️ Database is outdated. 2 pending migration(s) need to be applied."
// }
```

#### `isLastMigrationApplied(db, migrationsDir?)`

Simple boolean check - is the latest migration applied?

```typescript
import { isLastMigrationApplied } from "@diister/mongodbee/migration";

const upToDate = await isLastMigrationApplied(db);

if (!upToDate) {
  throw new Error("Database schema is outdated");
}
```

#### `assertMigrationsApplied(db, migrationsDir?)`

Throws an error if any migrations are pending. Perfect for production startup:

```typescript
import { assertMigrationsApplied } from "@diister/mongodbee/migration";

// Will throw if migrations are pending
await assertMigrationsApplied(db);

console.log("✓ Database is up-to-date");
```

#### `validateMigrationsForEnv(db, env, migrationsDir?)`

Environment-aware validation - warns in development, throws in production:

```typescript
import { validateMigrationsForEnv } from "@diister/mongodbee/migration";

const env = Deno.env.get("ENV") || "development";

// Warns in development, throws in production
await validateMigrationsForEnv(db, env);
```

### Recommended Startup Pattern

Here's the recommended pattern for application startup:

```typescript
// app.ts or main.ts
import { MongoClient } from "mongodb";
import { validateMigrationsForEnv, checkMigrationStatus } from "@diister/mongodbee/migration";

// Connect to database
const client = new MongoClient(process.env.MONGO_URI);
await client.connect();
const db = client.db("myapp");

// Validate migrations before starting the application
const env = process.env.NODE_ENV || "development";

try {
  // Option 1: Simple environment-aware check (recommended)
  await validateMigrationsForEnv(db, env);
  
  // Option 2: Custom logic based on detailed status
  const status = await checkMigrationStatus(db);
  if (!status.isUpToDate) {
    const message = `Database needs ${status.pendingMigrations.length} migration(s)`;
    
    if (env === "production") {
      // Block production startup
      throw new Error(`${message}. Pending: [${status.pendingMigrations.join(', ')}]`);
    } else {
      // Warn in development but allow startup
      console.warn(`⚠️  ${message}`);
      console.warn(`   Run: deno task migrate:apply`);
    }
  }
  
  console.log("✓ Database schema is up-to-date");
  
} catch (error) {
  console.error("Migration validation failed:", error.message);
  if (env === "production") {
    process.exit(1); // Stop production deployment
  }
}

// Start your application
await startServer();
```

### Integration with CI/CD

Add migration validation to your deployment pipeline:

```bash
# In your CI/CD script
#!/bin/bash

# Apply pending migrations
deno task migrate:apply

# Validate all migrations are applied (will exit with code 1 if not)
deno run --allow-all scripts/validate-migrations.ts

# Deploy application only if validation passes
deploy-application
```

```typescript
// scripts/validate-migrations.ts
import { MongoClient } from "mongodb";
import { assertMigrationsApplied } from "@diister/mongodbee/migration";

const client = new MongoClient(Deno.env.get("MONGO_URI")!);
await client.connect();

try {
  const db = client.db("myapp");
  await assertMigrationsApplied(db);
  console.log("✓ All migrations applied");
} catch (error) {
  console.error("❌ Migration validation failed:", error.message);
  Deno.exit(1);
} finally {
  await client.close();
}
```

### Best Practices

1. **Always validate in production** - Use `assertMigrationsApplied()` or `validateMigrationsForEnv()` in production environments

2. **Fail fast** - Don't let the application start if migrations are missing. It will only lead to runtime errors.

3. **Warn in development** - Use `validateMigrationsForEnv()` to warn developers without blocking local development

4. **Check before each deployment** - Add migration validation to your CI/CD pipeline

5. **Log migration status** - Use `checkMigrationStatus()` to log detailed information about migration state

6. **Separate migration from app code** - Run migrations in a separate step before deploying your application

## Summary

- **Migrations** = autonomous, frozen files that define their own schema
- **schemas.ts** = final state after all migrations, never imported in migrations
- **Models** = reusable templates to avoid duplication
- **Multi-collections** = multiple document types in one MongoDB collection
- **Metadata** = stored as `_information` and `_migrations` documents with fixed `_id`
- **Version tracking** = each instance knows at which migration it was created (`fromMigrationId`)
- **Collection names** = you have full control, no automatic prefixing
- **Builder API** = fluent, nested builders with `.end()` to navigate back
- **Transform** = requires both `up` and `down` functions for bidirectionality
- **Simulation** = validates migrations using mock data when instances don't exist
- **Mock data** = generated automatically from Valibot schemas
- **Validators & Indexes** = automatically applied and synchronized after migrations
- **CLI** = `generate` → edit → `apply` → `rollback` if needed

## Recent Improvements

### ✅ Automatic Schema Synchronization

After applying migration operations, the system automatically:
- Updates JSON Schema validators on all collections
- Creates/updates indexes defined with `withIndex()`
- Applies validators and indexes to all multi-collection instances

This means you don't need to manually call `updateIndexes()` unless you want to update indexes without other changes.

### ✅ Simplified Metadata Structure

Multi-collection metadata documents now have:
- Fixed `_id` values (`"_information"` and `"_migrations"`)
- Simplified fields:
  - `collectionType` (instead of `multiCollectionType`)
  - No `instanceName` (collection name is the full name)
  - `fromMigrationId` in `_migrations` (instead of `createdByMigration` in `_information`)
  - No `schemas` field (use MongoDB validators instead)

### ✅ No Automatic Prefixing

You have complete control over collection names:
```typescript
// All of these work:
await newMultiCollection(db, "comments_blog", commentsModel);
await newMultiCollection(db, "my_comments", commentsModel);
await newMultiCollection(db, "HELLO", commentsModel);
```

### ✅ Cleaner API

- `createMultiCollectionInstance()` → `newMultiCollection()` (shorter!)
- Functions take `collectionName` directly instead of separate `multiCollectionName` + `instanceName`
- Consistent naming throughout the codebase

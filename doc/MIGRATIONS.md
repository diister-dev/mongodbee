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
  exposition: {           // Multi-collection name
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
- `exposition` is the **template** (defined in schema)
- `exposition_louvre`, `exposition_pompidou` are **instances** (physical MongoDB collections)

Creating an instance:
```typescript
import { newMultiCollection } from "@diister/mongodbee";

// Creates the MongoDB collection "exposition_louvre" and records the version
const louvre = await newMultiCollection(db, "exposition_louvre", expositionModel);
```

### 5. Multi-Collection Version Tracking

Each multi-collection instance is created at a **specific migration version**. This information is stored in `__mongodbee_multicollections`:

```javascript
{
  multiCollectionName: "exposition",
  instanceName: "louvre",
  createdByMigration: "2025_10_01_1000_H0XWCWC4E6@initial"
}
```

**Importance**: During transformations via `transformMultiCollectionType`, the system:
1. Compares the instance's creation version with the current migration version
2. **Skips instances created AFTER** the migration
3. Applies only to instances created BEFORE

**Example**:
```
Timeline:
- M1: Creates "exposition" template
- Louvre created (version = M1)
- M2: Adds "description" field via transform
- Pompidou created (version = M2)

During M2.rollback():
- Louvre: receives the rollback (created BEFORE M2)
- Pompidou: SKIPPED (created AFTER M2, already has correct schema)
```

## CLI Commands

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
deno task mongodbee revert

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
import { userModel } from "./models.ts";

const usersMain = await newMultiCollection(db, "user_main", userModel);
```

**Why?** This function:
1. ✅ Automatically detects the last applied migration
2. ✅ Marks the instance with `createdByMigration: "M2_id"` (for version tracking)
3. ✅ Inserts metadata documents (`_type: "__mongodbee_info"` and `_type: "__mongodbee_migrations"`)
4. ✅ Returns a ready-to-use multi-collection instance

### ❌ Wrong: Manual Creation

```typescript
// ❌ DON'T DO THIS!
await db.createCollection("user_main");
await db.collection("user_main").insertMany([
  { _type: "admin", username: "alice", ... }
]);
```

**Problem**: The system can't track when this instance was created, so:
- ❌ Migrations won't know if they should apply transforms
- ❌ No version tracking
- ❌ `discoverMultiCollectionInstances()` won't find it

### 🔧 Retroactive Fix: Use `markAsMultiCollection()`

If you already have a collection created manually, you can adopt it:

```typescript
import { markAsMultiCollection } from "@diister/mongodbee/migration";

// Mark an existing collection as a multi-collection instance
await markAsMultiCollection(
  db,
  "user_main",                           // Full collection name
  "user",                                // Multi-collection template name
  "main",                                // Instance name
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
    // Create a collection (automatically applies JSON Schema validator from schemas)
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

    .compile();
}
```

**Important Notes**:
- `createCollection()` uses the schema from `migration.schemas.collections[name]` to create a MongoDB JSON Schema validator
- This validator is automatically applied to the MongoDB collection
- All insert/update operations will be validated against the schema
- Use `.done()` to return from collection builder to main migration builder

### Multi-Collections

The builder uses a **fluent API** with nested builders:

```typescript
migrate(migration) {
  return migration
    // Access the multi-collection builder
    .multiCollection("exposition")

      // Access a specific type builder
      .type("artwork")

        // Apply transformation with up/down functions
        .transform({
          up: (doc) => ({
            ...doc,
            description: `${doc.title} by ${doc.artist}`
          }),
          down: (doc) => {
            const { description, ...rest } = doc;
            return rest;
          }
        })

        // Return to multi-collection builder
        .end()

      // Return to main migration builder
      .end()

    .compile();
}
```

**Builder Hierarchy**:
1. `MigrationBuilder` (main)
   - `.multiCollection(name)` → `MultiCollectionBuilder`
   - `.multiCollectionInstance(name, instance)` → `MultiCollectionInstanceBuilder`
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

### Creating Multi-Collections in Migrations

**⚠️ IMPORTANT**: Multi-collections **do NOT need to be explicitly created** in migrations!

```typescript
migrate(migration) {
  // ❌ NO NEED TO DO THIS:
  // migration.createMultiCollectionInstance(...)

  // ✅ Just declare the schema:
  schemas: {
    multiCollections: {
      comments: {
        user_comment: { ... },
        admin_comment: { ... }
      }
    }
  }

  // ✅ The multi-collection collection will be created automatically when:
  // 1. First document is inserted
  // 2. OR you can explicitly create the MongoDB collection:
  migrate(migration) {
    return migration
      .createCollection("comments")  // Creates the physical MongoDB collection
      .done()
      .compile();
  }
}
```

**Why?**: Multi-collection metadata (instances) are registered when you insert documents with `_type` field, not in migrations. The migration only declares the schema structure.

**For seeding data in migrations** (rare use case):
```typescript
migrate(migration) {
  return migration
    .multiCollectionInstance("comments", "user_comment")
      .seedType("user_comment", [
        { _id: "c1", content: "Hello", ... }
      ])
      .end()
    .compile();
}
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

**Example**:
```typescript
// Migration M2: Add description field
migrate(migration) {
  return migration
    .multiCollection("exposition")
      .type("artwork")
      .transform({
        up: (doc) => ({
          ...doc,
          description: `${doc.title} by ${doc.artist} (${doc.year})`
        }),
        down: (doc) => {
          const { description, ...rest } = doc;
          return rest;
        }
      })
      .end()
    .end()
    .compile();
}
```

Even if no `exposition_*` instances exist, the simulation will:
- Generate a mock artwork document with realistic values for `title`, `artist`, `year`
- Apply the `up` transform to add `description`
- Apply the `down` transform to remove `description`
- Verify the document returns to its original state

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

4. **Name migrations clearly**
   ```bash
   deno task mongodbee generate --name add_user_email
   deno task mongodbee generate --name transform_artwork_description
   ```

5. **Test migrations**
   - Apply → verify → Rollback → verify

6. **Document complex transformations**
   ```typescript
   migrate(migration) {
     return migration
       // Adds description field by combining title and artist
       // Format: "Title by Artist (Year)"
       .transformMultiCollectionType({ ... })
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
   await db.createCollection("exposition_louvre");

   // ✅ YES
   await newMultiCollection(db, "exposition_louvre", expositionModel);
   ```

4. **Never skip version tracking**
   - `transformMultiCollectionType` automatically handles version tracking
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
import { getMultiCollectionInfo } from "@diister/mongodbee/migration";

const info = await getMultiCollectionInfo(db, "exposition", "louvre");
console.log(info.createdByMigration);  // Creation version
```

### Inspect MongoDB directly

```javascript
// Migration metadata collection
db.__mongodbee_migrations.find()

// Multi-collection metadata collection
db.__mongodbee_multicollections.find()
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

## Real-World Example: A Typical Development Day

This section demonstrates a complete, realistic workflow from project initialization to evolving schemas over multiple migrations.

### Day 1: Project Setup & Initial Migration

**Goal**: Create a basic blog with users and posts

#### Step 1: Initialize project

```bash
mkdir myblog && cd myblog

# Create configuration
cat > .env << 'EOF'
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=myblog_db
EOF

cat > deno.json << 'EOF'
{
  "tasks": {
    "mongodbee": "deno run --allow-read --allow-write --allow-net --allow-env --allow-sys --env-file=.env node_modules/@diister/mongodbee/src/migration/cli/bin.ts"
  },
  "imports": {
    "@diister/mongodbee": "npm:@diister/mongodbee",
    "valibot": "npm:valibot@^0.42.1",
    "mongodb": "npm:mongodb@^6.11.0"
  }
}
EOF

mkdir migrations
```

#### Step 2: Define initial schema

```typescript
// schemas.ts
import * as v from "valibot";

export const usersSchema = {
  _id: v.string(),
  username: v.pipe(v.string(), v.minLength(3)),
  email: v.pipe(v.string(), v.email()),
  createdAt: v.date(),
};

export const postsSchema = {
  _id: v.string(),
  title: v.pipe(v.string(), v.minLength(1)),
  content: v.string(),
  authorId: v.string(),
  createdAt: v.date(),
};

export const schemas = {
  collections: {
    users: usersSchema,
    posts: postsSchema,
  },
  multiCollections: {}
};
```

#### Step 3: Create and edit migration

```bash
deno task mongodbee generate --name "initial_setup"
```

```typescript
// migrations/2025_10_02_1000_XXXXX@initial_setup.ts
import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";

const id = "2025_10_02_1000_XXXXX@initial_setup";
const name = "initial_setup";

export default migrationDefinition(id, name, {
  parent: null,
  schemas: {
    collections: {
      users: {
        _id: v.string(),
        username: v.pipe(v.string(), v.minLength(3)),
        email: v.pipe(v.string(), v.email()),
        createdAt: v.date(),
      },
      posts: {
        _id: v.string(),
        title: v.pipe(v.string(), v.minLength(1)),
        content: v.string(),
        authorId: v.string(),
        createdAt: v.date(),
      },
    },
    multiCollections: {}
  },
  migrate(migration) {
    return migration
      .createCollection("users")
        .done()
      .createCollection("posts")
        .done()
      .compile();
  },
})
```

#### Step 4: Apply migration

```bash
deno task mongodbee apply
# ✅ Collections created with JSON Schema validators
```

#### Step 5: Insert initial data

```typescript
// insert_day1_data.ts
import { MongoClient } from "mongodb";

const client = new MongoClient("mongodb://localhost:27017");
await client.connect();
const db = client.db("myblog_db");

await db.collection('users').insertMany([
  { _id: 'user1', username: 'alice', email: 'alice@example.com', createdAt: new Date() },
  { _id: 'user2', username: 'bob', email: 'bob@example.com', createdAt: new Date() },
]);

await db.collection('posts').insertMany([
  { _id: 'post1', title: 'First Post', content: 'Hello world!', authorId: 'user1', createdAt: new Date() },
  { _id: 'post2', title: 'Second Post', content: 'Nice blog', authorId: 'user2', createdAt: new Date() },
]);

await client.close();
```

### Day 2: Add New Fields with Transforms

**Goal**: Add `bio` to users and `publishedAt` to posts, with transforms for existing data

#### Step 1: Update schemas.ts

```typescript
// schemas.ts (updated)
export const usersSchema = {
  _id: v.string(),
  username: v.pipe(v.string(), v.minLength(3)),
  email: v.pipe(v.string(), v.email()),
  bio: v.optional(v.string()), // 🆕 NEW
  createdAt: v.date(),
};

export const postsSchema = {
  _id: v.string(),
  title: v.pipe(v.string(), v.minLength(1)),
  content: v.string(),
  authorId: v.string(),
  publishedAt: v.optional(v.date()), // 🆕 NEW
  createdAt: v.date(),
};
```

#### Step 2: Generate and edit migration

```bash
deno task mongodbee generate --name "add_bio_and_published"
```

```typescript
// migrations/2025_10_02_1100_YYYYY@add_bio_and_published.ts
import { migrationDefinition } from "@diister/mongodbee/migration";
import parent from "./2025_10_02_1000_XXXXX@initial_setup.ts";
import * as v from "valibot";

const id = "2025_10_02_1100_YYYYY@add_bio_and_published";
const name = "add_bio_and_published";

export default migrationDefinition(id, name, {
  parent: parent,
  schemas: {
    collections: {
      users: {
        _id: v.string(),
        username: v.pipe(v.string(), v.minLength(3)),
        email: v.pipe(v.string(), v.email()),
        bio: v.optional(v.string()), // 🆕
        createdAt: v.date(),
      },
      posts: {
        _id: v.string(),
        title: v.pipe(v.string(), v.minLength(1)),
        content: v.string(),
        authorId: v.string(),
        publishedAt: v.optional(v.date()), // 🆕
        createdAt: v.date(),
      },
    },
    multiCollections: {}
  },
  migrate(migration) {
    return migration
      // Transform existing users
      .collection("users")
        .transform({
          up: (doc) => ({
            ...doc,
            bio: "No bio yet"  // Default for existing users
          }),
          down: (doc) => {
            const { bio, ...rest } = doc;
            return rest;
          }
        })
        .done()

      // Transform existing posts
      .collection("posts")
        .transform({
          up: (doc) => ({
            ...doc,
            publishedAt: doc.createdAt  // Use creation date as published date
          }),
          down: (doc) => {
            const { publishedAt, ...rest } = doc;
            return rest;
          }
        })
        .done()

      .compile();
  },
})
```

#### Step 3: Apply migration

```bash
deno task mongodbee apply
# ✅ Transforms applied to existing data
```

### Day 3: Add Multi-Collection Feature

**Goal**: Add a comments multi-collection with user_comment and admin_comment types

#### Step 1: Update schemas.ts

```typescript
// schemas.ts (updated)
export const commentsSchema = {
  user_comment: {
    _id: v.string(),
    postId: v.string(),
    authorId: v.string(),
    content: v.string(),
    createdAt: v.date(),
  },
  admin_comment: {
    _id: v.string(),
    postId: v.string(),
    adminName: v.string(),
    content: v.string(),
    priority: v.picklist(["low", "medium", "high"]),
    createdAt: v.date(),
  },
};

export const schemas = {
  collections: {
    users: usersSchema,
    posts: postsSchema,
  },
  multiCollections: {
    comments: commentsSchema,  // 🆕 NEW
  }
};
```

#### Step 2: Generate and edit migration

```bash
deno task mongodbee generate --name "add_comments"
```

```typescript
// migrations/2025_10_02_1200_ZZZZZ@add_comments.ts
import { migrationDefinition } from "@diister/mongodbee/migration";
import parent from "./2025_10_02_1100_YYYYY@add_bio_and_published.ts";
import * as v from "valibot";

const id = "2025_10_02_1200_ZZZZZ@add_comments";
const name = "add_comments";

export default migrationDefinition(id, name, {
  parent: parent,
  schemas: {
    collections: {
      ...parent.schemas.collections,
    },
    multiCollections: {
      comments: {
        user_comment: {
          _id: v.string(),
          postId: v.string(),
          authorId: v.string(),
          content: v.string(),
          createdAt: v.date(),
        },
        admin_comment: {
          _id: v.string(),
          postId: v.string(),
          adminName: v.string(),
          content: v.string(),
          priority: v.picklist(["low", "medium", "high"]),
          createdAt: v.date(),
        },
      },
    }
  },
  migrate(migration) {
    // Multi-collection declared - no operations needed
    // Can optionally create the collection explicitly:
    return migration
      .createCollection("comments")
        .done()
      .compile();
  },
})
```

#### Step 3: Apply migration and insert data

```bash
deno task mongodbee apply
```

```typescript
// insert_comments.ts
const db = client.db("myblog_db");

await db.collection('comments').insertMany([
  {
    _type: "user_comment",
    _id: "c1",
    postId: "post1",
    authorId: "user2",
    content: "Great post!",
    createdAt: new Date(),
  },
  {
    _type: "admin_comment",
    _id: "a1",
    postId: "post1",
    adminName: "Admin Alice",
    content: "Featured",
    priority: "high",
    createdAt: new Date(),
  },
]);
```

### Day 4: Evolve Multi-Collection Schema

**Goal**: Add `likes` to user_comment and `resolved` to admin_comment

#### Step 1: Update schemas.ts and generate migration

```bash
deno task mongodbee generate --name "add_comment_fields"
```

```typescript
// Migration with multi-collection transforms
migrate(migration) {
  return migration
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
      .type("admin_comment")
        .transform({
          up: (doc) => ({
            ...doc,
            resolved: false
          }),
          down: (doc) => {
            const { resolved, ...rest } = doc;
            return rest;
          }
        })
        .end()
      .end()
    .compile();
}
```

### Summary

After 4 days of development:
- ✅ Project initialized with migrations
- ✅ 2 collections (users, posts) with JSON Schema validation
- ✅ 1 multi-collection (comments) with 2 types
- ✅ 4 migrations applied (M1-M4)
- ✅ All data transformed correctly
- ✅ Full type safety with Valibot
- ✅ Reversible migrations with `.revert` command

## Complete Example: Workflow

### 1. Initial Setup

```bash
mkdir myproject && cd myproject
echo 'MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=myapp' > .env

# Create deno.json with config
deno task mongodbee init
```

### 2. Create the Model

```typescript
// models.ts
import * as v from "valibot";
import { createMultiCollectionModel } from "@diister/mongodbee";

export const catalogModel = createMultiCollectionModel("catalog", {
  schema: {
    product: {
      name: v.string(),
      price: v.number()
    }
  },
  version: "1.0.0"
});
```

### 3. Define schemas.ts

```typescript
// schemas.ts
import { catalogModel } from "./models.ts";

export const schemas = {
  collections: {},
  multiCollections: {
    ...catalogModel.expose()
  }
};
```

### 4. Generate and Edit the Migration

```bash
deno task mongodbee generate --name initial
```

```typescript
// migrations/2025_10_01_0115_H0XWCWC4E6@initial.ts
import { migrationDefinition } from "@diister/mongodbee/migration";
import * as v from "valibot";

const id = "2025_10_01_0115_H0XWCWC4E6@initial";
const name = "initial";

export default migrationDefinition(id, name, {
  parent: null,
  schemas: {
    collections: {},
    multiCollections: {
      catalog: {
        product: {
          name: v.string(),
          price: v.number()
        }
      }
    }
  },
  migrate(migration) {
    return migration.compile();
  }
})
```

### 5. Apply

```bash
deno task mongodbee apply
```

### 6. Use in Application

```typescript
// app.ts
import { MongoClient } from "mongodb";
import { newMultiCollection } from "@diister/mongodbee";
import { catalogModel } from "./models.ts";

const client = new MongoClient("mongodb://localhost:27017");
const db = client.db("myapp");

// Create an instance (with version tracking)
const electronics = await newMultiCollection(db, "catalog_electronics", catalogModel);

// Use it
await electronics.insertOne("product", {
  name: "Laptop",
  price: 999
});
```

### 7. Add a Field (New Migration)

```bash
deno task mongodbee generate --name add_product_stock
```

```typescript
// migrations/2025_10_01_0200_YYYYYYY@add_product_stock.ts
import { migrationDefinition } from "@diister/mongodbee/migration";
import parent from "./2025_10_01_0115_H0XWCWC4E6@initial.ts";
import * as v from "valibot";

const id = "2025_10_01_0200_YYYYYYY@add_product_stock";
const name = "add_product_stock";

export default migrationDefinition(id, name, {
  parent: parent,
  schemas: {
    collections: {
      ...parent.schemas.collections
    },
    multiCollections: {
      ...parent.schemas.multiCollections,
      catalog: {
        product: {
          name: v.string(),
          price: v.number(),
          stock: v.number()  // ✨ New field
        }
      }
    }
  },
  migrate(migration) {
    return migration
      .multiCollection("catalog")
        .type("product")
        .transform({
          up: (doc) => ({
            ...doc,
            stock: 0  // Initialize to 0
          }),
          down: (doc) => {
            const { stock, ...rest } = doc;
            return rest;
          }
        })
        .end()
      .end()
      .compile();
  }
})
```

### 8. Update schemas.ts

```typescript
// models.ts
export const catalogModel = createMultiCollectionModel("catalog", {
  schema: {
    product: {
      name: v.string(),
      price: v.number(),
      stock: v.number()  // ✨ Add here too
    }
  },
  version: "1.1.0"  // Increment version
});
```

### 9. Apply the New Migration

```bash
deno task mongodbee apply
```

Existing instances (`catalog_electronics` created in M1) will receive the `stock` field.

New instances created after M2 will already have the field in their schema.

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

❌ **Wrong** (old API that doesn't exist):
```typescript
.transformMultiCollectionType({
  multiCollectionName: "exposition",
  typeName: "artwork",
  transform: (doc) => ({ ...doc })
})
```

✅ **Correct** (fluent API):
```typescript
.multiCollection("exposition")
  .type("artwork")
  .transform({
    up: (doc) => ({ ...doc }),
    down: (doc) => doc
  })
  .end()
.end()
```

### 3. Forgetting to Update `schemas.ts`

After creating a migration with schema changes:
1. ✅ Update the migration file with the new schema
2. ✅ Update `models.ts` with the new fields
3. ❌ Forgetting to update the model version

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
await db.createCollection("exposition_louvre");
```

✅ **Correct**:
```typescript
import { newMultiCollection } from "@diister/mongodbee";

await newMultiCollection(db, "exposition_louvre", expositionModel);
```

This ensures the instance is tracked with `createdByMigration` for proper version tracking.

### 6. Understanding Simulation Warnings

You may see warnings like:
```
Warning: No instances found for multi-collection exposition. Transform operation will have no effect.
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

## Summary

- **Migrations** = autonomous, frozen files that define their own schema
- **schemas.ts** = final state after all migrations, never imported in migrations
- **Models** = reusable templates to avoid duplication
- **Multi-collections** = multiple document types in one MongoDB collection
- **Version tracking** = each instance knows at which migration it was created
- **Builder API** = fluent, nested builders with `.end()` to navigate back
- **Transform** = requires both `up` and `down` functions for bidirectionality
- **Simulation** = validates migrations using mock data when instances don't exist
- **Mock data** = generated automatically from Valibot schemas using `@diister/valibot-mock`
- **CLI** = `generate` → edit → `apply` → `rollback` if needed

## Technical Details: Mock Data Generation

The simulation system uses `@diister/valibot-mock` to generate realistic test data for validation:

**Implementation**:
1. The migration builder extracts the schema for each multi-collection type from the migration's `schemas` option
2. This schema is passed to the `TransformMultiCollectionTypeRule`
3. During simulation, if no instances exist, `createMockGenerator(schema).generate()` creates a test document
4. The `up` transform is applied to this mock document
5. The `down` transform is applied to reverse it
6. The simulation verifies the reversed document matches the original (reversibility check)

**Benefits of this approach**:
- Uses Faker.js under the hood for realistic data (names, dates, numbers, etc.)
- Validates transform logic even when database is empty
- Ensures migrations are reversible before applying to production
- No need to maintain seed data just for testing

**Files involved**:
- [library/src/migration/types.ts](library/src/migration/types.ts) - Schema field in transform rules
- [library/src/migration/builder.ts](library/src/migration/builder.ts) - Schema extraction and passing
- [library/src/migration/appliers/simulation.ts](library/src/migration/appliers/simulation.ts) - Mock generation and validation

## Known Issues & Roadmap

### ✅ Fixed Issues

1. **✅ JSON Schema validators created automatically**: The migration system now automatically creates MongoDB JSON Schema validators when creating collections. Validators are generated from the Valibot schema defined in `migration.schemas.collections[name]` using the `toMongoValidator()` function.

2. **✅ Simulation with parent migrations**: The simulation system correctly applies all parent migrations before validating the current migration. This ensures transforms have the correct initial state.

3. **✅ Reversibility validation**: The simulation validates that migrations can be reversed by comparing the final state to the initial state (with parent migrations applied), not an empty state.

### Issues to Fix

1. **⚠️ Transform application timing**: Multi-collection transforms show warnings when no instances exist: "No instances found for multi-collection X. Transform operation will have no effect."
   - **Current behavior**: Transforms only apply to instances that have metadata in the multi-collection registry
   - **This is by design**: Instances are registered when documents are inserted with `_type` field
   - **Workaround**: This is normal and expected. The warning is informational only.

2. **❌ Collection transforms not yet implemented**: While multi-collection transforms work via `.multiCollection().type().transform()`, regular collection transforms via `.collection().transform()` may not be fully implemented in the MongoDB applier.

### Recommendations for Implementation

#### Fix Transform Application (Priority: CRITICAL)

The MongoDB applier needs to:

1. **For collection transforms** (`transform_collection`):
   ```typescript
   // In mongodb applier
   const collection = db.collection(operation.collectionName);
   const docs = await collection.find({}).toArray();
   
   for (const doc of docs) {
     const transformed = operation.up(doc);
     await collection.updateOne(
       { _id: doc._id },
       { $set: transformed }
     );
   }
   ```

2. **For multi-collection type transforms** (`transform_multicollection_type`):
   ```typescript
   // Find all instances of this multi-collection
   const instances = await db.collection('__mongodbee_multicollections')
     .find({ multiCollectionName: operation.multiCollectionName })
     .toArray();

   for (const instance of instances) {
     // Check version: only transform if instance was created BEFORE this migration
     if (shouldApplyTransform(instance.createdByMigration, currentMigrationId)) {
       const collectionName = `${instance.multiCollectionName}_${instance.instanceName}`;
       const collection = db.collection(collectionName);
       
       // Transform all documents of the specified type
       const docs = await collection.find({ _type: operation.typeName }).toArray();
       
       for (const doc of docs) {
         const transformed = operation.up(doc);
         await collection.updateOne(
           { _id: doc._id },
           { $set: transformed }
         );
       }
     }
   }
   ```

3. **Version comparison logic**:
   ```typescript
   function shouldApplyTransform(instanceVersion: string, migrationVersion: string): boolean {
     // Parse migration IDs (format: YYYY_MM_DD_HHMM_ULID@name)
     // Compare timestamps/ULIDs
     // Return true if instance was created BEFORE the migration
     return instanceVersion < migrationVersion;
   }
   ```

#### Fix Migration Counter

The bug is likely in the CLI status command where it calculates:
```typescript
const applied = appliedMigrations.length;  // This seems to be counting duplicates or wrong
const pending = allMigrations.length - applied;
```

Check the discovery/status code for duplicate counting or incorrect queries to `__mongodbee_migrations` collection.

### Testing Checklist

#### ✅ Verified Working

- [x] M1 creates collections with JSON Schema validators
- [x] JSON Schema validators reject invalid data (wrong types, missing fields, invalid formats)
- [x] M2 simulation correctly applies parent migrations first
- [x] Reversibility validation uses correct initial state (not empty state)
- [x] Multi-collection schemas can be declared without explicit creation
- [x] Collection transforms can be defined with `.collection().transform()`
- [x] Multi-collection type transforms can be defined with `.multiCollection().type().transform()`
- [x] Migration names can be specified with `--name` flag
- [x] Schema validation passes when last migration matches schemas.ts

#### ⚠️ Needs Verification

- [ ] Collection transforms are actually applied to existing documents (MongoDB applier)
- [ ] Multi-collection transforms respect instance metadata (`createdByMigration`)
- [ ] `revert` command correctly reverses transforms on data
- [ ] Multi-collection instances created at M1 receive transforms from M2
- [ ] Multi-collection instances created at M2 don't receive M2 transforms

### Documentation Improvements Needed

- Add examples of complete migration workflows with actual data
- Document the MongoDB applier architecture
- Explain version tracking in detail with examples
- Add troubleshooting guide for common errors

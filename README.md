<div align="center">
  <img src="./brand/logo.svg" alt="MongoDBee Logo" width="200" />

# MongoDBee 🍃🐝

<p align="center">
    <strong>A type-safe MongoDB wrapper with built-in validation powered by Valibot</strong>
  </p>
  <p align="center">
    <a href="#-vision--philosophy">Vision</a> •
    <a href="#-key-features">Features</a> •
    <a href="#-installation">Installation</a> •
    <a href="#-usage">Usage</a> •
    <a href="#-scoped-multi-collections">Scoped</a> •
    <a href="#-migration-system">Migrations</a> •
    <a href="#-transactions">Transactions</a> •
    <a href="#-opentelemetry-tracing">Telemetry</a> •
    <a href="#-indexes-with-withindex">Indexing</a> •
    <a href="#-project-status">Status</a>
  </p>

<p align="center">
    <a href="https://www.npmjs.com/package/mongodbee">
      <img src="https://img.shields.io/npm/v/mongodbee" alt="npm version">
    </a>
    <a href="https://jsr.io/@diister/mongodbee">
      <img src="https://jsr.io/badges/@diister/mongodbee" alt="JSR Score">
    </a>
    <a href="https://github.com/diister-dev/mongodbee/blob/main/LICENSE">
      <img src="https://img.shields.io/github/license/diister-dev/mongodbee" alt="License">
    </a>
  </p>
</div>

> ✅ **Stable Core Features**: The MultiCollection API, Multi-Model System, and
> Migration System are production-ready. Other features are in active
> development. See the [Project Status](#-project-status) section for details.

## 💡 Vision & Philosophy

MongoDBee is built around a simple but powerful philosophy: **schemas are your
source of truth, and migrations are how you evolve them over time**.

### The MongoDBee Mindset

**Schema-First Design**: Define your data structure with Valibot schemas before
writing any database code. Your TypeScript types are automatically inferred from
these schemas, ensuring perfect alignment between runtime validation and
compile-time type checking.

**Multi-Collection Pattern**: Instead of creating dozens of MongoDB collections,
group related document types into logical multi-collections. This reduces
collection sprawl, simplifies queries, and maintains type safety across
different document types in the same collection.

**Multi-Model Pattern**: Need multiple collections with the same structure?
Define a model once and create as many instances as you need. Perfect for user
workspaces, tenant data, or any per-entity collections.

**Migration-Driven Evolution**: Never modify your database structure manually.
Use migrations to track every schema change, making your database evolution
reproducible, reversible, and auditable. Rollback any migration with confidence.

**Type Safety Everywhere**: From queries to updates, from aggregations to
transactions - every operation is type-checked. If it compiles, it's type-safe.
No more runtime surprises from typos or wrong field types.

**Protection by Default**: MongoDBee protects you from common mistakes.
Accidental `deleteMany()` without filters? Blocked. Invalid data that doesn't
match your schema? Caught before it reaches the database. The library is
designed to prevent data loss and maintain integrity.

### When to Use MongoDBee

✅ **Perfect For**:

- Applications that need strong type safety with MongoDB
- Projects that value schema validation and data integrity
- Teams that want reproducible database changes through migrations
- Systems that benefit from grouping related document types
- Codebases that prefer declarative schemas over imperative code

❌ **Not Ideal For**:

- Completely schema-less, dynamic data structures
- Projects that never need to track database evolution
- Simple scripts or prototypes where type safety isn't critical
- Applications that require maximum MongoDB driver flexibility

## 🌟 Key Features

- **🔒 Type-Safe**: Strong TypeScript support with automatic type inference
- **✅ Validation**: Runtime schema validation with Valibot - invalid data is
  caught before insertion
- **🏗️ Multi-Collection**: Store different document types in a single MongoDB
  collection with type-safety
- **📦 Multi-Model**: Create multiple collection instances from a single schema
  definition
- **🔄 Migration System**: Built-in migration framework with CLI for schema
  evolution and rollbacks
- **⚡ Transactions**: Built-in transaction support with AsyncLocalStorage
- **🛡️ Data Protection**: Built-in safeguards against accidental data deletion
- **📡 Change Streams**: Type-safe event listeners for collection changes
- **🔗 Dot Notation**: Support for nested object operations (MultiCollection
  API)
- **📊 Indexing**: Declarative index creation with `withIndex()` for performance
  optimization

## 📦 Installation

MongoDBee runs on **Bun, Node.js (22.18+) and Deno**. It is published to npm as
`mongodbee` and to JSR as `@diister/mongodbee`; the two carry the same code.

### Bun / Node.js / npm

```bash
bun add mongodbee mongodb     # or: npm install / pnpm add / yarn add
```

```ts
import { collection, multiCollection, withIndex } from "mongodbee";
import * as v from "mongodbee/schema";

// For migrations
import { migrationDefinition } from "mongodbee/migration";
```

The migration CLI ships as a binary, so no path juggling:

```bash
bunx mongodbee migrate        # or: npx mongodbee migrate
```

### Deno

```bash
deno add jsr:@diister/mongodbee
```

```ts
import { collection, multiCollection, withIndex } from "mongodbee";
import * as v from "mongodbee/schema";

// For migrations
import { migrationDefinition } from "mongodbee/migration";
```

> **Node.js 22.18 or newer.** The migration CLI loads your migration files
> directly as TypeScript, which relies on the runtime stripping types — on by
> default since 22.18, and stable (no longer experimental) since 24.12. The
> library also uses `Set.prototype.difference`, which landed in Node 22.
>
> Type stripping *erases* types, it does not compile them, so your **migration
> and schema files** must avoid the TypeScript constructs that need real code
> generation: `enum`, `namespace` containing runtime code, parameter
> properties, decorators, and import aliases. Write `import type { … }` for
> type-only imports. None of this applies under Bun or Deno, which transpile
> fully, nor to the library itself, which ships compiled JavaScript to npm.

## 🚀 Usage

### Basic Collection

```typescript
import { collection, MongoClient } from "mongodbee";
import * as v from "mongodbee/schema";

// Connect to MongoDB
const client = new MongoClient("mongodb://localhost:27017");
await client.connect();
const db = client.db("myapp");

// Define a schema with validation
const users = await collection(db, "users", {
  username: v.pipe(v.string(), v.minLength(3)),
  email: withIndex(v.pipe(v.string(), v.email()), { unique: true }),
  age: v.pipe(v.number(), v.minValue(0)),
  createdAt: v.date(),
});

// Insert with automatic validation
const userId = await users.insertOne({
  username: "janedoe",
  email: "jane@example.com",
  age: 28,
  createdAt: new Date(),
});

// Query with type safety
const user = await users.findOne({ _id: userId });
console.log(user.email); // TypeScript knows this is a string
```

### Multi-Collection API

The MultiCollection API allows you to store different document types in a single
MongoDB collection while maintaining strong type safety:

```typescript
import { multiCollection } from "mongodbee";
import * as v from "mongodbee/schema";

// Define a catalog schema with multiple document types in a single collection
const catalog = await multiCollection(db, "catalog", {
  product: { // First document type
    name: withIndex(v.string(), { unique: true }),
    price: v.number(),
    stock: v.number(),
    category: v.string(),
  },
  category: { // Second document type in the same collection
    name: withIndex(v.string(), { unique: true }),
    parentId: v.optional(v.string()),
  },
});

// Insert into the collection with type "category"
const electronicsId = await catalog.insertOne("category", {
  name: "Electronics",
});

await catalog.insertOne("product", {
  name: "Smartphone",
  price: 499.99,
  stock: 100,
  category: electronicsId,
});

// Query by document type
const electronics = await catalog.findOne("category", { name: "Electronics" });
const products = await catalog.find("product", { category: electronics._id });
```

> ⚠️ **Important Note**: MultiCollection uses a single MongoDB collection to
> store different document types, not multiple collections. This provides a more
> efficient storage model while maintaining type safety. Each document
> automatically gets a `_type` field to identify its document type.

## 🔐 Scoped Multi-Collections

A **scoped multi-collection** takes the Multi-Collection pattern one step
further: it partitions a single physical collection by a `_scope` discriminator
— a value like `tenantId` or `expositionId`. Every scope lives in the same
MongoDB collection, but the API only ever hands you **scope-bound views**, so a
query cannot accidentally cross a scope boundary.

**Scope safety by construction**: instead of remembering to add
`{ tenantId: ... }` to every query (and hoping nobody forgets), you narrow to a
scope once with `.scope(id)`. From then on every read, write, aggregation and
paginated query issued through that view is automatically constrained to it.
Cross-scope access is not a query you can forget to write — it is a surface you
have to deliberately opt into.

Each document carries three reserved meta fields injected by the library: `_id`,
`_type` (the document type, as in a plain multi-collection) and `_scope` (the
partition). `_scope` and `_type` are reserved — they cannot appear in your field
schemas, and they are injected for you on insert.

### Quickstart

```typescript
import { refId, scopedMultiCollection, withIndex } from "mongodbee";
import * as v from "mongodbee/schema";

// The `scope` schema validates every scope id on `.scope(id)`.
const catalog = await scopedMultiCollection(db, "catalog", {
  scope: refId("exposition"),
  types: {
    artwork: {
      title: v.string(),
      year: v.number(),
      artistId: v.optional(refId("artist")),
    },
    artist: {
      name: withIndex(v.string(), { unique: true }),
    },
  },
});

// Narrow to one scope — validated synchronously, throws on a bad id.
const expo = catalog.scope("exposition:abc123");

// Insert (never pass _scope/_type — they are injected).
const artistId = await expo.insertOne("artist", { name: "Da Vinci" });
await expo.insertOne("artwork", {
  title: "Mona Lisa",
  year: 1503,
  artistId,
});

// Read — automatically constrained to this scope.
const artworks = await expo.find("artwork", { year: { $gte: 1500 } });

// Paginate — cursor-based, scope-safe.
const page = await expo.paginate("artwork", {}, {
  limit: 25,
  sort: { year: -1 },
});
console.log(page.total, page.position, page.data);
```

The scope-bound view also exposes `getById`, `findOne`, `findOneAny` / `findAny`
(cross-type reads), `countDocuments`, `updateOne` / `updateMany` (with
`removeField()` support), `deleteId` / `deleteIds` / `deleteMany`, a scope-safe
`aggregate`, and `findProject` for fast partial reads. See
[SCOPED-MULTI-COLLECTIONS.md](./doc/SCOPED-MULTI-COLLECTIONS.md) for the full
surface.

### Reading across several scopes

`scopes([...])` returns a **read-only** view spanning the listed scopes.
Inserts, updates and deletes are intentionally absent — to write you must narrow
to a single scope, which keeps the write target unambiguous:

```typescript
const bothExpos = catalog.scopes([
  "exposition:abc123",
  "exposition:def456",
]);

const artworks = await bothExpos.find("artwork", {}); // from both scopes
const count = await bothExpos.countDocuments("artwork");
```

### The unscoped admin view

Cross-scope reads over the **whole** collection are off by default — you have to
opt in with `allowUnscoped: true`. Even then, `.unscoped` is read-only, and any
code path that reaches it has deliberately stepped outside scope safety:

```typescript
const catalog = await scopedMultiCollection(db, "catalog", {
  scope: refId("exposition"),
  types: { artwork: { title: v.string() } },
  allowUnscoped: true, // opt in
});

const everything = await catalog.unscoped.find("artwork", {});
```

Without `allowUnscoped: true`, touching `.unscoped` throws.

### Lifecycle helpers

The collection object exposes a few scope-level operations:

```typescript
await catalog.listScopes(); // string[] of scope ids in use
await catalog.scopeExists("exposition:abc123"); // boolean
await catalog.scopeStats("exposition:abc123"); // { total, byType: { artwork: 3, ... } }

// Destructive operations require an explicit confirmation flag.
await catalog.dropScope("exposition:abc123", { confirm: true }); // deletes one scope
await catalog.drop({ force: true }); // drops the whole collection
```

### Transactions

Like every other MongoDBee collection, a scoped multi-collection participates in
sessions. `withSession` runs its callback inside a MongoDB transaction; every
operation on any view of this collection (and on other collections sharing the
same client) joins the same session:

```typescript
await catalog.withSession(async () => {
  const expo = catalog.scope("exposition:abc123");
  const artistId = await expo.insertOne("artist", { name: "Vermeer" });
  await expo.insertOne("artwork", {
    title: "Girl with a Pearl Earring",
    year: 1665,
    artistId,
  });
  // Both succeed or both roll back.
});
```

> 📖 **Deeper dive**: For the `findProject` contract, the full `paginate` option
> set and cursor semantics, the scoped aggregate stage builder (scope-injecting
> `lookup` / `anyLookup` / `externalLookup`) and index semantics
> (per-`(scope, type)` uniqueness, `global: true`, the always-on base index, and
> the `unique` + optional-field `null` caveat), see
> [SCOPED-MULTI-COLLECTIONS.md](./doc/SCOPED-MULTI-COLLECTIONS.md).

## 🔄 Migration System

MongoDBee includes a **powerful migration framework** that lets you evolve your
database schema over time with full type safety and rollback support. Migrations
are the recommended way to manage schema changes in production.

### Why Migrations?

- **Reproducible**: Same migrations produce the same database state across
  environments
- **Reversible**: Every migration can be rolled back with confidence
- **Auditable**: Full history of all schema changes
- **Validated**: Migrations are validated against schemas before applying to
  catch errors early
- **Multi-Collection Native**: First-class support for multi-collection and
  multi-model operations

### Quick Start

Initialize migrations in your project:

```bash
# Bun / Node
bunx mongodbee init           # or: npx mongodbee init

# Deno
deno run -A jsr:@diister/mongodbee/migration/cli/bin init
```

This creates:

- `migrations/` folder for your migration files
- `mongodbee.config.ts` configuration file
- `schemas.ts` file to define your current database schemas

**Configuration**: The generated `mongodbee.config.ts` uses a simple structure:

```typescript
import { defineConfig } from "mongodbee";
import process from "node:process";

export default defineConfig({
  database: {
    connection: {
      uri: process.env.MONGODB_URI!,
    },
    name: process.env.MONGODB_DATABASE!,
  },
  paths: {
    migrations: "./migrations",
    schemas: "./schemas.ts",
  },
});
```

### Creating Your First Migration

Generate a new migration file:

```bash
bunx mongodbee generate --name initial_schema
```

This creates a migration file in `migrations/` with this structure:

```typescript
import { migrationDefinition } from "mongodbee/migration";
import { dbId } from "mongodbee";
import * as v from "valibot";

const id = "2025_10_14_1234_ABC123@initial_schema";
const name = "initial_schema";

export default migrationDefinition(id, name, {
  parent: null, // First migration has no parent
  schemas: {
    collections: {
      users: {
        _id: dbId("user"),
        name: v.string(),
        email: v.string(),
        createdAt: v.string(),
      },
    },
    multiCollections: {},
    multiModels: {},
  },
  migrate(migration) {
    // Create the users collection
    migration.createCollection("users")
      .seed([
        { name: "Alice", email: "alice@example.com", createdAt: "2025-10-01" },
        { name: "Bob", email: "bob@example.com", createdAt: "2025-10-02" },
      ])
      .end();

    return migration.compile();
  },
});
```

**Important**: After creating a migration, update your `schemas.ts` file to
match:

```typescript
import { dbId } from "mongodbee";
import * as v from "valibot";
import { type SchemasDefinition } from "mongodbee/migration";

export const schemas = {
  collections: {
    users: {
      _id: dbId("user"),
      name: v.string(),
      email: v.string(),
      createdAt: v.string(),
    },
  },
  multiCollections: {},
  multiModels: {},
} satisfies SchemasDefinition;
```

### Validating and Applying Migrations

```bash
# Validate migrations before applying
bunx mongodbee check

# Check migration status
bunx mongodbee status

# Apply all pending migrations
bunx mongodbee migrate

# Rollback the last migration
bunx mongodbee rollback
```

The `check` command validates your migrations by:

- Checking that schemas are consistent between migrations and `schemas.ts`
- Simulating each migration to catch errors before they hit your database
- Validating that seed data matches your schema definitions

### Creating Multi-Collections

Multi-collections store different document types in a single collection:

```typescript
export default migrationDefinition(id, name, {
  parent: previousMigration,
  schemas: {
    collections: {
      ...parent.schemas.collections,
    },
    multiCollections: {
      analytics: {
        dailyStats: {
          _id: v.string(), // Date format: "YYYY-MM-DD"
          views: v.number(),
          newUsers: v.number(),
        },
        monthlyStats: {
          _id: v.string(), // Date format: "YYYY-MM"
          totalViews: v.number(),
          totalUsers: v.number(),
        },
      },
    },
    multiModels: {},
  },
  migrate(migration) {
    migration.createMultiCollection("analytics")
      .type("dailyStats")
      .seed([
        { _id: "2025-10-01", views: 150, newUsers: 5 },
        { _id: "2025-10-02", views: 200, newUsers: 8 },
      ])
      .end()
      .type("monthlyStats")
      .seed([
        { _id: "2025-10", totalViews: 350, totalUsers: 13 },
      ])
      .end();

    return migration.compile();
  },
});
```

### Data Transformations

Transform existing documents when you change schemas:

```typescript
export default migrationDefinition(id, name, {
  parent: previousMigration,
  schemas: {
    collections: {
      ...parent.schemas.collections,
      posts: {
        ...parent.schemas.collections.posts,
        tags: v.array(v.string()), // New field!
      },
    },
    multiCollections: {
      ...parent.schemas.multiCollections,
    },
    multiModels: {
      ...parent.schemas.multiModels,
    },
  },
  migrate(migration) {
    // Transform all posts to add tags field
    migration.collection("posts")
      .transform({
        up: (doc) => {
          // Add tags based on content
          const tags = [];
          if (doc.title.includes("TypeScript")) {
            tags.push("programming", "typescript");
          }
          return { ...doc, tags };
        },
        down: (doc) => {
          // Remove tags on rollback
          const { tags, ...rest } = doc;
          return rest;
        },
        lossy: false, // false because we can safely rollback
      })
      .end();

    return migration.compile();
  },
});
```

### Multi-Model Pattern

Multi-models let you create multiple collections with the same structure.
Perfect for per-user workspaces, tenant isolation, or per-entity data:

**1. Define the model:**

```typescript
// In your schemas.ts or a separate file
import { defineModel } from "mongodbee";
import * as v from "valibot";

export const workspaceModel = defineModel("workspace", {
  schema: {
    info: {
      _id: v.literal("info:0"),
      ownerId: dbId("user"),
      name: v.string(),
      createdAt: v.string(),
    },
    task: {
      _id: dbId("task"),
      title: v.string(),
      completed: v.boolean(),
      createdAt: v.string(),
    },
    note: {
      _id: dbId("note"),
      title: v.string(),
      content: v.string(),
      createdAt: v.string(),
    },
  },
});
```

**2. Use in migrations:**

```typescript
export default migrationDefinition(id, name, {
  parent: previousMigration,
  schemas: {
    collections: {
      ...parent.schemas.collections,
    },
    multiCollections: {
      ...parent.schemas.multiCollections,
    },
    multiModels: {
      workspace: workspaceModel.schema,
    },
  },
  migrate(migration) {
    // Create instances for specific users
    migration.createMultiModelInstance("workspace-alice", "workspace")
      .type("info")
      .seed([
        {
          _id: "info:0",
          ownerId: "user:123",
          name: "Alice's Workspace",
          createdAt: "2025-10-01",
        },
      ])
      .end()
      .type("task")
      .seed([
        { title: "Write blog post", completed: false, createdAt: "2025-10-01" },
      ])
      .end()
      .type("note")
      .seed([
        {
          title: "Ideas",
          content: "Some thoughts...",
          createdAt: "2025-10-01",
        },
      ])
      .end();

    return migration.compile();
  },
});
```

**3. Use in your application:**

```typescript
import { multiCollection } from "mongodbee";
import { discoverMultiCollectionInstances } from "mongodbee/migration";
import { workspaceModel } from "./schemas.ts";

// Discover all workspace instances
const instances = await discoverMultiCollectionInstances(db, "workspace");
console.log(`Found ${instances.length} workspaces`);

// Access a specific workspace
const aliceWorkspace = await multiCollection(
  db,
  "workspace-alice",
  workspaceModel.schema,
);

// Use it like any multi-collection
const tasks = await aliceWorkspace.find("task", {});
console.log(`Alice has ${tasks.length} tasks`);
```

### Migration Operations Reference

**Collections**:

- `createCollection(name)` - Create a new collection
- `collection(name)` - Access existing collection for transformations

**Multi-Collections**:

- `createMultiCollection(name)` - Create a new multi-collection
- `.type(typeName)` - Add a document type to the multi-collection
- `.seed(docs)` - Add seed data for this type
- `.end()` - Complete the type definition

**Multi-Models**:

- `createMultiModelInstance(instanceName, modelType)` - Create a collection
  instance from a model
- Then use `.type()`, `.seed()`, `.end()` just like multi-collections

**Data Transformations**:

- `collection(name).transform({ up, down, lossy? })` - Transform existing
  documents
  - `up`: Function to transform documents forward
  - `down`: Function to reverse the transformation
  - `lossy`: Set to `true` if rollback loses data

**Always remember**:

- Call `.end()` to complete builder chains
- Return `migration.compile()` at the end of `migrate()`
- Update `schemas.ts` after each migration to match the last migration's schema

### Application Startup Validation

Validate your migration system at application startup to catch issues before
they become problems:

```typescript
import { checkMigrationStatus } from "mongodbee/migration";

// Automatically loads paths from mongodbee.config.ts
const status = await checkMigrationStatus({ db });

// Simple check
if (!status.ok) {
  console.error(status.message);
  throw new Error("Migration system unhealthy");
}

// Check for pending migrations
if (status.database && !status.database.isUpToDate) {
  console.warn(`${status.database.pendingCount} pending migration(s)`);
}
```

**Result structure:**

```typescript
{
  ok: boolean,              // ✅ Main health check
  message: string,          // Human-readable summary

  counts: {
    total: number,          // Total migrations
    valid: number,          // Valid migrations
    invalid: number         // Invalid migrations
  },

  validation: {
    isSchemaConsistent: boolean,  // Schemas match
    areMigrationsValid: boolean,  // Can be simulated
    errors: string[],             // Validation errors
    warnings: string[]            // Validation warnings
  },

  database?: {               // Only when db provided
    isUpToDate: boolean,     // All migrations applied
    appliedCount: number,    // Number applied
    pendingCount: number,    // Number pending
    pendingIds: string[]     // IDs of pending migrations
  }
}
```

**Fail-fast mode:**

```typescript
import { assertMigrationSystemHealthy } from "mongodbee/migration";

// Throws if unhealthy - loads from config automatically
await assertMigrationSystemHealthy({ db });
```

### Learn More

For complete migration documentation, see [MIGRATIONS.md](./doc/MIGRATIONS.md).

## 📊 Indexes with `withIndex`

MongoDBee provides declarative index creation through the `withIndex()`
function. Indexes are automatically created and managed based on your schema
definition:

```typescript
import { withIndex } from "mongodbee";

// Basic unique index
const users = await collection(db, "users", {
  email: withIndex(v.string(), { unique: true }),
  username: withIndex(v.string(), { unique: true, insensitive: true }),
});

// MultiCollection with indexes
const catalog = await multiCollection(db, "catalog", {
  user: {
    email: withIndex(v.string(), { unique: true }),
    name: withIndex(v.string(), { unique: true }),
  },
  product: {
    name: withIndex(v.string(), { unique: true }),
    price: v.number(),
  },
});
```

### Index Options

- **`unique`**: Creates a unique index to prevent duplicate values (default:
  `false`)
- **`insensitive`**: Case-insensitive index using MongoDB collation (default:
  `false`)
- **`collation`**: Custom MongoDB collation options for advanced text sorting

```typescript
// Case-insensitive unique index
email: withIndex(v.string(), {
  unique: true,
  insensitive: true,
});

// Custom collation
name: withIndex(v.string(), {
  unique: true,
  collation: { locale: "fr", strength: 2 },
});
```

### Multi-Collection Index Behavior

In multi-collections, indexes are automatically scoped to document types using
MongoDB partial filter expressions. This provides several benefits:

- **Type-scoped uniqueness**: Unique constraints apply only within the same
  document type
- **Schema flexibility**: Each document type can have the same field name with
  different uniqueness rules
- **Performance optimization**: Indexes are automatically filtered by `_type`
  field for better query performance
- **Automatic management**: Index creation, updates, and cleanup are handled
  automatically

> ⚠️ **Work in Progress**: Index support is currently in development. Basic
> functionality works, but advanced features are being added.

## 🔄 Change Streams

Listen to real-time database changes:

```typescript
// Subscribe to collection events
const unsubscribe = users.on("insert", (event) => {
  console.log("New user created:", event.fullDocument);
});

// Each .on() call returns an unsubscribe function
unsubscribe(); // Stop listening to insert events
```

> ⚠️ **Note**: Change streams require MongoDB to be running as a replica set or
> sharded cluster.

## 💼 Transactions

MongoDBee makes transactions easier with automatic session management:

```typescript
// Start a transaction
await users.withSession(async () => {
  // All operations within this callback use the same session
  const userId = await users.insertOne({
    username: "newuser",
    email: "new@example.com",
    age: 25,
    createdAt: new Date(),
  });

  await orders.insertOne({
    userId,
    items: [{ product: "Item 1", price: 29.99 }],
    total: 29.99,
    status: "pending",
  });

  // If any operation fails, the entire transaction is rolled back
  // When the callback completes successfully, the transaction is committed
});
```

### Cross-collection transactions

Transactions work across regular and multi-collections:

```typescript
await users.withSession(async () => {
  // Regular collection operation
  const userId = await users.insertOne({
    username: "alice",
    email: "alice@example.com",
    age: 32,
  });

  // Multi-collection operation in the same transaction
  await catalog.insertOne("product", {
    name: "User's Product",
    price: 79.99,
    stock: 1,
    category: "user-created",
  });

  // Both operations succeed or fail together
});
```

## 📡 OpenTelemetry Tracing

MongoDBee can emit OpenTelemetry spans for every collection operation and
transaction. Tracing is strictly **opt-in** and depends only on
`@opentelemetry/api`: MongoDBee never instantiates an SDK, and when no provider
is registered (or telemetry is disabled) every code path is a silent no-op with
zero overhead.

```typescript
// 1. The application registers its own OpenTelemetry SDK
//    (e.g. NodeTracerProvider + BatchSpanProcessor + OTLP exporter,
//     then provider.register() — or Deno's built-in OTel support)

// 2. Enable telemetry per collection
const users = await collection(db, "users", schema, {
  telemetry: { enabled: true },
});

await users.insertOne({ ... }); // → CLIENT span "insertOne users"

await users.withSession(async () => {
  // → INTERNAL span "mongodb.transaction" wrapping the operation spans
});
```

Spans never carry filter/update values or document contents — only field names,
operator names and counts. Error messages are scrubbed of user values before
recording: validation messages are replaced by a synthetic message, MongoDBee's
own errors are recorded with their interpolated ids/scope values stripped, and
driver messages are recorded after redacting known value-embedding patterns
(e.g. duplicate-key values). Unknown driver messages may in rare cases still
contain values; either way the caller always receives the original, untouched
error.

For the full configuration reference, span catalog, attribute table, PII policy
and limitations, see [TELEMETRY.md](./doc/TELEMETRY.md). A ready-to-import
[Grafana dashboard](./doc/grafana/README.md) (with the matching OTel Collector
config) ships in `doc/grafana/`.

## 📋 Examples

### Dot Notation (MultiCollection API)

The MultiCollection API has full support for updating nested fields using dot
notation:

```typescript
// Define a schema with nested objects
const posts = await multiCollection(db, "content", {
  post: {
    title: v.string(),
    content: v.string(),
    metadata: v.object({
      views: v.number(),
      tags: v.array(v.string()),
    }),
    comments: v.array(v.object({
      user: v.string(),
      text: v.string(),
      likes: v.number(),
    })),
  },
});

// Insert a post
const postId = await posts.insertOne("post", {
  title: "My First Post",
  content: "Hello world!",
  metadata: {
    views: 0,
    tags: ["welcome", "first"],
  },
  comments: [
    { user: "user1", text: "Great post!", likes: 0 },
  ],
});

// Update nested fields using dot notation
await posts.updateOne("post", postId, {
  "metadata.views": 42,
  "comments.0.likes": 5,
  "metadata.tags.1": "updated",
});
```

> ⚠️ **Note**: Dot notation support in regular collections is still in
> development.

### Aggregation (MultiCollection API)

For multi-collections, the library provides helper functions to simplify working
with multiple document types in a single collection:

```typescript
// Aggregation helpers for working with different document types in the same collection
const results = await catalog.aggregate((stage) => [
  stage.match("product", { price: { $gt: 50 } }),
  stage.lookup("product", "category", "_id"),
]);
```

### Custom Schema Types

Create reusable schema components for consistency:

```typescript
// Define a reusable schema
const addressSchema = {
  street: v.string(),
  city: v.string(),
  state: v.string(),
  zipCode: v.string(),
  country: v.string(),
};

// Use it in multiple collections
const customers = await collection(db, "customers", {
  name: v.string(),
  email: withIndex(v.string(), { unique: true }),
  billingAddress: v.object(addressSchema),
  shippingAddress: v.object(addressSchema),
});
```

### Real-world Index Example

Here's an example from the playground showing indexes in action:

```typescript
// Regular collection with unique email index
const users = await collection(db, "users", {
  _id: dbId("user"),
  name: v.string(),
  email: withIndex(v.string(), { unique: true, insensitive: false }),
});

// Multi-collection with type-scoped unique indexes
const catalog = await multiCollection(db, "catalog", {
  user: {
    _id: dbId("user"),
    name: withIndex(v.string(), { unique: true }),
    email: withIndex(v.string(), { unique: true, insensitive: false }),
  },
  product: {
    _id: dbId("product"),
    name: withIndex(v.string(), { unique: true }),
    price: v.number(),
  },
});

// This works because unique constraints are scoped by document type
await catalog.insertOne("user", { name: "Alice", email: "alice@example.com" });
await catalog.insertOne("product", { name: "Alice", price: 19.99 }); // Same name, different type
```

## 📊 Project Status

Current implementation status by feature:

| Feature                   | Status            | Notes                                                                    |
| ------------------------- | ----------------- | ------------------------------------------------------------------------ |
| Migration System          | ✅ Complete       | Full CLI with check, apply, rollback, status commands                    |
| MultiCollection API       | ✅ Complete       | Full implementation with multiple document types in a single collection  |
| Multi-Model System        | ✅ Complete       | Reusable model definitions with `defineModel()` and instance creation    |
| Transactions              | ✅ Complete       | Session-based transaction support with AsyncLocalStorage                 |
| Change Streams            | ✅ Complete       | Real-time data change events and listeners                               |
| Index Management          | ✅ Complete       | `withIndex()` with automatic index creation and multi-collection scoping |
| Basic Collection API      | ⚠️ Partial        | Core functionality works, some operations need improved validation       |
| Type Inference            | ⚠️ Partial        | Good typing for basic operations, limited for advanced operations        |
| Dot Notation (Collection) | ⚠️ In Development | Full support in MultiCollection, partial in regular Collection           |

### Implementation Details

- **Complete Features**:
  - **Migration System** - Full CLI with init, generate, check, migrate,
    rollback, status commands
  - **Schema Validation** - Migrations are validated against schemas before
    applying
  - **Multi-Model System** - Create multiple collection instances from a single
    schema definition
  - **Insert Operations** - `insertOne`, `insertMany` with full validation
  - **MultiCollection API** - Complete implementation with automatic `_type`
    field
  - **Transaction Support** - Full session management with AsyncLocalStorage
  - **Change Streams** - Real-time event listeners with type safety
  - **Index Management** - Declarative indexes with automatic creation and
    cleanup

- **Partial Support**:
  - **Update Operations** - `updateOne`, `updateMany` need improved type
    validation
  - **Find and Modify** - `findOneAndDelete`, `findOneAndReplace`,
    `findOneAndUpdate` basic implementation
  - **Advanced Queries** - `aggregate`, `bulkWrite` with limited type safety
  - **Dot Notation** - Full support in MultiCollection, partial in regular
    Collection

## 🤝 Contributing

Contributions are welcome! Here's how to get started:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE)
file for details.

---

<div align="center">
  <sub>Built with ❤️ by <a href="https://github.com/diister-dev">diister-dev</a></sub>
</div>

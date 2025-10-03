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
    <a href="#-migration-system">Migrations</a> •
    <a href="#-transactions">Transactions</a> •
    <a href="#-indexes-with-withindex">Indexing</a> •
    <a href="#-project-status">Status</a>
  </p>
  
  <p align="center">
    <a href="https://jsr.io/@diister/mongodbee">
      <img src="https://jsr.io/badges/@diister/mongodbee" alt="JSR Score">
    </a>
    <a href="https://github.com/diister-dev/mongodbee/blob/main/LICENSE">
      <img src="https://img.shields.io/github/license/diister-dev/mongodbee" alt="License">
    </a>
  </p>
</div>

> ✅ **Stable Core Features**: The MultiCollection API, Multi-Collection Models, and Migration System are production-ready. Other features are in active development. See the [Project Status](#-project-status) section for details.

## 💡 Vision & Philosophy

MongoDBee is built around a simple but powerful philosophy: **schemas are your source of truth, and migrations are how you evolve them over time**.

### The MongoDBee Mindset

**Schema-First Design**: Define your data structure with Valibot schemas before writing any database code. Your TypeScript types are automatically inferred from these schemas, ensuring perfect alignment between runtime validation and compile-time type checking.

**Multi-Collection Pattern**: Instead of creating dozens of MongoDB collections, group related document types into logical multi-collections. This reduces collection sprawl, simplifies queries, and maintains type safety across different document types in the same collection.

**Migration-Driven Evolution**: Never modify your database structure manually. Use migrations to track every schema change, making your database evolution reproducible, reversible, and auditable. Rollback any migration with confidence.

**Type Safety Everywhere**: From queries to updates, from aggregations to transactions - every operation is type-checked. If it compiles, it's type-safe. No more runtime surprises from typos or wrong field types.

**Protection by Default**: MongoDBee protects you from common mistakes. Accidental `deleteMany()` without filters? Blocked. Unique constraint violations? Caught at the schema level. The library is designed to prevent data loss and maintain integrity.

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
- **✅ Validation**: Define schemas with Valibot for runtime and database-level validation
- **🏗️ Multi-Collection**: Store different document types in a single MongoDB collection with type-safety
- **🔄 Migration System**: Built-in migration framework with CLI for schema evolution and rollbacks
- **📦 Multi-Collection Models**: Reusable model definitions for consistent multi-collection structures
- **⚡ Transactions**: Built-in transaction support with AsyncLocalStorage
- **🛡️ Data Protection**: Built-in safeguards against accidental data deletion
- **� Change Streams**: Type-safe event listeners for collection changes
- **🔗 Dot Notation**: Support for nested object operations (MultiCollection API)
- **📊 Indexing**: Declarative index creation with `withIndex()` for performance optimization

## 📦 Installation

### Deno

```ts
// Import from JSR
import { collection, multiCollection, withIndex } from "jsr:@diister/mongodbee";
import * as v from "jsr:@diister/mongodbee/schema";

// For migrations
import { migrationBuilder } from "jsr:@diister/mongodbee/migration";
```

### Node.js

```bash
npm install mongodbee mongodb
```

```ts
// Import in Node.js
import { collection, multiCollection, withIndex } from "mongodbee";
import * as v from "mongodbee/schema";

// For migrations
import { migrationBuilder } from "mongodbee/migration";
```

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
  createdAt: v.date()
});

// Insert with automatic validation
const userId = await users.insertOne({
  username: "janedoe",
  email: "jane@example.com",
  age: 28,
  createdAt: new Date()
});

// Query with type safety
const user = await users.findOne({ _id: userId });
console.log(user.email); // TypeScript knows this is a string
```

### Multi-Collection API

The MultiCollection API allows you to store different document types in a single MongoDB collection while maintaining strong type safety:

```typescript
import { multiCollection } from "mongodbee";
import * as v from "mongodbee/schema";

// Define a catalog schema with multiple document types in a single collection
const catalog = await multiCollection(db, "catalog", {
  product: {  // First document type
    name: withIndex(v.string(), { unique: true }),
    price: v.number(),
    stock: v.number(),
    category: v.string()
  },
  category: { // Second document type in the same collection
    name: withIndex(v.string(), { unique: true }),
    parentId: v.optional(v.string())
  }
});

// Insert into the collection with type "category"
const electronicsId = await catalog.insertOne("category", {
  name: "Electronics"
});

await catalog.insertOne("product", {
  name: "Smartphone",
  price: 499.99,
  stock: 100,
  category: electronicsId
});

// Query by document type
const electronics = await catalog.findOne("category", { name: "Electronics" });
const products = await catalog.find("product", { category: electronics._id });
```

> ⚠️ **Important Note**: MultiCollection uses a single MongoDB collection to store different document types, not multiple collections. This provides a more efficient storage model while maintaining type safety. Each document automatically gets a `_type` field to identify its document type.

## 🔄 Migration System

MongoDBee includes a **powerful migration framework** that lets you evolve your database schema over time with full type safety and rollback support. Migrations are the recommended way to manage schema changes in production.

### Why Migrations?

- **Reproducible**: Same migrations produce the same database state across environments
- **Reversible**: Every migration can be rolled back with confidence
- **Auditable**: Full history of all schema changes
- **Type-Safe**: Migrations are validated before applying to catch errors early
- **Multi-Collection Native**: First-class support for multi-collection operations

### Quick Start

Initialize migrations in your project:

```bash
# Deno
deno task migrate:init

# Or directly
deno run --allow-read --allow-write --allow-net --allow-env jsr:@diister/mongodbee/migration/cli init
```

This creates a `migrations/` folder and `mongodbee.config.ts` configuration file.

**Configuration**: The generated `mongodbee.config.ts` uses a simple configuration structure with just `database` and `paths` settings. For advanced configuration options and environment-specific settings, see the [Migration Documentation](./doc/MIGRATIONS.md#configuration-file).

### Creating Migrations

Generate a new migration file:

```bash
deno task migrate:generate add_user_fields
```

Edit the generated migration file:

```typescript
import { migrationBuilder } from "@diister/mongodbee/migration";
import * as v from "@diister/mongodbee/schema";

export async function migrate(migration: ReturnType<typeof migrationBuilder>) {
  return migration
    // Create a new collection
    .createCollection("users", {
      username: v.string(),
      email: v.string(),
      createdAt: v.date()
    })
    
    // Create a multi-collection with multiple document types
    .newMultiCollection("catalog", "product")
      .seedType("product", [
        { name: "Laptop", price: 999.99, stock: 10 }
      ])
      .end()
    
    // Transform all documents of a specific type across all instances
    .multiCollection("product")
      .type("product")
        .transform({
          up: (doc) => ({ ...doc, featured: false }),
          down: (doc) => {
            const { featured, ...rest } = doc;
            return rest;
          }
        })
        .end()
      .end()
    
    .compile();
}
```

### Applying Migrations

```bash
# Apply all pending migrations
deno task migrate:apply

# Check migration status
deno task migrate:status

# View migration history
deno task migrate:history

# Rollback the last migration
deno task migrate:rollback
```

### Multi-Collection Models

For consistency across your application, define reusable multi-collection models:

```typescript
// models/comments.ts
import { createMultiCollectionModel } from "@diister/mongodbee";
import * as v from "@diister/mongodbee/schema";

export const commentsModel = createMultiCollectionModel({
  user_comment: {
    content: v.string(),
    userId: v.string(),
    createdAt: v.date()
  },
  admin_comment: {
    content: v.string(),
    adminId: v.string(),
    pinned: v.boolean()
  }
});
```

Use the model in migrations:

```typescript
import { commentsModel } from "./models/comments.ts";

export async function migrate(migration) {
  return migration
    .newMultiCollectionFromModel("comments_main", commentsModel)
    .compile();
}
```

Use the model in your application:

```typescript
import { newMultiCollection } from "@diister/mongodbee";
import { commentsModel } from "./models/comments.ts";

// Create instances with the same schema
const blogComments = await newMultiCollection(db, "blog_comments", commentsModel);
const forumComments = await newMultiCollection(db, "forum_comments", commentsModel);

// Both instances share the same type-safe schema
await blogComments.insertOne("user_comment", {
  content: "Great post!",
  userId: "user123",
  createdAt: new Date()
});
```

### Migration Operations

MongoDBee supports a comprehensive set of migration operations:

**Collections**:
- `createCollection(name, schema)` - Create a new collection
- `dropCollection(name)` - Remove a collection
- `renameCollection(oldName, newName)` - Rename a collection

**Multi-Collections**:
- `newMultiCollection(name, type)` - Create a multi-collection instance
- `markAsMultiCollection(name, type)` - Convert existing collection to multi-collection
- `multiCollection(type).type(name).transform()` - Transform documents across ALL instances

**Data**:
- `seedCollection(name, docs)` - Insert seed data
- `seedType(type, docs)` - Insert seed data for a specific document type
- `transform({ up, down })` - Bidirectional data transformation

**Schema**:
- `updateIndexes(name, schema)` - Update collection indexes
- `customOperation({ apply, reverse })` - Custom migration logic

### Advanced: Marking Existing Collections

If you have an existing collection that follows multi-collection structure but lacks metadata:

```typescript
export async function migrate(migration) {
  return migration
    // Mark existing collection as multi-collection
    .markAsMultiCollection("legacy_events", "events")
    .compile();
}
```

This creates the necessary metadata documents without modifying existing data.

### Application Startup Validation

MongoDBee provides validation functions to check your database state at application startup. This is **critical for production** to ensure everything is in sync.

#### Complete Validation (Recommended)

The simplest and most comprehensive approach - validates both migrations and schemas:

```typescript
import { validateDatabaseState } from "@diister/mongodbee/migration";

// At application startup
const db = client.db("myapp");
const env = Deno.env.get("ENV") || "development";

const result = await validateDatabaseState(db, { env });

if (!result.isValid) {
  console.error("❌ Database validation failed!");
  console.error(result.message);
  
  // Log specific issues
  for (const issue of result.issues) {
    console.error(`  - ${issue}`);
  }
  
  if (env === "production") {
    throw new Error("Database validation failed - cannot start application");
  }
} else {
  console.log("✓ Database validation passed");
}
```

This single function checks:
- ✅ All migrations are applied
- ✅ Application schemas match database structure
- ✅ Environment-aware error reporting

#### Individual Validation Functions

For more granular control, you can use individual validation functions:

**Migration Status Validation**:

```typescript
import { 
  checkMigrationStatus,
  isLastMigrationApplied,
  validateMigrationsForEnv 
} from "@diister/mongodbee/migration";

// Option 1: Check detailed migration status
const status = await checkMigrationStatus(db);
if (!status.isUpToDate) {
  console.warn(`⚠️ ${status.message}`);
  console.warn(`Pending migrations: [${status.pendingMigrations.join(', ')}]`);
}

// Option 2: Simple boolean check
const upToDate = await isLastMigrationApplied(db);
if (!upToDate) {
  throw new Error("Latest migration not applied");
}

// Option 3: Environment-aware (warns in dev, throws in prod)
await validateMigrationsForEnv(db, env);
```

**Schema Alignment Validation**:

```typescript
import { checkSchemaAlignment } from "@diister/mongodbee/migration";

const schemaCheck = await checkSchemaAlignment(db);

if (!schemaCheck.isAligned) {
  console.error(`❌ ${schemaCheck.message}`);
  
  for (const error of schemaCheck.errors) {
    console.error(
      `  ${error.collection}.${error.field}: ` +
      `expected ${error.expected}, got ${error.actual}`
    );
  }
}
```

#### Why This Matters

- **Prevents runtime errors** from schema mismatches
- **Catches forgotten migrations** before production issues
- **Detects schema drift** when schemas.ts changed without migrations
- **Enforces deployment discipline** - no deploys without proper database state
- **Clear feedback** to developers about what's wrong

### Learn More

For complete migration documentation, see [MIGRATIONS.md](./doc/MIGRATIONS.md).

## 📊 Indexes with `withIndex`

MongoDBee provides declarative index creation through the `withIndex()` function. Indexes are automatically created and managed based on your schema definition:

```typescript
import { withIndex } from "mongodbee";

// Basic unique index
const users = await collection(db, "users", {
  email: withIndex(v.string(), { unique: true }),
  username: withIndex(v.string(), { unique: true, insensitive: true })
});

// MultiCollection with indexes
const catalog = await multiCollection(db, "catalog", {
  user: {
    email: withIndex(v.string(), { unique: true }),
    name: withIndex(v.string(), { unique: true })
  },
  product: {
    name: withIndex(v.string(), { unique: true }),
    price: v.number()
  }
});
```

### Index Options

- **`unique`**: Creates a unique index to prevent duplicate values (default: `false`)
- **`insensitive`**: Case-insensitive index using MongoDB collation (default: `false`)
- **`collation`**: Custom MongoDB collation options for advanced text sorting

```typescript
// Case-insensitive unique index
email: withIndex(v.string(), { 
  unique: true, 
  insensitive: true 
})

// Custom collation
name: withIndex(v.string(), { 
  unique: true,
  collation: { locale: 'fr', strength: 2 }
})
```

### Multi-Collection Index Behavior

In multi-collections, indexes are automatically scoped to document types using MongoDB partial filter expressions. This provides several benefits:

- **Type-scoped uniqueness**: Unique constraints apply only within the same document type
- **Schema flexibility**: Each document type can have the same field name with different uniqueness rules  
- **Performance optimization**: Indexes are automatically filtered by `type` field for better query performance
- **Automatic management**: Index creation, updates, and cleanup are handled automatically

> ⚠️ **Work in Progress**: Index support is currently in development. Basic functionality works, but advanced features are being added.

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

> ⚠️ **Note**: Change streams require MongoDB to be running as a replica set or sharded cluster.

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
    createdAt: new Date()
  });
  
  await orders.insertOne({
    userId,
    items: [{ product: "Item 1", price: 29.99 }],
    total: 29.99,
    status: "pending"
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
    age: 32
  });
  
  // Multi-collection operation in the same transaction
  await catalog.insertOne("product", {
    name: "User's Product",
    price: 79.99,
    stock: 1,
    category: "user-created"
  });
  
  // Both operations succeed or fail together
});
```

## 📋 Examples

### Dot Notation (MultiCollection API)

The MultiCollection API has full support for updating nested fields using dot notation:

```typescript
// Define a schema with nested objects
const posts = await multiCollection(db, "content", {
  post: {
    title: v.string(),
    content: v.string(),
    metadata: v.object({
      views: v.number(),
      tags: v.array(v.string())
    }),
    comments: v.array(v.object({
      user: v.string(),
      text: v.string(),
      likes: v.number()
    }))
  }
});

// Insert a post
const postId = await posts.insertOne("post", {
  title: "My First Post",
  content: "Hello world!",
  metadata: {
    views: 0,
    tags: ["welcome", "first"]
  },
  comments: [
    { user: "user1", text: "Great post!", likes: 0 }
  ]
});

// Update nested fields using dot notation
await posts.updateOne("post", postId, {
  "metadata.views": 42,
  "comments.0.likes": 5,
  "metadata.tags.1": "updated"
});
```

> ⚠️ **Note**: Dot notation support in regular collections is still in development.

### Aggregation (MultiCollection API)

For multi-collections, the library provides helper functions to simplify working with multiple document types in a single collection:

```typescript
// Aggregation helpers for working with different document types in the same collection
const results = await catalog.aggregate((stage) => [
  stage.match("product", { price: { $gt: 50 } }),
  stage.lookup("product", "category", "_id")
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
  country: v.string()
};

// Use it in multiple collections
const customers = await collection(db, "customers", {
  name: v.string(),
  email: withIndex(v.string(), { unique: true }),
  billingAddress: v.object(addressSchema),
  shippingAddress: v.object(addressSchema)
});
```

### Real-world Index Example

Here's an example from the playground showing indexes in action:

```typescript
// Regular collection with unique email index
const users = await collection(db, "users", {
  _id: dbId("user"),
  name: v.string(),
  email: withIndex(v.string(), { unique: true, insensitive: false })
});

// Multi-collection with type-scoped unique indexes
const catalog = await multiCollection(db, "catalog", {
  user: {
    _id: dbId("user"),
    name: withIndex(v.string(), { unique: true }),
    email: withIndex(v.string(), { unique: true, insensitive: false })
  },
  product: {
    _id: dbId("product"),
    name: withIndex(v.string(), { unique: true }),
    price: v.number()
  }
});

// This works because unique constraints are scoped by document type
await catalog.insertOne("user", { name: "Alice", email: "alice@example.com" });
await catalog.insertOne("product", { name: "Alice", price: 19.99 }); // Same name, different type
```

## 📊 Project Status

Current implementation status by feature:

| Feature | Status | Notes |
|---------|--------|-------|
| Migration System | ✅ Complete | Full CLI with apply/rollback, multi-collection transforms, version tracking |
| MultiCollection API | ✅ Complete | Full implementation with multiple document types in a single collection |
| MultiCollection Models | ✅ Complete | Reusable model definitions with `createMultiCollectionModel()` |
| Transactions | ✅ Complete | Session-based transaction support with AsyncLocalStorage |
| Change Streams | ✅ Complete | Real-time data change events and listeners |
| Index Management | ✅ Complete | `withIndex()` with automatic index creation and multi-collection scoping |
| Basic Collection API | ⚠️ Partial | Core functionality works, some operations need improved validation |
| Type Inference | ⚠️ Partial | Good typing for basic operations, limited for advanced operations |
| Dot Notation (Collection) | ⚠️ In Development | Full support in MultiCollection, partial in regular Collection |

### Implementation Details

- **Complete Features**: 
  - **Migration System** - Full CLI with init, generate, apply, rollback, status, history commands
  - **Multi-Collection Transforms** - Transform documents across ALL instances with version tracking
  - **Multi-Collection Models** - Define once, use everywhere with `createMultiCollectionModel()`
  - **Insert Operations** - `insertOne`, `insertMany` with full validation
  - **MultiCollection API** - Complete implementation with automatic `_type` field
  - **Transaction Support** - Full session management with AsyncLocalStorage
  - **Change Streams** - Real-time event listeners with type safety
  - **Index Management** - Declarative indexes with automatic creation and cleanup
  
- **Partial Support**:
  - **Update Operations** - `updateOne`, `updateMany` need improved type validation
  - **Find and Modify** - `findOneAndDelete`, `findOneAndReplace`, `findOneAndUpdate` basic implementation
  - **Advanced Queries** - `aggregate`, `bulkWrite` with limited type safety
  - **Dot Notation** - Full support in MultiCollection, partial in regular Collection

## 🤝 Contributing

Contributions are welcome! Here's how to get started:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

---

<div align="center">
  <sub>Built with ❤️ by <a href="https://github.com/diister-dev">diister-dev</a></sub>
</div>

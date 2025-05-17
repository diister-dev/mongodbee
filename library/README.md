<div align="center">
  <img src="../brand/logo.svg" alt="MongoDBee Logo" width="200" />
  
  # MongoDBee 🍃🐝

  <p align="center">
    <strong>A type-safe MongoDB wrapper with built-in validation powered by Valibot</strong>
  </p>
  
  <p align="center">
    <a href="#key-features">Features</a> •
    <a href="#installation">Installation</a> •
    <a href="#usage">Usage</a> •
    <a href="#examples">Examples</a> •
    <a href="#transactions">Transactions</a> •
    <a href="#project-status">Status</a>
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

> ⚠️ **Project Under Development**: MongoDBee is in early development. The MultiCollection API is stable, but other features may be incomplete. See the [Project Status](#project-status) section for details.

## 🌟 Key Features

- **🔒 Type-Safe**: Strong TypeScript support with automatic type inference
- **✅ Validation**: Define schemas with Valibot for runtime and database-level validation
- **🏗️ Multi-Collection**: Store different document types in a single MongoDB collection with type-safety
- **⚡ Transactions**: Built-in transaction support with AsyncLocalStorage
- **🛡️ Data Protection**: Built-in safeguards against accidental data deletion
- **🔄 Change Streams**: Type-safe event listeners for collection changes
- **🔗 Dot Notation**: Support for nested object operations (MultiCollection API)

## 📦 Installation

### Deno

```ts
// Import from JSR
import { collection, multiCollection } from "jsr:@diister/mongodbee";
import * as v from "jsr:@diister/mongodbee/schema";
```

### Node.js

```bash
npm install mongodbee mongodb
```

```ts
// Import in Node.js
import { collection, multiCollection } from "mongodbee";
import * as v from "mongodbee/schema";
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
  email: v.pipe(v.string(), v.email()),
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
    name: v.string(),
    price: v.number(),
    stock: v.number(),
    category: v.string()
  },
  category: { // Second document type in the same collection
    name: v.string(),
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

> ⚠️ **Important Note**: MultiCollection uses a single MongoDB collection to store different document types, not multiple collections. This provides a more efficient storage model while maintaining type safety.

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
  email: v.string(),
  billingAddress: v.object(addressSchema),
  shippingAddress: v.object(addressSchema)
});
```

## 📊 Project Status

Current implementation status by feature:

| Feature | Status | Notes |
|---------|--------|-------|
| MultiCollection API | ✅ Complete | Full implementation with multiple document types in a single collection |
| Transactions | ✅ Complete | Session-based transaction support with AsyncLocalStorage |
| Change Streams | ✅ Complete | Real-time data change events and listeners |
| Basic Collection API | ⚠️ Partial | Core functionality works, some operations need improved validation |
| Type Inference | ⚠️ Partial | Good typing for basic operations, limited for advanced operations |
| Dot Notation (Collection) | ⚠️ In Development | Full support in MultiCollection, partial in regular Collection |

### Implementation Details

- **Full Support**: 
  - `insertOne`, `insertMany` - Fully implemented with validation
  - MultiCollection API - Complete implementation
  - Transaction support - Complete implementation
  - Change streams - Complete implementation
  
- **Partial Support**:
  - `updateOne`, `updateMany` (Collection API) - Needs improved type validation
  - `findOneAndDelete`, `findOneAndReplace`, `findOneAndUpdate` - Basic implementation
  - `aggregate`, `bulkWrite` - Limited type safety

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

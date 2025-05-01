<div align="center">
  <img src="./brand/logo.svg" alt="MongoDBee Logo" width="200" />
</div>

# MongoDBee 🍃🐝

A lightweight, strongly typed MongoDB wrapper for TypeScript with built-in validation using Valibot.

## Overview

MongoDBee is a MongoDB wrapper that brings the power of TypeScript and schema validation to your database operations. It combines the flexibility of MongoDB with the safety of static typing and runtime validation.

Key features:
- **Schema Validation**: Define your schemas using Valibot and automatically validate documents at runtime
- **MongoDB Integration**: Translates Valibot schemas into MongoDB JSON Schema validators
- **Type Safety**: Full TypeScript support for all MongoDB operations
- **Safe Operations**: Built-in safeguards against accidental data deletion
- **Simple API**: Maintains the familiar MongoDB API while adding validation

## Installation

```bash
# Using npm
npm install mongodbee

# Using deno
import { Collection } from "mongodbee";
```

## Quick Start

```typescript
import { Collection } from "mongodbee";
import { MongoClient } from "mongodb";
import * as v from 'valibot';

// Define your schema using Valibot
const userSchema = v.object({
  username: v.pipe(v.string(), v.minLength(3), v.maxLength(20)),
  email: v.pipe(v.string(), v.email()),
  age: v.pipe(v.number(), v.minValue(0), v.maxValue(120)),
  createdAt: v.date()
});

// Connect to MongoDB
const client = new MongoClient('mongodb://localhost:27017');
await client.connect();
const db = client.db('myapp');

// Create a typed collection with validation
const users = await Collection(db, 'users', userSchema);

// Insert a document - automatic validation
await users.insertOne({
  username: 'johndoe',
  email: 'john@example.com',
  age: 30,
  createdAt: new Date()
});

// Query with full type safety
const john = await users.findOne({ username: 'johndoe' });
```

## Features

### Validation

MongoDBee validates your data at two levels:
1. **Application-level**: Using Valibot to validate data before it reaches MongoDB
2. **Database-level**: By translating Valibot schemas into MongoDB JSON Schema validators

This dual-layer approach ensures data consistency both in your application and directly in the database.

### Type Safety

All MongoDB operations maintain full TypeScript type safety:

```typescript
// TypeScript knows the exact type of the 'user' document
const user = await users.findOne({ email: 'john@example.com' });
console.log(user.username); // No type errors
console.log(user.unknownField); // TypeScript error!
```

### Safe Operations

MongoDBee includes safeguards against accidental data loss:

```typescript
// This will throw an error because the filter is empty
await users.deleteMany({});

// This only allows deletion with specific filters
await users.deleteMany({ age: { $lt: 18 } });
```

## Roadmap

Current development status:

### 1. CRUD Operations with Validation
- **Create**:
  - ✅ insertOne
  - ✅ insertMany
- **Read**:
  - ✅ findOne
  - ✅ find
- **Update**:
  - ⚠️ updateOne (in progress)
  - ✅ replaceOne
  - ⚠️ updateMany (in progress)
- **Delete**:
  - ✅ deleteOne
  - ✅ deleteMany
- **Compound**:
  - 🔲 findOneAndUpdate
  - 🔲 findOneAndReplace
  - 🔲 findOneAndDelete
- **Aggregate**:
  - 🔲 aggregate
  - 🔲 bulkWrite

### 2. MongoDB JSON Schema Validation
- ✅ Create a collection with a validator
- ✅ Update a collection with a validator
- 🔲 Validate a collection with a validator
- 🔲 Validate a document with a validator

### 3. Future Features
- 🔲 Support deep key validation (e.g. "a.b.c")
- 🔲 Support for aggregation with strong typing
- 🔲 Integration with popular ORM frameworks
- 🔲 Migration utilities
- 🔲 Performance optimization
- 🔲 Extended validator support for complex data structures

## Project Ideas

- **CLI Tools**: Create tools for schema generation, migration, and database inspection
- **Visualization**: Build a UI for exploring and managing typed collections
- **Validator Extensions**: Extend Valibot support with custom validators for MongoDB-specific types
- **Documentation Site**: Build comprehensive documentation with examples and interactive tutorials

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
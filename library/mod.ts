/**
 * MongoDBee 🍃🐝
 * 
 * A type-safe MongoDB wrapper with built-in validation powered by Valibot.
 * Provides schemas, collections with validation, and support for different document types in a single collection.
 * 
 * @module
 * @example
 * ```typescript
 * import { collection, multiCollection } from "mongodbee";
 * import * as v from "mongodbee/schema";
 * 
 * // Create a type-safe collection with validation
 * const users = await collection(db, "users", {
 *   username: v.string(),
 *   email: v.pipe(v.string(), v.email())
 * });
 * 
 * // Create a single collection for multiple document types
 * const catalog = await multiCollection(db, "catalog", {
 *   product: { name: v.string(), price: v.number() },
 *   category: { name: v.string() }
 * });
 * ```
 */

export * from "./src/collection.ts";
export * from "./src/multi-collection.ts";
export * from "./src/mongodb.ts";
export * from "./src/indexes.ts";
export * from "./src/config.ts";
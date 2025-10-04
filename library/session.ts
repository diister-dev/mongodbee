/**
 * MongoDBee Session Module 🍃🐝
 *
 * This module provides MongoDB session and transaction management functionality.
 * Use these utilities to work with MongoDB transactions across async boundaries.
 *
 * @module
 * @example
 * ```typescript
 * import { getSessionContext } from "mongodbee/session";
 * import { MongoClient } from "mongodbee";
 *
 * // Connect to MongoDB
 * const client = new MongoClient("mongodb://localhost:27017");
 * await client.connect();
 *
 * // Get the session context
 * const { withSession } = await getSessionContext(client);
 *
 * // Use transactions with automatic commit/rollback
 * await withSession(async (session) => {
 *   // All operations in this function will use the same session
 *   await collection.insertOne({ name: "Document 1" }, { session });
 *   await collection.insertOne({ name: "Document 2" }, { session });
 * });
 * ```
 */

export * from "./src/session.ts";

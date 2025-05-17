/**
 * MongoDBee Schema Module 🍃🐝
 * 
 * This module provides validation schemas for MongoDB documents using Valibot.
 * Use these schema utilities to define document structures with validation rules.
 * 
 * @module
 * @example
 * ```typescript
 * import * as v from "mongodbee/schema";
 * 
 * const userSchema = {
 *   username: v.pipe(v.string(), v.minLength(3)),
 *   email: v.pipe(v.string(), v.email()),
 *   age: v.pipe(v.number(), v.minValue(0))
 * };
 * ```
 */

export * from "./src/schema.ts";

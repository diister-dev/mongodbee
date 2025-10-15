/**
 * @fileoverview Security utilities for manually applying validators and indexes
 *
 * These functions allow you to manually apply JSON Schema validators and indexes
 * to existing collections. Normally, these are applied automatically by:
 * - Migrations when creating collections
 * - collection()/multiCollection() when initializing
 *
 * Use these functions when you need to:
 * - Force re-application of security after manual schema changes
 * - Apply security to collections created outside MongoDBee
 * - Debug validator/index issues
 *
 * @module
 */

import type { Db } from "./mongodb.ts";
import * as v from "valibot";
import { toMongoValidator } from "./validator.ts";
import { extractIndexes } from "./indexes.ts";
import { sanitizePathName } from "./schema-navigator.ts";
import { mongoOperationQueue } from "./operation.ts";

/**
 * Options for applying security (validators and indexes)
 */
export interface ApplySecurityOptions {
  /** Whether to apply JSON Schema validator (default: true) */
  applyValidator?: boolean;

  /** Whether to apply indexes (default: true) */
  applyIndexes?: boolean;

  /** Whether to force re-application even if already applied (default: false) */
  force?: boolean;
}

/**
 * Manually apply JSON Schema validator and indexes to a collection
 *
 * **When to use:**
 * - Applying security to legacy collections not created by migrations
 * - Force re-application after manual schema changes
 * - Debugging validator issues
 *
 * **When NOT to use:**
 * - After migrations (already applied automatically)
 * - When using collection() (applies automatically)
 *
 * @example
 * ```typescript
 * import { applySecurityToCollection } from "@diister/mongodbee/security";
 * import * as v from "valibot";
 *
 * const userSchema = {
 *   _id: v.string(),
 *   username: v.string(),
 *   email: v.pipe(v.string(), v.email()),
 * };
 *
 * // Apply validator and indexes
 * await applySecurityToCollection(db, "users", userSchema);
 * ```
 *
 * @param db - MongoDB database instance
 * @param collectionName - Name of the collection
 * @param schema - Valibot schema (will be wrapped in v.object())
 * @param options - Security options
 */
export async function applySecurityToCollection(
  db: Db,
  collectionName: string,
  schema: Record<string, any>,
  options: ApplySecurityOptions = {},
): Promise<void> {
  const opts = {
    applyValidator: true,
    applyIndexes: true,
    force: false,
    ...options,
  };

  const collection = db.collection(collectionName);

  // Apply JSON Schema validator
  if (opts.applyValidator) {
    const wrappedSchema = v.object(schema);
    const validator = toMongoValidator(wrappedSchema);

    // Check if collection exists
    const collections = await db.listCollections({ name: collectionName })
      .toArray();

    if (collections.length === 0) {
      throw new Error(
        `Collection ${collectionName} does not exist. Please create it first or use migrations.`,
      );
    }

    // Check existing validator
    const existingOptions = await db.command({
      listCollections: 1,
      filter: { name: collectionName },
    });
    const currentValidator = existingOptions.cursor?.firstBatch?.[0]?.options
      ?.validator;

    // Compare validators
    const needsUpdate = opts.force ||
      !currentValidator ||
      JSON.stringify(currentValidator) !== JSON.stringify(validator);

    if (needsUpdate) {
      await db.command({
        collMod: collectionName,
        validator,
      });
    }
  }

  // Apply indexes
  if (opts.applyIndexes) {
    const wrappedSchema = v.object(schema);
    const indexes = extractIndexes(wrappedSchema);

    if (indexes.length === 0) {
      return;
    }

    await Promise.all(indexes.map((index) => {
      return mongoOperationQueue.add(async () => {
        const indexName = sanitizePathName(index.path);
        const keySpec: Record<string, number> = {};
        keySpec[index.path] = 1;

        try {
          await collection.createIndex(keySpec, {
            name: indexName,
            unique: index.metadata.unique || false,
            sparse: false,
          });
        } catch (error) {
          if (
            error instanceof Error && error.message.includes("already exists")
          ) {
            // Index already exists, skip silently
          } else {
            throw error;
          }
        }
      });
    }));
  }
}

/**
 * Manually apply JSON Schema validator and indexes to a multi-collection
 *
 * This creates a union validator that validates all document types in the multi-collection.
 *
 * @example
 * ```typescript
 * import { applySecurityToMultiCollection } from "@diister/mongodbee/security";
 *
 * const commentsSchema = {
 *   user_comment: {
 *     _id: v.string(),
 *     content: v.string(),
 *   },
 *   admin_comment: {
 *     _id: v.string(),
 *     content: v.string(),
 *     priority: v.picklist(["low", "high"]),
 *   },
 * };
 *
 * await applySecurityToMultiCollection(db, "comments", commentsSchema);
 * ```
 *
 * @param db - MongoDB database instance
 * @param collectionName - Name of the multi-collection MongoDB collection
 * @param multiCollectionSchema - Multi-collection schema (type -> fields)
 * @param options - Security options
 */
export async function applySecurityToMultiCollection(
  db: Db,
  collectionName: string,
  multiCollectionSchema: Record<string, Record<string, any>>,
  options: ApplySecurityOptions = {},
): Promise<void> {
  const opts = {
    applyValidator: true,
    applyIndexes: true,
    force: false,
    ...options,
  };

  const collection = db.collection(collectionName);

  // Apply union validator for all types
  if (opts.applyValidator) {
    const schemas = Object.entries(multiCollectionSchema).map(
      ([typeName, typeSchema]) => {
        return v.object({
          _type: v.literal(typeName),
          ...typeSchema,
        });
      },
    );

    const validator = toMongoValidator(
      v.union(schemas as [any, any, ...any[]]),
    );

    // Check if collection exists
    const collections = await db.listCollections({ name: collectionName })
      .toArray();

    if (collections.length === 0) {
      throw new Error(
        `Collection ${collectionName} does not exist. Please create it first or use migrations.`,
      );
    }

    // Update validator
    const existingOptions = await db.command({
      listCollections: 1,
      filter: { name: collectionName },
    });
    const currentValidator = existingOptions.cursor?.firstBatch?.[0]?.options
      ?.validator;

    const needsUpdate = opts.force ||
      !currentValidator ||
      JSON.stringify(currentValidator) !== JSON.stringify(validator);

    if (needsUpdate) {
      await db.command({
        collMod: collectionName,
        validator,
      });
    }
  }

  // Apply indexes for each type
  if (opts.applyIndexes) {
    const indexOperations = [];

    for (
      const [typeName, typeSchema] of Object.entries(multiCollectionSchema)
    ) {
      const wrappedSchema = v.object(typeSchema);
      const indexes = extractIndexes(wrappedSchema);

      if (indexes.length === 0) continue;

      for (const index of indexes) {
        indexOperations.push(
          mongoOperationQueue.add(async () => {
            const indexName = sanitizePathName(`${typeName}_${index.path}`);
            const keySpec: Record<string, number> = {};
            keySpec[index.path] = 1;

            try {
              await collection.createIndex(keySpec, {
                name: indexName,
                unique: index.metadata.unique || false,
                sparse: false,
                partialFilterExpression: { _type: typeName }, // Only index docs of this type
              });
            } catch (error) {
              if (
                error instanceof Error &&
                error.message.includes("already exists")
              ) {
                // Index already exists, skip silently
              } else {
                throw error;
              }
            }
          }),
        );
      }
    }

    await Promise.all(indexOperations);
  }
}

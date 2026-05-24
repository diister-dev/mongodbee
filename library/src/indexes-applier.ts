/**
 * @fileoverview Centralized index application logic for MongoDB collections
 *
 * This module provides reusable functions for applying indexes to both simple
 * collections and multi-collections. It ensures consistency across runtime
 * collection management and migrations.
 *
 * @module
 */

import * as m from "mongodb";
import type * as v from "./schema.ts";
import { extractIndexes, keyEqual, normalizeIndexOptions } from "./indexes.ts";
import { sanitizePathName } from "./schema-navigator.ts";
import { createLogger } from "./utils/logger.ts";

const log = createLogger("indexes-applier");

export interface ApplyIndexesOptions {
  /**
   * Optional queue for managing concurrent MongoDB operations.
   * If not provided, operations will run directly without queuing.
   */
  queue?: {
    add<T>(fn: () => Promise<T>): Promise<T>;
  };
}

/**
 * Applies indexes for a simple collection (non-multi-collection).
 *
 * This function:
 * 1. Compares existing indexes with desired indexes from the schema
 * 2. Drops orphaned mongodbee-created indexes that are no longer in the schema
 * 3. Drops and recreates indexes that have changed
 * 4. Creates new indexes
 *
 * @param collection - MongoDB collection to apply indexes to
 * @param schema - Valibot object schema defining the collection structure
 * @param options - Optional configuration including operation queue
 *
 * @example
 * ```typescript
 * import { applyCollectionIndexes } from "./indexes-applier.ts";
 * import * as v from "valibot";
 *
 * const schema = v.object({
 *   email: withIndex(v.string(), { unique: true }),
 *   name: v.string()
 * });
 *
 * await applyCollectionIndexes(collection, schema);
 * ```
 */
export async function applyCollectionIndexes(
  collection: m.Collection<any>,
  schema: v.ObjectSchema<any, any>,
  options: ApplyIndexesOptions = {}
): Promise<void> {
  const currentIndexes = await collection.indexes();
  const indexes = extractIndexes(schema);

  // Collect all indexes that need to be created or recreated
  const indexesToCreate: Array<{
    key: Record<string, number>;
    options: m.CreateIndexesOptions;
  }> = [];
  const indexesToDrop: string[] = [];

  // Collect all expected index names from the current schema
  const expectedIndexNames = new Set<string>();
  for (const index of indexes) {
    const indexPath = sanitizePathName(index.path);
    expectedIndexNames.add(indexPath);
  }

  // Get all possible field paths from the current schema to detect potential mongodbee indexes
  const allSchemaPaths = new Set<string>();
  function collectPaths(obj: Object, prefix = "") {
    for (const [key, value] of Object.entries(obj)) {
      const fullPath = prefix ? `${prefix}.${key}` : key;
      const sanitizedPath = sanitizePathName(fullPath);
      allSchemaPaths.add(sanitizedPath);

      if (value && typeof value === "object" && "entries" in value) {
        collectPaths((value as any).entries, fullPath);
      }
    }
  }
  collectPaths((schema as any).entries);

  // Find orphaned indexes that were created by mongodbee but are no longer in the schema
  for (const existingIndex of currentIndexes) {
    const indexName = existingIndex.name;
    if (!indexName || indexName === "_id_") continue; // Skip default _id index

    // Check if this index name matches any field in our schema
    const isSchemaField = allSchemaPaths.has(indexName);

    if (isSchemaField && !expectedIndexNames.has(indexName)) {
      // This is an orphaned mongodbee index that should be removed
      indexesToDrop.push(indexName);
    }
  }

  for (const index of indexes) {
    const keySpec = { [index.path]: 1 };
    const indexPath = sanitizePathName(index.path);

    // Try to find an existing index: prefer exact name, fallback to key match
    const existingIndex = currentIndexes.find((i) => i.name === indexPath) ||
      currentIndexes.find((i) => keyEqual(i.key || {}, keySpec));

    const desiredOptions = {
      ...index.metadata,
      name: indexPath,
    };

    // Note: changes to expireAfterSeconds rebuild the index via drop+recreate.
    // MongoDB's collMod could change just the TTL value online, but we keep
    // drop+recreate for consistency with how unique/collation changes are handled.
    let needsRecreate = true;
    if (existingIndex) {
      const existingNorm = normalizeIndexOptions(existingIndex);
      const desiredNorm = normalizeIndexOptions(desiredOptions);
      if (
        existingNorm.unique === desiredNorm.unique &&
        existingNorm.collation === desiredNorm.collation &&
        existingNorm.partialFilterExpression ===
          desiredNorm.partialFilterExpression &&
        existingNorm.expireAfterSeconds === desiredNorm.expireAfterSeconds
      ) {
        needsRecreate = false;
      }
    }

    if (!needsRecreate) continue;

    if (existingIndex) {
      indexesToDrop.push(existingIndex.name!);
    }

    indexesToCreate.push({
      key: keySpec,
      options: desiredOptions,
    });
  }

  // Drop indexes
  if (indexesToDrop.length > 0) {
    const dropPromises = indexesToDrop.map((indexName) => {
      const dropFn = () => collection.dropIndex(indexName).catch((err: unknown) => {
        // tolerate race / already dropped
        if (
          err instanceof m.MongoServerError &&
          err.codeName === "IndexNotFound"
        ) {
          // ignore
          return;
        }
        const maybe = err as { code?: number };
        if (maybe.code === 27) {
          // legacy IndexNotFound code
          return;
        }
        throw err;
      });

      return options.queue ? options.queue.add(dropFn) : dropFn();
    });

    await Promise.all(dropPromises);
  }

  // Create indexes
  if (indexesToCreate.length > 0) {
    const createPromises = indexesToCreate.map((indexSpec) => {
      const createFn = () => collection.createIndex(
        indexSpec.key,
        indexSpec.options,
      );

      return options.queue ? options.queue.add(createFn) : createFn();
    });

    await Promise.all(createPromises);
  }
}

/**
 * Applies indexes for a multi-collection (collection with multiple document types).
 *
 * Each type in a multi-collection gets its own set of indexes with:
 * - Index names prefixed by the type (e.g., "user_email" for type "user")
 * - A partialFilterExpression to filter by _type
 *
 * This function:
 * 1. Compares existing indexes with desired indexes for all types
 * 2. Drops orphaned type-specific indexes that are no longer in the schema
 * 3. Drops and recreates indexes that have changed
 * 4. Creates new indexes with proper type filtering
 *
 * @param collection - MongoDB collection to apply indexes to
 * @param schemasPerType - Map of type names to their valibot schemas
 * @param options - Optional configuration including operation queue
 *
 * @example
 * ```typescript
 * import { applyMultiCollectionIndexes } from "./indexes-applier.ts";
 * import * as v from "valibot";
 *
 * const schemas = {
 *   user: v.object({
 *     email: withIndex(v.string(), { unique: true }),
 *     name: v.string()
 *   }),
 *   admin: v.object({
 *     email: withIndex(v.string(), { unique: true }),
 *     level: v.number()
 *   })
 * };
 *
 * await applyMultiCollectionIndexes(collection, schemas);
 * ```
 */
export async function applyMultiCollectionIndexes(
  collection: m.Collection<any>,
  schemasPerType: Record<string, v.ObjectSchema<any, any>>,
  options: ApplyIndexesOptions = {}
): Promise<void> {
  const collName = collection.collectionName;
  log.debug(`applyMultiCollectionIndexes(${collName}): list current indexes`);
  const currentIndexes = await collection.indexes();
  log.debug(`applyMultiCollectionIndexes(${collName}): found ${currentIndexes.length} existing indexes`);

  // Ensure _type index exists - this is critical for multi-collection performance
  // All queries filter by _type, and partial indexes depend on efficient _type filtering
  const typeIndexName = "_type_1";
  const hasTypeIndex = currentIndexes.some((i) => i.name === typeIndexName);

  if (!hasTypeIndex) {
    log.debug(`applyMultiCollectionIndexes(${collName}): create _type index`);
    const createFn = () => collection.createIndex(
      { _type: 1 },
      { name: typeIndexName }
    );

    if (options.queue) {
      await options.queue.add(createFn);
    } else {
      await createFn();
    }
    log.debug(`applyMultiCollectionIndexes(${collName}): _type index created`);
  }

  // Extract indexes for all types
  const allIndexes = Object.entries(schemasPerType).map(([type, typeSchema]) => {
    // Wrap the type schema to include _type field
    const schemaWithType = {
      _type: { type: "literal" } as any, // Simplified for extraction
      ...(typeSchema as any).entries,
    };
    const wrappedSchema = {
      ...typeSchema,
      entries: schemaWithType
    } as v.ObjectSchema<any, any>;

    const indexes = extractIndexes(wrappedSchema);
    return {
      type,
      indexes,
    };
  });

  // Collect all indexes that need to be created or recreated
  const indexesToCreate: Array<{
    key: Record<string, number>;
    options: m.CreateIndexesOptions;
  }> = [];
  const indexesToDrop: string[] = [];

  // Collect all expected index names from the current schema
  const expectedIndexNames = new Set<string>();
  for (const { type, indexes } of allIndexes) {
    for (const index of indexes) {
      const indexName = sanitizePathName(`${type}_${index.path}`);
      expectedIndexNames.add(indexName);
    }
  }

  // Find orphaned indexes that were created by mongodbee but are no longer in the schema
  for (const existingIndex of currentIndexes) {
    const indexName = existingIndex.name;
    if (!indexName || indexName === "_id_") continue; // Skip default _id index

    // Check if this looks like a mongodbee-created index (has type prefix)
    const hasTypePrefix = Object.keys(schemasPerType).some((type) =>
      indexName.startsWith(`${type}_`)
    );

    if (hasTypePrefix && !expectedIndexNames.has(indexName)) {
      // This is an orphaned mongodbee index that should be removed
      indexesToDrop.push(indexName);
    }
  }

  // Process indexes for each type
  for (const { type, indexes } of allIndexes) {
    for (const index of indexes) {
      const keySpec = { [index.path]: 1 };
      const indexName = sanitizePathName(`${type}_${index.path}`);

      const existingIndex = currentIndexes.find((i) =>
        i.name === indexName
      ) || currentIndexes.find((i) => keyEqual(i.key || {}, keySpec));

      // partialFilterExpression is needed to scope unique constraints by type
      // e.g., two different types can have the same value on a unique field.
      // If the user provided their own partialFilterExpression on the index,
      // AND-merge it with the type filter so both conditions are enforced.
      // Order is fixed [userFilter, typeFilter] so JSON.stringify in
      // normalizeIndexOptions produces a stable diff key across reapplies.
      const userFilter = index.metadata.partialFilterExpression;
      const typeFilter = { _type: { $eq: type } };
      const partialFilterExpression = userFilter
        ? { $and: [userFilter, typeFilter] }
        : typeFilter;

      const desiredOptions = {
        ...index.metadata,
        partialFilterExpression,
        name: indexName,
      };

      let needsRecreate = true;
      if (existingIndex) {
        const existingNorm = normalizeIndexOptions(existingIndex);
        const desiredNorm = normalizeIndexOptions(desiredOptions);
        if (
          existingNorm.unique === desiredNorm.unique &&
          existingNorm.collation === desiredNorm.collation &&
          existingNorm.partialFilterExpression ===
            desiredNorm.partialFilterExpression &&
          existingNorm.expireAfterSeconds === desiredNorm.expireAfterSeconds
        ) {
          needsRecreate = false;
        }
      }

      if (!needsRecreate) {
        continue;
      }

      if (existingIndex) {
        indexesToDrop.push(existingIndex.name!);
      }

      indexesToCreate.push({
        key: keySpec,
        options: desiredOptions,
      });
    }
  }

  // Drop indexes
  if (indexesToDrop.length > 0) {
    log.debug(`applyMultiCollectionIndexes(${collName}): dropping ${indexesToDrop.length} indexes: ${indexesToDrop.join(", ")}`);
    const dropPromises = indexesToDrop.map((indexName) => {
      const dropFn = () => collection.dropIndex(indexName).catch((e) => {
        // tolerate index already dropped
        if (
          e instanceof m.MongoServerError && e.codeName === "IndexNotFound"
        ) {
          // already gone, continue
          return;
        }
        const maybe = e as { code?: number };
        if (maybe.code === 27) {
          // legacy code
          return;
        }
        throw e;
      });

      return options.queue ? options.queue.add(dropFn) : dropFn();
    });

    await Promise.all(dropPromises);
    log.debug(`applyMultiCollectionIndexes(${collName}): drops done`);
  }

  // Create indexes
  if (indexesToCreate.length > 0) {
    log.debug(
      `applyMultiCollectionIndexes(${collName}): creating ${indexesToCreate.length} indexes:`,
      indexesToCreate.map((i) => i.options.name).join(", "),
    );
    const createPromises = indexesToCreate.map((indexSpec) => {
      const createFn = () => collection.createIndex(
        indexSpec.key,
        indexSpec.options,
      );

      return options.queue ? options.queue.add(createFn) : createFn();
    });

    await Promise.all(createPromises);
    log.debug(`applyMultiCollectionIndexes(${collName}): creates done`);
  }
}

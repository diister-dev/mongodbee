/**
 * @fileoverview MongoDB applier v2 - Refactored with concise syntax
 *
 * This module provides a MongoDB applier that executes migration operations
 * against real MongoDB databases. Refactored to use a more concise and
 * maintainable syntax similar to the memory applier.
 *
 * @module
 */

import type { Db } from "../../mongodb.ts";
import type {
  MigrationDefinition,
  MigrationRule,
  SchemasDefinition,
} from "../types.ts";
import * as v from "valibot";
import { toMongoValidator } from "../../validator.ts";
import {
  applyCollectionIndexes,
  applyMultiCollectionIndexes,
} from "../../indexes-applier.ts";
import {
  createMetadataSchemas,
  createMultiCollectionInfo,
  discoverMultiCollectionInstances,
  MULTI_COLLECTION_INFO_TYPE,
  MULTI_COLLECTION_MIGRATIONS_TYPE,
  multiCollectionInstanceExists,
  recordMultiCollectionMigration,
  shouldInstanceReceiveMigrationFromChain,
} from "../multicollection-registry.ts";
import {
  extractIdPrefix,
  flowDocumentPrefix,
  flowScopeTargetId,
  flowTargetId,
  resolveSeedId,
} from "../utils/seed-id.ts";
import { createLiveTransformContext } from "../utils/transform-context.ts";
import { getIrreversibleOperations } from "../builder.ts";
import { scopedMultiCollection } from "../../scoped-multi-collection.ts";
import { getSessionContext } from "../../session.ts";

/**
 * Resolve the `_id` of a seed document: honour an explicit `_id`, else
 * derive a deterministic one (so apply and reverse compute the same id and
 * rollback can delete exactly what was inserted).
 */
function resolveSeedDocId(
  originalDoc: Record<string, unknown>,
  schemaIdField: unknown,
  fallbackPrefix: string,
  migrationId: string,
  opSignature: string,
  docIndex: number,
): string {
  return resolveSeedId(
    originalDoc,
    schemaIdField,
    fallbackPrefix,
    migrationId,
    opSignature,
    docIndex,
  );
}

/**
 * Wrap a scoped-collection transformer so the discriminators (`_id`, `_type`,
 * `_scope`) are ALWAYS re-pinned from the original document after the user
 * transform runs. A transform that accidentally drops `_scope`/`_type` would
 * otherwise make its documents invisible to every scoped query — and, because
 * the memory (simulation) applier already force-restores these fields, a
 * dry-run would pass while production silently corrupted the data. Re-pinning
 * here keeps the two appliers in lockstep: the discriminators can never be lost.
 */
function repinScopedDiscriminators(
  transform: (doc: Record<string, unknown>) => Record<string, unknown>,
): (doc: Record<string, unknown>) => Record<string, unknown> {
  return (doc) => ({
    ...transform(doc),
    _type: doc._type,
    _scope: doc._scope,
    _id: doc._id,
  });
}

/**
 * A single progress/measurement event emitted from a long-running migration
 * loop. Doubles as a performance probe: `processed` + `elapsedMs` yield live
 * throughput (docs/s) with no external profiler — the foundation for the
 * measure → optimize → re-measure loop.
 */
export interface MigrationProgressEvent {
  /**
   * The operation type or migration phase currently running, e.g.
   * "flow_to_scope", "seed_collection", or "validators+indexes".
   */
  operationType: string;
  /** Collection being written to / transformed (when meaningful). */
  collection?: string;
  /** Lifecycle phase of this operation's loop. */
  phase: "start" | "progress" | "done";
  /** Documents (or instances) processed so far within this operation. */
  processed: number;
  /** Total expected count, when cheaply knowable; `undefined` otherwise. */
  total?: number;
  /** Wall-clock milliseconds since this operation's loop started. */
  elapsedMs: number;
}

/**
 * Configuration options for the MongoDB applier
 */
export interface MongodbApplierOptions {
  /** Whether to validate operations strictly before applying */
  strictValidation?: boolean;
  /** Maximum number of documents to process in a single batch */
  batchSize?: number;
  /** Current migration ID being applied (for version tracking) */
  currentMigrationId?: string;
  /**
   * Optional hook invoked from long-running loops (transform, flow,
   * flow_to_scope). Use it to render live progress AND to measure throughput.
   * Defaults to a no-op, so it costs nothing when unused.
   */
  onProgress?: (event: MigrationProgressEvent) => void;
}

const DEFAULT_OPTIONS: Required<MongodbApplierOptions> = {
  strictValidation: true,
  batchSize: 1000,
  currentMigrationId: "unknown",
  onProgress: () => {},
};

export function createMongodbApplier(
  db: Db,
  migration: MigrationDefinition,
  options: MongodbApplierOptions = {},
): {
  applyOperation: (operation: MigrationRule) => Promise<void>;
  reverseOperation: (operation: MigrationRule) => Promise<void>;
  applyMigration: (
    operations: MigrationRule[],
    direction: "up" | "down",
  ) => Promise<void>;
  setCurrentMigrationId: (migrationId: string) => void;
} {
  const opts = { ...DEFAULT_OPTIONS, ...options };
  const context = createLiveTransformContext(migration.id);

  // Track which multi-model instances have been recorded for this migration
  // to avoid duplicate recordings when multiple operations target the same instance
  const recordedInstances = new Set<string>();

  /**
   * Build a progress reporter for one operation. Emits a `start` event now, a
   * throttled `progress` event roughly every `batchSize` items, and a final
   * `done`. `processed` + `elapsedMs` let a consumer compute throughput.
   */
  function makeReporter(
    operationType: string,
    collection: string | undefined,
    total: number | undefined,
  ): { add: (n: number) => void; done: () => void } {
    const start = performance.now();
    let processed = 0;
    let lastEmit = start;
    const emit = (phase: MigrationProgressEvent["phase"]) =>
      opts.onProgress({
        operationType,
        collection,
        phase,
        processed,
        total,
        elapsedMs: performance.now() - start,
      });
    emit("start");
    return {
      add(n) {
        processed += n;
        // Throttle emits by TIME (~20/s) rather than by document count, so the
        // signal works for both doc-batch loops (millions of items) and the
        // per-instance sync loop (one item ≈ one collection).
        const now = performance.now();
        if (now - lastEmit >= 50) {
          lastEmit = now;
          emit("progress");
        }
      },
      done() {
        emit("done");
      },
    };
  }

  /**
   * Insert seed documents in batches, emitting progress so a large seed isn't
   * a silent wait. The total is known up front, so the line shows a bar + %.
   *
   * Writes are idempotent: every seed doc already carries a DETERMINISTIC `_id`
   * (see utils/seed-id.ts), so a `replaceOne {upsert:true}` keyed on `_id` is
   * exactly-once. A plain `insertMany` would throw E11000 on the first already
   * inserted doc when a crashed migration (some pages written, migration not
   * recorded) is retried — permanently bricking it. The upsert form re-runs
   * cleanly.
   */
  async function insertSeedBatches(
    collection: ReturnType<Db["collection"]>,
    documents: unknown[],
    operationType: string,
    collectionName: string,
  ): Promise<void> {
    const reporter = makeReporter(
      operationType,
      collectionName,
      documents.length,
    );
    for (let i = 0; i < documents.length; i += opts.batchSize) {
      const batch = documents.slice(i, i + opts.batchSize);
      const bulkOps = batch.map((doc) => ({
        replaceOne: {
          filter: { _id: (doc as Record<string, unknown>)._id } as Record<
            string,
            unknown
          >,
          replacement: doc,
          upsert: true,
        },
      }));
      if (bulkOps.length > 0) {
        // deno-lint-ignore no-explicit-any
        await collection.bulkWrite(bulkOps as any);
      }
      reporter.add(batch.length);
    }
    reporter.done();
  }

  /**
   * Run `fn` over every instance with bounded concurrency. Falls back to fully
   * sequential when a MongoDB session is active — sessions are NOT
   * concurrency-safe, so a migration wrapped in `withSession()` stays correct.
   * Errors propagate (the first rejection fails the whole batch). Used for the
   * per-instance validator/index/record loops, which dominate the cost of a
   * migration spanning many multi-model instances.
   */
  async function forEachInstance(
    instances: string[],
    fn: (instanceName: string) => Promise<void>,
  ): Promise<void> {
    const session = getSessionContext(db.client).getSession();
    const concurrency = session ? 1 : Math.min(16, instances.length || 1);
    let next = 0;
    const worker = async () => {
      while (next < instances.length) {
        const i = next++;
        await fn(instances[i]);
      }
    };
    await Promise.all(Array.from({ length: concurrency }, () => worker()));
  }

  /**
   * Best-effort row count for a filter — instant `estimatedDocumentCount` for
   * the whole-collection case, accurate `countDocuments` for a filtered set.
   * Returns `undefined` (never throws) if counting fails: a missing
   * denominator must not abort a migration.
   */
  async function countForProgress(
    collectionName: string,
    filter: Record<string, unknown>,
  ): Promise<number | undefined> {
    try {
      const collection = db.collection(collectionName);
      return Object.keys(filter).length === 0
        ? await collection.estimatedDocumentCount()
        : await collection.countDocuments(filter as Record<string, unknown>);
    } catch {
      return undefined;
    }
  }

  /**
   * Helper to check if a collection exists
   */
  async function collectionExists(collectionName: string): Promise<boolean> {
    // Do NOT swallow errors here: listCollections already returns an empty
    // array for a missing collection, so the only thing a catch could hide is a
    // real fault (auth, connection, transient cluster error). Reporting that as
    // "collection does not exist" silently skips validator/index re-sync and
    // masks the true cause — let it propagate so the migration fails loudly.
    const collections = await db.listCollections({ name: collectionName })
      .toArray();
    return collections.length > 0;
  }

  /**
   * Drops a collection, tolerating ONLY the "collection does not exist" case
   * (so a `consume` re-run stays idempotent). Any other failure — auth denial,
   * write conflict, transient cluster error — must propagate: silently
   * swallowing it would report a green migration while leaving a consumed
   * source collection behind with its data still intact.
   */
  async function dropToleratingMissing(collectionName: string): Promise<void> {
    try {
      await db.collection(collectionName).drop();
    } catch (error) {
      const e = error as { code?: number; codeName?: string };
      if (e?.code === 26 || e?.codeName === "NamespaceNotFound") return;
      throw error;
    }
  }

  /**
   * Helper to disable validator for a collection temporarily
   */
  async function disableValidator(collectionName: string): Promise<void> {
    if (await collectionExists(collectionName)) {
      try {
        await db.command({
          collMod: collectionName,
          validator: {},
          validationLevel: "off",
        });
      } catch (error) {
        // Tolerate errors (collection might not have validators)
        console.warn(
          `Could not disable validator for ${collectionName}:`,
          error,
        );
      }
    }
  }

  /**
   * Synchronizes validators and indexes for all collections, multi-collections, and multi-models
   * based on the target schemas
   */
  async function synchronizeValidatorsAndIndexes(
    schemas: SchemasDefinition,
  ): Promise<void> {
    // Synchronize simple collections
    if (schemas.collections) {
      for (
        const [collectionName, schema] of Object.entries(schemas.collections)
      ) {
        if (await collectionExists(collectionName)) {
          // Update validator
          const collectionSchema = v.object(
            schema as Record<
              string,
              v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
            >,
          );
          const validator = toMongoValidator(collectionSchema);
          await db.command({
            collMod: collectionName,
            validator,
            validationLevel: "strict",
          });

          // Synchronize indexes using shared applier
          const collection = db.collection(collectionName);
          await applyCollectionIndexes(collection, collectionSchema);
        }
      }
    }

    // Synchronize multi-collections (WITH metadata)
    if (schemas.multiCollections) {
      for (
        const [collectionName, multiSchema] of Object.entries(
          schemas.multiCollections,
        )
      ) {
        if (await collectionExists(collectionName)) {
          // Build union validator with metadata schemas
          const typeSchemas = Object.entries(multiSchema).map(
            ([typeName, typeSchema]) =>
              v.object({
                _type: v.literal(typeName),
                ...(typeSchema as Record<
                  string,
                  v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
                >),
              }),
          );

          // Include metadata schemas so _information and _migrations documents can be inserted
          const allSchemas = [...typeSchemas, ...createMetadataSchemas()];

          const unionSchema = allSchemas.length > 0
            // deno-lint-ignore no-explicit-any
            ? v.union(allSchemas as any)
            : v.object({ _type: v.string() });

          const validator = toMongoValidator(unionSchema);
          await db.command({
            collMod: collectionName,
            validator,
            validationLevel: "strict",
          });

          // Synchronize indexes using shared applier
          const collection = db.collection(collectionName);
          const schemasPerType = Object.entries(multiSchema).reduce<
            Record<
              string,
              v.ObjectSchema<
                Record<
                  string,
                  v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
                >,
                undefined
              >
            >
          >((acc, [typeName, typeSchema]) => {
            acc[typeName] = v.object(
              typeSchema as Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >,
            );
            return acc;
          }, {});
          await applyMultiCollectionIndexes(collection, schemasPerType);
        }
      }
    }

    // Synchronize multi-models (WITH metadata). The validator + per-type index
    // schemas are IDENTICAL for every instance of a model type, so build them
    // ONCE per type, then fan out across instances with bounded concurrency —
    // this loop is the dominant cost of a migration over many instances.
    if (schemas.multiModels) {
      for (
        const [modelType, multiSchema] of Object.entries(schemas.multiModels)
      ) {
        const instances = await discoverMultiCollectionInstances(db, modelType);
        if (instances.length === 0) continue;

        // Build union validator with metadata schemas (once per model type).
        const typeSchemas = Object.entries(multiSchema).map(
          ([typeName, typeSchema]) =>
            v.object({
              _type: v.literal(typeName),
              ...(typeSchema as Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >),
            }),
        );
        const allSchemas = [...typeSchemas, ...createMetadataSchemas()];
        const unionSchema = allSchemas.length > 0
          // deno-lint-ignore no-explicit-any
          ? v.union(allSchemas as any)
          : v.object({ _type: v.string() });
        const validator = toMongoValidator(unionSchema);
        const schemasPerType = Object.entries(multiSchema).reduce(
          (acc, [typeName, typeSchema]) => {
            acc[typeName] = v.object(
              typeSchema as Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >,
            );
            return acc;
          },
          {} as Record<
            string,
            v.ObjectSchema<
              Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >,
              undefined
            >
          >,
        );

        // `discoverMultiCollectionInstances` only returns existing collections,
        // so the previous per-instance `collectionExists` guard was a redundant
        // listCollections round-trip — dropped.
        const syncReporter = makeReporter(
          "validators+indexes",
          modelType,
          instances.length,
        );
        await forEachInstance(instances, async (instanceName) => {
          await db.command({
            collMod: instanceName,
            validator,
            validationLevel: "strict",
          });
          await applyMultiCollectionIndexes(
            db.collection(instanceName),
            schemasPerType,
          );
          syncReporter.add(1);
        });
        syncReporter.done();
      }
    }

    // Synchronize scoped multi-collections — delegate to the runtime factory
    // so validator + scoped indexes match exactly what scopedMultiCollection()
    // would apply.
    if (schemas.scopedMultiCollections) {
      for (
        const [collectionName, scopedSchema] of Object.entries(
          schemas.scopedMultiCollections,
        )
      ) {
        if (await collectionExists(collectionName)) {
          await scopedMultiCollection(db, collectionName, {
            scope: scopedSchema.scope,
            // deno-lint-ignore no-explicit-any
            types: scopedSchema.types as any,
          });
        }
      }
    }
  }

  /**
   * Disables all validators for collections in the target schemas
   * Used before rollback to prevent validation errors
   */
  async function disableAllValidators(
    schemas: SchemasDefinition,
  ): Promise<void> {
    // Disable validators for simple collections
    if (schemas.collections) {
      for (const collectionName of Object.keys(schemas.collections)) {
        await disableValidator(collectionName);
      }
    }

    // Disable validators for multi-collections
    if (schemas.multiCollections) {
      for (const collectionName of Object.keys(schemas.multiCollections)) {
        await disableValidator(collectionName);
      }
    }

    // Disable validators for multi-models
    if (schemas.multiModels) {
      for (const modelType of Object.keys(schemas.multiModels)) {
        const instances = await discoverMultiCollectionInstances(db, modelType);
        await forEachInstance(
          instances,
          (instanceName) => disableValidator(instanceName),
        );
      }
    }

    // Disable validators for scoped multi-collections
    if (schemas.scopedMultiCollections) {
      for (
        const collectionName of Object.keys(schemas.scopedMultiCollections)
      ) {
        await disableValidator(collectionName);
      }
    }
  }

  /**
   * Helper to transform documents in batches.
   *
   * Pagination is keyed on `_id` (sorted ascending, `_id > lastSeen`) rather
   * than `skip`/`limit`. `skip` is unsafe on a dataset being mutated in place:
   * documents shifting position can be skipped or processed twice. Because the
   * transform uses `replaceOne` (the `_id` never changes), the `_id` cursor
   * advances monotonically and each document is processed exactly once.
   */
  async function transformDocuments(
    collectionName: string,
    filter: Record<string, unknown>,
    transformer: (doc: Record<string, unknown>) => Record<string, unknown>,
    operationType?: MigrationRule["type"],
  ): Promise<void> {
    const collection = db.collection(collectionName);
    const reporter = operationType
      ? makeReporter(
        operationType,
        collectionName,
        await countForProgress(collectionName, filter),
      )
      : undefined;
    let lastId: unknown = undefined;

    while (true) {
      const pageFilter = lastId === undefined
        ? filter
        : { $and: [filter, { _id: { $gt: lastId } }] };

      const documents = await collection.find(
        pageFilter as Record<string, unknown>,
      )
        .sort({ _id: 1 })
        .limit(opts.batchSize)
        .toArray();

      if (documents.length === 0) break;

      const bulkOps = documents.map((doc) => {
        try {
          const transformed = transformer(doc);
          return {
            replaceOne: {
              filter: { _id: doc._id },
              replacement: transformed,
            },
          };
        } catch (error) {
          if (opts.strictValidation) {
            throw new Error(
              `Transform failed for document ${doc._id}: ${
                error instanceof Error ? error.message : String(error)
              }`,
            );
          }
          console.warn(
            `Skipping document ${doc._id} due to transform error:`,
            error,
          );
          return null;
        }
      }).filter((op) => op !== null);

      if (bulkOps.length > 0) {
        await collection.bulkWrite(bulkOps);
      }

      reporter?.add(documents.length);
      lastId = documents[documents.length - 1]._id;
    }
    reporter?.done();
  }

  const migrations: {
    [K in MigrationRule["type"]]: {
      apply: (operation: Extract<MigrationRule, { type: K }>) => Promise<void>;
      reverse: (
        operation: Extract<MigrationRule, { type: K }>,
      ) => Promise<void>;
    };
  } = {
    create_collection: {
      apply: async (operation) => {
        const collExist = await collectionExists(operation.collectionName);

        const collOptions: Record<string, unknown> = {};
        if (operation.schema) {
          const wrappedSchema = v.object(
            operation.schema as Record<
              string,
              v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
            >,
          );
          collOptions.validator = toMongoValidator(wrappedSchema);
        }

        if (collExist) {
          if (opts.strictValidation) {
            // throw new Error(`Collection ${operation.collectionName} already exists`);
            console.warn(
              `Collection ${operation.collectionName} already exists, skipping creation.`,
            );
          }

          // If collection exists, still apply indexes && update validator if schema provided
          if (operation.schema) {
            const collection = db.collection(operation.collectionName);
            const collectionSchema = v.object(
              operation.schema as Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >,
            );
            await applyCollectionIndexes(collection, collectionSchema);

            await db.command({
              collMod: operation.collectionName,
              validator: collOptions.validator || {},
              validationLevel: "strict",
            });
          }
        } else {
          await db.createCollection(operation.collectionName, collOptions);
        }

        if (operation.schema) {
          const collection = db.collection(operation.collectionName);
          const collectionSchema = v.object(
            operation.schema as Record<
              string,
              v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
            >,
          );
          await applyCollectionIndexes(collection, collectionSchema);
        }
      },
      reverse: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist`,
          );
        }
        await db.collection(operation.collectionName).drop();
      },
    },

    rename_collection: {
      apply: async (operation) => {
        if (!await collectionExists(operation.from)) {
          if (opts.strictValidation) {
            throw new Error(
              `Cannot rename: collection ${operation.from} does not exist`,
            );
          }
          return;
        }
        await db.renameCollection(operation.from, operation.to, {
          dropTarget: operation.dropTarget ?? false,
        });
      },
      reverse: async (operation) => {
        if (!await collectionExists(operation.to)) {
          if (opts.strictValidation) {
            throw new Error(
              `Cannot rename back: collection ${operation.to} does not exist`,
            );
          }
          return;
        }
        await db.renameCollection(operation.to, operation.from);
      },
    },

    create_multicollection: {
      apply: async (operation) => {
        const collExist = await collectionExists(operation.collectionName);

        // Create union validator for all types (including metadata schemas)
        const typeSchemas = Object.entries(operation.schema).map(
          ([typeName, typeSchema]) =>
            v.object({
              _type: v.literal(typeName),
              ...(typeSchema as Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >),
            }),
        );

        // Include metadata schemas so _information and _migrations documents can be inserted
        const allSchemas = [...typeSchemas, ...createMetadataSchemas()];

        const unionSchema = allSchemas.length > 0
          // deno-lint-ignore no-explicit-any
          ? v.union(allSchemas as any)
          : v.object({ _type: v.string() });

        const validator = toMongoValidator(unionSchema);

        if (collExist) {
          if (opts.strictValidation) {
            console.warn(
              `Multi-collection ${operation.collectionName} already exists, skipping creation.`,
            );
          }

          // If collection exists, still apply indexes && update validator
          await db.command({
            collMod: operation.collectionName,
            validator,
            validationLevel: "strict",
          });
        } else {
          const collOptions = { validator };
          await db.createCollection(operation.collectionName, collOptions);
        }

        // Apply indexes using shared applier
        const collection = db.collection(operation.collectionName);
        const schemasPerType = Object.entries(operation.schema).reduce(
          (acc, [typeName, typeSchema]) => {
            acc[typeName] = v.object(
              typeSchema as Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >,
            );
            return acc;
          },
          {} as Record<
            string,
            v.ObjectSchema<
              Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >,
              undefined
            >
          >,
        );
        await applyMultiCollectionIndexes(collection, schemasPerType);
      },
      reverse: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        await db.collection(operation.collectionName).drop();
      },
    },

    create_multimodel_instance: {
      apply: async (operation) => {
        const collExist = await collectionExists(operation.collectionName);
        // Whether this is a *registered* instance (has the `_information`
        // bookkeeping doc), not merely whether a collection of that name
        // exists. A plain collection colliding with the instance name — or a
        // crash between createCollection and the metadata insert on a
        // non-transactional run — leaves collExist=true but registered=false.
        const registered = collExist
          ? await multiCollectionInstanceExists(db, operation.collectionName)
          : false;

        // Create union validator for all types + metadata schemas
        const typeSchemas = Object.entries(operation.schema).map(
          ([typeName, typeSchema]) =>
            v.object({
              _type: v.literal(typeName),
              ...(typeSchema as Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >),
            }),
        );

        const allSchemas = [...typeSchemas, ...createMetadataSchemas()];
        const unionSchema = allSchemas.length > 0
          // deno-lint-ignore no-explicit-any
          ? v.union(allSchemas as any)
          : v.object({ _type: v.string() });

        const validator = toMongoValidator(unionSchema);

        if (collExist) {
          if (opts.strictValidation && registered) {
            console.warn(
              `Multi-model instance ${operation.collectionName} already exists, skipping creation.`,
            );
          }

          // If collection exists, still apply indexes && update validator
          await db.command({
            collMod: operation.collectionName,
            validator,
            validationLevel: "strict",
          });
        } else {
          const collOptions = { validator };
          await db.createCollection(operation.collectionName, collOptions);
        }

        // Ensure the bookkeeping metadata exists whenever the instance is not
        // yet registered — including the case where the collection pre-existed
        // without metadata. The old behaviour gated this solely on
        // collection-name existence, so a name collision (or a partial prior
        // run) silently left the instance untracked: no `_information` doc,
        // per-instance migration history never recorded, invisible to
        // multiCollectionInstanceExists — yet the migration still reported
        // success.
        if (!registered) {
          await createMultiCollectionInfo(
            db,
            operation.collectionName,
            operation.modelType,
            opts.currentMigrationId,
          );

          // Mark this migration as already recorded for this instance
          // createMultiCollectionInfo already added it to the appliedMigrations array
          if (opts.currentMigrationId) {
            const recordKey =
              `${operation.collectionName}:${opts.currentMigrationId}:applied`;
            recordedInstances.add(recordKey);
          }
        }

        // Apply indexes using shared applier
        const multiCollection = db.collection(operation.collectionName);
        const schemasPerType = Object.entries(operation.schema).reduce(
          (acc, [typeName, typeSchema]) => {
            acc[typeName] = v.object(
              typeSchema as Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >,
            );
            return acc;
          },
          {} as Record<
            string,
            v.ObjectSchema<
              Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >,
              undefined
            >
          >,
        );
        await applyMultiCollectionIndexes(multiCollection, schemasPerType);
      },
      reverse: async (operation) => {
        if (
          opts.strictValidation &&
          !await multiCollectionInstanceExists(db, operation.collectionName)
        ) {
          throw new Error(
            `Multi-model instance ${operation.collectionName} does not exist`,
          );
        }
        await db.collection(operation.collectionName).drop();
      },
    },

    mark_as_multimodel: {
      apply: async (operation) => {
        const collection = db.collection(operation.collectionName);

        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          // throw new Error(`Collection ${operation.collectionName} does not exist`);
          console.warn(
            `Collection ${operation.collectionName} does not exist, creating it first.`,
          );
        }

        const existing = await collection.findOne({
          _type: MULTI_COLLECTION_INFO_TYPE,
        });
        if (existing) {
          throw new Error(
            `Collection ${operation.collectionName} is already marked as multi-model`,
          );
        }

        // Get the schema for this model type from the migration schemas
        const modelSchema = migration.schemas.multiModels
          ?.[operation.modelType];
        if (!modelSchema) {
          throw new Error(
            `Model type ${operation.modelType} not found in migration schemas`,
          );
        }

        // Update validator to include metadata schemas BEFORE inserting _information document
        const typeSchemas = Object.entries(modelSchema).map(
          ([typeName, typeSchema]) =>
            v.object({
              _type: v.literal(typeName),
              ...(typeSchema as Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >),
            }),
        );

        const allSchemas = [...typeSchemas, ...createMetadataSchemas()];
        const unionSchema = allSchemas.length > 0
          // deno-lint-ignore no-explicit-any
          ? v.union(allSchemas as any)
          : v.object({ _type: v.string() });

        const validator = toMongoValidator(unionSchema);
        await db.command({
          collMod: operation.collectionName,
          validator,
          validationLevel: "strict",
        });

        await createMultiCollectionInfo(
          db,
          operation.collectionName,
          operation.modelType,
          opts.currentMigrationId,
        );

        // Mark this migration as already recorded for this instance
        // createMultiCollectionInfo already added it to the appliedMigrations array
        if (opts.currentMigrationId) {
          const recordKey =
            `${operation.collectionName}:${opts.currentMigrationId}:applied`;
          recordedInstances.add(recordKey);
        }

        // Apply indexes using shared applier (critical for multi-model tracking)
        const modelCollection = db.collection(operation.collectionName);
        const schemasPerType = Object.entries(modelSchema).reduce(
          (acc, [typeName, typeSchema]) => {
            acc[typeName] = v.object(
              typeSchema as Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >,
            );
            return acc;
          },
          {} as Record<
            string,
            v.ObjectSchema<
              Record<
                string,
                v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
              >,
              undefined
            >
          >,
        );
        await applyMultiCollectionIndexes(modelCollection, schemasPerType);
      },
      reverse: async (operation) => {
        const collection = db.collection(operation.collectionName);
        await collection.deleteMany({
          _type: {
            $in: [MULTI_COLLECTION_INFO_TYPE, MULTI_COLLECTION_MIGRATIONS_TYPE],
          },
        });

        // Restore validator WITHOUT metadata schemas (back to plain multi-collection)
        // Get schema from the model type definition in current migration
        const modelSchema = migration.schemas.multiModels
          ?.[operation.modelType];

        if (modelSchema) {
          const typeSchemas = Object.entries(modelSchema).map(
            ([typeName, typeSchema]) =>
              v.object({
                _type: v.literal(typeName),
                ...(typeSchema as Record<
                  string,
                  v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
                >),
              }),
          );

          // NO metadata schemas - just type schemas
          const unionSchema = typeSchemas.length > 0
            // deno-lint-ignore no-explicit-any
            ? v.union(typeSchemas as any)
            : v.object({ _type: v.string() });

          const validator = toMongoValidator(unionSchema);
          await db.command({
            collMod: operation.collectionName,
            validator,
            validationLevel: "strict",
          });
        }
      },
    },

    seed_collection: {
      apply: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist`,
          );
        }

        const collection = db.collection(operation.collectionName);
        const sig = operation.collectionName;
        const documents = operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          // Resolve the deterministic _id BEFORE validation so schemas with a
          // required _id (e.g. a bare refId without a default) still validate.
          const _id = resolveSeedDocId(
            typedDoc,
            operation.schema._id,
            "",
            migration.id,
            sig,
            i,
          );
          const value = v.safeParse(v.object(operation.schema), {
            ...typedDoc,
            _id,
          });
          if (!value.success) {
            throw new Error(
              `Document validation failed: ${JSON.stringify(value.issues)}`,
            );
          }
          return { ...(value.output as Record<string, unknown>), _id };
        });

        await insertSeedBatches(
          collection,
          documents,
          operation.type,
          operation.collectionName,
        );
      },
      reverse: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist`,
          );
        }

        const collection = db.collection(operation.collectionName);
        const sig = operation.collectionName;
        const documentIds = operation.documents.map((doc: unknown, i) =>
          resolveSeedDocId(
            doc as Record<string, unknown>,
            operation.schema._id,
            "",
            migration.id,
            sig,
            i,
          )
        );
        if (documentIds.length > 0) {
          await collection.deleteMany(
            { _id: { $in: documentIds } } as Record<string, unknown>,
          );
        }
      },
    },

    seed_multicollection_type: {
      apply: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }

        const collection = db.collection(operation.collectionName);
        const sig = `${operation.collectionName}:${operation.documentType}`;
        const documents = operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          const _id = resolveSeedDocId(
            typedDoc,
            operation.schema._id,
            operation.documentType,
            migration.id,
            sig,
            i,
          );
          const value = v.safeParse(
            v.object({
              _type: v.literal(operation.documentType),
              ...operation.schema,
            }),
            {
              ...typedDoc,
              _id,
              _type: operation.documentType,
            },
          );
          if (!value.success) {
            throw new Error(
              `Document validation failed: ${JSON.stringify(value.issues)}`,
            );
          }
          return { ...(value.output as Record<string, unknown>), _id };
        });

        await insertSeedBatches(
          collection,
          documents,
          operation.type,
          operation.collectionName,
        );
      },
      reverse: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }

        const collection = db.collection(operation.collectionName);
        const sig = `${operation.collectionName}:${operation.documentType}`;
        const documentIds = operation.documents.map((doc: unknown, i) =>
          resolveSeedDocId(
            doc as Record<string, unknown>,
            operation.schema._id,
            operation.documentType,
            migration.id,
            sig,
            i,
          )
        );
        if (documentIds.length > 0) {
          await collection.deleteMany(
            { _id: { $in: documentIds } } as Record<string, unknown>,
          );
        }
      },
    },

    seed_multimodel_instance_type: {
      apply: async (operation) => {
        if (
          opts.strictValidation &&
          !await multiCollectionInstanceExists(db, operation.collectionName)
        ) {
          throw new Error(
            `Multi-model instance ${operation.collectionName} does not exist`,
          );
        }

        const collection = db.collection(operation.collectionName);
        const sig =
          `${operation.collectionName}:${operation.modelType}:${operation.documentType}`;
        const documents = operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          const _id = resolveSeedDocId(
            typedDoc,
            operation.schema._id,
            operation.documentType,
            migration.id,
            sig,
            i,
          );
          const value = v.safeParse(
            v.object({
              _type: v.literal(operation.documentType),
              ...operation.schema,
            }),
            {
              ...typedDoc,
              _id,
              _type: operation.documentType,
            },
          );
          if (!value.success) {
            throw new Error(
              `Document validation failed: ${JSON.stringify(value.issues)}`,
            );
          }
          return { ...(value.output as Record<string, unknown>), _id };
        });

        await insertSeedBatches(
          collection,
          documents,
          operation.type,
          operation.collectionName,
        );
      },
      reverse: async (operation) => {
        if (
          opts.strictValidation &&
          !await multiCollectionInstanceExists(db, operation.collectionName)
        ) {
          throw new Error(
            `Multi-model instance ${operation.collectionName} does not exist`,
          );
        }

        const collection = db.collection(operation.collectionName);
        const sig =
          `${operation.collectionName}:${operation.modelType}:${operation.documentType}`;
        const documentIds = operation.documents.map((doc: unknown, i) =>
          resolveSeedDocId(
            doc as Record<string, unknown>,
            operation.schema._id,
            operation.documentType,
            migration.id,
            sig,
            i,
          )
        );
        if (documentIds.length > 0) {
          await collection.deleteMany(
            { _id: { $in: documentIds } } as Record<string, unknown>,
          );
        }
      },
    },

    seed_multimodel_instances_type: {
      apply: async (operation) => {
        const instances = await discoverMultiCollectionInstances(
          db,
          operation.modelType,
        );

        if (instances.length === 0) {
          console.warn(
            `No instances found for model type ${operation.modelType}`,
          );
          return;
        }

        for (const collectionName of instances) {
          // Check if this instance should receive this migration. Uses the
          // chain-based comparison (walks `migration.parent` chain) — safe
          // even when migration IDs from different generators coexist
          // (legacy padded vs. timestamp+ULID).
          const shouldReceive = await shouldInstanceReceiveMigrationFromChain(
            db,
            collectionName,
            migration,
          );
          if (!shouldReceive) {
            console.log(
              `Skipping instance ${collectionName} - already has this migration`,
            );
            continue;
          }

          const collection = db.collection(collectionName);
          const sig = `${operation.modelType}:${operation.documentType}`;
          const documents = operation.documents.map((doc: unknown, i) => {
            const typedDoc = doc as Record<string, unknown>;
            return {
              ...typedDoc,
              _id: resolveSeedDocId(
                typedDoc,
                operation.schema._id,
                operation.documentType,
                migration.id,
                sig,
                i,
              ),
              _type: operation.documentType,
            };
          });

          await insertSeedBatches(
            collection,
            documents,
            operation.type,
            collectionName,
          );

          // Record migration for this instance (only once per migration, even if multiple seed operations)
          if (opts.currentMigrationId) {
            const recordKey = `${collectionName}:${opts.currentMigrationId}`;
            if (!recordedInstances.has(recordKey)) {
              await recordMultiCollectionMigration(
                db,
                collectionName,
                opts.currentMigrationId,
              );
              recordedInstances.add(recordKey);
            }
          }
        }
      },
      reverse: async (operation) => {
        const instances = await discoverMultiCollectionInstances(
          db,
          operation.modelType,
        );

        const sig = `${operation.modelType}:${operation.documentType}`;
        for (const collectionName of instances) {
          const collection = db.collection(collectionName);
          const documentIds = operation.documents.map((doc: unknown, i) =>
            resolveSeedDocId(
              doc as Record<string, unknown>,
              operation.schema._id,
              operation.documentType,
              migration.id,
              sig,
              i,
            )
          );
          if (documentIds.length > 0) {
            await collection.deleteMany(
              { _id: { $in: documentIds } } as Record<string, unknown>,
            );
          }

          // Record rollback for this instance (only once per migration, even if multiple seed operations)
          if (opts.currentMigrationId) {
            const recordKey =
              `${collectionName}:${opts.currentMigrationId}:reverted`;
            if (!recordedInstances.has(recordKey)) {
              await recordMultiCollectionMigration(
                db,
                collectionName,
                opts.currentMigrationId,
                "reverted",
              );
              recordedInstances.add(recordKey);
            }
          }
        }
      },
    },

    transform_collection: {
      apply: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist`,
          );
        }
        await transformDocuments(
          operation.collectionName,
          {},
          (doc: Record<string, unknown>) => operation.up(doc, context),
          operation.type,
        );
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist`,
          );
        }
        await transformDocuments(
          operation.collectionName,
          {},
          (doc: Record<string, unknown>) => operation.down(doc, context),
        );
      },
    },

    transform_multicollection_type: {
      apply: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        await transformDocuments(
          operation.collectionName,
          { _type: operation.documentType } as Record<string, unknown>,
          (doc: Record<string, unknown>) => operation.up(doc, context),
          operation.type,
        );
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        await transformDocuments(
          operation.collectionName,
          { _type: operation.documentType } as Record<string, unknown>,
          (doc: Record<string, unknown>) => operation.down(doc, context),
        );
      },
    },

    transform_multimodel_instance_type: {
      apply: async (operation) => {
        if (
          opts.strictValidation &&
          !await multiCollectionInstanceExists(db, operation.collectionName)
        ) {
          throw new Error(
            `Multi-model instance ${operation.collectionName} does not exist`,
          );
        }
        await transformDocuments(
          operation.collectionName,
          { _type: operation.documentType } as Record<string, unknown>,
          (doc: Record<string, unknown>) => operation.up(doc, context),
          operation.type,
        );
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        if (
          opts.strictValidation &&
          !await multiCollectionInstanceExists(db, operation.collectionName)
        ) {
          throw new Error(
            `Multi-model instance ${operation.collectionName} does not exist`,
          );
        }
        await transformDocuments(
          operation.collectionName,
          { _type: operation.documentType } as Record<string, unknown>,
          (doc: Record<string, unknown>) => operation.down(doc, context),
        );
      },
    },

    transform_multimodel_instances_type: {
      apply: async (operation) => {
        const instances = await discoverMultiCollectionInstances(
          db,
          operation.modelType,
        );

        if (instances.length === 0) {
          console.warn(
            `No instances found for model type ${operation.modelType}`,
          );
          return;
        }

        for (const collectionName of instances) {
          // Check if this instance should receive this migration. Uses the
          // chain-based comparison (walks `migration.parent` chain) — safe
          // even when migration IDs from different generators coexist
          // (legacy padded vs. timestamp+ULID).
          const shouldReceive = await shouldInstanceReceiveMigrationFromChain(
            db,
            collectionName,
            migration,
          );
          if (!shouldReceive) {
            console.log(
              `Skipping instance ${collectionName} - already has this migration`,
            );
            continue;
          }

          await transformDocuments(
            collectionName,
            { _type: operation.documentType } as Record<string, unknown>,
            (doc: Record<string, unknown>) => operation.up(doc, context),
            operation.type,
          );

          // Record migration for this instance (only once per migration, even if multiple operations)
          if (opts.currentMigrationId) {
            const recordKey = `${collectionName}:${opts.currentMigrationId}`;
            if (!recordedInstances.has(recordKey)) {
              await recordMultiCollectionMigration(
                db,
                collectionName,
                opts.currentMigrationId,
              );
              recordedInstances.add(recordKey);
            }
          }
        }
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }

        const instances = await discoverMultiCollectionInstances(
          db,
          operation.modelType,
        );

        for (const collectionName of instances) {
          await transformDocuments(
            collectionName,
            { _type: operation.documentType } as Record<string, unknown>,
            (doc: Record<string, unknown>) => operation.down(doc, context),
          );

          // Record rollback for this instance (only once per migration, even if multiple operations)
          if (opts.currentMigrationId) {
            const recordKey =
              `${collectionName}:${opts.currentMigrationId}:reverted`;
            if (!recordedInstances.has(recordKey)) {
              await recordMultiCollectionMigration(
                db,
                collectionName,
                opts.currentMigrationId,
                "reverted",
              );
              recordedInstances.add(recordKey);
            }
          }
        }
      },
    },

    flow: {
      apply: async (operation) => {
        const prefix = extractIdPrefix(operation.targetIdSchema, "");
        const source = db.collection(operation.from.collection);
        const target = db.collection(operation.into.collection);
        const baseFilter = (operation.from.where ?? {}) as Record<
          string,
          unknown
        >;
        const reporter = makeReporter(
          operation.type,
          operation.into.collection,
          await countForProgress(operation.from.collection, baseFilter),
        );

        // Batch by _id cursor (stable on a mutating set; see transformDocuments).
        let lastId: unknown = undefined;
        while (true) {
          const pageFilter = lastId === undefined
            ? baseFilter
            : { $and: [baseFilter, { _id: { $gt: lastId } }] };
          const docs = await source.find(pageFilter as Record<string, unknown>)
            .sort({ _id: 1 })
            .limit(opts.batchSize)
            .toArray();
          if (docs.length === 0) break;

          const mapped = docs.map((doc) => {
            const out = operation.map({ ...doc }) as Record<string, unknown>;
            out._id = flowTargetId(
              flowDocumentPrefix(operation.targetIsTyped, prefix, out),
              migration.id,
              operation.from.collection,
              String(doc._id),
            );
            return out;
          });
          // Idempotent copy: the target `_id` is DETERMINISTIC (flowTargetId),
          // so upsert-by-`_id` re-runs cleanly. A plain insertMany would throw
          // E11000 on a retry after a crash left some pages already written.
          const bulkOps = mapped.map((doc) => ({
            replaceOne: {
              filter: { _id: doc._id } as Record<string, unknown>,
              replacement: doc,
              upsert: true,
            },
          }));
          if (bulkOps.length > 0) {
            // deno-lint-ignore no-explicit-any
            await target.bulkWrite(bulkOps as any);
          }
          reporter.add(docs.length);
          lastId = docs[docs.length - 1]._id;
        }
        reporter.done();

        if (operation.sourceDisposition === "consume") {
          await source.deleteMany(baseFilter as Record<string, unknown>);
        }
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(
            "Flow with source: 'consume' (move) is irreversible — cannot roll back",
          );
        }
        const prefix = extractIdPrefix(operation.targetIdSchema, "");
        const source = db.collection(operation.from.collection);
        const target = db.collection(operation.into.collection);
        const baseFilter = (operation.from.where ?? {}) as Record<
          string,
          unknown
        >;

        // Copy reverse: recompute target ids from the still-present source and
        // delete those copies, batched by _id cursor.
        let lastId: unknown = undefined;
        while (true) {
          const pageFilter = lastId === undefined
            ? baseFilter
            : { $and: [baseFilter, { _id: { $gt: lastId } }] };
          const docs = await source.find(pageFilter as Record<string, unknown>)
            .sort({ _id: 1 })
            .limit(opts.batchSize)
            .toArray();
          if (docs.length === 0) break;

          const ids = docs.map((doc) => {
            const out = operation.map({ ...doc }) as Record<string, unknown>;
            return flowTargetId(
              flowDocumentPrefix(operation.targetIsTyped, prefix, out),
              migration.id,
              operation.from.collection,
              String(doc._id),
            );
          });
          await target.deleteMany(
            { _id: { $in: ids } } as Record<string, unknown>,
          );
          lastId = docs[docs.length - 1]._id;
        }
      },
    },
    flow_to_scope: {
      /**
       * Route documents from a source into a scoped multi-collection.
       *
       * Conflict handling (`onConflict`) is keyed on the target `_id` — the
       * primary key of the single physical scoped collection, so an `_id`
       * match already implies a `(scope, type, _id)` conflict.
       *
       * `source: "consume"` deletes the source only for documents that
       * ACTUALLY landed in the target (inserted or merged). With
       * `onConflict: "skip"`, a skipped document never lands, so its source is
       * preserved: the whole-collection drop / `deleteMany(where)` fast path is
       * used ONLY when zero documents were skipped, otherwise the consume falls
       * back to per-`_id` deletes of the landed docs and leaves skipped source
       * docs in place.
       *
       * Writes use deterministic ids + upsert, so a retry never DUPLICATES an
       * already-flowed document. Idempotent replay is only clean under
       * `onConflict: "skip"` or `"merge"`, though: with the DEFAULT
       * `onConflict: "error"`, a retry (or a C8 catch-up re-applying a
       * `source: "keep"` flow) hits its own previously-written docs and throws a
       * conflict — fail-loud, not a silent no-op. Prefer `skip`/`merge` for
       * consolidations expected to be retried or caught up.
       */
      apply: async (operation) => {
        const target = db.collection(operation.into.collection);
        const from = operation.from;

        // Resolve the concrete source collections + the context for each.
        const sources: {
          coll: string;
          ctx: {
            sourceCollection?: string;
            instanceName?: string;
            documentType?: string;
          };
          where?: Record<string, unknown>;
        }[] = [];
        if (from.kind === "collection") {
          sources.push({
            coll: from.name,
            ctx: { sourceCollection: from.name },
            where: from.where,
          });
        } else if (from.kind === "multiModelInstances") {
          const instances = await discoverMultiCollectionInstances(
            db,
            from.model,
          );
          for (const inst of instances) {
            sources.push({
              coll: inst,
              ctx: { instanceName: inst },
              // Skip the multi-collection's internal bookkeeping docs
              // (`_information`/`_migrations`) — they are mongodbee plumbing,
              // not real sub-documents, and would collide on `_id` across
              // instances when flowed into one scoped collection.
              where: {
                _type: {
                  $nin: [
                    MULTI_COLLECTION_INFO_TYPE,
                    MULTI_COLLECTION_MIGRATIONS_TYPE,
                  ],
                },
              },
            });
          }
        } else {
          sources.push({
            coll: from.collectionName,
            ctx: { documentType: from.documentType },
            where: { _type: from.documentType },
          });
        }

        const reporter = makeReporter(
          operation.type,
          operation.into.collection,
          undefined,
        );
        const onConflict = operation.onConflict ?? "error";

        for (const src of sources) {
          // Never flow a collection into itself: the target is not a source
          // (guards against discovery returning the in-progress target, which
          // would re-read + re-insert and collide on `_id`).
          if (src.coll === operation.into.collection) continue;
          const sourceColl = db.collection(src.coll);
          const baseWhere = (src.where ?? {}) as Record<string, unknown>;

          // Source `_id`s whose target doc actually LANDED (inserted/merged).
          // Only tracked for `onConflict: "skip"` — the only mode that can
          // leave a source doc behind — so error/merge keep the whole-source
          // fast path with no bookkeeping cost.
          const landedSourceIds: unknown[] = [];
          let anySkipped = false;

          // Batch by `_id` cursor. The source is consumed only AFTER the whole
          // loop, never mutated mid-iteration, so the cursor is stable. Each
          // page does ONE bulk existence read + ONE bulkWrite — replacing the
          // findOne+write-per-document N+1 of the previous implementation.
          let lastId: unknown = undefined;
          while (true) {
            const pageFilter = lastId === undefined
              ? baseWhere
              : { $and: [baseWhere, { _id: { $gt: lastId } }] };
            const page = await sourceColl
              .find(pageFilter as Record<string, unknown>)
              .sort({ _id: 1 })
              .limit(opts.batchSize)
              .toArray();
            if (page.length === 0) break;
            lastId = page[page.length - 1]._id;

            // Compute the target shape for every source doc up front.
            const computed = page.map((raw) => {
              const doc = raw as Record<string, unknown>;
              const scope = operation.scope(doc, src.ctx);
              const mapped = operation.map
                ? operation.map({ ...doc }, src.ctx)
                : { ...doc };
              const toType = operation.toType
                ? operation.toType(doc, src.ctx)
                : (mapped._type ?? doc._type) as string;
              let id = mapped._id;
              if (id === undefined || id === null) {
                // DETERMINISTIC id (not a random UUID): a retry recomputes the
                // same id, so an already-flowed doc is recognised by the
                // existence read below instead of being silently duplicated.
                id = flowScopeTargetId(
                  toType,
                  migration.id,
                  src.coll,
                  String(doc._id),
                );
              }
              const outDoc = {
                ...mapped,
                _id: id,
                _type: toType,
                _scope: scope,
              };
              return {
                id: id as string,
                sourceId: doc._id,
                scope,
                toType,
                outDoc,
              };
            });

            // One existence read for the whole page. `_id` is the primary key,
            // so a match by `_id` already means a (scope, type, id) conflict —
            // the extra `_type`/`_scope` of the old per-doc findOne were
            // redundant given `_id` uniqueness.
            const existingDocs = await target
              .find(
                { _id: { $in: computed.map((c) => c.id) } } as Record<
                  string,
                  unknown
                >,
              )
              .toArray();
            const existingMap = new Map<string, Record<string, unknown>>(
              existingDocs.map((
                d,
              ) => [String(d._id), d as Record<string, unknown>]),
            );

            // Plan one write per id, collapsing duplicates WITHIN the page and
            // resolving conflicts against already-persisted docs. Track which
            // source docs land so a `consume` deletes ONLY those (skipped docs
            // must survive in the source).
            const planned = new Map<string, Record<string, unknown>>();
            for (const c of computed) {
              const base = planned.get(c.id) ?? existingMap.get(c.id);
              if (base) {
                if (onConflict === "error") {
                  throw new Error(
                    `flow_to_scope: conflict on (${c.scope}, ${c.toType}, ${c.id})`,
                  );
                }
                if (onConflict === "skip") {
                  // Incoming doc dropped — it never lands, so its source stays.
                  anySkipped = true;
                  continue;
                }
                const merged = operation.merge
                  ? operation.merge(base, c.outDoc)
                  : { ...base, ...c.outDoc };
                planned.set(c.id, {
                  ...merged,
                  _id: c.id,
                  _type: c.toType,
                  _scope: c.scope,
                });
              } else {
                planned.set(c.id, c.outDoc);
              }
              if (onConflict === "skip") landedSourceIds.push(c.sourceId);
            }

            // Every write is an idempotent upsert keyed on `_id` — a crashed
            // run that already wrote some of this page is retryable without an
            // E11000 on re-insert.
            const bulkOps = [...planned.values()].map((doc) => ({
              replaceOne: {
                filter: { _id: doc._id } as Record<string, unknown>,
                replacement: doc,
                upsert: true,
              },
            }));
            if (bulkOps.length > 0) {
              // deno-lint-ignore no-explicit-any
              await target.bulkWrite(bulkOps as any);
            }
            reporter.add(page.length);
          }

          if (operation.sourceDisposition === "consume") {
            if (onConflict === "skip" && anySkipped) {
              // Some docs were skipped and never landed — a full drop /
              // deleteMany(where) would destroy them. Delete ONLY the source
              // docs that actually landed, leaving skipped docs in the source.
              for (
                let i = 0;
                i < landedSourceIds.length;
                i += opts.batchSize
              ) {
                const chunk = landedSourceIds.slice(i, i + opts.batchSize);
                if (chunk.length > 0) {
                  await sourceColl.deleteMany(
                    { _id: { $in: chunk } } as Record<string, unknown>,
                  );
                }
              }
            } else if (src.ctx.instanceName) {
              // A whole multi-model instance is consolidated away — drop it
              // entirely, including the `_information`/`_migrations` bookkeeping
              // (the read `where` only excludes those from the flow, not the drop).
              await dropToleratingMissing(src.coll);
            } else if (src.where) {
              await sourceColl.deleteMany(src.where as Record<string, unknown>);
            } else {
              await dropToleratingMissing(src.coll);
            }
          }
        }
        reporter.done();
      },
      reverse: async (_operation) => {
        throw new Error("flow_to_scope is irreversible — cannot roll back");
      },
    },
    update_indexes: {
      apply: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Collection ${operation.collectionName} does not exist`,
          );
        }
        const collection = db.collection(operation.collectionName);
        const collectionSchema = v.object(
          operation.schema as Record<
            string,
            v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>
          >,
        );
        await applyCollectionIndexes(collection, collectionSchema);
      },
      reverse: (_operation) => {
        // Index updates are idempotent, no reversal needed
        return Promise.resolve();
      },
    },

    delete_multicollection_type: {
      apply: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        const collection = db.collection(operation.collectionName);
        await collection.deleteMany(
          { _type: operation.documentType } as Record<string, unknown>,
        );
      },
      reverse: async (_operation) => {
        // Cannot restore deleted documents - this is irreversible
        throw new Error(
          `Cannot reverse delete_multicollection_type: operation is irreversible`,
        );
      },
    },

    delete_multimodel_instances_type: {
      apply: async (operation) => {
        const instances = await discoverMultiCollectionInstances(
          db,
          operation.modelType,
        );

        if (instances.length === 0) {
          console.warn(
            `No instances found for model type ${operation.modelType}`,
          );
          return;
        }

        for (const collectionName of instances) {
          const collection = db.collection(collectionName);
          await collection.deleteMany(
            { _type: operation.documentType } as Record<string, unknown>,
          );
        }
      },
      reverse: async (_operation) => {
        // Cannot restore deleted documents - this is irreversible
        throw new Error(
          `Cannot reverse delete_multimodel_instances_type: operation is irreversible`,
        );
      },
    },

    delete_scoped_multicollection_type: {
      apply: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Scoped multi-collection ${operation.collectionName} does not exist`,
          );
        }
        // One physical collection holds every scope: dropping the type is a
        // single deleteMany across all scopes.
        const collection = db.collection(operation.collectionName);
        await collection.deleteMany(
          { _type: operation.documentType } as Record<string, unknown>,
        );
      },
      reverse: async (_operation) => {
        // Cannot restore deleted documents - this is irreversible
        throw new Error(
          `Cannot reverse delete_scoped_multicollection_type: operation is irreversible`,
        );
      },
    },

    rename_multicollection_type: {
      apply: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        const collection = db.collection(operation.collectionName);
        await collection.updateMany(
          { _type: operation.oldTypeName } as Record<string, unknown>,
          { $set: { _type: operation.newTypeName } } as Record<string, unknown>,
        );
      },
      reverse: async (operation) => {
        if (
          opts.strictValidation &&
          !await collectionExists(operation.collectionName)
        ) {
          throw new Error(
            `Multi-collection ${operation.collectionName} does not exist`,
          );
        }
        const collection = db.collection(operation.collectionName);
        await collection.updateMany(
          { _type: operation.newTypeName } as Record<string, unknown>,
          { $set: { _type: operation.oldTypeName } } as Record<string, unknown>,
        );
      },
    },

    create_scoped_multicollection: {
      apply: async (operation) => {
        // Delegates to the runtime factory so validator + scoped indexes are
        // applied identically to a runtime scopedMultiCollection() call.
        await scopedMultiCollection(db, operation.collectionName, {
          scope: operation.schema.scope,
          // deno-lint-ignore no-explicit-any
          types: operation.schema.types as any,
        });
      },
      reverse: async (operation) => {
        if (await collectionExists(operation.collectionName)) {
          await db.collection(operation.collectionName).drop();
        }
      },
    },

    seed_scoped_multicollection_type: {
      apply: async (operation) => {
        const collection = db.collection(operation.collectionName);
        const sig =
          `${operation.collectionName}:${operation.scope}:${operation.documentType}`;
        const documents = operation.documents.map((doc: unknown, i) => {
          const typedDoc = doc as Record<string, unknown>;
          const _id = resolveSeedDocId(
            typedDoc,
            operation.schema._id,
            operation.documentType,
            migration.id,
            sig,
            i,
          );
          // Validate the user fields ; meta fields are added afterwards and
          // enforced by the collection's own validator on insert.
          const value = v.safeParse(v.object(operation.schema), {
            ...typedDoc,
            _id,
          });
          if (!value.success) {
            throw new Error(
              `Document validation failed: ${JSON.stringify(value.issues)}`,
            );
          }
          return {
            ...(value.output as Record<string, unknown>),
            _id,
            _type: operation.documentType,
            _scope: operation.scope,
          };
        });

        await insertSeedBatches(
          collection,
          documents,
          operation.type,
          operation.collectionName,
        );
      },
      reverse: async (operation) => {
        const collection = db.collection(operation.collectionName);
        const sig =
          `${operation.collectionName}:${operation.scope}:${operation.documentType}`;
        const ids = operation.documents.map((doc: unknown, i) =>
          resolveSeedDocId(
            doc as Record<string, unknown>,
            operation.schema._id,
            operation.documentType,
            migration.id,
            sig,
            i,
          )
        );
        if (ids.length > 0) {
          await collection.deleteMany(
            { _id: { $in: ids } } as Record<string, unknown>,
          );
        }
      },
    },

    transform_scoped_multicollection_type: {
      apply: async (operation) => {
        const filter: Record<string, unknown> = {
          _type: operation.documentType,
        };
        if (operation.scopeFilter && operation.scopeFilter.length > 0) {
          filter._scope = { $in: operation.scopeFilter };
        }
        await transformDocuments(
          operation.collectionName,
          filter,
          repinScopedDiscriminators(
            (doc: Record<string, unknown>) => operation.up(doc, context),
          ),
          operation.type,
        );
      },
      reverse: async (operation) => {
        if (operation.irreversible) {
          throw new Error(`Operation is irreversible`);
        }
        const filter: Record<string, unknown> = {
          _type: operation.documentType,
        };
        if (operation.scopeFilter && operation.scopeFilter.length > 0) {
          filter._scope = { $in: operation.scopeFilter };
        }
        await transformDocuments(
          operation.collectionName,
          filter,
          repinScopedDiscriminators(
            (doc: Record<string, unknown>) => operation.down(doc, context),
          ),
        );
      },
    },

    rename_multimodel_instances_type: {
      apply: async (operation) => {
        const instances = await discoverMultiCollectionInstances(
          db,
          operation.modelType,
        );

        if (instances.length === 0) {
          console.warn(
            `No instances found for model type ${operation.modelType}`,
          );
          return;
        }

        for (const collectionName of instances) {
          const collection = db.collection(collectionName);
          const oldTypePrefix = `${operation.oldTypeName}:`;
          const newTypePrefix = `${operation.newTypeName}:`;
          await renameTypeInPlace(
            collection,
            operation.oldTypeName,
            operation.newTypeName,
            oldTypePrefix,
            newTypePrefix,
          );
        }
      },
      reverse: async (operation) => {
        const instances = await discoverMultiCollectionInstances(
          db,
          operation.modelType,
        );

        for (const collectionName of instances) {
          const collection = db.collection(collectionName);
          const oldTypePrefix = `${operation.oldTypeName}:`;
          const newTypePrefix = `${operation.newTypeName}:`;
          // Reverse direction: newTypeName → oldTypeName
          await renameTypeInPlace(
            collection,
            operation.newTypeName,
            operation.oldTypeName,
            newTypePrefix,
            oldTypePrefix,
          );
        }
      },
    },
  };

  /**
   * Rename every document of `fromType` to `toType` in a multi-model
   * instance collection, rewriting the `_id` prefix when present.
   *
   * Iterates by repeatedly querying `{ _type: fromType }` and processing a
   * batch — no `skip`. Each renamed document stops matching the query, so
   * the loop drains the set without skipping or double-processing (the old
   * `skip(processedCount)` approach silently lost documents because the
   * matching set shrinks as we rename).
   */
  async function renameTypeInPlace(
    // deno-lint-ignore no-explicit-any
    collection: ReturnType<Db["collection"]> | any,
    fromType: string,
    toType: string,
    fromPrefix: string,
    toPrefix: string,
  ): Promise<void> {
    while (true) {
      const documents = await collection.find(
        { _type: fromType } as Record<string, unknown>,
      ).limit(opts.batchSize).toArray();

      if (documents.length === 0) break;

      for (const doc of documents) {
        const currentId = doc._id;
        let nextId = currentId;
        if (typeof currentId === "string" && currentId.startsWith(fromPrefix)) {
          nextId = toPrefix + currentId.slice(fromPrefix.length);
        }

        if (nextId !== currentId) {
          // _id is immutable in MongoDB → delete + re-insert with new id.
          await collection.deleteOne(
            { _id: currentId } as Record<string, unknown>,
          );
          await collection.insertOne({ ...doc, _id: nextId, _type: toType });
        } else {
          await collection.updateOne(
            { _id: currentId } as Record<string, unknown>,
            { $set: { _type: toType } } as Record<string, unknown>,
          );
        }
      }
    }
  }

  async function applyOperation(operation: MigrationRule): Promise<void> {
    const handler = migrations[operation.type]?.apply;
    if (!handler) {
      throw new Error(`No handler for operation type: ${operation.type}`);
    }
    // Type assertion is safe here because we're dispatching to the correct handler
    // deno-lint-ignore no-explicit-any
    return await handler(operation as any);
  }

  async function reverseOperation(operation: MigrationRule): Promise<void> {
    const handler = migrations[operation.type]?.reverse;
    if (!handler) {
      throw new Error(
        `No reverse handler for operation type: ${operation.type}`,
      );
    }
    // Type assertion is safe here because we're dispatching to the correct handler
    // deno-lint-ignore no-explicit-any
    return await handler(operation as any);
  }

  /**
   * Applies a complete migration (all operations + schema synchronization)
   *
   * This is the recommended way to apply migrations as it ensures validators
   * and indexes are synchronized after all operations are executed.
   *
   * Strategy:
   * 1. Disable ALL validators before starting (prevents validation errors during transforms)
   * 2. Apply all operations without validation interference
   * 3. Re-enable and synchronize validators with target schemas
   *
   * @param operations - Array of migration operations to apply
   * @param direction - 'up' for forward migration, 'down' for rollback
   */
  async function applyMigration(
    operations: MigrationRule[],
    direction: "up" | "down",
  ): Promise<void> {
    // Pre-scan: refuse to roll back if any operation is irreversible, BEFORE
    // touching validators or data — otherwise we'd leave the database in a
    // partially rolled-back state.
    if (direction === "down") {
      const irreversible = getIrreversibleOperations(operations);
      if (irreversible.length > 0) {
        throw new Error(
          `Cannot roll back: migration contains ${irreversible.length} ` +
            `irreversible operation(s) [${
              irreversible.map((o) => o.type).join(", ")
            }]. ` +
            `Rollback aborted before any changes were made.`,
        );
      }
    }

    // Determine target schemas based on direction
    const targetSchemas = direction === "up"
      ? migration.schemas
      : (migration.parent?.schemas || migration.schemas);

    // STEP 1: Disable ALL validators (prevents validation errors during transforms)
    // This is critical for both up and down migrations because:
    // - Up: old validators would reject documents transformed to new schema
    // - Down: new validators would reject documents transformed back to old schema
    await disableAllValidators(targetSchemas);

    // STEP 2: Apply all operations without validation interference.
    // Rollback undoes operations in LIFO order — reverse the list for 'down'
    // so dependent operations (e.g. a seed) are undone before the
    // create_collection they rely on.
    //
    // The whole apply/re-enable sequence is wrapped so validators are ALWAYS
    // restored, even if an operation throws mid-migration. Leaving validators
    // disabled is the worst outcome (silent acceptance of invalid documents);
    // re-syncing in `finally` guarantees the collection regains its guard.
    const ordered = direction === "down"
      ? [...operations].reverse()
      : operations;
    let applyError: unknown;
    try {
      for (const operation of ordered) {
        if (direction === "up") {
          await applyOperation(operation);
        } else {
          await reverseOperation(operation);
        }
      }
    } catch (err) {
      applyError = err;
    } finally {
      // STEP 3: Re-enable validators/indexes with the target schemas — even
      // on failure. If re-sync itself fails, surface it loudly but don't
      // mask the original error.
      try {
        await synchronizeValidatorsAndIndexes(targetSchemas);
      } catch (syncErr) {
        console.error(
          "CRITICAL: failed to re-enable validators after migration. " +
            "Collections may be left without validation until the next " +
            "`migrate`/`check` run.",
          syncErr,
        );
        if (!applyError) applyError = syncErr;
      }
    }

    if (applyError) throw applyError;

    // STEP 4: Record migration on ALL multi-model instances (even if not affected)
    // This ensures complete tracking of which migrations each instance has seen.
    // Use the direction's target schemas so a `down` on a migration that
    // REMOVED a model still records against the model the parent reinstates.
    if (opts.currentMigrationId && targetSchemas.multiModels) {
      await recordMigrationOnAllMultiModelInstances(
        direction === "up" ? "applied" : "reverted",
        targetSchemas.multiModels,
      );
    }
  }

  /**
   * Records the current migration on all instances of all multi-model types
   * This is called after migration to ensure all instances track the migration,
   * even if they weren't directly affected by it
   */
  async function recordMigrationOnAllMultiModelInstances(
    operation: "applied" | "reverted",
    multiModels: SchemasDefinition["multiModels"],
  ): Promise<void> {
    if (!opts.currentMigrationId) return;
    if (!multiModels) return;

    const modelTypes = Object.keys(multiModels);
    const recordedKey = `${opts.currentMigrationId}:${operation}:all`;

    // Prevent duplicate recording
    if (recordedInstances.has(recordedKey)) {
      return;
    }

    for (const modelType of modelTypes) {
      const instances = await discoverMultiCollectionInstances(db, modelType);

      await forEachInstance(instances, async (collectionName) => {
        const instanceKey =
          `${collectionName}:${opts.currentMigrationId}:${operation}`;

        // Skip if already recorded by operation handlers
        if (recordedInstances.has(instanceKey)) return;

        await recordMultiCollectionMigration(
          db,
          collectionName,
          opts.currentMigrationId,
          operation,
        );

        recordedInstances.add(instanceKey);
      });
    }

    recordedInstances.add(recordedKey);
  }

  return {
    applyOperation,
    reverseOperation,
    applyMigration,
    setCurrentMigrationId: (migrationId: string) => {
      opts.currentMigrationId = migrationId;
    },
  };
}

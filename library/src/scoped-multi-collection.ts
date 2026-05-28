/**
 * @fileoverview Scoped multi-collection — a MultiCollection partitioned by a
 * discriminator key (e.g. `expositionId`, `tenantId`). All scopes live in a
 * single physical MongoDB collection ; the API enforces scope safety so a
 * query can never accidentally cross scope boundaries.
 *
 * @module
 */

import * as v from "./schema.ts";
import type * as m from "mongodb";
import type { Db } from "./mongodb.ts";
import { toMongoValidator } from "./validator.ts";
import { dbId, newId } from "./ids.ts";
import { extractFieldsToRemove, sanitizeForMongoDB } from "./sanitizer.ts";
import { getSessionContext } from "./session.ts";
import { retryOnWriteConflict } from "./utils/retry.ts";
import { dirtyEquivalent } from "./utils/object.ts";
import { createLogger } from "./utils/logger.ts";
import { applyScopedMultiCollectionIndexes } from "./indexes-applier.ts";
import { mongoOperationQueue } from "./operation.ts";

const log = createLogger("scoped-multi-collection");

/** Reserved internal field names — cannot appear in user-defined type schemas. */
const RESERVED_FIELDS: Set<string> = new Set(["_scope", "_type"]);

// deno-lint-ignore no-explicit-any
type AnyMessage = any;
type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

/**
 * Map of document-type names to their field schemas. Each type's fields are
 * augmented with `_id`, `_type`, and `_scope` internally.
 */
export type ScopedMultiCollectionTypes = Record<
  string,
  Record<string, AnySchema>
>;

/**
 * Configuration for {@link scopedMultiCollection}.
 *
 * @template T - The map of document types
 * @template S - The Valibot schema validating scope values
 */
export type ScopedMultiCollectionConfig<
  T extends ScopedMultiCollectionTypes,
  S extends AnySchema,
> = {
  /**
   * Valibot schema validating scope values. Typically `refId("exposition")`
   * or `v.string()`. The schema is invoked on every `.scope(id)` call to
   * fail-fast on invalid identifiers.
   */
  scope: S;
  /** Map of document-type names to their field schemas. */
  types: T;
  /**
   * Enable the unscoped admin view (`catalog.unscoped`). Off by default so
   * cross-scope reads have to be opted in. Even when on, the unscoped view
   * is read-only.
   */
  allowUnscoped?: boolean;
};

// -------- Per-type schema augmentation -----------------------------------

// Use the user-provided _id schema if any (literal IDs), otherwise dbId(type).
type DynId<TField> = TField extends v.LiteralSchema<infer _L, AnyMessage>
  ? TField
  : ReturnType<typeof dbId>;

// Element schema with all reserved fields for **storage** (used when validating
// docs read back from Mongo and as the source for the MongoDB JSON Schema
// validator). `_type` is a strict literal here, not optional.
type StorageElementSchema<
  T extends ScopedMultiCollectionTypes,
  K extends keyof T,
  S extends AnySchema,
> = v.ObjectSchema<
  & {
    _id: DynId<T[K]["_id"]>;
    _type: v.LiteralSchema<K & string, AnyMessage>;
    _scope: S;
  }
  & T[K],
  // deno-lint-ignore no-explicit-any
  any
>;

// Element schema used for **insert validation** : _type is optional (auto-
// filled), _id is optional (auto-generated), _scope is supplied by the
// ScopedView before validation.
type InsertElementSchema<
  T extends ScopedMultiCollectionTypes,
  K extends keyof T,
  S extends AnySchema,
> = v.ObjectSchema<
  & {
    _id: DynId<T[K]["_id"]>;
    _type: v.OptionalSchema<v.LiteralSchema<K & string, AnyMessage>, () => K & string>;
    _scope: S;
  }
  & T[K],
  // deno-lint-ignore no-explicit-any
  any
>;

/** Input shape the user passes to insertOne (no _scope/_type). */
type UserInputDoc<T extends ScopedMultiCollectionTypes, K extends keyof T> =
  Omit<v.InferInput<InsertElementSchema<T, K, AnySchema>>, "_scope" | "_type">;

/** Output shape returned by reads. Always includes _scope. */
type OutputDoc<
  T extends ScopedMultiCollectionTypes,
  K extends keyof T,
  S extends AnySchema,
> = v.InferOutput<StorageElementSchema<T, K, S>>;

/** Union of every type's output shape — for cross-type (`*Any`) reads. */
type AnyScopedOutput<
  T extends ScopedMultiCollectionTypes,
  S extends AnySchema,
> = { [K in keyof T]: OutputDoc<T, K, S> }[keyof T];

// `OmitScopedMeta` / `ScopedMetaField` live in the dependency-free
// `./types.ts` (exported as `@diister/mongodbee/types`) so frontend code can
// use them without pulling the server-only ODM graph. Re-exported here for
// server-side convenience.
export type { OmitScopedMeta, ScopedMetaField } from "./types.ts";

/**
 * Allow `removeField()` (a symbol) anywhere in an update document, mirroring
 * `multiCollection.updateOne`. Recurses into nested objects so a field can be
 * removed at any depth.
 */
type DeepWithRemovable<X> = X extends Record<string, unknown>
  ? { [K in keyof X]: DeepWithRemovable<X[K]> | symbol }
  : X;
type WithRemovable<X> = { [K in keyof X]: DeepWithRemovable<X[K]> | symbol };

// -------- Views ----------------------------------------------------------

/**
 * Read+write view bound to one scope. All operations are automatically
 * narrowed to documents whose `_scope` matches the scope value passed to
 * {@link ScopedMultiCollectionResult.scope}.
 */
export type ScopedView<
  T extends ScopedMultiCollectionTypes,
  S extends AnySchema = AnySchema,
> = {
  /** The scope value this view is bound to. */
  readonly _scope: string;

  insertOne<K extends keyof T>(
    type: K,
    doc: UserInputDoc<T, K>,
  ): Promise<string>;

  insertMany<K extends keyof T>(
    type: K,
    docs: UserInputDoc<T, K>[],
  ): Promise<string[]>;

  getById<K extends keyof T>(
    type: K,
    id: string,
  ): Promise<OutputDoc<T, K, S>>;

  findOne<K extends keyof T>(
    type: K,
    filter?: m.Filter<OutputDoc<T, K, S>>,
  ): Promise<OutputDoc<T, K, S> | null>;

  find<K extends keyof T>(
    type: K,
    filter?: m.Filter<OutputDoc<T, K, S>>,
    options?: m.FindOptions,
  ): Promise<OutputDoc<T, K, S>[]>;

  /**
   * Find the first document matching a cross-type filter — no `_type`
   * constraint is injected, but the bound scope IS. Symmetric to
   * `multiCollection.findOneAny`. The caller may put `_type` in the filter
   * to branch across document types (e.g. a polymorphic existence check).
   */
  findOneAny(
    filter?: m.Filter<AnyScopedOutput<T, S>>,
  ): Promise<AnyScopedOutput<T, S> | null>;

  /**
   * Find all documents matching a cross-type filter — no `_type` constraint
   * injected, but scoped. Invalid docs are dropped (same posture as `find`).
   */
  findAny(
    filter?: m.Filter<AnyScopedOutput<T, S>>,
    options?: m.FindOptions,
  ): Promise<AnyScopedOutput<T, S>[]>;

  countDocuments<K extends keyof T>(
    type: K,
    filter?: m.Filter<OutputDoc<T, K, S>>,
    options?: m.CountDocumentsOptions,
  ): Promise<number>;

  deleteId<K extends keyof T>(type: K, id: string): Promise<number>;
  deleteIds<K extends keyof T>(type: K, ids: string[]): Promise<number>;
  deleteMany<K extends keyof T>(
    type: K,
    filter: m.Filter<OutputDoc<T, K, S>>,
  ): Promise<number>;

  updateOne<K extends keyof T>(
    type: K,
    id: string,
    doc: WithRemovable<Partial<UserInputDoc<T, K>>>,
  ): Promise<number>;

  updateMany(
    ops: {
      [K in keyof T]?: {
        [id: string]: WithRemovable<Partial<UserInputDoc<T, K>>>;
      };
    },
  ): Promise<number>;

  aggregate(
    stageBuilder: (stage: ScopedStageBuilder<T>) => AggregationStage[],
  // deno-lint-ignore no-explicit-any
  ): Promise<any[]>;

  paginate<K extends keyof T>(
    type: K,
    filter?: m.Filter<OutputDoc<T, K, S>>,
    options?: {
      limit?: number;
      afterId?: string;
      sort?: m.Sort | m.SortDirection;
    },
  ): Promise<{
    total: number;
    data: OutputDoc<T, K, S>[];
  }>;
};

/** Single MongoDB aggregation stage (already-built object form). */
export type AggregationStage = Record<string, unknown>;

/**
 * Stage builder injected into the user callback of
 * {@link ScopedView.aggregate}. Every helper produces a stage that respects
 * the bound scope ; in particular, `lookup` injects `_scope` + `_type` into
 * the joined sub-pipeline so cross-scope leakage is impossible.
 */
export type ScopedStageBuilder<T extends ScopedMultiCollectionTypes> = {
  match: <K extends keyof T>(
    type: K,
    filter: Record<string, unknown>,
  ) => AggregationStage;
  unwind: <K extends keyof T>(type: K, field: string) => AggregationStage;
  lookup: <K extends keyof T>(
    type: K,
    localField: string,
    foreignField: string,
    asOrOptions?: string | {
      as?: string;
      pipeline?: (stage: ScopedStageBuilder<T>) => AggregationStage[];
      let?: Record<string, unknown>;
    },
  ) => AggregationStage;
  /** Lookup ignoring `_type` ; still scope-bounded. */
  anyLookup: (
    localField: string,
    foreignField: string,
    asOrOptions?: string | {
      as?: string;
      pipeline?: (stage: ScopedStageBuilder<T>) => AggregationStage[];
      let?: Record<string, unknown>;
    },
  ) => AggregationStage;
  /** Lookup into an external collection ; no scope injection. */
  externalLookup: (
    fromCollection: string,
    localField: string,
    foreignField: string,
    asOrOptions?: string | {
      as?: string;
      pipeline?: AggregationStage[];
      let?: Record<string, unknown>;
    },
  ) => AggregationStage;
  project: (
    projection: Record<string, 1 | 0 | string | Record<string, unknown>>,
  ) => AggregationStage;
  addFields: (fields: Record<string, unknown>) => AggregationStage;
  group: (grouping: Record<string, unknown>) => AggregationStage;
  sort: (sort: Record<string, 1 | -1>) => AggregationStage;
  limit: (limit: number) => AggregationStage;
  skip: (skip: number) => AggregationStage;
};

/**
 * Read-only view across multiple scopes. Inserts/updates/deletes are
 * intentionally absent — to write, narrow to a single scope with
 * {@link ScopedMultiCollectionResult.scope}.
 */
export type ReadOnlyMultiScopeView<
  T extends ScopedMultiCollectionTypes,
  S extends AnySchema = AnySchema,
> = {
  readonly _scopes: readonly string[];

  findOne<K extends keyof T>(
    type: K,
    filter?: m.Filter<OutputDoc<T, K, S>>,
  ): Promise<OutputDoc<T, K, S> | null>;

  find<K extends keyof T>(
    type: K,
    filter?: m.Filter<OutputDoc<T, K, S>>,
    options?: m.FindOptions,
  ): Promise<OutputDoc<T, K, S>[]>;

  countDocuments<K extends keyof T>(
    type: K,
    filter?: m.Filter<OutputDoc<T, K, S>>,
    options?: m.CountDocumentsOptions,
  ): Promise<number>;

  aggregate(
    stageBuilder: (stage: ScopedStageBuilder<T>) => AggregationStage[],
  // deno-lint-ignore no-explicit-any
  ): Promise<any[]>;
};

/**
 * Read-only view spanning every scope in the collection. Off by default ;
 * enable via `{ allowUnscoped: true }` at construction time. Use sparingly :
 * any code path reaching `.unscoped` has bypassed scope safety.
 */
export type UnscopedView<
  T extends ScopedMultiCollectionTypes,
  S extends AnySchema = AnySchema,
> = ReadOnlyMultiScopeView<T, S>;

/** Public API surface returned by {@link scopedMultiCollection}. */
export type ScopedMultiCollectionResult<
  T extends ScopedMultiCollectionTypes,
  S extends AnySchema,
> = {
  /**
   * Return a read+write view bound to a single scope.
   * Throws synchronously if `id` is empty/null/undefined or does not validate
   * against the scope schema.
   */
  scope(id: v.InferInput<S>): ScopedView<T, S>;
  /**
   * Return a read-only view spanning multiple scopes. Inserts, updates and
   * deletes are intentionally absent from this surface to prevent
   * ambiguous-target writes.
   */
  scopes(ids: v.InferInput<S>[]): ReadOnlyMultiScopeView<T, S>;
  /**
   * Admin / cross-scope read-only view. Throws on access unless
   * `allowUnscoped: true` was set on the configuration.
   */
  readonly unscoped: UnscopedView<T, S>;

  // -------- Lifecycle ---------------------------------------------------

  /** Return the list of scope values present in the collection. */
  listScopes(): Promise<string[]>;

  /** `true` iff at least one document carries the given scope value. */
  scopeExists(id: v.InferInput<S>): Promise<boolean>;

  /**
   * Delete every document of a scope. Requires `{ confirm: true }` — the
   * scope-wide nature of this operation makes the safeguard worth the
   * extra keystroke.
   *
   * @returns Number of documents removed.
   */
  dropScope(
    id: v.InferInput<S>,
    options: { confirm: true },
  ): Promise<number>;

  /**
   * Aggregate counts per type for one scope. Returns `{ total: 0, byType: {} }`
   * when the scope has no documents.
   */
  scopeStats(id: v.InferInput<S>): Promise<{
    total: number;
    byType: Partial<Record<keyof T, number>>;
  }>;

  /**
   * Run `fn` inside a MongoDB transaction. All operations performed on any
   * view returned by this scopedMultiCollection (and on every other
   * collection sharing the same MongoClient) participate in the same
   * session.
   */
  withSession: ReturnType<typeof getSessionContext>["withSession"];

  /**
   * Drop the underlying MongoDB collection entirely. Destroys every scope
   * in one go ; requires `{ force: true }` for symmetry with
   * `multiCollection.drop`.
   */
  drop(options: { force: true }): Promise<boolean>;
};

// -------- Factory --------------------------------------------------------

/**
 * Create a scoped multi-collection.
 *
 * Documents are stored in a single MongoDB collection and partitioned by the
 * `_scope` discriminator. The API exposes scope-bound views so accidentally
 * crossing scope boundaries becomes a compile-time error rather than a
 * runtime bug.
 *
 * @example
 * ```typescript
 * import { scopedMultiCollection, refId } from "@diister/mongodbee";
 * import * as v from "@diister/mongodbee/schema";
 *
 * const catalog = await scopedMultiCollection(db, "catalog", {
 *   scope: refId("exposition"),
 *   types: {
 *     artwork: { title: v.string() },
 *     artist:  { name: v.string() },
 *   },
 * });
 *
 * const expo = catalog.scope("exposition:abc123");
 * const id  = await expo.insertOne("artwork", { title: "Mona Lisa" });
 * const all = await expo.find("artwork");
 * ```
 */
export async function scopedMultiCollection<
  const T extends ScopedMultiCollectionTypes,
  S extends AnySchema,
>(
  db: Db,
  collectionName: string,
  config: ScopedMultiCollectionConfig<T, S>,
): Promise<ScopedMultiCollectionResult<T, S>> {
  validateConfig(config);

  // Per-type schemas, used for insert validation. `_type` is optional with a
  // default of the type name so the user can omit it ; `_id` is optional via
  // dbId (auto-generated) ; `_scope` is required and supplied by the view.
  const insertSchemas = Object.entries(config.types).reduce(
    (acc, [typeName, fields]) => {
      acc[typeName] = v.object({
        _id: dbId(typeName),
        _type: v.optional(v.literal(typeName), () => typeName),
        _scope: config.scope,
        ...fields,
      });
      return acc;
    },
    // deno-lint-ignore no-explicit-any
    {} as Record<string, v.ObjectSchema<any, any>>,
  );

  // Storage schemas, used to build the MongoDB validator and to parse docs
  // read back from the database. `_type` is a strict literal here.
  const storageSchemas = Object.entries(config.types).reduce(
    (acc, [typeName, fields]) => {
      acc[typeName] = v.object({
        _id: dbId(typeName),
        _type: v.literal(typeName),
        _scope: config.scope,
        ...fields,
      });
      return acc;
    },
    // deno-lint-ignore no-explicit-any
    {} as Record<string, v.ObjectSchema<any, any>>,
  );

  const storageUnion = v.union(Object.values(storageSchemas));

  await applyValidator(db, collectionName, storageUnion);

  // deno-lint-ignore no-explicit-any
  const collection = db.collection<any>(collectionName);
  const sessionContext = getSessionContext(db.client);

  await applyScopedMultiCollectionIndexes(collection, storageSchemas, {
    queue: mongoOperationQueue,
  });

  function assertScopeValue(id: unknown): string {
    if (id === null || id === undefined || id === "") {
      throw new Error(
        "scope(): scope value must be a non-empty string ; received " +
          (id === "" ? "empty string" : String(id)),
      );
    }
    try {
      v.parse(config.scope, id);
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      throw new Error(
        `scope(): value "${String(id)}" does not validate against the ` +
          `configured scope schema: ${detail}`,
      );
    }
    return String(id);
  }

  function assertNoReservedFields(doc: Record<string, unknown>) {
    for (const reserved of RESERVED_FIELDS) {
      if (reserved in doc) {
        throw new Error(
          `Cannot pass "${reserved}" in an insertOne/insertMany/updateOne ` +
            `document — it is injected automatically by the scoped view.`,
        );
      }
    }
  }

  function buildScopedView(scopeId: string): ScopedView<T, S> {
    return {
      _scope: scopeId,

      async insertOne(type, doc) {
        const typeName = type as string;
        const record = doc as Record<string, unknown>;
        assertNoReservedFields(record);

        const schema = insertSchemas[typeName];
        const parsed = v.parse(schema, {
          ...record,
          // Auto-mint `_id` when the caller omits it, mirroring the
          // multiCollection contract. The per-type `_id` schema is often a
          // bare `refId(type)` (required, no default), so we cannot rely on a
          // `dbId` default here — without this, inserting such a type would
          // throw "Expected _id but received undefined".
          _id: record._id ?? `${typeName}:${newId()}`,
          _scope: scopeId,
        });

        const safeDoc = sanitizeForMongoDB(parsed, {
          undefinedBehavior: "remove",
          deep: true,
          // deno-lint-ignore no-explicit-any
        }) as any;

        const session = sessionContext.getSession();
        const result = await collection.insertOne(safeDoc, { session });
        if (!result.acknowledged) throw new Error("Insert failed");
        return result.insertedId as unknown as string;
      },

      async insertMany(type, docs) {
        const typeName = type as string;
        const schema = insertSchemas[typeName];

        const parsed = docs.map((d) => {
          const record = d as Record<string, unknown>;
          assertNoReservedFields(record);
          return v.parse(schema, {
            ...record,
            _id: record._id ?? `${typeName}:${newId()}`,
            _scope: scopeId,
          });
        });

        const safeDocs = parsed.map((p) =>
          sanitizeForMongoDB(p, {
            undefinedBehavior: "remove",
            deep: true,
            // deno-lint-ignore no-explicit-any
          }) as any
        );

        const session = sessionContext.getSession();
        const result = await collection.insertMany(safeDocs, { session });
        if (!result.acknowledged) throw new Error("Insert failed");
        return Object.values(result.insertedIds) as unknown as string[];
      },

      async getById(type, id) {
        const typeName = type as string;
        const session = sessionContext.getSession();
        const raw = await collection.findOne({
          _id: id,
          _type: typeName,
          _scope: scopeId,
        // deno-lint-ignore no-explicit-any
        } as any, { session });
        if (!raw) {
          throw new Error(
            `getById(${typeName}, ${id}): no element found in scope "${scopeId}"`,
          );
        }
        // deno-lint-ignore no-explicit-any
        return v.parse(storageSchemas[typeName], raw) as any;
      },

      async findOne(type, filter) {
        const typeName = type as string;
        const session = sessionContext.getSession();
        const conditions: Record<string, unknown>[] = [
          { _type: typeName },
          { _scope: scopeId },
        ];
        if (filter) conditions.push(filter as Record<string, unknown>);

        // deno-lint-ignore no-explicit-any
        const raw = await collection.findOne({ $and: conditions } as any, {
          session,
        });
        if (!raw) return null;
        // deno-lint-ignore no-explicit-any
        return v.parse(storageSchemas[typeName], raw) as any;
      },

      async find(type, filter, options) {
        const typeName = type as string;
        const session = sessionContext.getSession();
        const conditions: Record<string, unknown>[] = [
          { _type: typeName },
          { _scope: scopeId },
        ];
        if (filter) conditions.push(filter as Record<string, unknown>);

        // deno-lint-ignore no-explicit-any
        const cursor = collection.find({ $and: conditions } as any, {
          session,
          ...options,
        });
        const raw = await cursor.toArray();
        const out: unknown[] = [];
        for (const item of raw) {
          const parsed = v.safeParse(storageSchemas[typeName], item);
          if (parsed.success) out.push(parsed.output);
        }
        // deno-lint-ignore no-explicit-any
        return out as any;
      },

      async findOneAny(filter) {
        const session = sessionContext.getSession();
        const conditions: Record<string, unknown>[] = [{ _scope: scopeId }];
        if (filter) conditions.push(filter as Record<string, unknown>);
        // deno-lint-ignore no-explicit-any
        const raw = await collection.findOne({ $and: conditions } as any, {
          session,
        });
        if (!raw) return null;
        // deno-lint-ignore no-explicit-any
        return v.parse(storageUnion, raw) as any;
      },

      async findAny(filter, options) {
        const session = sessionContext.getSession();
        const conditions: Record<string, unknown>[] = [{ _scope: scopeId }];
        if (filter) conditions.push(filter as Record<string, unknown>);
        const cursor = collection.find(
          // deno-lint-ignore no-explicit-any
          { $and: conditions } as any,
          { session, ...options },
        );
        const raw = await cursor.toArray();
        const out: unknown[] = [];
        for (const item of raw) {
          const parsed = v.safeParse(storageUnion, item);
          if (parsed.success) out.push(parsed.output);
        }
        // deno-lint-ignore no-explicit-any
        return out as any;
      },

      countDocuments(type, filter, options) {
        const typeName = type as string;
        const session = sessionContext.getSession();
        const conditions: Record<string, unknown>[] = [
          { _type: typeName },
          { _scope: scopeId },
        ];
        if (filter) conditions.push(filter as Record<string, unknown>);
        return collection.countDocuments(
          // deno-lint-ignore no-explicit-any
          { $and: conditions } as any,
          { session, ...options },
        );
      },

      async deleteId(type, id) {
        const typeName = type as string;
        const session = sessionContext.getSession();
        const result = await collection.deleteOne({
          _id: id,
          _type: typeName,
          _scope: scopeId,
        // deno-lint-ignore no-explicit-any
        } as any, { session });
        if (!result.acknowledged) throw new Error("Delete failed");
        if (result.deletedCount === 0) {
          throw new Error(
            `deleteId(${typeName}, ${id}): no element found in scope "${scopeId}"`,
          );
        }
        return result.deletedCount;
      },

      async deleteIds(type, ids) {
        const typeName = type as string;
        const session = sessionContext.getSession();
        const result = await collection.deleteMany({
          _id: { $in: ids },
          _type: typeName,
          _scope: scopeId,
        // deno-lint-ignore no-explicit-any
        } as any, { session });
        if (!result.acknowledged) throw new Error("Delete failed");
        return result.deletedCount;
      },

      async deleteMany(type, filter) {
        const typeName = type as string;
        const session = sessionContext.getSession();
        const result = await collection.deleteMany({
          ...(filter as Record<string, unknown>),
          _type: typeName,
          _scope: scopeId,
        // deno-lint-ignore no-explicit-any
        } as any, { session });
        if (!result.acknowledged) throw new Error("Delete failed");
        return result.deletedCount;
      },

      async updateOne(type, id, doc) {
        const typeName = type as string;
        assertNoReservedFields(doc as Record<string, unknown>);

        // Split out removeField() symbols → $unset, the rest → $set.
        const { set, unset } = extractFieldsToRemove(
          doc as Record<string, unknown>,
        );
        const updateOps = buildUpdateOps(set, unset);
        if (Object.keys(updateOps).length === 0) return 0;

        return retryOnWriteConflict(async () => {
          const session = sessionContext.getSession();
          const result = await collection.updateOne({
            _id: id,
            _type: typeName,
            _scope: scopeId,
          // deno-lint-ignore no-explicit-any
          // deno-lint-ignore no-explicit-any
          } as any, updateOps as any, { session });
          if (!result.acknowledged) throw new Error("Update failed");
          if (result.matchedCount === 0) {
            throw new Error(
              `updateOne(${typeName}, ${id}): no element found in scope "${scopeId}"`,
            );
          }
          return result.modifiedCount;
        });
      },

      async updateMany(ops) {
        const bulkOps: m.AnyBulkWriteOperation[] = [];
        for (const typeName in ops) {
          const items = ops[typeName as keyof T];
          if (!items) continue;
          for (const id in items) {
            const partial = items[id];
            if (!partial) continue;
            assertNoReservedFields(partial as Record<string, unknown>);
            const { set, unset } = extractFieldsToRemove(
              partial as Record<string, unknown>,
            );
            const updateOps = buildUpdateOps(set, unset);
            if (Object.keys(updateOps).length === 0) continue;
            bulkOps.push({
              updateOne: {
                filter: {
                  _id: id,
                  _type: typeName,
                  _scope: scopeId,
                  // deno-lint-ignore no-explicit-any
                } as any,
                update: updateOps,
              },
            });
          }
        }
        if (bulkOps.length === 0) return 0;

        return retryOnWriteConflict(async () => {
          const session = sessionContext.getSession();
          const result = await collection.bulkWrite(bulkOps, { session });
          return result.modifiedCount;
        });
      },

      async aggregate(stageBuilder) {
        const stage = buildScopedStageBuilder<T>(collectionName, {
          kind: "single",
          id: scopeId,
        });
        const userPipeline = stageBuilder(stage);
        // First stage always narrows to the bound scope. All subsequent
        // stages operate on the scoped subset only.
        const pipeline: AggregationStage[] = [
          { $match: { _scope: scopeId } },
          ...userPipeline,
        ];
        const session = sessionContext.getSession();
        const cursor = collection.aggregate(pipeline, { session });
        return await cursor.toArray();
      },

      async paginate(type, filter, options) {
        const typeName = type as string;
        const limit = options?.limit ?? 100;
        const afterId = options?.afterId;
        const sortInput = options?.sort ?? { _id: 1 };

        const sortObj: Record<string, 1 | -1> =
          typeof sortInput === "object" && !Array.isArray(sortInput)
            ? { ...(sortInput as Record<string, 1 | -1>) }
            : {
              _id: sortInput === 1 || sortInput === "asc" ||
                  sortInput === "ascending"
                ? 1
                : -1,
            };
        if (!("_id" in sortObj)) sortObj._id = 1;

        const session = sessionContext.getSession();

        const baseQuery: Record<string, unknown>[] = [
          { _scope: scopeId },
          { _type: typeName },
        ];
        if (filter) baseQuery.push(filter as Record<string, unknown>);

        const total = await collection.countDocuments(
          // deno-lint-ignore no-explicit-any
          { $and: baseQuery } as any,
          { session },
        );

        let cursorFilter: Record<string, unknown> | null = null;
        if (afterId) {
          if (!afterId.startsWith(`${typeName}:`)) {
            throw new Error(
              `paginate: invalid afterId format — expected "${typeName}:..." prefix`,
            );
          }
          const anchor = await collection.findOne(
            // deno-lint-ignore no-explicit-any
            { _id: afterId, _scope: scopeId, _type: typeName } as any,
            { session },
          );
          if (anchor) {
            const sortFields = Object.keys(sortObj);
            if (
              sortFields.length === 1 && sortFields[0] === "_id"
            ) {
              const op = sortObj._id === 1 ? "$gt" : "$lt";
              cursorFilter = { _id: { [op]: afterId } };
            } else {
              const conditions: Record<string, unknown>[] = [];
              for (let i = 0; i < sortFields.length; i++) {
                const f = sortFields[i];
                const condition: Record<string, unknown> = {};
                for (let j = 0; j < i; j++) {
                  const prev = sortFields[j];
                  condition[prev] = (anchor as Record<string, unknown>)[prev];
                }
                const op = sortObj[f] === 1 ? "$gt" : "$lt";
                condition[f] = {
                  [op]: (anchor as Record<string, unknown>)[f],
                };
                conditions.push(condition);
              }
              cursorFilter = { $or: conditions };
            }
          }
        }

        const finalQuery = cursorFilter
          ? { $and: [...baseQuery, cursorFilter] }
          : { $and: baseQuery };

        const docs = await collection
          // deno-lint-ignore no-explicit-any
          .find(finalQuery as any, { session })
          .sort(sortObj as m.Sort)
          .limit(limit)
          .toArray();

        const data: unknown[] = [];
        for (const d of docs) {
          const parsed = v.safeParse(storageSchemas[typeName], d);
          if (parsed.success) data.push(parsed.output);
        }

        // deno-lint-ignore no-explicit-any
        return { total, data: data as any };
      },
    };
  }

  function buildReadOnlyView(
    scopeIds: string[] | null,
  ): ReadOnlyMultiScopeView<T, S> {
    // scopeIds: array of scope ids (may be empty), or null for unscoped.
    const filter: ScopeFilterShape = scopeIds === null
      ? null
      : scopeIds.length === 1
      ? { kind: "single", id: scopeIds[0] }
      : { kind: "multi", ids: scopeIds };

    function scopeMatch(): Record<string, unknown> | null {
      if (scopeIds === null) return null;
      if (scopeIds.length === 0) return { _scope: { $in: [] } };
      if (scopeIds.length === 1) return { _scope: scopeIds[0] };
      return { _scope: { $in: scopeIds } };
    }

    return {
      _scopes: scopeIds === null ? [] : [...scopeIds],

      async findOne(type, userFilter) {
        const typeName = type as string;
        const session = sessionContext.getSession();
        const conditions: Record<string, unknown>[] = [{ _type: typeName }];
        const sm = scopeMatch();
        if (sm) conditions.push(sm);
        if (userFilter) conditions.push(userFilter as Record<string, unknown>);

        // deno-lint-ignore no-explicit-any
        const raw = await collection.findOne({ $and: conditions } as any, {
          session,
        });
        if (!raw) return null;
        // deno-lint-ignore no-explicit-any
        return v.parse(storageSchemas[typeName], raw) as any;
      },

      async find(type, userFilter, options) {
        const typeName = type as string;
        const session = sessionContext.getSession();
        const conditions: Record<string, unknown>[] = [{ _type: typeName }];
        const sm = scopeMatch();
        if (sm) conditions.push(sm);
        if (userFilter) conditions.push(userFilter as Record<string, unknown>);

        const cursor = collection.find(
          // deno-lint-ignore no-explicit-any
          { $and: conditions } as any,
          { session, ...options },
        );
        const raw = await cursor.toArray();
        const out: unknown[] = [];
        for (const item of raw) {
          const parsed = v.safeParse(storageSchemas[typeName], item);
          if (parsed.success) out.push(parsed.output);
        }
        // deno-lint-ignore no-explicit-any
        return out as any;
      },

      countDocuments(type, userFilter, options) {
        const typeName = type as string;
        const session = sessionContext.getSession();
        const conditions: Record<string, unknown>[] = [{ _type: typeName }];
        const sm = scopeMatch();
        if (sm) conditions.push(sm);
        if (userFilter) conditions.push(userFilter as Record<string, unknown>);

        return collection.countDocuments(
          // deno-lint-ignore no-explicit-any
          { $and: conditions } as any,
          { session, ...options },
        );
      },

      async aggregate(stageBuilder) {
        const stage = buildScopedStageBuilder<T>(collectionName, filter);
        const userPipeline = stageBuilder(stage);
        const pipeline: AggregationStage[] = [];
        const sm = scopeMatch();
        if (sm) pipeline.push({ $match: sm });
        pipeline.push(...userPipeline);
        const session = sessionContext.getSession();
        const cursor = collection.aggregate(pipeline, { session });
        return await cursor.toArray();
      },
    };
  }

  const unscopedView: UnscopedView<T, S> = config.allowUnscoped
    ? buildReadOnlyView(null)
    : new Proxy({} as UnscopedView<T, S>, {
      get(_target, prop) {
        if (prop === "_scopes") return [];
        throw new Error(
          `unscoped: access to "${
            String(prop)
          }" is disabled. Set { allowUnscoped: true } on the ` +
            `scopedMultiCollection config to enable cross-scope reads.`,
        );
      },
    });

  return {
    scope(id) {
      const validated = assertScopeValue(id);
      return buildScopedView(validated);
    },
    scopes(ids) {
      const validated = ids.map((id) => assertScopeValue(id));
      return buildReadOnlyView(validated);
    },
    unscoped: unscopedView,

    async listScopes() {
      const session = sessionContext.getSession();
      const values = await collection.distinct("_scope", {}, { session });
      return values.filter((v): v is string => typeof v === "string");
    },

    async scopeExists(id) {
      const validated = assertScopeValue(id);
      const session = sessionContext.getSession();
      const count = await collection.countDocuments(
        // deno-lint-ignore no-explicit-any
        { _scope: validated } as any,
        { session, limit: 1 },
      );
      return count > 0;
    },

    async dropScope(id, options) {
      if (!options || options.confirm !== true) {
        throw new Error(
          "dropScope() requires { confirm: true } to proceed — the operation " +
            "deletes every document of the scope and is irreversible.",
        );
      }
      const validated = assertScopeValue(id);
      const session = sessionContext.getSession();
      const result = await collection.deleteMany(
        // deno-lint-ignore no-explicit-any
        { _scope: validated } as any,
        { session },
      );
      if (!result.acknowledged) throw new Error("dropScope: delete failed");
      return result.deletedCount;
    },

    async scopeStats(id) {
      const validated = assertScopeValue(id);
      const session = sessionContext.getSession();
      const cursor = collection.aggregate(
        [
          { $match: { _scope: validated } },
          { $group: { _id: "$_type", count: { $sum: 1 } } },
        ],
        { session },
      );
      const groups = await cursor.toArray();
      const byType: Record<string, number> = {};
      let total = 0;
      for (const g of groups as { _id: string; count: number }[]) {
        byType[g._id] = g.count;
        total += g.count;
      }
      // deno-lint-ignore no-explicit-any
      return { total, byType: byType as any };
    },

    withSession: sessionContext.withSession,

    async drop(options) {
      if (!options?.force) {
        throw new Error(
          "drop() requires { force: true } to proceed — the operation " +
            "deletes the underlying collection and all its data.",
        );
      }
      const session = sessionContext.getSession();
      return await collection.drop({ session });
    },
  };
}

// -------- Stage builder (aggregate) -------------------------------------

/**
 * Build the `_scope` constraint to inject into lookup sub-pipelines.
 * - `null` means no scope filter (unscoped admin view)
 * - single id → `$eq` on `_scope`
 * - multiple ids → `$in` on `_scope`
 */
type ScopeFilterShape =
  | null
  | { kind: "single"; id: string }
  | { kind: "multi"; ids: string[] };

function scopeExpr(filter: ScopeFilterShape): AggregationStage | null {
  if (!filter) return null;
  if (filter.kind === "single") {
    return { $eq: ["$_scope", filter.id] };
  }
  return { $in: ["$_scope", filter.ids] };
}

/**
 * Build a stage builder bound to a scope filter. Lookups inject the same
 * scope constraint into the joined sub-pipeline so cross-scope leakage is
 * structurally impossible (single scope) or limited to the declared set
 * (multi-scope view).
 *
 * When `scopeFilter` is `null` (unscoped admin), no `_scope` constraint is
 * added — every doc is reachable.
 */
function buildScopedStageBuilder<T extends ScopedMultiCollectionTypes>(
  collectionName: string,
  scopeFilter: ScopeFilterShape,
): ScopedStageBuilder<T> {
  const scopeMatchExpr = scopeExpr(scopeFilter);
  const stage: ScopedStageBuilder<T> = {
    match: (type, filter) => ({
      $match: {
        _type: type as string,
        ...filter,
      },
    }),
    unwind: (_type, field) => ({ $unwind: `$${field}` }),
    lookup: (type, localField, foreignField, asOrOptions) => {
      const typeName = type as string;
      const exprs: AggregationStage[] = [
        { $eq: [`$${foreignField}`, "$$localValue"] },
        { $eq: ["$_type", typeName] },
      ];
      if (scopeMatchExpr) exprs.push(scopeMatchExpr);

      if (typeof asOrOptions === "string") {
        return {
          $lookup: {
            from: collectionName,
            let: { localValue: `$${localField}` },
            pipeline: [{ $match: { $expr: { $and: exprs } } }],
            as: asOrOptions,
          },
        };
      }
      const options = asOrOptions || {};
      const as = options.as || localField;
      const basePipeline: AggregationStage[] = [
        { $match: { $expr: { $and: exprs } } },
      ];
      if (options.pipeline) {
        basePipeline.push(...options.pipeline(stage));
      }
      return {
        $lookup: {
          from: collectionName,
          let: { localValue: `$${localField}`, ...(options.let || {}) },
          pipeline: basePipeline,
          as,
        },
      };
    },
    anyLookup: (localField, foreignField, asOrOptions) => {
      const exprs: AggregationStage[] = [
        { $eq: [`$${foreignField}`, "$$localValue"] },
      ];
      if (scopeMatchExpr) exprs.push(scopeMatchExpr);

      if (typeof asOrOptions === "string") {
        return {
          $lookup: {
            from: collectionName,
            let: { localValue: `$${localField}` },
            pipeline: [{ $match: { $expr: { $and: exprs } } }],
            as: asOrOptions,
          },
        };
      }
      const options = asOrOptions || {};
      const as = options.as || localField;
      const basePipeline: AggregationStage[] = [
        { $match: { $expr: { $and: exprs } } },
      ];
      if (options.pipeline) {
        basePipeline.push(...options.pipeline(stage));
      }
      return {
        $lookup: {
          from: collectionName,
          let: { localValue: `$${localField}`, ...(options.let || {}) },
          pipeline: basePipeline,
          as,
        },
      };
    },
    externalLookup: (fromCollection, localField, foreignField, asOrOptions) => {
      if (typeof asOrOptions === "string") {
        return {
          $lookup: {
            from: fromCollection,
            localField,
            foreignField,
            as: asOrOptions,
          },
        };
      }
      const options = asOrOptions || {};
      const as = options.as || localField;
      const stageObj: Record<string, unknown> = {
        from: fromCollection,
        localField,
        foreignField,
        as,
      };
      if (options.let) stageObj.let = options.let;
      if (options.pipeline) stageObj.pipeline = options.pipeline;
      return { $lookup: stageObj };
    },
    project: (projection) => ({ $project: projection }),
    addFields: (fields) => ({ $addFields: fields }),
    group: (grouping) => ({ $group: grouping }),
    sort: (sortSpec) => ({ $sort: sortSpec }),
    limit: (l) => ({ $limit: l }),
    skip: (s) => ({ $skip: s }),
  };
  return stage;
}

// -------- Internal helpers ----------------------------------------------

/**
 * Build a Mongo update document from a `{ set, unset }` split (as produced
 * by {@link extractFieldsToRemove}). `set` values are sanitized ; `unset`
 * keys become a `$unset`. Returns `{}` when there is nothing to do.
 */
function buildUpdateOps(
  set: Record<string, unknown>,
  unset: Record<string, unknown>,
): Record<string, unknown> {
  const sanitized = sanitizeForMongoDB(set, {
    undefinedBehavior: "remove",
    deep: true,
  }) as Record<string, unknown>;
  const ops: Record<string, unknown> = {};
  if (Object.keys(sanitized).length > 0) ops.$set = sanitized;
  if (Object.keys(unset).length > 0) ops.$unset = unset;
  return ops;
}

function validateConfig<
  T extends ScopedMultiCollectionTypes,
  S extends AnySchema,
>(config: ScopedMultiCollectionConfig<T, S>): void {
  if (!config.scope) {
    throw new Error("scopedMultiCollection: `scope` schema is required");
  }

  const typeNames = Object.keys(config.types);
  if (typeNames.length === 0) {
    throw new Error(
      "scopedMultiCollection: `types` must define at least one type",
    );
  }

  for (const typeName of typeNames) {
    const fields = config.types[typeName];
    for (const fieldName of Object.keys(fields)) {
      if (RESERVED_FIELDS.has(fieldName)) {
        throw new Error(
          `scopedMultiCollection: field name "${fieldName}" is reserved ` +
            `and cannot appear in type "${typeName}". ` +
            `Reserved fields are: ${[...RESERVED_FIELDS].join(", ")}.`,
        );
      }
    }
  }
}

async function applyValidator(
  db: Db,
  collectionName: string,
  unionSchema: AnySchema,
): Promise<void> {
  const validator = toMongoValidator(unionSchema);
  const collections = await db.listCollections({ name: collectionName })
    .toArray();

  if (collections.length === 0) {
    log.debug(`applyValidator(${collectionName}): createCollection`);
    await db.createCollection(collectionName, { validator });
    return;
  }

  const existingOptions = await db.command({
    listCollections: 1,
    filter: { name: collectionName },
  });
  const currentValidator =
    existingOptions.cursor?.firstBatch?.[0]?.options?.validator || {};

  if (dirtyEquivalent(currentValidator, validator)) {
    log.debug(`applyValidator(${collectionName}): unchanged`);
    return;
  }

  log.debug(`applyValidator(${collectionName}): collMod`);
  await db.command({ collMod: collectionName, validator });
}

// Internal — exported so tests can audit reserved-field handling.
export const __INTERNAL__ = { RESERVED_FIELDS } as const;

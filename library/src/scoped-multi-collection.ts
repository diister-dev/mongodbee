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
import { createDotNotationSchema, getNestedValue } from "./dot-notation.ts";
import { retryOnWriteConflict } from "./utils/retry.ts";
import { dirtyEquivalent } from "./utils/object.ts";
import { createLogger } from "./utils/logger.ts";
import { applyScopedMultiCollectionIndexes } from "./indexes-applier.ts";
import { mongoOperationQueue } from "./operation.ts";
import {
  createOperationTracer,
  errorWithSafeMessage,
  filterKeys,
  type OpContext,
  registerClientTelemetry,
  TELEMETRY_ATTRIBUTES as TA,
  type TelemetryOptions,
  traced,
} from "./telemetry.ts";

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
  /** Opt-in OpenTelemetry tracing for this collection's operations. */
  telemetry?: TelemetryOptions;
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
    _type: v.OptionalSchema<
      v.LiteralSchema<K & string, AnyMessage>,
      () => K & string
    >;
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
    options?: m.FindOptions & { validate?: boolean },
  ): Promise<OutputDoc<T, K, S>[]>;

  /**
   * Projected read: return only the listed fields, plus the meta fields
   * (`_id`/`_type`/`_scope`). A distinct method — not a flag on `find` —
   * because the result contract is different: documents are PARTIAL and
   * UNVALIDATED by construction (you can't validate a subset against the full
   * type schema). It cuts BSON deserialization, the dominant cost of a large
   * read — measured ~2.3× faster than a full validated `find`. Use it when you
   * need a few fields from many documents.
   */
  findProject<K extends keyof T, P extends keyof OutputDoc<T, K, S>>(
    type: K,
    fields: readonly P[],
    filter?: m.Filter<OutputDoc<T, K, S>>,
    options?: m.FindOptions,
  ): Promise<Pick<OutputDoc<T, K, S>, P | "_id" | "_type" | "_scope">[]>;

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
    options?: m.FindOptions & { validate?: boolean },
  ): Promise<AnyScopedOutput<T, S>[]>;

  countDocuments<K extends keyof T>(
    type: K,
    filter?: m.Filter<OutputDoc<T, K, S>>,
    options?: m.CountDocumentsOptions,
  ): Promise<number>;

  deleteId<K extends keyof T>(type: K, id: string): Promise<number>;
  /**
   * Delete several ids at once within the bound scope ; returns the number of
   * documents removed.
   *
   * Deliberate divergence from `multiCollection.deleteIds`: this does NOT throw
   * when `deletedCount === 0`. A scoped batch delete where some (or all) ids
   * belong to another scope — and thus match nothing here — is a normal
   * outcome, so an empty delete returns `0` rather than raising.
   */
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

  /**
   * Apply per-id partial updates across one or more types, all within the
   * bound scope ; returns the total number of documents modified.
   *
   * Deliberate divergence from `multiCollection.updateMany`: this returns `0`
   * (and does NOT throw) when nothing matched — an empty batch, or a batch
   * whose ids all fall outside this scope, is a normal outcome here rather than
   * an error condition.
   */
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

  paginate<K extends keyof T, EN = OutputDoc<T, K, S>, R = EN>(
    type: K,
    filter?: m.Filter<OutputDoc<T, K, S>>,
    options?: {
      limit?: number;
      afterId?: string;
      beforeId?: string;
      sort?: m.Sort | m.SortDirection;
      /** Scope-safe pipeline stages run server-side before pagination (lookups, addFields, …). */
      pipeline?: (stage: ScopedStageBuilder<T>) => AggregationStage[];
      prepare?: (doc: OutputDoc<T, K, S>) => Promise<EN> | EN;
      filter?: (doc: EN) => Promise<boolean> | boolean;
      format?: (doc: EN) => Promise<R> | R;
      /** Skip the countDocuments call(s); `total` and `position` come back undefined. */
      skipTotal?: boolean;
      /** Fetch one extra row to set `hasMore` cheaply; the extra row is dropped. */
      peek?: boolean;
    },
  ): Promise<{
    /** Total docs matching the (scoped) query — omitted when `skipTotal`. */
    total?: number;
    /** 0-based count of docs before this page's first row — omitted when `skipTotal`. */
    position?: number;
    data: R[];
    /** Present only when `peek` was requested. */
    hasMore?: boolean;
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
    options?: m.FindOptions & { validate?: boolean },
  ): Promise<OutputDoc<T, K, S>[]>;

  /**
   * Projected read across the view's scopes — returns the listed fields plus
   * `_id`/`_type`/`_scope`. Partial + unvalidated by construction; cuts
   * deserialization cost. See {@link ScopedView.findProject}.
   */
  findProject<K extends keyof T, P extends keyof OutputDoc<T, K, S>>(
    type: K,
    fields: readonly P[],
    filter?: m.Filter<OutputDoc<T, K, S>>,
    options?: m.FindOptions,
  ): Promise<Pick<OutputDoc<T, K, S>, P | "_id" | "_type" | "_scope">[]>;

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

  // Dot-notation schemas, one per type, used to validate the `$set` paths of
  // updateOne/updateMany. Same mechanism as `multiCollection` (see
  // `createDotNotationSchema`) : a dotted update key (`a.b.c`) is validated
  // against the matching leaf schema — running the schema's pipe checks and
  // transforms — instead of being deferred to Mongo's opaque
  // "Document failed validation". Built from the per-type insert schemas.
  const dotSchemaElements = Object.entries(insertSchemas).reduce(
    (acc, [typeName, schema]) => {
      acc[typeName] = createDotNotationSchema(schema);
      return acc;
    },
    // deno-lint-ignore no-explicit-any
    {} as Record<string, v.BaseSchema<any, any, any>>,
  );

  await applyValidator(db, collectionName, storageUnion);

  // deno-lint-ignore no-explicit-any
  const collection = db.collection<any>(collectionName);
  const sessionContext = getSessionContext(db.client);

  const tele = createOperationTracer(config.telemetry, {
    dbName: db.databaseName,
    collectionName,
    getSession: () => sessionContext.getSession(),
  });
  registerClientTelemetry(db.client, config.telemetry);

  // Whether spans carry the `mongodbee.scope` attribute. Disabled for
  // deployments whose scope values are PII-bearing (e.g. emails) : when
  // `false`, every scope attribute is set to `undefined`, which `prune()`
  // drops before the value ever reaches the SDK.
  const recordScope = config.telemetry?.recordScope !== false;

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
    let parsed: unknown;
    try {
      parsed = v.parse(config.scope, id);
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      throw new Error(
        `scope(): value "${String(id)}" does not validate against the ` +
          `configured scope schema: ${detail}`,
      );
    }
    // Return the parse OUTPUT, not the raw input : if the scope schema
    // transforms (trim/lowercase/…), inserts store the transformed `_scope`
    // (they parse through `insertSchemas`), so every view filter MUST use that
    // same transformed value — otherwise documents become invisible to the
    // very view that inserted them.
    return String(parsed);
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
        const run = async () => {
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
        };
        return traced(
          tele,
          "insertOne",
          () => ({
            [TA.SCOPE]: recordScope ? scopeId : undefined,
            [TA.DOC_TYPE]: String(type),
          }),
          run,
          () => ({ [TA.INSERTED_COUNT]: 1 }),
        );
      },

      async insertMany(type, docs) {
        const run = async () => {
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
        };
        return traced(
          tele,
          "insertMany",
          () => ({
            [TA.SCOPE]: recordScope ? scopeId : undefined,
            [TA.DOC_TYPE]: String(type),
            [TA.BATCH_SIZE]: docs.length,
          }),
          run,
          (ids) => ({ [TA.INSERTED_COUNT]: ids.length }),
        );
      },

      async getById(type, id) {
        const run = async () => {
          const typeName = type as string;
          const session = sessionContext.getSession();
          const raw = await collection.findOne({
            _id: id,
            _type: typeName,
            _scope: scopeId,
            // deno-lint-ignore no-explicit-any
          } as any, { session });
          if (!raw) {
            throw errorWithSafeMessage(
              `getById(${typeName}, ${id}): no element found in scope "${scopeId}"`,
              `getById(${typeName}): no element found in scope`,
            );
          }
          // deno-lint-ignore no-explicit-any
          return v.parse(storageSchemas[typeName], raw) as any;
        };
        return traced(tele, "getById", () => ({
          [TA.SCOPE]: recordScope ? scopeId : undefined,
          [TA.DOC_TYPE]: String(type),
          [TA.FILTER_KEYS]: "_id",
        }), run);
      },

      async findOne(type, filter) {
        const run = async () => {
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
        };
        return traced(tele, "findOne", () => ({
          [TA.SCOPE]: recordScope ? scopeId : undefined,
          [TA.DOC_TYPE]: String(type),
          [TA.FILTER_KEYS]: filterKeys(filter),
        }), run);
      },

      async find(type, filter, options) {
        const run = async () => {
          const typeName = type as string;
          const session = sessionContext.getSession();
          const { validate = true, ...findOptions } = options ?? {};
          const conditions: Record<string, unknown>[] = [
            { _type: typeName },
            { _scope: scopeId },
          ];
          if (filter) conditions.push(filter as Record<string, unknown>);

          // deno-lint-ignore no-explicit-any
          const cursor = collection.find({ $and: conditions } as any, {
            session,
            ...findOptions,
          });
          const raw = await cursor.toArray();
          // `validate: false` skips the per-document parse for trusted hot-path
          // reads, returning the raw stored docs. Schema transforms are NOT
          // applied in that mode — opt out only when you don't depend on them.
          if (validate === false) {
            // deno-lint-ignore no-explicit-any
            return raw as any;
          }
          const out: unknown[] = [];
          for (const item of raw) {
            const parsed = v.safeParse(storageSchemas[typeName], item);
            if (parsed.success) out.push(parsed.output);
          }
          // deno-lint-ignore no-explicit-any
          return out as any;
        };
        return traced(
          tele,
          "find",
          () => ({
            [TA.SCOPE]: recordScope ? scopeId : undefined,
            [TA.DOC_TYPE]: String(type),
            [TA.FILTER_KEYS]: filterKeys(filter),
          }),
          run,
          (docs) => ({ [TA.RETURNED_ROWS]: docs.length }),
        );
      },

      async findProject(type, fields, filter, options) {
        const run = async () => {
          const typeName = type as string;
          const session = sessionContext.getSession();
          const conditions: Record<string, unknown>[] = [
            { _type: typeName },
            { _scope: scopeId },
          ];
          if (filter) conditions.push(filter as Record<string, unknown>);
          const cursor = collection.find(
            // deno-lint-ignore no-explicit-any
            { $and: conditions } as any,
            {
              session,
              ...options,
              projection: buildProjection(fields as readonly string[]),
            },
          );
          // Projected docs are partial — return them raw. Validating against
          // the full type schema would reject the omitted fields.
          // deno-lint-ignore no-explicit-any
          return (await cursor.toArray()) as any;
        };
        return traced(
          tele,
          "findProject",
          () => ({
            [TA.SCOPE]: recordScope ? scopeId : undefined,
            [TA.DOC_TYPE]: String(type),
            [TA.FILTER_KEYS]: filterKeys(filter),
          }),
          run,
          (docs) => ({ [TA.RETURNED_ROWS]: docs.length }),
        );
      },

      async findOneAny(filter) {
        const run = async () => {
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
        };
        return traced(tele, "findOneAny", () => ({
          [TA.SCOPE]: recordScope ? scopeId : undefined,
          [TA.FILTER_KEYS]: filterKeys(filter),
        }), run);
      },

      async findAny(filter, options) {
        const run = async () => {
          const session = sessionContext.getSession();
          const { validate = true, ...findOptions } = options ?? {};
          const conditions: Record<string, unknown>[] = [{ _scope: scopeId }];
          if (filter) conditions.push(filter as Record<string, unknown>);
          const cursor = collection.find(
            // deno-lint-ignore no-explicit-any
            { $and: conditions } as any,
            { session, ...findOptions },
          );
          const raw = await cursor.toArray();
          // deno-lint-ignore no-explicit-any
          if (validate === false) return raw as any;
          const out: unknown[] = [];
          for (const item of raw) {
            const parsed = v.safeParse(storageUnion, item);
            if (parsed.success) out.push(parsed.output);
          }
          // deno-lint-ignore no-explicit-any
          return out as any;
        };
        return traced(
          tele,
          "findAny",
          () => ({
            [TA.SCOPE]: recordScope ? scopeId : undefined,
            [TA.FILTER_KEYS]: filterKeys(filter),
          }),
          run,
          (docs) => ({ [TA.RETURNED_ROWS]: docs.length }),
        );
      },

      countDocuments(type, filter, options) {
        const run = () => {
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
        };
        return traced(tele, "countDocuments", () => ({
          [TA.SCOPE]: recordScope ? scopeId : undefined,
          [TA.DOC_TYPE]: String(type),
          [TA.FILTER_KEYS]: filterKeys(filter),
        }), run);
      },

      async deleteId(type, id) {
        const run = async () => {
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
            throw errorWithSafeMessage(
              `deleteId(${typeName}, ${id}): no element found in scope "${scopeId}"`,
              `deleteId(${typeName}): no element found in scope`,
            );
          }
          return result.deletedCount;
        };
        return traced(tele, "deleteId", () => ({
          [TA.SCOPE]: recordScope ? scopeId : undefined,
          [TA.DOC_TYPE]: String(type),
          [TA.FILTER_KEYS]: "_id",
        }), run);
      },

      async deleteIds(type, ids) {
        const run = async () => {
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
        };
        return traced(
          tele,
          "deleteIds",
          () => ({
            [TA.SCOPE]: recordScope ? scopeId : undefined,
            [TA.DOC_TYPE]: String(type),
            [TA.FILTER_KEYS]: "_id",
            [TA.BATCH_SIZE]: ids.length,
          }),
          run,
          (count) => ({ [TA.DELETED_COUNT]: count }),
        );
      },

      async deleteMany(type, filter) {
        const run = async () => {
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
        };
        return traced(
          tele,
          "deleteMany",
          () => ({
            [TA.SCOPE]: recordScope ? scopeId : undefined,
            [TA.DOC_TYPE]: String(type),
            [TA.FILTER_KEYS]: filterKeys(filter),
          }),
          run,
          (count) => ({ [TA.DELETED_COUNT]: count }),
        );
      },

      async updateOne(type, id, doc) {
        const run = async (op?: OpContext) => {
          const typeName = type as string;
          assertNoReservedFields(doc as Record<string, unknown>);

          // Split out removeField() symbols → $unset, the rest → $set.
          const { set, unset } = extractFieldsToRemove(
            doc as Record<string, unknown>,
          );

          // Validate the $set paths against the per-type dot-notation schema
          // (runs the schema's pipe checks/transforms) so a bad value surfaces
          // a clear Valibot error instead of Mongo's opaque "Document failed
          // validation". Reserved fields are already rejected above. Mirrors
          // multiCollection.updateOne. Done before the retry — a validation
          // error is not a transient write conflict — but inside `run` so the
          // failure is recorded on the span.
          const dotSchema = dotSchemaElements[typeName];
          if (!dotSchema) {
            throw new Error(`updateOne: unknown type "${typeName}"`);
          }
          if (Object.keys(set).length > 0) v.parse(dotSchema, set);

          const updateOps = buildUpdateOps(set, unset);
          if (Object.keys(updateOps).length === 0) return 0;

          return retryOnWriteConflict(async () => {
            const session = sessionContext.getSession();
            const result = await collection.updateOne(
              {
                _id: id,
                _type: typeName,
                _scope: scopeId,
                // deno-lint-ignore no-explicit-any
              } as any,
              updateOps as any,
              { session },
            );
            if (!result.acknowledged) throw new Error("Update failed");
            if (result.matchedCount === 0) {
              throw errorWithSafeMessage(
                `updateOne(${typeName}, ${id}): no element found in scope "${scopeId}"`,
                `updateOne(${typeName}): no element found in scope`,
              );
            }
            return result.modifiedCount;
          }, op ? { onRetry: op.onRetry } : undefined);
        };
        return traced(
          tele,
          "updateOne",
          () => ({
            [TA.SCOPE]: recordScope ? scopeId : undefined,
            [TA.DOC_TYPE]: String(type),
            [TA.UPDATE_FIELDS]: Object.keys(doc).length,
          }),
          run,
          (modified) => ({ [TA.MODIFIED_COUNT]: modified }),
        );
      },

      async updateMany(ops) {
        const run = async (op?: OpContext) => {
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

              // Validate the $set paths against the per-type dot-notation schema
              // (pipe checks/transforms) before building the bulk op — same as
              // updateOne / multiCollection.updateMany.
              const dotSchema = dotSchemaElements[typeName];
              if (!dotSchema) {
                throw new Error(`updateMany: unknown type "${typeName}"`);
              }
              if (Object.keys(set).length > 0) v.parse(dotSchema, set);

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
          // Set BATCH_SIZE before the empty-batch early return so no-op spans
          // still carry a (zero) batch size.
          op?.setAttributes({ [TA.BATCH_SIZE]: bulkOps.length });
          if (bulkOps.length === 0) return 0;

          return retryOnWriteConflict(async () => {
            const session = sessionContext.getSession();
            const result = await collection.bulkWrite(bulkOps, { session });
            return result.modifiedCount;
          }, op ? { onRetry: op.onRetry } : undefined);
        };
        return traced(
          tele,
          "updateMany",
          () => ({ [TA.SCOPE]: recordScope ? scopeId : undefined }),
          run,
          (modified) => ({ [TA.MODIFIED_COUNT]: modified }),
        );
      },

      async aggregate(stageBuilder) {
        const run = async () => {
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
        };
        return traced(
          tele,
          "aggregate",
          () => ({ [TA.SCOPE]: recordScope ? scopeId : undefined }),
          run,
          (rows) => ({ [TA.RETURNED_ROWS]: rows.length }),
        );
      },

      async paginate(type, filter, options) {
        const run = async () => {
          const typeName = type as string;
          const { skipTotal = false, peek = false } = options || {};
          const requestedLimit = options?.limit ?? 100;
          let limit = peek ? requestedLimit + 1 : requestedLimit;
          const afterId = options?.afterId;
          const beforeId = options?.beforeId;
          const prepare = options?.prepare;
          const customFilter = options?.filter;
          const format = options?.format;
          const pipelineBuilder = options?.pipeline;
          const sortInput = options?.sort ?? { _id: 1 };
          const session = sessionContext.getSession();

          // Normalize sort + always add an `_id` tie-breaker so duplicate sort
          // values keep a stable (cursor-safe) order.
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
          let sort: Record<string, 1 | -1> = { ...sortObj };

          // Scope + type are non-bypassable; the user filter narrows further.
          const baseQuery: Record<string, unknown>[] = [
            { _scope: scopeId },
            { _type: typeName },
          ];
          if (filter) baseQuery.push(filter as Record<string, unknown>);

          // Build a cursor filter from an anchor doc. Single-field `_id` sort
          // uses a simple comparison; a compound sort emits the lexicographic
          // `$or` ladder. The anchor is fetched WITHIN the bound scope, so a
          // cross-scope id can never seed a cursor.
          const buildCursorFilter = async (
            anchorId: string,
            direction: "after" | "before",
          ): Promise<Record<string, unknown> | null> => {
            const anchor = await collection.findOne(
              // deno-lint-ignore no-explicit-any
              { _id: anchorId, _scope: scopeId, _type: typeName } as any,
              { session },
            );
            if (!anchor) return null;
            const anchorDoc = anchor as Record<string, unknown>;
            const sortFields = Object.keys(sortObj);
            const isForward = direction === "after";
            if (sortFields.length === 1 && sortFields[0] === "_id") {
              const op = (sortObj._id === 1) === isForward ? "$gt" : "$lt";
              return { _id: { [op]: anchorId } };
            }
            const conditions: Record<string, unknown>[] = [];
            for (let i = 0; i < sortFields.length; i++) {
              const f = sortFields[i];
              const dir = sortObj[f];
              const condition: Record<string, unknown> = {};
              for (let j = 0; j < i; j++) {
                const prev = sortFields[j];
                condition[prev] = getNestedValue(anchorDoc, prev);
              }
              const op = (dir === 1) === isForward ? "$gt" : "$lt";
              condition[f] = { [op]: getNestedValue(anchorDoc, f) };
              conditions.push(condition);
            }
            return { $or: conditions };
          };

          // Resolve the (mutually exclusive) cursor. Backward paging walks the
          // reversed sort and re-reverses the page below.
          let cursorFilter: Record<string, unknown> | null = null;
          if (afterId) {
            if (!afterId.startsWith(`${typeName}:`)) {
              throw new Error(
                `paginate: invalid afterId format — expected "${typeName}:..." prefix`,
              );
            }
            cursorFilter = await buildCursorFilter(afterId, "after");
            // Anchor must exist WITHIN this scope+type. This API is new, so we
            // fail loud rather than silently restart at page 1 with a bogus
            // position (as the previous impl did) — a stale/cross-scope id is a
            // caller bug, not "start over".
            if (cursorFilter === null) {
              throw errorWithSafeMessage(
                `paginate: afterId "${afterId}" was not found as type ` +
                  `"${typeName}" in scope "${scopeId}" — cannot anchor the page`,
                `paginate: afterId was not found as type "${typeName}" in ` +
                  `scope — cannot anchor the page`,
              );
            }
          } else if (beforeId) {
            if (!beforeId.startsWith(`${typeName}:`)) {
              throw new Error(
                `paginate: invalid beforeId format — expected "${typeName}:..." prefix`,
              );
            }
            cursorFilter = await buildCursorFilter(beforeId, "before");
            if (cursorFilter === null) {
              throw errorWithSafeMessage(
                `paginate: beforeId "${beforeId}" was not found as type ` +
                  `"${typeName}" in scope "${scopeId}" — cannot anchor the page`,
                `paginate: beforeId was not found as type "${typeName}" in ` +
                  `scope — cannot anchor the page`,
              );
            }
            const reversed: Record<string, 1 | -1> = {};
            for (const [f, d] of Object.entries(sortObj)) {
              reversed[f] = (d === 1 ? -1 : 1) as 1 | -1;
            }
            sort = reversed;
          }

          // User pipeline (scope-aware builder). Used for BOTH the count and the
          // data fetch so `total` reflects docs that survive the WHOLE pipeline
          // (e.g. a $lookup-based JOIN filter), not just the base scope+type
          // match.
          const stageBuilder = buildScopedStageBuilder<T>(collectionName, {
            kind: "single",
            id: scopeId,
          });
          const userPipeline = pipelineBuilder
            ? pipelineBuilder(stageBuilder)
            : [];

          // total + position. `position` is the 0-based count of docs preceding
          // the current page's first row (0 on the first page). `-1` marks the
          // backward path, where the absolute position is resolved post-fetch.
          let total: number | undefined;
          let position: number | undefined;
          if (!skipTotal) {
            if (userPipeline.length > 0) {
              const countPipeline: AggregationStage[] = [
                { $match: { $and: baseQuery } },
                ...userPipeline,
                { $count: "total" },
              ];
              const totalResult = await collection
                .aggregate(countPipeline, { session })
                .toArray();
              total = (totalResult[0]?.total as number | undefined) ?? 0;
              if (afterId) {
                if (cursorFilter) {
                  const afterPipeline: AggregationStage[] = [
                    { $match: { $and: [...baseQuery, cursorFilter] } },
                    ...userPipeline,
                    { $count: "total" },
                  ];
                  const afterResult = await collection
                    .aggregate(afterPipeline, { session })
                    .toArray();
                  const afterCount =
                    (afterResult[0]?.total as number | undefined) ?? 0;
                  position = total - afterCount;
                } else {
                  position = 1;
                }
              } else if (beforeId) {
                position = -1;
              } else {
                position = 0;
              }
            } else {
              total = await collection.countDocuments(
                // deno-lint-ignore no-explicit-any
                { $and: baseQuery } as any,
                { session },
              );
              if (afterId) {
                if (cursorFilter) {
                  const afterCount = await collection.countDocuments(
                    // deno-lint-ignore no-explicit-any
                    { $and: [...baseQuery, cursorFilter] } as any,
                    { session },
                  );
                  position = total - afterCount;
                } else {
                  position = 1;
                }
              } else if (beforeId) {
                position = -1;
              } else {
                position = 0;
              }
            }
          }

          const finalQuery = cursorFilter
            ? { $and: [...baseQuery, cursorFilter] }
            : { $and: baseQuery };

          // Find-path server cap: bound the query at `limit` UNLESS a `filter(doc)`
          // callback is set — a rejecting filter can shrink the page, so we keep
          // the cursor open past `limit` and stop in JS instead. With no filter the
          // cap makes it a bounded top-`limit` query (index-friendly). The pipeline
          // path never server-caps (see below) — it relies on the JS limit.
          const serverCap = customFilter ? undefined : limit;

          // deno-lint-ignore no-explicit-any
          let cursor: m.FindCursor<any> | m.AggregationCursor<any>;
          if (userPipeline.length > 0) {
            // `$sort` goes BEFORE the user pipeline so it can ride an index, and
            // so the expensive pipeline stages ($lookup, …) run LAZILY — only for
            // the ~`limit` docs the JS loop consumes before it closes the cursor,
            // not for the whole cursor-filtered set. No server-side `$limit` here:
            // a filtering pipeline can shrink the page, so `limit` is enforced
            // JS-side (which is also what lets the cursor close early).
            //
            // NOTE: this deliberately DIVERGES from multiCollection.paginate,
            // which sorts AFTER the user pipeline (so callers can sort on fields
            // the pipeline adds — e.g. a `$lookup`/`$addFields` result). Here the
            // sort runs FIRST — mirroring collection.paginate — to keep it
            // index-backed and the `$lookup` lazy; the trade-off is that sorting
            // on pipeline-added fields is NOT supported by the scoped view.
            const dataPipeline: AggregationStage[] = [
              { $match: finalQuery },
              { $sort: sort },
              ...userPipeline,
            ];
            // deno-lint-ignore no-explicit-any
            cursor = collection.aggregate(dataPipeline as any, { session });
          } else {
            // deno-lint-ignore no-explicit-any
            const findCursor = collection.find(finalQuery as any, { session })
              .sort(sort as m.Sort);
            cursor = serverCap !== undefined
              ? findCursor.limit(serverCap)
              : findCursor;
          }

          // Stream rows: the `limit` is applied in JS so a rejecting
          // `filter(doc)` can shrink the page below `limit` while the cursor
          // keeps yielding more candidates. `hardLimit` bounds a pathological
          // filter that rejects everything.
          let hardLimit = 10_000;
          const data: unknown[] = [];
          try {
            while (hardLimit-- > 0 && limit > 0) {
              const doc = await cursor.next();
              if (!doc) break;
              const parsed = v.safeParse(storageSchemas[typeName], doc);
              if (!parsed.success) continue;
              // Preserve pipeline-added fields ($lookup results) alongside the
              // validated document (parse strips unknown keys).
              const validatedDoc = pipelineBuilder
                ? { ...doc, ...parsed.output }
                : parsed.output;
              const enriched = prepare
                ? await prepare(validatedDoc as never)
                : validatedDoc;
              const keep = (await customFilter?.(enriched as never)) ?? true;
              if (!keep) continue;
              const finalDoc = format
                ? await format(enriched as never)
                : enriched;
              data.push(finalDoc);
              limit--;
            }
          } finally {
            await cursor.close();
          }

          // peek: the extra row signals "more after this page" without a count.
          let hasMore: boolean | undefined;
          if (peek) {
            if (data.length > requestedLimit) {
              hasMore = true;
              data.pop();
            } else {
              hasMore = false;
            }
          }

          // Backward page came back in reversed order — restore forward order and
          // resolve the absolute position now that the page length is known.
          if (beforeId) {
            data.reverse();
            if (!skipTotal) {
              if (cursorFilter) {
                // The before-count must be computed through the SAME shape as
                // `total`: when a user pipeline exists, count via the
                // aggregate($count) form so `position` stays consistent with a
                // pipeline-aware `total` (a plain countDocuments would ignore the
                // pipeline's filtering stages).
                let beforeCount: number;
                if (userPipeline.length > 0) {
                  const beforePipeline: AggregationStage[] = [
                    { $match: { $and: [...baseQuery, cursorFilter] } },
                    ...userPipeline,
                    { $count: "total" },
                  ];
                  const beforeResult = await collection
                    .aggregate(beforePipeline, { session })
                    .toArray();
                  beforeCount =
                    (beforeResult[0]?.total as number | undefined) ??
                      0;
                } else {
                  beforeCount = await collection.countDocuments(
                    // deno-lint-ignore no-explicit-any
                    { $and: [...baseQuery, cursorFilter] } as any,
                    { session },
                  );
                }
                position = Math.max(0, beforeCount - data.length);
              } else {
                position = 0;
              }
            }
          }

          return {
            total,
            position,
            // deno-lint-ignore no-explicit-any
            data: data as any,
            ...(peek ? { hasMore } : {}),
          };
        };
        return traced(
          tele,
          "paginate",
          () => ({
            [TA.SCOPE]: recordScope ? scopeId : undefined,
            [TA.DOC_TYPE]: String(type),
            [TA.FILTER_KEYS]: filterKeys(filter),
          }),
          run,
          (result) => ({ [TA.RETURNED_ROWS]: result.data.length }),
        );
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

    // Telemetry: a multi-scope view reports its scope ids as an array; the
    // unscoped view (scopeIds === null) carries no scope attribute at all.
    // Only allocated when telemetry is enabled and scope recording is on —
    // when `recordScope` is false the attribute is entirely absent.
    const scopeAttr = tele && recordScope && scopeIds !== null
      ? [...scopeIds]
      : undefined;

    return {
      _scopes: scopeIds === null ? [] : [...scopeIds],

      async findOne(type, userFilter) {
        const run = async () => {
          const typeName = type as string;
          const session = sessionContext.getSession();
          const conditions: Record<string, unknown>[] = [{ _type: typeName }];
          const sm = scopeMatch();
          if (sm) conditions.push(sm);
          if (userFilter) {
            conditions.push(userFilter as Record<string, unknown>);
          }

          // deno-lint-ignore no-explicit-any
          const raw = await collection.findOne({ $and: conditions } as any, {
            session,
          });
          if (!raw) return null;
          // deno-lint-ignore no-explicit-any
          return v.parse(storageSchemas[typeName], raw) as any;
        };
        return traced(tele, "findOne", () => ({
          [TA.SCOPE]: scopeAttr,
          [TA.DOC_TYPE]: String(type),
          [TA.FILTER_KEYS]: filterKeys(userFilter),
        }), run);
      },

      async find(type, userFilter, options) {
        const run = async () => {
          const typeName = type as string;
          const session = sessionContext.getSession();
          const { validate = true, ...findOptions } = options ?? {};
          const conditions: Record<string, unknown>[] = [{ _type: typeName }];
          const sm = scopeMatch();
          if (sm) conditions.push(sm);
          if (userFilter) {
            conditions.push(userFilter as Record<string, unknown>);
          }

          const cursor = collection.find(
            // deno-lint-ignore no-explicit-any
            { $and: conditions } as any,
            { session, ...findOptions },
          );
          const raw = await cursor.toArray();
          // deno-lint-ignore no-explicit-any
          if (validate === false) return raw as any;
          const out: unknown[] = [];
          for (const item of raw) {
            const parsed = v.safeParse(storageSchemas[typeName], item);
            if (parsed.success) out.push(parsed.output);
          }
          // deno-lint-ignore no-explicit-any
          return out as any;
        };
        return traced(
          tele,
          "find",
          () => ({
            [TA.SCOPE]: scopeAttr,
            [TA.DOC_TYPE]: String(type),
            [TA.FILTER_KEYS]: filterKeys(userFilter),
          }),
          run,
          (docs) => ({ [TA.RETURNED_ROWS]: docs.length }),
        );
      },

      async findProject(type, fields, userFilter, options) {
        const run = async () => {
          const typeName = type as string;
          const session = sessionContext.getSession();
          const conditions: Record<string, unknown>[] = [{ _type: typeName }];
          const sm = scopeMatch();
          if (sm) conditions.push(sm);
          if (userFilter) {
            conditions.push(userFilter as Record<string, unknown>);
          }
          const cursor = collection.find(
            // deno-lint-ignore no-explicit-any
            { $and: conditions } as any,
            {
              session,
              ...options,
              projection: buildProjection(fields as readonly string[]),
            },
          );
          // deno-lint-ignore no-explicit-any
          return (await cursor.toArray()) as any;
        };
        return traced(
          tele,
          "findProject",
          () => ({
            [TA.SCOPE]: scopeAttr,
            [TA.DOC_TYPE]: String(type),
            [TA.FILTER_KEYS]: filterKeys(userFilter),
          }),
          run,
          (docs) => ({ [TA.RETURNED_ROWS]: docs.length }),
        );
      },

      countDocuments(type, userFilter, options) {
        const run = () => {
          const typeName = type as string;
          const session = sessionContext.getSession();
          const conditions: Record<string, unknown>[] = [{ _type: typeName }];
          const sm = scopeMatch();
          if (sm) conditions.push(sm);
          if (userFilter) {
            conditions.push(userFilter as Record<string, unknown>);
          }

          return collection.countDocuments(
            // deno-lint-ignore no-explicit-any
            { $and: conditions } as any,
            { session, ...options },
          );
        };
        return traced(tele, "countDocuments", () => ({
          [TA.SCOPE]: scopeAttr,
          [TA.DOC_TYPE]: String(type),
          [TA.FILTER_KEYS]: filterKeys(userFilter),
        }), run);
      },

      async aggregate(stageBuilder) {
        const run = async () => {
          const stage = buildScopedStageBuilder<T>(collectionName, filter);
          const userPipeline = stageBuilder(stage);
          const pipeline: AggregationStage[] = [];
          const sm = scopeMatch();
          if (sm) pipeline.push({ $match: sm });
          pipeline.push(...userPipeline);
          const session = sessionContext.getSession();
          const cursor = collection.aggregate(pipeline, { session });
          return await cursor.toArray();
        };
        return traced(
          tele,
          "aggregate",
          () => ({ [TA.SCOPE]: scopeAttr }),
          run,
          (rows) => ({ [TA.RETURNED_ROWS]: rows.length }),
        );
      },
    };
  }

  const unscopedView: UnscopedView<T, S> = config.allowUnscoped
    ? buildReadOnlyView(null)
    : new Proxy({} as UnscopedView<T, S>, {
      get(_target, prop) {
        if (prop === "_scopes") return [];
        // Let thenable checks (`then`), JSON serialization (`toJSON`) and any
        // inspection symbol (Symbol.toStringTag / Symbol.iterator / Node's
        // util.inspect.custom, …) probe the object harmlessly — otherwise a
        // bare `console.log(catalog)` or an accidental `await catalog.unscoped`
        // would explode. Only the real view methods stay guarded.
        if (typeof prop === "symbol" || prop === "then" || prop === "toJSON") {
          return undefined;
        }
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
      const run = async () => {
        const session = sessionContext.getSession();
        const values = await collection.distinct("_scope", {}, { session });
        return values.filter((v): v is string => typeof v === "string");
      };
      return traced(
        tele,
        "listScopes",
        undefined,
        run,
        (scopes) => ({ [TA.RETURNED_ROWS]: scopes.length }),
      );
    },

    async scopeExists(id) {
      const validated = assertScopeValue(id);
      const run = async () => {
        const session = sessionContext.getSession();
        const count = await collection.countDocuments(
          // deno-lint-ignore no-explicit-any
          { _scope: validated } as any,
          { session, limit: 1 },
        );
        return count > 0;
      };
      return traced(tele, "scopeExists", () => ({
        [TA.SCOPE]: recordScope ? validated : undefined,
      }), run);
    },

    async dropScope(id, options) {
      if (!options || options.confirm !== true) {
        throw new Error(
          "dropScope() requires { confirm: true } to proceed — the operation " +
            "deletes every document of the scope and is irreversible.",
        );
      }
      const validated = assertScopeValue(id);
      const run = async () => {
        const session = sessionContext.getSession();
        const result = await collection.deleteMany(
          // deno-lint-ignore no-explicit-any
          { _scope: validated } as any,
          { session },
        );
        if (!result.acknowledged) throw new Error("dropScope: delete failed");
        return result.deletedCount;
      };
      return traced(
        tele,
        "dropScope",
        () => ({ [TA.SCOPE]: recordScope ? validated : undefined }),
        run,
        (count) => ({ [TA.DELETED_COUNT]: count }),
      );
    },

    async scopeStats(id) {
      const validated = assertScopeValue(id);
      const run = async () => {
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
      };
      return traced(
        tele,
        "scopeStats",
        () => ({ [TA.SCOPE]: recordScope ? validated : undefined }),
        run,
        (stats) => ({ [TA.RETURNED_ROWS]: Object.keys(stats.byType).length }),
      );
    },

    withSession: sessionContext.withSession,

    async drop(options) {
      if (!options?.force) {
        throw new Error(
          "drop() requires { force: true } to proceed — the operation " +
            "deletes the underlying collection and all its data.",
        );
      }
      const run = async () => {
        const session = sessionContext.getSession();
        return await collection.drop({ session });
      };
      return traced(tele, "drop", undefined, run);
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
 * Build an inclusion projection from a list of field names, always keeping the
 * meta fields (`_id`/`_type`/`_scope`) so a projected document stays
 * identifiable — matching the `Pick<…, P | "_id" | "_type" | "_scope">`
 * return type of {@link ScopedView.findProject}.
 */
function buildProjection(fields: readonly string[]): Record<string, 1> {
  const projection: Record<string, 1> = { _id: 1, _type: 1, _scope: 1 };
  for (const field of fields) projection[field] = 1;
  return projection;
}

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

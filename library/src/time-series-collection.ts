/**
 * `timeSeriesCollection()`: a MongoDB time series collection behind the same
 * Valibot validation as the other collection kinds, exposing only what a
 * time series supports — inserts, reads and deletes.
 *
 * MongoDB accepts no validator on a time series collection, so the schema is
 * enforced here alone, on insert and on read. Everything the server would
 * refuse with an opaque error (a write inside a transaction, an update, a
 * unique index) is refused before the round trip, by name.
 *
 * @module
 */

import * as v from "./schema.ts";
import type * as m from "mongodb";
import type { Db } from "./mongodb.ts";
import { withDatabaseDdlLock } from "./ddl-lock.ts";
import { applyTimeSeriesIndexes } from "./indexes-applier.ts";
import {
  type DriverCollectionOptions,
  readingCollection,
  readOpts,
  type WithReadPreferenceInput,
} from "./read-preference.ts";
import { isSchemaManaged } from "./runtime-config.ts";
import { sanitizeForMongoDB } from "./sanitizer.ts";
import { ambientSessionCollection, getSessionContext } from "./session.ts";
import {
  createOperationTracer,
  filterKeys,
  registerClientTelemetry,
  TELEMETRY_ATTRIBUTES as TA,
  type TelemetryOptions,
  traced,
} from "./telemetry.ts";
import {
  isTimeSeriesDefinition,
  type TimeSeriesDefinition,
  TimeSeriesDefinitionError,
  TimeSeriesOptionsDriftError,
  TimeSeriesTransactionError,
  TimeSeriesUnsupportedError,
} from "./time-series.ts";
import type { ObjectLikeSchema } from "./type-definition.ts";
import {
  ensureTimeSeries,
  inspectTimeSeries,
} from "./utils/ensure-time-series.ts";
import { createLogger } from "./utils/logger.ts";
import { isRecord } from "./utils/guards.ts";
import { dirtyEquivalent } from "./utils/object.ts";
import { DocumentValidationError } from "./validation-error.ts";

const log = createLogger("time-series");

export type TimeSeriesRuntimeOptions = {
  /**
   * Refuse a `deleteMany` whose filter is empty (default `true`). A view
   * bound by `withMeta` still needs a filter of its own.
   */
  safeDelete?: boolean;
  /** How to handle undefined values in inserted measurements. */
  undefinedBehavior?: "remove" | "ignore" | "error";
  /** Skip creation and index synchronisation entirely. */
  noInit?: boolean;
  /**
   * - "auto": create the collection and apply its options and indexes
   * - "managed": leave that to the migrations
   * - "inherit": follow the global runtime config (default)
   */
  schemaManagement?: "auto" | "managed" | "inherit";
  /** Opt-in OpenTelemetry tracing for this collection's operations. */
  telemetry?: TelemetryOptions;
};

type Simplify<T> = { [K in keyof T]: T[K] } & {};

type RequiredKeys<T> = {
  [K in keyof T]-?: Record<never, never> extends Pick<T, K> ? never : K;
}[keyof T];

/** A measurement as read: the schema's output, with the `_id` MongoDB gave it. */
export type TimeSeriesDocument<S extends ObjectLikeSchema> =
  v.InferOutput<S> extends { _id: unknown }
    ? v.InferOutput<S>
    : v.InferOutput<S> & { _id: m.ObjectId };

/** The meta object of a declaration, or `never` when it declares none. */
export type TimeSeriesMeta<
  S extends ObjectLikeSchema,
  M extends string | undefined,
> = M extends keyof v.InferOutput<S> ? v.InferOutput<S>[M] : never;

/**
 * A measurement as inserted through a view bound to the meta keys `B`: those
 * keys may be left out, the view fills them.
 */
export type TimeSeriesInput<
  S extends ObjectLikeSchema,
  M extends string | undefined,
  B extends PropertyKey,
> = [B] extends [never]
  ? v.InferInput<S>
  : M extends keyof v.InferInput<S>
    ? Simplify<
        Omit<v.InferInput<S>, M> &
          (RequiredKeys<Omit<v.InferInput<S>[M], B>> extends never
            ? { [K in M]?: BoundMetaInput<v.InferInput<S>[M], B> }
            : { [K in M]: BoundMetaInput<v.InferInput<S>[M], B> })
      >
    : v.InferInput<S>;

type BoundMetaInput<Meta, B extends PropertyKey> = Simplify<
  Omit<Meta, B> & Partial<Pick<Meta, B & keyof Meta>>
>;

type WriteOptions<O> = Omit<O, "session">;

/**
 * The operations of a time series collection, on the whole collection or on
 * a view bound by {@link TimeSeriesView.withMeta}. There is deliberately no
 * update, replace, upsert, `findOneAnd*`, `bulkWrite` nor `watch`: MongoDB
 * does not support them on a time series.
 */
export interface TimeSeriesView<
  S extends ObjectLikeSchema,
  M extends string | undefined,
  B extends PropertyKey = never,
> {
  readonly collectionName: string;
  /** The declaration this collection was opened with. */
  readonly definition: TimeSeriesDefinition<S, M>;
  /** The schema every measurement is checked against, on insert and on read. */
  readonly schema: v.ObjectSchema<
    {
      readonly _id: v.OptionalSchema<v.UnknownSchema, undefined>;
    } & S["entries"],
    undefined
  >;
  /** The meta values this view injects and filters on; empty on the collection. */
  readonly boundMeta: Readonly<Record<string, unknown>>;

  /**
   * Validates and inserts one measurement; returns its `_id`. Measurements
   * may arrive in any order and late: MongoDB files each one in the bucket
   * of its own `timeField`. Throws {@link TimeSeriesTransactionError} inside
   * `withSession`.
   */
  insertOne(
    doc: TimeSeriesInput<S, M, B>,
    options?: WriteOptions<m.InsertOneOptions>,
  ): Promise<TimeSeriesDocument<S>["_id"]>;
  /** Validates every measurement, then inserts them in one command. */
  insertMany(
    docs: readonly TimeSeriesInput<S, M, B>[],
    options?: WriteOptions<m.BulkWriteOptions>,
  ): Promise<m.InsertManyResult>;

  /** A driver cursor (`sort`, `limit`, `skip`, iteration) over validated measurements. */
  find(
    filter?: m.Filter<v.InferOutput<S>>,
    options?: WithReadPreferenceInput<m.FindOptions> & m.Abortable,
  ): m.FindCursor<TimeSeriesDocument<S>>;
  findOne(
    filter?: m.Filter<v.InferOutput<S>>,
    options?: WithReadPreferenceInput<m.FindOptions> & m.Abortable,
  ): Promise<TimeSeriesDocument<S> | null>;
  countDocuments(
    filter?: m.Filter<v.InferOutput<S>>,
    options?: WithReadPreferenceInput<m.CountDocumentsOptions> & m.Abortable,
  ): Promise<number>;
  /** An aggregation; on a bound view it starts with a `$match` on the bound meta. */
  aggregate<R extends m.Document = m.Document>(
    pipeline?: m.Document[],
    options?: WithReadPreferenceInput<m.AggregateOptions> & m.Abortable,
  ): m.AggregationCursor<R>;

  /**
   * Deletes every measurement matching `filter`, on any field (meta, time or
   * measurement). Throws {@link TimeSeriesTransactionError} inside
   * `withSession`.
   */
  deleteMany(
    filter: m.Filter<v.InferOutput<S>>,
    options?: WriteOptions<m.DeleteOptions>,
  ): Promise<m.DeleteResult>;

  /**
   * A view bound to part of the meta object, the time series counterpart of
   * a scoped multi-collection's `.scope(id)`: its inserts fill the bound
   * keys, and its reads and deletes only see the measurements carrying them.
   * Needs a `metaField` whose schema is an object.
   *
   * @example
   * ```typescript
   * const ofExposition = scans.withMeta({ exposition: expositionId });
   * await ofExposition.insertOne({ at, meta: { point }, scanId });
   * await ofExposition.find({ at: { $gte: from, $lt: to } }).toArray();
   * ```
   */
  withMeta<const P extends Partial<TimeSeriesMeta<S, M>>>(
    bound: P,
  ): TimeSeriesView<S, M, B | keyof P>;
}

/** A time series collection: {@link TimeSeriesView} on every measurement. */
export interface TimeSeriesCollection<
  S extends ObjectLikeSchema,
  M extends string | undefined,
> extends TimeSeriesView<S, M> {
  /**
   * The driver collection, for what the typed API does not cover. It skips
   * validation. Its reads join the ambient `withSession` transaction; its
   * writes throw there, as the typed ones do.
   */
  readonly collection: m.Collection<m.Document>;
  withSession: ReturnType<typeof getSessionContext>["withSession"];
}

const MEASUREMENTS_ARE_IMMUTABLE =
  "a measurement is immutable here. Insert a new one, or deleteMany and insert again.";

const UNSUPPORTED: Readonly<Record<string, string>> = {
  updateOne: `MongoDB refuses a single-document update on a time series collection; ${MEASUREMENTS_ARE_IMMUTABLE}`,
  updateMany: `MongoDB only updates a time series collection by matching and modifying its metaField, which mongodbee does not expose; ${MEASUREMENTS_ARE_IMMUTABLE}`,
  replaceOne: `MongoDB cannot replace a document of a time series collection; ${MEASUREMENTS_ARE_IMMUTABLE}`,
  findOneAndUpdate: `MongoDB does not support findAndModify on a time series collection; ${MEASUREMENTS_ARE_IMMUTABLE}`,
  findOneAndReplace: `MongoDB does not support findAndModify on a time series collection; ${MEASUREMENTS_ARE_IMMUTABLE}`,
  findOneAndDelete:
    "MongoDB does not support findAndModify on a time series collection. Read with find, then deleteMany.",
  bulkWrite:
    "a bulk write mixes updates, replaces and upserts, none of which a time series collection supports. Use insertMany and deleteMany.",
  upsert:
    "MongoDB cannot upsert into a time series collection. Use insertOne; nothing there is unique, so deduplicate on read.",
  watch: "MongoDB does not support change streams on a time series collection.",
  on: "MongoDB does not support change streams on a time series collection.",
  off: "MongoDB does not support change streams on a time series collection.",
};

const RAW_WRITES: ReadonlySet<string> = new Set([
  "insertOne",
  "insertMany",
  "updateOne",
  "updateMany",
  "replaceOne",
  "deleteOne",
  "deleteMany",
  "findOneAndUpdate",
  "findOneAndReplace",
  "findOneAndDelete",
  "bulkWrite",
  "initializeOrderedBulkOp",
  "initializeUnorderedBulkOp",
]);

/**
 * Opens (and, unless the schema is managed by migrations, creates) a time
 * series collection.
 *
 * @example
 * ```typescript
 * const scans = await timeSeriesCollection(db, "badge_scans", defineTimeSeries({
 *   schema: v.object({
 *     at: v.date(),
 *     meta: v.object({ exposition: v.string(), point: v.string() }),
 *     scanId: v.string(),
 *   }),
 *   timeField: "at",
 *   metaField: "meta",
 *   granularity: "minutes",
 *   indexes: (f) => [index(f.scanId)],
 * }));
 *
 * await scans.insertOne({ at: new Date(), meta: { exposition, point }, scanId });
 * const last = await scans.withMeta({ exposition })
 *   .find({ at: { $gte: since } }).sort({ at: -1 }).limit(50).toArray();
 * ```
 */
export async function timeSeriesCollection<
  const S extends ObjectLikeSchema,
  const M extends string | undefined,
>(
  db: Db,
  collectionName: string,
  definition: TimeSeriesDefinition<S, M>,
  options?: DriverCollectionOptions & TimeSeriesRuntimeOptions,
): Promise<TimeSeriesCollection<S, M>> {
  if (!isTimeSeriesDefinition(definition)) {
    throw new TimeSeriesDefinitionError(
      `timeSeriesCollection("${collectionName}"): the third argument must come from defineTimeSeries({...})`,
    );
  }
  const unsupportedOption = options as Record<string, unknown> | undefined;
  if (unsupportedOption?.enableWatching) {
    throw new TimeSeriesUnsupportedError(
      collectionName,
      "enableWatching",
      UNSUPPORTED.watch,
    );
  }

  type Stored = TimeSeriesDocument<S>;

  const opts: DriverCollectionOptions & TimeSeriesRuntimeOptions = {
    safeDelete: true,
    undefinedBehavior: "remove",
    ...options,
  };
  const { timeField, metaField } = definition.options;

  const schema = v.object({
    _id: v.optional(v.unknown()),
    ...definition.schema.entries,
  }) as TimeSeriesView<S, M>["schema"];
  const metaEntries = metaField
    ? (definition.schema.entries[metaField] as { entries?: unknown }).entries
    : undefined;

  const sessionContext = getSessionContext(db.client);
  const collection = readingCollection<m.Document, typeof opts>(
    db,
    collectionName,
    opts,
  );

  const shouldAutoApply =
    opts.schemaManagement === "auto" ||
    (opts.schemaManagement !== "managed" && !isSchemaManaged());
  let verified = false;
  if (!opts.noInit && shouldAutoApply && !sessionContext.getSession()) {
    await withDatabaseDdlLock(db, async () => {
      await ensureTimeSeries(db, collectionName, definition.options);
      await applyTimeSeriesIndexes(
        collection,
        schema as v.ObjectSchema<any, any>,
        { composites: definition.indexes },
      );
    });
    verified = true;
  }

  /**
   * Before the first insert of a collection this process did not create:
   * MongoDB creates a missing collection on insert, as a REGULAR one, and a
   * regular collection can never become a time series. When the migrations
   * own the schema, an insert that outruns them must fail instead.
   */
  async function assertTimeSeriesExists(operation: string): Promise<void> {
    if (verified) return;
    const inspection = await inspectTimeSeries(
      db,
      collectionName,
      definition.options,
    );
    if (inspection.state === "missing") {
      throw new TimeSeriesUnsupportedError(
        collectionName,
        operation,
        `the collection does not exist yet, and inserting now would make MongoDB create it as a regular collection, which can never become a time series. ` +
          `Create it with a migration (createTimeSeriesCollection), or open it with schemaManagement: "auto".`,
      );
    }
    if (inspection.state === "not-time-series") {
      throw new TimeSeriesOptionsDriftError(collectionName, [
        `"${collectionName}" exists as a ${inspection.type === "view" ? "view" : "regular collection"}, not as a time series collection`,
      ]);
    }
    if (inspection.plan.blocked.length > 0) {
      throw new TimeSeriesOptionsDriftError(
        collectionName,
        inspection.plan.blocked,
      );
    }
    if (inspection.plan.changes.length > 0) {
      log.warn(
        `time series "${collectionName}" differs from its declaration: ${inspection.plan.changes.join("; ")}. Not applied: the schema is managed by migrations (timeSeriesCollection("${collectionName}").updateOptions()).`,
      );
    }
    verified = true;
  }

  const tele = createOperationTracer(opts.telemetry, {
    dbName: db.databaseName,
    collectionName,
    getSession: () => sessionContext.getSession(),
  });
  registerClientTelemetry(db.client, opts.telemetry);

  function refuseTransaction(operation: string, given?: object): void {
    const explicit = (given as { session?: m.ClientSession } | undefined)
      ?.session;
    if (sessionContext.getSession() || explicit?.inTransaction()) {
      throw new TimeSeriesTransactionError(collectionName, operation);
    }
  }

  function parseStored(document: unknown): Stored {
    const parsed = v.safeParse(schema, document);
    if (!parsed.success) throw new DocumentValidationError(parsed, document);
    return parsed.output as Stored;
  }

  function validated(
    doc: unknown,
    bound: Readonly<Record<string, unknown>>,
  ): m.Document {
    const parsed = v.safeParse(schema, withBoundMeta(doc, bound));
    if (!parsed.success) throw new DocumentValidationError(parsed, doc);
    const output = parsed.output as Record<string, unknown>;
    if (!(output[timeField] instanceof Date)) {
      throw new TimeSeriesDefinitionError(
        `timeSeriesCollection("${collectionName}"): "${timeField}" must be a Date once validated; MongoDB refuses a measurement whose timeField is not a BSON date`,
      );
    }
    return sanitizeForMongoDB(output, {
      undefinedBehavior: opts.undefinedBehavior || "remove",
      deep: true,
    }) as m.Document;
  }

  function withBoundMeta(
    doc: unknown,
    bound: Readonly<Record<string, unknown>>,
  ): unknown {
    if (metaField === undefined || Object.keys(bound).length === 0) return doc;
    if (!isRecord(doc)) return doc;
    const given = doc[metaField];
    if (given !== undefined && !isRecord(given)) return doc;
    for (const [key, value] of Object.entries(bound)) {
      const own = given?.[key];
      if (own !== undefined && !dirtyEquivalent(own, value)) {
        throw new Error(
          `timeSeriesCollection("${collectionName}"): the measurement carries ${metaField}.${key} = ${JSON.stringify(own)}, but this view is bound to ${JSON.stringify(value)}`,
        );
      }
    }
    return { ...doc, [metaField]: { ...given, ...bound } };
  }

  function boundFilter(
    bound: Readonly<Record<string, unknown>>,
  ): Record<string, unknown> {
    return Object.fromEntries(
      Object.entries(bound).map(([key, value]) => [
        `${metaField}.${key}`,
        value,
      ]),
    );
  }

  function scoped(
    bound: Readonly<Record<string, unknown>>,
    filter: unknown,
  ): m.Filter<m.Document> {
    const own = (filter ?? {}) as Record<string, unknown>;
    if (Object.keys(bound).length === 0) return own;
    return Object.keys(own).length === 0
      ? boundFilter(bound)
      : { $and: [boundFilter(bound), own] };
  }

  function assertSafeDelete(filter: unknown): void {
    if (!opts.safeDelete) return;
    const hasCondition =
      isRecord(filter) &&
      Object.values(filter).some((value) => value !== undefined);
    if (!hasCondition) {
      throw new Error(
        `timeSeriesCollection("${collectionName}").deleteMany: the filter is empty; name what to delete, or open the collection with safeDelete: false`,
      );
    }
  }

  function bind(
    current: Readonly<Record<string, unknown>>,
    added: unknown,
  ): Readonly<Record<string, unknown>> {
    const fail = (message: string): never => {
      throw new TimeSeriesDefinitionError(
        `timeSeriesCollection("${collectionName}").withMeta: ${message}`,
      );
    };
    if (metaField === undefined) {
      return fail("the declaration has no metaField to bind");
    }
    if (!isRecord(metaEntries)) {
      return fail(
        `metaField "${metaField}" is not an object in the schema; only the keys of an object can be bound`,
      );
    }
    if (!isRecord(added) || Object.keys(added).length === 0) {
      return fail("expects an object with at least one meta key");
    }
    const next: Record<string, unknown> = { ...current };
    for (const [key, value] of Object.entries(added)) {
      const entry = metaEntries[key];
      if (entry === undefined) {
        fail(`"${key}" is not a key of ${metaField} in the schema`);
      }
      if (value === undefined) fail(`"${key}" is undefined`);
      const parsed = v.safeParse(entry as v.GenericSchema, value);
      if (!parsed.success) throw new DocumentValidationError(parsed, added);
      if (key in current && !dirtyEquivalent(current[key], parsed.output)) {
        fail(
          `"${key}" is already bound to ${JSON.stringify(current[key])} on this view`,
        );
      }
      next[key] = parsed.output;
    }
    return Object.freeze(next);
  }

  function createView<B extends PropertyKey>(
    bound: Readonly<Record<string, unknown>>,
  ): TimeSeriesView<S, M, B> {
    const view: TimeSeriesView<S, M, B> = {
      collectionName,
      definition,
      schema,
      boundMeta: bound,

      insertOne(doc, insertOptions) {
        const run = async () => {
          refuseTransaction("insertOne", insertOptions);
          await assertTimeSeriesExists("insertOne");
          const inserted = await collection.insertOne(
            validated(doc, bound),
            insertOptions,
          );
          if (!inserted.acknowledged) throw new Error("Insert failed");
          return inserted.insertedId as Stored["_id"];
        };
        return traced(tele, "insertOne", undefined, run, () => ({
          [TA.INSERTED_COUNT]: 1,
        }));
      },

      insertMany(docs, insertOptions) {
        const run = async () => {
          refuseTransaction("insertMany", insertOptions);
          await assertTimeSeriesExists("insertMany");
          const safeDocs = docs.map((doc) => validated(doc, bound));
          const inserted = await collection.insertMany(safeDocs, insertOptions);
          if (!inserted.acknowledged) throw new Error("Insert failed");
          return inserted;
        };
        return traced(
          tele,
          "insertMany",
          () => ({ [TA.BATCH_SIZE]: docs.length }),
          run,
          (result) => ({ [TA.INSERTED_COUNT]: result.insertedCount }),
        );
      },

      find(filter, findOptions) {
        const cursor = collection
          .find(
            scoped(bound, filter),
            readOpts(sessionContext.getSession(), findOptions),
          )
          .map(parseStored);
        if (tele) {
          cursor.toArray = tele.wrapToArray(
            "find",
            { [TA.FILTER_KEYS]: filterKeys(filter) },
            cursor.toArray.bind(cursor),
          );
        }
        return cursor;
      },

      findOne(filter, findOptions) {
        const run = async () => {
          const found = await collection.findOne(
            scoped(bound, filter),
            readOpts(sessionContext.getSession(), findOptions),
          );
          return found === null ? null : parseStored(found);
        };
        return traced(
          tele,
          "findOne",
          () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
          run,
          (found) => ({ [TA.RETURNED_ROWS]: found ? 1 : 0 }),
        );
      },

      countDocuments(filter, countOptions) {
        const run = () =>
          collection.countDocuments(
            scoped(bound, filter),
            readOpts(sessionContext.getSession(), countOptions),
          );
        return traced(
          tele,
          "countDocuments",
          () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
          run,
        );
      },

      aggregate<R extends m.Document = m.Document>(
        pipeline: m.Document[] = [],
        aggregateOptions?: WithReadPreferenceInput<m.AggregateOptions> &
          m.Abortable,
      ) {
        const stages =
          Object.keys(bound).length === 0
            ? pipeline
            : [{ $match: boundFilter(bound) }, ...pipeline];
        return collection.aggregate<R>(
          stages,
          readOpts(sessionContext.getSession(), aggregateOptions),
        );
      },

      deleteMany(filter, deleteOptions) {
        const run = async () => {
          refuseTransaction("deleteMany", deleteOptions);
          assertSafeDelete(filter);
          return await collection.deleteMany(
            scoped(bound, filter),
            deleteOptions,
          );
        };
        return traced(
          tele,
          "deleteMany",
          () => ({ [TA.FILTER_KEYS]: filterKeys(filter) }),
          run,
          (result) => ({ [TA.DELETED_COUNT]: result.deletedCount }),
        );
      },

      withMeta(added) {
        return createView(bind(bound, added));
      },
    };

    // Not in the type, on purpose. A caller reaching one anyway (plain
    // JavaScript, a cast, code ported from `collection()`) gets the reason
    // instead of "is not a function" or the server's error.
    for (const [operation, reason] of Object.entries(UNSUPPORTED)) {
      Object.defineProperty(view, operation, {
        enumerable: false,
        value: () => {
          throw new TimeSeriesUnsupportedError(
            collectionName,
            operation,
            reason,
          );
        },
      });
    }
    return view;
  }

  const session = ambientSessionCollection(collection);
  const raw = new Proxy(session, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver);
      if (typeof property !== "string" || !RAW_WRITES.has(property)) {
        return value;
      }
      return (...args: unknown[]) => {
        if (sessionContext.getSession()) {
          throw new TimeSeriesTransactionError(
            collectionName,
            `collection.${property}`,
          );
        }
        return (value as (...given: unknown[]) => unknown).apply(target, args);
      };
    },
  });

  const root = createView<never>(Object.freeze({}));
  return Object.defineProperties(root, {
    collection: { value: raw, enumerable: true },
    withSession: { value: sessionContext.withSession, enumerable: true },
  }) as TimeSeriesCollection<S, M>;
}

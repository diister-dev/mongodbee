/**
 * Time series declarations: what a time series collection is created with,
 * and what MongoDB lets change afterwards.
 *
 * Every rule here was checked against MongoDB 8.0 (see doc/TIME-SERIES.md for
 * the list and the sources): a time series collection takes no validator, no
 * unique index and no write inside a transaction; its `timeField` and
 * `metaField` are fixed at creation; its buckets can only widen.
 *
 * @module
 */

import type * as v from "./schema.ts";
import {
  createFieldProxy,
  type FieldsOf,
  IndexDeclaration,
} from "./index-builder.ts";
import { type CompositeIndexDescriptor, extractIndexes } from "./indexes.ts";
import type { ObjectLikeSchema } from "./type-definition.ts";
import { isRecord } from "./utils/guards.ts";

/** The bucket presets MongoDB names. */
export type TimeSeriesGranularity = "seconds" | "minutes" | "hours";

/**
 * The options a time series collection is created with, as MongoDB names
 * them. `granularity` and the `bucket*Seconds` pair exclude each other; the
 * pair must be given together and equal.
 */
export type TimeSeriesOptions = {
  /** Top-level field holding the measurement's date. Fixed at creation. */
  readonly timeField: string;
  /** Top-level field labelling the series. Fixed at creation. */
  readonly metaField?: string;
  /** Bucket preset; MongoDB's default is `"seconds"`. Can only be raised. */
  readonly granularity?: TimeSeriesGranularity;
  /** Custom bucket span, 1 to 31536000. Can only be raised. */
  readonly bucketMaxSpanSeconds?: number;
  /** Must equal `bucketMaxSpanSeconds`. */
  readonly bucketRoundingSeconds?: number;
  /**
   * Seconds after its `timeField` at which a measurement expires. MongoDB
   * removes a bucket once every measurement in it has expired. Absent: never.
   */
  readonly expireAfterSeconds?: number;
};

/** A time series declaration MongoDB would refuse, or mongodbee cannot honour. */
export class TimeSeriesDefinitionError extends Error {
  override readonly name = "TimeSeriesDefinitionError";
}

/**
 * The database's time series collection differs from its declaration in a
 * way `collMod` cannot bring back. `blocked` lists each difference.
 */
export class TimeSeriesOptionsDriftError extends Error {
  override readonly name = "TimeSeriesOptionsDriftError";
  readonly collectionName: string;
  readonly blocked: readonly string[];

  constructor(collectionName: string, blocked: readonly string[]) {
    super(
      `Time series collection "${collectionName}" cannot be brought to its declaration: ${blocked.join("; ")}. ` +
        `These options are fixed once the collection exists; create a new collection and copy the measurements, or restore the declaration.`,
    );
    this.collectionName = collectionName;
    this.blocked = blocked;
  }
}

/** An operation MongoDB does not support on a time series collection. */
export class TimeSeriesUnsupportedError extends Error {
  override readonly name: string = "TimeSeriesUnsupportedError";
  readonly collectionName: string;
  readonly operation: string;

  constructor(collectionName: string, operation: string, reason: string) {
    super(
      `timeSeriesCollection("${collectionName}").${operation} is not available: ${reason}`,
    );
    this.collectionName = collectionName;
    this.operation = operation;
  }
}

/** A write to a time series collection attempted inside a transaction. */
export class TimeSeriesTransactionError extends TimeSeriesUnsupportedError {
  override readonly name = "TimeSeriesTransactionError";

  constructor(collectionName: string, operation: string) {
    super(
      collectionName,
      operation,
      "MongoDB cannot write to a time series collection inside a transaction, and this call runs in one (withSession). " +
        "Move it after the transaction (afterCommit), or wrap it in outsideTransaction(() => ...) so it commits on its own.",
    );
  }
}

const GRANULARITY_BUCKETS: Readonly<
  Record<TimeSeriesGranularity, { rounding: number; span: number }>
> = {
  seconds: { rounding: 60, span: 3600 },
  minutes: { rounding: 3600, span: 86_400 },
  hours: { rounding: 86_400, span: 2_592_000 },
};

const MAX_BUCKET_SPAN_SECONDS = 31_536_000;

type Bucketing = {
  readonly granularity?: TimeSeriesGranularity;
  readonly rounding: number;
  readonly span: number;
};

function isGranularity(value: unknown): value is TimeSeriesGranularity {
  return value === "seconds" || value === "minutes" || value === "hours";
}

function bucketingOf(options: TimeSeriesOptions): Bucketing {
  if (options.granularity !== undefined) {
    return {
      granularity: options.granularity,
      ...GRANULARITY_BUCKETS[options.granularity],
    };
  }
  if (options.bucketMaxSpanSeconds !== undefined) {
    return {
      rounding: options.bucketRoundingSeconds ?? options.bucketMaxSpanSeconds,
      span: options.bucketMaxSpanSeconds,
    };
  }
  return { granularity: "seconds", ...GRANULARITY_BUCKETS.seconds };
}

function describeBucketing(bucketing: Bucketing): string {
  return bucketing.granularity !== undefined
    ? `granularity "${bucketing.granularity}"`
    : `bucketMaxSpanSeconds ${bucketing.span}`;
}

function sameBucketing(a: Bucketing, b: Bucketing): boolean {
  return (
    a.granularity === b.granularity &&
    a.rounding === b.rounding &&
    a.span === b.span
  );
}

function numberOf(value: unknown): number | undefined {
  if (typeof value === "number") return value;
  if (typeof value === "bigint") return Number(value);
  if (
    isRecord(value) &&
    typeof (value as { toNumber?: unknown }).toNumber === "function"
  ) {
    return (value as unknown as { toNumber(): number }).toNumber();
  }
  return undefined;
}

/**
 * Checks options the way MongoDB 8.0 does at creation, so a wrong declaration
 * fails where it is written instead of at the first boot.
 */
export function assertTimeSeriesOptions(
  options: TimeSeriesOptions,
  context = "defineTimeSeries",
): void {
  const fail = (message: string): never => {
    throw new TimeSeriesDefinitionError(`${context}: ${message}`);
  };
  const { timeField, metaField } = options;
  if (typeof timeField !== "string" || timeField.length === 0) {
    fail("timeField is required");
  }
  if (timeField.includes(".")) {
    fail(`timeField "${timeField}" must be a top-level field, without a "."`);
  }
  if (timeField === "_id") fail('timeField cannot be "_id"');
  if (metaField !== undefined) {
    if (typeof metaField !== "string" || metaField.length === 0) {
      fail("metaField must be a field name");
    }
    if (metaField.includes(".")) {
      fail(`metaField "${metaField}" must be a top-level field, without a "."`);
    }
    if (metaField === "_id") fail('metaField cannot be "_id"');
    if (metaField === timeField) {
      fail(`metaField cannot be the timeField ("${timeField}")`);
    }
  }
  const { granularity, bucketMaxSpanSeconds, bucketRoundingSeconds } = options;
  if (granularity !== undefined && !isGranularity(granularity)) {
    fail(
      `granularity must be "seconds", "minutes" or "hours", got ${JSON.stringify(granularity)}`,
    );
  }
  const custom =
    bucketMaxSpanSeconds !== undefined || bucketRoundingSeconds !== undefined;
  if (custom) {
    if (granularity !== undefined) {
      fail(
        "granularity cannot be combined with bucketMaxSpanSeconds / bucketRoundingSeconds; choose one",
      );
    }
    if (
      bucketMaxSpanSeconds === undefined ||
      bucketRoundingSeconds === undefined ||
      bucketMaxSpanSeconds !== bucketRoundingSeconds
    ) {
      fail(
        "bucketMaxSpanSeconds and bucketRoundingSeconds must be set together, to the same value",
      );
    }
    const span = bucketMaxSpanSeconds as number;
    if (!Number.isInteger(span) || span < 1 || span > MAX_BUCKET_SPAN_SECONDS) {
      fail(
        `bucketMaxSpanSeconds must be an integer from 1 to ${MAX_BUCKET_SPAN_SECONDS}, got ${span}`,
      );
    }
  }
  const { expireAfterSeconds } = options;
  if (
    expireAfterSeconds !== undefined &&
    (!Number.isInteger(expireAfterSeconds) || expireAfterSeconds < 0)
  ) {
    fail(
      `expireAfterSeconds must be a non-negative integer, got ${String(expireAfterSeconds)}`,
    );
  }
}

/** `options` without absent keys, in a fixed order: what a snapshot compares. */
export function normalizeTimeSeriesOptions(
  options: TimeSeriesOptions,
): TimeSeriesOptions {
  const bucketing = bucketingOf(options);
  return {
    timeField: options.timeField,
    ...(options.metaField !== undefined && { metaField: options.metaField }),
    ...(bucketing.granularity !== undefined
      ? { granularity: bucketing.granularity }
      : {
          bucketMaxSpanSeconds: bucketing.span,
          bucketRoundingSeconds: bucketing.rounding,
        }),
    ...(options.expireAfterSeconds !== undefined && {
      expireAfterSeconds: options.expireAfterSeconds,
    }),
  };
}

/**
 * The options of an existing time series collection, read from the `options`
 * of its `listCollections` entry; `undefined` when that entry is not a time
 * series.
 */
export function storedTimeSeriesOptions(
  collectionOptions: unknown,
): TimeSeriesOptions | undefined {
  if (!isRecord(collectionOptions)) return undefined;
  const stored = collectionOptions.timeseries;
  if (!isRecord(stored) || typeof stored.timeField !== "string") {
    return undefined;
  }
  const expireAfterSeconds = numberOf(collectionOptions.expireAfterSeconds);
  const span = numberOf(stored.bucketMaxSpanSeconds);
  const rounding = numberOf(stored.bucketRoundingSeconds);
  return normalizeTimeSeriesOptions({
    timeField: stored.timeField,
    ...(typeof stored.metaField === "string" && {
      metaField: stored.metaField,
    }),
    ...(isGranularity(stored.granularity)
      ? { granularity: stored.granularity }
      : span !== undefined && {
          bucketMaxSpanSeconds: span,
          bucketRoundingSeconds: rounding ?? span,
        }),
    ...(expireAfterSeconds !== undefined && { expireAfterSeconds }),
  });
}

/** The `create` options of a declaration. */
export function timeSeriesCreateOptions(options: TimeSeriesOptions): {
  timeseries: Omit<TimeSeriesOptions, "expireAfterSeconds">;
  expireAfterSeconds?: number;
} {
  const { expireAfterSeconds, ...timeseries } =
    normalizeTimeSeriesOptions(options);
  return {
    timeseries,
    ...(expireAfterSeconds !== undefined && { expireAfterSeconds }),
  };
}

/**
 * What it takes to bring a time series collection from `from` to `to`.
 * `commands` are `collMod` bodies (without the collection name), to run in
 * order; `blocked` are the differences no command can apply.
 */
export type TimeSeriesChangePlan = {
  /** No difference at all. */
  readonly inSync: boolean;
  /** `collMod` bodies that apply every applicable difference. */
  readonly commands: readonly Record<string, unknown>[];
  /** The applicable differences, one sentence each. */
  readonly changes: readonly string[];
  /** The differences MongoDB cannot apply to an existing collection. */
  readonly blocked: readonly string[];
  /** Applying widens the buckets, which cannot be undone. */
  readonly irreversible: boolean;
};

/**
 * Plans the change from one set of options to another, with the rules
 * MongoDB 8.0 enforces: `timeField` and `metaField` never change; buckets
 * only widen (neither the rounding nor the span may decrease);
 * `expireAfterSeconds` changes freely.
 */
export function planTimeSeriesChange(
  from: TimeSeriesOptions,
  to: TimeSeriesOptions,
): TimeSeriesChangePlan {
  const commands: Record<string, unknown>[] = [];
  const changes: string[] = [];
  const blocked: string[] = [];
  let irreversible = false;

  if (from.timeField !== to.timeField) {
    blocked.push(
      `timeField is "${from.timeField}" and cannot become "${to.timeField}"`,
    );
  }
  if (from.metaField !== to.metaField) {
    const name = (field: string | undefined) =>
      field === undefined ? "absent" : `"${field}"`;
    blocked.push(
      `metaField is ${name(from.metaField)} and cannot become ${name(to.metaField)}`,
    );
  }

  const current = bucketingOf(from);
  const wanted = bucketingOf(to);
  if (!sameBucketing(current, wanted)) {
    if (wanted.rounding >= current.rounding && wanted.span >= current.span) {
      commands.push({
        timeseries:
          wanted.granularity !== undefined
            ? { granularity: wanted.granularity }
            : {
                bucketMaxSpanSeconds: wanted.span,
                bucketRoundingSeconds: wanted.rounding,
              },
      });
      changes.push(
        `buckets widen from ${describeBucketing(current)} to ${describeBucketing(wanted)}`,
      );
      irreversible = true;
    } else {
      blocked.push(
        `buckets are ${describeBucketing(current)} and cannot narrow to ${describeBucketing(wanted)} (MongoDB only widens them)`,
      );
    }
  }

  if (from.expireAfterSeconds !== to.expireAfterSeconds) {
    commands.push({ expireAfterSeconds: to.expireAfterSeconds ?? "off" });
    const describe = (seconds: number | undefined) =>
      seconds === undefined ? "off" : `${seconds}s`;
    changes.push(
      `expireAfterSeconds goes from ${describe(from.expireAfterSeconds)} to ${describe(to.expireAfterSeconds)}`,
    );
  }

  return {
    inSync: commands.length === 0 && blocked.length === 0,
    commands,
    changes,
    blocked,
    irreversible,
  };
}

type AnySchema = v.BaseSchema<unknown, unknown, v.BaseIssue<unknown>>;

type DateKeys<T> = {
  [K in keyof T & string]-?: T[K] extends Date ? K : never;
}[keyof T & string];

/** Index declarations of a time series: `index(...)` only, never `unique(...)`. */
export type TimeSeriesIndexBuild<TOutput> = (
  f: FieldsOf<TOutput>,
) => ReadonlyArray<IndexDeclaration | CompositeIndexDescriptor>;

export interface TimeSeriesDefinitionInput<
  S extends ObjectLikeSchema,
  M extends (keyof S["entries"] & string) | undefined = undefined,
> {
  /** One measurement. Validated by mongodbee on insert and on read. */
  schema: S;
  /** The field holding the measurement's date; a required `v.date()`. */
  timeField: DateKeys<v.InferOutput<S>>;
  /** The field labelling the series; low cardinality, never updated. */
  metaField?: M;
  granularity?: TimeSeriesGranularity;
  bucketMaxSpanSeconds?: number;
  bucketRoundingSeconds?: number;
  expireAfterSeconds?: number;
  indexes?:
    | TimeSeriesIndexBuild<v.InferOutput<S>>
    | ReadonlyArray<IndexDeclaration | CompositeIndexDescriptor>;
}

const BRAND: symbol = Symbol.for("mongodbee.time-series-definition");

/**
 * A time series collection as declared: the measurement schema, the creation
 * options and the secondary indexes. The same object feeds
 * `timeSeriesCollection()` and a migration's `schemas.timeSeriesCollections`.
 */
export class TimeSeriesDefinition<
  S extends ObjectLikeSchema = ObjectLikeSchema,
  M extends string | undefined = string | undefined,
> {
  readonly schema: S;
  readonly options: TimeSeriesOptions;
  readonly indexes: readonly CompositeIndexDescriptor[];
  declare readonly metaField: M;

  constructor(
    schema: S,
    options: TimeSeriesOptions,
    indexes: readonly CompositeIndexDescriptor[],
  ) {
    this.schema = schema;
    this.options = options;
    this.indexes = indexes;
    Object.defineProperty(this, "metaField", {
      value: options.metaField,
      enumerable: false,
    });
    Object.defineProperty(this, BRAND, { value: true, enumerable: false });
  }

  get entries(): S["entries"] {
    return this.schema.entries;
  }
}

export function isTimeSeriesDefinition(
  value: unknown,
): value is TimeSeriesDefinition {
  if (value instanceof TimeSeriesDefinition) return true;
  return (
    typeof value === "object" &&
    value !== null &&
    (value as Record<PropertyKey, unknown>)[BRAND] === true
  );
}

const OPTIONAL_WRAPPERS: ReadonlySet<string> = new Set([
  "optional",
  "exact_optional",
  "nullable",
  "nullish",
  "undefinedable",
]);

const NOT_A_DATE: ReadonlySet<string> = new Set([
  "string",
  "number",
  "boolean",
  "bigint",
  "object",
  "loose_object",
  "strict_object",
  "object_with_rest",
  "array",
  "tuple",
  "record",
  "literal",
  "picklist",
  "enum",
  "null",
  "undefined",
]);

function assertTimeFieldSchema(name: string, schema: AnySchema): void {
  if (OPTIONAL_WRAPPERS.has(schema.type)) {
    throw new TimeSeriesDefinitionError(
      `defineTimeSeries: timeField "${name}" must be required; MongoDB refuses a measurement without it`,
    );
  }
  if (NOT_A_DATE.has(schema.type)) {
    throw new TimeSeriesDefinitionError(
      `defineTimeSeries: timeField "${name}" must be a date (v.date()), not a ${schema.type}; MongoDB only accepts a BSON date there`,
    );
  }
}

/**
 * Refuses the indexes MongoDB 8.0 does not build on a time series
 * collection, naming the declaration instead of the server's error.
 */
export function assertTimeSeriesIndex(
  label: string,
  index: { unique?: boolean; expireAfterSeconds?: number },
  context = "defineTimeSeries",
): void {
  if (index.unique) {
    throw new TimeSeriesDefinitionError(
      `${context}: index ${label} is unique, and MongoDB does not support unique indexes on a time series collection. ` +
        `Declare it with index(...) and deduplicate on read, or keep what must be unique in a regular collection.`,
    );
  }
  if (index.expireAfterSeconds !== undefined) {
    throw new TimeSeriesDefinitionError(
      `${context}: index ${label} is a TTL index; a time series collection expires its measurements with the collection-level expireAfterSeconds option instead.`,
    );
  }
}

function keyLabel(key: Record<string, unknown>): string {
  return `(${Object.keys(key).join(", ")})`;
}

/**
 * Declares a time series collection.
 *
 * @example
 * ```typescript
 * const scans = defineTimeSeries({
 *   schema: v.object({
 *     at: v.date(),
 *     meta: v.object({ exposition: v.string(), point: v.string() }),
 *     scanId: v.string(),
 *     participantId: v.string(),
 *   }),
 *   timeField: "at",
 *   metaField: "meta",
 *   granularity: "minutes",
 *   expireAfterSeconds: 2 * 365 * 24 * 3600,
 *   indexes: (f) => [index(f.scanId), index(f.participantId, desc(f.at))],
 * });
 * ```
 */
export function defineTimeSeries<
  const S extends ObjectLikeSchema,
  const M extends (keyof S["entries"] & string) | undefined = undefined,
>(input: TimeSeriesDefinitionInput<S, M>): TimeSeriesDefinition<S, M> {
  const options = normalizeOptionsInput(input);
  assertTimeSeriesOptions(options);

  const entries = input.schema?.entries;
  if (!isRecord(entries)) {
    throw new TimeSeriesDefinitionError(
      "defineTimeSeries: schema must be an object schema (v.object({...}))",
    );
  }
  const timeSchema = entries[options.timeField] as AnySchema | undefined;
  if (timeSchema === undefined) {
    throw new TimeSeriesDefinitionError(
      `defineTimeSeries: timeField "${options.timeField}" does not exist in the schema`,
    );
  }
  assertTimeFieldSchema(options.timeField, timeSchema);
  if (
    options.metaField !== undefined &&
    !Object.hasOwn(entries, options.metaField)
  ) {
    throw new TimeSeriesDefinitionError(
      `defineTimeSeries: metaField "${options.metaField}" does not exist in the schema`,
    );
  }

  const declared = input.indexes ?? [];
  const raw =
    typeof declared === "function"
      ? declared(createFieldProxy<v.InferOutput<S>>())
      : declared;
  const descriptors = raw.map((item) =>
    item instanceof IndexDeclaration ? item.toDescriptor() : item,
  );
  for (const descriptor of descriptors) {
    const paths = Object.keys(descriptor.key);
    if (paths.length === 0) {
      throw new TimeSeriesDefinitionError(
        "defineTimeSeries: an index needs at least one key",
      );
    }
    for (const path of paths) {
      const root = path.split(".")[0];
      if (root !== "_id" && !Object.hasOwn(entries, root)) {
        throw new TimeSeriesDefinitionError(
          `defineTimeSeries: key path "${path}" does not exist in the schema`,
        );
      }
    }
    assertTimeSeriesIndex(
      descriptor.name ? `"${descriptor.name}"` : keyLabel(descriptor.key),
      descriptor,
    );
  }
  for (const { path, metadata } of extractIndexes(
    input.schema as unknown as v.ObjectSchema<any, any>,
  )) {
    assertTimeSeriesIndex(`on "${path}" (withIndex)`, metadata);
  }

  return new TimeSeriesDefinition(input.schema, options, descriptors);
}

function normalizeOptionsInput(input: {
  timeField: string;
  metaField?: string;
  granularity?: TimeSeriesGranularity;
  bucketMaxSpanSeconds?: number;
  bucketRoundingSeconds?: number;
  expireAfterSeconds?: number;
}): TimeSeriesOptions {
  return {
    timeField: input.timeField,
    ...(input.metaField !== undefined && { metaField: input.metaField }),
    ...(input.granularity !== undefined && { granularity: input.granularity }),
    ...(input.bucketMaxSpanSeconds !== undefined && {
      bucketMaxSpanSeconds: input.bucketMaxSpanSeconds,
    }),
    ...(input.bucketRoundingSeconds !== undefined && {
      bucketRoundingSeconds: input.bucketRoundingSeconds,
    }),
    ...(input.expireAfterSeconds !== undefined && {
      expireAfterSeconds: input.expireAfterSeconds,
    }),
  };
}

/** The differences between two declarations' options, one sentence each. */
export function describeTimeSeriesOptionsDifference(
  from: TimeSeriesOptions,
  to: TimeSeriesOptions,
): string[] {
  const plan = planTimeSeriesChange(from, to);
  return [...plan.blocked, ...plan.changes];
}

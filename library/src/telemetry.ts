/**
 * @module
 *
 * OpenTelemetry tracing support for MongoDBee 🐝
 *
 * This module only depends on `@opentelemetry/api`: it never instantiates an
 * SDK, an exporter or a tracer provider. When the host application has not
 * registered a provider (or telemetry is not enabled), every code path is a
 * no-op. Tracing is strictly opt-in through {@link TelemetryOptions} passed to
 * `collection()`, `multiCollection()` or `scopedMultiCollection()`.
 *
 * Privacy: spans never carry filter, update or document *values* — only
 * structural information such as field names, operator names and counts.
 * Validation issues are replaced by a synthetic message; MongoDBee's own
 * thrown errors are recorded with their interpolated user values stripped; and
 * driver error messages are recorded only after redacting known
 * value-embedding patterns (e.g. duplicate-key values). Unknown driver
 * messages may, in rare cases, still contain values.
 *
 * @example
 * ```typescript
 * import { collection } from "@diister/mongodbee";
 * import type { TelemetryOptions } from "@diister/mongodbee/telemetry";
 *
 * // The application registers its own OpenTelemetry SDK / provider.
 * const telemetry: TelemetryOptions = { enabled: true };
 * const users = await collection(db, "users", { name: v.string() }, {
 *   telemetry,
 * });
 * ```
 */
import {
  type Attributes,
  type Span,
  SpanKind,
  SpanStatusCode,
  trace,
  type Tracer,
  type TracerProvider,
} from "@opentelemetry/api";
import type { ClientSession, MongoClient } from "mongodb";
import denoJson from "../deno.json" with { type: "json" };

/**
 * Opt-in tracing configuration accepted by `collection()`,
 * `multiCollection()` and `scopedMultiCollection()`.
 *
 * The flag is resolved once when the collection is created: when disabled
 * (the default), no span is ever allocated and the operations run on their
 * regular, non-instrumented code path.
 */
export interface TelemetryOptions {
  /**
   * Enable span emission for this collection (and transaction spans for the
   * underlying MongoDB client).
   * @default false
   */
  enabled?: boolean;
  /**
   * Tracer provider used to obtain the tracer. Defaults to the global
   * OpenTelemetry API provider (`trace.getTracerProvider()`), which is a
   * silent no-op when the application has not registered an SDK.
   */
  tracerProvider?: TracerProvider;
}

/**
 * Names of the attributes emitted on MongoDBee spans.
 *
 * Follows the stable OpenTelemetry database semantic conventions
 * (`db.*` keys) plus a `mongodbee.*` namespace for ODM-specific data
 * (document `_type`, scope, PII-safe filter/update summaries, result counts).
 */
export const TELEMETRY_ATTRIBUTES = {
  /** Database system, always `"mongodb"`. */
  DB_SYSTEM: "db.system.name",
  /** Database name. */
  DB_NAMESPACE: "db.namespace",
  /** Physical MongoDB collection name. */
  COLLECTION_NAME: "db.collection.name",
  /** Name of the public MongoDBee operation (`insertOne`, `paginate`, ...). */
  OPERATION_NAME: "db.operation.name",
  /** Number of documents/operations in a batched call. */
  BATCH_SIZE: "db.operation.batch.size",
  /** Number of documents returned by the operation. */
  RETURNED_ROWS: "db.response.returned_rows",
  /** Error class name when the operation failed. */
  ERROR_TYPE: "error.type",
  /** Document `_type` targeted by a multi-collection operation. */
  DOC_TYPE: "mongodbee.doc_type",
  /** Scope value(s) of a scoped multi-collection view (string or string[]). */
  SCOPE: "mongodbee.scope",
  /** Sorted, comma-joined field paths of the user filter (never values). */
  FILTER_KEYS: "mongodbee.filter.keys",
  /** Sorted, comma-joined update operator names (`$set,$unset`). */
  UPDATE_OPERATORS: "mongodbee.update.operators",
  /** Number of fields touched by a document-style update (never values). */
  UPDATE_FIELDS: "mongodbee.update.fields",
  /** Write-conflict retries performed by this operation. */
  RETRY_COUNT: "mongodbee.retry.count",
  /** Documents matched by an update/replace operation. */
  MATCHED_COUNT: "mongodbee.result.matched_count",
  /** Documents modified by an update/replace operation. */
  MODIFIED_COUNT: "mongodbee.result.modified_count",
  /** Documents deleted by a delete operation. */
  DELETED_COUNT: "mongodbee.result.deleted_count",
  /** Documents inserted by an insert operation. */
  INSERTED_COUNT: "mongodbee.result.inserted_count",
  /** Transaction outcome: `"committed"` or `"aborted"`. */
  TX_OUTCOME: "mongodbee.transaction.outcome",
  /** Write-conflict retries of operations executed inside the transaction. */
  TX_RETRY_COUNT: "mongodbee.transaction.retry_count",
} as const;

const A = TELEMETRY_ATTRIBUTES;

const TRACER_NAME = "@diister/mongodbee";
const SAFE_VALIDATION_MESSAGE =
  "Validation failed (details omitted: may contain document values)";

/**
 * Context handed to instrumented operation bodies.
 * @internal
 */
export interface OpContext {
  /** Attach attributes discovered while the operation runs (e.g. batch size). */
  setAttributes(attributes: Attributes): void;
  /**
   * Ready-made callback for `RetryOptions.onRetry` at `retryOnWriteConflict`
   * call sites: records the operation retry count and feeds the ambient
   * transaction retry counter.
   */
  readonly onRetry: (error: Error, attempt: number, delayMs: number) => void;
}

/**
 * Per-collection span factory. `null` when telemetry is disabled — callers
 * must branch to the direct, non-instrumented path in that case.
 * @internal
 */
export interface OperationTracer {
  /**
   * Runs `fn` under a new CLIENT span named `"{operationName} {collection}"`,
   * child of the active context. Errors are recorded (PII-safe) and re-thrown
   * unchanged.
   */
  withOp<T>(
    operationName: string,
    attributes: Attributes | undefined,
    fn: (op: OpContext) => Promise<T>,
    resultAttributes?: (result: T) => Attributes | undefined,
  ): Promise<T>;
  /**
   * Wraps a `toArray`-style function so that a span covers the cursor
   * materialization. The span starts when `toArray()` is invoked, not when
   * the cursor is created.
   */
  wrapToArray<TDoc>(
    operationName: string,
    attributes: Attributes | undefined,
    toArray: () => Promise<TDoc[]>,
  ): () => Promise<TDoc[]>;
}

/**
 * Traces the lifecycle of a MongoDB transaction started by `withSession`.
 * @internal
 */
export interface TransactionTracer {
  /**
   * Runs `run` under an INTERNAL `mongodb.transaction` span recording the
   * outcome (`committed`/`aborted`) and the write-conflict retries of the
   * operations executed within `session`.
   */
  withTransaction<T>(session: ClientSession, run: () => Promise<T>): Promise<T>;
}

const clientTransactionTracers = new WeakMap<MongoClient, TransactionTracer>();
const transactionRetryCounters = new WeakMap<
  ClientSession,
  { count: number }
>();

function resolveTracer(telemetry: TelemetryOptions): Tracer {
  const provider = telemetry.tracerProvider ?? trace.getTracerProvider();
  return provider.getTracer(TRACER_NAME, denoJson.version);
}

/** Drops `undefined` entries so they never reach the SDK. */
function prune(attributes: Attributes): Attributes {
  const output: Attributes = {};
  for (const key of Object.keys(attributes)) {
    const value = attributes[key];
    if (value !== undefined) output[key] = value;
  }
  return output;
}

function isValidationError(error: unknown): boolean {
  if (error instanceof Error) return error.name === "ValiError";
  // MongoDBee validation failures are thrown as plain objects:
  // `{ message: "Validation error", errors, result }`
  return typeof error === "object" && error !== null &&
    (error as { message?: unknown }).message === "Validation error";
}

/**
 * Well-known property key attached to errors thrown by MongoDBee itself to
 * carry a PII-free variant of the message. When present,
 * {@link recordSafeError} records this variant on the span instead of the
 * caller-facing `message` — which may interpolate user-provided ids or scope
 * values. Use {@link Symbol.for} so the key is stable across module instances.
 */
export const TELEMETRY_SAFE_MESSAGE: symbol = Symbol.for(
  "mongodbee.telemetrySafeMessage",
);

/**
 * Builds an `Error` whose caller-visible `message` (and hence `stack`) is
 * exactly `message`, but which also carries `safeMessage` under
 * {@link TELEMETRY_SAFE_MESSAGE} for the span recorder. Use it at throw sites
 * whose message interpolates user-provided values (ids, scope values) so that
 * the span records the structural variant only, byte-for-byte identical
 * caller-facing behaviour aside.
 * @internal
 */
export function errorWithSafeMessage(
  message: string,
  safeMessage: string,
): Error {
  return Object.assign(new Error(message), {
    [TELEMETRY_SAFE_MESSAGE]: safeMessage,
  });
}

/** Reads a {@link TELEMETRY_SAFE_MESSAGE} annotation off an error, if present. */
function readSafeMessage(error: Error): string | undefined {
  const value = (error as unknown as Record<symbol, unknown>)[
    TELEMETRY_SAFE_MESSAGE
  ];
  return typeof value === "string" ? value : undefined;
}

/**
 * Ordered list of `[pattern, replacement]` redactions applied to raw MongoDB
 * driver error messages. Each entry targets a known driver message shape that
 * embeds a user-controlled *value* in its text, redacting the value while
 * keeping the message recognizable. Extend this list as new value-embedding
 * driver patterns surface.
 */
const DRIVER_MESSAGE_REDACTIONS: readonly [RegExp, string][] = [
  // Duplicate-key errors embed the indexed value:
  // `E11000 duplicate key error ... dup key: { email: "user@example.com" }`.
  [/dup key: \{.*\}/s, "dup key: <redacted>"],
];

/**
 * Redacts known value-embedding substrings from a raw driver error message so
 * it can be recorded on a span without leaking indexed values. Returns the
 * message unchanged when no pattern matches.
 */
function scrubDriverErrorMessage(message: string): string {
  let scrubbed = message;
  for (const [pattern, replacement] of DRIVER_MESSAGE_REDACTIONS) {
    scrubbed = scrubbed.replace(pattern, replacement);
  }
  return scrubbed;
}

/**
 * Records an error on a span without ever serializing user data.
 *
 * Priority: (1) validation errors (valibot `ValiError`, MongoDBee validation
 * throws) embed received values in their message/payload and are replaced by a
 * synthetic message; (2) MongoDBee's own errors carrying a
 * {@link TELEMETRY_SAFE_MESSAGE} annotation record that PII-free variant;
 * (3) other errors have their message scrubbed of known value-embedding driver
 * patterns — a changed message is recorded as a synthetic exception without a
 * stack (a stack's first line re-embeds the raw message), an unchanged message
 * keeps the native exception (with its stack) per OpenTelemetry conventions;
 * (4) non-`Error` values fall back to a generic message.
 */
function recordSafeError(span: Span, error: unknown): void {
  let errorType: string;
  let statusMessage: string;
  let safeMessage: string | undefined;
  if (isValidationError(error)) {
    errorType = error instanceof Error ? error.name : "ValidationError";
    statusMessage = SAFE_VALIDATION_MESSAGE;
    span.recordException({ name: errorType, message: SAFE_VALIDATION_MESSAGE });
  } else if (
    error instanceof Error &&
    (safeMessage = readSafeMessage(error)) !== undefined
  ) {
    errorType = error.name;
    statusMessage = safeMessage;
    span.recordException({ name: errorType, message: safeMessage });
  } else if (error instanceof Error) {
    errorType = error.name;
    const scrubbed = scrubDriverErrorMessage(error.message);
    if (scrubbed !== error.message) {
      // A redaction fired: record a synthetic exception carrying only the
      // scrubbed text. No stack — its first line would re-embed the raw value.
      statusMessage = scrubbed;
      span.recordException({ name: errorType, message: scrubbed });
    } else {
      statusMessage = error.message;
      span.recordException(error);
    }
  } else {
    errorType = "Error";
    statusMessage = "Operation failed";
    span.recordException({ name: errorType, message: statusMessage });
  }
  span.setAttribute(A.ERROR_TYPE, errorType);
  span.setStatus({ code: SpanStatusCode.ERROR, message: statusMessage });
}

/**
 * Builds the per-collection span factory, or `null` when telemetry is not
 * enabled. The enabled flag is resolved here, once, at collection creation.
 *
 * @param telemetry - The options given to the collection factory
 * @param target - Collection identity + lazy access to the ambient session
 * @internal
 */
export function createOperationTracer(
  telemetry: TelemetryOptions | undefined,
  target: {
    dbName: string;
    collectionName: string;
    getSession: () => ClientSession | undefined;
  },
): OperationTracer | null {
  if (!telemetry?.enabled) return null;
  const tracer = resolveTracer(telemetry);
  const baseAttributes: Attributes = {
    [A.DB_SYSTEM]: "mongodb",
    [A.DB_NAMESPACE]: target.dbName,
    [A.COLLECTION_NAME]: target.collectionName,
  };

  function withOp<T>(
    operationName: string,
    attributes: Attributes | undefined,
    fn: (op: OpContext) => Promise<T>,
    resultAttributes?: (result: T) => Attributes | undefined,
  ): Promise<T> {
    return tracer.startActiveSpan(
      `${operationName} ${target.collectionName}`,
      {
        kind: SpanKind.CLIENT,
        attributes: prune({
          ...baseAttributes,
          [A.OPERATION_NAME]: operationName,
          ...attributes,
        }),
      },
      async (span) => {
        const op: OpContext = {
          setAttributes: (extra) => span.setAttributes(prune(extra)),
          onRetry: (_error, attempt, _delayMs) => {
            span.setAttribute(A.RETRY_COUNT, attempt);
            const session = target.getSession();
            const counter = session
              ? transactionRetryCounters.get(session)
              : undefined;
            if (counter) counter.count++;
          },
        };
        try {
          const result = await fn(op);
          if (resultAttributes && span.isRecording()) {
            const extra = resultAttributes(result);
            if (extra) span.setAttributes(prune(extra));
          }
          return result;
        } catch (error) {
          recordSafeError(span, error);
          throw error;
        } finally {
          span.end();
        }
      },
    );
  }

  return {
    withOp,
    wrapToArray: (operationName, attributes, toArray) => () =>
      withOp(
        operationName,
        attributes,
        () => toArray(),
        (docs) => ({ [A.RETURNED_ROWS]: docs.length }),
      ),
  };
}

/**
 * Registers a transaction tracer for a MongoDB client so that `withSession`
 * emits `mongodb.transaction` spans. No-op unless `telemetry.enabled` is
 * true. Called by the collection factories; the last enabled registration
 * wins for a given client.
 * @internal
 */
export function registerClientTelemetry(
  client: MongoClient,
  telemetry: TelemetryOptions | undefined,
): void {
  if (!telemetry?.enabled) return;
  const tracer = resolveTracer(telemetry);
  clientTransactionTracers.set(client, {
    withTransaction<T>(
      session: ClientSession,
      run: () => Promise<T>,
    ): Promise<T> {
      return tracer.startActiveSpan(
        "mongodb.transaction",
        {
          kind: SpanKind.INTERNAL,
          attributes: { [A.DB_SYSTEM]: "mongodb" },
        },
        async (span) => {
          const counter = { count: 0 };
          transactionRetryCounters.set(session, counter);
          try {
            const result = await run();
            span.setAttribute(A.TX_OUTCOME, "committed");
            return result;
          } catch (error) {
            span.setAttribute(A.TX_OUTCOME, "aborted");
            recordSafeError(span, error);
            throw error;
          } finally {
            span.setAttribute(A.TX_RETRY_COUNT, counter.count);
            transactionRetryCounters.delete(session);
            span.end();
          }
        },
      );
    },
  });
}

/**
 * Returns the transaction tracer registered for a client, if any.
 * @internal
 */
export function getTransactionTracer(
  client: MongoClient,
): TransactionTracer | undefined {
  return clientTransactionTracers.get(client);
}

const LOGICAL_OPERATORS = new Set(["$and", "$or", "$nor"]);
const MAX_FILTER_DEPTH = 3;

function collectFilterKeys(
  filter: Record<string, unknown>,
  out: Set<string>,
  depth: number,
): void {
  for (const key of Object.keys(filter)) {
    if (LOGICAL_OPERATORS.has(key) && depth < MAX_FILTER_DEPTH) {
      const clauses = filter[key];
      if (Array.isArray(clauses)) {
        for (const clause of clauses) {
          if (clause && typeof clause === "object" && !Array.isArray(clause)) {
            collectFilterKeys(
              clause as Record<string, unknown>,
              out,
              depth + 1,
            );
          }
        }
        continue;
      }
    }
    out.add(key);
  }
}

/**
 * PII-safe summary of a MongoDB filter: the sorted, comma-joined field paths.
 * Logical operators (`$and`/`$or`/`$nor`) are flattened into their clauses'
 * keys. Values are never read. Returns `undefined` for empty filters.
 * @internal
 */
export function filterKeys(filter: unknown): string | undefined {
  if (!filter || typeof filter !== "object" || Array.isArray(filter)) {
    return undefined;
  }
  const keys = new Set<string>();
  collectFilterKeys(filter as Record<string, unknown>, keys, 0);
  if (keys.size === 0) return undefined;
  return [...keys].sort().join(",");
}

/**
 * PII-safe summary of an operator-style update: the sorted, comma-joined
 * `$` operator names (`"$set,$unset"`). Returns `undefined` when the update
 * carries no operators (document-style updates).
 * @internal
 */
export function updateOperators(update: unknown): string | undefined {
  if (!update || typeof update !== "object" || Array.isArray(update)) {
    return undefined;
  }
  const operators = Object.keys(update).filter((key) => key.startsWith("$"));
  if (operators.length === 0) return undefined;
  return operators.sort().join(",");
}

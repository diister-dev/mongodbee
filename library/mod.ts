/**
 * MongoDBee 🍃🐝
 *
 * A type-safe MongoDB wrapper with built-in validation powered by Valibot.
 * Provides schemas, collections with validation, and support for different document types in a single collection.
 *
 * @module
 * @example
 * ```typescript
 * import { collection, multiCollection } from "mongodbee";
 * import * as v from "mongodbee/schema";
 *
 * // Create a type-safe collection with validation
 * const users = await collection(db, "users", {
 *   username: v.string(),
 *   email: v.pipe(v.string(), v.email())
 * });
 *
 * // Create a single collection for multiple document types
 * const catalog = await multiCollection(db, "catalog", {
 *   product: { name: v.string(), price: v.number() },
 *   category: { name: v.string() }
 * });
 * ```
 */

export * from "./src/collection.ts";
export * from "./src/multi-collection.ts";
export * from "./src/multi-collection-model.ts";
export * from "./src/scoped-multi-collection.ts";
export * from "./src/mongodb.ts";
export * from "./src/indexes.ts";
export * from "./src/type-definition.ts";
export * from "./src/duplicate-key.ts";
export * from "./src/config.ts";
export * from "./src/security.ts";
export * from "./src/runtime-config.ts";
export * from "./src/ids.ts";
export { partial, removeField } from "./src/sanitizer.ts";
export {
  addToSet,
  increment,
  isUpdateOperator,
  max,
  min,
  type OperatorFor,
  pull,
  push,
  type UpdateFieldValue,
  UpdateOperator,
  type UpdateOperatorName,
} from "./src/update-operators.ts";
export { DocumentValidationError } from "./src/validation-error.ts";
export type { Page } from "./src/page.ts";
export type { TelemetryOptions } from "./src/telemetry.ts";
export {
  type LogLevel,
  type LogRecord,
  type LogSink,
  setLogSink,
} from "./src/utils/logger.ts";
export * from "./src/computed.ts";
export {
  type ComputedField,
  type ComputedLocation,
  type ComputedSchemas,
  ComputedTopology,
  ComputedTopologyError,
  computedTopology,
} from "./src/computed-topology.ts";
export {
  type ApplyComputedOptions,
  type ApplyComputedResult,
  applyComputed,
  type CheckComputedOptions,
  type CheckComputedResult,
  type ComputedDrift,
  ComputedEntriesExceededError,
  checkComputed,
  repairComputed,
} from "./src/computed-apply.ts";
export {
  type ComputedRegistrationOptions,
  ComputedNotRegisteredError,
  ComputedRequiresTransactionError,
  ComputedUnsupportedWriteError,
  computedRegistration,
  DEFAULT_INLINE_RECOMPUTE_LIMIT,
  registerComputed,
  unregisterComputed,
} from "./src/computed-maintenance.ts";
export {
  COMPUTED_FENCES_COLLECTION,
  COMPUTED_PENDING_COLLECTION,
  type ComputedMark,
  type DrainComputedOptions,
  type DrainComputedResult,
  drainComputedPending,
  type PendingComputedSummary,
  pendingComputed,
} from "./src/computed-marks.ts";
export type { ReadOptions } from "./src/read-preference.ts";
export {
  type AnyReader,
  type CompositeReader,
  DEFAULT_READER_LIMITS,
  type DeepReadonly,
  type FrozenDate,
  invalidateAllReaders,
  type Plain,
  type QueryReader,
  type ReaderArgument,
  ReaderArgumentError,
  ReaderDatabaseError,
  ReaderDefinitionError,
  ReaderDirectReadError,
  type ReaderLimits,
  ReaderNotRegisteredError,
  type ReaderQuery,
  type ReaderRegistrationOptions,
  type ReaderStats,
  reader,
  registerReaders,
  requestReaderStats,
  unregisterReaders,
} from "./src/readers.ts";

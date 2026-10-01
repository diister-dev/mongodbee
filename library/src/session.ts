import type { ClientSession, Db, MongoClient } from "../mod.ts";
import * as m from "mongodb";
import { contextVariable } from "./context-variable.ts";
import { PRIMARY } from "./read-preference.ts";
import { asMongodbeeWrite, invalidateReads } from "./request-context.ts";
import { getTransactionTracer } from "./telemetry.ts";
import {
  runInTransactionScope,
  runOutsideTransactionScope,
  type TransactionRun,
} from "./transaction-scope.ts";
import { createLogger } from "./utils/logger.ts";

const log = createLogger("session");

/**
 * Options of a transaction opened by `withSession`. The read preference is
 * not configurable: a transaction always runs on the primary.
 */
export type WithSessionOptions = Omit<
  m.TransactionOptions,
  "readPreference"
> & {
  /**
   * Re-run the whole callback when the transaction fails with a
   * `TransientTransactionError` (e.g. a primary election), for up to 120 s.
   * Off by default: only enable it when the callback has no side effect
   * outside the transaction.
   */
  retry?: boolean;
};

/** Same budget as the driver's `ClientSession.withTransaction`. */
const TRANSACTION_RETRY_BUDGET_MS = 120_000;

/** A commit that hit `maxCommitTimeMS` is not retried, as in the driver. */
const MAX_TIME_MS_EXPIRED = 50;

function hasErrorLabel(error: unknown, label: string): boolean {
  return error instanceof m.MongoError && error.hasErrorLabel(label);
}

/**
 * The transaction options: primary, snapshot reads and majority writes by
 * default. The client's read preference (e.g. `primaryPreferred` in the URI)
 * never leaks into the transaction, where the driver would reject it.
 */
function transactionOptions(
  options: WithSessionOptions | undefined,
): m.TransactionOptions {
  const { retry: _retry, ...rest } = options ?? {};
  return {
    readConcern: { level: "snapshot" },
    writeConcern: { w: "majority" },
    ...rest,
    readPreference: PRIMARY,
  };
}

/**
 * Commits, retrying while the outcome is unknown (network error, primary
 * stepdown): committing twice is safe, the server deduplicates it.
 */
async function commitWithRetry(
  session: ClientSession,
  startedAt: number,
): Promise<void> {
  while (true) {
    try {
      await session.commitTransaction();
      return;
    } catch (error) {
      const retryable =
        hasErrorLabel(error, "UnknownTransactionCommitResult") &&
        !(
          error instanceof m.MongoServerError &&
          error.code === MAX_TIME_MS_EXPIRED
        ) &&
        Date.now() - startedAt < TRANSACTION_RETRY_BUDGET_MS;
      if (!retryable) throw error;
    }
  }
}

/**
 * Checks if MongoDB transactions are enabled on the current database
 *
 * This function uses lightweight administrative commands instead of creating test collections
 * to determine if transactions are supported. Transactions require either a replica set
 * or a sharded cluster configuration.
 *
 * Results are cached per MongoClient to avoid repeated network calls.
 *
 * @param mongoClient - MongoDB client instance
 * @param mongoDb - MongoDB database instance
 * @returns A promise that resolves to true if transactions are enabled, false otherwise
 * @internal
 */
export async function checkTransactionEnabled(
  mongoClient: MongoClient,
  mongoDb: Db,
): Promise<boolean> {
  // Check cache first
  const cachedResult = transactionSupportCache.get(mongoClient);
  if (cachedResult !== undefined) {
    return cachedResult;
  }

  try {
    // First, check using the hello command (most efficient)
    const helloResult = await mongoDb.command({ hello: 1 });

    // Transactions are supported if:
    // 1. It's a replica set (has setName)
    // 2. It's a mongos instance (sharded cluster)
    if (helloResult.setName || helloResult.msg === "isdbgrid") {
      transactionSupportCache.set(mongoClient, true);
      return true;
    }

    // Fallback: check serverStatus for additional information
    const serverStatus = await mongoDb.command({ serverStatus: 1 });

    // Check if it's a replica set via serverStatus
    const isReplicaSet = serverStatus.repl && serverStatus.repl.setName;
    // Check if it's a mongos instance
    const isMongos = serverStatus.process === "mongos";

    const supportsTransactions = !!(isReplicaSet || isMongos);
    transactionSupportCache.set(mongoClient, supportsTransactions);
    return supportsTransactions;
  } catch (error) {
    // If administrative commands fail, fall back to the original method
    // This might happen in restricted environments
    log.warn(
      "Unable to check transaction support via administrative commands, falling back to test transaction:",
      error,
    );

    const session = mongoClient.startSession();
    const collectionId = `transaction_test_${crypto.randomUUID()}`;
    try {
      session.startTransaction({ readPreference: PRIMARY });
      await mongoDb.collection(collectionId).insertOne(
        { test: true },
        {
          session,
        },
      );
      await mongoDb.collection(collectionId).deleteOne(
        { test: true },
        {
          session,
        },
      );
      await session.commitTransaction();
      transactionSupportCache.set(mongoClient, true);
      return true;
    } catch (_) {
      await session.abortTransaction();
      transactionSupportCache.set(mongoClient, false);
      return false;
    } finally {
      await session.endSession();
      await mongoDb
        .collection(collectionId)
        .drop()
        .catch(() => {}); // Ignore drop errors
    }
  }
}

/** A transaction's session, and the `outsideTransaction` call it was opened in. */
type AmbientSession = {
  readonly session: ClientSession;
  readonly detachment: object | undefined;
};

const detachment = contextVariable<object>("mongodbee.outsideTransaction");

/**
 * Runs `fn` outside the ambient `withSession` transaction, on every client:
 * the typed operations it makes see no session and commit on their own,
 * `insideTransaction()` is false and `afterCommit` runs right away. A
 * `withSession` inside `fn` opens a transaction of its own.
 *
 * For writes that must not join the caller's transaction, whose conflict
 * would abort the caller's work or whose effect must survive its rollback:
 * a job claim, a lease, a cross-pod lock.
 *
 * @example
 * ```typescript
 * await withSession(async () => {
 *   const claimed = await outsideTransaction(() =>
 *     jobs.findOneAndUpdate("job", { _id: id, status: "pending" }, {
 *       status: "running",
 *     }),
 *   );
 * });
 * ```
 */
export function outsideTransaction<T>(fn: () => T): T {
  return detachment.run({}, () => runOutsideTransactionScope(fn));
}

const sessionContextMap = new WeakMap<
  MongoClient,
  ReturnType<typeof createSessionContext>
>();
const transactionSupportCache = new WeakMap<MongoClient, boolean>();

/**
 * Gets or creates a session context for a MongoDB client
 *
 * Creates a session context for managing MongoDB transactions. This context
 * provides utilities to transparently propagate sessions across async boundaries,
 * making transaction management simpler.
 *
 * @param mongoClient - MongoDB client instance
 * @returns A session context that can be used for transaction management
 * @example
 * ```typescript
 * const client = new MongoClient("mongodb://localhost:27017");
 * await client.connect();
 *
 * const { withSession } = getSessionContext(client);
 *
 * // Use a transaction with automatic commit/rollback
 * await withSession(async () => {
 *   // All operations using the session will be part of the same transaction
 *   await users.insertOne({ name: "Alice" });
 *   await orders.insertOne({ userId: user._id });
 * });
 * ```
 */
export function getSessionContext(
  mongoClient: MongoClient,
): ReturnType<typeof createSessionContext> {
  let context = sessionContextMap.get(mongoClient);
  if (!context) {
    context = createSessionContext(mongoClient);
    sessionContextMap.set(mongoClient, context);
  }
  return context;
}

/**
 * Creates a new session context for MongoDB transactions
 *
 * This function creates a context that manages MongoDB sessions using AsyncLocalStorage,
 * allowing for transparent session propagation across async boundaries.
 *
 * @param mongoClient - MongoDB client instance
 * @returns An object with functions to manage sessions and transactions
 * @internal
 */
export function createSessionContext(mongoClient: MongoClient): {
  /**
   * Gets the current MongoDB session from the async context
   *
   * @returns The current MongoDB session or undefined if no session is active
   */
  getSession: () => ClientSession | undefined;

  /**
   * Executes a function within a MongoDB session context
   *
   * If there's already an active session, it reuses it.
   * Otherwise, it creates a new session and automatically manages
   * the transaction lifecycle (start, commit, abort).
   *
   * The transaction always runs on the primary, whatever the client's read
   * preference. `options` only apply to the outermost call, the one that
   * opens the transaction.
   *
   * @param fn - The function to execute within the session context
   * @param options - Transaction concerns and the opt-in transient retry
   * @returns A promise that resolves to the function's result
   */
  withSession: <T>(
    fn: (session?: ClientSession) => Promise<T>,
    options?: WithSessionOptions,
  ) => Promise<T>;
} {
  let warningDisplayed = false;
  let transactionsEnabledPromise: Promise<boolean> | undefined;

  const asyncSession = contextVariable<AmbientSession>("mongodbee.session");

  function getSession(): ClientSession | undefined {
    const ambient = asyncSession.get();
    return ambient && ambient.detachment === detachment.get()
      ? ambient.session
      : undefined;
  }

  async function withSession<T>(
    fn: (session?: ClientSession) => Promise<T>,
    options?: WithSessionOptions,
  ): Promise<T> {
    // Lazy loading: ne vérifie le support des transactions qu'au premier appel de withSession
    if (!transactionsEnabledPromise) {
      transactionsEnabledPromise = checkTransactionEnabled(
        mongoClient,
        mongoClient.db(),
      );
    }

    const transactionsEnabled = await transactionsEnabledPromise;

    if (!warningDisplayed && !transactionsEnabled) {
      log.warn(
        "MongoDB transactions are not enabled. This may cause issues with concurrent operations.",
      );
      warningDisplayed = true;
    }

    const session = getSession();
    if (!transactionsEnabled || session) {
      return await fn(session);
    }

    const newSession = mongoClient.startSession();
    const ambient = { session: newSession, detachment: detachment.get() };
    const committed = await asyncSession.run(ambient, () => {
      const txOptions = transactionOptions(options);
      const startedAt = Date.now();
      const canRetry = (e: unknown) =>
        options?.retry === true &&
        hasErrorLabel(e, "TransientTransactionError") &&
        Date.now() - startedAt < TRANSACTION_RETRY_BUDGET_MS;
      const execute = async () => {
        try {
          while (true) {
            newSession.startTransaction(txOptions);
            let result: TransactionRun<T>;
            try {
              result = await runInTransactionScope(() => fn(newSession));
            } catch (e) {
              if (newSession.inTransaction()) {
                await newSession.abortTransaction();
              }
              if (canRetry(e)) continue;
              throw e;
            }
            try {
              await asMongodbeeWrite(() =>
                commitWithRetry(newSession, startedAt),
              );
            } catch (e) {
              if (canRetry(e)) continue;
              throw e;
            } finally {
              invalidateReads();
              for (const invalidate of result.atCommit) invalidate();
            }
            return result;
          }
        } finally {
          await newSession.endSession();
        }
      };
      const txTracer = getTransactionTracer(mongoClient);
      return txTracer
        ? txTracer.withTransaction(newSession, execute)
        : execute();
    });
    for (const callback of committed.afterCommit) {
      try {
        await callback();
      } catch (error) {
        log.error("An afterCommit callback failed after the commit", error);
      }
    }
    return committed.result;
  }

  return {
    getSession,
    withSession,
  };
}

const SESSION_OPTIONS_POSITION: Readonly<Record<string, number>> = {
  insertOne: 1,
  insertMany: 1,
  updateOne: 2,
  updateMany: 2,
  replaceOne: 2,
  deleteOne: 1,
  deleteMany: 1,
  findOneAndUpdate: 2,
  findOneAndReplace: 2,
  findOneAndDelete: 1,
  bulkWrite: 1,
  initializeOrderedBulkOp: 0,
  initializeUnorderedBulkOp: 0,
  find: 1,
  findOne: 1,
  aggregate: 1,
  countDocuments: 1,
  distinct: 2,
};

/**
 * A driver collection whose operations join the ambient `withSession`
 * transaction: every CRUD, read and bulk call that is not given a `session`
 * option gets the current one, so a raw write rolls back with the
 * transaction instead of committing on its own. Passing `session`, even
 * `session: undefined`, keeps the call as written. `watch`, DDL (indexes,
 * `drop`, `rename`) and `estimatedDocumentCount` are left alone: none of
 * them may run inside a transaction.
 *
 * `collection.collection`, and `.collection` on a multi-collection or a
 * scoped multi-collection, are already wrapped this way.
 *
 * @example
 * ```typescript
 * const raw = ambientSessionCollection(db.collection("jobs"));
 * await withSession(async () => {
 *   await raw.updateOne({ _id: id }, { $inc: { attempts: 1 } });
 * });
 * ```
 */
export function ambientSessionCollection<T extends m.Document>(
  target: m.Collection<T>,
): m.Collection<T> {
  const context = getSessionContext(target.db.client as MongoClient);
  return new Proxy(target, {
    get(object, property, receiver) {
      const value = Reflect.get(object, property, receiver);
      if (typeof property !== "string" || typeof value !== "function") {
        return value;
      }
      const position = SESSION_OPTIONS_POSITION[property];
      if (position === undefined) return value.bind(object);
      return (...args: unknown[]) => {
        const session = context.getSession();
        const given = args[position];
        if (
          session === undefined ||
          (typeof given === "object" && given !== null && "session" in given)
        ) {
          return value.apply(object, args);
        }
        const next = Array.from(
          { length: Math.max(args.length, position + 1) },
          (_, index) => args[index],
        );
        next[position] = { ...(given as object | undefined), session };
        return value.apply(object, next);
      };
    },
  });
}

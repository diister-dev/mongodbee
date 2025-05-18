import type { ClientSession, Db, MongoClient } from "../mod.ts";
import { AsyncLocalStorage } from "node:async_hooks";

/**
 * Checks if MongoDB transactions are enabled on the current database
 * 
 * @param mongoClient - MongoDB client instance
 * @param mongoDb - MongoDB database instance
 * @returns A promise that resolves to true if transactions are enabled, false otherwise
 * @internal
 */
export async function checkTransactionEnabled(mongoClient: MongoClient, mongoDb: Db) {
    const session = mongoClient.startSession();
    const collectionId = `transaction_test_${crypto.randomUUID()}`;
    try {
        session.startTransaction();
        await mongoDb.collection(collectionId).insertOne({ test: true }, { session });
        await mongoDb.collection(collectionId).deleteOne({ test: true }, { session });
        await session.commitTransaction();
        return true;
    } catch (_) {
        await session.abortTransaction();
        return false;
    } finally {
        await session.endSession();
        await mongoDb.collection(collectionId).drop();
    }
}

const sessionContextMap = new WeakMap<MongoClient, Awaited<ReturnType<typeof createSessionContext>>>();

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
 * const { withSession } = await getSessionContext(client);
 * 
 * // Use a transaction with automatic commit/rollback
 * await withSession(async () => {
 *   // All operations using the session will be part of the same transaction
 *   await users.insertOne({ name: "Alice" });
 *   await orders.insertOne({ userId: user._id });
 * });
 * ```
 */
export async function getSessionContext(mongoClient: MongoClient) : ReturnType<typeof createSessionContext> {
    let context = sessionContextMap.get(mongoClient);
    if (!context) {
        context = await createSessionContext(mongoClient);
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
export async function createSessionContext(mongoClient: MongoClient) : Promise<{
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
     * @param fn - The function to execute within the session context
     * @returns A promise that resolves to the function's result
     */
    withSession: <T>(fn: (session?: ClientSession) => Promise<T>) => Promise<T>;
}> {
    let warningDisplayed = false;
    const transactionsEnabled = await checkTransactionEnabled(mongoClient, mongoClient.db());

    const asyncSession = new AsyncLocalStorage<ClientSession | undefined>();

    function getSession() {
        return asyncSession.getStore();
    }

    async function withSession<T>(fn: (session?: ClientSession) => Promise<T>) {
        if (!warningDisplayed && !transactionsEnabled) {
            console.warn("MongoDB transactions are not enabled. This may cause issues with concurrent operations.");
            warningDisplayed = true;
        }

        const session = getSession();    
        if (!transactionsEnabled || session) {
            return await fn(session);
        }
        
        const newSession = mongoClient.startSession();
        return asyncSession.run(newSession, async () => {
            try {
                newSession.startTransaction();
                const result = await fn(newSession);
                await newSession.commitTransaction();
                return result as T;
            } catch(e) {
                if(newSession.inTransaction()) {
                    await newSession.abortTransaction();
                }
                throw e;
            } finally {
                await newSession.endSession();
            }
        });
    }

    return {
        getSession,
        withSession,
    }
}
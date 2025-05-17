import type { ClientSession, Db, MongoClient } from "../mod.ts";
import { AsyncLocalStorage } from "node:async_hooks";

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

export async function getSessionContext(mongoClient: MongoClient) : ReturnType<typeof createSessionContext> {
    let context = sessionContextMap.get(mongoClient);
    if (!context) {
        context = await createSessionContext(mongoClient);
        sessionContextMap.set(mongoClient, context);
    }
    return context;
}

export async function createSessionContext(mongoClient: MongoClient) : Promise<{
    getSession: () => ClientSession | undefined;
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
        asyncSession.enterWith(newSession);
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
    }

    return {
        getSession,
        withSession,
    }
}
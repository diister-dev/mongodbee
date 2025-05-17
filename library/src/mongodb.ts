import * as base from "mongodb"

/**
 * Extended MongoDB database type that includes a reference to its client
 */
export type Db = base.Db & {
    client: MongoClient;
};

/**
 * Enhanced MongoDB client with improved type support for the MongoDBee library
 * 
 * This class extends the standard MongoDB client to provide better integration
 * with MongoDBee's type system and features.
 */
export class MongoClient extends base.MongoClient {
    /**
     * Gets a database with added MongoDBee functionality
     * 
     * @param args - Same parameters as the standard MongoDB client db method
     * @returns A database instance with added MongoDBee functionality
     */
    override db(...args: Parameters<typeof base.MongoClient.prototype.db>) : Db {
        const db = super.db(...args) as Db;
        db.client = this;
        return db;
    }
}

/**
 * MongoDB ClientSession type for transaction support
 */
export const ClientSession = base.ClientSession;
export type ClientSession = base.ClientSession;
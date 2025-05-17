import * as base from "mongodb"

export type Db = base.Db & {
    client: MongoClient;
};

export class MongoClient extends base.MongoClient {
    override db(...args: Parameters<typeof base.MongoClient.prototype.db>) : Db {
        const db = super.db(...args) as Db;
        db.client = this;
        return db;
    }
}

export const ClientSession = base.ClientSession;
export type ClientSession = base.ClientSession;
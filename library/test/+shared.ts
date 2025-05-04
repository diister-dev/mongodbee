import { type Db, MongoClient } from "mongodb";

function computePrefix(prefix: string) {
    // Replace all non-alphanumeric characters with "_"
    const prefix_sanitize = prefix.replace(/[^a-zA-Z0-9]/g, "_").toLocaleLowerCase();
    // Remove all leading and trailing "_"
    const prefix_fixed = prefix_sanitize.replace(/^_+|_+$/g, "");

    return `@TEST_${prefix_fixed}@`;
}

function randomDBName(prefix: string) {
    return `${computePrefix(prefix)}${crypto.randomUUID().replace(/-/g, "").substring(0, 8)}`;
}

async function deleteTestDatabase(client: MongoClient, prefix = "UNKNOWN") {
    const dbs = await client.db().admin().listDatabases();
    for (const db of dbs.databases) {
        if (db.name.startsWith(computePrefix(prefix))) {
            await client.db(db.name).dropDatabase();
        }
    }
}

export async function withDatabase(prefix: string, work: (db: Db) => Promise<void>) {
    const client = new MongoClient("mongodb://localhost:27017");
    await deleteTestDatabase(client, prefix);
    const db = client.db(randomDBName(prefix));
    try {
        await work(db);
    } finally {
        await db.dropDatabase(); // Uncomment to debug after test
        await client.close();
    }
}
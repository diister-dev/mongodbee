import { MongoClient, collection, multiCollection } from "@diister/mongodbee"
import { checkMigrationStatus, discoverMultiCollectionInstances } from "@diister/mongodbee/migration";
import { schemas } from "./schemas.ts"

console.log("Hello application !");
const mongoUrl = Deno.env.get("MONGODB_URI")!;
const mongodbName = Deno.env.get("MONGODB_DATABASE")!;
const client = new MongoClient(mongoUrl);
await client.connect();
const db = client.db(mongodbName);

const migrationState = await checkMigrationStatus({ db });

if(!migrationState.ok) {
    console.error("Migration check failed:", migrationState.message);
    Deno.exit(1);
}

// App logic here

client.close();
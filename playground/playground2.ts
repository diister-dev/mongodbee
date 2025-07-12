import { MongoClient } from "mongodb"

const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);

const db = client.db("test_diivento");

const itemsCollection = db.collection("items");
const itemsMetadataCollection = db.collection("items_metadata");

// Create random items
const items = Array.from({ length: 100000 }, (_, i) => ({
    _id: `item:${i + 1}`,
    name: `Item ${i + 1}`,
    value: Math.floor(Math.random() * 100),
}));

// Create metadata for each item
const itemsMetadata = items.map(item => ({
    itemId: item._id,
    rand: Math.floor(Math.random() * 10),
}));

await itemsCollection.deleteMany({});
await itemsMetadataCollection.deleteMany({});

console.time("Inserting documents...");
await itemsCollection.insertMany(items);
await itemsMetadataCollection.insertMany(itemsMetadata);
console.timeEnd("Inserting documents...");

console.time("Finding documents...");
const cursor = itemsCollection.find({}).sort({ _id: 1 });
let i = 0;
while (await cursor.hasNext()) {
    console.log(`Processing document ${i + 1}...`);
    const doc = await cursor.next();
    const metadata = await itemsMetadataCollection.findOne({ itemId: doc?._id });
    if(doc?.value == 5 && metadata?.rand == 5) {
        console.log(doc, i);
        break;
    }
    i++;
}
console.timeEnd("Finding documents...");


// console.time("Finding documents...");
// const itemsObtained = await itemsCollection.find({}).sort({ _id: 1 }).toArray();
// const itemsMetadataObtained = await itemsMetadataCollection.find({}).toArray();
// for(let i = 0; i < itemsObtained.length; i++) {
//     const doc = itemsObtained[i];
//     const metadata = itemsMetadataObtained.find(m => m.itemId === doc?._id);
//     if(doc?.value == 5 && metadata?.rand == 5) {
//         console.log(doc, i);
//         break;
//     }
// }
// console.timeEnd("Finding documents...");
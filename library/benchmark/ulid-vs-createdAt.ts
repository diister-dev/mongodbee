import { MongoClient } from "mongodb";
import { ulid } from "@std/ulid";
import * as v from "../src/schema.ts";
import { ulidTimeMatcher } from "../src/ulid-date-matcher.ts";
import { collection } from "../src/collection.ts";
import { dbId } from "../mod.ts";

// Configuration
const DOCUMENT_COUNT = 10_000; // Number of documents to insert

const url = "mongodb://localhost:27017";
const client = new MongoClient(url);

const dbName = "@BENCHMARK_ULID_VS_CREATEDAT";

const collectionName = "ulid-vs-createdAt";

await client.db(dbName).dropDatabase();
const db = client.db(dbName);

Deno.bench("Create (Id only)", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  b.start();
  await dbase.insertMany(
    Array.from({ length: DOCUMENT_COUNT }, () => ({})),
  )
  b.end();
});

Deno.bench("Create (Id and createdAt)", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  const now = new Date();

  b.start();
  await dbase.insertMany(
    Array.from({ length: DOCUMENT_COUNT }, () => ({
      createdAt: now,
    })),
  )
  b.end();
});

Deno.bench("Create with index (Id and createdAt)", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  const now = new Date();

  await dbase.createIndex({ createdAt: 1 });

  b.start();
  await dbase.insertMany(
    Array.from({ length: DOCUMENT_COUNT }, () => ({
      createdAt: now,
    })),
  )
  b.end();
});

Deno.bench("Select by date (Id only)", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  const now = new Date();

  // Prepare data
  const data = Array.from({ length: DOCUMENT_COUNT }, (_, i) => ({
    _id: `user:${ulid(now.getTime() - (DOCUMENT_COUNT / 2) + i)}`,
  }));
  await dbase.insertMany(data as any);

  const compGt = `user:${ulid(now.getTime()).slice(0, 10)}${'0'.repeat(16)}`;

  b.start();
  const afterNow = await dbase.aggregate([
    { $match: { _id: { $gte: compGt as any } } },
    { $project: { _id: 1 } },
  ]).toArray();
  b.end();
});

Deno.bench("Select by date regex (Id only)", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  const now = new Date();

  // Prepare data
  const data = Array.from({ length: DOCUMENT_COUNT }, (_, i) => ({
    _id: `user:${ulid(now.getTime() - (DOCUMENT_COUNT / 2) + i)}`,
  }));
  await dbase.insertMany(data as any);

  b.start();
  const afterNow = await dbase.aggregate([
    { $match: { _id: { $regex: ulidTimeMatcher({ start: now, startEqual: true, prefix: "user:" }) as any } } },
    { $project: { _id: 1 } },
  ]).toArray();
  b.end();
});

Deno.bench("Select by date (Id and createdAt)", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  const now = new Date();

  // Prepare data
  const data = Array.from({ length: DOCUMENT_COUNT }, (_, i) => ({
    _id: `user:${ulid(now.getTime() - (DOCUMENT_COUNT / 2) + i)}`,
    createdAt: new Date(now.getTime() - (DOCUMENT_COUNT / 2) + i),
  }));
  await dbase.insertMany(data as any);

  b.start();
  const afterNow = await dbase.aggregate([
    { $match: { createdAt: { $gte: now } } },
    { $project: { _id: 1, createdAt: 1 } },
  ]).toArray();
  b.end();
});

Deno.bench("Select by date with index (Id and createdAt)", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  const now = new Date();

  // Prepare data
  const data = Array.from({ length: DOCUMENT_COUNT }, (_, i) => ({
    _id: `user:${ulid(now.getTime() - (DOCUMENT_COUNT / 2) + i)}`,
    createdAt: new Date(now.getTime() - (DOCUMENT_COUNT / 2) + i),
  }));
  await dbase.insertMany(data as any);

  await dbase.createIndex({ createdAt: 1 });

  b.start();
  const afterNow = await dbase.aggregate([
    { $match: { createdAt: { $gte: now } } },
    { $project: { _id: 1, createdAt: 1 } },
  ]).toArray();
  b.end();
});

Deno.bench("Select by date (Id only) with date", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  const now = new Date();

  // Prepare data
  const data = Array.from({ length: DOCUMENT_COUNT }, (_, i) => ({
    _id: `user:${ulid(now.getTime() - (DOCUMENT_COUNT / 2) + i)}`,
    createdAt: new Date(now.getTime() - (DOCUMENT_COUNT / 2) + i),
  }));
  await dbase.insertMany(data as any);

  const compGt = `user:${ulid(now.getTime()).slice(0, 10)}${'0'.repeat(16)}`;

  b.start();
  const afterNow = await dbase.aggregate([
    { $match: { _id: { $gte: compGt as any } } },
    { $project: { _id: 1 } },
  ]).toArray();
  b.end();
});

Deno.bench("Select by date regex (Id only) with date", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  const now = new Date();

  // Prepare data
  const data = Array.from({ length: DOCUMENT_COUNT }, (_, i) => ({
    _id: `user:${ulid(now.getTime() - (DOCUMENT_COUNT / 2) + i)}`,
    createdAt: new Date(now.getTime() - (DOCUMENT_COUNT / 2) + i),
  }));
  await dbase.insertMany(data as any);

  b.start();
  const afterNow = await dbase.aggregate([
    { $match: { _id: { $regex: ulidTimeMatcher({ start: now, startEqual: true, prefix: "user:" }) as any } } },
    { $project: { _id: 1 } },
  ]).toArray();
  b.end();
});

Deno.bench("Select by date (Id only) with date with :user", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  const now = new Date();

  // Prepare data
  const data = Array.from({ length: DOCUMENT_COUNT }, (_, i) => ({
    _id: `user:${ulid(now.getTime() - (DOCUMENT_COUNT / 2) + i)}`,
    createdAt: new Date(now.getTime() - (DOCUMENT_COUNT / 2) + i),
  }));
  await dbase.insertMany(data as any);

  const compGt = `user:${ulid(now.getTime()).slice(0, 10)}${'0'.repeat(16)}`;

  b.start();
  const afterNow = await dbase.aggregate([
    { $match: { _id: {
        $gte: compGt as any,
        $regex: /^user:/,
      } } },
    { $project: { _id: 1 } },
  ]).toArray();
  b.end();
});

Deno.bench("Select by date (Id and createdAt) with :user", async (b) => {
  const dbase = await db.collection(collectionName);
  await dbase.deleteMany({});

  const now = new Date();

  // Prepare data
  const data = Array.from({ length: DOCUMENT_COUNT }, (_, i) => ({
    _id: `user:${ulid(now.getTime() - (DOCUMENT_COUNT / 2) + i)}`,
    createdAt: new Date(now.getTime() - (DOCUMENT_COUNT / 2) + i),
  }));
  await dbase.insertMany(data as any);

  b.start();
  const afterNow = await dbase.aggregate([
    { $match: {
        _id: {
          $regex: /^user:/,
        },
        createdAt: { $gte: now }
      } },
    { $project: { _id: 1, createdAt: 1 } },
  ]).toArray();
  b.end();
});
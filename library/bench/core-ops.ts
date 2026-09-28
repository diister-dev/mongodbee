import process from "node:process";
import { MongoClient } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { getSessionContext } from "../src/session.ts";

const URI = process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017";
const ITERATIONS = Number(process.env.BENCH_ITERATIONS ?? "300");
const READ_SIZE = 500;
const SCOPE = "exposition:benchcoreaaaa1";

const fields = {
  name: v.pipe(v.string(), v.trim(), v.nonEmpty()),
  email: v.pipe(v.string(), v.toLowerCase(), v.regex(/^[^@\s]+@[^@\s]+$/)),
  age: v.pipe(v.number(), v.integer(), v.minValue(0)),
  tags: v.array(v.string()),
  address: v.object({ city: v.string(), zip: v.optional(v.string()) }),
};

type Stats = { median: number; p95: number };

function stats(samples: number[]): Stats {
  const sorted = [...samples].sort((a, b) => a - b);
  const at = (q: number) =>
    sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
  return { median: at(0.5), p95: at(0.95) };
}

async function timed(
  times: number,
  run: (index: number) => Promise<unknown>,
): Promise<Stats> {
  const samples: number[] = [];
  for (let index = 0; index < times; index++) {
    const start = performance.now();
    await run(index);
    samples.push(performance.now() - start);
  }
  return stats(samples);
}

const person = (i: number) => ({
  name: `Person ${i}`,
  email: `person${i}@example.test`,
  age: 20 + (i % 50),
  tags: ["a", "b", `t${i % 7}`],
  address: { city: `City ${i % 13}`, zip: "75001" },
});

const results: Record<string, Stats> = {};
const record = (label: string, value: Stats) => {
  results[label] = value;
};

const client = new MongoClient(URI);
await client.connect();
getSessionContext(client);
const db = client.db(`bench_core_${Date.now()}`);

try {
  const raw = db.collection<{ _id: string } & ReturnType<typeof person>>("raw");
  const rawIds: string[] = [];
  record(
    "raw driver insertOne",
    await timed(ITERATIONS, async (i) => {
      const _id = `raw:${i}`;
      await raw.insertOne({ _id, ...person(i) });
      rawIds.push(_id);
    }),
  );
  await raw.insertMany(
    Array.from({ length: READ_SIZE }, (_, i) => ({
      _id: `rawbulk:${i}`,
      ...person(i),
    })),
  );
  record(
    "raw driver findOne by id",
    await timed(ITERATIONS, (i) =>
      raw.findOne({ _id: rawIds[i % rawIds.length]! }),
    ),
  );
  record(
    `raw driver find ${READ_SIZE} toArray`,
    await timed(30, () =>
      raw
        .find({ age: { $gte: 0 } })
        .limit(READ_SIZE)
        .toArray(),
    ),
  );
  record(
    "raw driver updateOne",
    await timed(ITERATIONS, (i) =>
      raw.updateOne(
        { _id: rawIds[i % rawIds.length]! },
        { $set: { age: i % 90 } },
      ),
    ),
  );

  const users = await collection(db, "users", fields, {
    schemaManagement: "auto",
  });
  const userIds: Parameters<typeof users.getById>[0][] = [];
  record(
    "collection insertOne",
    await timed(ITERATIONS, async (i) => {
      userIds.push(await users.insertOne(person(i)));
    }),
  );
  for (let i = 0; i < READ_SIZE; i += 100) {
    await Promise.all(
      Array.from({ length: 100 }, (_, j) => users.insertOne(person(i + j))),
    );
  }
  record(
    "collection getById",
    await timed(ITERATIONS, (i) => users.getById(userIds[i % userIds.length]!)),
  );
  record(
    `collection find ${READ_SIZE} toArray`,
    await timed(30, () => users.find({}, { limit: READ_SIZE }).toArray()),
  );
  record(
    "collection paginate 50",
    await timed(100, () => users.paginate({}, { limit: 50 })),
  );
  record(
    "collection updateOne",
    await timed(ITERATIONS, (i) =>
      users.updateOne(
        { _id: userIds[i % userIds.length]! },
        { $set: { age: i % 90 } },
      ),
    ),
  );

  const catalog = await multiCollection(
    db,
    "catalog",
    { person: fields, other: { label: v.string() } },
    { schemaManagement: "auto" },
  );
  const catalogIds: string[] = [];
  record(
    "multi insertOne",
    await timed(ITERATIONS, async (i) => {
      catalogIds.push(await catalog.insertOne("person", person(i)));
    }),
  );
  await catalog.insertMany(
    "person",
    Array.from({ length: READ_SIZE }, (_, i) => person(i)),
  );
  record(
    "multi getById",
    await timed(ITERATIONS, (i) =>
      catalog.getById("person", catalogIds[i % catalogIds.length]!),
    ),
  );
  record(
    `multi find ${READ_SIZE}`,
    await timed(30, () => catalog.find("person", {}, { limit: READ_SIZE })),
  );
  record(
    "multi paginate 50",
    await timed(100, () => catalog.paginate("person", {}, { limit: 50 })),
  );
  record(
    "multi updateOne",
    await timed(ITERATIONS, (i) =>
      catalog.updateOne("person", catalogIds[i % catalogIds.length]!, {
        age: i % 90,
      }),
    ),
  );

  const scoped = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: { person: fields, other: { label: v.string() } },
  });
  const view = scoped.scope(SCOPE);
  const scopedIds: string[] = [];
  record(
    "scoped insertOne",
    await timed(ITERATIONS, async (i) => {
      scopedIds.push(await view.insertOne("person", person(i)));
    }),
  );
  await view.insertMany(
    "person",
    Array.from({ length: READ_SIZE }, (_, i) => person(i)),
  );
  record(
    "scoped getById",
    await timed(ITERATIONS, (i) =>
      view.getById("person", scopedIds[i % scopedIds.length]!),
    ),
  );
  record(
    `scoped find ${READ_SIZE}`,
    await timed(30, () => view.find("person", {}, { limit: READ_SIZE })),
  );
  record(
    `scoped findProject ${READ_SIZE}`,
    await timed(30, () =>
      view.findProject("person", ["name"], {}, { limit: READ_SIZE }),
    ),
  );
  record(
    "scoped paginate 50",
    await timed(100, () => view.paginate("person", {}, { limit: 50 })),
  );
  record(
    "scoped updateOne",
    await timed(ITERATIONS, (i) =>
      view.updateOne("person", scopedIds[i % scopedIds.length]!, {
        age: i % 90,
      }),
    ),
  );

  const parseSamples: number[] = [];
  const doc = { _id: "users:x", ...person(1) };
  const schema = v.object({ _id: v.string(), ...fields });
  for (let round = 0; round < 20; round++) {
    const start = performance.now();
    for (let i = 0; i < 10_000; i++) v.parse(schema, doc);
    parseSamples.push((performance.now() - start) / 10_000);
  }
  record("valibot parse one document", stats(parseSamples));
} finally {
  await db.dropDatabase();
  await client.close();
}

const round = (value: number) => Math.round(value * 1000) / 1000;
for (const [label, value] of Object.entries(results)) {
  console.log(
    `${label.padEnd(34)} median ${String(round(value.median)).padStart(8)} ms   p95 ${String(round(value.p95)).padStart(8)} ms`,
  );
}
if (process.env.BENCH_JSON) {
  console.log(`JSON ${JSON.stringify(results)}`);
}

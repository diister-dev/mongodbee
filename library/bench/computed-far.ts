import process from "node:process";
import { MongoClient } from "../src/mongodb.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import { computedTopology } from "../src/computed-topology.ts";
import { registerComputed } from "../src/computed-maintenance.ts";
import { drainComputedPending } from "../src/computed-marks.ts";
import { getSessionContext } from "../src/session.ts";

const URI = process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017";
const ITERATIONS = Number(process.env.BENCH_ITERATIONS ?? "200");
const SCOPE = "exposition:benchfar001";

const Organization = defineType({
  schema: v.object({
    name: v.string(),
    status: v.picklist(["pending", "validated"]),
  }),
});

const Membership = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant")),
    organizationId: withIndex(refId("expo_organization")),
    status: v.picklist(["active", "removed"]),
  }),
});

const Participant = defineType({
  schema: v.object({ name: v.string() }),
  computed: {
    validatedOrganizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .through("expo_organization", Organization, (m) => m.organizationId)
      .where((o) => [o.status, "validated"])
      .collect((o) => o._id),
  },
});

const schemas = {
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: {
        participant: Participant,
        org_membership: Membership,
        expo_organization: Organization,
      },
    },
  },
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

async function open(db: Db) {
  registerComputed(db, computedTopology(schemas));
  const expositions = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: schemas.scopedMultiCollections["+expositions"].types,
  });
  return expositions.scope(SCOPE);
}

async function organizationsWith(db: Db, count: number, members: number) {
  const view = await open(db);
  const organizations = await view.insertMany(
    "expo_organization",
    Array.from({ length: count }, (_, i) => ({
      name: `O${i}`,
      status: "validated" as const,
    })),
  );
  const participants = await view.insertMany(
    "participant",
    Array.from({ length: count * members }, (_, i) => ({ name: `P${i}` })),
  );
  const memberships = participants.map((participantId, i) => ({
    participantId,
    organizationId: organizations[Math.floor(i / members)]!,
    status: "active" as const,
  }));
  for (let i = 0; i < memberships.length; i += 1000)
    await view.insertMany("org_membership", memberships.slice(i, i + 1000));
  await drainComputedPending(db, { limit: 100_000 });
  return { view, organizations, participants };
}

async function inDatabase<T>(
  client: MongoClient,
  label: string,
  work: (db: Db) => Promise<T>,
): Promise<T> {
  const db = client.db(`bench_far_${label}_${Date.now()}`);
  try {
    return await work(db);
  } finally {
    await db.dropDatabase();
  }
}

async function farWrite(client: MongoClient, members: number) {
  return await inDatabase(client, `far${members}`, async (db) => {
    const count = members === 1 ? ITERATIONS : 1;
    const { view, organizations } = await organizationsWith(db, count, members);
    const write = await timed(ITERATIONS, (i) =>
      view.updateOne(
        "expo_organization",
        organizations[i % organizations.length]!,
        { status: i % 2 === 0 ? "pending" : "validated" },
      ),
    );
    const start = performance.now();
    const drained = await drainComputedPending(db, { limit: 100_000 });
    return {
      write,
      drainMs: performance.now() - start,
      marks: drained.drained,
    };
  });
}

async function nearLink(client: MongoClient) {
  return await inDatabase(client, "near", async (db) => {
    const { view, organizations, participants } = await organizationsWith(
      db,
      ITERATIONS,
      1,
    );
    return await timed(ITERATIONS, (i) =>
      view.insertOne("org_membership", {
        participantId: participants[i]!,
        organizationId: organizations[(i + 1) % organizations.length]!,
        status: "active",
      }),
    );
  });
}

async function concurrentLinks(client: MongoClient) {
  return await inDatabase(client, "links", async (db) => {
    const { view, organizations } = await organizationsWith(db, 1, 1);
    const participants = await view.insertMany(
      "participant",
      Array.from({ length: 50 }, (_, i) => ({ name: `C${i}` })),
    );
    const start = performance.now();
    await Promise.all(
      participants.map((participantId) =>
        view.insertOne("org_membership", {
          participantId,
          organizationId: organizations[0]!,
          status: "active",
        }),
      ),
    );
    return performance.now() - start;
  });
}

const round = (value: number) => Math.round(value * 100) / 100;
const show = (label: string, value: Stats) =>
  console.log(
    `${label.padEnd(44)} median ${String(round(value.median)).padStart(6)} ms   p95 ${String(round(value.p95)).padStart(6)} ms`,
  );

const client = new MongoClient(URI);
await client.connect();
getSessionContext(client);
try {
  console.log(`== ${ITERATIONS} samples each`);
  for (const members of [1, 100]) {
    const result = await farWrite(client, members);
    show(`far status change reaching ${members} subject(s)`, result.write);
    console.log(
      `${"".padEnd(44)} then drain: ${result.marks} mark(s) in ${round(result.drainMs)} ms`,
    );
  }
  show("near insert creating a link", await nearLink(client));
  console.log(
    `50 concurrent links to one far document      ${round(await concurrentLinks(client))} ms`,
  );
} finally {
  await client.close();
}

import process from "node:process";
import { MongoClient } from "../src/mongodb.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { collection } from "../src/collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import {
  ComputedTopology,
  computedTopology,
} from "../src/computed-topology.ts";
import { registerComputed } from "../src/computed-maintenance.ts";
import { applyComputed } from "../src/computed-apply.ts";
import { getSessionContext } from "../src/session.ts";

const URI = process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017";
const ITERATIONS = Number(process.env.BENCH_ITERATIONS ?? "300");
const SCOPE = "exposition:benchaaaa01";
const OTHER_SCOPES = ["exposition:benchbbbb02", "exposition:benchcccc03"];

const Membership = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant")),
    organizationId: refId("expo_organization"),
    status: v.picklist(["active", "removed"]),
    note: v.optional(v.string()),
  }),
});

const Participant = defineType({
  schema: v.object({
    name: v.string(),
    userId: withIndex(v.string(), { global: true }),
  }),
  computed: {
    organizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .collect((m) => m.organizationId),
  },
});

const Account = defineType({
  schema: v.object({ email: v.string() }),
  computed: {
    participationCount: from("participant", Participant)
      .by((p) => p.userId)
      .count(),
  },
});

const schemas = {
  collections: { accounts: Account },
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: { participant: Participant, org_membership: Membership },
    },
  },
};

const ORG = "expo_organization:01aaaaaaaaaaaaaaaaaaaaaaaa";

type Stats = { median: number; p95: number; mean: number };

function stats(samples: number[]): Stats {
  const sorted = [...samples].sort((a, b) => a - b);
  const at = (q: number) =>
    sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
  return {
    median: at(0.5),
    p95: at(0.95),
    mean: samples.reduce((s, x) => s + x, 0) / samples.length,
  };
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

async function world(db: Db, maintained: boolean) {
  registerComputed(
    db,
    maintained ? computedTopology(schemas) : new ComputedTopology([]),
  );
  const expositions = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: schemas.scopedMultiCollections["+expositions"].types,
  });
  const accounts = await collection(db, "accounts", Account);
  return { expositions, accounts };
}

async function seedParticipants(
  db: Db,
  maintained: boolean,
  membershipsEach: number,
  count: number,
) {
  const { expositions } = await world(db, maintained);
  const view = expositions.scope(SCOPE);
  const participants = await view.insertMany(
    "participant",
    Array.from({ length: count }, (_, i) => ({
      name: `P${i}`,
      userId: `user:${i}`,
    })),
  );
  const memberships = participants.flatMap((participantId) =>
    Array.from({ length: membershipsEach }, () => ({
      participantId,
      organizationId: ORG,
      status: "active" as const,
    })),
  );
  for (let i = 0; i < memberships.length; i += 1000)
    await view.insertMany("org_membership", memberships.slice(i, i + 1000));
  return { view, participants };
}

async function perWrite(
  client: MongoClient,
  maintained: boolean,
  membershipsEach: number,
) {
  const db = client.db(
    `bench_computed_${maintained ? "on" : "off"}_${membershipsEach}_${Date.now()}`,
  );
  try {
    const { view, participants } = await seedParticipants(
      db,
      maintained,
      membershipsEach,
      50,
    );
    const pick = (i: number) => participants[i % participants.length]!;
    const inserted: string[] = [];
    const insert = await timed(ITERATIONS, async (i) => {
      inserted.push(
        await view.insertOne("org_membership", {
          participantId: pick(i),
          organizationId: ORG,
          status: "active",
        }),
      );
    });
    const statusFlip = await timed(ITERATIONS, (i) =>
      view.updateOne("org_membership", inserted[i % inserted.length]!, {
        status: i % 2 ? "active" : "removed",
      }),
    );
    const unrelated = await timed(ITERATIONS, (i) =>
      view.updateOne("org_membership", inserted[i % inserted.length]!, {
        note: `n${i}`,
      }),
    );
    const subjectUnrelated = await timed(ITERATIONS, (i) =>
      view.updateOne("participant", pick(i), { name: `N${i}` }),
    );
    const remove = await timed(Math.min(ITERATIONS, inserted.length), (i) =>
      view.deleteId("org_membership", inserted[i]!),
    );
    return { insert, statusFlip, unrelated, subjectUnrelated, remove };
  } finally {
    await db.dropDatabase();
  }
}

async function contention(client: MongoClient, maintained: boolean) {
  const db = client.db(
    `bench_contention_${maintained ? "on" : "off"}_${Date.now()}`,
  );
  try {
    const { view, participants } = await seedParticipants(db, maintained, 5, 1);
    const start = performance.now();
    await Promise.all(
      Array.from({ length: 50 }, () =>
        view.insertOne("org_membership", {
          participantId: participants[0]!,
          organizationId: ORG,
          status: "active",
        }),
      ),
    );
    return performance.now() - start;
  } finally {
    await db.dropDatabase();
  }
}

async function fullApply(client: MongoClient, subjects: number) {
  const db = client.db(`bench_apply_${Date.now()}`);
  try {
    await seedParticipants(db, false, 3, subjects);
    const start = performance.now();
    const result = await applyComputed(db, computedTopology(schemas), {
      subject: "participant",
      batchSize: 200,
    });
    const ms = performance.now() - start;
    return { ms, perSubjectMs: ms / subjects, written: result.written };
  } finally {
    await db.dropDatabase();
  }
}

async function plans(client: MongoClient) {
  const db = client.db(`bench_plans_${Date.now()}`);
  try {
    const { expositions, accounts } = await world(db, true);
    for (const scope of [SCOPE, ...OTHER_SCOPES]) {
      const view = expositions.scope(scope);
      const participants = await view.insertMany(
        "participant",
        Array.from({ length: 300 }, (_, i) => ({
          name: `P${i}`,
          userId: `user:${i % 40}`,
        })),
      );
      await view.insertMany(
        "org_membership",
        participants.map((participantId) => ({
          participantId,
          organizationId: ORG,
          status: "active" as const,
        })),
      );
    }
    await accounts.insertOne({ email: "a@x.test" });
    const coll = db.collection("+expositions");
    const someParticipant = (await coll.findOne({
      _type: "participant",
      _scope: SCOPE,
    }))!._id;
    const winning = async (filter: Record<string, unknown>) => {
      const explained = (await coll.find(filter).explain("executionStats")) as {
        queryPlanner: { winningPlan: unknown };
        executionStats: {
          totalDocsExamined: number;
          totalKeysExamined: number;
          nReturned: number;
        };
      };
      const stages: string[] = [];
      const walk = (node: unknown) => {
        if (!node || typeof node !== "object") return;
        const n = node as {
          stage?: string;
          indexName?: string;
          inputStage?: unknown;
          inputStages?: unknown[];
        };
        if (n.stage)
          stages.push(n.indexName ? `${n.stage}(${n.indexName})` : n.stage);
        walk(n.inputStage);
        for (const child of n.inputStages ?? []) walk(child);
      };
      walk(explained.queryPlanner.winningPlan);
      const { totalDocsExamined, totalKeysExamined, nReturned } =
        explained.executionStats;
      return {
        plan: stages.join(" < "),
        docsExamined: totalDocsExamined,
        keysExamined: totalKeysExamined,
        returned: nReturned,
      };
    };
    return {
      scopedRecompute: await winning({
        _type: "org_membership",
        status: "active",
        participantId: { $in: [someParticipant] },
        _scope: SCOPE,
      }),
      globalSubjectRecompute: await winning({
        _type: "participant",
        userId: { $in: ["user:1"] },
      }),
      subjectRead: await winning({
        _type: "participant",
        _id: { $in: [someParticipant] },
      }),
    };
  } finally {
    await db.dropDatabase();
  }
}

const round = (value: number) => Math.round(value * 100) / 100;
const show = (label: string, off: Stats, on: Stats) =>
  console.log(
    `${label.padEnd(34)} off ${String(round(off.median)).padStart(6)} ms (p95 ${String(round(off.p95)).padStart(6)})   on ${String(round(on.median)).padStart(6)} ms (p95 ${String(
      round(on.p95),
    ).padStart(6)})   x${round(on.median / off.median)}`,
  );

const client = new MongoClient(URI);
await client.connect();
getSessionContext(client);
try {
  for (const membershipsEach of [1, 10, 100]) {
    const off = await perWrite(client, false, membershipsEach);
    const on = await perWrite(client, true, membershipsEach);
    console.log(
      `\n== per write, participant with ${membershipsEach} memberships, ${ITERATIONS} samples each`,
    );
    show("insert membership", off.insert, on.insert);
    show("flip membership status", off.statusFlip, on.statusFlip);
    show("update unrelated membership field", off.unrelated, on.unrelated);
    show("update participant name", off.subjectUnrelated, on.subjectUnrelated);
    show("delete membership", off.remove, on.remove);
  }
  const contentionOff = await contention(client, false);
  const contentionOn = await contention(client, true);
  console.log(
    `\n== 50 concurrent inserts on ONE participant: off ${round(contentionOff)} ms, on ${round(contentionOn)} ms`,
  );
  const apply = await fullApply(client, 10_000);
  console.log(
    `\n== full apply, 10000 participants x 3 memberships: ${round(apply.ms)} ms (${round(apply.perSubjectMs)} ms per subject, ${apply.written} written)`,
  );
  console.log("\n== query plans");
  console.log(JSON.stringify(await plans(client), null, 2));
} finally {
  await client.close();
}

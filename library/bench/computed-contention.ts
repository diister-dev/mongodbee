import process from "node:process";
import { MongoClient } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import {
  ComputedTopology,
  computedTopology,
} from "../src/computed-topology.ts";
import {
  type ComputedRetryOptions,
  registerComputed,
} from "../src/computed-maintenance.ts";

const URI = process.env.MONGODBEE_TEST_URI ?? "mongodb://localhost:27017";
const SCOPE = "exposition:benchaaaa01";
const ORG = "expo_organization:01aaaaaaaaaaaaaaaaaaaaaaaa";
const ROUNDS = 5;

const Membership = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant")),
    organizationId: refId("expo_organization"),
    status: v.picklist(["active", "removed"]),
  }),
});
const Participant = defineType({
  schema: v.object({ name: v.string() }),
  computed: {
    organizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .collect((m) => m.organizationId),
  },
});
const schemas = {
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: { participant: Participant, org_membership: Membership },
    },
  },
};

async function run(
  client: MongoClient,
  concurrency: number,
  retry: ComputedRetryOptions | undefined,
  maintained: boolean,
): Promise<number> {
  const samples: number[] = [];
  for (let round = 0; round < ROUNDS; round++) {
    const db = client.db(`bench_cont_${Date.now()}_${round}`);
    try {
      registerComputed(
        db,
        maintained ? computedTopology(schemas) : new ComputedTopology([]),
        { retry },
      );
      const expositions = await scopedMultiCollection(db, "+expositions", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: schemas.scopedMultiCollections["+expositions"].types,
      });
      const view = expositions.scope(SCOPE);
      const participant = await view.insertOne("participant", { name: "P" });
      const start = performance.now();
      await Promise.all(
        Array.from({ length: concurrency }, () =>
          view.insertOne("org_membership", {
            participantId: participant,
            organizationId: ORG,
            status: "active",
          }),
        ),
      );
      samples.push(performance.now() - start);
    } finally {
      await db.dropDatabase();
    }
  }
  samples.sort((a, b) => a - b);
  return Math.round(samples[Math.floor(samples.length / 2)]!);
}

const client = new MongoClient(URI);
await client.connect();
const variants: Array<[string, ComputedRetryOptions | undefined]> = [
  [
    "previous default (10..400 ms)",
    { maxRetries: 12, initialDelay: 10, maxDelay: 400 },
  ],
  ["new default (full jitter 2..50 ms)", undefined],
  ["short (2..50 ms)", { maxRetries: 40, initialDelay: 2, maxDelay: 50 }],
  ["tiny (1..20 ms)", { maxRetries: 60, initialDelay: 1, maxDelay: 20 }],
  [
    "tiny no jitter",
    { maxRetries: 60, initialDelay: 1, maxDelay: 20, jitter: false },
  ],
  [
    "full jitter (2..50 ms)",
    { maxRetries: 60, initialDelay: 2, maxDelay: 50, jitter: "full" },
  ],
  [
    "full jitter (5..100 ms)",
    { maxRetries: 60, initialDelay: 5, maxDelay: 100, jitter: "full" },
  ],
  [
    "full jitter (1..20 ms)",
    { maxRetries: 80, initialDelay: 1, maxDelay: 20, jitter: "full" },
  ],
];
try {
  for (const concurrency of [5, 20, 50]) {
    console.log(
      `\n== ${concurrency} concurrent inserts on one participant, median of ${ROUNDS}`,
    );
    console.log(
      `  unmaintained baseline: ${await run(client, concurrency, undefined, false)} ms`,
    );
    for (const [label, retry] of variants)
      console.log(
        `  ${label.padEnd(30)} ${await run(client, concurrency, retry, true)} ms`,
      );
  }
} finally {
  await client.close();
}

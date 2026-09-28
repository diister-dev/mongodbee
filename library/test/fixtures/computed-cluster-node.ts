import process from "node:process";
import { MongoClient } from "../../src/mongodb.ts";
import { drainComputedPending } from "../../src/computed-marks.ts";
import { CLUSTER_SCOPES, mulberry32, openCluster } from "./computed-cluster.ts";

const uri = process.env.CLUSTER_URI!;
const dbName = process.env.CLUSTER_DB!;
const role = process.env.CLUSTER_ROLE!;
const seed = Number(process.env.CLUSTER_SEED ?? "1");
const iterations = Number(process.env.CLUSTER_ITERATIONS ?? "60");
const durationMs = Number(process.env.CLUSTER_DURATION_MS ?? "4000");
const leaseMs = Number(process.env.CLUSTER_LEASE_MS ?? "1500");

const client = new MongoClient(uri);
const db = client.db(dbName);
const { topology, expositions } = await openCluster(db, 3);
const random = mulberry32(seed);
const pick = <T>(items: readonly T[]): T | undefined =>
  items.length === 0 ? undefined : items[Math.floor(random() * items.length)];

async function idsOf(type: string, scope: string): Promise<string[]> {
  return (
    await db
      .collection("+expositions")
      .find({ _type: type, _scope: scope }, { projection: { _id: 1 } })
      .toArray()
  ).map((document) => String(document._id));
}

const tolerated = /no element found/i;

async function write(): Promise<void> {
  const scope = pick(CLUSTER_SCOPES)!;
  const view = expositions.scope(scope);
  const roll = random();
  if (roll < 0.1) {
    await view.insertOne("participant", {
      name: `P${Math.floor(random() * 1e6)}`,
    });
  } else if (roll < 0.35) {
    const participant = pick(await idsOf("participant", scope));
    const organization = pick(await idsOf("expo_organization", scope));
    if (participant && organization)
      await view.insertOne("org_membership", {
        participantId: participant,
        organizationId: organization,
        status: random() < 0.7 ? "active" : "removed",
      });
  } else if (roll < 0.5) {
    const membership = pick(await idsOf("org_membership", scope));
    if (membership)
      await view.updateOne("org_membership", membership, {
        status: random() < 0.5 ? "active" : "removed",
      });
  } else if (roll < 0.65) {
    const membership = pick(await idsOf("org_membership", scope));
    const participant = pick(await idsOf("participant", scope));
    if (membership && participant)
      await view.updateOne("org_membership", membership, {
        participantId: participant,
      });
  } else if (roll < 0.8) {
    const organization = pick(await idsOf("expo_organization", scope));
    if (organization)
      await view.updateOne("expo_organization", organization, {
        status: random() < 0.5 ? "validated" : "pending",
      });
  } else if (roll < 0.9) {
    await view.deleteMany("org_membership", { status: "removed" });
  } else {
    await view.updateWhere(
      "org_membership",
      { status: "removed" },
      { status: "active" },
    );
  }
}

let failures = 0;
if (role === "writer") {
  for (let index = 0; index < iterations; index++) {
    try {
      await write();
    } catch (error) {
      if (!tolerated.test(String((error as Error).message))) {
        failures++;
        console.error(
          `writer error: ${(error as Error).name}: ${(error as Error).message}`,
        );
      }
    }
  }
} else {
  const until = Date.now() + durationMs;
  while (Date.now() < until) {
    await drainComputedPending(db, {
      topology,
      leaseMs,
      afterRecompute: async () => {
        console.log("IN_DRAIN");
        await new Promise((resolve) => setTimeout(resolve, 150));
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

await client.close();
process.exit(failures === 0 ? 0 : 1);

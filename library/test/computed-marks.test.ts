import { test } from "./+harness.ts";
import { assert, assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import {
  ComputedTopology,
  computedTopology,
} from "../src/computed-topology.ts";
import { registerComputed } from "../src/computed-maintenance.ts";
import { checkComputed } from "../src/computed-apply.ts";
import {
  COMPUTED_PENDING_COLLECTION,
  drainComputedPending,
  markWhole,
  pendingComputed,
} from "../src/computed-marks.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";
const ORGANIZATION = "expo_organization:01aaaaaaaaaaaaaaaaaaaaaaaa";

const Membership = defineType({
  schema: v.object({
    participantId: refId("participant"),
    organizationId: refId("expo_organization"),
    status: v.picklist(["active", "removed"]),
  }),
});

const Scan = defineType({
  schema: v.object({ scannedIds: v.array(refId("participant")) }),
});
const ScansModel = defineModel("scans", { schema: { scan: Scan } });

const Participant = defineType({
  schema: v.object({ name: v.string() }),
  computed: {
    activeCount: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .count(),
    scanCount: from(ScansModel, "scan")
      .by((s) => s.scannedIds)
      .sameScope()
      .count(),
  },
});

const schemas = {
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: { participant: Participant, org_membership: Membership },
    },
    "+scans": { scope: refId("exposition"), types: ScansModel.schema },
  },
};

async function open(db: Db, inlineLimit: number) {
  const topology = computedTopology(schemas);
  registerComputed(db, topology, { inlineLimit });
  const expositions = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: schemas.scopedMultiCollections["+expositions"].types,
  });
  const scans = await scopedMultiCollection(db, "+scans", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: ScansModel.schema,
  });
  return { topology, expositions, scans };
}

async function stored(
  db: Db,
  id: string,
): Promise<Record<string, unknown> | undefined> {
  return (
    (await db.collection("+expositions").findOne({ _id: id as never })) as {
      _computed?: Record<string, unknown>;
    } | null
  )?._computed;
}

async function marks(db: Db) {
  return await db
    .collection<{ _id: string; generation: number; [key: string]: unknown }>(
      COMPUTED_PENDING_COLLECTION,
    )
    .find({}, { projection: { claimedUntil: 0, createdAt: 0, updatedAt: 0 } })
    .sort({ _id: 1 })
    .toArray();
}

test("computed marks: a write past the inline limit lands, leaves one durable mark bounded to its scope, and the drainer makes it exact", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, expositions } = await open(db, 2);
    const a = expositions.scope(EXPO_A);
    const participant = await a.insertOne("participant", { name: "Ada" });
    await a.insertMany(
      "org_membership",
      [0, 1].map(() => ({
        participantId: participant,
        organizationId: ORGANIZATION,
        status: "active" as const,
      })),
    );
    await a.insertOne("org_membership", {
      participantId: participant,
      organizationId: ORGANIZATION,
      status: "active",
    });
    assertEquals((await stored(db, participant))?.activeCount, 3);

    await a.deleteMany("org_membership", { status: "active" });

    assertEquals(
      await a.countDocuments("org_membership"),
      0,
      "the write itself is never refused",
    );
    assertEquals(
      (await stored(db, participant))?.activeCount,
      3,
      "past the limit the value is late, not wrong forever",
    );
    assertEquals(await marks(db), [
      {
        _id: `participant.activeCount|whole|${EXPO_A}`,
        field: "participant.activeCount",
        kind: "whole",
        scope: EXPO_A,
        reason: "a write targeted more documents than the inline limit",
        generation: 1,
      },
    ]);

    const drained = await drainComputedPending(db, { topology });
    assertEquals(drained.drained, 1);
    assertEquals(drained.remaining, 0);
    assertEquals((await stored(db, participant))?.activeCount, 0);
    assertEquals((await checkComputed(db, topology)).drifts, []);
  });
});

test("computed marks: a write affecting more subjects than the limit marks the whole field", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, expositions, scans } = await open(db, 2);
    const participants: string[] = [];
    for (const index of [0, 1, 2, 3])
      participants.push(
        await expositions
          .scope(EXPO_A)
          .insertOne("participant", { name: `P${index}` }),
      );
    assertEquals(await marks(db), [], "one subject at a time stays inline");

    await scans.scope(EXPO_A).insertOne("scan", { scannedIds: participants });

    assertEquals(
      (await marks(db)).map((mark) => mark._id),
      ["participant.scanCount|whole|*"],
    );
    await drainComputedPending(db, { topology });
    for (const participant of participants)
      assertEquals((await stored(db, participant))?.scanCount, 1);
    assertEquals((await pendingComputed(db)).count, 0);
  });
});

test("computed marks: dropping a source scope and dropping a source collection both leave the obligation durable", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, expositions, scans } = await open(db, 1);
    const a = await expositions
      .scope(EXPO_A)
      .insertMany("participant", [{ name: "A1" }, { name: "A2" }]);
    const b = await expositions
      .scope(EXPO_B)
      .insertOne("participant", { name: "B1" });
    await scans.scope(EXPO_A).insertOne("scan", { scannedIds: [a[0]!] });
    await scans.scope(EXPO_A).insertOne("scan", { scannedIds: [a[1]!] });
    await scans.scope(EXPO_B).insertOne("scan", { scannedIds: [b] });
    await drainComputedPending(db, { topology });
    assertEquals((await checkComputed(db, topology)).drifts, []);

    await scans.dropScope(EXPO_A, { confirm: true });
    assert(
      (await marks(db)).some(
        (mark) => mark._id === `participant.scanCount|whole|${EXPO_A}`,
      ),
      "the scoped drop marks only its scope",
    );
    await drainComputedPending(db, { topology });
    assertEquals((await stored(db, a[0]!))?.scanCount, 0);
    assertEquals(
      (await stored(db, b))?.scanCount,
      1,
      "the other scope keeps its value",
    );

    await scans.drop({ force: true });
    assertEquals(
      (await marks(db)).map((mark) => mark._id),
      ["participant.scanCount|whole|*"],
    );
    await drainComputedPending(db, { topology });
    assertEquals((await stored(db, b))?.scanCount, 0);
    assertEquals((await checkComputed(db, topology)).drifts, []);
  });
});

test("computed marks: a mark renewed while it is being drained is requeued, never lost", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, expositions } = await open(db, 2);
    const a = expositions.scope(EXPO_A);
    const participant = await a.insertOne("participant", { name: "Ada" });
    const addThree = () =>
      a.insertMany(
        "org_membership",
        [0, 1, 2].map(() => ({
          participantId: participant,
          organizationId: ORGANIZATION,
          status: "active" as const,
        })),
      );
    await addThree();
    await a.deleteMany("org_membership", {});
    assertEquals(
      (await marks(db)).map((mark) => mark.generation),
      [1],
    );

    let renewed = false;
    const first = await drainComputedPending(db, {
      topology,
      limit: 1,
      afterRecompute: async () => {
        if (renewed) return;
        renewed = true;
        await addThree();
        await a.deleteMany("org_membership", {});
      },
    });
    assertEquals(
      first.requeued,
      1,
      "the mark was renewed after the drainer read the sources",
    );
    assertEquals(
      (await marks(db)).map((mark) => mark.generation),
      [2],
      "the renewed obligation is still there",
    );
    assertEquals(
      (await stored(db, participant))?.activeCount,
      3,
      "the value is stale until the next drain",
    );

    const second = await drainComputedPending(db, { topology });
    assertEquals(second.remaining, 0);
    assertEquals((await stored(db, participant))?.activeCount, 0);
  });
});

test("computed marks: concurrent drainers and concurrent past-limit writers converge to the truth with no mark left", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, expositions } = await open(db, 2);
    const views = [expositions.scope(EXPO_A), expositions.scope(EXPO_B)];
    const participants = await Promise.all(
      views.map((view) =>
        view.insertMany(
          "participant",
          [0, 1, 2].map((index) => ({ name: `P${index}` })),
        ),
      ),
    );
    await Promise.all(
      views.map((view, index) =>
        view.insertMany(
          "org_membership",
          participants[index]!.flatMap((participant) =>
            [0, 1].map(() => ({
              participantId: participant,
              organizationId: ORGANIZATION,
              status: "active" as const,
            })),
          ),
        ),
      ),
    );

    let writing = true;
    const writers = Array.from({ length: 12 }, (_, index) =>
      views[index % 2]!.updateWhere(
        "org_membership",
        {},
        { status: index % 3 === 0 ? "active" : "removed" },
      ),
    );
    const drainers = Array.from({ length: 3 }, async () => {
      while (writing) {
        await drainComputedPending(db, { topology, leaseMs: 5_000 });
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    });
    await Promise.all(writers);
    writing = false;
    await Promise.all(drainers);
    await drainComputedPending(db, { topology });

    assertEquals((await pendingComputed(db)).count, 0);
    assertEquals((await checkComputed(db, topology)).drifts, []);
  });
});

test("computed marks: a mark for a field that no longer exists is discarded", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology } = await open(db, 10);
    await markWhole(
      db,
      topology.field("participant", "activeCount"),
      undefined,
      "old",
    );
    const result = await drainComputedPending(db, {
      topology: new ComputedTopology([]),
    });
    assertEquals(result.drained, 1);
    assertEquals((await pendingComputed(db)).count, 0);
  });
});

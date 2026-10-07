import { test } from "./+harness.ts";
import { assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import { computedTopology } from "../src/computed-topology.ts";
import { registerComputed } from "../src/computed-maintenance.ts";
import { checkComputed } from "../src/computed-apply.ts";
import {
  COMPUTED_PENDING_COLLECTION,
  drainComputedPending,
} from "../src/computed-marks.ts";

const EXPO = "exposition:expoaaaaa01";

const Organization = defineType({
  schema: v.object({
    name: v.string(),
    status: v.picklist(["pending", "validated"]),
    note: v.optional(v.string()),
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
    validatedNames: from("org_membership", Membership)
      .by((m) => m.participantId)
      .through("expo_organization", Organization, (m) => m.organizationId)
      .where((o) => [o.status, "validated"])
      .collect((o) => o.name)
      .distinct(),
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

async function open(db: Db, inlineLimit?: number) {
  const topology = computedTopology(schemas);
  registerComputed(db, topology, { inlineLimit });
  const expositions = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: schemas.scopedMultiCollections["+expositions"].types,
  });
  return { topology, view: expositions.scope(EXPO) };
}

async function computedOf(db: Db, id: string) {
  const computed = (
    (await db.collection("+expositions").findOne({ _id: id as never })) as {
      _computed?: Record<string, unknown>;
    } | null
  )?._computed;
  if (computed === undefined) return undefined;
  const { _rev: _revision, ...values } = computed;
  return values;
}

async function revisionOf(db: Db, id: string): Promise<number | undefined> {
  return (
    (await db.collection("+expositions").findOne({ _id: id as never })) as {
      _computed?: { _rev?: number };
    } | null
  )?._computed?._rev;
}

async function markIds(db: Db): Promise<string[]> {
  return (
    await db
      .collection<{ _id: string }>(COMPUTED_PENDING_COLLECTION)
      .find({})
      .sort({ _id: 1 })
      .toArray()
  ).map((mark) => mark._id);
}

test("computed through: a near-side write is exact at commit", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { view } = await open(db);
    const participant = await view.insertOne("participant", { name: "Ada" });
    const validated = await view.insertOne("expo_organization", {
      name: "Acme",
      status: "validated",
    });
    const pending = await view.insertOne("expo_organization", {
      name: "Initech",
      status: "pending",
    });
    await drainComputedPending(db, { topology: computedTopology(schemas) });

    const membership = await view.insertOne("org_membership", {
      participantId: participant,
      organizationId: pending,
      status: "active",
    });
    assertEquals(
      (await computedOf(db, participant))?.validatedOrganizationIds,
      [],
    );
    await view.updateOne("org_membership", membership, {
      organizationId: validated,
    });
    assertEquals(
      (await computedOf(db, participant))?.validatedOrganizationIds,
      [validated],
      "moving the link recomputes in the same transaction",
    );
    assertEquals(await markIds(db), []);
  });
});

test("computed through: a far-side change is exact at commit, every subject it reaches recomputed with no mark", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view } = await open(db);
    const participant = await view.insertOne("participant", { name: "Ada" });
    const organization = await view.insertOne("expo_organization", {
      name: "Acme",
      status: "validated",
    });
    await view.insertOne("org_membership", {
      participantId: participant,
      organizationId: organization,
      status: "active",
    });
    await drainComputedPending(db, { topology });
    assertEquals(
      (await computedOf(db, participant))?.validatedOrganizationIds,
      [organization],
    );

    const colleague = await view.insertOne("participant", { name: "Bob" });
    await view.insertOne("org_membership", {
      participantId: colleague,
      organizationId: organization,
      status: "active",
    });

    await view.updateOne("expo_organization", organization, {
      status: "pending",
    });
    assertEquals(await markIds(db), []);
    for (const subject of [participant, colleague])
      assertEquals(await computedOf(db, subject), {
        validatedOrganizationIds: [],
        validatedNames: [],
      });
    assertEquals((await checkComputed(db, topology)).drifts, []);
  });
});

test("computed through: only a far update that can change a value recomputes its subjects", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view } = await open(db);
    const participant = await view.insertOne("participant", { name: "Ada" });
    const organization = await view.insertOne("expo_organization", {
      name: "Acme",
      status: "validated",
    });
    await view.insertOne("org_membership", {
      participantId: participant,
      organizationId: organization,
      status: "active",
    });
    await drainComputedPending(db, { topology });
    const revision = await revisionOf(db, participant);

    await view.updateOne("expo_organization", organization, {
      note: "unrelated",
    });
    assertEquals(await markIds(db), []);
    assertEquals(
      await revisionOf(db, participant),
      revision,
      "a field no computed value reads recomputes nobody",
    );

    await view.updateOne("expo_organization", organization, {
      name: "Acme Corp",
    });
    assertEquals(await markIds(db), []);
    assertEquals(await revisionOf(db, participant), (revision ?? 0) + 1);
    assertEquals((await computedOf(db, participant))?.validatedNames, [
      "Acme Corp",
    ]);
    assertEquals((await checkComputed(db, topology)).drifts, []);
  });
});

test("computed through: inserting and deleting a far document are both followed", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view } = await open(db);
    const participant = await view.insertOne("participant", { name: "Ada" });
    const future = "expo_organization:01bbbbbbbbbbbbbbbbbbbbbbbb";
    await view.insertOne("org_membership", {
      participantId: participant,
      organizationId: future,
      status: "active",
    });
    await drainComputedPending(db, { topology });
    assertEquals(
      (await computedOf(db, participant))?.validatedOrganizationIds,
      [],
    );

    await view.insertOne("expo_organization", {
      _id: future,
      name: "Late",
      status: "validated",
    } as never);
    await drainComputedPending(db, { topology });
    assertEquals(
      (await computedOf(db, participant))?.validatedOrganizationIds,
      [future],
      "a far document arriving after its links is picked up",
    );

    await view.deleteId("expo_organization", future);
    await drainComputedPending(db, { topology });
    assertEquals(
      (await computedOf(db, participant))?.validatedOrganizationIds,
      [],
    );
    assertEquals((await checkComputed(db, topology)).drifts, []);
  });
});

test("computed through: changing more far documents than the limit marks the whole field", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view } = await open(db, 1);
    const participant = await view.insertOne("participant", { name: "Ada" });
    for (const name of ["A", "B", "C"]) {
      const organization = await view.insertOne("expo_organization", {
        name,
        status: "validated",
      });
      await view.insertOne("org_membership", {
        participantId: participant,
        organizationId: organization,
        status: "active",
      });
    }
    await drainComputedPending(db, { topology });

    await view.deleteMany("expo_organization", {});
    assertEquals(await markIds(db), [
      `participant.validatedNames|whole|${EXPO}`,
      `participant.validatedOrganizationIds|whole|${EXPO}`,
    ]);
    await drainComputedPending(db, { topology });
    assertEquals(await computedOf(db, participant), {
      validatedOrganizationIds: [],
      validatedNames: [],
    });
  });
});

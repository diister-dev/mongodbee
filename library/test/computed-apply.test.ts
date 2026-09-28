import { test } from "./+harness.ts";
import {
  assert,
  assertEquals,
  assertRejects,
  assertThrows,
} from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { collection } from "../src/collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import {
  ComputedTopology,
  computedTopology,
  ComputedTopologyError,
} from "../src/computed-topology.ts";
import { registerComputed } from "../src/computed-maintenance.ts";
import {
  applyComputed,
  checkComputed,
  ComputedEntriesExceededError,
  repairComputed,
} from "../src/computed-apply.ts";

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

const Organization = defineType({
  schema: v.object({
    name: v.string(),
    status: v.picklist(["pending", "validated"]),
  }),
});

const Membership = defineType({
  schema: v.object({
    participantId: refId("participant"),
    organizationId: refId("expo_organization"),
    status: v.picklist(["active", "removed"]),
  }),
});

const Scan = defineType({
  schema: v.object({
    scannedIds: v.array(refId("participant")),
    kind: v.picklist(["security", "business", "vip"]),
  }),
});

const ScansModel = defineModel("scans", { schema: { scan: Scan } });

const Participant = defineType({
  schema: v.object({ name: v.string(), userId: v.string() }),
  computed: {
    organizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .collect((m) => m.organizationId),
    organizationCount: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .count(),
    scanKinds: from(ScansModel, "scan")
      .by((s) => s.scannedIds)
      .sameScope()
      .collect((s) => s.kind)
      .distinct()
      .maxEntries(2),
    scanCount: from(ScansModel, "scan")
      .by((s) => s.scannedIds)
      .sameScope()
      .count(),
    validatedOrganizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .through("expo_organization", Organization, (m) => m.organizationId)
      .where((o) => [o.status, "validated"])
      .collect((o) => o._id),
  },
});

const User = defineType({
  schema: v.object({ email: v.string() }),
  computed: {
    participationCount: from("participant", Participant)
      .by((p) => p.userId)
      .count(),
  },
});

const schemas = {
  collections: { users: User },
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: {
        participant: Participant,
        org_membership: Membership,
        expo_organization: Organization,
      },
    },
    "+scans": { scope: refId("exposition"), types: ScansModel.schema },
  },
};

async function open(db: Db) {
  registerComputed(db, new ComputedTopology([]));
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
  const users = await collection(db, "users", User);
  return { expositions, scans, users };
}

type Opened = Awaited<ReturnType<typeof open>>;

async function seed({ expositions, scans, users }: Opened) {
  const ada = await users.insertOne({ email: "ada@example.test" });
  const bob = await users.insertOne({ email: "bob@example.test" });
  const a = expositions.scope(EXPO_A);
  const b = expositions.scope(EXPO_B);
  const adaA = await a.insertOne("participant", {
    name: "Ada",
    userId: String(ada),
  });
  const bobA = await a.insertOne("participant", {
    name: "Bob",
    userId: String(bob),
  });
  const adaB = await b.insertOne("participant", {
    name: "Ada",
    userId: String(ada),
  });
  const validated = await a.insertOne("expo_organization", {
    name: "Acme",
    status: "validated",
  });
  const pending = await a.insertOne("expo_organization", {
    name: "Initech",
    status: "pending",
  });
  await a.insertOne("org_membership", {
    participantId: adaA,
    organizationId: validated,
    status: "active",
  });
  await a.insertOne("org_membership", {
    participantId: adaA,
    organizationId: pending,
    status: "active",
  });
  await a.insertOne("org_membership", {
    participantId: adaA,
    organizationId: validated,
    status: "active",
  });
  await a.insertOne("org_membership", {
    participantId: bobA,
    organizationId: validated,
    status: "removed",
  });
  await scans
    .scope(EXPO_A)
    .insertOne("scan", { scannedIds: [adaA, bobA], kind: "security" });
  await scans
    .scope(EXPO_A)
    .insertOne("scan", { scannedIds: [adaA], kind: "security" });
  await scans
    .scope(EXPO_A)
    .insertOne("scan", { scannedIds: [adaA, adaA], kind: "business" });
  await scans
    .scope(EXPO_B)
    .insertOne("scan", { scannedIds: [adaA], kind: "business" });
  return { ada, bob, adaA, bobA, adaB, validated, pending };
}

function unordered(
  computed: Record<string, unknown> | undefined,
): Record<string, unknown> | undefined {
  if (!computed) return computed;
  return Object.fromEntries(
    Object.entries(computed).map(([name, value]) => [
      name,
      Array.isArray(value) ? [...value].sort() : value,
    ]),
  );
}

async function stored(
  db: Db,
  collectionName: string,
  id: unknown,
): Promise<Record<string, unknown> | undefined> {
  const document = await db
    .collection(collectionName)
    .findOne({ _id: id as never });
  return (document as { _computed?: Record<string, unknown> } | null)
    ?._computed;
}

test("computed apply: the topology places every subject, source and far type in its physical collection", () => {
  const topology = computedTopology(schemas);
  const field = topology.field("participant", "scanKinds");
  assertEquals(field.at, {
    kind: "scoped",
    collection: "+expositions",
    type: "participant",
  });
  assertEquals(field.source, {
    kind: "scoped",
    collection: "+scans",
    type: "scan",
  });
  assert(field.scoped, "the scans of a participant are read in its own scope");
  assertEquals(topology.field("participant", "validatedOrganizationIds").far, {
    kind: "scoped",
    collection: "+expositions",
    type: "expo_organization",
  });
  assertEquals(topology.field("users", "participationCount").at, {
    kind: "collection",
    collection: "users",
  });
  assert(
    !topology.field("users", "participationCount").scoped,
    "a global subject counts across every scope",
  );
  assertEquals(topology.fields.length, 6);
});

test("computed apply: a topology that cannot place a field precisely is refused at boot", () => {
  const ParticipantScans = defineType({
    schema: v.object({ name: v.string() }),
    computed: {
      n: from(ScansModel, "scan")
        .by((s) => s.scannedIds)
        .count(),
    },
  });
  const Global = defineType({
    schema: v.object({ name: v.string() }),
    computed: {
      ids: from("org_membership", Membership)
        .by((m) => m.participantId)
        .collect((m) => m.organizationId),
    },
  });
  const cases: Array<[string, () => unknown, string]> = [
    [
      "unknown source",
      () => computedTopology({ collections: { users: User } }),
      "not declared in any collection",
    ],
    [
      "ambiguous source",
      () =>
        computedTopology({
          collections: { users: User, participant: Participant },
          scopedMultiCollections: schemas.scopedMultiCollections,
        }),
      "must live in one place",
    ],
    [
      "missing sameScope",
      () =>
        computedTopology({
          scopedMultiCollections: {
            "+expositions": {
              scope: refId("exposition"),
              types: { participant: ParticipantScans },
            },
            "+scans": schemas.scopedMultiCollections["+scans"],
          },
        }),
      "declare sameScope()",
    ],
    [
      "unbounded global collect",
      () =>
        computedTopology({
          collections: { people: Global },
          scopedMultiCollections: schemas.scopedMultiCollections,
        }),
      "needs maxEntries()",
    ],
    [
      "computed on a model template",
      () =>
        computedTopology({
          multiModels: { expo: { participant: Participant } },
        }),
      "not supported",
    ],
  ];
  for (const [label, build, message] of cases) {
    const error = assertThrows(build, ComputedTopologyError);
    assert(
      error.message.includes(message),
      `${label}: "${error.message}" should mention ${message}`,
    );
  }
});

test("computed apply: a full apply writes the truth of every field of every subject", async (t) => {
  await withDatabase(t.name, async (db) => {
    const opened = await open(db);
    const ids = await seed(opened);
    const topology = computedTopology(schemas);

    const participants = await applyComputed(db, topology, {
      subject: "participant",
      batchSize: 2,
    });
    assertEquals(participants.subjects, 3);
    assertEquals(participants.batches, 2);
    await applyComputed(db, topology, { subject: "users" });

    const ada = await stored(db, "+expositions", ids.adaA);
    assertEquals(unordered(ada), {
      organizationIds: [ids.validated, ids.validated, ids.pending].sort(),
      organizationCount: 3,
      scanKinds: ["business", "security"],
      scanCount: 3,
      validatedOrganizationIds: [ids.validated, ids.validated],
    });
    const memberships = await db
      .collection("+expositions")
      .find({ _type: "org_membership", participantId: ids.adaA })
      .sort({ _id: 1 })
      .toArray();
    assertEquals(
      ada?.organizationIds,
      memberships.map((membership) => membership.organizationId),
      "a collect keeps its sources' _id order",
    );
    assertEquals(await stored(db, "+expositions", ids.bobA), {
      organizationIds: [],
      organizationCount: 0,
      scanKinds: ["security"],
      scanCount: 1,
      validatedOrganizationIds: [],
    });
    assertEquals(
      (await stored(db, "+expositions", ids.adaB))?.scanCount,
      0,
      "a scan in another scope naming the same id does not count",
    );
    assertEquals(await stored(db, "users", ids.ada), { participationCount: 2 });
    assertEquals(await stored(db, "users", ids.bob), { participationCount: 1 });

    const again = await applyComputed(db, topology, { subject: "participant" });
    assertEquals(
      again.written,
      0,
      "a second full apply finds nothing to change and writes nothing",
    );
  });
});

test("computed apply: a full apply can target one field and one scope", async (t) => {
  await withDatabase(t.name, async (db) => {
    const opened = await open(db);
    const ids = await seed(opened);
    const topology = computedTopology(schemas);

    await applyComputed(db, topology, {
      subject: "participant",
      fields: ["organizationCount"],
      scope: EXPO_A,
    });
    assertEquals(await stored(db, "+expositions", ids.adaA), {
      organizationCount: 3,
    });
    assertEquals(
      await stored(db, "+expositions", ids.adaB),
      undefined,
      "the other scope is untouched",
    );
  });
});

test("computed apply: check reports missing and drifted values, and repair restores the truth", async (t) => {
  await withDatabase(t.name, async (db) => {
    const opened = await open(db);
    const ids = await seed(opened);
    const topology = computedTopology(schemas);

    const before = await checkComputed(db, topology, {
      subject: "participant",
      fields: ["organizationCount"],
    });
    assertEquals(before.drifts.length, 3);
    assert(
      before.drifts.every((drift) => drift.missing),
      "never computed is reported as missing, not as empty",
    );

    await applyComputed(db, topology, { subject: "participant" });
    await applyComputed(db, topology, { subject: "users" });
    const clean = await checkComputed(db, topology);
    assertEquals(clean.drifts, []);
    assertEquals(clean.checked, 5);
    assert(clean.complete);

    await db
      .collection("+expositions")
      .updateOne(
        { _id: ids.adaA as never },
        { $set: { "_computed.organizationCount": 7 } },
      );
    await db
      .collection("+expositions")
      .updateOne(
        { _id: ids.adaA as never },
        { $unset: { "_computed.scanKinds": "" } },
      );
    await db
      .collection("users")
      .updateOne(
        { _id: ids.bob as never },
        { $set: { "_computed.participationCount": 0 } },
      );

    const drifted = await checkComputed(db, topology);
    const summary = drifted.drifts
      .map(
        (drift) =>
          `${drift.subject}.${drift.field}:${drift.missing ? "missing" : `${drift.stored}->${drift.truth}`}`,
      )
      .sort();
    assertEquals(summary, [
      "participant.organizationCount:7->3",
      "participant.scanKinds:missing",
      "users.participationCount:0->1",
    ]);

    const bounded = await checkComputed(db, topology, {
      limit: 2,
      batchSize: 1,
    });
    assertEquals(bounded.checked, 2);
    assert(!bounded.complete, "a bounded check says it stopped early");

    assertEquals(await repairComputed(db, topology, drifted.drifts), 3);
    assertEquals((await checkComputed(db, topology)).drifts, []);
  });
});

test("computed apply: a collect past its maxEntries fails the batch and writes nothing", async (t) => {
  await withDatabase(t.name, async (db) => {
    const opened = await open(db);
    const ids = await seed(opened);
    const topology = computedTopology(schemas);
    await applyComputed(db, topology, {
      subject: "participant",
      fields: ["scanCount"],
    });
    await opened.scans
      .scope(EXPO_A)
      .insertOne("scan", { scannedIds: [ids.adaA], kind: "vip" });

    const error = await assertRejects(
      () =>
        applyComputed(db, topology, { subject: "participant", batchSize: 10 }),
      ComputedEntriesExceededError,
    );
    assertEquals(error.entries, 3);
    assertEquals(error.maxEntries, 2);
    assertEquals(
      await stored(db, "+expositions", ids.adaA),
      { scanCount: 3 },
      "the failed batch rolled back: no field of it was written",
    );
  });
});

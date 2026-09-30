import { test } from "./+harness.ts";
import { assertEquals, assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { dbId, refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { collection } from "../src/collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import { computedTopology } from "../src/computed-topology.ts";
import {
  type ComputedRegistrationOptions,
  ComputedRequiresTransactionError,
  registerComputed,
} from "../src/computed-maintenance.ts";
import { checkComputed } from "../src/computed-apply.ts";
import {
  COMPUTED_PENDING_COLLECTION,
  drainComputedPending,
} from "../src/computed-marks.ts";

const EXPO = "exposition:expoaaaaa01";
const OTHER = "exposition:expobbbbb02";

const ParticipantStatus = v.picklist(["active", "pending", "withdrawn"]);

const Participant = defineType({
  schema: v.object({ name: v.string(), status: ParticipantStatus }),
});

const Badge = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant"), { unique: true }),
    label: v.optional(v.string()),
  }),
  computed: {
    activeParticipantCount: from("badge", {
      _id: dbId("badge"),
      participantId: refId("participant"),
    })
      .by((badge) => badge._id)
      .through(
        "participant",
        { status: ParticipantStatus },
        (badge) => badge.participantId,
      )
      .where((participant) => [participant.status, "active"])
      .count(),
  },
});

const Scanner = defineType({
  schema: v.object({ status: v.picklist(["online", "offline"]) }),
});

const Kiosk = defineType({
  schema: v.object({ scannerId: withIndex(refId("scanner")) }),
  computed: {
    onlineScanners: from("kiosk", {
      _id: dbId("kiosk"),
      scannerId: refId("scanner"),
    })
      .by((kiosk) => kiosk._id)
      .sameScope()
      .through(
        "scanner",
        { status: v.picklist(["online", "offline"]) },
        (kiosk) => kiosk.scannerId,
      )
      .where((scanner) => [scanner.status, "online"])
      .count(),
  },
});

const scopedSchemas = {
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: { participant: Participant, badge: Badge, kiosk: Kiosk },
    },
    "+scans": { scope: refId("exposition"), types: { scanner: Scanner } },
  },
};

const Company = defineType({
  schema: v.object({
    _id: dbId("company"),
    name: v.string(),
    status: v.picklist(["pending", "validated"]),
  }),
});

const Job = defineType({
  schema: v.object({
    _id: dbId("job"),
    personId: withIndex(refId("person")),
    companyId: withIndex(refId("company")),
  }),
});

const Person = defineType({
  schema: v.object({ _id: dbId("person"), name: v.string() }),
  computed: {
    validatedEmployers: from("jobs", Job)
      .by((job) => job.personId)
      .through("companies", Company, (job) => job.companyId)
      .where((company) => [company.status, "validated"])
      .collect((company) => company._id)
      .maxEntries(20),
  },
});

const globalSchemas = {
  collections: { companies: Company, jobs: Job, people: Person },
};

function ticketType(global: boolean) {
  return defineType({
    schema: v.object({
      companyId: withIndex(refId("company"), global ? { global: true } : {}),
    }),
    computed: {
      validatedCompany: from("ticket", {
        _id: dbId("ticket"),
        companyId: refId("company"),
      })
        .by((ticket) => ticket._id)
        .through("companies", Company, (ticket) => ticket.companyId)
        .where((company) => [company.status, "validated"])
        .count(),
    },
  });
}

function crossScopeSchemas(global: boolean) {
  return {
    collections: { companies: Company },
    scopedMultiCollections: {
      "+events": {
        scope: refId("exposition"),
        types: { ticket: ticketType(global) },
      },
    },
  };
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

async function computedIn(
  db: Db,
  collectionName: string,
  id: string,
): Promise<Record<string, unknown> | undefined> {
  const stored = (await db
    .collection(collectionName)
    .findOne({ _id: id as never })) as {
    _computed?: Record<string, unknown>;
  } | null;
  return stored?._computed;
}

async function valueIn(
  db: Db,
  collectionName: string,
  id: string,
  field: string,
): Promise<unknown> {
  return (await computedIn(db, collectionName, id))?.[field];
}

async function revisionIn(
  db: Db,
  collectionName: string,
  id: string,
): Promise<unknown> {
  return (await computedIn(db, collectionName, id))?._rev;
}

async function openScoped(db: Db, options: ComputedRegistrationOptions = {}) {
  const topology = computedTopology(scopedSchemas);
  registerComputed(db, topology, options);
  const expositions = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: scopedSchemas.scopedMultiCollections["+expositions"].types,
  });
  const scans = await scopedMultiCollection(db, "+scans", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: scopedSchemas.scopedMultiCollections["+scans"].types,
  });
  return {
    topology,
    expositions,
    view: expositions.scope(EXPO),
    other: expositions.scope(OTHER),
    scans: scans.scope(EXPO),
    otherScans: scans.scope(OTHER),
  };
}

async function openGlobal(db: Db, options: ComputedRegistrationOptions = {}) {
  const topology = computedTopology(globalSchemas);
  registerComputed(db, topology, options);
  return {
    topology,
    companies: await collection(db, "companies", Company),
    jobs: await collection(db, "jobs", Job),
    people: await collection(db, "people", Person),
  };
}

async function expectNoDrift(
  db: Db,
  topology: ReturnType<typeof computedTopology>,
) {
  assertEquals((await checkComputed(db, topology)).drifts, []);
}

test("computed through far (lock): a status change on the far document leaves a far mark, the badge stays late until drained", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view } = await openScoped(db);
    const participant = await view.insertOne("participant", {
      name: "Ada",
      status: "active",
    });
    const badge = await view.insertOne("badge", { participantId: participant });
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      1,
    );
    await drainComputedPending(db, { topology });
    assertEquals(await markIds(db), []);

    await view.updateOne("participant", participant, { status: "withdrawn" });
    assertEquals(await markIds(db), [
      `badge.activeParticipantCount|far|${EXPO}|${participant}`,
    ]);
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      1,
    );

    await drainComputedPending(db, { topology });
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      0,
    );
    assertEquals(await markIds(db), []);
    await expectNoDrift(db, topology);
  });
});

test("computed through far (lock): moving the near link is recomputed in the writing transaction", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view } = await openScoped(db);
    const active = await view.insertOne("participant", {
      name: "Ada",
      status: "active",
    });
    const pending = await view.insertOne("participant", {
      name: "Bob",
      status: "pending",
    });
    const badge = await view.insertOne("badge", { participantId: pending });
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      0,
    );
    await drainComputedPending(db, { topology });

    await view.updateOne("badge", badge, { participantId: active });
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      1,
    );
    assertEquals(await markIds(db), []);
    await expectNoDrift(db, topology);
  });
});

test("computed through far (lock): updateMany, bulkWrite and deleteMany on far documents mark each far document, the drain makes them exact", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, companies, jobs, people } = await openGlobal(db);
    const acme = await companies.insertOne({ name: "Acme", status: "pending" });
    const initech = await companies.insertOne({
      name: "Initech",
      status: "pending",
    });
    const ada = await people.insertOne({ name: "Ada" });
    const bob = await people.insertOne({ name: "Bob" });
    await jobs.insertOne({ personId: ada, companyId: acme });
    await jobs.insertOne({ personId: ada, companyId: initech });
    await jobs.insertOne({ personId: bob, companyId: initech });
    await drainComputedPending(db, { topology });

    await companies.updateMany({}, { $set: { status: "validated" } });
    assertEquals(await markIds(db), [
      `people.validatedEmployers|far|*|${acme}`,
      `people.validatedEmployers|far|*|${initech}`,
    ]);
    assertEquals(await valueIn(db, "people", ada, "validatedEmployers"), []);
    await drainComputedPending(db, { topology });
    assertEquals(await valueIn(db, "people", ada, "validatedEmployers"), [
      acme,
      initech,
    ]);
    assertEquals(await valueIn(db, "people", bob, "validatedEmployers"), [
      initech,
    ]);

    await companies.bulkWrite([
      {
        updateOne: {
          filter: { _id: acme },
          update: { $set: { status: "pending" } },
        },
      },
      { deleteOne: { filter: { _id: initech } } },
    ]);
    assertEquals(await markIds(db), [
      `people.validatedEmployers|far|*|${acme}`,
      `people.validatedEmployers|far|*|${initech}`,
    ]);
    await drainComputedPending(db, { topology });
    assertEquals(await valueIn(db, "people", ada, "validatedEmployers"), []);
    assertEquals(await valueIn(db, "people", bob, "validatedEmployers"), []);

    await companies.insertOne({
      _id: initech,
      name: "Initech",
      status: "validated",
    });
    await drainComputedPending(db, { topology });
    assertEquals(await valueIn(db, "people", bob, "validatedEmployers"), [
      initech,
    ]);
    await companies.deleteMany({ name: { $in: ["Acme", "Initech"] } });
    assertEquals(await markIds(db), [
      `people.validatedEmployers|far|*|${acme}`,
      `people.validatedEmployers|far|*|${initech}`,
    ]);
    await drainComputedPending(db, { topology });
    assertEquals(await valueIn(db, "people", bob, "validatedEmployers"), []);
    await expectNoDrift(db, topology);
  });
});

test("computed through far (lock): a far write whose subjects exceed the inline limit leaves a far mark", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, companies, jobs, people } = await openGlobal(db, {
      inlineLimit: 2,
    });
    const acme = await companies.insertOne({ name: "Acme", status: "pending" });
    const everyone: string[] = [];
    for (const name of ["Ada", "Bob", "Cyd"]) {
      const person = await people.insertOne({ name });
      everyone.push(person);
      await jobs.insertOne({ personId: person, companyId: acme });
    }
    await drainComputedPending(db, { topology });

    await companies.updateOne({ _id: acme }, { $set: { status: "validated" } });
    assertEquals(await markIds(db), [
      `people.validatedEmployers|far|*|${acme}`,
    ]);
    await drainComputedPending(db, { topology });
    for (const person of everyone)
      assertEquals(await valueIn(db, "people", person, "validatedEmployers"), [
        acme,
      ]);
    await expectNoDrift(db, topology);
  });
});

test("computed through far (lock): with sameScope, a far change in one scope leaves a mark bounded to that scope and never touches another scope", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view, other, scans } = await openScoped(db);
    const scanner = await scans.insertOne("scanner", { status: "offline" });
    const here = await view.insertOne("kiosk", { scannerId: scanner });
    const there = await other.insertOne("kiosk", { scannerId: scanner });
    await drainComputedPending(db, { topology });
    const thereRevision = await revisionIn(db, "+expositions", there);

    await scans.updateOne("scanner", scanner, { status: "online" });
    assertEquals(await markIds(db), [
      `kiosk.onlineScanners|far|${EXPO}|${scanner}`,
    ]);
    await drainComputedPending(db, { topology });
    assertEquals(await valueIn(db, "+expositions", here, "onlineScanners"), 1);
    assertEquals(await valueIn(db, "+expositions", there, "onlineScanners"), 0);
    assertEquals(await revisionIn(db, "+expositions", there), thereRevision);
    await expectNoDrift(db, topology);
  });
});

for (const global of [false, true]) {
  test(`computed through far (lock): a global far document read by scoped near documents${global ? " with a global index" : ""} leaves an unscoped far mark`, async (t) => {
    await withDatabase(t.name, async (db) => {
      const schemas = crossScopeSchemas(global);
      const topology = computedTopology(schemas);
      registerComputed(db, topology);
      const companies = await collection(db, "companies", Company);
      const events = await scopedMultiCollection(db, "+events", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: schemas.scopedMultiCollections["+events"].types,
      });
      const acme = await companies.insertOne({
        name: "Acme",
        status: "pending",
      });
      const here = await events
        .scope(EXPO)
        .insertOne("ticket", { companyId: acme });
      const there = await events
        .scope(OTHER)
        .insertOne("ticket", { companyId: acme });
      await drainComputedPending(db, { topology });

      await companies.updateOne(
        { _id: acme },
        { $set: { status: "validated" } },
      );
      assertEquals(await markIds(db), [
        `ticket.validatedCompany|far|*|${acme}`,
      ]);
      await drainComputedPending(db, { topology });
      assertEquals(await valueIn(db, "+events", here, "validatedCompany"), 1);
      assertEquals(await valueIn(db, "+events", there, "validatedCompany"), 1);
      await expectNoDrift(db, topology);
    });
  });
}

test("computed through far (lock): a far change rolled back with its transaction leaves neither a mark nor a value", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, expositions, view } = await openScoped(db);
    const participant = await view.insertOne("participant", {
      name: "Ada",
      status: "active",
    });
    const badge = await view.insertOne("badge", { participantId: participant });
    await drainComputedPending(db, { topology });
    const revision = await revisionIn(db, "+expositions", badge);

    await assertRejects(() =>
      expositions.withSession(async () => {
        await view.updateOne("participant", participant, {
          status: "withdrawn",
        });
        throw new Error("rolled back on purpose");
      }),
    );
    assertEquals(await markIds(db), []);
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      1,
    );
    assertEquals(await revisionIn(db, "+expositions", badge), revision);
    await expectNoDrift(db, topology);
  });
});

test("computed through far (lock): a far write given a session without a transaction is refused by default", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, companies } = await openGlobal(db);
    const acme = await companies.insertOne({ name: "Acme", status: "pending" });
    await drainComputedPending(db, { topology });
    const session = db.client.startSession();
    try {
      await assertRejects(
        () =>
          companies.collection.updateOne(
            { _id: acme } as never,
            { $set: { status: "validated" } },
            { session },
          ),
        ComputedRequiresTransactionError,
      );
    } finally {
      await session.endSession();
    }
    assertEquals(await markIds(db), []);
    assertEquals((await companies.getById(acme)).status, "pending");
  });
});

test("computed through far (lock): in best-effort mode a far write without a transaction still reaches the truth", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, companies, jobs, people } = await openGlobal(db, {
      standaloneMode: "best-effort",
    });
    const acme = await companies.insertOne({ name: "Acme", status: "pending" });
    const ada = await people.insertOne({ name: "Ada" });
    await jobs.insertOne({ personId: ada, companyId: acme });
    const session = db.client.startSession();
    try {
      await companies.collection.updateOne(
        { _id: acme } as never,
        { $set: { status: "validated" } },
        { session },
      );
    } finally {
      await session.endSession();
    }
    await drainComputedPending(db, { topology });
    assertEquals(await valueIn(db, "people", ada, "validatedEmployers"), [
      acme,
    ]);
    await expectNoDrift(db, topology);
  });
});

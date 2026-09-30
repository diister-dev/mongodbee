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
import {
  type ComputedTopology,
  computedTopology,
} from "../src/computed-topology.ts";
import {
  type ComputedRegistrationOptions,
  ComputedRequiresTransactionError,
  registerComputed,
} from "../src/computed-maintenance.ts";
import { checkComputed } from "../src/computed-apply.ts";
import {
  COMPUTED_FENCES_COLLECTION,
  COMPUTED_PENDING_COLLECTION,
  drainComputedPending,
  markFar,
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

const ScannerStatus = v.picklist(["online", "offline"]);

const Scanner = defineType({
  schema: v.object({ status: ScannerStatus }),
});

const Kiosk = defineType({
  schema: v.object({
    scannerId: withIndex(refId("scanner")),
    enabled: v.boolean(),
  }),
  computed: {
    onlineScanners: from("kiosk", {
      _id: dbId("kiosk"),
      scannerId: refId("scanner"),
      enabled: v.boolean(),
    })
      .by((kiosk) => kiosk._id)
      .where((kiosk) => [kiosk.enabled, true])
      .sameScope()
      .through("scanner", { status: ScannerStatus }, (kiosk) => kiosk.scannerId)
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

async function expectNoDrift(db: Db, topology: ComputedTopology) {
  assertEquals((await checkComputed(db, topology)).drifts, []);
}

async function badgeWorld(db: Db) {
  const world = await openScoped(db);
  const participant = await world.view.insertOne("participant", {
    name: "Ada",
    status: "active",
  });
  const badge = await world.view.insertOne("badge", {
    participantId: participant,
  });
  return { ...world, participant, badge };
}

test("computed through far: a status change on the far document is exact at commit, with no mark", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view, participant, badge } = await badgeWorld(db);
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      1,
    );
    assertEquals(await markIds(db), []);

    await view.updateOne("participant", participant, { status: "withdrawn" });
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      0,
    );
    assertEquals(await markIds(db), []);

    await view.updateOne("participant", participant, { status: "active" });
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      1,
    );
    assertEquals(await markIds(db), []);
    await expectNoDrift(db, topology);
  });
});

test("computed through far: a far update that touches nothing the value reads recomputes nobody", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { view, participant, badge } = await badgeWorld(db);
    const revision = await revisionIn(db, "+expositions", badge);
    await view.updateOne("participant", participant, { name: "Ada L." });
    assertEquals(await revisionIn(db, "+expositions", badge), revision);
    assertEquals(await markIds(db), []);
  });
});

test("computed through far: moving the near link is recomputed in the writing transaction", async (t) => {
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

    await view.updateOne("badge", badge, { participantId: active });
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      1,
    );
    assertEquals(await markIds(db), []);
    await expectNoDrift(db, topology);
  });
});

test("computed through far: updateMany, bulkWrite and deleteMany on far documents recompute every subject they reach, at commit", async (t) => {
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

    await companies.updateMany({}, { $set: { status: "validated" } });
    assertEquals(await markIds(db), []);
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
    assertEquals(await markIds(db), []);
    assertEquals(await valueIn(db, "people", ada, "validatedEmployers"), []);
    assertEquals(await valueIn(db, "people", bob, "validatedEmployers"), []);

    await companies.insertOne({
      _id: initech,
      name: "Initech",
      status: "validated",
    });
    assertEquals(
      await valueIn(db, "people", bob, "validatedEmployers"),
      [initech],
      "a far document arriving after its links is picked up at commit",
    );
    await companies.deleteMany({ name: { $in: ["Acme", "Initech"] } });
    assertEquals(await markIds(db), []);
    assertEquals(await valueIn(db, "people", ada, "validatedEmployers"), []);
    assertEquals(await valueIn(db, "people", bob, "validatedEmployers"), []);
    await expectNoDrift(db, topology);
  });
});

test("computed through far: past the inline limit a far write falls back to a far mark, and the drain makes it exact", async (t) => {
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
    assertEquals(await markIds(db), []);

    await companies.updateOne({ _id: acme }, { $set: { status: "validated" } });
    assertEquals(await markIds(db), [
      `people.validatedEmployers|far|*|${acme}`,
    ]);
    assertEquals(
      await valueIn(db, "people", everyone[0]!, "validatedEmployers"),
      [],
    );
    await drainComputedPending(db, { topology });
    for (const person of everyone)
      assertEquals(await valueIn(db, "people", person, "validatedEmployers"), [
        acme,
      ]);
    assertEquals(await markIds(db), []);
    await expectNoDrift(db, topology);
  });
});

test("computed through far: the limit bounds the subjects of every far document a write changes together", async (t) => {
  for (const [inlineLimit, inline] of [
    [3, false],
    [4, true],
  ] as const) {
    await withDatabase(`${t.name} ${inlineLimit}`, async (db) => {
      const { topology, companies, jobs, people } = await openGlobal(db, {
        inlineLimit,
      });
      const acme = await companies.insertOne({
        name: "Acme",
        status: "pending",
      });
      const initech = await companies.insertOne({
        name: "Initech",
        status: "pending",
      });
      for (const [name, company] of [
        ["Ada", acme],
        ["Bob", acme],
        ["Cyd", initech],
        ["Dan", initech],
      ] as const) {
        const person = await people.insertOne({ name });
        await jobs.insertOne({ personId: person, companyId: company });
      }

      await companies.updateMany({}, { $set: { status: "validated" } });
      assertEquals(
        await markIds(db),
        inline
          ? []
          : [
              `people.validatedEmployers|far|*|${acme}`,
              `people.validatedEmployers|far|*|${initech}`,
            ],
        `four subjects under a limit of ${inlineLimit}`,
      );
      if (inline) await expectNoDrift(db, topology);
      await drainComputedPending(db, { topology });
      await expectNoDrift(db, topology);
    });
  }
});

test("computed through far: with sameScope, a far change recomputes its own scope at commit and never touches another scope", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view, other, scans } = await openScoped(db);
    const scanner = await scans.insertOne("scanner", { status: "offline" });
    const here = await view.insertOne("kiosk", {
      scannerId: scanner,
      enabled: true,
    });
    const there = await other.insertOne("kiosk", {
      scannerId: scanner,
      enabled: true,
    });
    const thereRevision = await revisionIn(db, "+expositions", there);

    await scans.updateOne("scanner", scanner, { status: "online" });
    assertEquals(await markIds(db), []);
    assertEquals(await valueIn(db, "+expositions", here, "onlineScanners"), 1);
    assertEquals(await valueIn(db, "+expositions", there, "onlineScanners"), 0);
    assertEquals(
      await revisionIn(db, "+expositions", there),
      thereRevision,
      "the kiosk of the other scope is not even read for writing",
    );
    await expectNoDrift(db, topology);
  });
});

for (const global of [false, true]) {
  test(`computed through far: a global far document read by scoped near documents ${global ? "with a global index is recomputed across scopes at commit" : "without a global index falls back to a far mark"}`, async (t) => {
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
      assertEquals(
        await markIds(db),
        global ? [] : [`ticket.validatedCompany|far|*|${acme}`],
      );
      if (!global) await drainComputedPending(db, { topology });
      assertEquals(await valueIn(db, "+events", here, "validatedCompany"), 1);
      assertEquals(await valueIn(db, "+events", there, "validatedCompany"), 1);
      await expectNoDrift(db, topology);
    });
  });
}

test("computed through far: a far change rolled back with its transaction leaves neither a mark nor a value", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, expositions, view, participant, badge } =
      await badgeWorld(db);
    const revision = await revisionIn(db, "+expositions", badge);

    await assertRejects(() =>
      expositions.withSession(async () => {
        await view.updateOne("participant", participant, {
          status: "withdrawn",
        });
        assertEquals(
          await valueIn(db, "+expositions", badge, "activeParticipantCount"),
          1,
          "outside the transaction the recomputed value is not visible yet",
        );
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

test("computed through far: a far write given a session without a transaction is refused by default", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { companies } = await openGlobal(db);
    const acme = await companies.insertOne({ name: "Acme", status: "pending" });
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

test("computed through far: in best-effort mode a far write without a transaction recomputes in sequence, with no mark", async (t) => {
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
    assertEquals(await markIds(db), []);
    assertEquals(await valueIn(db, "people", ada, "validatedEmployers"), [
      acme,
    ]);
    await expectNoDrift(db, topology);
  });
});

test("computed through far: a far mark left behind, stale or renewed, drains to the value already written at commit", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view, participant, badge } = await badgeWorld(db);
    const field = topology.field("badge", "activeParticipantCount");
    await markFar(db, field, participant, EXPO, "left behind by an old node");
    await view.updateOne("participant", participant, { status: "withdrawn" });
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      0,
    );
    await markFar(db, field, participant, EXPO, "renewed concurrently");
    const drained = await drainComputedPending(db, { topology });
    assertEquals(drained.remaining, 0);
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      0,
    );
    await expectNoDrift(db, topology);
  });
});

test("computed through far: a past-limit mark and later inline writes converge after the drain", async (t) => {
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
    await companies.updateOne({ _id: acme }, { $set: { status: "validated" } });
    assertEquals(await markIds(db), [
      `people.validatedEmployers|far|*|${acme}`,
    ]);
    const late = await people.insertOne({ name: "Eve" });
    await jobs.insertOne({ personId: late, companyId: acme });
    assertEquals(
      await valueIn(db, "people", late, "validatedEmployers"),
      [acme],
      "a near write reads the far document as committed, mark or not",
    );
    await drainComputedPending(db, { topology });
    assertEquals(await markIds(db), []);
    await expectNoDrift(db, topology);
  });
});

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function gate() {
  let open = () => {};
  const opened = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { open, opened };
}

async function interleave(
  withSession: (
    work: () => Promise<void>,
    options?: { retry?: boolean },
  ) => Promise<unknown>,
  first: () => Promise<unknown>,
  second: () => Promise<unknown>,
): Promise<void> {
  const firstWrote = gate();
  const firstMayCommit = gate();
  const a = withSession(async () => {
    await first();
    firstWrote.open();
    await firstMayCommit.opened;
  });
  await firstWrote.opened;
  const b = withSession(
    async () => {
      await second();
    },
    { retry: true },
  );
  await Promise.race([
    b.then(
      () => {},
      () => {},
    ),
    delay(300),
  ]);
  firstMayCommit.open();
  await a;
  await b;
}

type Skew = {
  readonly name: string;
  readonly world: (db: Db) => Promise<{
    readonly topology: ComputedTopology;
    readonly withSession: Parameters<typeof interleave>[0];
    readonly far: () => Promise<unknown>;
    readonly near: () => Promise<unknown>;
  }>;
};

const skews: readonly Skew[] = [
  {
    name: "a new badge and a status change of its participant",
    world: async (db) => {
      const { topology, expositions, view } = await openScoped(db);
      const participant = await view.insertOne("participant", {
        name: "Ada",
        status: "pending",
      });
      return {
        topology,
        withSession: expositions.withSession,
        far: () =>
          view.updateOne("participant", participant, { status: "active" }),
        near: () => view.insertOne("badge", { participantId: participant }),
      };
    },
  },
  {
    name: "a badge moved to a participant whose status changes",
    world: async (db) => {
      const { topology, expositions, view } = await openScoped(db);
      const first = await view.insertOne("participant", {
        name: "Ada",
        status: "active",
      });
      const second = await view.insertOne("participant", {
        name: "Bob",
        status: "pending",
      });
      const badge = await view.insertOne("badge", { participantId: first });
      return {
        topology,
        withSession: expositions.withSession,
        far: () => view.updateOne("participant", second, { status: "active" }),
        near: () => view.updateOne("badge", badge, { participantId: second }),
      };
    },
  },
  {
    name: "a kiosk enabled while its scanner comes online",
    world: async (db) => {
      const { topology, expositions, view, scans } = await openScoped(db);
      const scanner = await scans.insertOne("scanner", { status: "offline" });
      const kiosk = await view.insertOne("kiosk", {
        scannerId: scanner,
        enabled: false,
      });
      return {
        topology,
        withSession: expositions.withSession,
        far: () => scans.updateOne("scanner", scanner, { status: "online" }),
        near: () => view.updateOne("kiosk", kiosk, { enabled: true }),
      };
    },
  },
  {
    name: "a subject created while the far document its links reach changes",
    world: async (db) => {
      const { topology, companies, jobs, people } = await openGlobal(db);
      const acme = await companies.insertOne({
        name: "Acme",
        status: "pending",
      });
      const future = "person:01ffffffffffffffffffffffff";
      await jobs.insertOne({ personId: future, companyId: acme });
      return {
        topology,
        withSession: companies.withSession,
        far: () =>
          companies.updateOne({ _id: acme }, { $set: { status: "validated" } }),
        near: () => people.insertOne({ _id: future, name: "Late" }),
      };
    },
  },
  {
    name: "a badge created for a participant being deleted, whose fence was never written",
    world: async (db) => {
      const { topology, expositions, view } = await openScoped(db);
      const participant = "participant:01aaaaaaaaaaaaaaaaaaaaaaaa";
      await db.createCollection(COMPUTED_FENCES_COLLECTION);
      await db.collection("+expositions").insertOne({
        _id: participant as never,
        _type: "participant",
        _scope: EXPO,
        name: "Ada",
        status: "active",
      });
      return {
        topology,
        withSession: expositions.withSession,
        far: () => view.deleteId("participant", participant),
        near: () => view.insertOne("badge", { participantId: participant }),
      };
    },
  },
];

test("computed through far: fences leave no document behind", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view, participant, badge } = await badgeWorld(db);
    const fences = db.collection<{ _id: string }>(COMPUTED_FENCES_COLLECTION);
    assertEquals(
      (await db.listCollections({ name: COMPUTED_FENCES_COLLECTION }).toArray())
        .length,
      1,
      "the far insert and the badge insert both wrote fences",
    );
    assertEquals(await fences.countDocuments({}), 0);
    await view.updateOne("participant", participant, { status: "pending" });
    assertEquals(await fences.countDocuments({}), 0);

    await view.deleteId("participant", participant);
    assertEquals(await fences.countDocuments({}), 0);
    assertEquals(
      await valueIn(db, "+expositions", badge, "activeParticipantCount"),
      0,
    );
    assertEquals(await markIds(db), []);
    await expectNoDrift(db, topology);
  });
});

for (const skew of skews) {
  for (const farFirst of [true, false]) {
    test(`computed through far write skew: ${skew.name}, ${farFirst ? "far" : "near"} side first, is exact at commit`, async (t) => {
      await withDatabase(t.name, async (db) => {
        const world = await skew.world(db);
        await interleave(
          world.withSession,
          farFirst ? world.far : world.near,
          farFirst ? world.near : world.far,
        );
        assertEquals(await markIds(db), []);
        await expectNoDrift(db, world.topology);
      });
    });
  }
}

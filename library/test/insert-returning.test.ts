import { test } from "./+harness.ts";
import { assertEquals, assertRejects } from "./+assert.ts";
import { type Db, MongoClient } from "../src/mongodb.ts";
import { closeAllWatchers } from "../src/change-stream.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { collection } from "../src/collection.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import { computedTopology } from "../src/computed-topology.ts";
import { registerComputed } from "../src/computed-maintenance.ts";
import { TEST_URI } from "./+shared.ts";

const READS = new Set(["find", "aggregate", "count"]);
const EXPO = "exposition:returning01";

async function withCountedDatabase(
  work: (db: Db, reads: () => number) => Promise<void>,
) {
  const client = new MongoClient(TEST_URI, { monitorCommands: true });
  let count = 0;
  client.on("commandStarted", (event) => {
    if (READS.has(event.commandName)) count++;
  });
  const db = client.db(
    `@TEST_ret@${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`,
  );
  try {
    await work(db, () => count);
  } finally {
    await closeAllWatchers(db);
    await db.dropDatabase();
    await client.close();
  }
}

const Event = defineType({
  schema: v.object({
    title: v.pipe(v.string(), v.trim()),
    status: v.optional(v.picklist(["draft", "live"]), "draft"),
    startsAt: v.date(),
    tags: v.optional(v.array(v.string()), []),
    venue: v.optional(v.object({ room: v.string(), floor: v.number() })),
  }),
});

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
        event: Event,
        participant: Participant,
        org_membership: Membership,
        expo_organization: Organization,
      },
    },
  },
};

async function open(db: Db) {
  registerComputed(db, computedTopology(schemas));
  const expositions = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: schemas.scopedMultiCollections["+expositions"].types,
  });
  return expositions.scope(EXPO);
}

test("insertOneReturning: returns what getById reads, without reading", async () => {
  await withCountedDatabase(async (db, reads) => {
    const view = await open(db);
    const startsAt = new Date("2026-10-02T09:00:00.000Z");

    const before = reads();
    const returned = await view.insertOneReturning("event", {
      title: "  Keynote  ",
      startsAt,
      venue: { room: "A1", floor: 2 },
    });
    assertEquals(reads() - before, 0);

    assertEquals(returned, await view.getById("event", returned._id));
    assertEquals(returned.title, "Keynote");
    assertEquals(returned.status, "draft");
    assertEquals(returned.tags, []);
  });
});

test("insertOneReturning: a type with computed fields is read back with them", async () => {
  await withCountedDatabase(async (db, reads) => {
    const view = await open(db);

    const before = reads();
    const returned = await view.insertOneReturning("participant", {
      name: "Ada",
    });
    assertEquals(reads() - before > 0, true);
    assertEquals(returned, await view.getById("participant", returned._id));
  });
});

test("insertOneReturning: rejects a document insertOne would reject", async () => {
  await withCountedDatabase(async (db) => {
    const view = await open(db);
    await assertRejects(() =>
      view.insertOneReturning("event", { title: "No date" } as never),
    );
  });
});

test("insertManyReturning: returns, in input order, what getById reads, without reading", async () => {
  await withCountedDatabase(async (db, reads) => {
    const view = await open(db);
    const startsAt = new Date("2026-10-02T09:00:00.000Z");

    const before = reads();
    const returned = await view.insertManyReturning("event", [
      { title: " First ", startsAt },
      { title: "Second", startsAt, status: "live", tags: ["a"] },
    ]);
    assertEquals(reads() - before, 0);

    assertEquals(
      returned.map((doc) => doc.title),
      ["First", "Second"],
    );
    for (const doc of returned) {
      assertEquals(doc, await view.getById("event", doc._id));
    }
  });
});

test("insertManyReturning: a type with computed fields costs one read more than insertMany", async () => {
  await withCountedDatabase(async (db, reads) => {
    const view = await open(db);

    const plainStart = reads();
    await view.insertMany("participant", [{ name: "Ada" }, { name: "Grace" }]);
    const plainReads = reads() - plainStart;

    const before = reads();
    const returned = await view.insertManyReturning("participant", [
      { name: "Alan" },
      { name: "Edsger" },
    ]);
    assertEquals(reads() - before, plainReads + 1);
    for (const doc of returned) {
      assertEquals(doc, await view.getById("participant", doc._id));
    }
  });
});

const CrmModel = defineModel("crm", {
  schema: {
    event: Event,
    participant: Participant,
    org_membership: Membership,
    expo_organization: Organization,
  },
});

async function openMulti(db: Db) {
  registerComputed(
    db,
    computedTopology({ multiCollections: { crm: CrmModel.schema } }),
  );
  return await multiCollection(db, "crm", CrmModel);
}

test("multiCollection insertOneReturning: returns what getById reads, without reading", async () => {
  await withCountedDatabase(async (db, reads) => {
    const crm = await openMulti(db);
    const startsAt = new Date("2026-10-02T09:00:00.000Z");

    const before = reads();
    const returned = await crm.insertOneReturning("event", {
      title: "  Keynote  ",
      startsAt,
    });
    assertEquals(reads() - before, 0);
    assertEquals(returned, await crm.getById("event", returned._id));
    assertEquals(returned.title, "Keynote");
    assertEquals(returned.status, "draft");
  });
});

test("multiCollection insertManyReturning: input order, no read, and a computed type is read back", async () => {
  await withCountedDatabase(async (db, reads) => {
    const crm = await openMulti(db);
    const startsAt = new Date("2026-10-02T09:00:00.000Z");

    const before = reads();
    const events = await crm.insertManyReturning("event", [
      { title: " First ", startsAt },
      { title: "Second", startsAt, tags: ["a"] },
    ]);
    assertEquals(reads() - before, 0);
    assertEquals(
      events.map((doc) => doc.title),
      ["First", "Second"],
    );
    for (const doc of events)
      assertEquals(doc, await crm.getById("event", doc._id));

    const plainStart = reads();
    await crm.insertMany("participant", [{ name: "Ada" }]);
    const plainReads = reads() - plainStart;
    const computedStart = reads();
    const participants = await crm.insertManyReturning("participant", [
      { name: "Alan" },
      { name: "Edsger" },
    ]);
    assertEquals(reads() - computedStart, plainReads + 1);
    for (const doc of participants)
      assertEquals(doc, await crm.getById("participant", doc._id));
  });
});

test("multiCollection insertOneReturning: rejects a document insertOne would reject", async () => {
  await withCountedDatabase(async (db) => {
    const crm = await openMulti(db);
    await assertRejects(() =>
      crm.insertOneReturning("event", { title: "No date" } as never),
    );
  });
});

const NoteSchema = {
  title: v.pipe(v.string(), v.trim()),
  pinned: v.optional(v.boolean(), false),
  at: v.date(),
};

test("collection insertOneReturning and insertManyReturning: return what getById reads, in input order, without reading", async () => {
  await withCountedDatabase(async (db, reads) => {
    const notes = await collection(db, "notes", NoteSchema);
    const at = new Date("2026-10-02T09:00:00.000Z");

    const before = reads();
    const one = await notes.insertOneReturning({ title: "  Hello  ", at });
    const many = await notes.insertManyReturning([
      { title: " A ", at },
      { title: "B", at, pinned: true },
    ]);
    assertEquals(reads() - before, 0);

    assertEquals(one, await notes.getById(one._id));
    assertEquals(one.title, "Hello");
    assertEquals(one.pinned, false);
    assertEquals(
      many.map((doc) => doc.title),
      ["A", "B"],
    );
    for (const doc of many) assertEquals(doc, await notes.getById(doc._id));
  });
});

test("collection insertOneReturning: rejects a document insertOne would reject", async () => {
  await withCountedDatabase(async (db) => {
    const notes = await collection(db, "notes", NoteSchema);
    await assertRejects(() =>
      notes.insertOneReturning({ title: "No date" } as never),
    );
  });
});

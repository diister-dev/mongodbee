import { test } from "./+harness.ts";
import { assertEquals, assertRejects, assertThrows } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
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
import {
  ComputedNotRegisteredError,
  ComputedUnsupportedWriteError,
  registerComputed,
  unregisterComputed,
} from "../src/computed-maintenance.ts";
import { applyComputed, checkComputed } from "../src/computed-apply.ts";
import { getSessionContext } from "../src/session.ts";

const EXPO = "exposition:expoaaaaa01";

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
    membershipCount: from("org_membership", Membership)
      .by((m) => m.participantId)
      .count(),
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

async function open(db: Db, inlineLimit?: number) {
  const topology = computedTopology(schemas);
  registerComputed(db, topology, { inlineLimit });
  const expositions = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: schemas.scopedMultiCollections["+expositions"].types,
  });
  return { topology, view: expositions.scope(EXPO), raw: expositions };
}

const ORGANIZATION = "expo_organization:01aaaaaaaaaaaaaaaaaaaaaaaa";

test("computed maintenance: concurrent writers on one subject all land, and the subject ends exact", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view } = await open(db);
    const participant = await view.insertOne("participant", { name: "Ada" });

    await Promise.all(
      Array.from({ length: 25 }, () =>
        view.insertOne("org_membership", {
          participantId: participant,
          organizationId: ORGANIZATION,
          status: "active",
        }),
      ),
    );

    const stored = (await view.getById("participant", participant)) as {
      _computed?: { organizationIds?: string[]; membershipCount?: number };
    };
    assertEquals(stored._computed?.membershipCount, 25);
    assertEquals(stored._computed?.organizationIds?.length, 25);
    assertEquals((await checkComputed(db, topology)).drifts, []);
  });
});

test("computed maintenance: a full apply running during writes never overwrites a newer value", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view } = await open(db);
    const participants = await view.insertMany(
      "participant",
      Array.from({ length: 12 }, (_, index) => ({ name: `P${index}` })),
    );
    const { withSession } = getSessionContext(db.client);

    const writers = Array.from({ length: 40 }, (_, index) =>
      withSession(async () => {
        await view.insertOne("org_membership", {
          participantId: participants[index % participants.length]!,
          organizationId: ORGANIZATION,
          status: index % 3 === 0 ? "removed" : "active",
        });
      }).catch(async () => {
        await view.insertOne("org_membership", {
          participantId: participants[index % participants.length]!,
          organizationId: ORGANIZATION,
          status: index % 3 === 0 ? "removed" : "active",
        });
      }),
    );
    const appliers = [
      applyComputed(db, topology, { subject: "participant", batchSize: 3 }),
      applyComputed(db, topology, { subject: "participant", batchSize: 5 }),
    ];
    await Promise.all([...writers, ...appliers]);

    assertEquals((await checkComputed(db, topology)).drifts, []);
    assertEquals(await view.countDocuments("org_membership"), 40);
  });
});

test("computed maintenance: a topology registered on the client covers every database it opens, a database registration wins", async (t) => {
  await withDatabase(t.name, async (db) => {
    const topology = computedTopology(schemas);
    registerComputed(db.client, topology);
    const sibling = db.client.db(
      `@TEST_client_registration_${crypto.randomUUID().slice(0, 8)}`,
    );
    try {
      for (const target of [db, sibling]) {
        const expositions = await scopedMultiCollection(
          target,
          "+expositions",
          {
            schemaManagement: "auto",
            scope: refId("exposition"),
            types: schemas.scopedMultiCollections["+expositions"].types,
          },
        );
        const view = expositions.scope(EXPO);
        const participant = await view.insertOne("participant", {
          name: "Ada",
        });
        await view.insertOne("org_membership", {
          participantId: participant,
          organizationId: ORGANIZATION,
          status: "active",
        });
        const stored = (await view.getById("participant", participant)) as {
          _computed?: { membershipCount?: number };
        };
        assertEquals(
          stored._computed?.membershipCount,
          1,
          `${target.databaseName} is maintained through the client registration`,
        );
      }

      registerComputed(db, new ComputedTopology([]));
      const plain = await scopedMultiCollection(db, "+expositions", {
        schemaManagement: "auto",
        scope: refId("exposition"),
        types: schemas.scopedMultiCollections["+expositions"].types,
      });
      const participant = await plain
        .scope(EXPO)
        .insertOne("participant", { name: "Bob" });
      const stored = (await plain
        .scope(EXPO)
        .getById("participant", participant)) as { _computed?: unknown };
      assertEquals(
        stored._computed,
        undefined,
        "the database registration overrides the client one",
      );
    } finally {
      unregisterComputed(db.client);
      unregisterComputed(db);
      await sibling.dropDatabase();
    }
  });
});

test("computed maintenance: a node that never registered the topology cannot write computed types", async (t) => {
  await withDatabase(t.name, async (db) => {
    const unregistered = await collection(db, "people", Participant);
    await assertRejects(
      () => unregistered.insertOne({ name: "Ada" }),
      ComputedNotRegisteredError,
    );
  });
});

test("computed maintenance: a raw bulk op builder is refused on a collection that feeds computed fields", async (t) => {
  await withDatabase(t.name, async (db) => {
    const Person = defineType({
      schema: v.object({ name: v.string() }),
      computed: {
        membershipCount: from("org_membership", Membership)
          .by((m) => m.participantId)
          .count(),
      },
    });
    registerComputed(
      db,
      computedTopology({
        collections: { people: Person, org_membership: Membership },
      }),
    );
    const memberships = await collection(db, "org_membership", Membership);
    assertThrows(
      () => memberships.collection.initializeOrderedBulkOp(),
      ComputedUnsupportedWriteError,
    );
    assertThrows(
      () => memberships.collection.initializeUnorderedBulkOp(),
      ComputedUnsupportedWriteError,
    );
  });
});

test("computed maintenance: an update that touches no input of a field leaves the subject alone", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { view } = await open(db);
    const participant = await view.insertOne("participant", { name: "Ada" });
    const membership = await view.insertOne("org_membership", {
      participantId: participant,
      organizationId: ORGANIZATION,
      status: "active",
    });
    await db
      .collection("+expositions")
      .updateOne(
        { _id: participant as never },
        { $set: { "_computed.membershipCount": 99 } },
      );

    await view.updateOne("participant", participant, { name: "Ada Lovelace" });
    const untouched = (await view.getById("participant", participant)) as {
      _computed?: { membershipCount?: number };
    };
    assertEquals(
      untouched._computed?.membershipCount,
      99,
      "a subject's own unrelated update recomputes nothing",
    );

    await view.updateOne("org_membership", membership, { status: "removed" });
    const recomputed = (await view.getById("participant", participant)) as {
      _computed?: { membershipCount?: number; organizationIds?: string[] };
    };
    assertEquals(
      recomputed._computed,
      { organizationIds: [], membershipCount: 99 },
      "a status change recomputes the field filtered on status, not the unfiltered count",
    );

    await view.updateOne("org_membership", membership, {
      participantId: participant,
    });
    const healed = (await view.getById("participant", participant)) as {
      _computed?: { membershipCount?: number };
    };
    assertEquals(
      healed._computed?.membershipCount,
      1,
      "touching the subject link recomputes the count, which heals it",
    );
  });
});

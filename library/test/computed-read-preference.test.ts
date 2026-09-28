import { test } from "./+harness.ts";
import * as m from "mongodb";
import { assertEquals } from "./+assert.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { type Db, MongoClient } from "../src/mongodb.ts";
import { closeAllWatchers } from "../src/change-stream.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import { computedTopology } from "../src/computed-topology.ts";
import { registerComputed } from "../src/computed-maintenance.ts";
import { applyComputed, checkComputed } from "../src/computed-apply.ts";
import {
  drainComputedPending,
  pendingComputed,
} from "../src/computed-marks.ts";
import { computedValues, TEST_URI } from "./+shared.ts";

const EXPO = "exposition:expoaaaaa01";

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
    organizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .collect((m) => m.organizationId),
    validatedOrganizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
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
        participant: Participant,
        org_membership: Membership,
        expo_organization: Organization,
      },
    },
  },
};

async function withSecondaryPreferredClient(work: (db: Db) => Promise<void>) {
  const client = new MongoClient(TEST_URI, {
    readPreference: m.ReadPreference.SECONDARY_PREFERRED,
  });
  const db = client.db(
    `@TEST_computed_readpref@${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`,
  );
  try {
    await work(db);
  } finally {
    await closeAllWatchers(db);
    await db.dropDatabase();
    await client.close();
  }
}

test("computed + read preference: maintenance, marks, drain, full apply and check all run on a secondaryPreferred client and collection", async () => {
  await withSecondaryPreferredClient(async (db) => {
    const topology = computedTopology(schemas);
    registerComputed(db, topology);
    const expositions = await scopedMultiCollection(db, "+expositions", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: schemas.scopedMultiCollections["+expositions"].types,
      readPreference: new m.ReadPreference("secondaryPreferred", undefined, {
        maxStalenessSeconds: 90,
      }),
    });
    const view = expositions.scope(EXPO);
    const participant = await view.insertOne("participant", { name: "Ada" });
    const organization = await view.insertOne("expo_organization", {
      name: "Acme",
      status: "pending",
    });
    await view.insertOne("org_membership", {
      participantId: participant,
      organizationId: organization,
      status: "active",
    });
    await view.updateOne("expo_organization", organization, {
      status: "validated",
    });

    const drained = await drainComputedPending(db);
    assertEquals(drained.remaining, 0);
    await applyComputed(db, topology, { subject: "participant" });
    assertEquals((await checkComputed(db, topology)).drifts, []);
    assertEquals((await pendingComputed(db)).count, 0);

    const stored = (await view.getById("participant", participant)) as {
      _computed?: {
        organizationIds?: string[];
        validatedOrganizationIds?: string[];
      };
    };
    assertEquals(computedValues(stored._computed), {
      organizationIds: [organization],
      validatedOrganizationIds: [organization],
    });
  });
});

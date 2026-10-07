import * as v from "../../src/schema.ts";
import { refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import type { Db } from "../../src/mongodb.ts";
import { scopedMultiCollection } from "../../src/scoped-multi-collection.ts";
import { defineType } from "../../src/type-definition.ts";
import { from } from "../../src/computed.ts";
import { computedTopology } from "../../src/computed-topology.ts";
import { registerComputed } from "../../src/computed-maintenance.ts";

export const CLUSTER_SCOPES = [
  "exposition:expoaaaaa01",
  "exposition:expobbbbb02",
] as const;

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
    membershipCount: from("org_membership", Membership)
      .by((m) => m.participantId)
      .count(),
    validatedOrganizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .through("expo_organization", Organization, (m) => m.organizationId)
      .where((o) => [o.status, "validated"])
      .collect((o) => o._id),
  },
});

export const clusterSchemas = {
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

export async function openCluster(db: Db, inlineLimit: number) {
  const topology = computedTopology(clusterSchemas);
  registerComputed(db, topology, { inlineLimit });
  const expositions = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: clusterSchemas.scopedMultiCollections["+expositions"].types,
  });
  return { topology, expositions };
}

export function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

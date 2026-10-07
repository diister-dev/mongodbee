import { assert, assertEquals } from "../+assert.ts";
import { test } from "../+harness.ts";
import * as v from "../../src/schema.ts";
import { refId } from "../../src/ids.ts";
import { from } from "../../src/computed.ts";
import { defineType } from "../../src/type-definition.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  personal,
  personId,
} from "../../src/privacy/mod.ts";

const Membership = defineType({
  schema: v.object({
    participantId: refId("participant"),
    organizationId: refId("org"),
    label: personal(v.string(), { role: "direct" }),
  }),
});

const Participant = defineType({
  schema: v.object({
    _id: personId("participant"),
    name: personal(v.string(), { role: "direct" }),
  }),
  computed: {
    orgIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .collect((m) => m.organizationId),
    orgCount: from("org_membership", Membership)
      .by((m) => m.participantId)
      .count(),
    labels: from("org_membership", Membership)
      .by((m) => m.participantId)
      .collect((m) => m.label),
  },
});

const SCHEMAS = {
  collections: { participants: Participant, org_membership: Membership },
};
const PARTICIPANTS = "collections/participants/";
const MEMBERSHIPS = "collections/org_membership/";
const PARTICIPANT_ID = "participant:01j5zk3v8n2q4x6y8z0b1c3d5e";
const ORG_ID = "org:01j5zk3v8n2q4x6y8z0b1c3d5f";

test("computed: a type definition is planned and its computed values inherit the classification of what they collect", () => {
  const plan = buildPrivacyPlan({ schemas: SCHEMAS });
  assertEquals(
    plan.findings.filter((f) => f.level === "error"),
    [],
  );
  assertEquals(plan.summary.unknown, 0);
  const paths = new Map(
    plan.targets
      .get(PARTICIPANTS)
      ?.paths.map((path) => [path.path, path.role] as const),
  );
  assertEquals(paths.get("_computed.labels.*"), "direct");
  assertEquals(paths.get("_computed.orgIds.*"), "reference");
  assertEquals(paths.get("_computed.orgCount"), "technical");
});

test("computed: collected values stay equal to what a recomputation over the transformed sources would give", () => {
  const plan = buildPrivacyPlan({ schemas: SCHEMAS });
  const transformer = createPrivacyTransformer({
    plan,
    schemas: SCHEMAS,
    secret: "s3cret",
  });
  const participant = transformer.transform(PARTICIPANTS, {
    _id: PARTICIPANT_ID,
    name: "Alice",
    _computed: {
      orgIds: [ORG_ID],
      orgCount: 1,
      labels: ["Alice's secret club"],
      _rev: 3,
    },
  });
  const membership = transformer.transform(MEMBERSHIPS, {
    _id: "org_membership:01j5zk3v8n2q4x6y8z0b1c3d6a",
    participantId: PARTICIPANT_ID,
    organizationId: ORG_ID,
    label: "Alice's secret club",
  });
  const computed = participant.doc._computed as {
    labels: string[];
    orgIds: string[];
    orgCount: number;
    _rev: number;
  };
  assertEquals(membership.doc.participantId, participant.doc._id);
  assertEquals(computed.orgIds, [membership.doc.organizationId]);
  assertEquals(computed.labels, [membership.doc.label]);
  assert(computed.labels[0] !== "Alice's secret club");
  assertEquals(computed.orgCount, 1);
  assertEquals(computed._rev, 3);
});

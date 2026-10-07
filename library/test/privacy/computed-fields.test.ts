import { assert, assertEquals } from "../+assert.ts";
import { test } from "../+harness.ts";
import * as v from "../../src/schema.ts";
import { refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import { from } from "../../src/computed.ts";
import { defineType } from "../../src/type-definition.ts";
import {
  keepComputedRevision,
  transformState,
} from "../../src/migration/cli/commands/extract.ts";
import { createEmptyDatabaseState } from "../../src/migration/types.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  personal,
  personId,
} from "../../src/privacy/mod.ts";

const Membership = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant")),
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
      .collect((m) => m.organizationId)
      .maxEntries(10),
    orgCount: from("org_membership", Membership)
      .by((m) => m.participantId)
      .count(),
    labels: from("org_membership", Membership)
      .by((m) => m.participantId)
      .collect((m) => m.label)
      .maxEntries(10),
  },
});

const SCHEMAS = {
  collections: { participants: Participant, org_membership: Membership },
};
const PARTICIPANTS = "collections/participants/";
const PARTICIPANT_ID = "participant:01j5zk3v8n2q4x6y8z0b1c3d5e";
const ORG_ID = "org:01j5zk3v8n2q4x6y8z0b1c3d5f";

test("computed: a type definition is planned and its computed root is one derived path recomputed after the transform", () => {
  const plan = buildPrivacyPlan({ schemas: SCHEMAS });
  assertEquals(
    plan.findings.filter((f) => f.level === "error"),
    [],
  );
  assertEquals(plan.summary.unknown, 0);
  const computed = plan.targets
    .get(PARTICIPANTS)
    ?.paths.filter((path) => path.path.startsWith("_computed"));
  assertEquals(
    computed?.map((path) => [path.path, path.role, path.treatment.extract]),
    [["_computed", "derived", "recompute"]],
  );
});

test("computed: extracted computed values equal a recomputation over the transformed sources", () => {
  const plan = buildPrivacyPlan({ schemas: SCHEMAS, posture: "strict" });
  const transformer = createPrivacyTransformer({
    plan,
    schemas: SCHEMAS,
    secret: "s3cret",
    recompute: keepComputedRevision,
  });
  const state = createEmptyDatabaseState();
  state.collections.participants = {
    content: [
      {
        _id: PARTICIPANT_ID,
        name: "Alice",
        _computed: {
          orgIds: [ORG_ID],
          orgCount: 1,
          labels: ["Alice's secret club"],
          _rev: 3,
        },
      },
    ],
  };
  state.collections.org_membership = {
    content: [
      {
        _id: "org_membership:01j5zk3v8n2q4x6y8z0b1c3d6a",
        participantId: PARTICIPANT_ID,
        organizationId: ORG_ID,
        label: "Alice's secret club",
      },
    ],
  };
  const { state: out } = transformState(state, plan, transformer, {
    schemas: SCHEMAS,
    remapInstanceName: (name) => name,
  });
  const [participant] = out.collections.participants.content;
  const [membership] = out.collections.org_membership.content;
  const computed = participant._computed as {
    labels: string[];
    orgIds: string[];
    orgCount: number;
    _rev: number;
  };
  assertEquals(membership.participantId, participant._id);
  assertEquals(computed.orgIds, [membership.organizationId]);
  assertEquals(computed.labels, [membership.label]);
  assert(computed.labels[0] !== "Alice's secret club");
  assertEquals(computed.orgCount, 1);
  assertEquals(computed._rev, 3);
});

import { test } from "../../library/test/+harness.ts";
import { assert, assertEquals } from "../../library/test/+assert.ts";
import { withDatabase } from "./+db.ts";
import {
  computedTopology,
  defineType,
  from,
  refId,
  registerComputed,
  scopedMultiCollection,
  unregisterComputed,
  withIndex,
} from "@diister/mongodbee";
import * as v from "@diister/mongodbee/schema";
import type { SchemasDefinition } from "@diister/mongodbee/inspect";
import type { StudioContext } from "../src/context.ts";
import { checkComputedDrift, getComputedReport } from "../src/api/computed.ts";
import { describeComputed } from "../src/api/schema.ts";
import { conditionClause, listValues } from "../src/api/documents.ts";
import {
  sourceTarget,
  valueAt,
  withComputedColumns,
  withoutComputed,
} from "../src/ui/lib/computed.ts";
import { parseConditionParam } from "../src/ui/lib/query.ts";

const SCOPE = "exposition:expoaaaaa01";

const Organization = defineType({
  schema: v.object({
    name: v.string(),
    status: v.picklist(["pending", "validated"]),
  }),
});

const Membership = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant")),
    organizationId: withIndex(refId("organization")),
    status: v.picklist(["active", "removed", "invited"]),
  }),
});

const Participant = defineType({
  schema: v.object({ name: v.string() }),
  computed: {
    organizationIds: from("membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, ["active", "invited"]])
      .collect((m) => m.organizationId),
    membershipCount: from("membership", Membership)
      .by((m) => m.participantId)
      .count(),
    validatedIds: from("membership", Membership)
      .by((m) => m.participantId)
      .through("organization", Organization, (m) => m.organizationId)
      .where((o) => [o.status, "validated"])
      .collect((o) => o._id),
  },
});

const SCHEMAS = {
  collections: {},
  multiCollections: {},
  multiModels: {},
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: {
        participant: Participant,
        membership: Membership,
        organization: Organization,
      },
    },
  },
} satisfies SchemasDefinition;

function contextFor(db: StudioContext["db"]): StudioContext {
  return {
    db,
    schemas: SCHEMAS,
    schemasSource: "project",
    migrations: [],
    migrationFiles: new Map(),
    warnings: [],
  };
}

test("computed columns flatten _computed without its revision", () => {
  const fields = withComputedColumns({
    name: { kind: "string" },
    _computed: {
      kind: "object",
      entries: {
        organizationIds: { kind: "array", system: "computed" },
        _rev: { kind: "number", system: "revision" },
      },
    },
  });
  assertEquals(Object.keys(fields), ["name", "_computed.organizationIds"]);
  const item = {
    name: "Ada",
    _computed: { organizationIds: ["o:1"], _rev: 2 },
  };
  assertEquals(valueAt(item, "_computed.organizationIds"), ["o:1"]);
  assertEquals(valueAt(item, "name"), "Ada");
  assertEquals(
    valueAt({ name: "Bob" }, "_computed.organizationIds"),
    undefined,
  );
  assertEquals(withoutComputed(item), { name: "Ada" });
});

test("the sources of a computed value become a filtered data view", () => {
  const target = sourceTarget(
    {
      subject: "participant",
      name: "organizationIds",
      description: "",
      source: { collection: "+expositions", type: "membership", scoped: true },
      by: "participantId",
      where: { status: ["active", "invited"], archived: false },
      sameScope: false,
      aggregate: { kind: "collect", path: "organizationId", distinct: false },
    },
    { _id: "participant:p1", _scope: SCOPE },
  );
  assertEquals(target, {
    view: "collection",
    collection: "+expositions",
    tab: "data",
    type: "membership",
    scope: SCOPE,
    where: [
      "participantId:eq:participant:p1",
      'status:in:["active","invited"]',
      "archived:eq:false",
    ],
  });
  assertEquals(parseConditionParam("participantId:eq:participant:p1", 1), {
    id: 1,
    field: "participantId",
    op: "eq",
    value: "participant:p1",
  });
  assertEquals(parseConditionParam("status:between:a", 2), undefined);
});

test("the is-one-of filter reads a JSON list or a comma list", () => {
  assertEquals(listValues('["active","invited"]'), ["active", "invited"]);
  assertEquals(listValues(" a, b ,, c "), ["a", "b", "c"]);
  assertEquals(
    conditionClause({ field: "count", op: "in", value: "1, 2" }, "number"),
    { count: { $in: [1, 2] } },
  );
});

test("a computed description names its conditions and its far hop", () => {
  const participant = computedTopology(SCHEMAS).fieldsOf("participant");
  const byName = Object.fromEntries(
    participant.map((field) => [
      field.name,
      describeComputed(field.descriptor),
    ]),
  );
  assertEquals(
    byName.organizationIds,
    "organizationId of membership by participantId where status in active, invited",
  );
  assertEquals(byName.membershipCount, "count of membership by participantId");
  assertEquals(
    byName.validatedIds,
    "_id of organization where status = validated, through membership.organizationId by participantId",
  );
});

test({
  name: "studio computed report: filled share, revisions and a drift check",
  timeout: 60_000,
  fn: async (t) => {
    await withDatabase(t.name, async (db) => {
      registerComputed(db, computedTopology(SCHEMAS));
      try {
        const expositions = await scopedMultiCollection(db, "+expositions", {
          schemaManagement: "auto",
          scope: refId("exposition"),
          types: SCHEMAS.scopedMultiCollections["+expositions"].types,
        });
        const view = expositions.scope(SCOPE);
        const org = await view.insertOne("organization", {
          name: "Acme",
          status: "validated",
        });
        const ada = await view.insertOne("participant", { name: "Ada" });
        const bob = await view.insertOne("participant", { name: "Bob" });
        await view.insertOne("membership", {
          participantId: ada,
          organizationId: org,
          status: "active",
        });
        await db
          .collection<{ _id: string }>("+expositions")
          .updateOne(
            { _id: bob },
            { $set: { "_computed.membershipCount": 7 } },
          );

        const context = contextFor(db);
        const report = await getComputedReport(
          context,
          "+expositions",
          new URLSearchParams({ type: "participant" }),
        );
        assertEquals(report.subjects, ["participant"]);
        assertEquals(
          report.fields.map((field) => field.name),
          ["organizationIds", "membershipCount", "validatedIds"],
        );
        const ids = report.fields.find(
          (field) => field.name === "organizationIds",
        );
        assertEquals(ids?.source, {
          collection: "+expositions",
          type: "membership",
          scoped: true,
        });
        assertEquals(ids?.where, { status: ["active", "invited"] });
        assertEquals(ids?.sampled, 2);
        assertEquals(ids?.filled, 2);
        const validated = report.fields.find(
          (field) => field.name === "validatedIds",
        );
        assertEquals(validated?.through?.source.type, "organization");
        assertEquals(validated?.through?.via, "organizationId");
        assert((report.stats?.revisions.length ?? 0) > 0);
        assertEquals(
          report.pending?.count,
          report.fields.reduce((sum, field) => sum + (field.pending ?? 0), 0),
        );

        const light = await getComputedReport(
          context,
          "+expositions",
          new URLSearchParams({ type: "participant", stats: "false" }),
        );
        assertEquals(light.stats, undefined);
        assertEquals(light.fields[0]?.filled, undefined);

        const drift = await checkComputedDrift(
          context,
          "+expositions",
          new URLSearchParams({ type: "participant", limit: "50" }),
        );
        assertEquals(drift.checked, 2);
        assertEquals(drift.complete, true);
        assertEquals(drift.drifted, {
          organizationIds: 0,
          membershipCount: 1,
          validatedIds: 0,
        });
        assertEquals(drift.examples.length, 1);
        assertEquals(drift.examples[0].id, bob);
        assertEquals(drift.examples[0].open, JSON.stringify(bob));
        assertEquals(drift.examples[0].stored, 7);
        assertEquals(drift.examples[0].truth, 0);
      } finally {
        unregisterComputed(db);
      }
    });
  },
});

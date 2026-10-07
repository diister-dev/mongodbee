import { test } from "./+harness.ts";
import { assert, assertEquals, assertThrows } from "./+assert.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { index } from "../src/indexes.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import {
  computedOf,
  defineType,
  fieldsOf,
  withFields,
} from "../src/type-definition.ts";
import {
  COMPUTED_ROOT,
  ComputedDefinitionError,
  from,
} from "../src/computed.ts";

const Membership = defineType({
  schema: v.object({
    participantId: refId("participant"),
    organizationId: refId("expo_organization"),
    status: v.picklist(["active", "removed"]),
    note: v.optional(v.string()),
  }),
});

const Organization = defineType({
  schema: v.object({
    name: v.string(),
    data: v.object({
      status: v.picklist(["pending", "validated", "rejected"]),
    }),
  }),
});

const Scan = defineType({
  schema: v.object({
    participantId: refId("participant"),
    scanType: v.picklist(["security", "business"]),
  }),
});

const ScansModel = defineModel("scans", { schema: { scan: Scan } });

const participantSchema = v.object({
  status: v.picklist(["active", "detached"]),
  name: v.string(),
});

const Participant = defineType({
  schema: participantSchema,
  computed: {
    organizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .collect((m) => m.organizationId),
    organizationCount: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, ["active"]])
      .count(),
    scanKinds: from(ScansModel, "scan")
      .by((s) => s.participantId)
      .sameScope()
      .collect((s) => s.scanType)
      .distinct()
      .maxEntries(4),
    validatedOrganizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .through("expo_organization", Organization, (m) => m.organizationId)
      .where((o) => [o.data.status, "validated"])
      .collect((o) => o._id),
  },
  indexes: (f) => [index(f._computed.organizationIds)],
});

type ParticipantComputed = NonNullable<
  v.InferOutput<typeof Participant.schema>["_computed"]
>;
const typed: ParticipantComputed = {
  organizationIds: ["expo_organization:abc"],
  organizationCount: 1,
  scanKinds: ["security"],
  validatedOrganizationIds: ["expo_organization:abc"],
};

// @ts-expect-error the collected entry keeps the source type: a reference, never a number
export const mistyped: ParticipantComputed = { organizationIds: [1] };

test("computed: each declaration becomes a frozen, serializable descriptor", () => {
  assertEquals(computedOf(Participant), {
    organizationIds: {
      source: { type: "org_membership" },
      by: "participantId",
      sameScope: false,
      where: { status: "active" },
      aggregate: { kind: "collect", path: "organizationId", distinct: false },
    },
    organizationCount: {
      source: { type: "org_membership" },
      by: "participantId",
      sameScope: false,
      where: { status: ["active"] },
      aggregate: { kind: "count" },
    },
    scanKinds: {
      source: { model: "scans", type: "scan" },
      by: "participantId",
      sameScope: true,
      where: {},
      aggregate: {
        kind: "collect",
        path: "scanType",
        distinct: true,
        maxEntries: 4,
      },
    },
    validatedOrganizationIds: {
      source: { type: "org_membership" },
      by: "participantId",
      sameScope: false,
      where: { status: "active" },
      through: {
        source: { type: "expo_organization" },
        via: "organizationId",
        where: { "data.status": "validated" },
      },
      aggregate: { kind: "collect", path: "_id", distinct: false },
    },
  });
  const descriptor = computedOf(Participant).organizationIds;
  assert(
    Object.isFrozen(descriptor) &&
      Object.isFrozen(descriptor.where) &&
      Object.isFrozen(descriptor.aggregate),
  );
  assertEquals(
    JSON.parse(JSON.stringify(computedOf(Participant))),
    computedOf(Participant),
  );
  assertEquals(typed.organizationCount, 1);
});

test("computed: the schema gains a generated _computed root that validates what mongodbee will write", () => {
  const schema = v.object(fieldsOf(Participant));
  const base = { status: "active" as const, name: "Ada" };

  assert(
    v.safeParse(schema, base).success,
    "a subject not computed yet is valid",
  );
  assert(
    v.safeParse(schema, { ...base, _computed: {} }).success,
    "each computed field is optional: absent is not empty",
  );
  assert(v.safeParse(schema, { ...base, _computed: typed }).success);
  assert(
    !v.safeParse(schema, {
      ...base,
      _computed: { organizationIds: ["user:nope"] },
    }).success,
    "the collected entry keeps the source field's type",
  );
  assert(
    !v.safeParse(schema, { ...base, _computed: { organizationCount: -1 } })
      .success,
    "a count is a non-negative integer",
  );
  assert(
    !v.safeParse(schema, { ...base, _computed: { organizationCount: 1.5 } })
      .success,
  );
  assert(
    !v.safeParse(schema, {
      ...base,
      _computed: {
        scanKinds: ["security", "business", "security", "business", "security"],
      },
    }).success,
    "maxEntries bounds the array",
  );
  assertEquals(Object.keys(fieldsOf(Participant)).sort(), [
    "_computed",
    "name",
    "status",
  ]);
});

test("computed: a type without computed fields is left untouched", () => {
  const Plain = defineType({ schema: participantSchema });
  assertEquals(Object.keys(fieldsOf(Plain)).sort(), ["name", "status"]);
  assertEquals(computedOf(Plain), {});
  assert(Plain.schema === participantSchema);
});

test("computed: withFields keeps the declarations and does not re-declare the generated root", () => {
  const widened = withFields(Participant, { nickname: v.optional(v.string()) });
  assertEquals(Object.keys(computedOf(widened)).sort(), [
    "organizationCount",
    "organizationIds",
    "scanKinds",
    "validatedOrganizationIds",
  ]);
  assertEquals(Object.keys(fieldsOf(widened)).sort(), [
    COMPUTED_ROOT,
    "name",
    "nickname",
    "status",
  ]);
});

test("computed: a loose or piped subject schema keeps its kind and its checks", () => {
  const Loose = defineType({
    schema: v.looseObject({ name: v.string() }),
    computed: {
      n: from("org_membership", Membership)
        .by((m) => m.participantId)
        .count(),
    },
  });
  assertEquals(Loose.schema.type, "loose_object");
  const Checked = defineType({
    schema: v.pipe(
      v.object({ name: v.string() }),
      v.check((doc) => doc.name !== "forbidden", "name is forbidden"),
    ) as never,
    computed: {
      n: from("org_membership", Membership)
        .by((m) => m.participantId)
        .count(),
    },
  });
  assert(
    !v.safeParse(Checked.schema, { name: "forbidden" }).success,
    "the subject's own check still runs",
  );
});

test("computed: a mistake in a declaration fails at definition, naming what is wrong", () => {
  const cases: Array<[string, () => unknown, string]> = [
    [
      "no subject",
      () => from("org_membership", Membership).count(),
      "needs by()",
    ],
    [
      "unknown by path",
      () =>
        from("org_membership", Membership)
          .by((m) => (m as never as { nope: never }).nope)
          .count(),
      '"nope" does not exist',
    ],
    [
      "unknown where path",
      () =>
        from("org_membership", Membership)
          .by((m) => m.participantId)
          .where((m) => [(m as never as { nope: never }).nope, "x"]),
      '"nope" does not exist',
    ],
    [
      "unknown collected path",
      () =>
        from("org_membership", Membership)
          .by((m) => m.participantId)
          .collect((m) => (m as never as { nope: never }).nope),
      '"nope" does not exist',
    ],
    [
      "unknown far path",
      () =>
        from("org_membership", Membership)
          .by((m) => m.participantId)
          .through("expo_organization", Organization, (m) => m.organizationId)
          .collect((o) => (o as never as { nope: never }).nope),
      '"nope" does not exist',
    ],
    [
      "object in where",
      () =>
        from("org_membership", Membership)
          .by((m) => m.participantId)
          .where((m) => [m.status, { $ne: "x" } as never]),
      "string, number, boolean or null",
    ],
    [
      "two hops",
      () =>
        from("org_membership", Membership)
          .by((m) => m.participantId)
          .through("expo_organization", Organization, (m) => m.organizationId)
          .through("expo_organization", Organization, () => ({}) as never),
      "at most one extra hop",
    ],
    [
      "by after through",
      () =>
        from("org_membership", Membership)
          .through("expo_organization", Organization, (m) => m.organizationId)
          .by(() => ({}) as never),
      "before through()",
    ],
    [
      "bad maxEntries",
      () =>
        from("org_membership", Membership)
          .by((m) => m.participantId)
          .collect((m) => m.organizationId)
          .maxEntries(0),
      "positive integer",
    ],
    [
      "unknown model type",
      () => from(ScansModel, "nope" as never),
      'has no type "nope"',
    ],
    [
      "declared root",
      () =>
        defineType({
          schema: v.object({ _computed: v.object({}) }),
          computed: {
            n: from("org_membership", Membership)
              .by((m) => m.participantId)
              .count(),
          },
        }),
      "generated",
    ],
    [
      "bad name",
      () =>
        defineType({
          schema: participantSchema,
          computed: {
            "not-a-name": from("org_membership", Membership)
              .by((m) => m.participantId)
              .count(),
          },
        }),
      "identifier",
    ],
    [
      "not a declaration",
      () =>
        defineType({
          schema: participantSchema,
          computed: { raw: { kind: "count" } as never },
        }),
      "must be built with",
    ],
    [
      "index on missing computed",
      () =>
        defineType({
          schema: participantSchema,
          indexes: (f) => [
            index((f as never as { _computed: { x: never } })._computed.x),
          ],
        }),
      "does not exist",
    ],
  ];
  for (const [label, build, message] of cases) {
    const error = assertThrows(build, Error);
    assert(
      message === "does not exist" || error instanceof ComputedDefinitionError,
      `${label}: ${error.name}`,
    );
    assert(
      error.message.includes(message),
      `${label}: "${error.message}" should mention ${message}`,
    );
  }
});

import { assert, assertEquals, assertStringIncludes } from "@std/assert";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import {
  buildPrivacyPlan,
  dynamic,
  mention,
  mirrorOf,
  notPersonal,
  personal,
  personId,
  type PrivacyPlan,
  renderPrivacyReport,
} from "../../src/privacy/mod.ts";

const EmailSchema = personal(v.pipe(v.string(), v.email()), { role: "direct" });
const FirstNameSchema = personal(v.pipe(v.string(), v.minLength(1)), {
  role: "direct",
});
const LocaleSchema = personal(v.picklist(["fr", "en"]), { role: "quasi" });

function pathOf(plan: PrivacyPlan, key: string, path: string) {
  const target = plan.targets.get(key);
  assert(
    target,
    `target ${key} missing (have ${[...plan.targets.keys()].join(", ")})`,
  );
  const found = target.paths.find((p) => p.path === path);
  assert(
    found,
    `path ${path} missing in ${key} (have ${
      target.paths.map((p) => p.path).join(", ")
    })`,
  );
  return found;
}

Deno.test("messaging: a document about two persons is ambiguous until both owners are declared", () => {
  const messages = {
    _id: dbId("message"),
    from: refId("user"),
    to: refId("user"),
    body: personal(v.string(), { role: "content" }),
  };
  const users = { _id: personId("user"), email: EmailSchema };

  const undeclared = buildPrivacyPlan({
    schemas: { collections: { users, messages } },
  });
  const target = undeclared.targets.get("collections/messages/")!;
  assertEquals(target.owner.kind, "ambiguous");
  assert(
    undeclared.findings.some((f) =>
      f.level === "warning" && f.target === target.key
    ),
  );
  assertEquals(pathOf(undeclared, target.key, "body").tier, "declared");

  const declared = buildPrivacyPlan({
    schemas: {
      collections: {
        users,
        messages: {
          ...messages,
          _id: personal(dbId("message"), { of: ["from", "to"] }),
        },
      },
    },
  });
  const owner = declared.targets.get("collections/messages/")!.owner;
  assertEquals(owner.kind, "declared");
  assertEquals(owner.spaces, ["user", "user"]);
  assertEquals(owner.via, ["from", "to"]);
  assertEquals(owner.chain, ["user"]);
  assertEquals(
    pathOf(declared, "collections/messages/", "from").relation,
    "owner",
  );
  assertEquals(
    pathOf(declared, "collections/messages/", "to").relation,
    "owner",
  );
  assertEquals(declared.findings.filter((f) => f.level !== "info").length, 0);
});

Deno.test("guardian: a person referencing another person is a mention, never an inferred delegation", () => {
  const users = { _id: personId("user"), email: EmailSchema };
  const implicit = buildPrivacyPlan({
    schemas: {
      collections: {
        users,
        minors: {
          _id: personId("minor"),
          firstname: FirstNameSchema,
          guardianId: refId("user"),
        },
      },
    },
  });
  assertEquals(implicit.targets.get("collections/minors/")!.owner.kind, "self");
  assertEquals(
    pathOf(implicit, "collections/minors/", "guardianId").relation,
    "mention",
  );
  assertEquals(implicit.persons.get("minor")!.delegatesTo, []);

  const explicit = buildPrivacyPlan({
    schemas: {
      collections: {
        users,
        minors: {
          _id: personId("minor", { of: "user" }),
          firstname: FirstNameSchema,
          guardianId: refId("user"),
        },
      },
    },
  });
  assertEquals(
    pathOf(explicit, "collections/minors/", "guardianId").relation,
    "delegation",
  );
  assertEquals(explicit.persons.get("minor")!.delegatesTo, ["user"]);
});

Deno.test("delegation to a space that is not a person is an error", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        entreprises: { _id: dbId("entreprise"), name: v.string() },
        staff: {
          _id: personId("staff", { of: "entreprise" }),
          entrepriseId: refId("entreprise"),
          email: EmailSchema,
        },
      },
    },
  });
  assert(
    plan.findings.some((f) =>
      f.level === "error" && f.message.includes("not a person space")
    ),
  );
});

Deno.test("marketplace: ownership chains through a non-person space only when declared", () => {
  const users = { _id: personId("user"), email: EmailSchema };
  const orders = {
    _id: dbId("order"),
    customerId: refId("user"),
    total: v.number(),
  };
  const shipments = {
    _id: dbId("shipment"),
    orderId: refId("order"),
    trackingNotes: personal(v.string(), { role: "content" }),
  };

  const bare = buildPrivacyPlan({
    schemas: { collections: { users, orders, shipments } },
  });
  assertEquals(bare.targets.get("collections/orders/")!.owner.kind, "inferred");
  assertEquals(bare.targets.get("collections/orders/")!.owner.chain, ["user"]);
  assertEquals(
    pathOf(bare, "collections/orders/", "customerId").relation,
    "owner",
  );
  const total = pathOf(bare, "collections/orders/", "total");
  assertEquals([total.tier, total.role], ["inferred", "technical"]);

  const shipment = bare.targets.get("collections/shipments/")!;
  assertEquals(shipment.owner.kind, "none");
  assertEquals(pathOf(bare, shipment.key, "trackingNotes").tier, "unknown");
  assertEquals(
    pathOf(bare, shipment.key, "trackingNotes").treatment.extract,
    "drop",
  );
  assert(
    bare.findings.some((f) =>
      f.level === "warning" && f.target === shipment.key
    ),
  );

  const chained = buildPrivacyPlan({
    schemas: {
      collections: {
        users,
        orders,
        shipments: {
          ...shipments,
          _id: personal(dbId("shipment"), { of: "order" }),
        },
      },
    },
  });
  const owner = chained.targets.get("collections/shipments/")!.owner;
  assertEquals(owner.kind, "declared");
  assertEquals(owner.spaces, ["order"]);
  assertEquals(owner.chain, ["order", "user"]);
  assertEquals(
    pathOf(chained, "collections/shipments/", "orderId").relation,
    "owner",
  );
  assertEquals(
    pathOf(chained, "collections/shipments/", "trackingNotes").tier,
    "declared",
  );
});

Deno.test("owner declared on a space that no field references is an error", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: EmailSchema },
        notes: {
          _id: personal(dbId("note"), { of: "user" }),
          text: v.string(),
        },
      },
    },
  });
  assert(
    plan.findings.some((f) =>
      f.level === "error" && f.message.includes("no field references")
    ),
  );
});

const EXPOSITION = {
  collections: {
    "+users": {
      _id: personId("user"),
      email: withIndex(EmailSchema, { unique: true, insensitive: true }),
      firstname: v.optional(FirstNameSchema),
      preferredLocale: v.optional(LocaleSchema),
      status: v.picklist(["active", "banned"]),
      statusReason: v.string(),
      statusChangedBy: v.optional(refId("user")),
      tags: notPersonal(v.array(v.string()), "admin vocabulary"),
      marketing: v.boolean(),
    },
    "+accountless": {
      _id: personId("accountless_identity"),
      email: EmailSchema,
      firstname: v.string(),
    },
  },
  scopedMultiCollections: {
    exposition: {
      scope: refId("exposition"),
      types: {
        information: {
          _id: refId("exposition"),
          title: v.string(),
          createdBy: refId("user"),
        },
        participant: {
          _id: personId("participant", {
            of: ["user", "accountless_identity"],
          }),
          personRef: v.variant("kind", [
            v.object({ kind: v.literal("user"), userId: refId("user") }),
            v.object({
              kind: v.literal("accountless"),
              accountlessIdentityId: refId("accountless_identity"),
            }),
          ]),
          fields: v.record(
            v.string(),
            v.object({ t: v.string(), v: v.unknown() }),
          ),
          invitedBy: v.optional(refId("participant")),
        },
        scan_history: {
          _id: personal(dbId("scan_history"), { of: "participant" }),
          participantId: refId("participant"),
          scannedBy: v.nullable(refId("user")),
          label: v.picklist(["security", "business"]),
          at: v.date(),
        },
        badge: { participantId: refId("participant"), number: v.string() },
        exhibitor_contact: {
          _id: personal(dbId("exhibitor_contact"), { of: "participantId" }),
          participantId: refId("participant"),
          ownerId: v.optional(refId("participant")),
          comments: v.array(
            v.object({
              from: refId("user"),
              content: personal(v.string(), { role: "content" }),
            }),
          ),
        },
      },
    },
  },
};

Deno.test("exposition: persons, delegation, inference and declared owners across a scoped collection", () => {
  const plan = buildPrivacyPlan({ schemas: EXPOSITION });
  assertEquals([...plan.persons.keys()].sort(), [
    "accountless_identity",
    "participant",
    "user",
  ]);
  assertEquals(plan.persons.get("participant")!.delegatesTo, [
    "user",
    "accountless_identity",
  ]);

  const users = "collections/+users/";
  const email = pathOf(plan, users, "email");
  assertEquals([email.tier, email.role], ["declared", "direct"]);
  assertEquals(email.treatment.extract, "pseudonym");
  assertEquals(pathOf(plan, users, "firstname").tier, "declared");
  assertEquals(pathOf(plan, users, "preferredLocale").role, "quasi");
  const status = pathOf(plan, users, "status");
  assertEquals([status.tier, status.role], ["inferred", "technical"]);
  assertEquals(status.values, ["active", "banned"]);
  assertEquals(pathOf(plan, users, "statusReason").tier, "unknown");
  assertEquals(pathOf(plan, users, "statusChangedBy").relation, "mention");
  assertEquals(pathOf(plan, users, "tags").note, "admin vocabulary");
  assertEquals(pathOf(plan, users, "marketing").role, "technical");

  const participant = "scopedMultiCollections/exposition/participant";
  assertEquals(plan.targets.get(participant)!.owner.kind, "self");
  assertEquals(
    pathOf(plan, participant, "personRef.userId").relation,
    "delegation",
  );
  assertEquals(
    pathOf(plan, participant, "personRef.accountlessIdentityId").relation,
    "delegation",
  );
  assertEquals(pathOf(plan, participant, "personRef.kind").role, "technical");
  assertEquals(pathOf(plan, participant, "invitedBy").relation, "mention");
  assertEquals(pathOf(plan, participant, "fields.*.t").tier, "unknown");
  assertEquals(pathOf(plan, participant, "fields.*.v").tier, "unknown");

  const scan = plan.targets.get(
    "scopedMultiCollections/exposition/scan_history",
  )!;
  assertEquals(scan.owner.kind, "declared");
  assertEquals(scan.owner.via, ["participantId"]);
  assertEquals(scan.owner.chain, ["participant"]);
  assertEquals(pathOf(plan, scan.key, "scannedBy").relation, "mention");
  assertEquals(pathOf(plan, scan.key, "at").role, "technical");

  const badge = plan.targets.get("scopedMultiCollections/exposition/badge")!;
  assertEquals(badge.space, "badge");
  assertEquals(badge.owner.kind, "inferred");
  assertEquals(badge.owner.via, ["participantId"]);
  assertEquals(pathOf(plan, badge.key, "number").tier, "unknown");

  const contact = plan.targets.get(
    "scopedMultiCollections/exposition/exhibitor_contact",
  )!;
  assertEquals(contact.owner.kind, "declared");
  assertEquals(contact.owner.via, ["participantId"]);
  assertEquals(pathOf(plan, contact.key, "ownerId").relation, "mention");
  assertEquals(
    pathOf(plan, contact.key, "comments.*.from").relation,
    "mention",
  );
  assertEquals(pathOf(plan, contact.key, "comments.*.content").role, "content");

  const information = plan.targets.get(
    "scopedMultiCollections/exposition/information",
  )!;
  assertEquals(information.owner.kind, "inferred");
  assertEquals(information.owner.via, ["createdBy"]);

  assertEquals(plan.findings.filter((f) => f.level === "error"), []);
  assert(plan.summary.unknown >= 4);
});

Deno.test("embedded persons: direct identifiers in a document without owner are reported, not classified", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        programs: {
          _id: dbId("program"),
          title: v.string(),
          speakers: v.array(
            v.object({ name: FirstNameSchema, bio: v.string() }),
          ),
        },
      },
    },
  });
  const programs = plan.targets.get("collections/programs/")!;
  assertEquals(programs.owner.kind, "none");
  const name = pathOf(plan, programs.key, "speakers.*.name");
  assertEquals(name.tier, "unknown");
  assertStringIncludes(name.note ?? "", "without owner");
  assertEquals(pathOf(plan, programs.key, "title").tier, "none");
  assert(
    plan.findings.some((f) =>
      f.level === "warning" && f.target === programs.key
    ),
  );
});

Deno.test("exempt document: typed identifiers become none, person references stay mentions", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: EmailSchema },
        entreprises: {
          _id: notPersonal(dbId("entreprise"), "legal person"),
          email: EmailSchema,
          createdBy: refId("user"),
        },
      },
    },
  });
  const entreprises = plan.targets.get("collections/entreprises/")!;
  assertEquals(entreprises.owner.kind, "exempt");
  assertEquals(pathOf(plan, entreprises.key, "email").tier, "none");
  assertEquals(pathOf(plan, entreprises.key, "createdBy").relation, "mention");
  assertEquals(
    pathOf(plan, entreprises.key, "createdBy").treatment.extract,
    "remap",
  );
  assertEquals(plan.findings.filter((f) => f.level !== "info"), []);
});

Deno.test("derived values: mirrors and recomputed fields never keep the original", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: {
          _id: personId("user"),
          email: EmailSchema,
          emailLower: mirrorOf(v.string(), "user.email", {
            normalize: "lowercase",
          }),
          searchTokens: personal(v.array(v.string()), { role: "derived" }),
          avatar: mirrorOf(v.string(), "picture.path"),
        },
      },
    },
  });
  const users = "collections/users/";
  const lower = pathOf(plan, users, "emailLower");
  assertEquals([lower.role, lower.mirrorOf, lower.normalize], [
    "derived",
    "user.email",
    "lowercase",
  ]);
  assertEquals(lower.treatment.extract, "recompute");
  assertEquals(
    pathOf(plan, users, "searchTokens").treatment.extract,
    "recompute",
  );
  assert(
    plan.findings.some((f) => f.level === "warning" && f.path === "avatar"),
  );
});

Deno.test("polymorphic reference: only the person option counts for ownership", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: EmailSchema },
        entreprises: {
          _id: notPersonal(dbId("entreprise"), "legal person"),
          name: v.string(),
        },
        assets: {
          _id: dbId("asset"),
          ownerRef: v.union([refId("user"), refId("entreprise")]),
          size: v.number(),
        },
      },
    },
  });
  const assets = plan.targets.get("collections/assets/")!;
  assertEquals(assets.owner.kind, "inferred");
  assertEquals(assets.owner.spaces, ["user"]);
  const ref = pathOf(plan, assets.key, "ownerRef");
  assertEquals(ref.spaces, ["user", "entreprise"]);
  assertEquals(ref.note, "polymorphic reference");
  assertEquals(ref.relation, "owner");
});

Deno.test("treatment per direction: an audit reference is remapped on extract and kept on erase", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: EmailSchema },
        audit: {
          _id: personal(dbId("audit"), { of: "user" }),
          performedBy: personal(refId("user"), {
            role: "technical",
            treatment: { erase: "keep" },
          }),
          action: v.picklist(["login", "delete"]),
          at: v.date(),
        },
      },
    },
  });
  const performedBy = pathOf(plan, "collections/audit/", "performedBy");
  assertEquals(performedBy.relation, "owner");
  assertEquals(performedBy.treatment, {
    extract: "remap",
    erase: "keep",
    export: "exclude",
  });
});

Deno.test("report: renders persons, owners, unknowns and findings", () => {
  const plan = buildPrivacyPlan({ schemas: EXPOSITION });
  const text = renderPrivacyReport(plan);
  assertStringIncludes(text, "persons");
  assertStringIncludes(text, "participant");
  assertStringIncludes(text, "delegates to user | accountless_identity");
  assertStringIncludes(text, "owner participant (declared via participantId)");
  assertStringIncludes(text, "statusReason");
  assertStringIncludes(text, "UNKNOWN");
  assertStringIncludes(text, "summary");
});

Deno.test("retrofit trap: a schema with no person space at all is an error, not a green report", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: {
          _id: dbId("user"),
          email: v.pipe(v.string(), v.regex(/@/)),
          createdAt: v.date(),
        },
      },
    },
  });
  assertEquals(plan.persons.size, 0);
  assert(plan.findings.some((f) => f.level === "error" && f.target === "*"));
  assertStringIncludes(renderPrivacyReport(plan), "NONE DECLARED");
});

Deno.test("dates: a string carrying an ISO action is technical, like a date", () => {
  const DateSchema = v.union([v.date(), v.pipe(v.string(), v.isoTimestamp())]);
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: {
          _id: personId("user"),
          email: EmailSchema,
          createdAt: DateSchema,
          nickname: v.string(),
        },
      },
    },
  });
  const created = pathOf(plan, "collections/users/", "createdAt");
  assertEquals([created.tier, created.role], ["inferred", "technical"]);
  assertEquals(pathOf(plan, "collections/users/", "nickname").tier, "unknown");
});

Deno.test("authorship: mention() removes a reference from owner inference without losing the remap", () => {
  const users = { _id: personId("user"), email: EmailSchema };
  const dashboards = {
    _id: dbId("dashboard"),
    ownerId: refId("user"),
    createdBy: mention(refId("user")),
    updatedBy: mention(refId("user")),
    title: v.string(),
  };
  const plan = buildPrivacyPlan({
    schemas: { collections: { users, dashboards } },
  });
  const target = plan.targets.get("collections/dashboards/")!;
  assertEquals(target.owner.kind, "inferred");
  assertEquals(target.owner.via, ["ownerId"]);
  assertEquals(pathOf(plan, target.key, "createdBy").relation, "mention");
  assertEquals(
    pathOf(plan, target.key, "createdBy").treatment.extract,
    "remap",
  );
  assertEquals(plan.findings.filter((f) => f.level === "warning"), []);

  const onlyAuthors = buildPrivacyPlan({
    schemas: {
      collections: {
        users,
        roles: {
          _id: dbId("role"),
          name: v.string(),
          createdBy: mention(refId("user")),
        },
      },
    },
  });
  assertEquals(
    onlyAuthors.targets.get("collections/roles/")!.owner.kind,
    "none",
  );
});

Deno.test("wrappers: an optional boolean or picklist is technical, the wrapper is not a type", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: {
          _id: personId("user"),
          email: EmailSchema,
          marketingConsent: v.optional(v.boolean()),
          locale: v.nullable(v.picklist(["fr", "en"])),
          statusChangedAt: v.optional(
            v.union([v.date(), v.pipe(v.string(), v.isoTimestamp())]),
          ),
        },
      },
    },
  });
  for (const path of ["marketingConsent", "locale", "statusChangedAt"]) {
    const p = pathOf(plan, "collections/users/", path);
    assertEquals([p.tier, p.role, p.treatment.extract], [
      "inferred",
      "technical",
      "keep",
    ], path);
  }
});

Deno.test("dynamic: a data-driven subtree is delegated to a resolver, its static signals are kept", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: EmailSchema },
        forms: {
          _id: personal(dbId("form"), { of: "user" }),
          userId: refId("user"),
          fields: dynamic(v.record(
            v.string(),
            v.object({
              t: v.string(),
              v: v.unknown(),
              o: v.optional(v.picklist(["flow", "dashboard"])),
            }),
          )),
        },
      },
    },
  });
  const root = pathOf(plan, "collections/forms/", "fields");
  assertEquals([root.tier, root.role], ["declared", "dynamic"]);
  const value = pathOf(plan, "collections/forms/", "fields.*.v");
  assertEquals([value.tier, value.dynamicRoot, value.treatment.extract], [
    "dynamic",
    "fields",
    "drop",
  ]);
  assertEquals(
    pathOf(plan, "collections/forms/", "fields.*.t").tier,
    "dynamic",
  );
  const origin = pathOf(plan, "collections/forms/", "fields.*.o");
  assertEquals([origin.tier, origin.role, origin.dynamicRoot], [
    "inferred",
    "technical",
    "fields",
  ]);
  assertEquals(plan.summary.dynamic, 2);
  assertEquals(plan.summary.unknown, 0);
});

Deno.test("unique index: a direct identifier in an owned document, nothing in an unowned one", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: {
          _id: personId("user"),
          handle: withIndex(v.string(), { unique: true }),
        },
        roles: {
          _id: dbId("role"),
          name: withIndex(v.string(), { unique: true }),
        },
      },
    },
  });
  const handle = pathOf(plan, "collections/users/", "handle");
  assertEquals([handle.tier, handle.role], ["certain", "direct"]);
  assertEquals(pathOf(plan, "collections/roles/", "name").tier, "none");
  assertEquals(
    plan.findings.filter((f) => f.target === "collections/roles/"),
    [],
  );
});

Deno.test("delegation: a polymorphic reference that can point elsewhere is a mention, not a delegation path", () => {
  const plan = buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: EmailSchema },
        flow_sessions: { _id: dbId("flow_session"), step: v.number() },
        participants: {
          _id: personId("participant", { of: ["user"] }),
          userId: refId("user"),
          source: v.object({
            ref: v.union([refId("flow_session"), refId("user")]),
          }),
        },
      },
    },
  });
  const target = plan.targets.get("collections/participants/")!;
  assertEquals(target.owner.via, ["userId"]);
  assertEquals(pathOf(plan, target.key, "userId").relation, "delegation");
  assertEquals(pathOf(plan, target.key, "source.ref").relation, "mention");
});

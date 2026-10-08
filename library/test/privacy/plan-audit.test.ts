import { test } from "../+harness.ts";
import {
  assert,
  assertEquals,
  assertExists,
  assertNotEquals,
} from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import { from } from "../../src/computed.ts";
import { defineType } from "../../src/type-definition.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  dynamic,
  mention,
  mirrorOf,
  notPersonal,
  personal,
  personId,
  type PrivacyPlan,
  type PrivacyPosture,
  renderPrivacyReport,
  walkDocument,
} from "../../src/privacy/mod.ts";
import type { SchemasDefinition } from "../../src/migration/types.ts";

const Email = personal(v.pipe(v.string(), v.email()), { role: "direct" });
const Direct = personal(v.string(), { role: "direct" });

function pathOf(plan: PrivacyPlan, key: string, path: string) {
  const target = plan.targets.get(key);
  assertExists(target, `target ${key} missing`);
  const found = target.paths.find((p) => p.path === path);
  assertExists(
    found,
    `path ${path} missing in ${key} (have ${target.paths
      .map((p) => p.path)
      .join(", ")})`,
  );
  return found;
}

function pathNames(plan: PrivacyPlan, key: string): string[] {
  return plan.targets.get(key)!.paths.map((p) => p.path);
}

function plan(
  collections: Record<string, Record<string, unknown>>,
  posture?: PrivacyPosture,
) {
  return buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: Email },
        ...collections,
      },
    } as unknown as SchemasDefinition,
    posture,
  });
}

test("wrappers: privacy declared outside, inside or around an optional reaches the same leaf", () => {
  const p = plan({
    docs: {
      _id: personal(dbId("doc"), { of: "user" }),
      userId: refId("user"),
      outside: personal(v.optional(v.string()), { role: "direct" }),
      inside: v.optional(Direct),
      piped: v.pipe(
        v.optional(Direct),
        v.check(() => true),
      ),
      stacked: v.nullable(v.optional(Direct)),
    },
  });
  for (const path of ["outside", "inside", "piped", "stacked"]) {
    const found = pathOf(p, "collections/docs/", path);
    assertEquals([found.tier, found.role], ["declared", "direct"], path);
  }
});

test("wrappers: a privacy declaration on an optional container classifies the container, not its children", () => {
  const p = plan({
    profiles: {
      _id: personal(dbId("profile"), { of: "user" }),
      userId: refId("user"),
      address: personal(
        v.optional(v.object({ street: v.string(), city: v.string() })),
        { role: "direct" },
      ),
    },
  });
  assertEquals(pathNames(p, "collections/profiles/"), ["address", "userId"]);
  const address = pathOf(p, "collections/profiles/", "address");
  assertEquals(
    [address.tier, address.role, address.treatment.extract],
    ["declared", "direct", "pseudonym"],
  );
});

test("wrappers: non_optional and every other wrapper is seen through", () => {
  const p = plan({
    profiles: {
      _id: personal(dbId("profile"), { of: "user" }),
      userId: refId("user"),
      required: v.nonOptional(v.optional(v.string())),
    },
  });
  assertEquals(pathOf(p, "collections/profiles/", "required").tier, "unknown");
});

test("containers: rest entries, loose tuples and objects with rest are visible to the plan", () => {
  const p = plan({
    shapes: {
      _id: personal(dbId("shape"), { of: "user" }),
      userId: refId("user"),
      withRest: v.objectWithRest({ known: v.boolean() }, v.string()),
      tail: v.tupleWithRest([v.number()], v.string()),
      loose: v.looseTuple([v.string()]),
    },
  });
  assertEquals(pathNames(p, "collections/shapes/"), [
    "loose.0",
    "tail.*",
    "tail.0",
    "userId",
    "withRest.*",
    "withRest.known",
  ]);
  assertEquals(pathOf(p, "collections/shapes/", "withRest.*").tier, "unknown");
  assertEquals(pathOf(p, "collections/shapes/", "tail.0").tier, "inferred");
});

test("paths: every leaf the walk reaches has a plan path of the same name", () => {
  const fields = {
    _id: personal(dbId("shape"), { of: "user" }),
    userId: refId("user"),
    list: v.array(v.object({ a: v.string(), b: v.optional(v.number()) })),
    pair: v.tuple([v.string(), v.boolean()]),
    byKey: v.record(v.string(), v.object({ n: v.string() })),
    nested: v.optional(
      v.object({ deep: v.nullable(v.object({ s: v.string() })) }),
    ),
    either: v.union([v.object({ x: v.string() }), v.object({ y: v.number() })]),
  };
  const p = plan({ shapes: fields });
  const planned = new Set(pathNames(p, "collections/shapes/"));
  const reached: string[] = [];
  walkDocument(
    fields as never,
    {
      userId: "user:01j5zk3v8n2q4x6y8z0b1c3d5e",
      list: [{ a: "x", b: 1 }, { a: "y" }],
      pair: ["s", true],
      byKey: { k1: { n: "1" }, k2: { n: "2" } },
      nested: { deep: { s: "deep" } },
      either: { y: 3 },
    },
    (leaf) => {
      reached.push(leaf.path);
      return leaf.value;
    },
  );
  assert(reached.length >= 9);
  for (const path of reached) assert(planned.has(path), `${path} not planned`);
});

test("map and set are leaves, as the walk treats them, and are not silently trusted", () => {
  const p = plan({
    shapes: {
      _id: personal(dbId("shape"), { of: "user" }),
      userId: refId("user"),
      labels: v.map(v.string(), Direct),
      ids: v.set(Direct),
    },
  });
  assertEquals(pathNames(p, "collections/shapes/"), [
    "ids",
    "labels",
    "userId",
  ]);
  assertEquals(pathOf(p, "collections/shapes/", "labels").tier, "unknown");
  assertEquals(pathOf(p, "collections/shapes/", "ids").tier, "unknown");
});

test("union: options that disagree at the same path keep the most protective classification", () => {
  const p = plan({
    contacts: {
      _id: personal(dbId("contact"), { of: "user" }),
      userId: refId("user"),
      value: v.variant("kind", [
        v.object({
          kind: v.literal("public"),
          who: notPersonal(v.string(), "company switchboard"),
        }),
        v.object({ kind: v.literal("private"), who: Direct }),
      ]),
    },
  });
  const who = pathOf(p, "collections/contacts/", "value.who");
  assertEquals([who.role, who.treatment.extract], ["direct", "pseudonym"]);
});

test("union: a reference that can also hold another value is a polymorphic reference naming it", () => {
  const p = plan({
    notes: {
      _id: dbId("note"),
      author: v.union([refId("user"), v.string()]),
      contact: v.union([refId("user"), Email]),
      reviewer: v.union([refId("user"), v.null()]),
      editor: v.union([refId("user"), refId("note")]),
    },
  });
  const author = pathOf(p, "collections/notes/", "author");
  assertEquals([author.role, author.treatment.extract], ["reference", "remap"]);
  assertEquals(author.note, "polymorphic: reference|none");
  const contact = pathOf(p, "collections/notes/", "contact");
  assertEquals(contact.note, "polymorphic: reference|direct");
  const reviewer = pathOf(p, "collections/notes/", "reviewer");
  assertEquals(
    [reviewer.role, reviewer.treatment.extract],
    ["reference", "remap"],
  );
  const editor = pathOf(p, "collections/notes/", "editor");
  assertEquals(editor.spaces, ["user", "note"]);
  assertEquals(editor.note, "polymorphic reference");
});

test("reference beats exempt: notPersonal on a refId stays remapped and stops counting as an owner", () => {
  const p = plan({
    articles: {
      _id: dbId("article"),
      author: notPersonal(refId("user"), "public byline"),
      title: v.string(),
    },
  });
  const author = pathOf(p, "collections/articles/", "author");
  assertEquals(
    [author.role, author.relation, author.treatment.extract],
    ["reference", "mention", "remap"],
  );
  assertEquals(p.targets.get("collections/articles/")!.owner.kind, "none");
});

test("record keys that carry personal data are reported, since they are pseudonymised", () => {
  const p = plan({
    registry: {
      _id: personal(dbId("registry"), { of: "user" }),
      userId: refId("user"),
      byEmail: v.record(v.pipe(v.string(), v.email()), v.number()),
      byId: v.record(v.string(), v.number()),
    },
  });
  const findings = p.findings.filter((f) => f.message.includes("record keys"));
  assertEquals(
    findings.map((f) => f.path),
    ["byEmail"],
  );
});

test("person space declared twice is a warning: the second declaration loses its owner", () => {
  const p = buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: Email },
        users_legacy: { _id: personId("user"), email: Email },
      },
    } as unknown as SchemasDefinition,
  });
  assert(
    p.findings.some(
      (f) => f.level === "warning" && f.message.includes("also declared"),
    ),
  );
});

test("dynamic subtree in a document without owner is never kept in clear", () => {
  const p = plan({
    forms: {
      _id: dbId("form"),
      answers: dynamic(v.record(v.string(), v.object({ value: v.unknown() }))),
    },
  });
  const value = pathOf(p, "collections/forms/", "answers.*.value");
  assertEquals([value.tier, value.treatment.extract], ["dynamic", "drop"]);
});

test("gate gap: personal-looking strings without a signal in an ownerless document pass the unknown gate and stay in clear", () => {
  const p = plan({
    programs: {
      _id: dbId("program"),
      speakerName: v.string(),
      speakerPhone: v.string(),
      siret: withIndex(v.string(), { unique: true }),
    },
  });
  assertEquals(p.summary.unknown, 0);
  for (const path of ["speakerName", "speakerPhone", "siret"]) {
    assertEquals(
      pathOf(p, "collections/programs/", path).treatment.extract,
      "keep",
      path,
    );
  }
});

test("gate gap: a picklist of personal values and a precise date stay in clear in a person document", () => {
  const p = plan({
    profiles: {
      _id: personal(dbId("profile"), { of: "user" }),
      userId: refId("user"),
      nationality: v.picklist(["FR", "DE"]),
      birthDate: v.date(),
      birthDay: v.pipe(v.string(), v.isoDate()),
    },
  });
  for (const path of ["nationality", "birthDate", "birthDay"]) {
    const found = pathOf(p, "collections/profiles/", path);
    assertEquals([found.tier, found.treatment.extract], ["inferred", "keep"]);
  }
});

test("report: kept-in-clear section lists what survives unmodified, owner or not", () => {
  const p = plan({
    programs: {
      _id: dbId("program"),
      title: v.string(),
      sessionCount: v.number(),
      owner: mention(refId("user")),
    },
  });
  const report = renderPrivacyReport(p);
  const section = report.slice(report.indexOf("kept in clear on extract"));
  assert(section.includes("collections/programs/"));
  assert(/\n {4}title\s+none/.test(section));
  assert(/\n {4}sessionCount\s+none/.test(section));
  assert(!/\n {4}owner\s/.test(section));
});

test("report: a plan where nothing survives says so", () => {
  const p = plan({});
  assert(
    renderPrivacyReport(p).includes(
      "nothing: every value is remapped, replaced or dropped",
    ),
  );
});

const STRICT_SCHEMAS = {
  collections: {
    users: {
      _id: personId("user"),
      email: Email,
      nickname: v.string(),
      plan: v.picklist(["free", "pro"]),
      age: v.number(),
      phone: personal(v.string(), { role: "contact" }),
      locale: personal(v.picklist(["fr", "en"]), {
        role: "contact",
        treatment: { extract: "keep" },
      }),
    },
    companies: {
      _id: dbId("company"),
      name: v.string(),
      siret: withIndex(v.string(), { unique: true }),
      contactEmail: Email,
      employees: v.number(),
      active: v.boolean(),
      since: v.date(),
      slug: notPersonal(v.string(), "public url"),
      brand: personal(v.string(), {
        role: "content",
        treatment: { extract: "keep" },
      }),
      ownerId: mention(refId("user")),
      notes: v.array(v.object({ text: v.string(), at: v.date() })),
      anything: v.unknown(),
    },
    legal: {
      _id: notPersonal(dbId("legal"), "legal person"),
      denomination: v.string(),
      capital: v.number(),
    },
  },
} as unknown as SchemasDefinition;

function strictPlan() {
  return buildPrivacyPlan({ schemas: STRICT_SCHEMAS, posture: "strict" });
}

function extractOf(p: PrivacyPlan, key: string, path: string) {
  return pathOf(p, key, path).treatment.extract;
}

test("strict posture: the default posture is unchanged", () => {
  const implicit = buildPrivacyPlan({ schemas: STRICT_SCHEMAS });
  const explicit = buildPrivacyPlan({
    schemas: STRICT_SCHEMAS,
    posture: "personal",
  });
  assertEquals(implicit.posture, "personal");
  assertEquals(renderPrivacyReport(implicit), renderPrivacyReport(explicit));
  assertEquals(extractOf(implicit, "collections/companies/", "name"), "keep");
  assertEquals(extractOf(implicit, "collections/users/", "age"), "keep");
  assertEquals(implicit.summary.faked, 0);
});

test("strict posture: undeclared and unknown-typed leaves are faked in every kind of document, honestly tiered", () => {
  const p = strictPlan();
  for (const [key, path] of [
    ["collections/users/", "nickname"],
    ["collections/companies/", "name"],
    ["collections/companies/", "notes.*.text"],
    ["collections/companies/", "anything"],
    ["collections/legal/", "denomination"],
  ] as const) {
    const found = pathOf(p, key, path);
    assertEquals(
      [found.tier, found.treatment.extract, found.byPosture],
      ["inferred", "fake", true],
      `${key}${path}`,
    );
    assert(found.note?.startsWith("strict posture"));
  }
});

test("strict posture: a unique index is pseudonymised, a personal signal is treated without an owner, contact becomes pseudonym", () => {
  const p = strictPlan();
  const siret = pathOf(p, "collections/companies/", "siret");
  assertEquals([siret.role, siret.treatment.extract], ["direct", "pseudonym"]);
  assertEquals(
    extractOf(p, "collections/companies/", "contactEmail"),
    "pseudonym",
  );
  assertEquals(extractOf(p, "collections/users/", "phone"), "pseudonym");
});

test("strict posture: booleans, dates, picklists and references are not touched, numbers only in person-owned documents", () => {
  const p = strictPlan();
  for (const [key, path] of [
    ["collections/companies/", "employees"],
    ["collections/companies/", "active"],
    ["collections/companies/", "since"],
    ["collections/companies/", "notes.*.at"],
    ["collections/legal/", "capital"],
    ["collections/users/", "plan"],
  ] as const) {
    assertEquals(extractOf(p, key, path), "keep", `${key}${path}`);
  }
  assertEquals(extractOf(p, "collections/users/", "age"), "fake");
  const owner = pathOf(p, "collections/companies/", "ownerId");
  assertEquals([owner.role, owner.treatment.extract], ["reference", "remap"]);
});

test("strict posture: an explicit extract treatment, field-level notPersonal and a mirror are honoured", () => {
  const p = strictPlan();
  assertEquals(extractOf(p, "collections/companies/", "slug"), "keep");
  assertEquals(extractOf(p, "collections/companies/", "brand"), "keep");
  assertEquals(extractOf(p, "collections/users/", "locale"), "keep");
  const mirrored = plan(
    {
      logins: {
        _id: personal(dbId("login"), { of: "user" }),
        userId: refId("user"),
        emailLower: mirrorOf(v.string(), "user.email"),
      },
    },
    "strict",
  );
  assertEquals(
    pathOf(mirrored, "collections/logins/", "emailLower").role,
    "derived",
  );
});

test("strict posture: a polymorphic reference stays a remap", () => {
  const p = plan(
    {
      notes: {
        _id: dbId("note"),
        author: v.union([refId("user"), v.string()]),
      },
    },
    "strict",
  );
  assertEquals(extractOf(p, "collections/notes/", "author"), "remap");
});

test("strict posture: unresolved dynamic leaves are faked, a resolver can still classify them", () => {
  const p = plan(
    {
      forms: {
        _id: dbId("form"),
        answers: dynamic(
          v.record(v.string(), v.object({ value: v.unknown() })),
        ),
      },
    },
    "strict",
  );
  const value = pathOf(p, "collections/forms/", "answers.*.value");
  assertEquals(
    [value.treatment.extract, value.dynamicRoot],
    ["fake", "answers"],
  );
});

test("strict posture: unknown no longer blocks, and the lost realism is counted", () => {
  const strict = strictPlan();
  assertEquals(strict.summary.unknown, 0);
  assertEquals(strict.summary.faked, 7);
  assert(buildPrivacyPlan({ schemas: STRICT_SCHEMAS }).summary.none > 0);
});

test("strict posture: the report announces itself and its kept-in-clear section shrinks to what was declared", () => {
  const report = renderPrivacyReport(strictPlan());
  assert(report.startsWith("posture     strict"));
  assert(report.includes("replaced by the strict posture"));
  const section = report.slice(report.indexOf("kept in clear on extract"));
  assert(!/\n {4}name\s/.test(section));
  assert(/\n {4}slug\s/.test(section));
});

test("strict posture: a transformed ownerless document keeps its shape and relations but no real string", () => {
  const p = strictPlan();
  const transformer = createPrivacyTransformer({
    plan: p,
    schemas: STRICT_SCHEMAS,
    secret: "s3cret",
  });
  const doc = {
    _id: "company:01j5zk3v8n2q4x6y8z0b1c3d5e",
    name: "Acme Industrial Holdings",
    siret: "12345678901234",
    contactEmail: "boss@acme.example",
    employees: 42,
    active: true,
    since: new Date("2020-01-01T00:00:00.000Z"),
    slug: "acme",
    brand: "Acme",
    ownerId: "user:01j5zk3v8n2q4x6y8z0b1c3d5f",
    notes: [
      { text: "call back Bob on 0612345678", at: new Date("2021-02-03") },
    ],
    anything: "secret",
  };
  const again = transformer.transform("collections/companies/", doc);
  const out = again.doc;
  assertNotEquals(out.name, doc.name);
  assertNotEquals(out.siret, doc.siret);
  assertNotEquals(out.contactEmail, doc.contactEmail);
  assertNotEquals((out.notes as { text: string }[])[0].text, doc.notes[0].text);
  assertNotEquals(out.anything, doc.anything);
  assertEquals(out.employees, 42);
  assertEquals(out.active, true);
  assertEquals(out.slug, "acme");
  assertEquals(out.brand, "Acme");
  assertNotEquals(out.ownerId, doc.ownerId);
  assert(String(out.ownerId).startsWith("user:"));
  assertEquals(
    again.notes.filter(
      (n) => n.kind === "invalid" || n.kind === "unclassified",
    ),
    [],
  );
  const twice = transformer.transform("collections/companies/", doc).doc;
  assertEquals(twice.siret, out.siret);
});

const SUBSCRIBERS = defineType({
  schema: v.object({
    _id: dbId("subscriber"),
    listId: refId("list"),
    email: Email,
    nickname: v.string(),
  }),
});

const LISTS = defineType({
  schema: v.object({
    _id: dbId("list"),
    title: v.string(),
  }),
  computed: {
    emails: from("subscribers", SUBSCRIBERS)
      .by((s) => s.listId)
      .collect((s) => s.email),
    ids: from("subscribers", SUBSCRIBERS)
      .by((s) => s.listId)
      .collect((s) => s._id),
    size: from("subscribers", SUBSCRIBERS)
      .by((s) => s.listId)
      .count(),
  },
});

function computedPlan(posture?: PrivacyPosture) {
  return buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: Email },
        subscribers: SUBSCRIBERS,
        lists: LISTS,
      },
    } as unknown as SchemasDefinition,
    posture,
  });
}

test("computed: the _computed subtree is derived data, recomputed rather than copied, in documents with or without owner", () => {
  for (const posture of ["personal", "strict"] as const) {
    const p = computedPlan(posture);
    const lists = p.targets.get("collections/lists/");
    assertEquals(lists?.owner.kind, "none");
    const computed = pathOf(p, "collections/lists/", "_computed");
    assertEquals(
      [computed.tier, computed.role, computed.treatment.extract],
      ["declared", "derived", "recompute"],
      posture,
    );
    assertEquals(pathNames(p, "collections/lists/"), ["_computed", "title"]);
  }
});

test("computed: a computed subtree inside an exempt document is still recomputed", () => {
  const p = buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: Email },
        subscribers: SUBSCRIBERS,
        lists: defineType({
          schema: v.object({
            _id: notPersonal(dbId("list"), "not a person"),
            title: v.string(),
          }),
          computed: {
            emails: from("subscribers", SUBSCRIBERS)
              .by((s) => s.listId)
              .collect((s) => s.email),
          },
        }),
      },
    } as unknown as SchemasDefinition,
  });
  assertEquals(
    pathOf(p, "collections/lists/", "_computed").treatment.extract,
    "recompute",
  );
  assertEquals(
    pathOf(p, "collections/lists/", "title").treatment.extract,
    "keep",
  );
});

test("computed: the transformer never copies collected values", () => {
  const p = computedPlan();
  const schemas = {
    collections: {
      users: { _id: personId("user"), email: Email },
      subscribers: SUBSCRIBERS,
      lists: LISTS,
    },
  } as unknown as SchemasDefinition;
  const out = createPrivacyTransformer({
    plan: p,
    schemas,
    secret: "s",
  }).transform("collections/lists/", {
    _id: "list:01j5zk3v8n2q4x6y8z0b1c3d5e",
    title: "Newsletter",
    _computed: {
      emails: ["alice@corp.fr"],
      ids: ["subscriber:01j5zk3v8n2q4x6y8z0b1c3d5f"],
      size: 1,
      _rev: 3,
    },
  });
  assertEquals(out.doc._computed, undefined);
  assert(out.notes.some((n) => n.kind === "recompute_missing"));
});

test("unique composite index: its fields are unique signals like a single unique index", () => {
  const p = buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: Email },
        memberships: defineType({
          schema: v.object({
            _id: personal(dbId("membership"), { of: "user" }),
            userId: refId("user"),
            handle: v.string(),
            label: v.string(),
          }),
          indexes: [{ key: { userId: 1, handle: 1 }, unique: true }],
        }),
      },
    } as unknown as SchemasDefinition,
  });
  const handle = pathOf(p, "collections/memberships/", "handle");
  assertEquals(
    [handle.tier, handle.role, handle.note],
    ["certain", "direct", "unique index"],
  );
  assertEquals(pathOf(p, "collections/memberships/", "label").tier, "unknown");
});

const KEEP_SCHEMAS = {
  collections: {
    users: { _id: personId("user"), email: Email },
    flows: {
      _id: notPersonal(dbId("flow"), "flow configuration", { strict: "keep" }),
      name: v.string(),
      payload: v.unknown(),
      enabled: v.boolean(),
      createdAt: v.date(),
      owner: refId("user"),
      contact: personal(v.string(), { role: "direct" }),
      slugLower: mirrorOf(v.string(), "user.email"),
      steps: dynamic(v.record(v.string(), v.object({ label: v.string() }))),
    },
    audit: {
      _id: notPersonal(dbId("audit"), "audit log"),
      message: v.string(),
    },
  },
} as unknown as SchemasDefinition;

test("notPersonal strict keep: every undeclared leaf of the document is kept under strict, explicit declarations keep their treatment", () => {
  const p = buildPrivacyPlan({ schemas: KEEP_SCHEMAS, posture: "strict" });
  for (const path of [
    "name",
    "payload",
    "enabled",
    "createdAt",
    "steps.*.label",
  ]) {
    const found = pathOf(p, "collections/flows/", path);
    assertEquals(
      [found.treatment.extract, found.byPosture],
      ["keep", undefined],
      path,
    );
  }
  assertEquals(
    pathOf(p, "collections/flows/", "name").note,
    "kept by notPersonal(strict: keep)",
  );
  assertEquals(extractOf(p, "collections/flows/", "owner"), "remap");
  assertEquals(extractOf(p, "collections/flows/", "contact"), "pseudonym");
  assertEquals(pathOf(p, "collections/flows/", "slugLower").role, "derived");
  assertEquals(pathOf(p, "collections/flows/", "steps").role, "dynamic");
  assertEquals(p.targets.get("collections/flows/")!.owner.strictKeep, true);
});

test("notPersonal strict keep: a plain notPersonal still fakes its strings under strict", () => {
  const p = buildPrivacyPlan({ schemas: KEEP_SCHEMAS, posture: "strict" });
  assertEquals(extractOf(p, "collections/audit/", "message"), "fake");
});

test("notPersonal strict keep: the personal posture is unchanged and the report shows the kept paths", () => {
  const personalPlan = buildPrivacyPlan({ schemas: KEEP_SCHEMAS });
  assertEquals(extractOf(personalPlan, "collections/flows/", "name"), "keep");
  const report = renderPrivacyReport(
    buildPrivacyPlan({ schemas: KEEP_SCHEMAS, posture: "strict" }),
  );
  const section = report.slice(report.indexOf("kept in clear on extract"));
  assert(
    /\n {4}name\s+none\s+kept by notPersonal\(strict: keep\)/.test(section),
  );
  assert(report.includes("not personal (flow configuration, strict: keep)"));
});

test("notPersonal strict keep: a certain personal signal keeps its treatment and is reported", () => {
  const p = buildPrivacyPlan({
    schemas: {
      collections: {
        users: { _id: personId("user"), email: Email },
        flows: {
          _id: notPersonal(dbId("flow"), "configuration", { strict: "keep" }),
          notify: v.pipe(v.string(), v.email()),
          code: withIndex(v.string(), { unique: true }),
          label: v.string(),
        },
      },
    } as unknown as SchemasDefinition,
    posture: "strict",
  });
  assertEquals(extractOf(p, "collections/flows/", "notify"), "pseudonym");
  assertEquals(extractOf(p, "collections/flows/", "code"), "keep");
  assertEquals(extractOf(p, "collections/flows/", "label"), "keep");
  assertEquals(
    p.findings.filter((f) => f.level === "warning").map((f) => f.path),
    ["notify"],
  );
});

type Condition =
  | { type: "always" }
  | { type: "equals"; key: string; value: string | number }
  | { type: "and"; conditions: Condition[] }
  | { type: "not"; condition: Condition };

const ConditionSchema: v.GenericSchema<Condition> = v.lazy(() =>
  v.variant("type", [
    v.object({ type: v.literal("always") }),
    v.object({
      type: v.literal("equals"),
      key: v.string(),
      value: v.union([v.string(), v.number()]),
    }),
    v.object({ type: v.literal("and"), conditions: v.array(ConditionSchema) }),
    v.object({ type: v.literal("not"), condition: ConditionSchema }),
  ]),
);

const FLOW_SCHEMAS = {
  collections: {
    users: { _id: personId("user"), email: Email },
    flows: {
      _id: notPersonal(dbId("flow"), "flow configuration", { strict: "keep" }),
      edges: v.array(
        v.object({
          id: v.string(),
          data: v.object({ condition: ConditionSchema }),
        }),
      ),
    },
  },
} as unknown as SchemasDefinition;

const NESTED_CONDITION: Condition = {
  type: "and",
  conditions: [
    { type: "equals", key: "visitor.kind", value: "pro" },
    {
      type: "not",
      condition: { type: "equals", key: "visitor.country", value: "FR" },
    },
    {
      type: "and",
      conditions: [{ type: "equals", key: "deep.key", value: 3 }],
    },
  ],
};

test("lazy: a recursive schema is planned once at the path where it is first entered", () => {
  const p = buildPrivacyPlan({ schemas: FLOW_SCHEMAS, posture: "strict" });
  assertEquals(
    pathNames(p, "collections/flows/").filter((n) => n.includes("condition")),
    [
      "edges.*.data.condition.key",
      "edges.*.data.condition.type",
      "edges.*.data.condition.value",
    ],
  );
});

test("lazy: the walk maps a re-entered recursive schema onto the first expansion, so nothing is unclassified", () => {
  for (const posture of ["strict", "personal"] as const) {
    const p = buildPrivacyPlan({ schemas: FLOW_SCHEMAS, posture });
    const doc = {
      _id: "flow:01j5zk3v8n2q4x6y8z0b1c3d5e",
      edges: [{ id: "e1", data: { condition: NESTED_CONDITION } }],
    };
    const out = createPrivacyTransformer({
      plan: p,
      schemas: FLOW_SCHEMAS,
      secret: "s",
    }).transform("collections/flows/", doc);
    assertEquals(
      out.notes.filter((n) => n.kind === "unclassified"),
      [],
    );
    assertEquals(
      (out.doc.edges as { data: { condition: Condition } }[])[0].data.condition,
      NESTED_CONDITION,
      posture,
    );
  }
});

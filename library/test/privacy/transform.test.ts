import { assert, assertEquals, assertNotEquals } from "@std/assert";
import { decodeTime } from "@std/ulid";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  dynamic,
  isUlid,
  mirrorOf,
  notPersonal,
  personal,
  personId,
  remapId,
  SKIP_DYNAMIC,
  SKIP_RECOMPUTE,
  type TransformNoteKind,
} from "../../src/privacy/mod.ts";

const EmailSchema = personal(v.pipe(v.string(), v.email()), {
  role: "direct",
  consistent: "person",
});
const FirstNameSchema = personal(
  v.pipe(v.string(), v.minLength(2), v.maxLength(30)),
  { role: "direct" },
);
const DateSchema = v.union([v.date(), v.pipe(v.string(), v.isoTimestamp())]);

const SCHEMAS = {
  collections: {
    "+users": {
      _id: personId("user"),
      email: EmailSchema,
      emailLower: mirrorOf(v.string(), "user.email", {
        normalize: "lowercase",
      }),
      firstname: v.optional(FirstNameSchema),
      status: v.picklist(["active", "banned"]),
      statusReason: v.optional(v.string()),
      passwordHash: personal(v.string(), {
        role: "technical",
        treatment: { extract: "opaque" },
      }),
      recoveryHint: personal(v.optional(v.string()), {
        role: "technical",
        treatment: { extract: "opaque" },
      }),
      createdAt: DateSchema,
      searchTokens: personal(v.array(v.string()), { role: "derived" }),
      tags: notPersonal(v.array(v.string()), "admin vocabulary"),
      bio: personal(v.optional(v.string()), { role: "content" }),
    },
  },
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: {
        accountless_identity: {
          _id: personId("accountless_identity"),
          email: EmailSchema,
          firstname: v.optional(FirstNameSchema),
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
          fields: dynamic(v.record(
            v.string(),
            v.object({
              t: v.string(),
              v: v.unknown(),
              o: v.optional(v.picklist(["flow", "dashboard"])),
            }),
          )),
          birthday: personal(DateSchema, { role: "quasi" }),
          invitedBy: v.optional(refId("participant")),
        },
        scan_history: {
          _id: personal(dbId("scan_history"), { of: "participant" }),
          participantId: refId("participant"),
          scannedBy: v.nullable(refId("user")),
          label: v.picklist(["security", "business"]),
          at: DateSchema,
        },
      },
    },
  },
};

const PLAN = buildPrivacyPlan({ schemas: SCHEMAS });
const USERS = "collections/+users/";
const ACCOUNTLESS = "scopedMultiCollections/+expositions/accountless_identity";
const PARTICIPANT = "scopedMultiCollections/+expositions/participant";
const SCAN = "scopedMultiCollections/+expositions/scan_history";

const USER_ID = "user:01j5zk3v8n2q4x6y8z0b1c3d5e";
const USER_ID_LATER = "user:01j5zk3vt6bfqh0aer9a9s1nmc";
const PARTICIPANT_ID = "participant:01j5zk4a1b2c3d4e5f6g7h8j9k";

function user(overrides: Record<string, unknown> = {}) {
  return {
    _id: USER_ID,
    email: "Alice@Corp.fr",
    emailLower: "alice@corp.fr",
    firstname: "Alice",
    status: "active",
    statusReason: "flagged by support",
    passwordHash: "$argon2id$v=19$m=65536,t=3,p=4$abc$def",
    createdAt: new Date("2026-03-10T09:15:00.000Z"),
    searchTokens: ["ali", "alice", "corp"],
    tags: ["vip"],
    bio: "Alice runs the Corp booth every year",
    ...overrides,
  };
}

function transformer(overrides: Record<string, unknown> = {}) {
  return createPrivacyTransformer({
    plan: PLAN,
    schemas: SCHEMAS,
    secret: "s3cret",
    ...overrides,
  });
}

function kinds(
  notes: readonly { kind: TransformNoteKind; path: string }[],
  kind: TransformNoteKind,
) {
  return notes.filter((n) => n.kind === kind).map((n) => n.path);
}

Deno.test("pseudonym: the same value gives the same fake value across collections, and never the original", () => {
  const t = transformer();
  const a = t.transform(USERS, user()).doc;
  const b = t.transform(ACCOUNTLESS, {
    _id: "accountless_identity:01j5zk5a1b2c3d4e5f6g7h8j9k",
    _scope: "exposition:E",
    email: " alice@corp.fr ",
  }).doc;
  assertEquals(a.email, b.email);
  assertNotEquals(a.email, "Alice@Corp.fr");
  assert(v.safeParse(v.pipe(v.string(), v.email()), a.email).success);
  const other = transformer({ secret: "another" }).transform(USERS, user()).doc;
  assertNotEquals(other.email, a.email);
});

Deno.test("pseudonym: deterministic across runs", () => {
  const first = transformer().transform(USERS, user());
  const second = transformer().transform(USERS, user());
  assertEquals(first.doc, second.doc);
  assertEquals(first.notes, second.notes);
});

Deno.test("consistency: relationship separates scopes, person joins them, and a space can pin its own policy", () => {
  const doc = (scope: string) => ({
    _id: "accountless_identity:01j5zk5a1b2c3d4e5f6g7h8j9k",
    _scope: scope,
    email: "bob@corp.fr",
    firstname: "Bob",
  });
  const relationship = transformer({ consistency: "relationship" });
  const a = relationship.transform(ACCOUNTLESS, doc("exposition:A")).doc;
  const b = relationship.transform(ACCOUNTLESS, doc("exposition:B")).doc;
  assertNotEquals(a.firstname, b.firstname);
  assertEquals(a.email, b.email);
  const person = transformer({ consistency: "person" });
  assertEquals(
    person.transform(ACCOUNTLESS, doc("exposition:A")).doc.firstname,
    person.transform(ACCOUNTLESS, doc("exposition:B")).doc.firstname,
  );
});

Deno.test("ids: prefix kept, ulid shape kept, order and intervals kept, absolute time shifted", () => {
  const shift = 7 * 24 * 3600 * 1000;
  const a = remapId("s3cret", USER_ID, shift);
  const b = remapId("s3cret", USER_ID_LATER, shift);
  assert(a.startsWith("user:") && b.startsWith("user:"));
  assertNotEquals(a, USER_ID);
  assertEquals(a, a.toLowerCase());
  assert(isUlid(a.slice(5)));
  const inA = decodeTime(USER_ID.slice(5).toUpperCase());
  const inB = decodeTime(USER_ID_LATER.slice(5).toUpperCase());
  assertEquals(decodeTime(a.slice(5).toUpperCase()), inA + shift);
  assertEquals(
    decodeTime(b.slice(5).toUpperCase()) - decodeTime(a.slice(5).toUpperCase()),
    inB - inA,
  );
  assertEquals(remapId("s3cret", USER_ID, shift), a);
  assertEquals(
    remapId("s3cret", "session:9f86d081884c7d659a2feaa0c55ad015"),
    remapId("s3cret", "session:9f86d081884c7d659a2feaa0c55ad015"),
  );
  assertNotEquals(
    remapId("s3cret", "session:9f86d081884c7d659a2feaa0c55ad015"),
    "session:9f86d081884c7d659a2feaa0c55ad015",
  );
});

Deno.test("references: a remapped reference equals the remapped _id it points to", () => {
  const t = transformer();
  const userOut = t.transform(USERS, user()).doc;
  const participantOut = t.transform(PARTICIPANT, {
    _id: PARTICIPANT_ID,
    _scope: "exposition:E",
    personRef: { kind: "user", userId: USER_ID },
    fields: { company: { t: "text", v: "Corp" } },
    birthday: new Date("1990-06-17T00:00:00.000Z"),
  }).doc;
  assertEquals(
    (participantOut.personRef as Record<string, unknown>).userId,
    userOut._id,
  );
  assertEquals(
    (participantOut.personRef as Record<string, unknown>).kind,
    "user",
  );
  const scanOut = t.transform(SCAN, {
    _id: "scan_history:01j5zk6a1b2c3d4e5f6g7h8j9k",
    _scope: "exposition:E",
    participantId: PARTICIPANT_ID,
    scannedBy: null,
    label: "business",
    at: "2026-03-11T10:00:00.000Z",
  }).doc;
  assertEquals(scanOut.participantId, participantOut._id);
  assertEquals(scanOut._scope, participantOut._scope);
  assertEquals(scanOut.scannedBy, null);
  assertEquals(scanOut.label, "business");
});

Deno.test("fail-closed: unknown keys and unknown-tier values never reach the output", () => {
  const t = transformer();
  const { doc, notes } = t.transform(
    USERS,
    user({ legacyOwner: "user:01j5zk3v8n2q4x6y8z0b1c3d5e" }),
  );
  assertEquals(doc.legacyOwner, undefined);
  assertEquals(kinds(notes, "unknown_key"), ["legacyOwner"]);
  assertEquals(doc.statusReason, undefined);
  assert(kinds(notes, "dropped").includes("statusReason"));
  assertEquals(kinds(notes, "invalid"), []);
});

Deno.test("fail-closed: a required unknown value is replaced by a valid generated one, and said so", () => {
  const t = transformer();
  const { doc, notes } = t.transform(PARTICIPANT, {
    _id: PARTICIPANT_ID,
    _scope: "exposition:E",
    personRef: { kind: "user", userId: USER_ID },
    fields: { company: { t: "text", v: "Corp" } },
    birthday: new Date("1990-06-17T00:00:00.000Z"),
  });
  const company =
    (doc.fields as Record<string, Record<string, unknown>>).company;
  assertNotEquals(company.t, "text");
  assert(kinds(notes, "generated_required").includes("fields.*.t"));
  assert(kinds(notes, "unresolved").includes("fields.*.t"));
  assertEquals(kinds(notes, "invalid"), []);
});

Deno.test("mirror: the lowercased copy equals the lowercased pseudonym of its source", () => {
  const { doc } = transformer().transform(USERS, user());
  assertEquals(doc.emailLower, String(doc.email).toLowerCase());
});

Deno.test("opaque: removed when optional, regenerated when required", () => {
  const { doc, notes } = transformer().transform(
    USERS,
    user({ recoveryHint: "sha256:abc" }),
  );
  assertEquals(doc.recoveryHint, undefined);
  assertNotEquals(doc.passwordHash, "$argon2id$v=19$m=65536,t=3,p=4$abc$def");
  assertEquals(typeof doc.passwordHash, "string");
  assertEquals(kinds(notes, "opaque").sort(), ["passwordHash", "recoveryHint"]);
});

Deno.test("derived: recomputed by the consumer hook, dropped and reported without one", () => {
  const withHook = transformer({
    recompute: (ctx: { path: string; doc: Record<string, unknown> }) =>
      ctx.path === "searchTokens" ? ["recomputed"] : SKIP_RECOMPUTE,
  }).transform(USERS, user());
  assertEquals(withHook.doc.searchTokens, ["recomputed"]);

  const withoutHook = transformer().transform(USERS, user());
  assertNotEquals(withoutHook.doc.searchTokens, ["ali", "alice", "corp"]);
  assert(
    kinds(withoutHook.notes, "recompute_missing").includes("searchTokens"),
  );
});

Deno.test("keep, exempt, content and dates", () => {
  const shift = 3 * 24 * 3600 * 1000;
  const { doc, notes } = transformer({ timeShiftMs: shift }).transform(
    USERS,
    user(),
  );
  assertEquals(doc.status, "active");
  assertEquals(doc.tags, ["vip"]);
  assertNotEquals(doc.bio, "Alice runs the Corp booth every year");
  assertEquals(typeof doc.bio, "string");
  assertEquals(
    (doc.createdAt as Date).getTime(),
    new Date("2026-03-10T09:15:00.000Z").getTime() + shift,
  );
  assertEquals(kinds(notes, "invalid"), []);
});

Deno.test("generalise: a quasi-identifying date collapses to its month after the shift", () => {
  const shift = 40 * 24 * 3600 * 1000;
  const { doc } = transformer({ timeShiftMs: shift }).transform(PARTICIPANT, {
    _id: PARTICIPANT_ID,
    _scope: "exposition:E",
    personRef: {
      kind: "accountless",
      accountlessIdentityId: "accountless_identity:01j5zk5a1b2c3d4e5f6g7h8j9k",
    },
    fields: {},
    birthday: "1990-06-17T00:00:00.000Z",
  });
  assertEquals(doc.birthday, "1990-07-01T00:00:00.000Z");
});

Deno.test("validation: every transformed document still satisfies its schema", () => {
  const t = transformer({ timeShiftMs: 1000 });
  const results = [
    t.transform(USERS, user()),
    t.transform(ACCOUNTLESS, {
      _id: "accountless_identity:01j5zk5a1b2c3d4e5f6g7h8j9k",
      _scope: "exposition:E",
      email: "x@y.io",
    }),
    t.transform(SCAN, {
      _id: "scan_history:01j5zk6a1b2c3d4e5f6g7h8j9k",
      _scope: "exposition:E",
      participantId: PARTICIPANT_ID,
      scannedBy: USER_ID,
      label: "security",
      at: new Date(),
    }),
  ];
  for (const r of results) assertEquals(kinds(r.notes, "invalid"), []);
});

Deno.test("input: a document that violates its own schema is reported before anything is transformed", () => {
  const { notes } = transformer().transform(
    USERS,
    user({ status: "unknown-status" }),
  );
  assert(kinds(notes, "input_invalid").includes("status"));
});

Deno.test("dynamic: the resolver classifies each entry from its data, and joins values across collections", () => {
  const resolveDynamic = (unit: { value: unknown }) => {
    const t = (unit.value as { t: string }).t;
    if (t === "email") {
      return {
        t: { role: "technical" as const },
        v: {
          role: "direct" as const,
          consistent: "person" as const,
          schema: EmailSchema,
        },
      };
    }
    if (t === "text") {
      return {
        t: { role: "technical" as const },
        v: { role: "content" as const, schema: v.string() },
      };
    }
    return SKIP_DYNAMIC;
  };
  const t = transformer({ resolveDynamic });
  const userOut = t.transform(USERS, user()).doc;
  const { doc, notes } = t.transform(PARTICIPANT, {
    _id: PARTICIPANT_ID,
    _scope: "exposition:E",
    personRef: { kind: "user", userId: USER_ID },
    fields: {
      email: { t: "email", v: "alice@corp.fr", o: "flow" },
      bio: { t: "text", v: "Runs the booth", o: "dashboard" },
      agree: { t: "checkbox", v: true },
    },
    birthday: new Date("1990-06-17T00:00:00.000Z"),
  });
  const fields = doc.fields as Record<string, Record<string, unknown>>;
  assertEquals(fields.email.v, userOut.email);
  assertEquals(fields.email.t, "email");
  assertEquals(fields.email.o, "flow");
  assertEquals(fields.bio.t, "text");
  assertNotEquals(fields.bio.v, "Runs the booth");
  assertEquals(typeof fields.bio.v, "string");
  assertNotEquals(fields.agree.t, "checkbox");
  assert(kinds(notes, "unresolved").includes("fields.*.t"));
  assertEquals(kinds(notes, "invalid"), []);
});

Deno.test("pseudonym: a field keeps the realism its key gives to generation", () => {
  const { doc } = transformer().transform(USERS, user({ firstname: "Alice" }));
  assert(/^[A-Za-z'. -]+$/.test(String(doc.firstname)), String(doc.firstname));
  assertNotEquals(doc.firstname, "Alice");
});

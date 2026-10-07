import { ObjectId } from "mongodb";
import { test } from "../+harness.ts";
import { assert, assertEquals, assertNotEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { withIndex } from "../../src/indexes.ts";
import { dbId, refId } from "../../src/ids.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  defaultTimeShiftMs,
  dynamic,
  type DynamicResolution,
  isUlid,
  mirrorOf,
  personal,
  personId,
  remapId,
  walkDocument,
} from "../../src/privacy/mod.ts";

const USER_ID = "user:01j5zk3v8n2q4x6y8z0b1c3d5e";
const TARGET = "collections/+users/";

function transformOne(
  fields: Record<string, unknown>,
  doc: Record<string, unknown>,
  options: Record<string, unknown> = {},
) {
  const schemas = {
    collections: { "+users": { _id: personId("user"), ...fields } },
  } as never;
  const plan = buildPrivacyPlan({ schemas });
  const transformer = createPrivacyTransformer({
    plan,
    schemas,
    secret: "s3cret",
    ...options,
  });
  return transformer.transform(TARGET, { _id: USER_ID, ...doc });
}

test("walk: nullable is not optional, nullish is both", () => {
  const seen: Record<string, [boolean, boolean]> = {};
  const marked = personal(v.string(), { role: "direct" });
  walkDocument(
    {
      a: v.nullable(marked),
      b: v.optional(marked),
      c: v.nullish(marked),
      d: v.nonOptional(v.optional(marked)),
      e: marked,
    },
    { a: "x", b: "x", c: "x", d: "x", e: "x" },
    (leaf) => {
      seen[leaf.path] = [leaf.optional, leaf.nullable];
      return leaf.value;
    },
  );
  assertEquals(seen, {
    a: [false, true],
    b: [true, false],
    c: [true, true],
    d: [false, false],
    e: [false, false],
  });
});

test("transform: a required nullable field stays present as null when its value is removed", () => {
  const { doc, notes } = transformOne(
    {
      passwordHash: v.nullable(
        personal(v.string(), {
          role: "technical",
          treatment: { extract: "opaque" },
        }),
      ),
      reason: personal(v.nullable(v.string()), { role: "sensitive" }),
    },
    { passwordHash: "$argon2id$secret", reason: "fraud suspicion" },
  );
  assertEquals(doc.passwordHash, null);
  assertEquals(doc.reason, null);
  assertEquals(
    notes.filter((n) => n.kind === "invalid"),
    [],
  );
});

test("walk: array item keys follow the input position when earlier items are dropped", () => {
  const units: string[] = [];
  const schemas = {
    collections: {
      "+users": {
        _id: personId("user"),
        entries: dynamic(v.array(v.optional(v.string()))),
      },
    },
  } as never;
  const plan = buildPrivacyPlan({ schemas });
  const resolution = (role: "sensitive" | "technical"): DynamicResolution => ({
    "": { role },
  });
  const transformer = createPrivacyTransformer({
    plan,
    schemas,
    secret: "s3cret",
    resolveDynamic: (unit) => {
      units.push(`${unit.keys.join(".")}=${unit.value}`);
      return resolution(unit.value === "secret" ? "sensitive" : "technical");
    },
  });
  const { doc } = transformer.transform(TARGET, {
    _id: USER_ID,
    entries: ["secret", "open", "other"],
  });
  assertEquals(units, [
    "entries.0=secret",
    "entries.1=open",
    "entries.2=other",
  ]);
  assertEquals(doc.entries, ["open", "other"]);
});

test("walk: an intersect of objects is walked field by field", () => {
  const { doc, notes } = transformOne(
    {
      contact: v.intersect([
        v.object({
          email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
        }),
        v.object({ kind: v.picklist(["work", "home"]) }),
      ]),
    },
    { contact: { email: "alice@corp.fr", kind: "home" } },
  );
  const contact = doc.contact as { email: string; kind: string };
  assertEquals(contact.kind, "home");
  assertNotEquals(contact.email, "alice@corp.fr");
  assert(v.is(v.pipe(v.string(), v.email()), contact.email));
  assertEquals(
    notes.filter((n) => n.kind === "unclassified"),
    [],
  );
});

test("walk: a reference inside an intersect is remapped", () => {
  const { doc } = transformOne(
    {
      link: v.intersect([
        v.object({ owner: refId("user") }),
        v.object({ note: v.string() }),
      ]),
    },
    { link: { owner: USER_ID, note: "x" } },
  );
  const link = doc.link as { owner: string };
  assertEquals(link.owner, doc._id);
});

const ULID_ID = "user:01j5zk3v8n2q4x6y8z0b1c3d5e";

test("ids: a fractional or oversized time shift still yields a well-formed ulid", () => {
  for (const shift of [0.7 * 86_400_000, -1e15, 1e15]) {
    const mapped = remapId("s3cret", ULID_ID, shift).split(":")[1];
    assert(isUlid(mapped), `shift ${shift} gave "${mapped}"`);
    assertEquals(mapped.length, 26);
  }
});

test("ids: the same ulid written in upper or lower case maps to the same id", () => {
  const lower = remapId("s3cret", ULID_ID);
  const upper = remapId(
    "s3cret",
    ULID_ID.toUpperCase().replace("USER", "user"),
  );
  assertEquals(
    upper,
    lower.replace(/:(.*)$/, (_, uid) => `:${uid.toUpperCase()}`),
  );
});

const MIRROR_SCHEMAS = {
  collections: {
    "+users": {
      _id: personId("user"),
      email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
    },
  },
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: {
        participant: {
          _id: personId("participant", { of: ["user"] }),
          userId: refId("user"),
          emailCopy: mirrorOf(v.string(), "user.email"),
        },
      },
    },
  },
} as never;

test("mirror: a copy in a scoped document equals the pseudonym of its unscoped source under the default policy", () => {
  const plan = buildPrivacyPlan({ schemas: MIRROR_SCHEMAS });
  const transformer = createPrivacyTransformer({
    plan,
    schemas: MIRROR_SCHEMAS,
    secret: "s3cret",
  });
  const source = transformer.transform(TARGET, {
    _id: USER_ID,
    email: "alice@corp.fr",
  });
  for (const exposition of [
    "exposition:01j5zk3v8n2q4x6y8z0b1c3d5f",
    "exposition:01j5zk3v8n2q4x6y8z0b1c3d60",
  ]) {
    const copy = transformer.transform(
      "scopedMultiCollections/+expositions/participant",
      {
        _id: "participant:01j5zk3v8n2q4x6y8z0b1c3d5e",
        _scope: exposition,
        userId: USER_ID,
        emailCopy: "alice@corp.fr",
      },
    );
    assertEquals(copy.doc.emailCopy, source.doc.email);
  }
});

test("mirror: a copy next to a scoped source follows the scope of its own document", () => {
  const schemas = {
    scopedMultiCollections: {
      "+expositions": {
        scope: refId("exposition"),
        types: {
          participant: {
            _id: personId("participant"),
            email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
          },
          badge: {
            _id: personal(dbId("badge"), { of: "participant" }),
            participantId: refId("participant"),
            emailCopy: mirrorOf(v.string(), "participant.email"),
          },
        },
      },
    },
  } as never;
  const plan = buildPrivacyPlan({ schemas });
  const transformer = createPrivacyTransformer({
    plan,
    schemas,
    secret: "s3cret",
  });
  const fakeEmails = [
    "exposition:01j5zk3v8n2q4x6y8z0b1c3d5f",
    "exposition:01j5zk3v8n2q4x6y8z0b1c3d60",
  ].map((_scope) => {
    const source = transformer.transform(
      "scopedMultiCollections/+expositions/participant",
      {
        _id: "participant:01j5zk3v8n2q4x6y8z0b1c3d5e",
        _scope,
        email: "alice@corp.fr",
      },
    );
    const copy = transformer.transform(
      "scopedMultiCollections/+expositions/badge",
      {
        _id: "badge:01j5zk3v8n2q4x6y8z0b1c3d5e",
        _scope,
        participantId: "participant:01j5zk3v8n2q4x6y8z0b1c3d5e",
        emailCopy: "alice@corp.fr",
      },
    );
    assertEquals(copy.doc.emailCopy, source.doc.email);
    return source.doc.email;
  });
  assertNotEquals(fakeEmails[0], fakeEmails[1]);
});

test("dates: date-only strings follow the time shift and collapse to the first of the month when quasi-identifying", () => {
  const { doc, notes } = transformOne(
    {
      day: v.pipe(v.string(), v.isoDate()),
      birth: personal(v.pipe(v.string(), v.isoDate()), { role: "quasi" }),
    },
    { day: "2026-03-10", birth: "1990-07-23" },
    { timeShiftMs: 30 * 86_400_000 },
  );
  assertEquals(doc.day, "2026-04-09");
  assertEquals(doc.birth, "1990-08-01");
  assertEquals(
    notes.filter((n) => n.kind === "invalid" || n.kind === "dropped"),
    [],
  );
});

const CODE = v.pipe(v.string(), v.regex(/^[a-z]{2}$/));
const REAL_CODES = Array.from(
  { length: 200 },
  (_, i) =>
    `${String.fromCharCode(97 + (i % 26))}${String.fromCharCode(97 + Math.floor(i / 26))}`,
);

function codeSchemas(unique: boolean) {
  const field = personal(CODE, { role: "direct" });
  return {
    collections: {
      "+users": {
        _id: personId("user"),
        code: unique ? withIndex(field, { unique: true }) : field,
        codeCopy: mirrorOf(v.string(), "user.code"),
      },
    },
  } as never;
}

function pseudonymiseCodes(unique: boolean) {
  const schemas = codeSchemas(unique);
  const transformer = createPrivacyTransformer({
    plan: buildPrivacyPlan({ schemas }),
    schemas,
    secret: "s3cret",
  });
  return REAL_CODES.map((code, i) => {
    const { doc } = transformer.transform(TARGET, {
      _id: `user:01j5zk3v8n2q4x6y8z0b1c3d${String(i).padStart(2, "0")}`,
      code,
      codeCopy: code,
    });
    return doc as { code: string; codeCopy: string };
  });
}

test("collisions: distinct real values stay distinct on a unique-indexed pseudonym field", () => {
  const withoutIndex = pseudonymiseCodes(false).map((d) => d.code);
  assert(
    new Set(withoutIndex).size < REAL_CODES.length,
    "control must collide",
  );

  const docs = pseudonymiseCodes(true);
  const fakes = docs.map((d) => d.code);
  assertEquals(new Set(fakes).size, REAL_CODES.length);
  assert(fakes.every((code) => v.is(CODE, code)));
  assertEquals(
    docs.map((d) => d.codeCopy),
    fakes,
  );
  assertEquals(
    pseudonymiseCodes(true).map((d) => d.code),
    fakes,
  );
});

test("collisions: an exhausted output space is reported instead of looping", () => {
  const schemas = {
    collections: {
      "+users": {
        _id: personId("user"),
        tier: withIndex(personal(v.picklist(["a", "b"]), { role: "direct" }), {
          unique: true,
        }),
      },
    },
  } as never;
  const transformer = createPrivacyTransformer({
    plan: buildPrivacyPlan({ schemas }),
    schemas,
    secret: "s3cret",
  });
  const kinds = ["x", "y", "z"].flatMap((tier) =>
    transformer
      .transform(TARGET, { _id: USER_ID, tier })
      .notes.map((n) => n.kind),
  );
  assert(kinds.includes("collision"));
});

test("dates: zone-less and offset timestamps keep their shape under the shift", () => {
  const { doc } = transformOne(
    {
      a: personal(v.string(), { role: "technical" }),
      b: personal(v.string(), { role: "technical" }),
      c: personal(v.string(), { role: "technical" }),
    },
    {
      a: "2026-03-10T09:15",
      b: "2026-03-10T09:15:30.250",
      c: "2026-03-10T09:15:30+02:00",
    },
    { timeShiftMs: 86_400_000, validate: false },
  );
  assertEquals(doc.a, "2026-03-11T09:15");
  assertEquals(doc.b, "2026-03-11T09:15:30.250");
  assertEquals(doc.c, "2026-03-11T09:15:30+02:00");
});

test("keep: a value that does not fit its technical schema is faked and reported", () => {
  const { doc, notes } = transformOne(
    { count: v.number(), flag: v.optional(v.boolean()) },
    { count: "+33 6 12 34 56 78", flag: "zebulon" },
  );
  assertEquals(typeof doc.count, "number");
  assertEquals(doc.flag, undefined);
  assertEquals(
    notes.filter((n) => n.kind === "mismatch").map((n) => n.path),
    ["count", "flag"],
  );
});

test("remap: a reference holding a non-id is faked and reported, a union option that is not an id is pseudonymised silently", () => {
  const email = v.pipe(v.string(), v.email());
  const { doc, notes } = transformOne(
    {
      manager: refId("user"),
      assignee: v.union([refId("user"), email]),
    },
    { manager: "zebulon@acme.fr", assignee: "zebulon@acme.fr" },
  );
  assert(v.is(refId("user"), doc.manager));
  assert(v.is(email, doc.assignee));
  assertNotEquals(doc.assignee, "zebulon@acme.fr");
  assertEquals(
    notes.filter((n) => n.kind === "mismatch").map((n) => n.path),
    ["manager"],
  );
});

test("record keys: ids are remapped, vocabulary kept, anything else pseudonymised without collision", () => {
  const { doc } = transformOne(
    {
      seenBy: v.record(refId("user"), v.boolean()),
      perRole: v.record(v.picklist(["admin", "guest"]), v.number()),
      byContact: v.record(v.pipe(v.string(), v.email()), v.number()),
    },
    {
      seenBy: { [USER_ID]: true },
      perRole: { admin: 1, guest: 2 },
      byContact: { "a@corp.fr": 1, "b@corp.fr": 2, "A@corp.fr ": 3 },
    },
  );
  assertEquals(Object.keys(doc.seenBy as object), [doc._id]);
  assertEquals(doc.perRole, { admin: 1, guest: 2 });
  const keys = Object.keys(doc.byContact as object);
  assertEquals(keys.length, 3);
  assert(keys.every((key) => v.is(v.pipe(v.string(), v.email()), key)));
  assert(!keys.some((key) => key.toLowerCase().includes("corp.fr")));
});

test("ids: an ObjectId _id is remapped with its timestamp shifted, any other object _id is dropped", () => {
  const original = new ObjectId("665f1c2a9b3e4d5a6b7c8d9e");
  const { doc } = transformOne({}, {}, { timeShiftMs: -86_400_000 });
  assertEquals(typeof doc._id, "string");
  const schemas = {
    collections: {
      "+users": { _id: personId("user") },
      sessions: { device: v.string() },
    },
  } as never;
  const transformer = createPrivacyTransformer({
    plan: buildPrivacyPlan({ schemas }),
    schemas,
    secret: "s3cret",
    timeShiftMs: -86_400_000,
  });
  const remapped = transformer.transform("collections/sessions/", {
    _id: original,
    device: "web",
  }).doc._id as ObjectId;
  assert(remapped instanceof ObjectId);
  assertNotEquals(remapped.toHexString(), original.toHexString());
  assertEquals(
    original.getTimestamp().getTime() - remapped.getTimestamp().getTime(),
    86_400_000,
  );
  const hostile = transformer.transform("collections/sessions/", {
    _id: { email: "zebulon@acme.fr" },
    device: "web",
  });
  assertEquals(hostile.doc._id, undefined);
  assert(hostile.notes.some((n) => n.kind === "dropped" && n.path === "_id"));
});

test("shift: a strict plan shifts time by a secret-derived default, an explicit zero wins", () => {
  const schemas = {
    collections: {
      "+users": {
        _id: personId("user"),
        at: v.pipe(v.string(), v.isoTimestamp()),
      },
    },
  } as never;
  const plan = buildPrivacyPlan({ schemas, posture: "strict" });
  const shifted = (timeShiftMs?: number) =>
    createPrivacyTransformer({
      plan,
      schemas,
      secret: "s3cret",
      ...(timeShiftMs !== undefined && { timeShiftMs }),
    }).transform(TARGET, { _id: USER_ID, at: "2026-03-10T09:00:00.000Z" }).doc
      .at;
  const shift = defaultTimeShiftMs("s3cret");
  assert(shift <= -30 * 86_400_000 && shift > -366 * 86_400_000);
  assertEquals(shift, defaultTimeShiftMs("s3cret"));
  assertEquals(
    shifted(),
    new Date(Date.parse("2026-03-10T09:00:00.000Z") + shift).toISOString(),
  );
  assertEquals(shifted(0), "2026-03-10T09:00:00.000Z");
});

test("walk: rest keys and tuple rest items are walked under the * path the plan uses", () => {
  const { doc, notes } = transformOne(
    {
      extra: v.objectWithRest({ kind: v.picklist(["a", "b"]) }, v.string()),
      pair: v.tupleWithRest([v.number()], v.string()),
    },
    { extra: { kind: "a", city: "Ornans" }, pair: [1, "Ornans", "Loue"] },
  );
  assertEquals(
    notes.filter((n) => n.kind === "unclassified").length,
    0,
    JSON.stringify(notes),
  );
  assert(!JSON.stringify(doc).includes("Ornans"));
});

test("shift: the transformer exposes its effective shift and remaps ids with it", () => {
  const schemas = {
    collections: { "+users": { _id: personId("user") } },
  } as never;
  const plan = buildPrivacyPlan({ schemas, posture: "strict" });
  const transformer = createPrivacyTransformer({
    plan,
    schemas,
    secret: "s3cret",
  });
  assertEquals(transformer.timeShiftMs, defaultTimeShiftMs("s3cret"));
  assertEquals(
    transformer.remapId(USER_ID),
    transformer.transform(TARGET, { _id: USER_ID }).doc._id,
  );
});

test("remap: a reference whose prefix is not a declared space is faked, not kept", () => {
  const { doc, notes } = transformOne(
    { manager: refId("user") },
    { manager: "quixotique:42" },
  );
  assert(v.is(refId("user"), doc.manager));
  assertEquals(String(doc.manager).includes("quixotique"), false);
  assertEquals(
    notes.filter((n) => n.kind === "mismatch").map((n) => n.path),
    ["manager"],
  );
});

test("strict: a number whose key name looks personal is faked, a counter is kept", () => {
  const schemas = {
    collections: {
      "+users": { _id: personId("user") },
      companies: {
        _id: dbId("company"),
        phone: v.number(),
        capacity: v.number(),
      },
    },
  } as never;
  const plan = buildPrivacyPlan({ schemas, posture: "strict" });
  const { doc } = createPrivacyTransformer({
    plan,
    schemas,
    secret: "s3cret",
  }).transform("collections/companies/", {
    _id: "company:01j5zk3v8n2q4x6y8z0b1c3d5e",
    phone: 33612345678,
    capacity: 250,
  });
  assertNotEquals(doc.phone, 33612345678);
  assertEquals(doc.capacity, 250);
});

test("ids: a numeric _id is remapped injectively under the strict posture and noted", () => {
  const schemas = {
    collections: {
      "+users": { _id: personId("user") },
      cards: { _id: v.number(), label: v.string() },
    },
  } as never;
  const transformer = createPrivacyTransformer({
    plan: buildPrivacyPlan({ schemas, posture: "strict" }),
    schemas,
    secret: "s3cret",
  });
  const ids = [33612345678, 33612345679, 7].map((_id) =>
    transformer.transform("collections/cards/", { _id, label: "x" }),
  );
  const mapped = ids.map((r) => r.doc._id as number);
  assertEquals(new Set(mapped).size, 3);
  assertEquals(
    mapped.map((n) => String(n).length),
    [11, 11, 1],
  );
  assert(!mapped.includes(33612345678));
  assert(ids.every((r) => r.notes.some((n) => n.kind === "numeric_id")));
});

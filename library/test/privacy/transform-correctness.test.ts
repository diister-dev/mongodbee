import { test } from "../+harness.ts";
import { assert, assertEquals, assertNotEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
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

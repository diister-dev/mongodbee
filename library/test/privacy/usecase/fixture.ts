import { dbId, refId } from "../../../src/ids.ts";
import { withIndex } from "../../../src/indexes.ts";
import {
  type DynamicResolution,
  type DynamicUnit,
  dynamic,
  notPersonal,
  PRIVACY_SYMBOL,
  personal,
  personId,
  SKIP_DYNAMIC,
} from "../../../src/privacy/mod.ts";
import * as v from "../../../src/schema.ts";

type Schema = v.GenericSchema;

const EmailSchema = v.pipe(
  v.string(),
  v.trim(),
  v.toLowerCase(),
  v.email(),
  v.maxLength(254),
);
const NameSchema = v.pipe(
  v.string(),
  v.trim(),
  v.minLength(1),
  v.maxLength(128),
);

const personEmail = <T extends Schema>(schema: T) =>
  personal(schema, { role: "direct", space: "email", consistent: "person" });
const personName = <T extends Schema>(
  schema: T,
  space: "firstname" | "lastname",
) => personal(schema, { role: "direct", space, consistent: "person" });
const personPhone = <T extends Schema>(schema: T) =>
  personal(schema, { role: "direct", space: "phone", consistent: "person" });
const idString = <T extends Schema>(schema: T) =>
  personal(schema, { role: "technical", treatment: { extract: "remap" } });
const vocabulary = <T extends Schema>(schema: T) =>
  notPersonal(schema, "platform vocabulary, joins on its value");
const personRefId = <T extends Schema>(schema: T) =>
  v.pipe(schema, v.metadata({ [PRIVACY_SYMBOL]: { kind: "person", of: [] } }));

const FieldValueSchema = v.object({
  t: vocabulary(v.string()),
  v: v.unknown(),
  o: v.object({
    source: v.picklist(["flow", "dashboard", "import", "system"]),
    ref: v.string(),
  }),
});

export const SCHEMAS = {
  collections: {
    "+users": {
      _id: personId("user"),
      email: withIndex(personEmail(EmailSchema), { unique: true }),
      firstname: v.optional(personName(NameSchema, "firstname")),
      lastname: v.optional(personName(NameSchema, "lastname")),
      status: v.picklist(["active", "suspended", "deleted"]),
      notes: v.optional(
        v.array(
          v.object({
            authorId: refId("user"),
            body: v.pipe(v.string(), v.maxLength(5000)),
            createdAt: v.date(),
          }),
        ),
      ),
    },
    "+emails": {
      _id: dbId("email"),
      templateId: vocabulary(v.string()),
      expositionId: v.nullable(idString(v.string())),
      to: personEmail(EmailSchema),
      recipientUserId: v.nullable(idString(v.string())),
      status: v.picklist(["PENDING", "SENT"]),
      idempotencyKey: withIndex(v.string(), { unique: true }),
      devSnapshot: v.optional(
        v.object({ subject: v.string(), html: v.string(), text: v.string() }),
      ),
      createdAt: v.date(),
    },
  },
  multiCollections: {
    "+entreprises": {
      entreprise: {
        _id: dbId("entreprise"),
        identity: v.object({
          displayName: v.string(),
          legalName: v.nullable(v.string()),
        }),
        contact: v.object({
          email: personEmail(EmailSchema),
          phone: v.nullable(v.string()),
        }),
        createdByUserId: refId("user"),
      },
      member: {
        _id: personal(dbId("member"), { of: "userId" }),
        tenantId: refId("entreprise"),
        userId: refId("user"),
        role: vocabulary(v.string()),
      },
    },
  },
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: {
        information: {
          _id: refId("exposition"),
          name: v.pipe(v.string(), v.minLength(3)),
          createdBy: refId("user"),
          entreprise: refId("entreprise"),
        },
        field_definition: {
          _id: dbId("field_definition"),
          key: vocabulary(v.string()),
          typeId: vocabulary(v.string()),
          label: v.record(v.string(), v.string()),
        },
        expo_organization: {
          _id: refId("expo_organization"),
          entrepriseId: v.optional(refId("entreprise")),
          displayName: v.string(),
        },
        accountless_identity: {
          _id: personRefId(refId("accountless_identity")),
          email: personEmail(EmailSchema),
          firstname: personName(v.string(), "firstname"),
          lastname: personName(v.string(), "lastname"),
          phone: v.optional(personPhone(v.string())),
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
          status: v.picklist(["pending", "active", "withdrawn"]),
          fields: dynamic(v.record(v.string(), FieldValueSchema)),
          updatedAt: v.date(),
        },
        exhibitor_contact: {
          _id: personal(dbId("exhibitor_contact"), { of: "participantId" }),
          exhibitorId: refId("expo_organization"),
          participantId: refId("participant"),
          comments: v.array(
            v.object({
              from: refId("user"),
              content: v.pipe(v.string(), v.minLength(1), v.maxLength(2000)),
            }),
          ),
        },
      },
    },
    "+scans": {
      scope: refId("exposition"),
      types: {
        business_scan: {
          _id: personal(dbId("business_scan"), { of: "participantId" }),
          participantId: refId("participant"),
          scannedBy: refId("user"),
          exhibitorId: refId("expo_organization"),
          at: v.date(),
        },
      },
    },
  },
};

const origin = {
  "o.ref": { role: "technical", treatment: { extract: "remap" } },
} satisfies DynamicResolution;

const NAME_KEYS: Readonly<Record<string, "firstname" | "lastname">> = {
  firstname: "firstname",
  lastname: "lastname",
};
const KEPT_TYPES: ReadonlySet<string> = new Set([
  "number",
  "boolean",
  "date",
  "enum_single",
  "enum_multi",
]);

function isFieldValue(value: unknown): value is { t: string } {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { t?: unknown }).t === "string"
  );
}

export function resolveDynamic(
  unit: DynamicUnit,
): DynamicResolution | typeof SKIP_DYNAMIC {
  if (!isFieldValue(unit.value)) return SKIP_DYNAMIC;
  const type = unit.value.t;
  const name = NAME_KEYS[unit.key];
  if (type === "email") {
    return {
      ...origin,
      v: {
        role: "direct",
        space: "email",
        consistent: "person",
        schema: EmailSchema,
      },
    };
  }
  if (type === "phone") {
    return {
      ...origin,
      v: {
        role: "direct",
        space: "phone",
        consistent: "person",
        schema: v.string(),
      },
    };
  }
  if (type === "text" && name !== undefined) {
    return {
      ...origin,
      v: {
        role: "direct",
        space: name,
        consistent: "person",
        schema: v.string(),
      },
    };
  }
  if (KEPT_TYPES.has(type)) return { ...origin, v: { role: "technical" } };
  if (type === "ref_participant") {
    return {
      ...origin,
      v: { role: "technical", treatment: { extract: "remap" } },
    };
  }
  return { ...origin, v: { role: "content", schema: v.string() } };
}

const FIRSTNAMES = [
  "Ysolde",
  "Aldric",
  "Bérangère",
  "Corentin",
  "Eulalie",
  "Fulbert",
  "Gwenaëlle",
  "Hildebert",
  "Iseult",
  "Jocelyn",
  "Klervi",
  "Ludovine",
  "Maëlig",
  "Nolwenn",
  "Oriane",
  "Prosper",
  "Quitterie",
  "Romuald",
  "Sixtine",
  "Tanguy",
];
const LASTNAMES = [
  "Kerjean",
  "Abgrall",
  "Le Floc'h",
  "Quéméner",
  "Trébaol",
  "Guivarc'h",
  "Pennanéac'h",
  "Riou",
  "Tanguy-Morvan",
  "Coatanéa",
  "Huitric",
  "Prigent",
  "Le Goff",
  "Bourhis",
  "Cozic",
  "Derrien",
  "Gourmelon",
  "Jaouen",
];

function rng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz";

function ulidAt(time: number, random: () => number): string {
  let out = "";
  let t = time;
  for (let i = 0; i < 10; i++) {
    out = CROCKFORD[t % 32] + out;
    t = Math.floor(t / 32);
  }
  for (let i = 0; i < 16; i++) out += CROCKFORD[Math.floor(random() * 32)];
  return out;
}

export interface World {
  readonly users: Record<string, unknown>[];
  readonly emails: Record<string, unknown>[];
  readonly entreprises: Record<string, unknown>[];
  readonly expositions: Record<string, unknown>[];
  readonly scans: Record<string, unknown>[];
}

export const SIZES = {
  expositions: 5,
  users: 200,
  accountless: 50,
  scans: 2000,
} as const;

export function buildWorld(seed = 1): World {
  const random = rng(seed);
  const pick = <T>(list: readonly T[]): T =>
    list[Math.floor(random() * list.length)];
  let clock = Date.UTC(2025, 0, 6, 8, 0, 0);
  const id = (space: string) => {
    clock += 1000 + Math.floor(random() * 60_000);
    return `${space}:${ulidAt(clock, random)}`;
  };
  const at = () => new Date(clock + Math.floor(random() * 86_400_000));

  const users = Array.from({ length: SIZES.users }, (_, i) => {
    const firstname = pick(FIRSTNAMES);
    const lastname = pick(LASTNAMES);
    return {
      _id: id("user"),
      email: `${firstname}.${lastname}.${i}@corp-${i % 7}.fr`
        .normalize("NFD")
        .replace(/[^\x20-\x7e]/g, "")
        .replace(/[' ]/g, "")
        .toLowerCase(),
      firstname,
      lastname,
      status: "active",
    } satisfies Record<string, unknown>;
  });
  for (const user of users.slice(0, 20)) {
    Object.assign(user, {
      notes: [
        {
          authorId: users[0]._id,
          body: `Appelé ${user.firstname} ${user.lastname} au sujet de sa facture`,
          createdAt: at(),
        },
      ],
    });
  }

  const entreprises: Record<string, unknown>[] = [];
  for (let i = 0; i < 10; i++) {
    const owner = users[i];
    const tenant = id("entreprise");
    entreprises.push({
      _id: tenant,
      _type: "entreprise",
      identity: {
        displayName: `Ateliers ${owner.lastname} ${i}`,
        legalName: null,
      },
      contact: {
        email: owner.email,
        phone: `+3361234${String(i).padStart(4, "0")}`,
      },
      createdByUserId: owner._id,
    });
    entreprises.push({
      _id: id("member"),
      _type: "member",
      tenantId: tenant,
      userId: owner._id,
      role: "admin",
    });
  }

  const persons = Array.from({ length: 35 }, (_, i) => ({
    email: `invite.${i}@gmail.example`,
    firstname: pick(FIRSTNAMES),
    lastname: pick(LASTNAMES),
    phone: `+3370000${String(i).padStart(4, "0")}`,
  }));
  const expositions: Record<string, unknown>[] = [];
  const scans: Record<string, unknown>[] = [];
  const emails: Record<string, unknown>[] = [];
  const expoIds = Array.from({ length: SIZES.expositions }, () =>
    id("exposition"),
  );
  let accountlessLeft = SIZES.accountless;
  expoIds.forEach((expo, e) => {
    const push = (
      type: string,
      doc: Record<string, unknown>,
    ): Record<string, unknown> => {
      const full = { ...doc, _type: type, _scope: expo };
      expositions.push(full);
      return full;
    };
    push("information", {
      _id: expo,
      name: `Salon ${e + 1} des métiers`,
      createdBy: users[e]._id,
      entreprise: entreprises[0]._id,
    });
    for (const [key, typeId] of [
      ["firstname", "text"],
      ["lastname", "text"],
      ["email", "email"],
      ["phone", "phone"],
    ]) {
      push("field_definition", {
        _id: id("field_definition"),
        key,
        typeId,
        label: { fr: key },
      });
    }
    const orgs = Array.from({ length: 6 }, (_, o) =>
      push("expo_organization", {
        _id: id("expo_organization"),
        entrepriseId: entreprises[(o % 10) * 2]._id,
        displayName: `Stand ${o} ${pick(LASTNAMES)}`,
      }),
    );
    const participants: Record<string, unknown>[] = [];
    const fieldsOf = (
      person: { email: unknown; firstname: unknown; lastname: unknown },
      i: number,
    ) => {
      const o = { source: "dashboard", ref: users[i % 20]._id as string };
      const fields: Record<string, unknown> = {
        firstname: { t: "text", v: person.firstname, o },
        lastname: { t: "text", v: person.lastname, o },
        email: { t: "email", v: person.email, o },
      };
      if (i % 3 === 0) {
        Object.assign(fields, {
          job_title: {
            t: "text",
            v: `Responsable achats chez ${pick(LASTNAMES)} SA`,
            o,
          },
          newsletter: { t: "boolean", v: i % 2 === 0, o },
          visit_date: { t: "date", v: `2025-03-1${i % 9}`, o },
          interests: { t: "enum_multi", v: ["robotique", "iot"], o },
          bio: {
            t: "textarea",
            v: `Je suis ${person.firstname}, joignable au ${person.email}`,
            o,
          },
        });
      }
      if (i % 3 === 0 && participants.length > 0) {
        fields.referrer = { t: "ref_participant", v: participants[0]._id, o };
      }
      return fields;
    };
    users.forEach((user, i) => {
      if (random() > 0.95) return;
      const participant = push("participant", {
        _id: id("participant"),
        personRef: { kind: "user", userId: user._id },
        status: "active",
        fields: fieldsOf(user, i),
        updatedAt: at(),
      });
      participants.push(participant);
      if (i % 2 === 0) {
        emails.push({
          _id: id("email"),
          templateId: "core:participant_welcome",
          expositionId: expo,
          to: user.email,
          recipientUserId: user._id,
          status: "SENT",
          idempotencyKey: `welcome:${participant._id}`,
          devSnapshot: {
            subject: `Bienvenue ${user.firstname}`,
            html: `<p>Bonjour ${user.firstname} ${user.lastname}, votre badge (${user.email}) est prêt.</p>`,
            text: `Bonjour ${user.firstname} ${user.lastname}`,
          },
          createdAt: at(),
        });
      }
    });
    const share =
      e === expoIds.length - 1
        ? accountlessLeft
        : Math.floor(SIZES.accountless / expoIds.length);
    accountlessLeft -= share;
    for (let a = 0; a < share; a++) {
      const person = persons[(e * 7 + a) % persons.length];
      const identity = push("accountless_identity", {
        _id: id("accountless_identity"),
        ...person,
      });
      participants.push(
        push("participant", {
          _id: id("participant"),
          personRef: {
            kind: "accountless",
            accountlessIdentityId: identity._id,
          },
          status: "pending",
          fields: {
            ...fieldsOf(person, a),
            phone: {
              t: "phone",
              v: person.phone,
              o: { source: "flow", ref: "flow_session:x" },
            },
          },
          updatedAt: at(),
        }),
      );
    }
    participants.slice(0, 30).forEach((participant, c) => {
      push("exhibitor_contact", {
        _id: id("exhibitor_contact"),
        exhibitorId: orgs[c % orgs.length]._id,
        participantId: participant._id,
        comments: [
          {
            from: users[c]._id,
            content: `Rappeler ${(participant.fields as Record<string, { v: unknown }>).firstname.v} mardi`,
          },
        ],
      });
    });
    const perExpo = SIZES.scans / SIZES.expositions;
    for (let s = 0; s < perExpo; s++) {
      scans.push({
        _id: id("business_scan"),
        _type: "business_scan",
        _scope: expo,
        participantId: pick(participants)._id,
        scannedBy: pick(users)._id,
        exhibitorId: pick(orgs)._id,
        at: at(),
      });
    }
  });
  for (const user of users.slice(0, 100)) {
    emails.push({
      _id: id("email"),
      templateId: "system:password_reset",
      expositionId: null,
      to: user.email,
      recipientUserId: user._id,
      status: "PENDING",
      idempotencyKey: `reset:${user._id}`,
      createdAt: at(),
    });
  }
  return { users, emails, entreprises, expositions, scans };
}

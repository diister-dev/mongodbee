import { mkdir, writeFile } from "node:fs/promises";
import { ObjectId } from "mongodb";
import process from "node:process";
import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { decodeTime } from "../../src/utils/ulid.ts";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import { defineType } from "../../src/type-definition.ts";
import { from } from "../../src/computed.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  dynamic,
  mention,
  notPersonal,
  personal,
  personId,
  type PrivacyConsistency,
  type PrivacyTransformerOptions,
  remapId,
  type TransformNote,
} from "../../src/privacy/mod.ts";
import {
  extractCommand,
  transformState,
} from "../../src/migration/cli/commands/extract.ts";
import { MongoClient } from "../../src/mongodb.ts";
import { withTempDir } from "../migration/cli/shared.ts";
import {
  createEmptyDatabaseState,
  type DatabaseState,
  type SchemasDefinition,
} from "../../src/migration/types.ts";

const REAL = {
  firstname: "Zebulon",
  lastname: "Quixotique",
  email: "zebulon.quixotique@acme-reelle.fr",
  phone: "+33 6 12 34 56 78",
  street: "12 rue des Kumquats",
  city: "Ornans-sur-Loue",
  company: "Ornithorynque Industries",
  exposition: "Salon Ornithorynque 2026",
  freeText: "rappeler Zebulon Quixotique au sujet de sa facture",
  birthDay: "1985-03-14",
  lastSeenAt: "2026-05-04T13:37:42.123Z",
  postalCode: 75012,
  siret: "73282932000074",
} as const;

const BIRTH_DATE = new Date(Date.UTC(1985, 2, 14));
const USER_ULID = "01j5zk3v8n2q4x6y8z0b1c3d5e";
const USER_ID = `user:${USER_ULID}`;
const EXPOSITION_ID = "exposition:01j5zk3v8n2q4x6y8z0b1c3d5g";
const SECRET = "leak-audit";

function stringsOf(value: unknown, out: string[] = []): string[] {
  if (value === null || value === undefined) return out;
  if (value instanceof Date) {
    out.push(value.toISOString(), String(value.getTime()));
    return out;
  }
  if (Array.isArray(value)) {
    for (const item of value) stringsOf(item, out);
    return out;
  }
  if (typeof value === "object") {
    const proto = Object.getPrototypeOf(value);
    if (proto !== Object.prototype && proto !== null) {
      out.push(String(value));
      return out;
    }
    for (const [key, item] of Object.entries(value)) {
      out.push(key);
      stringsOf(item, out);
    }
    return out;
  }
  out.push(String(value));
  return out;
}

function leaked(
  output: unknown,
  originals: readonly (string | number | Date)[],
): string[] {
  const haystack = stringsOf(output).map((s) => s.toLowerCase());
  return originals
    .map((o) => (o instanceof Date ? o.toISOString() : String(o)).toLowerCase())
    .filter((needle) => haystack.some((s) => s.includes(needle)));
}

function assertNoLeak(
  output: unknown,
  originals: readonly (string | number | Date)[],
): void {
  const found = leaked(output, originals);
  assertEquals(found, [], `original values survived: ${found.join(", ")}`);
}

function extract(
  schemas: SchemasDefinition,
  state: DatabaseState,
  options: Partial<PrivacyTransformerOptions> & {
    consistency?: PrivacyConsistency;
  } = {},
) {
  const plan = buildPrivacyPlan({ schemas, posture: "strict" });
  const transformer = createPrivacyTransformer({
    plan,
    schemas,
    secret: SECRET,
    ...options,
  });
  return {
    plan,
    transformer,
    ...transformState(state, plan, transformer, {
      schemas,
      remapInstanceName: (name) =>
        remapId(SECRET, name, options.timeShiftMs ?? 0),
    }),
  };
}

function stateWith(
  collections: Record<string, Record<string, unknown>[]>,
): DatabaseState {
  const state = createEmptyDatabaseState();
  for (const [name, content] of Object.entries(collections)) {
    state.collections[name] = { content };
  }
  return state;
}

const EmailSchema = personal(v.pipe(v.string(), v.email()), {
  role: "direct",
  consistent: "person",
});

const USERS = {
  _id: personId("user"),
  email: EmailSchema,
  firstname: personal(v.string(), { role: "direct" }),
};

test({
  name: "leak L1: an unowned document keeps every unclassified string in clear",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": USERS,
        companies: {
          _id: dbId("company"),
          name: v.string(),
          address: v.object({ street: v.string(), city: v.string() }),
          notes: v.array(v.string()),
          siret: withIndex(v.string(), { unique: true }),
          contactFirstname: v.string(),
          contactEmail: v.string(),
        },
        expositions: {
          _id: dbId("exposition"),
          name: v.pipe(v.string(), v.minLength(1)),
          createdBy: mention(refId("user")),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
          },
        ],
        companies: [
          {
            _id: "company:01j5zk3v8n2q4x6y8z0b1c3d5h",
            name: REAL.company,
            address: { street: REAL.street, city: REAL.city },
            notes: [REAL.freeText],
            siret: REAL.siret,
            contactFirstname: REAL.firstname,
            contactEmail: REAL.email,
          },
        ],
        expositions: [
          {
            _id: EXPOSITION_ID,
            name: REAL.exposition,
            createdBy: USER_ID,
          },
        ],
      }),
    );
    assertEquals(result.plan.summary.unknown, 0, "no gate stops this extract");
    assertNoLeak(result.state, [
      REAL.company,
      REAL.street,
      REAL.city,
      REAL.freeText,
      REAL.siret,
      REAL.firstname,
      REAL.email,
      REAL.exposition,
    ]);
  },
});

test({
  name: "leak L2: role contact keeps the phone and the address of a person",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": {
          ...USERS,
          phone: personal(v.string(), { role: "contact" }),
          address: personal(v.string(), { role: "contact" }),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
            phone: REAL.phone,
            address: REAL.street,
          },
        ],
      }),
    );
    assertNoLeak(result.state, [REAL.phone, REAL.street]);
  },
});

test({
  name: "leak L3: numbers and dates of a person are inferred technical and kept exactly",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": {
          ...USERS,
          birthDate: v.date(),
          postalCode: v.number(),
          lastSeenAt: v.pipe(v.string(), v.isoTimestamp()),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
            birthDate: BIRTH_DATE,
            postalCode: REAL.postalCode,
            lastSeenAt: REAL.lastSeenAt,
          },
        ],
      }),
    );
    assertEquals(result.plan.summary.unknown, 0, "no gate stops this extract");
    assertNoLeak(result.state, [BIRTH_DATE, REAL.postalCode, REAL.lastSeenAt]);
  },
});

test({
  name: "leak L4: an ISO date string is technical and never shifted, even with a time shift",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": {
          ...USERS,
          birthDay: v.pipe(v.string(), v.isoDate()),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
            birthDay: REAL.birthDay,
          },
        ],
      }),
      { timeShiftMs: 37 * 86_400_000 },
    );
    assertNoLeak(result.state, [REAL.birthDay]);
  },
});

test({
  name: "leak L5: a union with one ISO option makes every string of the path technical, bypassing the unknown gate",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": {
          ...USERS,
          nickname: v.union([v.pipe(v.string(), v.isoTimestamp()), v.string()]),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
            nickname: REAL.lastname,
          },
        ],
      }),
    );
    assertEquals(result.plan.summary.unknown, 0, "no gate stops this extract");
    assertNoLeak(result.state, [REAL.lastname]);
  },
});

test({
  name: "leak L6: record keys are never transformed",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": {
          ...USERS,
          scoreByContact: v.record(v.string(), v.number()),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
            scoreByContact: { [REAL.email]: 3 },
          },
        ],
      }),
    );
    assertNoLeak(result.state, [REAL.email]);
  },
});

test({
  name: "leak L6: a record keyed by a reference keeps the source id, and no longer joins",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": {
          ...USERS,
          seenBy: v.record(refId("user"), v.boolean()),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
            seenBy: { [USER_ID]: true },
          },
        ],
      }),
    );
    const [user] = result.state.collections["+users"].content;
    assertEquals(Object.keys(user.seenBy as object), [user._id]);
  },
});

test({
  name: "leak L7: a value that does not match its technical or reference schema is written verbatim",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": {
          ...USERS,
          legacyCount: v.optional(v.number()),
          flag: v.optional(v.union([v.number(), v.boolean()])),
          managerId: v.optional(refId("user")),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
            legacyCount: REAL.phone,
            flag: REAL.city,
            managerId: REAL.email,
          },
        ],
      }),
    );
    assertNoLeak(result.state, [REAL.phone, REAL.city, REAL.email]);
  },
});

test({
  name: "leak L8: validation notes quote the received value",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: { "+users": USERS },
    };
    const plan = buildPrivacyPlan({ schemas });
    const transformer = createPrivacyTransformer({
      plan,
      schemas,
      secret: SECRET,
    });
    const malformed = "zebulon.quixotique(at)acme-reelle.fr";
    const { notes } = transformer.transform("collections/+users/", {
      _id: USER_ID,
      email: malformed,
      firstname: REAL.firstname,
    });
    assert(notes.some((n) => n.kind === "input_invalid"));
    assertNoLeak(
      notes.map((n: TransformNote) => n.message ?? ""),
      [malformed],
    );
  },
});

test({
  name: "leak L9: a non-string _id is copied as is, whatever it holds",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": USERS,
        sessions: { userId: refId("user"), device: v.picklist(["web", "app"]) },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
          },
        ],
        sessions: [
          {
            _id: { email: REAL.email, at: REAL.lastSeenAt },
            userId: USER_ID,
            device: "web",
          },
        ],
      }),
    );
    assertNoLeak(result.state, [REAL.email, REAL.lastSeenAt]);
  },
});

test({
  name: "leak L10: a dynamic subtree of an unowned document is kept in clear when the resolver skips it",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": USERS,
        forms: {
          _id: dbId("form"),
          answers: dynamic(v.record(v.string(), v.unknown())),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
          },
        ],
        forms: [
          {
            _id: "form:01j5zk3v8n2q4x6y8z0b1c3d5h",
            answers: { email: REAL.email, phone: REAL.phone },
          },
        ],
      }),
    );
    assertNoLeak(result.state, [REAL.email, REAL.phone]);
  },
});

test({
  name: "leak L11: an exempt document keeps its free text, which can quote a person",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": USERS,
        audit: {
          _id: notPersonal(dbId("audit"), "operational log"),
          message: v.string(),
          actorId: refId("user"),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
          },
        ],
        audit: [
          {
            _id: "audit:01j5zk3v8n2q4x6y8z0b1c3d5h",
            message: `password reset for ${REAL.email}`,
            actorId: USER_ID,
          },
        ],
      }),
    );
    assertNoLeak(result.state, [REAL.email]);
  },
});

test({
  name: "leak L12: without a time shift every ULID keeps its exact creation millisecond",
  fn: () => {
    const schemas: SchemasDefinition = { collections: { "+users": USERS } };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
          },
        ],
      }),
    );
    const out = result.state.collections["+users"].content[0]._id as string;
    const remapped = out.split(":")[1];
    assert(
      decodeTime(remapped.toUpperCase()) !==
        decodeTime(USER_ULID.toUpperCase()),
      "the creation time of the source document survives in its remapped id",
    );
  },
});

test({
  name: "leak L13: a resolver can classify a dynamic value as contact and keep it",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": {
          ...USERS,
          fields: dynamic(v.record(v.string(), v.unknown())),
        },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          {
            _id: USER_ID,
            email: REAL.email,
            firstname: REAL.firstname,
            fields: { phone: REAL.phone },
          },
        ],
      }),
      { resolveDynamic: () => ({ "": { role: "contact" } }) },
    );
    assertNoLeak(result.state, [REAL.phone]);
  },
});

test("guard: an id that embeds personal data is remapped, and its references follow", () => {
  const leakyId = `user:${REAL.email}`;
  const schemas: SchemasDefinition = {
    collections: {
      "+users": USERS,
      badges: {
        _id: personId("badge", { of: ["user"] }),
        userId: refId("user"),
      },
    },
  };
  const result = extract(
    schemas,
    stateWith({
      "+users": [
        {
          _id: leakyId,
          email: REAL.email,
          firstname: REAL.firstname,
        },
      ],
      badges: [{ _id: "badge:01j5zk3v8n2q4x6y8z0b1c3d5h", userId: leakyId }],
    }),
  );
  assertNoLeak(result.state, [
    REAL.email,
    REAL.firstname,
    "zebulon",
    "quixotique",
  ]);
  assertEquals(
    result.state.collections.badges.content[0].userId,
    result.state.collections["+users"].content[0]._id,
  );
});

test("guard: unknown keys of loose, nested and rest objects never reach the output", () => {
  const schemas: SchemasDefinition = {
    collections: {
      "+users": {
        ...USERS,
        prefs: v.looseObject({
          theme: v.picklist(["dark", "light"]),
          inner: v.looseObject({ size: v.number() }),
        }),
        extra: v.objectWithRest({ kind: v.picklist(["a", "b"]) }, v.string()),
        tuple: v.tuple([v.number(), v.string()]),
        lazy: v.lazy(() => v.object({ label: v.string() })),
        both: v.intersect([
          v.object({ a: v.string() }),
          v.object({ b: v.number() }),
        ]),
      },
    },
  };
  const result = extract(
    schemas,
    stateWith({
      "+users": [
        {
          _id: USER_ID,
          email: REAL.email,
          firstname: REAL.firstname,
          prefs: {
            theme: "dark",
            phone: REAL.phone,
            inner: { size: 3, street: REAL.street },
          },
          extra: { kind: "a", city: REAL.city },
          tuple: [1, REAL.lastname],
          lazy: { label: REAL.company },
          both: { a: REAL.exposition, b: 1 },
          [REAL.siret]: REAL.freeText,
        },
      ],
    }),
  );
  assertNoLeak(result.state, [
    REAL.email,
    REAL.firstname,
    REAL.phone,
    REAL.street,
    REAL.city,
    REAL.lastname,
    REAL.company,
    REAL.exposition,
    REAL.siret,
    REAL.freeText,
  ]);
});

test("guard: a hostile document never reaches the output, and no note message quotes it", () => {
  const schemas: SchemasDefinition = {
    collections: {
      "+users": {
        ...USERS,
        bio: personal(
          v.custom<string>(() => true),
          { role: "content" },
        ),
        secretAnswer: personal(v.string(), {
          role: "technical",
          treatment: { extract: "opaque" },
        }),
      },
    },
  };
  const plan = buildPrivacyPlan({ schemas });
  const transformer = createPrivacyTransformer({
    plan,
    schemas,
    secret: SECRET,
    validate: false,
  });
  const { doc, notes } = transformer.transform("collections/+users/", {
    _id: USER_ID,
    email: REAL.email,
    firstname: REAL.firstname,
    bio: REAL.freeText,
    secretAnswer: REAL.lastname,
    [REAL.phone]: REAL.street,
  });
  assertNoLeak(
    [doc, notes.map((n) => n.message ?? "")],
    [
      REAL.email,
      REAL.firstname,
      REAL.freeText,
      REAL.lastname,
      REAL.phone,
      REAL.street,
    ],
  );
});

test({
  name: "leak L14: the path of an unknown_key note is the dropped key itself",
  fn: () => {
    const schemas: SchemasDefinition = { collections: { "+users": USERS } };
    const plan = buildPrivacyPlan({ schemas });
    const transformer = createPrivacyTransformer({
      plan,
      schemas,
      secret: SECRET,
      validate: false,
    });
    const { notes } = transformer.transform("collections/+users/", {
      _id: USER_ID,
      email: REAL.email,
      firstname: REAL.firstname,
      [REAL.phone]: true,
    });
    assertNoLeak(
      notes.map((n) => n.path),
      [REAL.phone],
    );
  },
});

test({
  name: "leak L15: a multi-model instance name keeps the source id",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: { "+users": USERS },
      multiModels: {
        exposition: {
          badge: {
            _id: personId("badge", { of: ["user"] }),
            userId: refId("user"),
          },
        },
      },
    };
    const state = stateWith({
      "+users": [
        { _id: USER_ID, email: REAL.email, firstname: REAL.firstname },
      ],
    });
    state.multiModels[EXPOSITION_ID] = {
      modelType: "exposition",
      content: [
        {
          _id: "badge:01j5zk3v8n2q4x6y8z0b1c3d5h",
          _type: "badge",
          userId: USER_ID,
        },
      ],
    };
    const result = extract(schemas, state);
    assertNoLeak(Object.keys(result.state.multiModels), [
      EXPOSITION_ID.split(":")[1],
    ]);
  },
});

const MemberType = defineType({
  schema: v.object({
    _id: personId("user"),
    email: EmailSchema,
    nickname: v.optional(v.string()),
    teamId: withIndex(refId("team")),
  }),
});

const TeamType = defineType({
  schema: v.object({ _id: dbId("team"), label: v.picklist(["a", "b"]) }),
  computed: {
    memberEmails: from("+users", MemberType)
      .by((u) => u.teamId)
      .collect((u) => u.email)
      .maxEntries(10),
    memberNicknames: from("+users", MemberType)
      .by((u) => u.teamId)
      .collect((u) => u.nickname)
      .maxEntries(10),
  },
});

const TEAM_ID = "team:01j5zk3v8n2q4x6y8z0b1c3d5h";

function teamExtract() {
  const schemas: SchemasDefinition = {
    collections: { "+users": MemberType, teams: TeamType },
  };
  return extract(
    schemas,
    stateWith({
      "+users": [
        {
          _id: USER_ID,
          email: REAL.email,
          nickname: REAL.lastname,
          teamId: TEAM_ID,
        },
      ],
      teams: [
        {
          _id: TEAM_ID,
          label: "a",
          _computed: {
            memberEmails: [REAL.email],
            memberNicknames: [REAL.lastname],
            _rev: 1,
          },
        },
      ],
    }),
  );
}

test({
  name: "leak L16: a computed copy of a dropped person field is kept in clear by its unowned subject",
  fn: () => {
    const result = teamExtract();
    assertNoLeak(result.state.collections["+users"].content, [REAL.lastname]);
    assertNoLeak(result.state.collections.teams.content, [REAL.lastname]);
  },
});

test({
  name: "leak L17: a computed collection is transformed on its own instead of recomputed from the transformed sources",
  fn: () => {
    const result = teamExtract();
    const user = result.state.collections["+users"].content[0];
    const team = result.state.collections.teams.content[0] as {
      _computed?: { memberEmails?: unknown[] };
    };
    assertEquals(team._computed?.memberEmails, [user.email]);
  },
});

const TEST_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";
const SRC = new URL("../../src/", import.meta.url).href;
const CLI_SECRET = "canary-secret-7f3a91";

async function writeLeakProject(dir: string): Promise<void> {
  await mkdir(`${dir}/migrations`, { recursive: true });
  await writeFile(
    `${dir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: ${JSON.stringify(
      TEST_URI,
    )} } }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
  await writeFile(
    `${dir}/schemas.ts`,
    `
import * as v from "${SRC}schema.ts";
import { dbId } from "${SRC}ids.ts";
import { personal, personId } from "${SRC}privacy/mod.ts";
export const schemas = {
  collections: {
    "+users": {
      _id: personId("user"),
      email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
      firstname: personal(v.string(), { role: "direct" }),
      nickname: v.optional(v.string()),
    },
    companies: { _id: dbId("company"), name: v.string() },
  },
};
`,
  );
}

async function runCliExtract(options: {
  json: boolean;
}): Promise<{ output: string; written: Record<string, unknown>[] }> {
  const tag = crypto.randomUUID().replace(/-/g, "").slice(0, 8);
  const source = `mongodbee_test_leaks_src_${tag}`;
  const target = `mongodbee_test_leaks_out_${tag}`;
  const client = new MongoClient(TEST_URI);
  await client.connect();
  const lines: string[] = [];
  const original = { log: console.log, error: console.error };
  try {
    const db = client.db(source);
    await db.collection("+users").insertOne({
      _id: USER_ID,
      email: REAL.email,
      firstname: REAL.firstname,
      nickname: REAL.lastname,
    } as never);
    await db.collection("companies").insertOne({
      _id: "company:01j5zk3v8n2q4x6y8z0b1c3d5h",
      name: REAL.company,
    } as never);
    console.log = (...args: unknown[]) =>
      lines.push(args.map(String).join(" "));
    console.error = console.log;
    await withTempDir(async (dir) => {
      await writeLeakProject(dir);
      await extractCommand({
        cwd: dir,
        fromDb: source,
        toDb: target,
        secret: CLI_SECRET,
        allowUnknown: true,
        json: options.json,
      });
    });
    const out = client.db(target);
    const written = [
      ...(await out.collection("+users").find({}).toArray()),
      ...(await out.collection("companies").find({}).toArray()),
    ] as Record<string, unknown>[];
    return { output: lines.join("\n"), written };
  } finally {
    console.log = original.log;
    console.error = original.error;
    await client.db(source).dropDatabase();
    await client.db(target).dropDatabase();
    await client.close();
  }
}

test({
  name: "guard: the extract command prints neither the secret nor a source value, in text or json",
  timeout: 60_000,
  fn: async () => {
    for (const json of [false, true]) {
      const { output } = await runCliExtract({ json });
      assert(output.length > 0);
      assertNoLeak(output, [
        CLI_SECRET,
        REAL.email,
        REAL.firstname,
        REAL.lastname,
        REAL.company,
        USER_ULID,
      ]);
    }
  },
});

test({
  name: "leak L1 (cli): an unowned collection reaches the target database in clear",
  timeout: 60_000,
  fn: async () => {
    const { written } = await runCliExtract({ json: true });
    assertEquals(written.length, 2);
    assertNoLeak(written, [
      REAL.email,
      REAL.firstname,
      REAL.lastname,
      REAL.company,
    ]);
  },
});

test({
  name: "leak L18: a reference-or-email union keeps the email, with no note at all",
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": {
          ...USERS,
          assignee: v.union([refId("user"), v.pipe(v.string(), v.email())]),
        },
      },
    };
    const plan = buildPrivacyPlan({ schemas });
    const transformer = createPrivacyTransformer({
      plan,
      schemas,
      secret: SECRET,
    });
    const { doc, notes } = transformer.transform("collections/+users/", {
      _id: USER_ID,
      email: REAL.email,
      firstname: REAL.firstname,
      assignee: `external.${REAL.email}`,
    });
    assertEquals(notes, []);
    assertNoLeak(doc, [REAL.email]);
  },
});

const INSTANCE_CREATED_AT = new Date("2019-11-23T08:15:42.777Z");
const MIGRATION_APPLIED_AT = new Date("2021-07-09T17:03:11.321Z");
const USER_CREATED_AT = new Date(REAL.lastSeenAt);
const PHONE_NUMBER = 33612345678;
const LEGACY_REFERENCE = "quixotique:42";

async function writeStrictProject(dir: string): Promise<void> {
  await mkdir(`${dir}/migrations`, { recursive: true });
  await writeFile(
    `${dir}/mongodbee.config.ts`,
    `export default { database: { connection: { uri: ${JSON.stringify(
      TEST_URI,
    )} } }, paths: { migrations: "./migrations", schemas: "./schemas.ts" } };`,
  );
  await writeFile(
    `${dir}/schemas.ts`,
    `
import * as v from "${SRC}schema.ts";
import { dbId, refId } from "${SRC}ids.ts";
import { withIndex } from "${SRC}indexes.ts";
import { dynamic, personal, personId } from "${SRC}privacy/mod.ts";
export const schemas = {
  collections: {
    "+users": {
      _id: personId("user"),
      email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
      firstname: personal(v.string(), { role: "direct" }),
      createdAt: v.date(),
      answers: dynamic(v.record(v.string(), v.string())),
      managerId: v.optional(refId("user")),
    },
    companies: {
      _id: dbId("company"),
      name: v.string(),
      phone: v.number(),
      foundedAt: v.date(),
      slug: withIndex(v.string(), { unique: true }),
    },
  },
  multiCollections: {
    catalog: { product: { _id: dbId("product"), label: v.string() } },
  },
  multiModels: {
    exposition: {
      badge: { _id: personId("badge", { of: ["user"] }), userId: refId("user") },
    },
  },
};
`,
  );
}

interface StrictRun {
  readonly text: string;
  readonly json: string;
  readonly collections: Record<string, Record<string, unknown>[]>;
  readonly definitions: unknown;
}

async function seedStrictSource(client: MongoClient, source: string) {
  const db = client.db(source);
  await db.collection("+users").insertOne({
    _id: USER_ID,
    email: REAL.email,
    firstname: REAL.firstname,
    createdAt: USER_CREATED_AT,
    answers: { [REAL.email]: "yes" },
    managerId: LEGACY_REFERENCE,
  } as never);
  await db.collection("companies").insertOne({
    _id: "company:01j5zk3v8n2q4x6y8z0b1c3d5h",
    name: REAL.company,
    phone: PHONE_NUMBER,
    foundedAt: BIRTH_DATE,
    slug: "ornithorynque-industries",
  } as never);
  await db.collection("catalog").insertMany([
    {
      _id: "product:01j5zk3v8n2q4x6y8z0b1c3d5j",
      _type: "product",
      label: REAL.city,
    },
    { _id: "draft:1", _type: "_draft", note: REAL.freeText },
  ] as never);
  await db.collection(EXPOSITION_ID).insertMany([
    {
      _id: "_information",
      _type: "_information",
      collectionType: "exposition",
      createdAt: INSTANCE_CREATED_AT,
    },
    {
      _id: "_migrations",
      _type: "_migrations",
      fromMigrationId: "extract",
      mongodbeeVersion: "0.23.0",
      appliedMigrations: [
        {
          id: "extract",
          operation: "applied",
          appliedAt: MIGRATION_APPLIED_AT,
        },
      ],
    },
    {
      _id: "badge:01j5zk3v8n2q4x6y8z0b1c3d5k",
      _type: "badge",
      userId: USER_ID,
    },
  ] as never);
}

async function runStrictCli(json: boolean, target: string, source: string) {
  const lines: string[] = [];
  const original = { log: console.log, error: console.error };
  console.log = (...args: unknown[]) => lines.push(args.map(String).join(" "));
  console.error = console.log;
  try {
    await withTempDir(async (dir) => {
      await writeStrictProject(dir);
      await extractCommand({
        cwd: dir,
        fromDb: source,
        ...(json ? { dryRun: true } : { toDb: target }),
        secret: CLI_SECRET,
        json,
      });
    });
  } finally {
    console.log = original.log;
    console.error = original.error;
  }
  return lines.join("\n");
}

let strictRun: Promise<StrictRun> | undefined;

function strictCli(): Promise<StrictRun> {
  strictRun ??= (async () => {
    const tag = crypto.randomUUID().replace(/-/g, "").slice(0, 8);
    const source = `mongodbee_test_leaks_strict_src_${tag}`;
    const target = `mongodbee_test_leaks_strict_out_${tag}`;
    const client = new MongoClient(TEST_URI);
    await client.connect();
    try {
      await seedStrictSource(client, source);
      const text = await runStrictCli(false, target, source);
      const json = await runStrictCli(true, target, source);
      const out = client.db(target);
      const infos = await out.listCollections().toArray();
      const collections: Record<string, Record<string, unknown>[]> = {};
      const indexes: unknown[] = [];
      for (const info of infos) {
        collections[info.name] = (await out
          .collection(info.name)
          .find({})
          .toArray()) as Record<string, unknown>[];
        indexes.push(await out.collection(info.name).indexes());
      }
      return { text, json, collections, definitions: [infos, indexes] };
    } finally {
      await client.db(source).dropDatabase();
      await client.db(target).dropDatabase();
      await client.close();
    }
  })();
  return strictRun;
}

function instanceOf(run: StrictRun): Record<string, unknown>[] {
  const name = Object.keys(run.collections).find((n) =>
    n.startsWith("exposition:"),
  );
  assert(name !== undefined, "the multi-model instance was written");
  return run.collections[name];
}

test({
  name: "guard (strict cli): text and json output print no source value, id or secret",
  timeout: 60_000,
  fn: async () => {
    const run = await strictCli();
    assertNoLeak(
      [run.text, run.json],
      [
        CLI_SECRET,
        REAL.email,
        REAL.firstname,
        REAL.company,
        REAL.freeText,
        USER_ULID,
        EXPOSITION_ID.split(":")[1],
      ],
    );
  },
});

test({
  name: "guard (strict cli): collection names, validators and indexes of the target carry no source value",
  timeout: 60_000,
  fn: async () => {
    const run = await strictCli();
    assertNoLeak(
      [Object.keys(run.collections), run.definitions],
      [
        REAL.email,
        REAL.company,
        REAL.city,
        USER_ULID,
        EXPOSITION_ID.split(":")[1],
      ],
    );
  },
});

test({
  name: "guard (strict cli): unowned strings, unique slugs and multi-model content are faked or remapped",
  timeout: 60_000,
  fn: async () => {
    const run = await strictCli();
    assertNoLeak(
      [run.collections.companies, run.collections["+users"][0].firstname],
      [REAL.company, "ornithorynque", REAL.firstname],
    );
    const badge = instanceOf(run).find((d) => d._type === "badge");
    assertEquals(badge?.userId, run.collections["+users"][0]._id);
  },
});

// TODO(privacy): R1, unblocked by extract passing timeShiftMs only when --shift-days is given
test({
  name: "leak R1 (strict cli): without --shift-days the CLI passes a zero shift, so ids and dates keep their real time",
  ignore: true,
  timeout: 60_000,
  fn: async () => {
    const run = await strictCli();
    const [user] = run.collections["+users"];
    const remapped = String(user._id).split(":")[1];
    assert(
      decodeTime(remapped.toUpperCase()) !==
        decodeTime(USER_ULID.toUpperCase()),
      "the remapped ULID keeps the source creation millisecond",
    );
    assertNoLeak(
      [user.createdAt, run.collections.companies[0].foundedAt],
      [USER_CREATED_AT, BIRTH_DATE],
    );
  },
});

// TODO(privacy): R3, unblocked by remap checking the id prefix against the declared spaces
test({
  name: "leak R3 (strict cli): a reference holding a foreign prefix keeps that prefix in clear",
  ignore: true,
  timeout: 60_000,
  fn: async () => {
    const run = await strictCli();
    assertNoLeak(run.collections["+users"], ["quixotique"]);
  },
});

// TODO(privacy): R4, unblocked by copying only the known _information and _migrations metadata documents
test({
  name: "leak R4 (strict cli): any document whose _type starts with an underscore is copied verbatim",
  ignore: true,
  timeout: 60_000,
  fn: async () => {
    const run = await strictCli();
    assertNoLeak(run.collections.catalog, [REAL.freeText]);
  },
});

// TODO(privacy): R5, unblocked by shifting the dates of copied metadata documents
test({
  name: "leak R5 (strict cli): multi-model metadata keeps the real instance creation and migration times",
  ignore: true,
  timeout: 60_000,
  fn: async () => {
    const run = await strictCli();
    assertNoLeak(instanceOf(run), [INSTANCE_CREATED_AT, MIGRATION_APPLIED_AT]);
  },
});

// TODO(privacy): R6 (T12), unblocked by mapping dynamic keys once the resolver has read them
test({
  name: "leak R6 (strict cli): the keys of a dynamic record stay in clear",
  ignore: true,
  timeout: 60_000,
  fn: async () => {
    const run = await strictCli();
    assertNoLeak(run.collections["+users"], [REAL.email]);
  },
});

// TODO(privacy): R7, unblocked by a decision on numbers in unowned documents under the strict posture
test({
  name: "leak R7 (strict cli): a number in an unowned document is kept exactly, even when it is a phone",
  ignore: true,
  timeout: 60_000,
  fn: async () => {
    const run = await strictCli();
    assertNoLeak(run.collections.companies, [PHONE_NUMBER]);
  },
});

// TODO(privacy): R2, unblocked by remapping or refusing a numeric _id
test({
  name: "leak R2: a numeric _id is copied as is",
  ignore: true,
  fn: () => {
    const schemas: SchemasDefinition = {
      collections: {
        "+users": USERS,
        cards: { _id: v.number(), holderId: refId("user") },
      },
    };
    const result = extract(
      schemas,
      stateWith({
        "+users": [
          { _id: USER_ID, email: REAL.email, firstname: REAL.firstname },
        ],
        cards: [{ _id: PHONE_NUMBER, holderId: USER_ID }],
      }),
    );
    assertNoLeak(result.state.collections.cards, [PHONE_NUMBER]);
  },
});

test("guard: an ObjectId _id is remapped, keeps its shape and no longer carries the source time", () => {
  const source = new ObjectId("5f1d7c2a9b1e8a3c4d5e6f70");
  const schemas: SchemasDefinition = {
    collections: {
      "+users": USERS,
      sessions: { holderId: refId("user") },
    },
  };
  const result = extract(
    schemas,
    stateWith({
      "+users": [
        { _id: USER_ID, email: REAL.email, firstname: REAL.firstname },
      ],
      sessions: [{ _id: source, holderId: USER_ID }],
    }),
  );
  const [session] = result.state.collections.sessions.content;
  assert(session._id instanceof ObjectId);
  assert(!session._id.equals(source));
  assert(
    session._id.getTimestamp().getTime() !== source.getTimestamp().getTime(),
  );
  assertNoLeak(session, [source.toHexString().slice(8)]);
});

test("guard: record keys are faked distinctly and consistently, never the original key", () => {
  const schemas: SchemasDefinition = {
    collections: {
      "+users": {
        ...USERS,
        scoreByContact: v.record(v.string(), v.number()),
      },
    },
  };
  const keys = [REAL.email, REAL.phone, REAL.company];
  const result = extract(
    schemas,
    stateWith({
      "+users": [
        {
          _id: USER_ID,
          email: REAL.email,
          firstname: REAL.firstname,
          scoreByContact: Object.fromEntries(keys.map((k, i) => [k, i])),
        },
        {
          _id: "user:01j5zk3v8n2q4x6y8z0b1c3d5f",
          email: "other@acme-reelle.fr",
          firstname: REAL.lastname,
          scoreByContact: { [REAL.email]: 9 },
        },
      ],
    }),
  );
  const [first, second] = result.state.collections["+users"].content;
  const firstKeys = Object.keys(first.scoreByContact as object);
  assertEquals(new Set(firstKeys).size, keys.length);
  assertEquals(
    Object.keys(second.scoreByContact as object),
    [firstKeys[0]],
    "the same source key maps to the same fake key at the same path",
  );
  assertNoLeak(result.state, [
    REAL.email,
    REAL.phone,
    REAL.company,
    "acme-reelle",
  ]);
});

test("guard: a unique field that cannot be made distinct reports a collision without quoting a value", () => {
  const schemas: SchemasDefinition = {
    collections: {
      "+users": {
        ...USERS,
        initial: withIndex(v.pipe(v.string(), v.regex(/^[ab]$/)), {
          unique: true,
        }),
      },
    },
  };
  const result = extract(
    schemas,
    stateWith({
      "+users": ["a", "b", "a-prime"].map((initial, i) => ({
        _id: `user:01j5zk3v8n2q4x6y8z0b1c3d6${i}`,
        email: `zebulon${i}@acme-reelle.fr`,
        firstname: REAL.firstname,
        initial,
      })),
    }),
  );
  const notes = Object.values(result.summary).flatMap((entry) =>
    Object.keys(entry.notes),
  );
  assert(notes.includes("collision"), "the third value must collide");
  const plan = result.plan;
  const transformer = createPrivacyTransformer({
    plan,
    schemas,
    secret: SECRET,
  });
  const all = ["a", "b", "a-prime"].flatMap(
    (initial, i) =>
      transformer.transform("collections/+users/", {
        _id: `user:01j5zk3v8n2q4x6y8z0b1c3d6${i}`,
        email: `zebulon${i}@acme-reelle.fr`,
        firstname: REAL.firstname,
        initial,
      }).notes,
  );
  assertNoLeak(
    all.map((n) => `${n.path} ${n.message ?? ""}`),
    ["a-prime", "acme-reelle", REAL.firstname],
  );
});

// TODO(privacy): R8, unblocked by the summary echoing the remapped scope, not the source one
test({
  name: "leak R8 (strict cli): the --json summary echoes the source scope id",
  ignore: true,
  timeout: 60_000,
  fn: async () => {
    const tag = crypto.randomUUID().replace(/-/g, "").slice(0, 8);
    const source = `mongodbee_test_leaks_scope_${tag}`;
    const client = new MongoClient(TEST_URI);
    await client.connect();
    const lines: string[] = [];
    const original = { log: console.log, error: console.error };
    try {
      await seedStrictSource(client, source);
      console.log = (...args: unknown[]) =>
        lines.push(args.map(String).join(" "));
      console.error = console.log;
      await withTempDir(async (dir) => {
        await writeStrictProject(dir);
        await extractCommand({
          cwd: dir,
          fromDb: source,
          dryRun: true,
          scope: EXPOSITION_ID,
          json: true,
        });
      });
    } finally {
      console.log = original.log;
      console.error = original.error;
      await client.db(source).dropDatabase();
      await client.close();
    }
    assertNoLeak(lines.join("\n"), [EXPOSITION_ID.split(":")[1]]);
  },
});

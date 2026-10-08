import { Binary, Decimal128, ObjectId } from "mongodb";
import { test } from "../+harness.ts";
import { assertEquals, assertNotEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import {
  buildPrivacyPlan,
  createPrivacyTransformer,
  mirrorOf,
  notPersonal,
  personal,
  personId,
  type PrivacyConsistency,
} from "../../src/privacy/mod.ts";
import { checkScenarioState } from "../../src/scenario/mod.ts";
import { transformState } from "../../src/migration/cli/commands/extract.ts";
import {
  createEmptyDatabaseState,
  type DatabaseState,
  type SchemasDefinition,
} from "../../src/migration/types.ts";

const SECRET = "review2";

function stateWith(
  collections: Record<string, Record<string, unknown>[]>,
): DatabaseState {
  const state = createEmptyDatabaseState();
  for (const [name, content] of Object.entries(collections)) {
    state.collections[name] = { content };
  }
  return state;
}

function run(
  schemas: SchemasDefinition,
  state: DatabaseState,
  consistency?: PrivacyConsistency,
) {
  const plan = buildPrivacyPlan({ schemas, posture: "strict" });
  const transformer = createPrivacyTransformer({
    plan,
    schemas,
    secret: SECRET,
    ...(consistency !== undefined && { consistency }),
  });
  return {
    plan,
    transformer,
    ...transformState(state, plan, transformer, {
      schemas,
      remapInstanceName: transformer.remapId,
    }),
  };
}

const ulid = (n: number) =>
  `01j5zk3v8n2q4x6y8z0b1c3d${String(n).padStart(2, "0")}`;

test({
  name: "V1 a non-unique leaf sharing a space with an exact unique leaf follows the retried fake of its own value",
  fn: () => {
    const email = (unique: boolean) => {
      const s = personal(v.pipe(v.string(), v.email()), {
        role: "direct",
        space: "email",
        consistent: "person",
      });
      return unique ? withIndex(s, { unique: true }) : s;
    };
    const schemas = {
      collections: {
        "+users": { _id: personId("user"), email: email(true) },
        mails: {
          _id: personal(dbId("mail"), { of: "user" }),
          userId: refId("user"),
          to: email(false),
        },
      },
    } as never as SchemasDefinition;
    const users = [
      { _id: `user:${ulid(1)}`, email: "Jean.Dupont@acme.fr" },
      { _id: `user:${ulid(2)}`, email: "jean.dupont@acme.fr" },
    ];
    const mails = users.map((u, i) => ({
      _id: `mail:${ulid(10 + i)}`,
      userId: u._id,
      to: u.email,
    }));
    const { state } = run(schemas, stateWith({ "+users": users, mails }));
    const out = state.collections;
    const fakeOf = new Map(out["+users"].content.map((u) => [u._id, u.email]));
    assertNotEquals(
      fakeOf.get(out.mails.content[0].userId as string),
      fakeOf.get(out.mails.content[1].userId as string),
    );
    for (const mail of out.mails.content) {
      assertEquals(
        mail.to,
        fakeOf.get(mail.userId as string),
        `mail ${mail._id}`,
      );
    }
  },
});

test({
  name: "V2 Date objects inside an untyped payload are shifted like typed dates in strict",
  fn: () => {
    const schemas = {
      collections: {
        "+users": { _id: personId("user"), payload: v.any(), seenAt: v.date() },
      },
    } as never as SchemasDefinition;
    const when = new Date("2026-05-04T13:37:42.123Z");
    const { state, transformer } = run(
      schemas,
      stateWith({
        "+users": [
          { _id: `user:${ulid(1)}`, payload: { seenAt: when }, seenAt: when },
        ],
      }),
    );
    const user = state.collections["+users"].content[0];
    const typed = user.seenAt as Date;
    assertEquals(typed.getTime(), when.getTime() + transformer.timeShiftMs);
    const deep = (user.payload as { seenAt: Date }).seenAt;
    assertNotEquals(
      deep.getTime(),
      when.getTime(),
      "deep date survived verbatim",
    );
  },
});

test({
  name: "V3 numbers inside an untyped payload of a person document are faked like typed numbers in strict",
  fn: () => {
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          payload: v.any(),
          salary: v.number(),
        },
      },
    } as never as SchemasDefinition;
    const { state } = run(
      schemas,
      stateWith({
        "+users": [
          {
            _id: `user:${ulid(1)}`,
            payload: { phone: 33612345678, salary: 48213 },
            salary: 48213,
          },
        ],
      }),
    );
    const user = state.collections["+users"].content[0];
    assertNotEquals(user.salary, 48213);
    const payload = user.payload as { phone: number; salary: number };
    assertNotEquals(payload.salary, 48213, "deep number survived verbatim");
    assertNotEquals(payload.phone, 33612345678, "deep phone survived verbatim");
  },
});

test({
  name: "V4 an ObjectId reference inside an untyped payload is remapped like the _id it points at",
  fn: () => {
    const target = new ObjectId("65f000000000000000000001");
    const schemas = {
      collections: {
        "+users": { _id: personId("user"), payload: v.any() },
        things: { _id: v.instance(ObjectId), label: v.string() },
      },
    } as never as SchemasDefinition;
    const { state, transformer } = run(
      schemas,
      stateWith({
        "+users": [{ _id: `user:${ulid(1)}`, payload: { thing: target } }],
        things: [{ _id: target, label: "x" }],
      }),
    );
    const remapped = state.collections.things.content[0]._id as ObjectId;
    assertNotEquals(remapped.toHexString(), target.toHexString());
    const deep = (
      state.collections["+users"].content[0].payload as { thing: ObjectId }
    ).thing;
    assertEquals(
      String(deep),
      String(remapped),
      `timeShift ${transformer.timeShiftMs}`,
    );
  },
});

test({
  name: "V5 the fakes of a multi-model instance do not depend on the order instances are listed in",
  fn: () => {
    const schemas = {
      collections: { "+users": { _id: personId("user") } },
      multiModels: {
        exposition: {
          badge: {
            _id: personal(dbId("badge"), { of: "user" }),
            userId: refId("user"),
            holder: withIndex(
              personal(v.pipe(v.string(), v.minLength(1)), {
                role: "direct",
                consistent: "person",
              }),
              { unique: true },
            ),
          },
        },
      },
    } as never as SchemasDefinition;
    const userId = `user:${ulid(1)}`;
    const instances: Record<string, string> = {
      [`exposition:${ulid(20)}`]: "Bob",
      [`exposition:${ulid(21)}`]: "bob",
    };
    const extractIn = (order: string[]) => {
      const state = stateWith({ "+users": [{ _id: userId }] });
      for (const name of order) {
        state.multiModels[name] = {
          modelType: "exposition",
          content: [
            {
              _id: `badge:${ulid(30)}`,
              _type: "badge",
              userId,
              holder: instances[name],
            },
          ],
        };
      }
      const out = run(schemas, state).state.multiModels;
      return Object.fromEntries(
        Object.entries(out).map(([name, i]) => [
          name,
          i.content.find((d) => d._type === "badge")?.holder,
        ]),
      );
    };
    const names = Object.keys(instances);
    assertEquals(extractIn([...names].reverse()), extractIn(names));
  },
});

function mirrorViolations(
  schemas: SchemasDefinition,
  state: DatabaseState,
  consistency?: PrivacyConsistency,
) {
  const { plan, state: out } = run(schemas, state, consistency);
  return checkScenarioState({ state: out, schemas, plan }).filter(
    (violation) => violation.kind === "mirror_mismatch",
  );
}

test({
  name: "V9 a mirror in an unscoped collection of a value owned by a multi-model instance copies the source fake",
  fn: () => {
    const schemas = {
      collections: {
        "+users": { _id: personId("user") },
        bookings: {
          _id: personal(dbId("booking"), { of: "user" }),
          userId: refId("user"),
          zoneId: refId("zone"),
          zoneName: mirrorOf(v.string(), "zone.name"),
        },
      },
      multiModels: {
        exposition: {
          zone: {
            _id: refId("zone"),
            name: personal(v.pipe(v.string(), v.minLength(1)), {
              role: "direct",
            }),
          },
        },
      },
    } as never as SchemasDefinition;
    const userId = `user:${ulid(1)}`;
    const zoneId = `zone:${ulid(2)}`;
    const state = stateWith({
      "+users": [{ _id: userId }],
      bookings: [
        { _id: `booking:${ulid(3)}`, userId, zoneId, zoneName: "Hall Dupont" },
      ],
    });
    state.multiModels[`exposition:${ulid(4)}`] = {
      modelType: "exposition",
      content: [{ _id: zoneId, _type: "zone", name: "Hall Dupont" }],
    };
    assertEquals(mirrorViolations(schemas, state), []);
  },
});

test({
  name: "V10 --consistency transaction keeps mirrors aligned with their source",
  fn: () => {
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
        },
        mails: {
          _id: personal(dbId("mail"), { of: "user" }),
          userId: refId("user"),
          to: mirrorOf(v.string(), "user.email"),
        },
      },
    } as never as SchemasDefinition;
    const userId = `user:${ulid(1)}`;
    const state = stateWith({
      "+users": [{ _id: userId, email: "jean@acme.fr" }],
      mails: [{ _id: `mail:${ulid(2)}`, userId, to: "jean@acme.fr" }],
    });
    assertEquals(mirrorViolations(schemas, state, "relationship"), []);
    assertEquals(mirrorViolations(schemas, state, "transaction"), []);
  },
});

test({
  name: "V11 a declared remap of an unprefixed _id (number or bare string) joins the remapped _id it points at",
  fn: () => {
    const remap = <T>(schema: T) =>
      personal(schema as never, {
        role: "technical",
        treatment: { extract: "remap" },
      });
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          counterId: remap(v.number()),
          deviceId: remap(v.string()),
        },
        counters: { _id: v.number(), value: v.number() },
        devices: { _id: v.string(), model: v.picklist(["a", "b"]) },
      },
    } as never as SchemasDefinition;
    const device = "5f0c2a1e-9b7d-4c1a-8e3f-2b6d9a0c4e71";
    const { state } = run(
      schemas,
      stateWith({
        "+users": [
          { _id: `user:${ulid(1)}`, counterId: 4242, deviceId: device },
        ],
        counters: [{ _id: 4242, value: 1 }],
        devices: [{ _id: device, model: "a" }],
      }),
    );
    const user = state.collections["+users"].content[0];
    assertEquals(
      { counterId: user.counterId, deviceId: user.deviceId },
      {
        counterId: state.collections.counters.content[0]._id,
        deviceId: state.collections.devices.content[0]._id,
      },
    );
  },
});

test({
  // TODO(privacy): V12, numericId keeps one registry per target while types share the physical _id index; key the registry by physical collection, and make duplicate_id per collection and blocking
  ignore: true,
  name: "V12 numeric _ids of two types sharing one physical collection stay distinct",
  fn: () => {
    const schemas = {
      collections: { "+users": { _id: personId("user") } },
      multiCollections: {
        events: {
          a: { _id: v.number(), label: v.string() },
          b: { _id: v.number(), label: v.string() },
        },
      },
    } as never as SchemasDefinition;
    const state = stateWith({ "+users": [{ _id: `user:${ulid(1)}` }] });
    state.multiCollections.events = {
      content: [1, 2, 3, 4, 5, 6, 7, 8, 9].map((n) => ({
        _id: n,
        _type: n <= 4 ? "a" : "b",
        label: "x",
      })),
    };
    const out = run(schemas, state).state.multiCollections.events.content;
    const ids = out.map((d) => d._id);
    assertEquals(new Set(ids).size, ids.length, `ids ${ids.join(",")}`);
  },
});

test("guard: a fake depends only on the secret and the source, not on what the process transformed before", () => {
  const schemas = {
    collections: {
      "+users": {
        _id: personId("user"),
        email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
        name: v.string(),
        age: v.number(),
        born: v.date(),
        tags: v.array(v.string()),
        friend: v.optional(refId("user")),
        note: v.optional(v.string()),
      },
    },
  } as never as SchemasDefinition;
  const doc = (n: number) => ({
    _id: `user:${ulid(n)}`,
    email: `u${n}@acme.fr`,
    name: `Name ${n}`,
    age: 30 + n,
    born: new Date(Date.UTC(1980, 0, n)),
    tags: [`t${n}`, `u${n}`],
    note: `note ${n}`,
  });
  const fresh = () => {
    const plan = buildPrivacyPlan({ schemas, posture: "strict" });
    return createPrivacyTransformer({ plan, schemas, secret: SECRET });
  };
  const alone = fresh().transform("collections/+users/", doc(2)).doc;
  const warm = fresh();
  for (let i = 3; i < 40; i++) warm.transform("collections/+users/", doc(i));
  const after = warm.transform("collections/+users/", doc(2)).doc;
  assertEquals(after, alone);
});

test({
  name: "V14 a lowercase mirror of a mixed-case value under a case-sensitive unique index copies the source fake",
  fn: () => {
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          email: withIndex(
            personal(v.pipe(v.string(), v.email()), { role: "direct" }),
            { unique: true },
          ),
          emailLower: mirrorOf(v.string(), "user.email", {
            normalize: "lowercase",
          }),
        },
      },
    } as never as SchemasDefinition;
    const state = stateWith({
      "+users": [
        {
          _id: `user:${ulid(1)}`,
          email: "Jean.Dupont@acme.fr",
          emailLower: "jean.dupont@acme.fr",
        },
      ],
    });
    const { state: out } = run(schemas, state);
    const user = out.collections["+users"].content[0];
    assertEquals(
      { mirror: user.emailLower, violations: mirrorViolations(schemas, state) },
      { mirror: String(user.email).toLowerCase(), violations: [] },
    );
  },
});

test({
  ignore: true,
  name: "V15 a strict-keep configuration document does not keep a field whose schema certainly holds an email",
  fn: () => {
    const schemas = {
      collections: {
        "+users": { _id: personId("user") },
        flows: {
          _id: notPersonal(dbId("flow"), "flow configuration", {
            strict: "keep",
          }),
          notify: v.pipe(v.string(), v.email()),
          label: v.string(),
        },
      },
    } as never as SchemasDefinition;
    const { state, plan } = run(
      schemas,
      stateWith({
        "+users": [{ _id: `user:${ulid(1)}` }],
        flows: [
          {
            _id: `flow:${ulid(2)}`,
            notify: "jean.dupont@acme.fr",
            label: "Relance",
          },
        ],
      }),
    );
    const flow = state.collections.flows.content[0];
    assertEquals(flow.label, "Relance");
    const warned = plan.findings.some((f) =>
      JSON.stringify(f).includes("notify"),
    );
    assertEquals(
      flow.notify !== "jean.dupont@acme.fr" || warned,
      true,
      "certain email kept silently",
    );
  },
});

test({
  name: "V16 under strict a mirror of a string the posture fakes copies the source fake",
  fn: () => {
    const schemas = {
      collections: {
        "+users": { _id: personId("user") },
        expositions: {
          _id: refId("exposition"),
          name: v.pipe(v.string(), v.minLength(1)),
        },
        tickets: {
          _id: personal(dbId("ticket"), { of: "user" }),
          userId: refId("user"),
          expositionId: refId("exposition"),
          expositionName: mirrorOf(v.string(), "exposition.name"),
        },
      },
    } as never as SchemasDefinition;
    const userId = `user:${ulid(1)}`;
    const expositionId = `exposition:${ulid(2)}`;
    const state = stateWith({
      "+users": [{ _id: userId }],
      expositions: [{ _id: expositionId, name: "Salon Dupont 2026" }],
      tickets: [
        {
          _id: `ticket:${ulid(3)}`,
          userId,
          expositionId,
          expositionName: "Salon Dupont 2026",
        },
      ],
    });
    const { state: out } = run(schemas, state);
    assertEquals(
      out.collections.tickets.content[0].expositionName,
      out.collections.expositions.content[0].name,
    );
    assertEquals(mirrorViolations(schemas, state), []);
  },
});

test({
  name: "V13 a scoped collection whose scope is a picklist keeps its _scope inside the picklist",
  fn: () => {
    const schemas = {
      collections: { "+users": { _id: personId("user") } },
      scopedMultiCollections: {
        content: {
          scope: v.picklist(["fr", "en"]),
          types: { page: { _id: refId("page"), title: v.string() } },
        },
      },
    } as never as SchemasDefinition;
    const state = stateWith({ "+users": [{ _id: `user:${ulid(1)}` }] });
    state.scopedMultiCollections.content = {
      content: [
        {
          _id: `page:${ulid(2)}`,
          _type: "page",
          _scope: "fr",
          title: "Accueil",
        },
      ],
    };
    const page = run(schemas, state).state.scopedMultiCollections.content
      .content[0];
    assertEquals(page._scope, "fr");
  },
});

test({
  name: "V17 two unique fields of the same role holding the same value each get a fake valid for their own schema",
  fn: () => {
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          email: withIndex(
            personal(v.pipe(v.string(), v.email()), {
              role: "direct",
              consistent: "person",
            }),
            { unique: true },
          ),
          handle: withIndex(
            personal(v.pipe(v.string(), v.maxLength(8)), {
              role: "direct",
              consistent: "person",
            }),
            { unique: true },
          ),
        },
      },
    } as never as SchemasDefinition;
    const { notes } = createPrivacyTransformer({
      plan: buildPrivacyPlan({ schemas, posture: "strict" }),
      schemas,
      secret: SECRET,
    }).transform("collections/+users/", {
      _id: `user:${ulid(1)}`,
      email: "a@b.fr",
      handle: "a@b.fr",
    });
    assertEquals(
      notes.filter((n) => n.kind === "invalid"),
      [],
    );
  },
});

test({
  name: "V18 binary and decimal values inside an untyped payload do not survive a strict extract",
  fn: () => {
    const schemas = {
      collections: { "+users": { _id: personId("user"), payload: v.any() } },
    } as never as SchemasDefinition;
    const photo = new Binary(Buffer.from("JPEG of Jean Dupont"));
    const iban = Decimal128.fromString("76300040000123456789012345");
    const { state } = run(
      schemas,
      stateWith({
        "+users": [{ _id: `user:${ulid(1)}`, payload: { photo, iban } }],
      }),
    );
    const payload = state.collections["+users"].content[0].payload as Record<
      string,
      unknown
    >;
    assertEquals(
      {
        photo: String(payload.photo) === String(photo),
        iban: String(payload.iban) === String(iban),
      },
      { photo: false, iban: false },
    );
  },
});

test({
  name: "V19 a v.record(v.string()) keyed by ids of a minted space is rekeyed by the remapped ids",
  fn: () => {
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          scoreByUser: v.record(v.string(), v.boolean()),
        },
      },
    } as never as SchemasDefinition;
    const ids = [`user:${ulid(1)}`, `user:${ulid(2)}`];
    const { state, transformer } = run(
      schemas,
      stateWith({
        "+users": ids.map((_id, i) => ({
          _id,
          scoreByUser: { [ids[1 - i]]: true },
        })),
      }),
    );
    const users = state.collections["+users"].content;
    assertEquals(
      users.map((u) => Object.keys(u.scoreByUser as object)),
      [[transformer.remapId(ids[1])], [transformer.remapId(ids[0])]],
    );
  },
});

test({
  name: "V21 a mirror of a sensitive value that the source drops is dropped too, not pseudonymised",
  fn: () => {
    const schemas = {
      collections: {
        "+users": {
          _id: personId("user"),
          diagnosis: v.optional(personal(v.string(), { role: "sensitive" })),
        },
        visits: {
          _id: personal(dbId("visit"), { of: "user" }),
          userId: refId("user"),
          diagnosis: v.optional(mirrorOf(v.string(), "user.diagnosis")),
        },
      },
    } as never as SchemasDefinition;
    const ids = [`user:${ulid(1)}`, `user:${ulid(2)}`];
    const { state } = run(
      schemas,
      stateWith({
        "+users": ids.map((_id) => ({ _id, diagnosis: "diabète de type 2" })),
        visits: ids.map((userId, i) => ({
          _id: `visit:${ulid(10 + i)}`,
          userId,
          diagnosis: "diabète de type 2",
        })),
      }),
    );
    assertEquals(
      {
        users: state.collections["+users"].content.map((u) => u.diagnosis),
        visits: state.collections.visits.content.map((d) => d.diagnosis),
      },
      { users: [undefined, undefined], visits: [undefined, undefined] },
    );
  },
});

test({
  name: "V22 a record keyed by ISO days is rekeyed by the shifted days, like the dates it indexes",
  fn: () => {
    const schemas = {
      collections: {
        "+users": { _id: personId("user") },
        stats: {
          _id: refId("stat"),
          day: v.date(),
          scansByDay: v.record(v.string(), v.number()),
        },
      },
    } as never as SchemasDefinition;
    const day = new Date("2026-05-04T00:00:00.000Z");
    const { state, transformer } = run(
      schemas,
      stateWith({
        "+users": [{ _id: `user:${ulid(1)}` }],
        stats: [
          { _id: `stat:${ulid(2)}`, day, scansByDay: { "2026-05-04": 12 } },
        ],
      }),
    );
    const stat = state.collections.stats.content[0];
    const shifted = new Date(day.getTime() + transformer.timeShiftMs)
      .toISOString()
      .slice(0, 10);
    assertEquals(Object.keys(stat.scansByDay as object), [shifted]);
  },
});

test("V14 the oracle reports a mirror that differs from its source in the same document", () => {
  const schemas = {
    collections: {
      "+users": {
        _id: personId("user"),
        email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
        emailLower: mirrorOf(v.string(), "user.email", {
          normalize: "lowercase",
        }),
      },
    },
  } as never as SchemasDefinition;
  const plan = buildPrivacyPlan({ schemas, posture: "strict" });
  const state = stateWith({
    "+users": [
      {
        _id: `user:${ulid(1)}`,
        email: "Jean@acme.fr",
        emailLower: "paul@acme.fr",
      },
    ],
  });
  assertEquals(
    checkScenarioState({ state, schemas, plan }).map((v) => v.kind),
    ["mirror_mismatch"],
  );
});

test("V1 a non-unique leaf processed before the unique leaf of its space still ends on the same fake", () => {
  const email = (unique: boolean) => {
    const s = personal(v.pipe(v.string(), v.email()), {
      role: "direct",
      space: "email",
      consistent: "person",
    });
    return unique ? withIndex(s, { unique: true }) : s;
  };
  const schemas = {
    collections: {
      mails: {
        _id: personal(dbId("mail"), { of: "user" }),
        userId: refId("user"),
        to: email(false),
      },
      "+users": { _id: personId("user"), email: email(true) },
    },
  } as never as SchemasDefinition;
  const users = [
    "Jean.Dupont@acme.fr",
    "jean.dupont@acme.fr",
    "paul@acme.fr",
  ].map((address, i) => ({ _id: `user:${ulid(i + 1)}`, email: address }));
  const mails = users.map((u, i) => ({
    _id: `mail:${ulid(10 + i)}`,
    userId: u._id,
    to: u.email,
  }));
  const out = run(schemas, stateWith({ "+users": users, mails })).state
    .collections;
  const fakeOf = new Map(out["+users"].content.map((u) => [u._id, u.email]));
  assertEquals(new Set(fakeOf.values()).size, 3);
  for (const mail of out.mails.content) {
    assertEquals(mail.to, fakeOf.get(mail.userId as string));
  }
});

test("V14 a unique lowercase mirror of case-distinct sources stays distinct without the registry", () => {
  const schemas = {
    collections: {
      "+users": {
        _id: personId("user"),
        email: withIndex(
          personal(v.pipe(v.string(), v.email()), { role: "direct" }),
          { unique: true },
        ),
        emailLower: withIndex(
          mirrorOf(v.string(), "user.email", { normalize: "lowercase" }),
          { unique: true },
        ),
      },
    },
  } as never as SchemasDefinition;
  const state = stateWith({
    "+users": ["Bob@acme.fr", "bob@acme.fr"].map((email, i) => ({
      _id: `user:${ulid(i + 1)}`,
      email,
      emailLower: email.toLowerCase(),
    })),
  });
  const { plan, state: out } = run(schemas, state);
  const lowers = out.collections["+users"].content.map((u) => u.emailLower);
  assertEquals(new Set(lowers).size, 2);
  assertEquals(
    checkScenarioState({ state: out, schemas, plan }).map((v) => v.kind),
    [],
  );
});

test("mirrors: a mirror whose source document cannot be found keeps the value-keyed fallback and is counted", () => {
  const schemas = {
    collections: {
      "+users": {
        _id: personId("user"),
        email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
      },
      mails: {
        _id: personal(dbId("mail"), { of: "user" }),
        userId: refId("user"),
        to: mirrorOf(v.string(), "user.email"),
      },
    },
  } as never as SchemasDefinition;
  const { state, summary } = run(
    schemas,
    stateWith({
      "+users": [],
      mails: [
        {
          _id: `mail:${ulid(2)}`,
          userId: `user:${ulid(9)}`,
          to: "jean@acme.fr",
        },
      ],
    }),
  );
  const mail = state.collections.mails.content[0];
  assertNotEquals(mail.to, "jean@acme.fr");
  assertEquals(typeof mail.to, "string");
  assertEquals(summary["collections/mails/"].notes.mirror_unresolved, 1);
});

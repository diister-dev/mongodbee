import { assert, assertEquals, assertRejects } from "@std/assert";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { mention, personal, personId } from "../../src/privacy/mod.ts";
import {
  runScenario,
  type SeedScenario,
  SKIP,
} from "../../src/scenario/mod.ts";
import { isUlid } from "../../src/utils/ulid-encode.ts";

const EmailSchema = personal(v.pipe(v.string(), v.email()), {
  role: "direct",
  consistent: "person",
});
const FirstNameSchema = personal(
  v.pipe(v.string(), v.minLength(2), v.maxLength(30)),
  { role: "direct" },
);

const USERS = {
  _id: personId("user"),
  email: EmailSchema,
  firstname: FirstNameSchema,
  role: v.picklist(["admin", "member"]),
};
const EXPOSITIONS = {
  _id: refId("exposition"),
  name: v.string(),
  createdBy: mention(refId("user")),
};
const PARTICIPANT_V1 = {
  _id: personId("participant", { of: ["user"] }),
  userId: refId("user"),
  kind: v.picklist(["visitor", "exhibitor"]),
  label: v.pipe(v.string(), v.minLength(1)),
};
const SCAN_V1 = {
  _id: personal(dbId("scan_history"), { of: "participant" }),
  participantId: refId("participant"),
  scannedBy: mention(refId("user")),
  label: v.picklist(["security", "business"]),
  at: v.date(),
};

const S1 = {
  collections: { "+users": USERS, expositions: EXPOSITIONS },
  scopedMultiCollections: {
    "+expo": {
      scope: refId("exposition"),
      types: { participant: PARTICIPANT_V1, scan_history: SCAN_V1 },
    },
  },
};
const S2 = {
  ...S1,
  scopedMultiCollections: {
    "+expo": {
      scope: refId("exposition"),
      types: {
        participant: { ...PARTICIPANT_V1, versionId: v.string() },
        scan_history: SCAN_V1,
      },
    },
  },
};
const { label: _l, ...SCAN_V3_REST } = SCAN_V1;
const S3 = {
  ...S2,
  scopedMultiCollections: {
    "+expo": {
      scope: refId("exposition"),
      types: {
        participant: { ...PARTICIPANT_V1, versionId: v.string() },
        scan_history: {
          ...SCAN_V3_REST,
          kind: v.picklist(["security", "business"]),
        },
      },
    },
  },
};

const M1 = migrationDefinition("2026_01_01_0900_BIRTH01@birth", "birth", {
  parent: null,
  schemas: S1,
  migrate: (b) => b.compile(),
});
const M2 = migrationDefinition("2026_02_01_0900_VERSN01@version", "version", {
  parent: M1,
  schemas: S2,
  migrate: (b) =>
    b.scopedMultiCollection("+expo").type("participant").transform({
      up: (doc, ctx) => ({ ...doc, versionId: ctx.newId() }),
      down: (doc) => {
        const { versionId: _v, ...rest } = doc;
        return rest;
      },
    }).end().end().compile(),
});
const M3 = migrationDefinition("2026_03_01_0900_RENAM01@rename", "rename", {
  parent: M2,
  schemas: S3,
  migrate: (b) =>
    b.scopedMultiCollection("+expo").type("scan_history").transform({
      up: (doc) => {
        const { label, ...rest } = doc;
        return { ...rest, kind: label };
      },
      down: (doc) => doc,
      irreversible: true,
    }).end().end().compile(),
});
const CHAIN = [M1, M2, M3];

const ADMIN = "user:01j5zk3v8n2q4x6y8z0b1c3d5e";
const SALON = "exposition:01j5zk0a1b2c3d4e5f6g7h8j9k";

const SALON_SCENARIO: SeedScenario = {
  name: "salon",
  birth: M1.id,
  anchors: {
    collections: {
      "+users": [{
        _id: ADMIN,
        email: "admin@diister.fr",
        firstname: "Admin",
        role: "admin",
      }],
      expositions: [{ _id: SALON, name: "Salon Pro", createdBy: ADMIN }],
    },
  },
  shape: {
    "+users": 20,
    expositions: 3,
    participant: { per: "scope", count: 10 },
    scan_history: { per: "participant", count: 3 },
  },
  rules: {
    scan_history: {
      label: ({ faker }) =>
        faker.helpers.weightedArrayElement([{ weight: 9, value: "business" }, {
          weight: 1,
          value: "security",
        }]),
      at: () => SKIP,
    },
  },
  invariants: [
    ({ docs }) =>
      docs("participant").filter((p) => !p.label).map((p) =>
        `participant ${p._id} has no label`
      ),
  ],
};

Deno.test("scenario: the world at birth has the requested shape, the anchors, and no orphan", async () => {
  const { state, report } = await runScenario({
    migrations: CHAIN,
    scenario: SALON_SCENARIO,
    at: M1.id,
  });
  assertEquals(report.applied, []);
  assertEquals(report.generated["collections/+users/"], 21);
  assertEquals(report.generated["collections/expositions/"], 4);
  assertEquals(
    report.generated["scopedMultiCollections/+expo/participant"],
    40,
  );
  assertEquals(
    report.generated["scopedMultiCollections/+expo/scan_history"],
    120,
  );
  assertEquals(report.violations, []);
  assert(report.ok);

  const users = state.collections["+users"].content;
  assert(users.some((u) => u._id === ADMIN && u.email === "admin@diister.fr"));
  const expositions = new Set(
    state.collections.expositions.content.map((e) => e._id),
  );
  assert(expositions.has(SALON));

  const docs = state.scopedMultiCollections["+expo"].content;
  const participants = docs.filter((d) => d._type === "participant");
  const scans = docs.filter((d) => d._type === "scan_history");
  const participantById = new Map(participants.map((p) => [p._id, p]));
  for (const scan of scans) {
    const parent = participantById.get(scan.participantId as string);
    assert(parent, `scan ${scan._id} points at an unknown participant`);
    assertEquals(scan._scope, parent!._scope);
    assert(expositions.has(scan._scope as string));
  }
  const userIds = new Set(users.map((u) => u._id));
  for (const p of participants) assert(userIds.has(p.userId as string));
  for (const doc of [...users, ...participants, ...scans]) {
    if (doc._id === ADMIN) continue;
    assert(isUlid(String(doc._id).split(":")[1] ?? ""), String(doc._id));
  }
  const business = scans.filter((s) => s.label === "business").length;
  assert(
    business > 90,
    `expected a 9:1 weighting, got ${business}/120 business`,
  );
});

Deno.test("scenario: seeding at a later step replays the migrations on the same world, deterministically", async () => {
  const first = await runScenario({
    migrations: CHAIN,
    scenario: SALON_SCENARIO,
  });
  const second = await runScenario({
    migrations: CHAIN,
    scenario: SALON_SCENARIO,
  });
  assertEquals(first.state, second.state);
  assertEquals(first.report.applied, [M2.id, M3.id]);
  assertEquals(first.report.at, M3.id);
  assert(first.report.ok, JSON.stringify(first.report.violations));

  const docs = first.state.scopedMultiCollections["+expo"].content;
  const participants = docs.filter((d) => d._type === "participant");
  for (const p of participants) {
    assert(isUlid(p.versionId as string), String(p.versionId));
  }
  assertEquals(
    new Set(participants.map((p) => p.versionId)).size,
    participants.length,
  );
  const scans = docs.filter((d) => d._type === "scan_history");
  for (const s of scans) {
    assertEquals(s.label, undefined);
    assert(s.kind === "business" || s.kind === "security");
  }
  assertEquals(
    first.report.generated["scopedMultiCollections/+expo/scan_history"],
    120,
  );
});

Deno.test("scenario: an intermediate step is reachable, and a step before birth is not", async () => {
  const mid = await runScenario({
    migrations: CHAIN,
    scenario: SALON_SCENARIO,
    at: M2.id,
  });
  assertEquals(mid.report.applied, [M2.id]);
  assert(mid.report.ok);
  const late = { ...SALON_SCENARIO, birth: M2.id };
  await assertRejects(
    () => runScenario({ migrations: CHAIN, scenario: late, at: M1.id }),
    Error,
    "precedes the birth",
  );
  await assertRejects(
    () =>
      runScenario({
        migrations: CHAIN,
        scenario: { ...SALON_SCENARIO, shape: { nope: 3 } },
      }),
    Error,
    'unknown target "nope"',
  );
});

Deno.test("scenario: the oracle reports orphan owners and failed invariants", async () => {
  const broken: SeedScenario = {
    ...SALON_SCENARIO,
    name: "broken",
    anchors: {
      ...SALON_SCENARIO.anchors,
      scopedMultiCollections: {
        "+expo": [{
          _id: "scan_history:01j5zk9a1b2c3d4e5f6g7h8j9k",
          _type: "scan_history",
          _scope: SALON,
          participantId: "participant:01j5zk9a1b2c3d4e5f6g7h8j00",
          scannedBy: ADMIN,
          label: "business",
          at: new Date("2026-01-02T10:00:00.000Z"),
        }],
      },
    },
    invariants: [() => ["every world needs at least one violation"]],
  };
  const { report } = await runScenario({
    migrations: CHAIN,
    scenario: broken,
    at: M1.id,
  });
  assert(!report.ok);
  assert(
    report.violations.some((x) =>
      x.kind === "owner_unresolved" && x.message.startsWith("participantId")
    ),
  );
  assert(report.violations.some((x) => x.kind === "invariant"));
});

Deno.test("scenario: types without an _id in their schema still get one minted id per document", async () => {
  const schemas = {
    collections: { "+users": USERS },
    multiCollections: {
      "+auth": {
        auth_password: { userId: refId("user"), hash: v.string() },
        recovery_code: { userId: refId("user"), code: v.string() },
      },
    },
  };
  const M = migrationDefinition("2026_01_01_0900_AUTH01@auth", "auth", {
    parent: null,
    schemas,
    migrate: (b) => b.compile(),
  });
  const { state, report } = await runScenario({
    migrations: [M],
    scenario: {
      name: "auth",
      birth: M.id,
      shape: { "+users": 5, auth_password: 7, recovery_code: 4 },
    },
  });
  assert(report.ok, JSON.stringify(report.violations));
  const docs = state.multiCollections["+auth"].content;
  assertEquals(docs.length, 11);
  const ids = new Set(docs.map((d) => String(d._id)));
  assertEquals(ids.size, 11);
  for (const d of docs) {
    const prefix = String(d._id).split(":")[0];
    assertEquals(prefix, d._type);
    assert(isUlid(String(d._id).split(":")[1] ?? ""));
  }
});

Deno.test("scenario: a global document can reference a space that only exists inside scopes", async () => {
  const schemas = {
    collections: {
      "+users": USERS,
      "+invitation_links": {
        _id: dbId("invitation"),
        flowId: refId("flow"),
        createdBy: mention(refId("user")),
      },
    },
    scopedMultiCollections: {
      "+expo": {
        scope: refId("exposition"),
        types: {
          information: {
            _id: refId("exposition"),
            title: v.pipe(v.string(), v.minLength(1)),
          },
          flow: { name: v.pipe(v.string(), v.minLength(1)) },
        },
      },
    },
  };
  const M = migrationDefinition("2026_01_01_0900_FLOW01@flow", "flow", {
    parent: null,
    schemas,
    migrate: (b) => b.compile(),
  });
  const { state, report } = await runScenario({
    migrations: [M],
    scenario: {
      name: "flows",
      birth: M.id,
      shape: {
        "+users": 3,
        "+invitation_links": 6,
        flow: { per: "scope", count: 2 },
      },
    },
  });
  assert(report.ok, JSON.stringify(report.violations));
  const flows = new Set(
    state.scopedMultiCollections["+expo"].content.filter((d) =>
      d._type === "flow"
    ).map((d) => String(d._id)),
  );
  assert(flows.size >= 2);
  for (const link of state.collections["+invitation_links"].content) {
    assert(
      flows.has(String(link.flowId)),
      `flowId ${link.flowId} must be one of the scoped flows`,
    );
  }
});

Deno.test("scenario: finalize sees the whole document, after sees the whole world, indexes are positional", async () => {
  const schemas = {
    collections: {
      "+users": USERS,
      "+roles": { _id: dbId("role"), name: v.pipe(v.string(), v.minLength(1)) },
      tickets: {
        _id: personal(dbId("ticket"), { of: "user" }),
        userId: refId("user"),
        openedAt: v.date(),
        closedAt: v.optional(v.date()),
        replies: v.pipe(v.number(), v.integer(), v.minValue(0)),
      },
      counters: {
        _id: dbId("counter"),
        tickets: v.pipe(v.number(), v.integer(), v.minValue(0)),
      },
    },
  };
  const M = migrationDefinition("2026_01_01_0900_TICK01@tickets", "tickets", {
    parent: null,
    schemas,
    migrate: (b) => b.compile(),
  });
  const names = ["admin", "organizer", "visitor"];
  const { state, report } = await runScenario({
    migrations: [M],
    scenario: {
      name: "tickets",
      birth: M.id,
      shape: {
        "+users": 4,
        "+roles": 3,
        tickets: { per: "user", count: 3 },
        counters: 1,
      },
      rules: {
        "+roles": { name: ({ index }) => names[index] },
        tickets: { replies: ({ int }) => int(0, 5) },
      },
      finalize: {
        tickets: ({ doc, chance, dateBetween }) => ({
          ...doc,
          closedAt: chance(0.5)
            ? dateBetween(
              doc.openedAt as Date,
              new Date((doc.openedAt as Date).getTime() + 86_400_000),
            )
            : undefined,
        }),
      },
      after: ({ state, docs }) => {
        state.collections.counters.content[0].tickets = docs("tickets").length;
      },
    },
  });
  assert(report.ok, JSON.stringify(report.violations));
  assertEquals(state.collections["+roles"].content.map((r) => r.name), names);
  const tickets = state.collections.tickets.content;
  assertEquals(tickets.length, 12);
  for (const t of tickets) {
    if (t.closedAt !== undefined) {
      assert((t.closedAt as Date).getTime() >= (t.openedAt as Date).getTime());
    }
    assert(Number.isInteger(t.replies) && (t.replies as number) <= 5);
  }
  assertEquals(state.collections.counters.content[0].tickets, 12);
});

Deno.test("scenario: scopes realized by the singleton are ulids too", async () => {
  const schemas = {
    scopedMultiCollections: {
      "+expo": {
        scope: refId("exposition"),
        types: {
          information: {
            _id: refId("exposition"),
            title: v.pipe(v.string(), v.minLength(1)),
          },
        },
      },
    },
  };
  const M = migrationDefinition("2026_01_01_0900_SCOPE01@scope", "scope", {
    parent: null,
    schemas,
    migrate: (b) => b.compile(),
  });
  const { state } = await runScenario({
    migrations: [M],
    scenario: { name: "scopes", birth: M.id, shape: { information: 4 } },
  });
  const infos = state.scopedMultiCollections["+expo"].content;
  assertEquals(infos.length, 4);
  for (const info of infos) {
    assertEquals(info._id, info._scope);
    assert(isUlid(String(info._id).split(":")[1] ?? ""), String(info._id));
  }
});

Deno.test("scenario: ordinal counts across batches, singletons come first, undefined never survives finalize", async () => {
  const schemas = {
    scopedMultiCollections: {
      "+expo": {
        scope: refId("exposition"),
        types: {
          information: {
            _id: refId("exposition"),
            title: v.pipe(v.string(), v.minLength(1)),
            startsAt: v.date(),
          },
          program: {
            title: v.pipe(v.string(), v.minLength(1)),
            startsAt: v.date(),
            note: v.optional(v.string()),
          },
        },
      },
    },
  };
  const M = migrationDefinition("2026_01_01_0900_ORD01@ordinal", "ordinal", {
    parent: null,
    schemas,
    migrate: (b) => b.compile(),
  });
  const titles = ["Paris", "Lyon", "Nantes"];
  const { state, report } = await runScenario({
    migrations: [M],
    scenario: {
      name: "ordinal",
      birth: M.id,
      shape: { information: 3, program: { per: "scope", count: 2 } },
      rules: { information: { title: ({ ordinal }) => titles[ordinal] } },
      finalize: {
        program: ({ doc, docs, scope, index }) => {
          const info = docs("information").find((i) => i._scope === scope)!;
          return {
            ...doc,
            title: `${info.title} ${index}`,
            startsAt: info.startsAt,
            note: undefined,
          };
        },
      },
    },
  });
  assert(report.ok, JSON.stringify(report.violations));
  const docs = state.scopedMultiCollections["+expo"].content;
  assertEquals(
    docs.filter((d) => d._type === "information").map((d) => d.title).sort(),
    [...titles].sort(),
  );
  const programs = docs.filter((d) => d._type === "program");
  assertEquals(programs.length, 6);
  for (const p of programs) {
    assert(titles.some((t) => String(p.title).startsWith(t)), String(p.title));
    assert(!("note" in p), "undefined keys must be stripped");
  }
});

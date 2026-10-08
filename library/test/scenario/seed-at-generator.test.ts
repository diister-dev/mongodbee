import { test } from "../+harness.ts";
import { assert, assertEquals, assertRejects } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import {
  migrationDefinition,
  type SchemasDefinition,
} from "../../src/migration/mod.ts";
import { personal, personId } from "../../src/privacy/mod.ts";
import {
  runScenario,
  type SeedScenario,
  SKIP,
} from "../../src/scenario/mod.ts";

function birth(schemas: SchemasDefinition) {
  return migrationDefinition("2026_01_01_0900_GEN01@gen", "gen", {
    parent: null,
    schemas,
    migrate: (b) => b.compile(),
  });
}

const USERS = {
  _id: personId("user"),
  firstname: personal(v.pipe(v.string(), v.minLength(2)), { role: "direct" }),
};

const EXPO = "exposition:01j5zk0a1b2c3d4e5f6g7h8j9k";
const OTHER = "exposition:01j5zk0a1b2c3d4e5f6g7h8j9m";
const USER = "user:01j5zk3v8n2q4x6y8z0b1c3d5e";

const SCOPED = birth({
  collections: { "+users": USERS },
  scopedMultiCollections: {
    "+expo": {
      scope: refId("exposition"),
      types: {
        information: { _id: refId("exposition"), title: v.string() },
        participant: {
          _id: personId("participant", { of: ["user"] }),
          userId: refId("user"),
        },
        scan: {
          _id: dbId("scan"),
          participantId: refId("participant"),
          sourceId: v.optional(
            withIndex(refId("participant"), { global: true }),
          ),
        },
      },
    },
  },
});

function scopedAnchors(scanScope: string, sourceScope?: string) {
  const participant = (scope: string, id: string) => ({
    _id: id,
    _type: "participant",
    _scope: scope,
    userId: USER,
  });
  return {
    collections: { "+users": [{ _id: USER, firstname: "Ada" }] },
    scopedMultiCollections: {
      "+expo": [
        { _id: EXPO, _type: "information", _scope: EXPO, title: "A" },
        { _id: OTHER, _type: "information", _scope: OTHER, title: "B" },
        participant(EXPO, "participant:01j5zk9a1b2c3d4e5f6g7h8j01"),
        participant(OTHER, "participant:01j5zk9a1b2c3d4e5f6g7h8j02"),
        {
          _id: "scan:01j5zk9a1b2c3d4e5f6g7h8j03",
          _type: "scan",
          _scope: scanScope,
          participantId: "participant:01j5zk9a1b2c3d4e5f6g7h8j01",
          ...(sourceScope !== undefined && {
            sourceId:
              sourceScope === EXPO
                ? "participant:01j5zk9a1b2c3d4e5f6g7h8j01"
                : "participant:01j5zk9a1b2c3d4e5f6g7h8j02",
          }),
        },
      ],
    },
  };
}

test("seed-at 1: the oracle reports a scoped reference into another scope", async () => {
  const { report } = await runScenario({
    migrations: [SCOPED],
    scenario: {
      name: "cross-scope",
      birth: SCOPED.id,
      anchors: scopedAnchors(OTHER),
    },
  });
  const cross = report.violations.filter(
    (x) => x.kind === "cross_scope_reference",
  );
  assertEquals(
    cross.map((x) => [x.target, x.count]),
    [["scopedMultiCollections/+expo/scan", 1]],
  );
  assert(!report.ok);
});

test("seed-at 1: a reference whose index spans scopes may cross them", async () => {
  const { report } = await runScenario({
    migrations: [SCOPED],
    scenario: {
      name: "global-index",
      birth: SCOPED.id,
      anchors: scopedAnchors(EXPO, OTHER),
    },
  });
  assertEquals(report.violations, []);
});

const MODELS = birth({
  collections: {
    "+users": USERS,
    expositions: { _id: refId("exposition"), name: v.string() },
  },
  multiModels: {
    exposition: {
      badge: { _id: dbId("badge"), owner: refId("user") },
      scan: { _id: dbId("scan"), badgeId: refId("badge") },
    },
  },
});

test("seed-at 1: the oracle reports a multi-model reference into another instance", async () => {
  const { report } = await runScenario({
    migrations: [MODELS],
    scenario: {
      name: "cross-instance",
      birth: MODELS.id,
      anchors: {
        collections: {
          "+users": [{ _id: USER, firstname: "Ada" }],
          expositions: [
            { _id: EXPO, name: "A" },
            { _id: OTHER, name: "B" },
          ],
        },
        multiModels: {
          [EXPO]: {
            modelType: "exposition",
            content: [
              {
                _id: "badge:01j5zk9a1b2c3d4e5f6g7h8j01",
                _type: "badge",
                owner: USER,
              },
            ],
          },
          [OTHER]: {
            modelType: "exposition",
            content: [
              {
                _id: "scan:01j5zk9a1b2c3d4e5f6g7h8j02",
                _type: "scan",
                badgeId: "badge:01j5zk9a1b2c3d4e5f6g7h8j01",
              },
            ],
          },
        },
      },
    },
  });
  assertEquals(
    report.violations.map((x) => [x.kind, x.target]),
    [["cross_scope_reference", "multiModels/exposition/scan"]],
  );
});

test("seed-at 1: world.pick stays in the current scope unless asked to cross", async () => {
  const crossed: boolean[] = [];
  const { state, report } = await runScenario({
    migrations: [SCOPED],
    scenario: {
      name: "pick-scope",
      birth: SCOPED.id,
      shape: {
        "+users": 4,
        information: 3,
        participant: { per: "scope", count: 3 },
        scan: { per: "scope", count: 4 },
      },
      rules: {
        scan: {
          participantId: ({ pick }) => pick("participant")!._id,
          sourceId: ({ pick, scope }) => {
            const other = pick("participant", (p) => p._scope !== scope, {
              acrossScopes: true,
            });
            crossed.push(other !== undefined);
            return other?._id ?? SKIP;
          },
        },
      },
    },
  });
  assert(report.ok, JSON.stringify(report.violations));
  const docs = state.scopedMultiCollections["+expo"].content;
  const scopeOf = new Map(
    docs.filter((d) => d._type === "participant").map((p) => [p._id, p._scope]),
  );
  for (const scan of docs.filter((d) => d._type === "scan")) {
    assertEquals(scopeOf.get(scan.participantId), scan._scope);
  }
  assert(crossed.length > 0 && crossed.every(Boolean));
});

const OWNED = birth({
  collections: {
    "+users": USERS,
    notes: {
      _id: personal(dbId("note"), { of: "user" }),
      owner: v.object({ userId: refId("user") }),
      text: v.string(),
    },
    drafts: {
      _id: dbId("draft"),
      userId: v.optional(refId("user")),
    },
    loose: { _id: dbId("loose"), text: v.string() },
  },
});

test("seed-at 2: per links a nested owner path and an optional reference to the parent", async () => {
  const { state, report } = await runScenario({
    migrations: [OWNED],
    scenario: {
      name: "per-nested",
      birth: OWNED.id,
      shape: {
        "+users": 5,
        notes: { per: "user", count: 2 },
        drafts: { per: "user", count: 3 },
      },
    },
  });
  assert(report.ok, JSON.stringify(report.violations));
  const users = state.collections["+users"].content.map((u) => u._id);
  const count = (
    docs: Record<string, unknown>[],
    read: (d: any) => unknown,
  ) => {
    const by = new Map<unknown, number>();
    for (const d of docs) by.set(read(d), (by.get(read(d)) ?? 0) + 1);
    return users.map((u) => by.get(u) ?? 0);
  };
  assertEquals(
    count(state.collections.notes.content, (d) => d.owner.userId),
    users.map(() => 2),
  );
  assertEquals(
    count(state.collections.drafts.content, (d) => d.userId),
    users.map(() => 3),
  );
});

test("seed-at 2: per without a reference path to the parent is an error", async () => {
  await assertRejects(
    () =>
      runScenario({
        migrations: [OWNED],
        scenario: {
          name: "per-unlinked",
          birth: OWNED.id,
          shape: { "+users": 2, loose: { per: "user", count: 1 } },
        },
      }),
    Error,
    "no reference path",
  );
});

test("seed-at 2: a rule on the per path must return the parent's id", async () => {
  const agreeing = await runScenario({
    migrations: [OWNED],
    scenario: {
      name: "per-rule-ok",
      birth: OWNED.id,
      shape: { "+users": 2, drafts: { per: "user", count: 1 } },
      rules: { drafts: { userId: ({ parent }) => parent!._id } },
    },
  });
  assert(agreeing.report.ok, JSON.stringify(agreeing.report.violations));
  const conflicting = await runScenario({
    migrations: [OWNED],
    scenario: {
      name: "per-rule-conflict",
      birth: OWNED.id,
      shape: { "+users": 2, drafts: { per: "user", count: 1 } },
      rules: { drafts: { userId: () => USER } },
    },
  });
  assert(!conflicting.report.ok);
  assert(
    conflicting.report.violations.some(
      (x) => x.kind === "generation" && x.message.includes("userId"),
    ),
    JSON.stringify(conflicting.report.violations),
  );
});

const SIBLINGS = birth({
  collections: {
    "+users": USERS,
    expositions: { _id: refId("exposition"), name: v.string() },
    participants: {
      _id: personId("participant", { of: ["user"] }),
      userId: refId("user"),
      expositionId: refId("exposition"),
    },
    scans: {
      _id: personal(dbId("scan"), { of: "participant" }),
      participantId: refId("participant"),
      expositionId: refId("exposition"),
    },
  },
});

test("seed-at 2: a per child inherits the parent's reference to the same space", async () => {
  const { state, report } = await runScenario({
    migrations: [SIBLINGS],
    scenario: {
      name: "siblings",
      birth: SIBLINGS.id,
      shape: {
        "+users": 3,
        expositions: 4,
        participants: 6,
        scans: { per: "participants", count: 3 },
      },
    },
  });
  assert(report.ok, JSON.stringify(report.violations));
  const byId = new Map(
    state.collections.participants.content.map((p) => [p._id, p]),
  );
  for (const scan of state.collections.scans.content) {
    assertEquals(scan.expositionId, byId.get(scan.participantId)!.expositionId);
  }
});

test("seed-at 3: a rule on a path the schema does not declare is rejected", async () => {
  await assertRejects(
    () =>
      runScenario({
        migrations: [OWNED],
        scenario: {
          name: "bad-rule",
          birth: OWNED.id,
          shape: { "+users": 1, notes: 1 },
          rules: { notes: { "owner.userid": () => USER } },
        },
      }),
    Error,
    '"owner.userid"',
  );
});

test("seed-at 4: finalize sees the minted _id of a type that does not declare one", async () => {
  const M = birth({
    collections: { "+users": USERS },
    multiCollections: {
      "+auth": { password: { userId: refId("user"), hash: v.string() } },
    },
  });
  const seen: unknown[] = [];
  const { state, report } = await runScenario({
    migrations: [M],
    scenario: {
      name: "finalize-id",
      birth: M.id,
      shape: { "+users": 2, password: { per: "user", count: 1 } },
      finalize: {
        password: ({ doc }) => {
          seen.push(doc._id);
          return { ...doc, hash: `hash-of-${doc._id}` };
        },
      },
    },
  });
  assert(report.ok, JSON.stringify(report.violations));
  assertEquals(seen.length, 2);
  for (const doc of state.multiCollections["+auth"].content) {
    assertEquals(doc.hash, `hash-of-${doc._id}`);
  }
});

test("seed-at 5: a deferred finalize of the scope singleton sees the documents generated after it", async () => {
  const { state, report } = await runScenario({
    migrations: [SCOPED],
    scenario: {
      name: "deferred",
      birth: SCOPED.id,
      shape: {
        "+users": 3,
        information: 2,
        participant: { per: "scope", count: 2 },
      },
      finalize: {
        information: {
          deferred: true,
          run: ({ doc, docs }) => ({
            ...doc,
            title: `${docs("participant").length} participants, ${docs("+users").length} users`,
          }),
        },
      },
    },
  });
  assert(report.ok, JSON.stringify(report.violations));
  const infos = state.scopedMultiCollections["+expo"].content.filter(
    (d) => d._type === "information",
  );
  assertEquals(
    infos.map((i) => i.title),
    ["2 participants, 3 users", "2 participants, 3 users"],
  );
  for (const info of infos) assertEquals(info._id, info._scope);
});

test("seed-at 6: finalize and after may be async and stay deterministic", async () => {
  const scenario: SeedScenario = {
    name: "async",
    birth: OWNED.id,
    shape: { "+users": 3, loose: 2 },
    finalize: {
      loose: async ({ doc, int }) => {
        await new Promise((r) => setTimeout(r, 1));
        return { ...doc, text: `t${int(0, 1000)}` };
      },
    },
    after: async ({ state }) => {
      await Promise.resolve();
      state.collections.loose.content[0].text = "first";
    },
  };
  const first = await runScenario({ migrations: [OWNED], scenario });
  const second = await runScenario({ migrations: [OWNED], scenario });
  assert(first.report.ok, JSON.stringify(first.report.violations));
  assertEquals(first.state, second.state);
  const texts = first.state.collections.loose.content.map((d) => d.text);
  assertEquals(texts[0], "first");
  assert(/^t\d+$/.test(String(texts[1])), String(texts[1]));
});

test("seed-at 7: only named targets and what their references need are generated", async () => {
  const { state, report } = await runScenario({
    migrations: [SIBLINGS],
    scenario: { name: "defaults", birth: SIBLINGS.id, shape: { scans: 2 } },
  });
  assert(report.ok, JSON.stringify(report.violations));
  assertEquals(state.collections.scans.content.length, 2);
  assertEquals(report.onDemand, {
    "collections/+users/": 1,
    "collections/expositions/": 1,
    "collections/participants/": 1,
  });
  const quiet = await runScenario({
    migrations: [OWNED],
    scenario: { name: "quiet", birth: OWNED.id, shape: { loose: 2 } },
  });
  assertEquals(quiet.report.generated, { "collections/loose/": 2 });
  assertEquals(quiet.report.onDemand, {});
  const counted = await runScenario({
    migrations: [OWNED],
    scenario: {
      name: "counted",
      birth: OWNED.id,
      defaultCount: 2,
      shape: { loose: 1 },
    },
  });
  assertEquals(counted.report.generated["collections/loose/"], 1);
  assertEquals(counted.report.generated["collections/drafts/"], 2);
});

test("seed-at 8: a draft reference finalize removed does not block the oracle", async () => {
  const M = birth({
    scopedMultiCollections: {
      "+expo": {
        scope: refId("exposition"),
        types: {
          information: { _id: refId("exposition"), title: v.string() },
          participant: { _id: dbId("participant"), name: v.string() },
          badge: {
            _id: dbId("badge"),
            participantId: v.optional(refId("participant")),
          },
        },
      },
    },
  });
  const { report } = await runScenario({
    migrations: [M],
    scenario: {
      name: "final-docs",
      birth: M.id,
      shape: {
        information: 2,
        participant: 0,
        badge: { per: "scope", count: 6 },
      },
      rules: { badge: { participantId: () => SKIP } },
      finalize: {
        badge: ({ doc }) => {
          const { participantId: _p, ...rest } = doc;
          return rest;
        },
      },
    },
  });
  assert(report.ok, JSON.stringify(report.violations));
});

test("seed-at 10: finalize output with an unknown key is rejected with its path", async () => {
  const { report } = await runScenario({
    migrations: [OWNED],
    scenario: {
      name: "strict",
      birth: OWNED.id,
      shape: { "+users": 1, notes: 1 },
      finalize: {
        notes: ({ doc }) => ({
          ...doc,
          owner: { ...(doc.owner as object), extra: 1 },
        }),
      },
    },
  });
  assert(!report.ok);
  assert(
    report.violations.some(
      (x) => x.kind === "generation" && x.message.includes("owner.extra"),
    ),
    JSON.stringify(report.violations),
  );
});

const NOTIF_S1 = {
  collections: { "+users": USERS },
  scopedMultiCollections: {
    "+expo": {
      scope: refId("exposition"),
      types: {
        information: { _id: refId("exposition"), title: v.string() },
        participant: {
          _id: personId("participant", { of: ["user"] }),
          userId: refId("user"),
        },
      },
    },
  },
};
const NOTIF_S2 = {
  ...NOTIF_S1,
  scopedMultiCollections: {
    ...NOTIF_S1.scopedMultiCollections,
    "+notifications": {
      scope: refId("exposition"),
      types: {
        notification: {
          _id: personal(dbId("notification"), { of: "participant" }),
          participantId: refId("participant"),
          text: v.pipe(v.string(), v.minLength(1)),
        },
      },
    },
  },
};
const N1 = migrationDefinition("2026_01_01_0900_NOTIF00@base", "base", {
  parent: null,
  schemas: NOTIF_S1,
  migrate: (b) => b.compile(),
});
const N2 = migrationDefinition("2026_02_01_0900_NOTIF01@notif", "notif", {
  parent: N1,
  schemas: NOTIF_S2,
  migrate: (b) =>
    b.createScopedMultiCollection("+notifications").end().compile(),
});
const STAGED: SeedScenario = {
  name: "staged",
  birth: N1.id,
  shape: {
    "+users": 4,
    information: 2,
    participant: { per: "scope", count: 3 },
  },
  stages: {
    [N2.id]: {
      shape: { notification: { per: "participant", count: 2 } },
      rules: { notification: { text: ({ ordinal }) => `hello ${ordinal}` } },
    },
  },
};

test("seed-at 9: a stage populates a collection born after the scenario", async () => {
  const late = await runScenario({
    migrations: [N1, N2],
    scenario: STAGED,
    at: N2.id,
  });
  assert(late.report.ok, JSON.stringify(late.report.violations));
  const expo = late.state.scopedMultiCollections["+expo"].content;
  const participants = expo.filter((d) => d._type === "participant");
  assertEquals(participants.length, 6);
  assertEquals(expo.filter((d) => d._type === "information").length, 2);
  const notifications =
    late.state.scopedMultiCollections["+notifications"].content;
  assertEquals(notifications.length, 12);
  const scopeOf = new Map(participants.map((p) => [p._id, p._scope]));
  for (const n of notifications) {
    assertEquals(scopeOf.get(n.participantId), n._scope);
  }
  const early = await runScenario({
    migrations: [N1, N2],
    scenario: STAGED,
    at: N1.id,
  });
  assert(early.report.ok, JSON.stringify(early.report.violations));
  assertEquals(early.state.scopedMultiCollections["+notifications"], undefined);
  assertEquals(
    early.state.scopedMultiCollections["+expo"].content.length,
    expo.length,
  );
});

test("seed-at 9: a stage must name a migration after the birth", async () => {
  await assertRejects(
    () =>
      runScenario({
        migrations: [N1, N2],
        scenario: { ...STAGED, stages: { [N1.id]: { shape: {} } } },
      }),
    Error,
    "after the birth",
  );
  await assertRejects(
    () =>
      runScenario({
        migrations: [N1, N2],
        scenario: { ...STAGED, stages: { nope: { shape: {} } } },
      }),
    Error,
    "not in the chain",
  );
});

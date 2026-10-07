import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import { withIndex } from "../../src/indexes.ts";
import {
  migrationDefinition,
  type SchemasDefinition,
} from "../../src/migration/mod.ts";
import { mirrorOf, personal, personId } from "../../src/privacy/mod.ts";
import { runScenario, type SeedScenario } from "../../src/scenario/mod.ts";

function birth(schemas: SchemasDefinition) {
  return migrationDefinition("2026_01_01_0900_COHER01@coherence", "coherence", {
    parent: null,
    schemas,
    migrate: (b) => b.compile(),
  });
}

const USERS = {
  _id: personId("user"),
  email: personal(v.pipe(v.string(), v.email()), {
    role: "direct",
    consistent: "person",
  }),
};

const MIRRORED = birth({
  collections: {
    "+users": USERS,
    participants: {
      _id: personId("participant", { of: ["user"] }),
      userId: refId("user"),
      email: mirrorOf(v.pipe(v.string(), v.email()), "user.email", {
        normalize: "lowercase",
      }),
    },
  },
});

test("scenario world: a mirrored field carries its source's value, normalised", async () => {
  const { state, report } = await runScenario({
    migrations: [MIRRORED],
    scenario: {
      name: "mirror",
      birth: MIRRORED.id,
      shape: { "+users": 6, participants: 12 },
    },
  });
  assertEquals(report.violations, []);
  const emails = new Map(
    state.collections["+users"].content.map((u) => [u._id, u.email]),
  );
  const participants = state.collections.participants.content;
  assertEquals(participants.length, 12);
  for (const p of participants) {
    assertEquals(p.email, String(emails.get(p.userId as string)).toLowerCase());
  }
});

test("scenario oracle: a mirrored field that drifted from its source is reported", async () => {
  const scenario: SeedScenario = {
    name: "mirror-drift",
    birth: MIRRORED.id,
    anchors: {
      collections: {
        "+users": [
          {
            _id: "user:01j5zk3v8n2q4x6y8z0b1c3d5e",
            email: "real@diister.fr",
          },
        ],
        participants: [
          {
            _id: "participant:01j5zk3v8n2q4x6y8z0b1c3d5f",
            userId: "user:01j5zk3v8n2q4x6y8z0b1c3d5e",
            email: "someone-else@diister.fr",
          },
        ],
      },
    },
    shape: { "+users": 0, participants: 0 },
  };
  const { report } = await runScenario({ migrations: [MIRRORED], scenario });
  assert(!report.ok);
  assertEquals(
    report.violations.map((x) => [x.kind, x.target]),
    [["mirror_mismatch", "collections/participants/"]],
  );
});

const LINKS = birth({
  collections: {
    "+users": USERS,
    links: {
      _id: dbId("link"),
      userId: withIndex(refId("user"), { unique: true }),
      handle: withIndex(v.pipe(v.string(), v.minLength(1)), {
        unique: true,
        insensitive: true,
      }),
    },
  },
});

test("scenario world: a unique index is satisfied whenever the world can satisfy it", async () => {
  const { state, report } = await runScenario({
    migrations: [LINKS],
    scenario: {
      name: "unique",
      birth: LINKS.id,
      shape: { "+users": 12, links: 12 },
    },
  });
  assertEquals(report.violations, []);
  const links = state.collections.links.content;
  assertEquals(new Set(links.map((l) => l.userId)).size, 12);
  assertEquals(
    new Set(links.map((l) => String(l.handle).toLowerCase())).size,
    12,
  );
});

test("scenario oracle: an unsatisfiable unique index is reported, not silently written", async () => {
  const { report } = await runScenario({
    migrations: [LINKS],
    scenario: {
      name: "unique-impossible",
      birth: LINKS.id,
      shape: { "+users": 3, links: 10 },
    },
  });
  assert(!report.ok);
  const violation = report.violations.find((x) => x.kind === "unique_index");
  assertEquals(violation?.target, "collections/links/");
  assertEquals(violation?.count, 7);
});

const UNIQUE_PER_SCOPE = birth({
  scopedMultiCollections: {
    "+expo": {
      scope: refId("exposition"),
      types: {
        information: { _id: refId("exposition"), title: v.string() },
        badge: {
          code: withIndex(v.picklist(["A", "B", "C"]), { unique: true }),
        },
        slug: {
          value: withIndex(v.pipe(v.string(), v.minLength(1)), {
            unique: true,
            global: true,
          }),
        },
      },
    },
  },
});

test("scenario world: a scoped unique index is per scope, a global one spans every scope", async () => {
  const { state, report } = await runScenario({
    migrations: [UNIQUE_PER_SCOPE],
    scenario: {
      name: "unique-scoped",
      birth: UNIQUE_PER_SCOPE.id,
      shape: {
        information: 4,
        badge: { per: "scope", count: 3 },
        slug: { per: "scope", count: 5 },
      },
    },
  });
  assertEquals(report.violations, []);
  const docs = state.scopedMultiCollections["+expo"].content;
  assertEquals(docs.filter((d) => d._type === "badge").length, 12);
  const slugs = docs.filter((d) => d._type === "slug").map((d) => d.value);
  assertEquals(slugs.length, 20);
  assertEquals(new Set(slugs).size, 20);
});

const MODEL = birth({
  collections: {
    "+users": USERS,
    expositions: { _id: refId("exposition"), name: v.string() },
  },
  multiModels: {
    exposition: {
      badge: {
        _id: dbId("badge"),
        owner: refId("user"),
        label: withIndex(v.pipe(v.string(), v.minLength(1)), { unique: true }),
      },
      scan: {
        _id: dbId("scan"),
        badgeId: refId("badge"),
        expositionId: refId("exposition"),
      },
    },
  },
});

test("scenario world: every exposition gets a multi-model instance, filled and self-contained", async () => {
  const { state, report } = await runScenario({
    migrations: [MODEL],
    scenario: {
      name: "model",
      birth: MODEL.id,
      shape: { "+users": 4, expositions: 3, badge: 5, scan: 7 },
    },
  });
  assertEquals(report.violations, []);
  const expositionIds = state.collections.expositions.content
    .map((e) => String(e._id))
    .sort();
  assertEquals(Object.keys(state.multiModels).sort(), expositionIds);
  assertEquals(report.generated["multiModels/exposition/badge"], 15);
  assertEquals(report.generated["multiModels/exposition/scan"], 21);

  const userIds = new Set(
    state.collections["+users"].content.map((u) => u._id),
  );
  for (const [name, instance] of Object.entries(state.multiModels)) {
    assertEquals(instance.modelType, "exposition");
    const badges = instance.content.filter((d) => d._type === "badge");
    const scans = instance.content.filter((d) => d._type === "scan");
    assertEquals([badges.length, scans.length], [5, 7]);
    const badgeIds = new Set(badges.map((b) => b._id));
    for (const badge of badges) assert(userIds.has(badge.owner));
    for (const scan of scans) {
      assert(badgeIds.has(scan.badgeId), "a scan points inside its instance");
      assertEquals(scan.expositionId, name);
    }
    for (const doc of instance.content) assertEquals(doc._scope, undefined);
  }
});

test("scenario world: anchored multi-model instances are kept and completed, the same seed gives the same world", async () => {
  const scenario: SeedScenario = {
    name: "model-anchored",
    birth: MODEL.id,
    anchors: {
      collections: {
        expositions: [
          {
            _id: "exposition:01j5zk0a1b2c3d4e5f6g7h8j9k",
            name: "Salon",
          },
        ],
      },
      multiModels: {
        "exposition:01j5zk0a1b2c3d4e5f6g7h8j9k": {
          modelType: "exposition",
          content: [],
        },
      },
    },
    shape: { "+users": 2, expositions: 1, badge: 2, scan: 1 },
  };
  const first = await runScenario({ migrations: [MODEL], scenario });
  const second = await runScenario({ migrations: [MODEL], scenario });
  assertEquals(first.state, second.state);
  assertEquals(first.report.violations, []);
  assertEquals(Object.keys(first.state.multiModels).length, 2);
  assertEquals(
    first.state.multiModels["exposition:01j5zk0a1b2c3d4e5f6g7h8j9k"].content
      .length,
    3,
  );
});

const LINKS_V2 = migrationDefinition(
  "2026_02_01_0900_COLLAPSE1@collapse",
  "collapse",
  {
    parent: LINKS,
    schemas: LINKS.schemas,
    migrate: (b) =>
      b
        .collection("links")
        .transform({
          up: (doc) => ({
            ...doc,
            handle: "same",
            userId: "user:01j5zk3v8n2q4x6y8z0b1c3d5e",
          }),
          down: (doc) => doc,
          irreversible: true,
        })
        .end()
        .compile(),
  },
);

test("scenario oracle: a replayed migration that breaks uniqueness or an owner reference is caught at the target step", async () => {
  const { report } = await runScenario({
    migrations: [LINKS, LINKS_V2],
    scenario: {
      name: "collapse",
      birth: LINKS.id,
      shape: { "+users": 4, links: 4 },
    },
  });
  assert(!report.ok);
  const kinds = report.violations.map((x) => x.kind).sort();
  assertEquals(kinds, ["owner_unresolved", "unique_index"]);
});

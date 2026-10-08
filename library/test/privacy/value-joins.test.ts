import { test } from "../+harness.ts";
import { assertEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { dbId, refId } from "../../src/ids.ts";
import {
  buildPrivacyPlan,
  detectValueJoins,
  notPersonal,
  personal,
  personId,
} from "../../src/privacy/mod.ts";
import {
  createEmptyDatabaseState,
  type SchemasDefinition,
} from "../../src/migration/types.ts";

const SCHEMAS = {
  collections: {
    users: {
      _id: personId("user"),
      email: personal(v.string(), { role: "direct" }),
    },
    roles: {
      _id: dbId("role"),
      key: notPersonal(v.string(), "role vocabulary"),
    },
    members: {
      _id: personal(dbId("member"), { of: "user" }),
      userId: refId("user"),
      role: v.string(),
      fullName: v.string(),
    },
  },
} as unknown as SchemasDefinition;

const ROLE_KEYS = ["owner", "organiser", "staff", "guest", "billing"];
const NAMES = [
  "Zebulon Quixotique",
  "Anne-Sophie Delacroix",
  "Bertrand Moulinier",
  "Colette Ravenel",
  "Dominique Aubrac",
  "Eloi Fontanel",
];

function world(roleOf: (i: number) => string, nameOf: (i: number) => string) {
  const state = createEmptyDatabaseState();
  state.collections.users = {
    content: [{ _id: "user:01j5zk3v8n2q4x6y8z0b1c3d5e", email: "a@b.fr" }],
  };
  state.collections.roles = {
    content: ROLE_KEYS.map((key, i) => ({ _id: `role:0${i}`, key })),
  };
  state.collections.members = {
    content: Array.from({ length: 12 }, (_, i) => ({
      _id: `member:0${i}`,
      userId: "user:01j5zk3v8n2q4x6y8z0b1c3d5e",
      role: roleOf(i),
      fullName: nameOf(i),
    })),
  };
  return state;
}

test("value joins: an undeclared string whose values are the keys of a kept path is reported, never its values", () => {
  const plan = buildPrivacyPlan({ schemas: SCHEMAS, posture: "strict" });
  const joins = detectValueJoins(
    world(
      (i) => ROLE_KEYS[i % ROLE_KEYS.length],
      (i) => NAMES[i % NAMES.length],
    ),
    plan,
  );
  assertEquals(joins, [
    {
      target: "collections/members/",
      path: "role",
      joinsWith: { target: "collections/roles/", path: "key" },
      ratio: 1,
      distinct: 5,
    },
  ]);
  assertEquals(JSON.stringify(joins).includes("organiser"), false);
});

test("value joins: free text that matches nothing is not reported", () => {
  const plan = buildPrivacyPlan({ schemas: SCHEMAS, posture: "strict" });
  const joins = detectValueJoins(
    world(
      (i) => `custom-${i}`,
      (i) => NAMES[i % NAMES.length],
    ),
    plan,
  );
  assertEquals(joins, []);
});

test("value joins: a partial overlap under the ratio is not reported, over it is", () => {
  const plan = buildPrivacyPlan({ schemas: SCHEMAS, posture: "strict" });
  const mostly = world(
    (i) => (i < 11 ? ROLE_KEYS[i % ROLE_KEYS.length] : "legacy"),
    (i) => NAMES[i % NAMES.length],
  );
  assertEquals(detectValueJoins(mostly, plan).length, 1);
  const half = world(
    (i) => (i < 6 ? ROLE_KEYS[i % ROLE_KEYS.length] : `x${i}`),
    (i) => NAMES[i % NAMES.length],
  );
  assertEquals(detectValueJoins(half, plan), []);
});

test("value joins: ids and record keys are join targets too", () => {
  const schemas = {
    collections: {
      users: {
        _id: personId("user"),
        email: personal(v.string(), { role: "direct" }),
      },
      settings: {
        _id: dbId("setting"),
        byKey: notPersonal(v.record(v.string(), v.boolean()), "flags"),
      },
      logs: {
        _id: personal(dbId("log"), { of: "user" }),
        userId: refId("user"),
        flag: v.string(),
        other: v.string(),
      },
    },
  } as unknown as SchemasDefinition;
  const plan = buildPrivacyPlan({ schemas, posture: "strict" });
  const state = createEmptyDatabaseState();
  state.collections.users = {
    content: [{ _id: "user:01j5zk3v8n2q4x6y8z0b1c3d5e", email: "a@b.fr" }],
  };
  state.collections.settings = {
    content: [
      { _id: "setting:1", byKey: { dark: true, beta: false, wide: true } },
    ],
  };
  state.collections.logs = {
    content: [
      {
        _id: "log:1",
        userId: "user:01j5zk3v8n2q4x6y8z0b1c3d5e",
        flag: "dark",
        other: "log:1",
      },
      {
        _id: "log:2",
        userId: "user:01j5zk3v8n2q4x6y8z0b1c3d5e",
        flag: "beta",
        other: "log:2",
      },
    ],
  };
  const joins = detectValueJoins(state, plan);
  assertEquals(
    joins.map((j) => [j.path, j.joinsWith.target, j.joinsWith.path]),
    [
      ["flag", "collections/settings/", "byKey (keys)"],
      ["other", "collections/logs/", "_id"],
    ],
  );
});

import { refId } from "../../src/ids.ts";
import { unique, withIndex } from "../../src/indexes.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { personal, personId } from "../../src/privacy/mod.ts";
import { runScenario } from "../../src/scenario/mod.ts";
import * as v from "../../src/schema.ts";
import { defineType } from "../../src/type-definition.ts";
import { assert } from "../+assert.ts";
import { test } from "../+harness.ts";

const Membership = defineType({
  schema: v.object({
    _id: personal(refId("membership"), { of: "user" }),
    userId: refId("user"),
    roleId: refId("role"),
  }),
  indexes: (f) => [unique(f.userId, f.roleId)],
});

const BIRTH = migrationDefinition("2026_01_01_0900_BIRTH01@birth", "birth", {
  parent: null,
  schemas: {
    collections: {
      "+users": { _id: personId("user") },
      roles: { _id: refId("role"), name: v.string() },
      memberships: Membership,
    },
  },
  migrate: (b) => b.compile(),
});

test({
  // TODO(privacy): C16, scenario/unique.ts uniqueIndexesOf only reads field-level withIndex; read indexesOf(source) composites in generate (retry) and oracle (violation)
  ignore: true,
  name: "C16 scenario: a defineType unique composite is honoured by the generator or flagged by the oracle",
  fn: async () => {
    const run = await runScenario({
      migrations: [BIRTH],
      scenario: {
        name: "pigeonhole",
        birth: BIRTH.id,
        shape: { "+users": 2, roles: 1, memberships: 6 },
      },
    });
    const pairs = run.state.collections.memberships.content.map(
      (d) => `${d.userId}|${d.roleId}`,
    );
    const duplicated = new Set(pairs).size < pairs.length;
    const flagged = run.report.violations.some(
      (x) => x.kind === "unique_index",
    );
    assert(
      !duplicated || flagged,
      `duplicates on unique(userId, roleId), oracle ok=${run.report.ok}`,
    );
  },
});

test({
  // TODO(privacy): C17, runScenario/seed never recompute computed fields: the generator fills the computed root with mock values; call recomputeComputedFields(state, at.schemas) after replay
  ignore: true,
  name: "C17 scenario: seeded computed fields equal the truth of the seeded sources",
  fn: async () => {
    const { from } = await import("../../src/computed.ts");
    const { recomputeComputedFields } = await import(
      "../../src/scenario/mod.ts"
    );
    const { COMPUTED_ROOT } = await import("../../src/computed-guard.ts");
    const Member = defineType({
      schema: v.object({
        _id: personal(refId("member"), { of: "user" }),
        userId: withIndex(refId("user")),
      }),
    });
    const User = defineType({
      schema: v.object({ _id: personId("user") }),
      computed: {
        memberCount: from("members", Member)
          .by((m) => m.userId)
          .count(),
      },
    });
    const M = migrationDefinition("2026_01_01_0900_BIRTH01@birth", "birth", {
      parent: null,
      schemas: { collections: { users: User, members: Member } },
      migrate: (b) => b.compile(),
    });
    const run = await runScenario({
      migrations: [M],
      scenario: {
        name: "computed",
        birth: M.id,
        shape: { users: 3, members: 7 },
      },
    });
    const seeded = run.state.collections.users.content.map(
      (d) =>
        (d[COMPUTED_ROOT] as Record<string, unknown> | undefined)?.memberCount,
    );
    const truth = structuredClone(run.state);
    recomputeComputedFields(truth, M.schemas);
    const expected = truth.collections.users.content.map(
      (d) => (d[COMPUTED_ROOT] as Record<string, unknown>).memberCount,
    );
    assert(
      JSON.stringify(seeded) === JSON.stringify(expected),
      `seeded ${JSON.stringify(seeded)} vs truth ${JSON.stringify(expected)}`,
    );
  },
});

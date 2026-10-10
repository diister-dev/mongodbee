import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import * as v from "../../src/schema.ts";
import { dbId } from "../../src/ids.ts";
import { migrationDefinition } from "../../src/migration/definition.ts";
import { isBlockingViolation, runScenario } from "../../src/scenario/mod.ts";

const CONTESTED = migrationDefinition(
  "2026_01_01_0900_BLOCK01@contested",
  "contested",
  {
    parent: null,
    schemas: {
      collections: {
        drafts: { _id: dbId("doc"), title: v.string() },
        published: { _id: dbId("doc"), title: v.string() },
      },
    },
    migrate: (b) => b.compile(),
  },
);

test("scenario: a space minted by two targets is a note, flagged as such, not read from its message", async () => {
  const { report } = await runScenario({
    migrations: [CONTESTED],
    scenario: {
      name: "contested",
      birth: CONTESTED.id,
      shape: { drafts: 2, published: 2 },
    },
  });
  const contested = report.violations.filter((v) => v.kind === "correlation");
  assertEquals(contested.length, 1);
  assertEquals(contested[0].blocking, false);
  assert(report.ok, "a tie-broken space must not block the seed");
});

test("scenario: blocking follows the structured flag, whatever the message says", () => {
  assert(
    isBlockingViolation({
      kind: "correlation",
      target: "collections/x/",
      message: 'Identifier space "x" is minted by 2 targets',
    }),
    "an unflagged correlation violation blocks",
  );
  assert(
    !isBlockingViolation({
      kind: "correlation",
      target: "collections/x/",
      message: "anything",
      blocking: false,
    }),
  );
  assert(
    !isBlockingViolation({
      kind: "unique_unchecked",
      target: "collections/x/",
      message: "anything",
    }),
  );
});

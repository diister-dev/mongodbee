import { test } from "../+harness.ts";
import { assert, assertEquals } from "../+assert.ts";
import { parseArgs } from "../../src/utils/parse-args.ts";
import type { MigrationRule } from "../../src/migration/types.ts";
import { migrationBuilder } from "../../src/migration/builder.ts";
import { compareSteps, stepsOf } from "../../src/migration/steps.ts";
import * as v from "../../src/schema.ts";

function seededAlpha(alpha: Record<string, v.GenericSchema>) {
  return stepsOf(
    migrationBuilder({ schemas: { collections: { alpha } } })
      .createCollection("alpha")
      .seed([{ n: 1, at: new Date() }])
      .end()
      .collection("alpha")
      .transform({
        up: (doc) => ({ ...doc, n: (doc.n as number) + 1 }),
        down: (doc) => ({ ...doc, n: (doc.n as number) - 1 }),
      })
      .end()
      .compile().operations,
  );
}

test("an applied operation keeps its signature when a later step rewrites the schemas it carries", () => {
  const applied = seededAlpha({ n: v.number(), at: v.date() });
  const now = seededAlpha({
    n: v.number(),
    at: v.date(),
    label: v.optional(v.string()),
  });

  assertEquals(compareSteps(applied, now), { kind: "same" });
});

function expireBeforeNow() {
  return stepsOf(
    migrationBuilder({ schemas: { collections: { alpha: { at: v.date() } } } })
      .collection("alpha")
      .deleteWhere({ at: { $lt: new Date() } })
      .end()
      .compile().operations,
  );
}

test("an applied operation keeps its signature when its filter holds the time it was built at", async () => {
  const first = expireBeforeNow();
  await new Promise((resolve) => setTimeout(resolve, 5));
  const second = expireBeforeNow();

  assertEquals(compareSteps(first, second), { kind: "same" });
});

test("operations appended to applied ones are an extension, a different first operation is a change", () => {
  const applied = seededAlpha({ n: v.number(), at: v.date() });
  const extended = stepsOf(
    migrationBuilder({
      schemas: {
        collections: { alpha: { n: v.number(), at: v.date() }, beta: {} },
      },
    })
      .createCollection("alpha")
      .seed([{ n: 1, at: new Date() }])
      .end()
      .collection("alpha")
      .transform({ up: (doc) => doc, down: (doc) => doc })
      .end()
      .createCollection("beta")
      .end()
      .compile().operations,
  );

  assertEquals(compareSteps(applied, extended).kind, "extended");
  assertEquals(compareSteps(extended.slice(1), applied).kind, "changed");
  assertEquals(compareSteps(undefined, applied).kind, "unrecorded");
});

test("a signature covers what an operation is and where it acts, not its data, flags or fields added later", () => {
  const signatureOf = (operation: Record<string, unknown>) =>
    stepsOf([operation as unknown as MigrationRule])[0].signature;
  const seed = {
    type: "seed_collection",
    collectionName: "alpha",
    documents: [{ n: 1 }],
    schema: {},
  };

  assertEquals(
    signatureOf(seed),
    signatureOf({
      ...seed,
      documents: [{ n: 1 }, { n: 2 }],
      where: { at: Date.now() },
      lossy: true,
      futureDefault: "added by a later mongodbee",
    }),
  );
  assert(
    signatureOf(seed) !== signatureOf({ ...seed, collectionName: "beta" }),
  );
  assert(
    signatureOf(seed) !==
      signatureOf({ ...seed, type: "seed_multicollection_type" }),
  );
});

test("--accept-drift can be repeated on the command line", () => {
  const args = parseArgs(
    ["migrate", "--accept-drift", "a@one", "--accept-drift", "b@two"],
    { string: ["accept-drift"], collect: ["accept-drift"] },
  );

  assertEquals(args["accept-drift"], ["a@one", "b@two"]);
});

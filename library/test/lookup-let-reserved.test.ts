// Verrou — `let.localValue` is the correlated join binding of every
// `lookup`/`anyLookup` sub-pipeline; a user `let` must not be able to
// redefine it.
//
// Regression it guards: the stage builders emitted
// `let: { localValue: "$<localField>", ...options.let }`, so a caller passing
// `let: { localValue: <anything> }` silently REPOINTED the join — the
// sub-pipeline's `$expr: { $eq: ["$<foreignField>", "$$localValue"] }`
// compared against the caller's expression instead of the local field, and
// every joined array was quietly wrong. Both duplicated builders in
// multi-collection (paginate's and aggregate's) and the scoped builder are
// covered.

import { test } from "./+harness.ts";
import { assertRejects } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";

const EXPO = "exposition:expoaaaaa01";

test("lookup: let.localValue is refused (multi aggregate builder)", async () => {
  await withDatabase("lookup-let-reserved-multi-agg", async (db) => {
    const catalog = await multiCollection(db, "catalog", {
      participant: { name: v.string() },
      badge: { participantId: v.string(), label: v.string() },
    });
    await assertRejects(
      () =>
        catalog.aggregate((stage) => [
          stage.match("participant", {}),
          stage.lookup("badge", "_id", "participantId", {
            as: "badges",
            let: { localValue: "$name" },
          }),
        ]),
      Error,
      "localValue",
    );
  });
});

test("lookup: let.localValue is refused (multi paginate builder)", async () => {
  await withDatabase("lookup-let-reserved-multi-pag", async (db) => {
    const catalog = await multiCollection(db, "catalog", {
      participant: { name: v.string() },
      badge: { participantId: v.string(), label: v.string() },
    });
    await assertRejects(
      () =>
        catalog.paginate(
          "participant",
          {},
          {
            pipeline: (stage) => [
              stage.lookup("badge", "_id", "participantId", {
                as: "badges",
                let: { localValue: "$name" },
              }),
            ],
          },
        ),
      Error,
      "localValue",
    );
  });
});

test("lookup: let.localValue is refused (scoped builder, lookup + anyLookup)", async () => {
  await withDatabase("lookup-let-reserved-scoped", async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        participant: { name: v.string() },
        badge: { participantId: v.string(), label: v.string() },
      },
    });
    const view = catalog.scope(EXPO);
    await assertRejects(
      () =>
        view.aggregate((stage) => [
          stage.match("participant", {}),
          stage.lookup("badge", "_id", "participantId", {
            as: "badges",
            let: { localValue: "$name" },
          }),
        ]),
      Error,
      "localValue",
    );
    await assertRejects(
      () =>
        view.aggregate((stage) => [
          stage.match("participant", {}),
          stage.anyLookup("_id", "participantId", {
            as: "any",
            let: { localValue: "$name" },
          }),
        ]),
      Error,
      "localValue",
    );
  });
});

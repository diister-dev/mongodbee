import { test } from "./+harness.ts";
import {
  assert,
  assertEquals,
  assertRejects,
  assertThrows,
} from "./+assert.ts";
import { computedValues, withDatabase } from "./+shared.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import { defineType } from "../src/type-definition.ts";
import {
  ComputedFieldWriteError,
  from,
  refuseComputedWrite,
} from "../src/computed.ts";
import {
  type ComputedSchemas,
  computedTopology,
} from "../src/computed-topology.ts";
import { registerComputed } from "../src/computed-maintenance.ts";
import type { Db } from "../src/mongodb.ts";

const EXPO = "exposition:expoaaaaa01";

const Membership = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant")),
    organizationId: refId("expo_organization"),
  }),
});

const Participant = defineType({
  schema: v.object({ name: v.string() }),
  computed: {
    organizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .collect((m) => m.organizationId)
      .maxEntries(10),
  },
});

function register(db: Db, schemas: ComputedSchemas): void {
  registerComputed(db, computedTopology(schemas));
}

const forged = { organizationIds: ["expo_organization:forged"] };

async function refused(
  label: string,
  write: () => Promise<unknown>,
): Promise<void> {
  const error = await assertRejects(write, Error);
  assert(
    error instanceof ComputedFieldWriteError,
    `${label}: expected ComputedFieldWriteError, got ${error.name}: ${error.message}`,
  );
}

test("computed guard: the payload shapes an application could use to reach _computed are all refused", () => {
  const shapes: unknown[] = [
    { _computed: forged },
    { $set: { _computed: forged } },
    { $set: { "_computed.organizationIds": [] } },
    { $unset: { "_computed.organizationIds": "" } },
    { $push: { "_computed.organizationIds": "expo_organization:x" } },
    { $inc: { "_computed.count": 1 } },
    { $setOnInsert: { _computed: forged } },
    { $rename: { name: "_computed.organizationIds" } },
    [{ $set: { _computed: forged } }],
    [{ $unset: "_computed.organizationIds" }],
    [{ $set: { name: "$_computed.organizationIds" } }],
  ];
  for (const shape of shapes) {
    assertThrows(() => refuseComputedWrite(shape), ComputedFieldWriteError);
  }
  refuseComputedWrite({
    name: "Ada",
    nested: {
      _computed: "an application field deeper down is its own business",
    },
  });
  refuseComputedWrite({ $set: { name: "Ada" } });
  refuseComputedWrite([{ $set: { name: "Ada" } }]);
});

test("computed guard: a collection refuses every write that touches _computed", async (t) => {
  await withDatabase(t.name, async (db) => {
    register(db, {
      collections: { participants: Participant, org_membership: Membership },
    });
    const participants = await collection(db, "participants", Participant);
    const id = await participants.insertOne({ name: "Ada" });

    await refused("insertOne", () =>
      participants.insertOne({ name: "Bob", _computed: forged } as never),
    );
    await refused("insertMany", () =>
      participants.insertMany([{ name: "Bob", _computed: forged }] as never),
    );
    await refused("updateOne $set", () =>
      participants.updateOne({ _id: id }, {
        $set: { "_computed.organizationIds": [] },
      } as never),
    );
    await refused("updateOne $unset", () =>
      participants.updateOne({ _id: id }, {
        $unset: { _computed: "" },
      } as never),
    );
    await refused("updateOne pipeline", () =>
      participants.updateOne({ _id: id }, [
        { $set: { _computed: forged } },
      ] as never),
    );
    await refused("updateMany", () =>
      participants.updateMany({}, { $set: { _computed: forged } } as never),
    );
    await refused("replaceOne", () =>
      participants.replaceOne(
        { _id: id } as never,
        { name: "Ada", _computed: forged } as never,
      ),
    );
    await refused("findOneAndUpdate", () =>
      participants.findOneAndUpdate(
        { _id: id } as never,
        { $set: { _computed: forged } } as never,
      ),
    );
    await refused("findOneAndReplace", () =>
      participants.findOneAndReplace(
        { _id: id } as never,
        { name: "Ada", _computed: forged } as never,
      ),
    );
    const bulkShapes: unknown[] = [
      [{ insertOne: { document: { name: "Bob", _computed: forged } } }],
      [
        {
          updateOne: {
            filter: { _id: id },
            update: { $set: { "_computed.organizationIds": [] } },
          },
        },
      ],
      [
        {
          replaceOne: {
            filter: { _id: id },
            replacement: { name: "x", _computed: forged },
          },
        },
      ],
    ];
    for (const operations of bulkShapes) {
      await refused("bulkWrite", () =>
        participants.bulkWrite(operations as never),
      );
    }

    const stored = await participants.getById(id);
    assertEquals(
      computedValues((stored as { _computed?: unknown })._computed),
      { organizationIds: [] },
      "only the truth reached the document, never the forged value",
    );
  });
});

test("computed guard: a multi-collection refuses every write that touches _computed", async (t) => {
  await withDatabase(t.name, async (db) => {
    const model = defineModel("expo", {
      schema: { participant: Participant, org_membership: Membership },
    });
    register(db, { multiCollections: { expo: model.schema } });
    const expo = await multiCollection(db, "expo", model);
    const id = await expo.insertOne("participant", { name: "Ada" });

    await refused("insertOne", () =>
      expo.insertOne("participant", {
        name: "Bob",
        _computed: forged,
      } as never),
    );
    await refused("updateOne", () =>
      expo.updateOne("participant", id, { _computed: forged } as never),
    );
    await refused("updateWhere", () =>
      expo.updateWhere(
        "participant",
        { _id: id },
        { _computed: forged } as never,
        {} as never,
      ),
    );
    await refused("findOneAndUpdate", () =>
      expo.findOneAndUpdate(
        "participant",
        { _id: id },
        { _computed: forged } as never,
        {} as never,
      ),
    );
    await refused("updateMany", () =>
      expo.updateMany({
        participant: { [id]: { _computed: forged } },
      } as never),
    );

    const stored = await expo.findOne("participant", { _id: id });
    assertEquals(
      computedValues((stored as { _computed?: unknown } | null)?._computed),
      {
        organizationIds: [],
      },
    );
  });
});

test("computed guard: a scoped view refuses every write that touches _computed", async (t) => {
  await withDatabase(t.name, async (db) => {
    register(db, {
      scopedMultiCollections: {
        "+expositions": {
          types: { participant: Participant, org_membership: Membership },
        },
      },
    });
    const scoped = await scopedMultiCollection(db, "+expositions", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { participant: Participant, org_membership: Membership },
    });
    const view = scoped.scope(EXPO);
    const id = await view.insertOne("participant", { name: "Ada" });

    await refused("insertOne", () =>
      view.insertOne("participant", {
        name: "Bob",
        _computed: forged,
      } as never),
    );
    await refused("updateOne", () =>
      view.updateOne("participant", id, { _computed: forged } as never),
    );
    await refused("updateWhere", () =>
      view.updateWhere(
        "participant",
        { _id: id },
        { _computed: forged } as never,
        {} as never,
      ),
    );
    await refused("findOneAndUpdate", () =>
      view.findOneAndUpdate(
        "participant",
        { _id: id },
        { _computed: forged } as never,
        {} as never,
      ),
    );
    await refused("updateMany", () =>
      view.updateMany({
        participant: { [id]: { _computed: forged } },
      } as never),
    );

    const stored = await view.findOne("participant", { _id: id });
    assertEquals(
      computedValues((stored as { _computed?: unknown } | null)?._computed),
      {
        organizationIds: [],
      },
    );
  });
});

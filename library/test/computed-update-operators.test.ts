import { test } from "./+harness.ts";
import { assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import type { Db } from "../src/mongodb.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineType } from "../src/type-definition.ts";
import { from } from "../src/computed.ts";
import { computedTopology } from "../src/computed-topology.ts";
import { registerComputed } from "../src/computed-maintenance.ts";
import { checkComputed } from "../src/computed-apply.ts";
import { increment, pull, push } from "../src/update-operators.ts";

const EXPO = "exposition:expoaaaaa01";

const Ticket = defineType({
  schema: v.object({
    ownerId: withIndex(refId("owner")),
    level: v.number(),
    labels: v.array(v.string()),
  }),
});

const Owner = defineType({
  schema: v.object({ name: v.string() }),
  computed: {
    goldTickets: from("ticket", Ticket)
      .by((t) => t.ownerId)
      .where((t) => [t.level, 2])
      .count(),
    labels: from("ticket", Ticket)
      .by((t) => t.ownerId)
      .collect((t) => t.labels),
  },
});

const schemas = {
  scopedMultiCollections: {
    "+support": {
      scope: refId("exposition"),
      types: { owner: Owner, ticket: Ticket },
    },
  },
};

async function open(db: Db) {
  const topology = computedTopology(schemas);
  registerComputed(db, topology);
  const support = await scopedMultiCollection(db, "+support", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: schemas.scopedMultiCollections["+support"].types,
  });
  return { topology, view: support.scope(EXPO) };
}

type Computed = {
  _computed?: { goldTickets?: number; labels?: unknown[] };
};

test("computed maintenance: an incremented source field recomputes its dependents", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view } = await open(db);
    const owner = await view.insertOne("owner", { name: "Ada" });
    const ticket = await view.insertOne("ticket", {
      ownerId: owner,
      level: 1,
      labels: [],
    });
    const read = async () =>
      ((await view.getById("owner", owner)) as Computed)._computed;

    assertEquals((await read())?.goldTickets, 0);
    await view.updateOne("ticket", ticket, { level: increment(1) });
    assertEquals((await read())?.goldTickets, 1);
    await view.updateWhere("ticket", { _id: ticket }, { level: increment(1) });
    assertEquals((await read())?.goldTickets, 0);
    assertEquals((await checkComputed(db, topology)).drifts, []);
  });
});

test("computed maintenance: a pushed or pulled source field recomputes its dependents", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { topology, view } = await open(db);
    const owner = await view.insertOne("owner", { name: "Ada" });
    const ticket = await view.insertOne("ticket", {
      ownerId: owner,
      level: 1,
      labels: ["a"],
    });
    const read = async () =>
      ((await view.getById("owner", owner)) as Computed)._computed?.labels;
    const before = await read();

    await view.updateOne("ticket", ticket, { labels: push("b") });
    const pushed = await read();
    assertEquals(JSON.stringify(pushed) === JSON.stringify(before), false);
    assertEquals((await checkComputed(db, topology)).drifts, []);

    await view.findOneAndUpdate(
      "ticket",
      { _id: ticket },
      {
        labels: pull("a"),
      },
    );
    assertEquals((await checkComputed(db, topology)).drifts, []);
    assertEquals(JSON.stringify(await read()).includes('"a"'), false);
  });
});

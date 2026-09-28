import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import { assertEquals, assertRejects } from "./+assert.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import type { Db } from "../src/mongodb.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { defineType } from "../src/type-definition.ts";
import { ComputedFieldWriteError, from } from "../src/computed.ts";
import { computedTopology } from "../src/computed-topology.ts";
import {
  registerComputed,
  unregisterComputed,
} from "../src/computed-maintenance.ts";
import { checkComputed } from "../src/computed-apply.ts";
import { withDatabase } from "./+shared.ts";

const Membership = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant")),
    organizationId: refId("expo_organization"),
    status: v.picklist(["active", "removed"]),
  }),
});

const Participant = defineType({
  schema: v.object({ name: v.string() }),
  computed: {
    organizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .collect((m) => m.organizationId)
      .distinct(),
  },
});

const schemas = {
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: { participant: Participant, org_membership: Membership },
    },
  },
};

const SCOPE = "exposition:skewaaaaaaaaaaaaaaaaaaaaa1";
const ORG = "expo_organization:01aaaaaaaaaaaaaaaaaaaaaaaa";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function gate() {
  let open = () => {};
  const opened = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { open, opened };
}

async function openWorld(db: Db) {
  const expositions = await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: schemas.scopedMultiCollections["+expositions"].types,
  });
  const view = expositions.scope(SCOPE);
  const participant = await view.insertOne("participant", { name: "P" });
  const membership = (status: "active" | "removed") =>
    view.insertOne("org_membership", {
      participantId: participant,
      organizationId: ORG,
      status,
    });
  return { expositions, view, participant, membership };
}

type World = Awaited<ReturnType<typeof openWorld>>;

async function interleave(
  world: World,
  first: () => Promise<unknown>,
  second: () => Promise<unknown>,
): Promise<void> {
  const firstWrote = gate();
  const firstMayCommit = gate();
  const a = world.expositions.withSession(async () => {
    await first();
    firstWrote.open();
    await firstMayCommit.opened;
  });
  await firstWrote.opened;
  const b = world.expositions.withSession(second, { retry: true });
  await Promise.race([
    b.then(
      () => {},
      () => {},
    ),
    delay(300),
  ]);
  firstMayCommit.open();
  await a;
  await b;
}

async function withSkewWorld(
  name: string,
  scenario: (world: World) => Promise<void>,
): Promise<void> {
  await withDatabase(name, async (db) => {
    const topology = computedTopology(schemas);
    registerComputed(db, topology);
    try {
      await scenario(await openWorld(db));
      const { drifts } = await checkComputed(db, topology);
      assertEquals(drifts, []);
    } finally {
      unregisterComputed(db);
    }
  });
}

test("computed write skew: a write that leaves the value unchanged in its snapshot still conflicts with one that changes it", async (t) => {
  await withSkewWorld(t.name, async (world) => {
    const first = await world.membership("active");
    await interleave(
      world,
      () =>
        world.view.updateOne("org_membership", first, { status: "removed" }),
      () => world.membership("active"),
    );
  });
});

test("computed write skew: two writes that each leave the value unchanged still conflict", async (t) => {
  await withSkewWorld(t.name, async (world) => {
    const first = await world.membership("active");
    const second = await world.membership("active");
    await interleave(
      world,
      () =>
        world.view.updateOne("org_membership", first, { status: "removed" }),
      () =>
        world.view.updateOne("org_membership", second, { status: "removed" }),
    );
  });
});

async function withRevisionWorld(
  name: string,
  scenario: (world: World) => Promise<void>,
): Promise<void> {
  await withDatabase(name, async (db) => {
    registerComputed(db, computedTopology(schemas));
    try {
      await scenario(await openWorld(db));
    } finally {
      unregisterComputed(db);
    }
  });
}

async function revisionOf(world: World): Promise<number | undefined> {
  const stored = await world.view.getById("participant", world.participant);
  return stored._computed?._rev;
}

test("computed revision: every write that recomputes a subject bumps _rev, even when the value stays the same", async (t) => {
  await withRevisionWorld(t.name, async (world) => {
    const before = (await revisionOf(world)) ?? 0;
    await world.membership("removed");
    const afterUnchanged = await revisionOf(world);
    assertEquals(afterUnchanged, before + 1);
    await world.membership("active");
    assertEquals(await revisionOf(world), before + 2);
  });
});

test("computed revision: a write that touches no computed input leaves _rev alone", async (t) => {
  await withRevisionWorld(t.name, async (world) => {
    const before = await revisionOf(world);
    await world.view.updateOne("participant", world.participant, {
      name: "Renamed",
    });
    assertEquals(await revisionOf(world), before);
  });
});

test("computed revision: the application cannot write _rev", async (t) => {
  await withRevisionWorld(t.name, async (world) => {
    await assertRejects(
      () =>
        world.view.updateOne("participant", world.participant, {
          _computed: { _rev: 99 },
        } as never),
      ComputedFieldWriteError,
    );
  });
});

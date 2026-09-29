import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import {
  assert,
  assertEquals,
  assertRejects,
  assertThrows,
} from "./+assert.ts";
import { type Db, MongoClient } from "../src/mongodb.ts";
import { closeAllWatchers } from "../src/change-stream.ts";
import { refId } from "../src/ids.ts";
import { withIndex } from "../src/indexes.ts";
import { index } from "../src/index-builder.ts";
import { defineType } from "../src/type-definition.ts";
import { defineModel } from "../src/multi-collection-model.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { collection } from "../src/collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { from, scoped } from "../src/computed.ts";
import { computedTopology } from "../src/computed-topology.ts";
import {
  registerComputed,
  unregisterComputed,
} from "../src/computed-maintenance.ts";
import { getSessionContext } from "../src/session.ts";
import { afterCommit } from "../src/transaction-scope.ts";
import {
  readingCollection,
  withReadPreference,
} from "../src/read-preference.ts";
import {
  invalidateReadsOnDriverWrites,
  withRequestContext,
} from "../src/request-context.ts";
import {
  type CompositeReader,
  ReaderArgumentError,
  ReaderDatabaseError,
  ReaderDefinitionError,
  ReaderDirectReadError,
  ReaderNotRegisteredError,
  reader,
  registerReaders,
  requestReaderStats,
  unregisterReaders,
} from "../src/readers.ts";
import { TEST_URI } from "./+shared.ts";

const EXPO = "exposition:expoaaaaa01";
const OTHER_EXPO = "exposition:expobbbbb02";

const Badge = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant")),
  }),
});

const Participant = defineType({
  schema: v.object({
    status: v.picklist(["active", "pending", "withdrawn"]),
    personRef: v.variant("kind", [
      v.object({ kind: v.literal("user"), userId: v.string() }),
      v.object({ kind: v.literal("accountless"), identityId: v.string() }),
    ]),
    displayName: v.string(),
  }),
  indexes: (f) => [index(f.personRef.userId)],
  computed: {
    badgeCount: from("badge", Badge)
      .by((b) => b.participantId)
      .count(),
  },
});

const Role = defineType({
  schema: v.object({
    key: withIndex(v.string()),
    permissions: v.array(v.string()),
  }),
});

const Member = defineType({
  schema: v.object({
    userId: withIndex(v.string()),
    tenantId: v.string(),
    role: v.string(),
    status: v.picklist(["active", "invited", "left"]),
    invitationToken: v.optional(v.string()),
  }),
});

const Note = defineType({
  schema: v.object({ title: withIndex(v.string()) }),
});

const Information = defineType({
  schema: v.object({ name: v.string(), lifecycle: v.array(v.string()) }),
});

const SPACE_SCOPE = v.pipe(v.string(), v.toLowerCase());

const ExpositionModel = defineModel("exposition", {
  schema: {
    participant: Participant,
    role: Role,
    badge: Badge,
    information: Information,
  },
});

const SpaceModel = defineModel("space", { schema: { note: Note } });

const Expo = scoped(ExpositionModel, refId("exposition"));
const Space = scoped(SpaceModel, SPACE_SCOPE);

const SessionFields = {
  userId: withIndex(v.string()),
  device: v.string(),
};

const schemas = {
  collections: { "+sessions": SessionFields },
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: ExpositionModel.schema,
    },
    "+spaces": { scope: SPACE_SCOPE, types: SpaceModel.schema },
  },
  multiCollections: {
    "+entreprises": { member: Member },
  },
};

const participationsOf = reader(
  "participations-of-user",
  from(Expo, "participant")
    .by((p) => p.personRef.userId)
    .where((p) => [p.personRef.kind, "user"])
    .select(["status", "personRef"]),
);

const entrepriseMemberships = reader(
  "entreprise-memberships-of-user",
  from("member", Member)
    .by((m) => m.userId)
    .where((m) => [m.status, ["active", "invited"]])
    .select(["tenantId", "role", "status"]),
);

const roleByKey = reader(
  "role-by-key",
  from(Expo, "role")
    .by((r) => r.key)
    .one()
    .select(["key", "permissions"]),
);

const notesTitled = reader(
  "notes-titled",
  from(Space, "note")
    .by((n) => n.title)
    .select(["title"]),
);

const accessOf = reader(
  "access-of-user",
  async (expositionId: string, userId: string) => {
    const [participations, memberships] = await Promise.all([
      participationsOf(expositionId, userId),
      entrepriseMemberships(userId),
    ]);
    return {
      active: participations.some((p) => p.status === "active"),
      tenants: memberships.map((m) => m.tenantId),
    };
  },
);

const expositionInformation = reader(
  "exposition-information",
  from(Expo, "information").one().select(["name"]),
);

const sessionsOf = reader(
  "sessions-of-user",
  from("+sessions", SessionFields)
    .by((s) => s.userId)
    .select(["device"]),
);

const participantComputed = reader(
  "participant-computed",
  from(Expo, "participant")
    .by((p) => p.personRef.userId)
    .select(["_computed"]),
);

const tenantsOf = reader("tenants-of-users", async (userIds: string[]) => {
  const memberships = await entrepriseMemberships.many(userIds);
  return [...memberships.values()].flat().map((m) => m.tenantId);
});

const READERS = [
  participationsOf,
  entrepriseMemberships,
  roleByKey,
  notesTitled,
  accessOf,
  expositionInformation,
  sessionsOf,
  participantComputed,
  tenantsOf,
];

interface Fixture {
  readonly db: Db;
  readonly client: MongoClient;
  readonly finds: () => number;
  readonly modes: string[];
  readonly expo: Awaited<ReturnType<typeof openExpositions>>;
  readonly entreprises: Awaited<ReturnType<typeof openEntreprises>>;
  readonly sessions: Awaited<ReturnType<typeof openSessions>>;
  readonly inRequest: <T>(fn: () => Promise<T>) => Promise<T>;
}

async function openSessions(db: Db) {
  return await collection(db, "+sessions", SessionFields, {
    schemaManagement: "auto",
  });
}

async function openExpositions(db: Db) {
  return await scopedMultiCollection(db, "+expositions", {
    schemaManagement: "auto",
    scope: refId("exposition"),
    types: ExpositionModel.schema,
  });
}

async function openEntreprises(db: Db) {
  return await multiCollection(
    db,
    "+entreprises",
    { member: Member },
    { schemaManagement: "auto" },
  );
}

function randomName(): string {
  return `@TEST_readers@${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`;
}

async function withReaders(
  work: (fixture: Fixture) => Promise<void>,
  options: {
    limits?: { entriesPerRequest?: number; rowsPerEntry?: number };
  } = {},
) {
  const client = new MongoClient(TEST_URI, { monitorCommands: true });
  let finds = 0;
  const modes: string[] = [];
  client.on("commandStarted", (event) => {
    if (event.commandName !== "find") return;
    finds++;
    const preference = event.command.$readPreference as
      | { mode?: string }
      | undefined;
    modes.push(preference?.mode ?? "primary");
  });
  const db = client.db(randomName());
  const topology = computedTopology(schemas);
  registerComputed(db, topology);
  registerReaders(db, {
    topology,
    readers: READERS,
    limits: options.limits,
  });
  try {
    await work({
      db,
      client,
      finds: () => finds,
      modes,
      expo: await openExpositions(db),
      entreprises: await openEntreprises(db),
      sessions: await openSessions(db),
      inRequest: (fn) => withRequestContext(fn, { database: db }),
    });
  } finally {
    unregisterReaders(db);
    unregisterComputed(db);
    await closeAllWatchers(db);
    await db.dropDatabase();
    await client.close();
  }
}

async function seedParticipant(
  fixture: Fixture,
  userId: string,
  scope = EXPO,
  status: "active" | "pending" = "pending",
): Promise<string> {
  return await fixture.expo.scope(scope).insertOne("participant", {
    status,
    personRef: { kind: "user", userId },
    displayName: "Ada",
  });
}

async function seedMember(
  fixture: Fixture,
  status: "active" | "invited" | "left" = "active",
): Promise<string> {
  return await fixture.entreprises.insertOne("member", {
    userId: "user:ada",
    tenantId: "tenant:a",
    role: "owner",
    status,
  });
}

function statusOf(
  rows: readonly { readonly status: string }[],
): string | undefined {
  return rows[0]?.status;
}

test("readers: one key is loaded once per request, and every call outside a request loads", async () => {
  await withReaders(async (fixture) => {
    await seedParticipant(fixture, "user:ada");
    await seedParticipant(fixture, "user:bob");

    const before = fixture.finds();
    await fixture.inRequest(async () => {
      const [first, second] = await Promise.all([
        participationsOf(EXPO, "user:ada"),
        participationsOf(EXPO, "user:ada"),
      ]);
      assertEquals(first, second);
      assertEquals(first.length, 1);
      assertEquals(statusOf(first), "pending");
      await participationsOf(EXPO, "user:ada");
      await participationsOf(EXPO, "user:bob");
      assertEquals(requestReaderStats()?.loads, 2);
      assertEquals(requestReaderStats()?.hits, 2);
    });
    assertEquals(fixture.finds() - before, 2);
  });
});

test("readers: a write by id whose patch holds no by field is seen by the next call", async () => {
  await withReaders(async (fixture) => {
    const id = await seedParticipant(fixture, "user:ada");
    await fixture.inRequest(async () => {
      assertEquals(
        statusOf(await participationsOf(EXPO, "user:ada")),
        "pending",
      );
      await fixture.expo
        .scope(EXPO)
        .updateOne("participant", id, { status: "active" });
      assertEquals(
        statusOf(await participationsOf(EXPO, "user:ada")),
        "active",
      );
    });
  });
});

test("readers: a document entering the where by id is seen by the next call", async () => {
  await withReaders(async (fixture) => {
    const id = await seedMember(fixture, "left");
    await fixture.inRequest(async () => {
      assertEquals(await entrepriseMemberships("user:ada"), []);
      await fixture.entreprises.updateOne("member", id, { status: "active" });
      assertEquals(
        (await entrepriseMemberships("user:ada")).map((m) => m.tenantId),
        ["tenant:a"],
      );
    });
  });
});

test("readers: with computed fields registered, a write touching no selected, where or by path keeps every entry", async () => {
  await withReaders(async (fixture) => {
    const id = await seedParticipant(fixture, "user:ada");
    await seedMember(fixture);
    await fixture.inRequest(async () => {
      await participationsOf(EXPO, "user:ada");
      await entrepriseMemberships("user:ada");
      const view = fixture.expo.scope(EXPO);
      await view.updateOne("participant", id, { displayName: "Ada L." });
      await view.insertOne("badge", { participantId: id });
      await view.insertOne("role", { key: "staff", permissions: [] });
      await seedParticipant(fixture, "user:ada", OTHER_EXPO);
      await participationsOf(EXPO, "user:ada");
      await entrepriseMemberships("user:ada");
      assertEquals(requestReaderStats()?.loads, 2);
      assertEquals(requestReaderStats()?.invalidations, 0);
    });
  });
});

test("readers: a read-only transaction invalidates nothing", async () => {
  await withReaders(async (fixture) => {
    await seedParticipant(fixture, "user:ada");
    const { withSession } = getSessionContext(fixture.db.client);
    await fixture.inRequest(async () => {
      await participationsOf(EXPO, "user:ada");
      await withSession(async () => {
        await fixture.expo.scope(EXPO).find("role");
      });
      await participationsOf(EXPO, "user:ada");
      assertEquals(requestReaderStats()?.loads, 1);
      assertEquals(requestReaderStats()?.hits, 1);
    });
  });
});

test("readers: a call after a write never joins a load started before it", async () => {
  await withReaders(async (fixture) => {
    const id = await seedParticipant(fixture, "user:ada");
    await fixture.inRequest(async () => {
      const started = participationsOf(EXPO, "user:ada");
      await fixture.expo
        .scope(EXPO)
        .updateOne("participant", id, { status: "active" });
      const after = await participationsOf(EXPO, "user:ada");
      await started;
      assertEquals(statusOf(after), "active");
      assertEquals(
        statusOf(await participationsOf(EXPO, "user:ada")),
        "active",
      );
    });
  });
});

test("readers: a load racing a write is returned to its caller and never stored", async () => {
  await withReaders(async (fixture) => {
    const id = await seedParticipant(fixture, "user:ada");
    await fixture.inRequest(async () => {
      const racing = participationsOf(EXPO, "user:ada");
      const write = fixture.expo
        .scope(EXPO)
        .updateOne("participant", id, { status: "active" });
      await Promise.all([racing, write]);
      const before = fixture.finds();
      assertEquals(
        statusOf(await participationsOf(EXPO, "user:ada")),
        "active",
      );
      assertEquals(fixture.finds() - before, 1);
    });
  });
});

test("readers: a raw driver write invalidates when the client monitors writes", async () => {
  await withReaders(async (fixture) => {
    const stop = invalidateReadsOnDriverWrites(fixture.client);
    try {
      const id = await seedParticipant(fixture, "user:ada");
      await fixture.inRequest(async () => {
        await participationsOf(EXPO, "user:ada");
        await fixture.db
          .collection<{ _id: string }>("+expositions")
          .updateOne({ _id: id }, { $set: { status: "withdrawn" } });
        assertEquals(
          statusOf(await participationsOf(EXPO, "user:ada")),
          "withdrawn",
        );
      });
    } finally {
      stop();
    }
  });
});

test("readers: a transaction's commit invalidates before afterCommit runs, even over a read that raced it", async () => {
  await withReaders(async (fixture) => {
    const id = await seedParticipant(fixture, "user:ada");
    const { withSession } = getSessionContext(fixture.db.client);
    await fixture.inRequest(async () => {
      let written!: () => void;
      const writeDone = new Promise<void>((resolve) => {
        written = resolve;
      });
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      let seenAfterCommit: string | undefined;
      const transaction = withSession(async () => {
        await fixture.expo
          .scope(EXPO)
          .updateOne("participant", id, { status: "active" });
        await afterCommit(async () => {
          seenAfterCommit = statusOf(await participationsOf(EXPO, "user:ada"));
        });
        written();
        await gate;
      });
      await writeDone;
      assertEquals(
        statusOf(await participationsOf(EXPO, "user:ada")),
        "pending",
      );
      release();
      await transaction;
      assertEquals(seenAfterCommit, "active");
      assertEquals(
        statusOf(await participationsOf(EXPO, "user:ada")),
        "active",
      );
    });
  });
});

test("readers: inside a transaction a reader reads through the session and caches nothing", async () => {
  await withReaders(async (fixture) => {
    const id = await seedParticipant(fixture, "user:ada");
    const { withSession } = getSessionContext(fixture.db.client);
    await fixture.inRequest(async () => {
      await withSession(async () => {
        await fixture.expo
          .scope(EXPO)
          .updateOne("participant", id, { status: "active" });
        assertEquals(
          statusOf(await participationsOf(EXPO, "user:ada")),
          "active",
        );
        assertEquals(
          statusOf(await participationsOf(EXPO, "user:ada")),
          "active",
        );
      });
      assertEquals(requestReaderStats()?.loads, 0);
    });
  });
});

test("readers: a write in a nested request context invalidates the enclosing one", async () => {
  await withReaders(async (fixture) => {
    const id = await seedMember(fixture);
    await fixture.inRequest(async () => {
      assertEquals((await entrepriseMemberships("user:ada")).length, 1);
      await withRequestContext(() =>
        fixture.entreprises.updateOne("member", id, { status: "left" }),
      );
      assertEquals(await entrepriseMemberships("user:ada"), []);
    });
  });
});

test("readers: many() reads missing keys in one query, maps each key, and later calls hit", async () => {
  await withReaders(async (fixture) => {
    const view = fixture.expo.scope(EXPO);
    await view.insertMany("role", [
      { key: "staff", permissions: ["scan"] },
      { key: "admin", permissions: ["all"] },
    ]);
    await fixture.inRequest(async () => {
      const before = fixture.finds();
      const roles = await roleByKey.many(EXPO, ["staff", "admin", "ghost"]);
      assertEquals(roles.get("staff")?.permissions, ["scan"]);
      assertEquals(roles.get("admin")?.permissions, ["all"]);
      assertEquals(roles.get("ghost"), null);
      assertEquals(fixture.finds() - before, 1);
      assertEquals((await roleByKey(EXPO, "staff"))?.permissions, ["scan"]);
      assertEquals(await roleByKey(EXPO, "ghost"), null);
      assertEquals(fixture.finds() - before, 1);
    });
  });
});

test("readers: the scope argument goes through the scope schema, as writes do", async () => {
  await withReaders(async (fixture) => {
    const spaces = await scopedMultiCollection(fixture.db, "+spaces", {
      schemaManagement: "auto",
      scope: SPACE_SCOPE,
      types: SpaceModel.schema,
    });
    await spaces.scope("SPACE:A").insertOne("note", { title: "plan" });
    await fixture.inRequest(async () => {
      assertEquals((await notesTitled("SPACE:A", "plan")).length, 1);
      assertEquals((await notesTitled("space:a", "plan")).length, 1);
      assertEquals(requestReaderStats()?.hits, 1);
    });
  });
});

test("readers: values are frozen and shared, and unselected fields never reach them", async () => {
  await withReaders(async (fixture) => {
    await fixture.entreprises.insertOne("member", {
      userId: "user:ada",
      tenantId: "tenant:a",
      role: "owner",
      status: "invited",
      invitationToken: "secret",
    });
    await fixture.inRequest(async () => {
      const first = await entrepriseMemberships("user:ada");
      const second = await entrepriseMemberships("user:ada");
      assert(first === second);
      assert(Object.isFrozen(first) && Object.isFrozen(first[0]));
      assertEquals("invitationToken" in (first[0] ?? {}), false);
      assertThrows(() => {
        (first as unknown as unknown[]).push({});
      }, TypeError);
    });
  });
});

test("readers: a composite value is a frozen copy, through maps, dates, cycles and frozen parents", async () => {
  await withReaders(async (fixture) => {
    const { ObjectId } = await import("mongodb");
    const id = new ObjectId();
    const callerList = [1];
    const alreadyFrozen = Object.freeze({ list: [1] });
    interface Cyclic {
      self?: Cyclic;
      n: number;
    }
    const cyclic: Cyclic = { n: 1 };
    cyclic.self = cyclic;
    const shape = reader("frozen-shape", async () => ({
      id,
      at: new Date(0),
      byKey: new Map([["a", [1]]]),
      callerList,
      alreadyFrozen,
      cyclic,
    }));
    await fixture.inRequest(async () => {
      const value = await shape();
      assert(value.id.equals(id));
      assertThrows(() => {
        (value.at as Date).setTime(5);
      }, TypeError);
      assertThrows(() => {
        (value.byKey as Map<string, number[]>).set("b", []);
      }, TypeError);
      assertThrows(() => {
        (value.byKey.get("a") as number[]).push(2);
      }, TypeError);
      assertThrows(() => {
        (value.alreadyFrozen.list as number[]).push(2);
      }, TypeError);
      assert(value.cyclic.self === value.cyclic);
      callerList.push(2);
      alreadyFrozen.list.push(2);
      assertEquals(value.callerList, [1]);
    });
  });
});

test("readers: a composite is invalidated with the readers it used", async () => {
  await withReaders(async (fixture) => {
    const id = await seedParticipant(fixture, "user:ada");
    await fixture.inRequest(async () => {
      assertEquals((await accessOf(EXPO, "user:ada")).active, false);
      assertEquals((await accessOf(EXPO, "user:ada")).active, false);
      assertEquals(requestReaderStats()?.hits, 1);
      await fixture.expo
        .scope(EXPO)
        .updateOne("participant", id, { status: "active" });
      assertEquals((await accessOf(EXPO, "user:ada")).active, true);
    });
  });
});

test("readers: a composite that reads a collection or a reading collection directly throws", async () => {
  await withReaders(async (fixture) => {
    const direct = reader("direct-read", async () => {
      return (await fixture.expo.scope(EXPO).find("participant")).length;
    });
    const raw = reader("raw-read", async () => {
      return (await readingCollection(fixture.db, "+expositions").findOne({}))
        ? 1
        : 0;
    });
    await fixture.inRequest(async () => {
      await assertRejects(() => direct(), ReaderDirectReadError);
      await assertRejects(() => raw(), ReaderDirectReadError);
    });
  });
});

test("readers: a composite that throws synchronously throws for every caller", async () => {
  await withReaders(async (fixture) => {
    const failing = reader("sync-throw", (() => {
      throw new Error("boom");
    }) as unknown as () => Promise<number>);
    await fixture.inRequest(async () => {
      await assertRejects(() => failing(), Error, "boom");
      await assertRejects(() => failing(), Error, "boom");
    });
  });
});

test("readers: a composite calling itself with the same arguments is refused", async () => {
  await withReaders(async (fixture) => {
    const looping: CompositeReader<[number], number> = reader(
      "self-call",
      async (n: number): Promise<number> => await looping(n),
    );
    await fixture.inRequest(async () => {
      await assertRejects(
        () => looping(1),
        ReaderDefinitionError,
        "calls itself",
      );
    });
  });
});

test("readers: composite arguments are keyed exactly, and unkeyable ones are refused", async () => {
  await withReaders(async (fixture) => {
    const echo = reader(
      "echo",
      async (value: string | null | undefined) => `${value}`,
    );
    const takesAnything = reader(
      "takes-anything",
      async (value: string) => value.length,
    );
    await fixture.inRequest(async () => {
      assertEquals(await echo(null), "null");
      assertEquals(await echo(undefined), "undefined");
      await assertRejects(
        () =>
          (takesAnything as unknown as (x: unknown) => Promise<number>)(
            new Set(["a"]),
          ),
        ReaderArgumentError,
      );
    });
  });
});

test("readers: two composites with the same name keep their own entries", async () => {
  await withReaders(async (fixture) => {
    const first = reader("same-name", async () => "first");
    const second = reader("same-name", async () => "second");
    await fixture.inRequest(async () => {
      assertEquals(await first(), "first");
      assertEquals(await second(), "second");
    });
  });
});

test("readers: a query reader not listed in its registration is refused", async () => {
  await withReaders(async (fixture) => {
    const unlisted = reader(
      "unlisted",
      from(Expo, "role")
        .by((r) => r.key)
        .select(["key"]),
    );
    await fixture.inRequest(async () => {
      await assertRejects(
        () => unlisted(EXPO, "staff"),
        ReaderNotRegisteredError,
      );
    });
  });
});

test("readers: two databases in one process share no entry", async () => {
  await withReaders(async (fixture) => {
    const other = fixture.client.db(`${fixture.db.databaseName}_b`);
    const topology = computedTopology(schemas);
    registerComputed(other, topology);
    registerReaders(other, { topology, readers: READERS });
    try {
      const otherExpo = await openExpositions(other);
      await seedParticipant(fixture, "user:ada");
      await otherExpo.scope(EXPO).insertOne("participant", {
        status: "active",
        personRef: { kind: "user", userId: "user:ada" },
        displayName: "Ada",
      });
      const statuses = await withRequestContext(async () => {
        const first = await withRequestContext(
          () => participationsOf(EXPO, "user:ada"),
          { database: fixture.db },
        );
        const second = await withRequestContext(
          () => participationsOf(EXPO, "user:ada"),
          { database: other },
        );
        return [statusOf(first), statusOf(second)];
      });
      assertEquals(statuses, ["pending", "active"]);
    } finally {
      unregisterReaders(other);
      unregisterComputed(other);
      await other.dropDatabase();
    }
  });
});

test("readers: a registered resolver gives the database inside and outside request contexts, and nothing is guessed", async () => {
  const client = new MongoClient(TEST_URI);
  const db = client.db(randomName());
  const topology = computedTopology(schemas);
  try {
    const members = await multiCollection(
      db,
      "+entreprises",
      { member: Member },
      { schemaManagement: "auto" },
    );
    await members.insertOne("member", {
      userId: "user:ada",
      tenantId: "tenant:a",
      role: "owner",
      status: "active",
    });
    const resolve = () => db;
    const otherClient = new MongoClient(TEST_URI);
    registerReaders(client, { topology, readers: READERS, database: resolve });
    registerReaders(otherClient, {
      topology,
      readers: READERS,
      database: resolve,
    });
    assertEquals((await entrepriseMemberships("user:ada")).length, 1);
    assertEquals(
      (await withRequestContext(() => entrepriseMemberships("user:ada")))
        .length,
      1,
    );
    unregisterReaders(otherClient);
    unregisterReaders(client);
    registerReaders(db, { topology, readers: READERS });
    unregisterReaders(client.db(db.databaseName));
    await assertRejects(
      () => entrepriseMemberships("user:ada"),
      ReaderDatabaseError,
    );
  } finally {
    unregisterReaders(client);
    await db.dropDatabase();
    await client.close();
  }
});

test("readers: a reader reads the primary whatever the ambient read preference", async () => {
  await withReaders(async (fixture) => {
    await seedParticipant(fixture, "user:ada");
    fixture.modes.length = 0;
    await withReadPreference("secondaryPreferred", () =>
      fixture.inRequest(() => participationsOf(EXPO, "user:ada")),
    );
    assertEquals(fixture.modes, ["primary"]);
  });
});

test("readers: past the request limit a reader loads and stores nothing", async () => {
  await withReaders(
    async (fixture) => {
      await seedParticipant(fixture, "user:ada");
      await seedParticipant(fixture, "user:bob");
      await fixture.inRequest(async () => {
        await participationsOf(EXPO, "user:ada");
        const before = fixture.finds();
        await participationsOf(EXPO, "user:bob");
        await participationsOf(EXPO, "user:bob");
        assertEquals(fixture.finds() - before, 2);
        assertEquals(requestReaderStats()?.bypasses, 2);
      });
    },
    { limits: { entriesPerRequest: 1 } },
  );
});

test("readers: a result above the row limit is returned and not stored", async () => {
  await withReaders(
    async (fixture) => {
      await seedParticipant(fixture, "user:ada");
      await seedParticipant(fixture, "user:ada", EXPO, "active");
      await fixture.inRequest(async () => {
        assertEquals((await participationsOf(EXPO, "user:ada")).length, 2);
        const before = fixture.finds();
        await participationsOf(EXPO, "user:ada");
        assertEquals(fixture.finds() - before, 1);
      });
    },
    { limits: { rowsPerEntry: 1 } },
  );
});

test("readers: registration refuses a scoped type read without its scope, an unindexed by field and duplicate names", () => {
  const unscopedSource = reader(
    "unscoped-source",
    from(ExpositionModel, "participant")
      .by((p) => p.status)
      .select(["status"]),
  );
  const client = new MongoClient(TEST_URI);
  const db = client.db("@TEST_readers_definition");
  assertThrows(
    () =>
      registerReaders(db, {
        topology: computedTopology(schemas),
        readers: [unscopedSource],
      }),
    ReaderDefinitionError,
    "scoped(Model, scopeSchema)",
  );
  const unindexed = reader(
    "unindexed",
    from(Expo, "participant")
      .by((p) => p.displayName)
      .select(["status"]),
  );
  assertThrows(
    () =>
      registerReaders(db, {
        topology: computedTopology(schemas),
        readers: [unindexed],
      }),
    ReaderDefinitionError,
    "no declared index",
  );
  assertThrows(
    () =>
      registerReaders(db, {
        topology: computedTopology(schemas),
        readers: [participationsOf, participationsOf],
      }),
    ReaderDefinitionError,
    "two readers",
  );
});

test("readers: a where declared twice on one path is refused", () => {
  assertThrows(
    () =>
      from(Expo, "participant")
        .where((p) => [p.status, "active"])
        .where((p) => [p.status, "pending"]),
    Error,
    "declared twice",
  );
});

async function seedInformation(fixture: Fixture): Promise<string> {
  await fixture.expo
    .scope(OTHER_EXPO)
    .insertOne("information", { name: "Other", lifecycle: [] });
  return await fixture.expo
    .scope(EXPO)
    .insertOne("information", { name: "Salon", lifecycle: [] });
}

function readAllInformation(fixture: Fixture) {
  return Promise.all(
    [EXPO, OTHER_EXPO].map((scope) =>
      fixture.expo.scope(scope).find("information"),
    ),
  );
}

test("readers: primeFrom fills a singleton reader from the reads it wraps, and writes still invalidate it", async () => {
  await withReaders(async (fixture) => {
    const id = await seedInformation(fixture);
    await fixture.inRequest(async () => {
      const lists = await expositionInformation.primeFrom(() =>
        readAllInformation(fixture),
      );
      assertEquals(lists.flat().length, 2);
      assertEquals(requestReaderStats()?.primed, 2);
      assertEquals((await expositionInformation(EXPO))?.name, "Salon");
      assertEquals((await expositionInformation(OTHER_EXPO))?.name, "Other");
      assertEquals(requestReaderStats()?.loads, 0);
      await fixture.expo
        .scope(EXPO)
        .updateOne("information", id, { name: "Salon 2" });
      assertEquals((await expositionInformation(EXPO))?.name, "Salon 2");
    });
  });
});

test("readers: primeFrom primes nothing off the primary, in a transaction, from a projection, or across a write", async () => {
  await withReaders(async (fixture) => {
    await seedInformation(fixture);
    const { withSession } = getSessionContext(fixture.db.client);
    await fixture.inRequest(async () => {
      await withReadPreference("secondaryPreferred", () =>
        expositionInformation.primeFrom(() => readAllInformation(fixture)),
      );
      await withSession(() =>
        expositionInformation.primeFrom(() =>
          fixture.expo.scope(EXPO).find("information"),
        ),
      );
      await expositionInformation.primeFrom(() =>
        fixture.expo.scope(EXPO).findProject("information", ["name"]),
      );
      await expositionInformation.primeFrom(() =>
        Promise.all([
          readAllInformation(fixture),
          fixture.expo
            .scope(EXPO)
            .insertOne("role", { key: "staff", permissions: [] }),
        ]),
      );
      assertEquals(requestReaderStats()?.primed, 0);
    });
  });
});

test("readers: primeFrom is refused on a reader that is not a singleton", async () => {
  await withReaders(async (fixture) => {
    await fixture.inRequest(async () => {
      const notSingleton = participationsOf as unknown as {
        primeFrom: (read: () => Promise<unknown>) => Promise<unknown>;
      };
      await assertRejects(
        () => notSingleton.primeFrom(async () => []),
        ReaderDefinitionError,
        "not a singleton",
      );
    });
  });
});

test("readers: a composite reading through a nested request context is not kept past its facts", async () => {
  await withReaders(async (fixture) => {
    const id = await seedParticipant(fixture, "user:ada");
    const nested = reader(
      "nested-access",
      async (expositionId: string, userId: string) =>
        statusOf(
          await withRequestContext(() =>
            participationsOf(expositionId, userId),
          ),
        ),
    );
    await fixture.inRequest(async () => {
      assertEquals(await nested(EXPO, "user:ada"), "pending");
      await fixture.expo
        .scope(EXPO)
        .updateOne("participant", id, { status: "active" });
      assertEquals(await nested(EXPO, "user:ada"), "active");
    });
  });
});

test("readers: many() at the request limit serves the keys it already holds", async () => {
  await withReaders(
    async (fixture) => {
      await seedMember(fixture);
      await fixture.entreprises.insertOne("member", {
        userId: "user:bob",
        tenantId: "tenant:b",
        role: "owner",
        status: "active",
      });
      await fixture.inRequest(async () => {
        await entrepriseMemberships.many(["user:ada", "user:bob"]);
        const before = fixture.finds();
        const again = await entrepriseMemberships.many([
          "user:ada",
          "user:bob",
        ]);
        assertEquals(again.get("user:bob")?.[0]?.tenantId, "tenant:b");
        assertEquals(fixture.finds(), before);
        assertEquals(requestReaderStats()?.bypasses, 0);
      });
    },
    { limits: { entriesPerRequest: 2 } },
  );
});

test("readers: a reader selecting _computed follows the recomputation of its subject", async () => {
  await withReaders(async (fixture) => {
    const id = await seedParticipant(fixture, "user:ada");
    await fixture.inRequest(async () => {
      const before = await participantComputed(EXPO, "user:ada");
      assertEquals(before[0]?._computed?.badgeCount ?? 0, 0);
      await fixture.expo.scope(EXPO).insertOne("badge", { participantId: id });
      const after = await participantComputed(EXPO, "user:ada");
      assertEquals(after[0]?._computed?.badgeCount, 1);
    });
  });
});

test("readers: with driver writes monitored, mongodbee writes keep the entries they do not touch", async () => {
  await withReaders(async (fixture) => {
    const stop = invalidateReadsOnDriverWrites(fixture.client);
    try {
      const id = await seedParticipant(fixture, "user:ada");
      await seedMember(fixture);
      await fixture.inRequest(async () => {
        await participationsOf(EXPO, "user:ada");
        await entrepriseMemberships("user:ada");
        const view = fixture.expo.scope(EXPO);
        await view.updateOne("participant", id, { displayName: "Ada L." });
        await view.insertOne("badge", { participantId: id });
        await participationsOf(EXPO, "user:ada");
        await entrepriseMemberships("user:ada");
        assertEquals(requestReaderStats()?.loads, 2);
        assertEquals(requestReaderStats()?.invalidations, 0);
      });
    } finally {
      stop();
    }
  });
});

test("readers: a reader over a plain collection is invalidated by its writes, and keeps its ObjectId ids", async () => {
  await withReaders(async (fixture) => {
    await fixture.sessions.insertOne({ userId: "user:ada", device: "phone" });
    await fixture.inRequest(async () => {
      const first = await sessionsOf("user:ada");
      assertEquals(first.length, 1);
      assert(typeof first[0]?._id === "object" && "_bsontype" in first[0]._id);
      await fixture.sessions.insertOne({
        userId: "user:ada",
        device: "laptop",
      });
      assertEquals((await sessionsOf("user:ada")).map((s) => s.device).sort(), [
        "laptop",
        "phone",
      ]);
    });
  });
});

test("readers: a composite over many() is invalidated with the keys it read", async () => {
  await withReaders(async (fixture) => {
    const id = await seedMember(fixture);
    await fixture.inRequest(async () => {
      assertEquals(await tenantsOf(["user:ada"]), ["tenant:a"]);
      assertEquals(await tenantsOf(["user:ada"]), ["tenant:a"]);
      await fixture.entreprises.updateOne("member", id, {
        tenantId: "tenant:z",
      });
      assertEquals(await tenantsOf(["user:ada"]), ["tenant:z"]);
    });
  });
});

test("readers: a composite whose inner read bypassed the cache is not kept", async () => {
  await withReaders(
    async (fixture) => {
      await seedMember(fixture);
      await fixture.inRequest(async () => {
        await tenantsOf(["user:ada", "user:bob"]);
        await tenantsOf(["user:ada", "user:bob"]);
        assertEquals(requestReaderStats()?.hits, 0);
      });
    },
    { limits: { entriesPerRequest: 1 } },
  );
});

test("readers: deleteMany, bulk updates and dropScope invalidate the readers of their type", async () => {
  await withReaders(async (fixture) => {
    const view = fixture.expo.scope(EXPO);
    const id = await seedParticipant(fixture, "user:ada");
    await fixture.inRequest(async () => {
      await participationsOf(EXPO, "user:ada");
      await view.updateMany({ participant: { [id]: { status: "active" } } });
      assertEquals(
        statusOf(await participationsOf(EXPO, "user:ada")),
        "active",
      );
      await view.deleteMany("participant", { displayName: "Ada" });
      assertEquals(await participationsOf(EXPO, "user:ada"), []);
      await seedParticipant(fixture, "user:ada");
      assertEquals((await participationsOf(EXPO, "user:ada")).length, 1);
      await fixture.expo.dropScope(EXPO, { confirm: true });
      assertEquals(await participationsOf(EXPO, "user:ada"), []);
    });
  });
});

test("readers: a rolled back transaction leaves nothing stale", async () => {
  await withReaders(async (fixture) => {
    const id = await seedParticipant(fixture, "user:ada");
    const { withSession } = getSessionContext(fixture.db.client);
    await fixture.inRequest(async () => {
      await participationsOf(EXPO, "user:ada");
      await assertRejects(() =>
        withSession(async () => {
          await fixture.expo
            .scope(EXPO)
            .updateOne("participant", id, { status: "active" });
          throw new Error("rolled back");
        }),
      );
      assertEquals(
        statusOf(await participationsOf(EXPO, "user:ada")),
        "pending",
      );
    });
  });
});

test("readers: a composite calling itself outside any request context is refused", async () => {
  const looping: CompositeReader<[number], number> = reader(
    "self-call-outside",
    async (n: number): Promise<number> => await looping(n),
  );
  await assertRejects(() => looping(1), ReaderDefinitionError, "calls itself");
});

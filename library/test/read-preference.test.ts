import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import * as m from "mongodb";
import { assert, assertEquals, assertRejects } from "./+assert.ts";
import { collection } from "../src/collection.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import { getMigrationOperationsCollection } from "../src/migration/history.ts";
import { refId } from "../src/ids.ts";
import { type Db, MongoClient } from "../src/mongodb.ts";
import { closeAllWatchers } from "../src/change-stream.ts";
import { TEST_URI } from "./+shared.ts";

/**
 * Like `withDatabase`, on a client built with `options` — e.g. the
 * `primaryPreferred` default a production URI carries.
 */
async function withClient(
  options: m.MongoClientOptions,
  work: (db: Db, client: MongoClient) => Promise<void>,
) {
  const client = new MongoClient(TEST_URI, options);
  const db = client.db(
    `@TEST_readpref@${crypto.randomUUID().replace(/-/g, "").slice(0, 8)}`,
  );
  try {
    await work(db, client);
  } finally {
    await closeAllWatchers(db);
    await db.dropDatabase();
    await client.close();
  }
}

/** The primary's address, when the client sees a replica set. */
function primaryAddress(client: MongoClient): string | undefined {
  const description = (
    client as unknown as {
      topology?: { description: m.TopologyDescription };
    }
  ).topology?.description;
  if (!description) return undefined;
  for (const server of description.servers.values()) {
    if (server.type === "RSPrimary") return server.address;
  }
  return undefined;
}

const userSchema = { name: v.string(), age: v.number() };

for (const mode of ["primaryPreferred", "secondaryPreferred"] as const) {
  test(`ReadPreference: transaction works with a ${mode} client`, async () => {
    await withClient({ readPreference: mode }, async (db) => {
      const users = await collection(db, "users", userSchema);
      await users.insertOne({ name: "Ada", age: 36 });

      const found = await users.withSession(async () => {
        const doc = await users.findOne({ name: "Ada" });
        await users.insertOne({ name: "Bob", age: 40 });
        return doc;
      });

      assertEquals(found?.name, "Ada");
      assertEquals(
        await users.countDocuments({}, { readPreference: "primary" }),
        2,
      );
    });
  });
}

test("ReadPreference: collection-level secondaryPreferred reads in a transaction", async () => {
  await withClient({}, async (db) => {
    const users = await collection(db, "users", userSchema, {
      readPreference: "secondaryPreferred",
    });
    const id = await users.insertOne({ name: "Ada", age: 36 });

    await users.withSession(async () => {
      assertEquals((await users.getById(id)).name, "Ada");
      assertEquals((await users.find({}).toArray()).length, 1);
      assertEquals(await users.countDocuments({}), 1);
      assertEquals((await users.paginate({})).data.length, 1);
    });
  });
});

test("ReadPreference: a per-call readPreference is neutralised inside a transaction", async () => {
  await withClient({}, async (db) => {
    const users = await collection(db, "users", userSchema);
    await users.insertOne({ name: "Ada", age: 36 });

    await users.withSession(async () => {
      const docs = await users
        .find({}, { readPreference: "secondary" })
        .toArray();
      assertEquals(docs.length, 1);
      assertEquals(
        await users.countDocuments({}, { readPreference: "secondary" }),
        1,
      );
    });
  });
});

test("ReadPreference: multiCollection with secondaryPreferred reads in a transaction", async () => {
  await withClient({}, async (db) => {
    const catalog = await multiCollection(
      db,
      "catalog",
      { product: { name: v.string() } },
      { readPreference: "secondaryPreferred", schemaManagement: "auto" },
    );
    const id = await catalog.insertOne("product", { name: "Pen" });

    await catalog.withSession(async () => {
      assertEquals((await catalog.getById("product", id)).name, "Pen");
      assertEquals(
        (await catalog.find("product", {}, { readPreference: "secondary" }))
          .length,
        1,
      );
      assertEquals((await catalog.paginate("product", {})).data.length, 1);
    });
  });
});

test("ReadPreference: scopedMultiCollection with secondaryPreferred reads in a transaction", async () => {
  await withClient({}, async (db) => {
    const catalog = await scopedMultiCollection(db, "catalog", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { title: v.string() } },
      readPreference: "secondaryPreferred",
    });
    const view = catalog.scope("exposition:abc");
    const id = await view.insertOne("artwork", { title: "Mona" });

    await catalog.withSession(async () => {
      assertEquals((await view.getById("artwork", id)).title, "Mona");
      assertEquals(
        (await view.find("artwork", {}, { readPreference: "secondary" }))
          .length,
        1,
      );
    });
  });
});

test("ReadPreference: index sync and migration history read the primary", async () => {
  await withClient(
    { readPreference: "secondaryPreferred", monitorCommands: true },
    async (db, client) => {
      const started: m.CommandStartedEvent[] = [];
      client.on("commandStarted", (event) => started.push(event));

      const users = await collection(
        db,
        "users",
        { name: v.string(), age: v.number() },
        { schemaManagement: "auto" },
      );
      await users.find({}).toArray();

      assertEquals(
        getMigrationOperationsCollection(db).readPreference?.mode,
        "primary",
      );

      const primary = primaryAddress(client);
      const listIndexes = started.filter(
        (e) => e.commandName === "listIndexes",
      );
      assert(listIndexes.length > 0, "index sync lists the indexes");
      for (const event of listIndexes) {
        assertEquals(
          (event.command.$readPreference as { mode?: string } | undefined)
            ?.mode ?? "primary",
          "primary",
        );
        if (primary) assertEquals(event.address, primary);
      }

      // Control: a plain read does follow the client's secondaryPreferred.
      const find = started.find((e) => e.commandName === "find");
      if (primary && find?.command.$readPreference) {
        assertEquals(
          (find.command.$readPreference as { mode: string }).mode,
          "secondaryPreferred",
        );
      }
    },
  );
});

function transientError(): m.MongoError {
  const error = new m.MongoError("simulated election");
  error.addErrorLabel("TransientTransactionError");
  return error;
}

test("ReadPreference: withSession retries a transient transaction error when asked", async () => {
  await withClient({ readPreference: "primaryPreferred" }, async (db) => {
    const users = await collection(db, "users", userSchema);

    let attempts = 0;
    await users.withSession(
      async () => {
        attempts++;
        await users.insertOne({ name: `try-${attempts}`, age: attempts });
        if (attempts === 1) throw transientError();
      },
      { retry: true },
    );

    assertEquals(attempts, 2);
    const names = (await users.find({}).toArray()).map((u) => u.name);
    assertEquals(names, ["try-2"]);
  });
});

test("ReadPreference: withSession does not retry by default", async () => {
  await withClient({}, async (db) => {
    const users = await collection(db, "users", userSchema);

    let attempts = 0;
    await assertRejects(() =>
      users.withSession(async () => {
        attempts++;
        await users.insertOne({ name: "x", age: 1 });
        throw transientError();
      }),
    );

    assertEquals(attempts, 1);
    assertEquals(await users.countDocuments({}), 0);
  });
});

const STALENESS = {
  mode: "secondaryPreferred",
  maxStalenessSeconds: 90,
} as const;

function sentPreferences(
  started: readonly m.CommandStartedEvent[],
  collectionName: string,
): unknown[] {
  return started
    .filter(
      (event) =>
        event.command.find === collectionName ||
        event.command.aggregate === collectionName,
    )
    .map((event) => event.command.$readPreference);
}

test("ReadPreference: the { mode, maxStalenessSeconds } form reaches the driver on every collection kind", async () => {
  await withClient({ monitorCommands: true }, async (db, client) => {
    const started: m.CommandStartedEvent[] = [];
    client.on("commandStarted", (event) => started.push(event));

    const users = await collection(db, "users", userSchema, {
      readPreference: STALENESS,
    });
    await users.insertOne({ name: "Ada", age: 36 });
    assertEquals(users.collection.readPreference?.mode, "secondaryPreferred");
    assertEquals(users.collection.readPreference?.maxStalenessSeconds, 90);
    assertEquals((await users.find({}).toArray()).length, 1);

    const catalog = await multiCollection(
      db,
      "catalog",
      { product: { name: v.string() } },
      { readPreference: STALENESS, schemaManagement: "auto" },
    );
    await catalog.insertOne("product", { name: "Pen" });
    assertEquals((await catalog.find("product", {})).length, 1);

    const scoped = await scopedMultiCollection(db, "scoped", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { title: v.string() } },
      readPreference: STALENESS,
    });
    const view = scoped.scope("exposition:abc");
    await view.insertOne("artwork", { title: "Mona" });
    assertEquals((await view.find("artwork", {})).length, 1);

    const plain = await collection(db, "plain", userSchema);
    await plain.insertOne({ name: "Bob", age: 40 });
    assertEquals(
      await plain.countDocuments({}, { readPreference: STALENESS }),
      1,
    );

    const plainCatalog = await multiCollection(
      db,
      "plain_catalog",
      { product: { name: v.string() } },
      { schemaManagement: "auto" },
    );
    await plainCatalog.insertOne("product", { name: "Ink" });
    assertEquals(
      (await plainCatalog.find("product", {}, { readPreference: STALENESS }))
        .length,
      1,
    );

    const plainScoped = await scopedMultiCollection(db, "plain_scoped", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: { artwork: { title: v.string() } },
    });
    const plainView = plainScoped.scope("exposition:abc");
    await plainView.insertOne("artwork", { title: "Nike" });
    assertEquals(
      (await plainView.find("artwork", {}, { readPreference: STALENESS }))
        .length,
      1,
    );

    for (const name of [
      "users",
      "catalog",
      "scoped",
      "plain",
      "plain_catalog",
      "plain_scoped",
    ]) {
      assert(
        sentPreferences(started, name).some(
          (sent) =>
            JSON.stringify(sent) ===
            JSON.stringify({
              mode: "secondaryPreferred",
              maxStalenessSeconds: 90,
            }),
        ),
        `a read on "${name}" carries the staleness bound`,
      );
    }
  });
});

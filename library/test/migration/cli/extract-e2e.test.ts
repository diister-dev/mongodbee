import { test } from "../../+harness.ts";
import {
  assert,
  assertEquals,
  assertNotEquals,
  assertRejects,
  assertStringIncludes,
} from "../../+assert.ts";
import process from "node:process";
import { withTempDir } from "./shared.ts";
import { MongoClient } from "../../../src/mongodb.ts";
import { newId } from "../../../src/ids.ts";
import { classifyCommand } from "../../../src/migration/cli/commands/classify.ts";
import { extractCommand } from "../../../src/migration/cli/commands/extract.ts";
import {
  getAppliedMigrationIds,
  markMigrationAsAdopted,
} from "../../../src/migration/state.ts";
import { buildPrivacyPlan } from "../../../src/privacy/mod.ts";
import { defineModel } from "../../../src/multi-collection-model.ts";
import { multiCollection } from "../../../src/multi-collection.ts";
import {
  discoverMultiCollectionInstances,
  getMultiCollectionMigrations,
} from "../../../src/migration/multicollection-registry.ts";
import { remapId } from "../../../src/privacy/pseudonym.ts";
import type { SchemasDefinition } from "../../../src/migration/types.ts";
import { findDanglingReferences } from "./referential-integrity.ts";
import {
  BIRTH,
  dbName,
  populateSource,
  rawCollection,
  REAL,
  type SourceWorld,
  TEST_URI,
  withProject,
  writeProject,
} from "./extract-fixture.ts";

const SECRET = "test-secret";

const e2e = (name: string, fn: () => Promise<void>) =>
  test({ name, fn, timeout: 60_000 });

async function captureOutput<T>(work: () => Promise<T>): Promise<{
  result?: T;
  error?: unknown;
  output: string;
  stderr: string;
  combined: string;
}> {
  const out: string[] = [];
  const err: string[] = [];
  const originals = { log: console.log, error: console.error };
  console.log = (...args: unknown[]) => {
    out.push(args.map(String).join(" "));
  };
  console.error = (...args: unknown[]) => {
    err.push(args.map(String).join(" "));
  };
  const report = () => ({
    output: out.join("\n"),
    stderr: err.join("\n"),
    combined: [...out, ...err].join("\n"),
  });
  try {
    return { result: await work(), ...report() };
  } catch (error) {
    return { error, ...report() };
  } finally {
    console.log = originals.log;
    console.error = originals.error;
  }
}

async function withSourceAndTarget(
  work: (context: {
    dir: string;
    client: MongoClient;
    source: string;
    target: string;
    world: SourceWorld;
    schemas: SchemasDefinition;
  }) => Promise<void>,
): Promise<void> {
  await withProject(async (dir) => {
    const source = dbName("src");
    const target = dbName("dst");
    const client = new MongoClient(TEST_URI);
    await client.connect();
    try {
      const world = await populateSource(client.db(source));
      const { schemas } = await import(`${dir}/schemas.ts`);
      await work({ dir, client, source, target, world, schemas });
    } finally {
      await client.db(source).dropDatabase();
      await client.db(target).dropDatabase();
      await client.close();
    }
  });
}

e2e(
  "extract: multi-model instances keep their transformed documents under a remapped name that still joins its exposition",
  async () => {
    await withSourceAndTarget(
      async ({ dir, client, source, target, world, schemas }) => {
        await extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: SECRET,
          json: true,
        });
        const out = client.db(target);
        const expectedNames = world.instanceNames.map((n) =>
          remapId(SECRET, n),
        );
        const names = await discoverMultiCollectionInstances(out, "exposition");
        assertEquals(names, [...expectedNames].sort());
        for (const name of world.instanceNames) {
          assertEquals(
            await out.listCollections({ name }).toArray(),
            [],
            "the real instance name must not survive",
          );
        }

        const expoIds = (
          await rawCollection(out, "expositions").find({}).toArray()
        )
          .map((d) => d._id)
          .sort();
        assertEquals(
          expoIds,
          [...expectedNames].sort(),
          "instance names join the remapped exposition ids",
        );

        const model = defineModel("exposition", {
          schema: schemas.multiModels?.exposition ?? {},
        });
        const instance = await multiCollection(out, expectedNames[0], model, {
          schemaManagement: "managed",
        });
        const zones = await instance.find("zone", {});
        assertEquals(zones.length, 2);
        const userIds = new Set(
          (await rawCollection(out, "users").find({}).toArray()).map(
            (u) => u._id,
          ),
        );
        for (const zone of zones) {
          assert(
            userIds.has(zone.ownerId as string),
            "zone.ownerId joins a user",
          );
        }
        const migrations = await getMultiCollectionMigrations(
          out,
          expectedNames[0],
        );
        assertEquals(
          migrations?.appliedMigrations.map((m) => m.id),
          [BIRTH],
        );
      },
    );
  },
);

e2e(
  "extract: every remapped reference of every target joins a document of the target database",
  async () => {
    await withSourceAndTarget(
      async ({ dir, client, source, target, schemas }) => {
        await extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: SECRET,
          json: true,
        });
        const plan = buildPrivacyPlan({ schemas });
        const dangling = await findDanglingReferences(
          client.db(target),
          schemas,
          plan,
        );
        assertEquals(dangling, []);

        const [user] = await client
          .db(target)
          .collection("users")
          .find({})
          .toArray();
        await client
          .db(target)
          .collection("users")
          .deleteOne({ _id: user._id });
        const broken = await findDanglingReferences(
          client.db(target),
          schemas,
          plan,
        );
        assert(
          broken.length > 0 &&
            broken.every((d) => d.value === String(user._id)),
          "the helper must see the references of a deleted user",
        );
      },
    );
  },
);

e2e(
  "extract: the target database gets the validators and unique indexes of the head schemas",
  async () => {
    await withSourceAndTarget(async ({ dir, client, source, target }) => {
      await extractCommand({
        cwd: dir,
        fromDb: source,
        toDb: target,
        secret: SECRET,
        json: true,
      });
      const out = client.db(target);
      const usersIndexes = await out.collection("users").indexes();
      assert(
        usersIndexes.some((i) => i.unique && i.key.email === 1),
        `expected a unique email index, got ${JSON.stringify(usersIndexes)}`,
      );
      const listing = await out.command({
        listCollections: 1,
        filter: { name: "users" },
      });
      assertNotEquals(
        listing.cursor.firstBatch[0].options?.validator,
        undefined,
      );
      const scanIndexes = await out.collection("scans").indexes();
      assert(scanIndexes.some((i) => i.key._type === 1));
    });
  },
);

e2e(
  "extract: schema collections without documents are still created with their indexes",
  async () => {
    await withSourceAndTarget(async ({ dir, client, source, target }) => {
      await client.db(source).collection("expo").deleteMany({});
      await extractCommand({
        cwd: dir,
        fromDb: source,
        toDb: target,
        secret: SECRET,
        json: true,
      });
      const names = (await client.db(target).listCollections().toArray()).map(
        (c) => c.name,
      );
      assert(names.includes("expo"), `expo missing among ${names}`);
    });
  },
);

e2e(
  "extract: a pseudonym collision fails loudly, names the index, leaves nothing behind and prints no real value",
  async () => {
    await withSourceAndTarget(async ({ dir, client, source, target }) => {
      const twin = "ALICE.REAL@acme-corp.example";
      await rawCollection(client.db(source), "users").insertOne({
        _id: `user:${newId()}`,
        email: twin,
        firstname: "Alicetwin",
        role: "member",
      });
      const gated = await captureOutput(() =>
        extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: SECRET,
        }),
      );
      assert(gated.error instanceof Error);
      assertStringIncludes(gated.error.message, "unique_index");
      assertEquals(await client.db(target).listCollections().toArray(), []);

      const { error, combined: output } = await captureOutput(() =>
        extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: SECRET,
          allowViolations: true,
        }),
      );
      assert(error instanceof Error);
      assertStringIncludes(error.message, "users");
      assertStringIncludes(error.message, "duplicate");
      for (const real of [...REAL.emails, twin, "Alicetwin"]) {
        assert(!error.message.includes(real), "error leaks a real value");
        assert(!output.includes(real), "output leaks a real value");
      }
      assertEquals(
        await client.db(target).listCollections().toArray(),
        [],
        "a failed extract must not leave a half-written database",
      );
    });
  },
);

e2e(
  "extract: refuses to write into its own source whatever the URI spelling",
  async () => {
    await withSourceAndTarget(async ({ dir, source }) => {
      const alias = TEST_URI.replace("localhost", "127.0.0.1");
      assertNotEquals(alias, TEST_URI, "the test needs a localhost URI");
      await assertRejects(
        () =>
          extractCommand({
            cwd: dir,
            from: TEST_URI,
            to: `${alias}/`,
            fromDb: source,
            toDb: source,
            secret: SECRET,
            json: true,
          }),
        Error,
        "own source",
      );
    });
  },
);

e2e(
  "extract: a non-empty target needs --force, which merges without touching existing data and baselines the ledger once",
  async () => {
    await withSourceAndTarget(async ({ dir, client, source, target }) => {
      const out = client.db(target);
      await rawCollection(out, "unrelated").insertOne({
        _id: "keep",
        value: "mine",
      });
      await assertRejects(
        () =>
          extractCommand({
            cwd: dir,
            fromDb: source,
            toDb: target,
            secret: SECRET,
            json: true,
          }),
        Error,
        "already holds",
      );
      const options = {
        cwd: dir,
        fromDb: source,
        toDb: target,
        secret: SECRET,
        json: true,
        force: true,
      };
      await captureOutput(() => extractCommand(options));
      assertEquals(await rawCollection(out, "unrelated").countDocuments(), 1);
      assertEquals(await rawCollection(out, "users").countDocuments(), 4);
      assertEquals(await getAppliedMigrationIds(out), [BIRTH]);

      const again = await captureOutput(() => extractCommand(options));
      assert(
        again.error instanceof Error,
        "a second merge must collide loudly",
      );
      assertEquals(await rawCollection(out, "users").countDocuments(), 4);
      assertEquals(await getAppliedMigrationIds(out), [BIRTH]);
    });
  },
);

e2e(
  "extract: documents of a _type the schema does not declare are counted in the summary, not silently dropped",
  async () => {
    await withSourceAndTarget(async ({ dir, client, source, target }) => {
      const { result, error } = await captureOutput(async () => {
        await extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: SECRET,
          json: true,
        });
      });
      assertEquals(error, undefined, String(result));
      const { output } = await captureOutput(() =>
        extractCommand({
          cwd: dir,
          fromDb: source,
          dryRun: true,
          secret: SECRET,
          json: true,
        }),
      );
      const summary = JSON.parse(output);
      assertEquals(summary.skipped, {
        "multiCollections/scans": { ghost: 1, _hidden: 1 },
        "multiModels/exposition": { _audit: 2 },
      });
      assertEquals(
        await client.db(target).collection("scans").countDocuments({
          _type: "ghost",
        }),
        0,
        "undeclared types are never copied",
      );
    });
  },
);

e2e(
  "extract: --scope reports the references its cut leaves dangling",
  async () => {
    await withSourceAndTarget(async ({ dir, source, world }) => {
      const { output } = await captureOutput(() =>
        extractCommand({
          cwd: dir,
          fromDb: source,
          dryRun: true,
          secret: SECRET,
          scope: world.expositionIds[0],
          json: true,
        }),
      );
      const summary = JSON.parse(output);
      const dangling = summary.violations;
      assertEquals(dangling.length, 1);
      assertEquals(dangling[0].kind, "owner_unresolved");
      assertEquals(dangling[0].target, "multiCollections/scans/note");
    });
  },
);

e2e(
  "extract: stdout, json summary and dry-run never contain a real value",
  async () => {
    await withSourceAndTarget(async ({ dir, source, target }) => {
      const real = [
        ...REAL.emails,
        ...REAL.firstnames,
        ...REAL.badges,
        ...REAL.zoneLabels,
      ];
      for (const options of [
        { json: true },
        { json: false },
        {
          dryRun: true,
          json: true,
        },
      ]) {
        const { combined: output, error } = await captureOutput(() =>
          extractCommand({
            cwd: dir,
            fromDb: source,
            toDb: `${target}_${options.dryRun ? "dry" : options.json ? "j" : "t"}`,
            secret: SECRET,
            ...options,
          }),
        );
        assertEquals(error, undefined);
        for (const value of real) {
          assert(!output.includes(value), `output leaks ${value}`);
        }
        assert(!output.includes(SECRET), "output leaks the secret");
      }
    });
  },
);

e2e(
  "extract: the strict posture is the default and fakes values the schemas do not declare",
  async () => {
    await withSourceAndTarget(async ({ dir, client, source, target }) => {
      const names = (database: string) =>
        rawCollection(client.db(database), "expositions")
          .find({})
          .toArray()
          .then((docs) => docs.map((d) => String(d.name)));
      await captureOutput(() =>
        extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: SECRET,
          json: true,
        }),
      );
      for (const name of await names(target)) {
        assert(
          !(REAL.expositionNames as readonly string[]).includes(name),
          "an undeclared string must not survive the default posture",
        );
      }
      const personalTarget = `${target}_personal`;
      await captureOutput(() =>
        extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: personalTarget,
          secret: SECRET,
          posture: "personal",
          json: true,
        }),
      );
      assertEquals(
        (await names(personalTarget)).sort(),
        [...REAL.expositionNames].sort(),
      );
      await client.db(personalTarget).dropDatabase();
    });
  },
);

e2e(
  "extract: a literal --secret is warned about, an env: secret is not, and neither is ever printed",
  async () => {
    await withSourceAndTarget(async ({ dir, source, target }) => {
      const literal = await captureOutput(() =>
        extractCommand({
          cwd: dir,
          fromDb: source,
          dryRun: true,
          secret: SECRET,
          json: true,
        }),
      );
      assertStringIncludes(literal.stderr, "env:NAME");
      assert(!literal.combined.includes(SECRET));

      process.env.EXTRACT_TEST_SECRET = "from-the-environment";
      try {
        const fromEnv = await captureOutput(() =>
          extractCommand({
            cwd: dir,
            fromDb: source,
            toDb: target,
            secret: "env:EXTRACT_TEST_SECRET",
          }),
        );
        assertEquals(fromEnv.error, undefined);
        assert(!fromEnv.stderr.includes("env:NAME"));
        assert(!fromEnv.combined.includes("from-the-environment"));
      } finally {
        delete process.env.EXTRACT_TEST_SECRET;
      }
    });
  },
);

e2e(
  "extract: multi-model bookkeeping is rebuilt from known fields, with its times shifted and no error text",
  async () => {
    await withSourceAndTarget(
      async ({ dir, client, source, target, world }) => {
        await captureOutput(() =>
          extractCommand({
            cwd: dir,
            fromDb: source,
            toDb: target,
            secret: SECRET,
            shiftDays: 2,
            json: true,
          }),
        );
        const out = client.db(target);
        const name = remapId(SECRET, world.instanceNames[0], 2 * 86_400_000);
        const docs = await rawCollection(out, name).find({}).toArray();
        assert(
          !docs.some((d) => String(d._type).startsWith("_a")),
          "an underscore-prefixed type that is not bookkeeping is not copied",
        );
        const info = docs.find((d) => d._type === "_information");
        assertEquals(
          info?.createdAt,
          new Date("2026-01-03T00:00:00Z"),
          "the instance creation time is shifted",
        );
        const migrations = docs.find((d) => d._type === "_migrations") as
          | { appliedMigrations: Record<string, unknown>[] }
          | undefined;
        const [applied] = migrations?.appliedMigrations ?? [];
        assertEquals(applied.appliedAt, new Date("2026-01-03T00:00:00Z"));
        assertEquals("error" in applied, false);
      },
    );
  },
);

e2e(
  "extract: the --json summary echoes the remapped scope, never the source one",
  async () => {
    await withSourceAndTarget(async ({ dir, source, world }) => {
      const scope = world.expositionIds[0];
      const { output } = await captureOutput(() =>
        extractCommand({
          cwd: dir,
          fromDb: source,
          dryRun: true,
          secret: SECRET,
          scope,
          json: true,
        }),
      );
      assertEquals(JSON.parse(output).scope.scope, remapId(SECRET, scope));
      assert(!output.includes(scope.split(":")[1]));
    });
  },
);

e2e(
  "extract: a project without a migrations directory extracts with the indexes of schemas.ts",
  async () => {
    await withTempDir(async (dir) => {
      await writeProject(dir, { migrations: false });
      const source = dbName("src");
      const target = dbName("dst");
      const client = new MongoClient(TEST_URI);
      await client.connect();
      try {
        await populateSource(client.db(source));
        await captureOutput(() =>
          extractCommand({
            cwd: dir,
            fromDb: source,
            toDb: target,
            secret: SECRET,
            json: true,
          }),
        );
        const out = client.db(target);
        assertEquals(await rawCollection(out, "users").countDocuments(), 4);
        const indexes = await out.collection("users").indexes();
        assert(indexes.some((i) => i.unique && i.key.email === 1));
        assertEquals(await getAppliedMigrationIds(out), []);
      } finally {
        await client.db(source).dropDatabase();
        await client.db(target).dropDatabase();
        await client.close();
      }
    });
  },
);

e2e(
  "extract: data that breaks its own schemas is reported in the summary and only written with --allow-violations",
  async () => {
    await withSourceAndTarget(async ({ dir, client, source }) => {
      await rawCollection(client.db(source), "users").insertOne({
        _id: `user:${newId()}`,
        email: REAL.emails[0].toUpperCase(),
        firstname: "Brokenreal",
        role: "member",
      });
      const preview = await captureOutput(() =>
        extractCommand({
          cwd: dir,
          fromDb: source,
          dryRun: true,
          secret: SECRET,
          allowViolations: true,
          json: true,
        }),
      );
      assertEquals(preview.error, undefined);
      const summary = JSON.parse(preview.output);
      assertEquals(summary.violationsAllowed, true);
      assertEquals(
        summary.violations.map((v: { kind: string }) => v.kind),
        ["unique_index"],
      );
      assert(!preview.combined.includes("Brokenreal"));
    });
  },
);

e2e(
  "extract: a source ledger the local chain does not know is refused, a ledger at the head is read as is",
  async () => {
    await withSourceAndTarget(async ({ dir, client, source, target }) => {
      await markMigrationAsAdopted(
        client.db(source),
        "2030_01_01_0000_FUTURE01@future",
        "future",
      );
      const refused = await captureOutput(() =>
        extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: SECRET,
          json: true,
        }),
      );
      assert(refused.error instanceof Error);
      assertStringIncludes(refused.error.message, "FUTURE01");
      assertEquals(await client.db(target).listCollections().toArray(), []);
    });
  },
);

e2e(
  "classify: previews exactly what extract uses, the head migration schemas under the strict posture",
  async () => {
    await withProject(async (dir) => {
      const plans: Record<string, unknown>[] = [];
      const { output } = await captureOutput(() =>
        classifyCommand({ cwd: dir, json: true }),
      );
      plans.push(JSON.parse(output));
      const viaAt = await captureOutput(() =>
        classifyCommand({ cwd: dir, at: BIRTH, json: true }),
      );
      plans.push(JSON.parse(viaAt.output));
      assertEquals(plans[0], plans[1]);
      assertEquals(plans[0].posture, "strict");
    });
  },
);

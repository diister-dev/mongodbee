import { mkdir, writeFile } from "node:fs/promises";
import process from "node:process";
import { computedValues } from "../../+shared.ts";
import { assert, assertEquals, assertNotEquals } from "../../+assert.ts";
import { test } from "../../+harness.ts";
import { computedTopology } from "../../../src/computed-topology.ts";
import { checkComputed } from "../../../src/computed-apply.ts";
import { newId } from "../../../src/ids.ts";
import { extractCommand } from "../../../src/migration/cli/commands/extract.ts";
import { ObjectId } from "mongodb";
import { MongoClient } from "../../../src/mongodb.ts";
import { withTempDir } from "./shared.ts";

const TEST_URI =
  process.env.TEST_MONGODB_URI ||
  process.env.MONGODBEE_TEST_URI ||
  "mongodb://localhost:27017";
const SRC = new URL("../../../src/", import.meta.url).href;
const BIRTH = "2026_01_01_0900_BIRTH01@birth";

type Raw = { _id: string; [key: string]: unknown };

function names(tag: string) {
  const suffix = crypto.randomUUID().replace(/-/g, "").slice(0, 8);
  return {
    source: `mongodbee_test_hooks_${tag}_src_${suffix}`,
    target: `mongodbee_test_hooks_${tag}_dst_${suffix}`,
  };
}

async function writeProject(
  dir: string,
  schemasSource: string,
  extraConfig = "",
): Promise<void> {
  await mkdir(`${dir}/migrations`, { recursive: true });
  await writeFile(
    `${dir}/mongodbee.config.ts`,
    `${extraConfig.includes("import") ? extraConfig.split("\n---\n")[0] : ""}
export default { database: { connection: { uri: ${JSON.stringify(TEST_URI)} } }, paths: { migrations: "./migrations", schemas: "./schemas.ts" }${
      extraConfig.includes("---") ? extraConfig.split("\n---\n")[1] : ""
    } };`,
  );
  await writeFile(`${dir}/schemas.ts`, schemasSource);
  await writeFile(
    `${dir}/migrations/${BIRTH}.ts`,
    `
import { migrationDefinition } from "${SRC}migration/definition.ts";
import { schemas } from "../schemas.ts";
export default migrationDefinition(${JSON.stringify(BIRTH)}, "birth", {
  parent: null,
  schemas,
  migrate: (b) => b.compile(),
});
`,
  );
}

const COMPUTED_SCHEMAS = `
import * as v from "${SRC}schema.ts";
import { refId } from "${SRC}ids.ts";
import { withIndex } from "${SRC}indexes.ts";
import { defineType } from "${SRC}type-definition.ts";
import { from } from "${SRC}computed.ts";
import { personal, personId } from "${SRC}privacy/mod.ts";

export const Membership = defineType({
  schema: v.object({
    participantId: withIndex(refId("participant")),
    organizationId: refId("organization"),
    label: personal(v.string(), { role: "content" }),
  }),
});

export const Participant = defineType({
  schema: v.object({
    _id: personId("participant"),
    name: personal(v.string(), { role: "direct" }),
  }),
  computed: {
    organizationIds: from("org_membership", Membership)
      .by((m) => m.participantId)
      .collect((m) => m.organizationId)
      .maxEntries(20),
    membershipCount: from("org_membership", Membership)
      .by((m) => m.participantId)
      .count(),
  },
});

export const schemas = {
  scopedMultiCollections: {
    "+expositions": {
      scope: refId("exposition"),
      types: { participant: Participant, org_membership: Membership },
    },
  },
};
`;

test({
  name: "extract: a type with computed fields is written, reads back through mongodbee and matches its recomputation",
  timeout: 90_000,
  fn: async () => {
    await withTempDir(async (dir) => {
      await writeProject(dir, COMPUTED_SCHEMAS);
      const { source, target } = names("computed");
      const client = new MongoClient(TEST_URI);
      await client.connect();
      try {
        const scope = `exposition:${newId()}`;
        const participantId = `participant:${newId()}`;
        const organizationId = `organization:${newId()}`;
        await client
          .db(source)
          .collection<Raw>("+expositions")
          .insertMany([
            {
              _id: participantId,
              _type: "participant",
              _scope: scope,
              name: "Realname",
              _computed: {
                organizationIds: [organizationId],
                membershipCount: 1,
                _rev: 4,
              },
            },
            {
              _id: `org_membership:${newId()}`,
              _type: "org_membership",
              _scope: scope,
              participantId,
              organizationId,
              label: "Real secret club",
            },
          ]);
        await extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: "hooks-secret",
          json: true,
        });
        const out = client.db(target);
        const { schemas } = await import(`${dir}/schemas.ts`);
        const check = await checkComputed(out, computedTopology(schemas));
        assertEquals(check.checked, 1);
        assertEquals(check.drifts, []);
        const stored = await out
          .collection<Raw>("+expositions")
          .findOne({ _type: "participant" });
        const membership = await out
          .collection<Raw>("+expositions")
          .findOne({ _type: "org_membership" });
        assertEquals(computedValues(stored?._computed), {
          organizationIds: [membership?.organizationId],
          membershipCount: 1,
        });
        const computed = stored?._computed as
          | { organizationIds: string[]; _rev: number }
          | undefined;
        assertNotEquals(computed?.organizationIds, [organizationId]);
        assertEquals(computed?._rev, 4);
      } finally {
        await client.db(source).dropDatabase();
        await client.db(target).dropDatabase();
        await client.close();
      }
    });
  },
});

const DYNAMIC_SCHEMAS = `
import * as v from "${SRC}schema.ts";
import { dynamic, personal, personId } from "${SRC}privacy/mod.ts";
export const schemas = {
  collections: {
    users: {
      _id: personId("user"),
      email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
      fields: dynamic(v.record(v.string(), v.object({ t: v.string(), v: v.string() }))),
    },
  },
};
`;

const RESOLVER_CONFIG = `import { SKIP_DYNAMIC } from "${SRC}privacy/mod.ts";
---
, privacy: {
  resolveDynamic: (unit: { value: { t?: string } }) =>
    unit.value.t === "public"
      ? { t: { role: "technical" }, v: { role: "technical" } }
      : SKIP_DYNAMIC,
}`;

test({
  name: "extract: the privacy hook of the config file resolves dynamic subtrees",
  timeout: 90_000,
  fn: async () => {
    await withTempDir(async (dir) => {
      await writeProject(dir, DYNAMIC_SCHEMAS, RESOLVER_CONFIG);
      const { source, target } = names("dynamic");
      const client = new MongoClient(TEST_URI);
      await client.connect();
      try {
        await client
          .db(source)
          .collection<Raw>("users")
          .insertOne({
            _id: `user:${newId()}`,
            email: "dyn.real@acme-corp.example",
            fields: {
              a: { t: "public", v: "kept-because-resolved" },
              b: { t: "private", v: "Real private answer" },
            },
          });
        await extractCommand({
          cwd: dir,
          fromDb: source,
          toDb: target,
          secret: "hooks-secret",
          json: true,
        });
        const [user] = await client
          .db(target)
          .collection<Raw>("users")
          .find({})
          .toArray();
        const fields = user.fields as Record<string, { t: string; v: string }>;
        assertEquals(
          Object.values(fields)
            .filter((f) => f.t === "public")
            .map((f) => f.v),
          ["kept-because-resolved"],
        );
        assert(
          !JSON.stringify(user).includes("Real private answer"),
          "an unresolved entry is faked",
        );
      } finally {
        await client.db(source).dropDatabase();
        await client.db(target).dropDatabase();
        await client.close();
      }
    });
  },
});

const OBJECT_ID_SCHEMAS = `
import * as v from "${SRC}schema.ts";
import { ObjectId } from "mongodb";
import { personal, personId } from "${SRC}privacy/mod.ts";
export const schemas = {
  collections: {
    users: {
      _id: personId("user"),
      email: personal(v.pipe(v.string(), v.email()), { role: "direct" }),
      token: personal(v.instance(ObjectId), { role: "direct" }),
    },
  },
};
`;

test({
  name: "extract: a generator warning is printed once, not once per document",
  timeout: 90_000,
  fn: async () => {
    await withTempDir(async (dir) => {
      await writeProject(dir, OBJECT_ID_SCHEMAS);
      const { source } = names("warn");
      const client = new MongoClient(TEST_URI);
      await client.connect();
      const warnings: string[] = [];
      const original = console.warn;
      try {
        await client
          .db(source)
          .collection<Raw>("users")
          .insertMany(
            Array.from({ length: 5 }, (_, index) => ({
              _id: `user:${newId()}`,
              email: `warn${index}@acme-corp.example`,
              token: new ObjectId(),
            })),
          );
        console.warn = (...args: unknown[]) => {
          warnings.push(args.map(String).join(" "));
        };
        await extractCommand({
          cwd: dir,
          fromDb: source,
          dryRun: true,
          secret: "hooks-secret",
          allowViolations: true,
          json: true,
        });
      } finally {
        console.warn = original;
        await client.db(source).dropDatabase();
        await client.close();
      }
      const handlers = warnings.filter((w) => w.includes("No handler"));
      assert(handlers.length <= 1, `${handlers.length} identical warnings`);
    });
  },
});

import { test } from "../../+harness.ts";
import { MongoServerError } from "mongodb";
import {
  assertEquals,
  assertRejects,
  assertStringIncludes,
} from "../../+assert.ts";
import type { MongoClient } from "../../../src/mongodb.ts";
import {
  compareWithSource,
  type DatabaseFingerprint,
  fingerprintDatabase,
  isAuthorizationRefusal,
  type ServerTopology,
  sourceRefusal,
} from "../../../src/migration/cli/commands/extract.ts";

const fingerprint = (
  topology: ServerTopology,
  ids: { deploymentId?: string; clusterId?: string | null; processId?: string },
  ...collectionIds: string[]
): DatabaseFingerprint => ({
  ...ids,
  topology,
  collectionIds: new Set(collectionIds),
});

test("compareWithSource: a shared collection identifies the database across processes", () => {
  assertEquals(
    compareWithSource(
      fingerprint("replicaSet", { processId: "secondary" }, "a", "b"),
      fingerprint("replicaSet", { processId: "primary" }, "b"),
      true,
    ),
    "same",
  );
});

test("compareWithSource: a shared collection identifies the database whatever its name", () => {
  assertEquals(
    compareWithSource(
      fingerprint("standalone", { deploymentId: "x" }, "a"),
      fingerprint("standalone", { deploymentId: "y" }, "a"),
      false,
    ),
    "same",
  );
});

test("compareWithSource: another name on the same deployment is another database", () => {
  assertEquals(
    compareWithSource(
      fingerprint("standalone", { deploymentId: "x", processId: "p" }),
      fingerprint("standalone", { deploymentId: "x", processId: "p" }),
      false,
    ),
    "distinct",
  );
});

test("compareWithSource: an empty database read on a secondary and written on the primary is the source", () => {
  assertEquals(
    compareWithSource(
      fingerprint("replicaSet", { deploymentId: "rs", processId: "secondary" }),
      fingerprint("replicaSet", { deploymentId: "rs", processId: "primary" }),
      true,
    ),
    "same",
  );
});

test("compareWithSource: two routers of one cluster reach the same database", () => {
  assertEquals(
    compareWithSource(
      fingerprint("router", { deploymentId: "cluster", processId: "router1" }),
      fingerprint("router", { deploymentId: "cluster", processId: "router2" }),
      true,
    ),
    "same",
  );
});

test("compareWithSource: distinct deployments holding a same-named database are told apart", () => {
  assertEquals(
    compareWithSource(
      fingerprint("replicaSet", { deploymentId: "one", processId: "p" }, "a"),
      fingerprint("replicaSet", { deploymentId: "two", processId: "p" }, "b"),
      true,
    ),
    "distinct",
  );
});

test("compareWithSource: without a deployment id, the same process is the source", () => {
  assertEquals(
    compareWithSource(
      fingerprint("replicaSet", { processId: "p" }),
      fingerprint("replicaSet", { processId: "p" }),
      true,
    ),
    "same",
  );
});

test("compareWithSource: without a deployment id, two standalone processes are distinct servers", () => {
  assertEquals(
    compareWithSource(
      fingerprint("standalone", { processId: "one" }),
      fingerprint("standalone", { processId: "two" }),
      true,
    ),
    "distinct",
  );
});

test("compareWithSource: without a deployment id, two replica set members cannot be told apart", () => {
  assertEquals(
    compareWithSource(
      fingerprint("replicaSet", { processId: "secondary" }),
      fingerprint("replicaSet", { processId: "primary" }),
      true,
    ),
    "undecidable",
  );
});

test("compareWithSource: without a deployment id, two routers cannot be told apart", () => {
  assertEquals(
    compareWithSource(
      fingerprint("router", { processId: "router1" }),
      fingerprint("router", { processId: "router2" }),
      true,
    ),
    "undecidable",
  );
});

test("compareWithSource: a server that identifies nothing is undecidable, not distinct", () => {
  assertEquals(
    compareWithSource(
      fingerprint("standalone", {}),
      fingerprint("standalone", { processId: "p" }),
      true,
    ),
    "undecidable",
  );
});

test("compareWithSource: a deployment id on one side only falls back to the process", () => {
  assertEquals(
    compareWithSource(
      fingerprint("standalone", { deploymentId: "x", processId: "one" }),
      fingerprint("standalone", { processId: "two" }),
      true,
    ),
    "distinct",
  );
});

test("sourceRefusal: the source itself is refused", () => {
  assertStringIncludes(String(sourceRefusal("same", "db")), "own source");
});

test("sourceRefusal: an undecidable target is refused, naming the database to change", () => {
  const refusal = String(sourceRefusal("undecidable", "db"));
  assertStringIncludes(refusal, "cannot tell");
  assertStringIncludes(refusal, '"db"');
  assertStringIncludes(refusal, "--to-db");
});

test("sourceRefusal: a distinct target is let through", () => {
  assertEquals(sourceRefusal("distinct", "db"), undefined);
});

const shard = (clusterId: string | undefined) =>
  fingerprint("replicaSet", {
    deploymentId: "shard",
    clusterId,
    processId: "shard",
  });
const router = (clusterId: string | undefined) =>
  fingerprint("router", {
    deploymentId: "config",
    clusterId,
    processId: "router",
  });

test("compareWithSource: a shard read directly and written through its router is the source", () => {
  assertEquals(compareWithSource(shard("c1"), router("c1"), true), "same");
});

test("compareWithSource: a router read and a shard of its cluster written directly is the source", () => {
  assertEquals(compareWithSource(router("c1"), shard("c1"), true), "same");
});

test("compareWithSource: two shards of one cluster are the same database", () => {
  assertEquals(
    compareWithSource(
      shard("c1"),
      fingerprint("replicaSet", {
        deploymentId: "other-shard",
        clusterId: "c1",
        processId: "p2",
      }),
      true,
    ),
    "same",
  );
});

test("compareWithSource: a router and a shard of another cluster are distinct", () => {
  assertEquals(compareWithSource(router("c1"), shard("c2"), true), "distinct");
});

test("compareWithSource: a router and a server outside any cluster are distinct", () => {
  assertEquals(
    compareWithSource(
      router("c1"),
      fingerprint("replicaSet", {
        deploymentId: "rs",
        clusterId: null,
        processId: "p",
      }),
      true,
    ),
    "distinct",
  );
});

test("compareWithSource: a shard whose router hides its cluster id is undecidable", () => {
  assertEquals(
    compareWithSource(shard("c1"), router(undefined), true),
    "undecidable",
  );
});

test("compareWithSource: a router facing a server whose cluster id is hidden is undecidable", () => {
  assertEquals(
    compareWithSource(router("c1"), shard(undefined), true),
    "undecidable",
  );
});

test("isAuthorizationRefusal: code 13 and the authorization code names are refusals, nothing else", () => {
  assertEquals(
    isAuthorizationRefusal(new MongoServerError({ message: "no", code: 13 })),
    true,
  );
  assertEquals(
    isAuthorizationRefusal(
      new MongoServerError({
        message: "no",
        code: 0,
        codeName: "AuthorizationFailure",
      }),
    ),
    true,
  );
  assertEquals(
    isAuthorizationRefusal(
      new MongoServerError({
        message: "no",
        code: 0,
        codeName: "Unauthorized",
      }),
    ),
    true,
  );
  assertEquals(
    isAuthorizationRefusal(
      new MongoServerError({
        message: "down",
        code: 91,
        codeName: "ShutdownInProgress",
      }),
    ),
    false,
  );
  assertEquals(isAuthorizationRefusal(new Error("socket closed")), false);
});

function fakeClient(adminError: Error): MongoClient {
  const fail = async () => {
    throw adminError;
  };
  const db = (name: string) => ({
    command: async () => ({ topologyVersion: { processId: "p" } }),
    listCollections: () => ({
      toArray: name === "admin" ? fail : async () => [],
    }),
    collection: () => ({ findOne: name === "admin" ? fail : async () => null }),
  });
  // @ts-expect-error a client reduced to the calls fingerprintDatabase makes
  return { db } satisfies Partial<Record<keyof MongoClient, unknown>>;
}

test("fingerprintDatabase: identities an account may not read are left unknown", async () => {
  const refused = new MongoServerError({
    message: "not authorized",
    code: 0,
    codeName: "AuthorizationFailure",
  });
  const fingerprint = await fingerprintDatabase(fakeClient(refused), "db");
  assertEquals(fingerprint.deploymentId, undefined);
  assertEquals(fingerprint.clusterId, undefined);
  assertEquals(fingerprint.processId, "p");
});

test("fingerprintDatabase: any other failure to read an identity stops extract", async () => {
  await assertRejects(
    () => fingerprintDatabase(fakeClient(new Error("socket closed")), "db"),
    Error,
    "could not read the deployment id",
  );
});

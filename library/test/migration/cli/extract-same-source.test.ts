import { test } from "../../+harness.ts";
import { assertEquals, assertStringIncludes } from "../../+assert.ts";
import {
  compareWithSource,
  type DatabaseFingerprint,
  type ServerTopology,
  sourceRefusal,
} from "../../../src/migration/cli/commands/extract.ts";

const fingerprint = (
  topology: ServerTopology,
  ids: { deploymentId?: string; processId?: string },
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

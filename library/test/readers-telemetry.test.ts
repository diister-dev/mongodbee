import { test } from "./+harness.ts";
import { dumpSpans, makeTestTelemetry } from "./+telemetry.ts";
import * as v from "../src/schema.ts";
import { assert, assertEquals } from "./+assert.ts";
import { withDatabase } from "./+shared.ts";
import { withIndex } from "../src/indexes.ts";
import { defineType } from "../src/type-definition.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { from } from "../src/computed.ts";
import { computedTopology } from "../src/computed-topology.ts";
import { withRequestContext } from "../src/request-context.ts";
import { reader, registerReaders, unregisterReaders } from "../src/readers.ts";
import { TELEMETRY_ATTRIBUTES as A } from "../src/telemetry.ts";

const Member = defineType({
  schema: v.object({
    userId: withIndex(v.string()),
    tenantId: v.string(),
    status: v.picklist(["active", "left"]),
  }),
});

const schemas = { multiCollections: { "+entreprises": { member: Member } } };

const membershipsOf = reader(
  "telemetry-memberships",
  from("member", Member)
    .by((m) => m.userId)
    .where((m) => [m.status, "active"])
    .select(["tenantId"]),
);

const tenantsOf = reader("telemetry-tenants", async (userId: string) =>
  (await membershipsOf(userId)).map((m) => m.tenantId),
);

test("readers telemetry: one span per call, with its outcome, nested under the composite, and no key value", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { exporter, telemetry } = makeTestTelemetry();
    registerReaders(db, {
      topology: computedTopology(schemas),
      readers: [membershipsOf, tenantsOf],
      telemetry,
    });
    try {
      const entreprises = await multiCollection(
        db,
        "+entreprises",
        { member: Member },
        { schemaManagement: "auto" },
      );
      await entreprises.insertOne("member", {
        userId: "user:secretada",
        tenantId: "tenant:a",
        status: "active",
      });
      await withRequestContext(
        async () => {
          assertEquals(await tenantsOf("user:secretada"), ["tenant:a"]);
          await membershipsOf("user:secretada");
          await membershipsOf.many(["user:secretada", "user:nobody"]);
        },
        { database: db },
      );

      const spans = exporter.getFinishedSpans();
      const described = spans.map((span) => ({
        name: span.name,
        kind: span.attributes[A.READER_KIND],
        level: span.attributes[A.READER_LEVEL],
        outcome: span.attributes[A.READER_OUTCOME],
        keys: span.attributes[A.READER_KEYS],
      }));
      assertEquals(described, [
        {
          name: "reader telemetry-memberships",
          kind: "query",
          level: "request",
          outcome: "load",
          keys: undefined,
        },
        {
          name: "reader telemetry-tenants",
          kind: "composite",
          level: "request",
          outcome: "load",
          keys: undefined,
        },
        {
          name: "reader telemetry-memberships",
          kind: "query",
          level: "request",
          outcome: "hit",
          keys: undefined,
        },
        {
          name: "reader telemetry-memberships",
          kind: "query",
          level: "request",
          outcome: "load",
          keys: 2,
        },
      ]);
      const [inner, outer] = spans;
      assertEquals(
        inner?.parentSpanContext?.spanId,
        outer?.spanContext().spanId,
      );
      assertEquals(inner?.attributes[A.COLLECTION_NAME], "+entreprises");
      assert(!dumpSpans(exporter).includes("secretada"));
    } finally {
      unregisterReaders(db);
    }
  });
});

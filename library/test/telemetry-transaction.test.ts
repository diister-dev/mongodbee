/**
 * Telemetry span parenting and `mongodb.transaction` spans emitted by
 * `getSessionContext(client).withSession(...)`: parenting under application
 * spans, commit/abort outcome, and write-conflict retry accounting.
 */
import { assert, assertEquals, assertExists, assertRejects } from "@std/assert";
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import { withDatabase } from "./+shared.ts";
import { collection } from "../src/collection.ts";
import { getSessionContext } from "../src/session.ts";
import * as v from "../src/schema.ts";
import { TELEMETRY_ATTRIBUTES as A } from "../telemetry.ts";
import { makeTestTelemetry, type TestTelemetry } from "./+telemetry.ts";

type FinishedSpan = ReturnType<
  TestTelemetry["exporter"]["getFinishedSpans"]
>[number];

function spansNamed(t: TestTelemetry, name: string): FinishedSpan[] {
  return t.exporter.getFinishedSpans().filter((span) => span.name === name);
}

/** a <= b for HrTime tuples [seconds, nanoseconds]. */
function hrTimeLE(a: [number, number], b: [number, number]): boolean {
  return a[0] < b[0] || (a[0] === b[0] && a[1] <= b[1]);
}

/** Span ids of `root` and all of its (transitive) descendants. */
function descendantIds(root: FinishedSpan, all: FinishedSpan[]): Set<string> {
  const ids = new Set<string>([root.spanContext().spanId]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const span of all) {
      const parentId = span.parentSpanContext?.spanId;
      const spanId = span.spanContext().spanId;
      if (parentId && ids.has(parentId) && !ids.has(spanId)) {
        ids.add(spanId);
        changed = true;
      }
    }
  }
  return ids;
}

const userSchema = {
  name: v.string(),
};

Deno.test("telemetry parenting: op span is a child of the active application span", async () => {
  await withDatabase("telemetry-tx-parenting", async (db) => {
    const t = makeTestTelemetry();
    const users = await collection(db, "users", userSchema, {
      telemetry: t.telemetry,
    });

    const tracer = t.provider.getTracer("app-test");
    let appSpanId = "";
    await tracer.startActiveSpan("app.handler", async (appSpan) => {
      appSpanId = appSpan.spanContext().spanId;
      await users.insertOne({ name: "parenting-user" });
      appSpan.end();
    });

    const insertSpans = spansNamed(t, "insertOne users");
    assertEquals(insertSpans.length, 1);
    assertEquals(insertSpans[0].parentSpanContext?.spanId, appSpanId);
  });
});

Deno.test("telemetry transaction: commit emits an INTERNAL span parenting the ops", async () => {
  await withDatabase("telemetry-tx-commit", async (db) => {
    const t = makeTestTelemetry();
    const users = await collection(db, "users", userSchema, {
      telemetry: t.telemetry,
    });

    await getSessionContext(db.client).withSession(async () => {
      await users.insertOne({ name: "tx-user-1" });
      await users.insertOne({ name: "tx-user-2" });
    });

    const txSpans = spansNamed(t, "mongodb.transaction");
    assertEquals(txSpans.length, 1);
    const tx = txSpans[0];
    assertEquals(tx.kind, SpanKind.INTERNAL);
    assertEquals(tx.attributes[A.TX_OUTCOME], "committed");
    assertEquals(tx.attributes[A.TX_RETRY_COUNT], 0);

    const insertSpans = spansNamed(t, "insertOne users");
    assertEquals(insertSpans.length, 2);
    const txId = tx.spanContext().spanId;
    for (const op of insertSpans) {
      assertEquals(op.parentSpanContext?.spanId, txId);
      assert(
        hrTimeLE(op.endTime, tx.endTime),
        "transaction span must end after its op spans",
      );
    }
  });
});

Deno.test("telemetry transaction: abort records outcome + ERROR and rolls back", async () => {
  await withDatabase("telemetry-tx-abort", async (db) => {
    const t = makeTestTelemetry();
    const users = await collection(db, "users", userSchema, {
      telemetry: t.telemetry,
    });

    const error = await assertRejects(
      () =>
        getSessionContext(db.client).withSession(async () => {
          await users.insertOne({ name: "ghost-user" });
          throw new Error("boom");
        }),
      Error,
      "boom",
    );
    // The original error surfaces unchanged to the caller
    assertEquals(error.message, "boom");

    const txSpans = spansNamed(t, "mongodb.transaction");
    assertEquals(txSpans.length, 1);
    const tx = txSpans[0];
    assertEquals(tx.attributes[A.TX_OUTCOME], "aborted");
    assertEquals(tx.status.code, SpanStatusCode.ERROR);
    assertExists(tx.attributes[A.TX_RETRY_COUNT]);

    // The insert was rolled back with the transaction
    assertEquals(await users.findOne({ name: "ghost-user" }), null);
    assertEquals(await users.countDocuments({}), 0);
  });
});

Deno.test("telemetry transaction: concurrent write-conflict retries are counted", async () => {
  await withDatabase("telemetry-tx-retries", async (db) => {
    const t = makeTestTelemetry();
    const items = await collection(db, "items", {
      name: v.string(),
      value: v.number(),
    }, { telemetry: t.telemetry });

    const itemId = await items.insertOne({ name: "contended", value: 0 });
    const sessionContext = getSessionContext(db.client);

    // Two concurrent transactions updating the SAME document: the second one
    // hits a write conflict. Retries are non-deterministic, so the assertions
    // only check the accounting invariants, never that a retry happened.
    const run = (value: number) =>
      sessionContext.withSession(async () => {
        await items.updateOne({ _id: itemId }, { $set: { value } });
        await new Promise((resolve) => setTimeout(resolve, 150));
      });
    const results = await Promise.allSettled([run(1), run(2)]);
    assert(
      results.some((r) => r.status === "fulfilled"),
      "at least one transaction should commit",
    );

    const txSpans = spansNamed(t, "mongodb.transaction");
    assertEquals(txSpans.length, 2);

    const all = t.exporter.getFinishedSpans();
    for (const tx of txSpans) {
      const retryCount = tx.attributes[A.TX_RETRY_COUNT];
      assertEquals(typeof retryCount, "number");
      assert((retryCount as number) >= 0);

      // The transaction retry count is the sum of the write-conflict retries
      // of the operations executed inside this transaction.
      const ids = descendantIds(tx, all);
      let sum = 0;
      for (const span of all) {
        if (span === tx) continue;
        if (!ids.has(span.spanContext().spanId)) continue;
        const opRetries = span.attributes[A.RETRY_COUNT];
        if (typeof opRetries === "number") sum += opRetries;
      }
      assertEquals(sum, retryCount);
    }
  });
});

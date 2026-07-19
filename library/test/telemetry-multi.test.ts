/**
 * Telemetry attributes specific to multiCollection / scopedMultiCollection:
 * `mongodbee.doc_type`, `mongodbee.scope` (string, string[] or absent) and
 * `db.operation.batch.size`, plus the anti-PII guarantee (inserted/filtered
 * values never reach the spans).
 */
import { assert, assertEquals, assertExists, assertRejects } from "@std/assert";
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import { withDatabase } from "./+shared.ts";
import { multiCollection } from "../src/multi-collection.ts";
import { scopedMultiCollection } from "../src/scoped-multi-collection.ts";
import * as v from "../src/schema.ts";
import { refId } from "../src/ids.ts";
import { TELEMETRY_ATTRIBUTES as A } from "../telemetry.ts";
import {
  dumpSpans,
  makeTestTelemetry,
  type TestTelemetry,
} from "./+telemetry.ts";

type FinishedSpan = ReturnType<
  TestTelemetry["exporter"]["getFinishedSpans"]
>[number];

function spansNamed(t: TestTelemetry, name: string): FinishedSpan[] {
  return t.exporter.getFinishedSpans().filter((span) => span.name === name);
}

const EXPO_A = "exposition:expoaaaaa01";
const EXPO_B = "exposition:expobbbbb02";

Deno.test("telemetry multi: doc_type on typed ops, none on *Any, batch size on deleteIds", async () => {
  await withDatabase("telemetry-multi-doctype", async (db) => {
    const t = makeTestTelemetry();
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
        price: v.number(),
      },
      category: {
        name: v.string(),
      },
    }, { telemetry: t.telemetry });

    const SENTINEL_NAME = "SENTINEL_PRODUCT_NAME_93f1c";

    await catalog.insertOne("product", { name: SENTINEL_NAME, price: 424242 });
    await catalog.findOne("product", { name: SENTINEL_NAME });
    const found = await catalog.find("product", { name: SENTINEL_NAME });
    assertEquals(found.length, 1);

    // Typed operations carry mongodbee.doc_type = "product"
    const insertSpans = spansNamed(t, "insertOne catalog");
    assertEquals(insertSpans.length, 1);
    const insertSpan = insertSpans[0];
    assertEquals(insertSpan.kind, SpanKind.CLIENT);
    assertEquals(insertSpan.attributes[A.DB_SYSTEM], "mongodb");
    assertEquals(insertSpan.attributes[A.DB_NAMESPACE], db.databaseName);
    assertEquals(insertSpan.attributes[A.COLLECTION_NAME], "catalog");
    assertEquals(insertSpan.attributes[A.OPERATION_NAME], "insertOne");
    assertEquals(insertSpan.attributes[A.DOC_TYPE], "product");

    const findOneSpans = spansNamed(t, "findOne catalog");
    assertEquals(findOneSpans.length, 1);
    assertEquals(findOneSpans[0].attributes[A.DOC_TYPE], "product");

    // multiCollection().find() materializes: one span covering the whole call
    const findSpans = spansNamed(t, "find catalog");
    assertEquals(findSpans.length, 1);
    assertEquals(findSpans[0].attributes[A.DOC_TYPE], "product");
    assertEquals(findSpans[0].attributes[A.RETURNED_ROWS], 1);

    // Cross-type operations carry NO mongodbee.doc_type
    await catalog.findOneAny({ name: SENTINEL_NAME });
    await catalog.findAny({ name: SENTINEL_NAME });

    const findOneAnySpans = spansNamed(t, "findOneAny catalog");
    assertEquals(findOneAnySpans.length, 1);
    assert(
      !(A.DOC_TYPE in findOneAnySpans[0].attributes),
      "findOneAny span must not carry mongodbee.doc_type",
    );

    const findAnySpans = spansNamed(t, "findAny catalog");
    assertEquals(findAnySpans.length, 1);
    assert(
      !(A.DOC_TYPE in findAnySpans[0].attributes),
      "findAny span must not carry mongodbee.doc_type",
    );

    // Batched operation reports db.operation.batch.size
    const idA = await catalog.insertOne("product", {
      name: "SENTINEL_BATCH_A_71b2",
      price: 1,
    });
    const idB = await catalog.insertOne("product", {
      name: "SENTINEL_BATCH_B_71b2",
      price: 2,
    });
    const deleted = await catalog.deleteIds("product", [idA, idB]);
    assertEquals(deleted, 2);

    const deleteIdsSpans = spansNamed(t, "deleteIds catalog");
    assertEquals(deleteIdsSpans.length, 1);
    assertEquals(deleteIdsSpans[0].attributes[A.BATCH_SIZE], 2);
    assertEquals(deleteIdsSpans[0].attributes[A.DOC_TYPE], "product");

    // Anti-PII: no inserted/filtered value ever reaches a span
    const dump = dumpSpans(t.exporter);
    assert(dump.length > 0, "expected at least one finished span");
    assert(!dump.includes(SENTINEL_NAME), "document value leaked into spans");
    assert(!dump.includes("SENTINEL_BATCH_A_71b2"), "value leaked into spans");
    assert(!dump.includes("SENTINEL_BATCH_B_71b2"), "value leaked into spans");
    assert(
      !dump.includes("424242"),
      "numeric document value leaked into spans",
    );
  });
});

Deno.test("telemetry scoped: scope + doc_type attributes on views", async () => {
  await withDatabase("telemetry-scoped-attrs", async (db) => {
    const t = makeTestTelemetry();
    const catalog = await scopedMultiCollection(db, "catalog2", {
      scope: refId("exposition"),
      types: {
        artwork: {
          title: v.string(),
          year: v.number(),
        },
        artist: {
          name: v.string(),
        },
      },
      allowUnscoped: true,
      telemetry: t.telemetry,
    });

    const SENTINEL_TITLE = "SENTINEL_ARTWORK_TITLE_5a8d";

    // Single-scope view: mongodbee.scope = scope id (string) + doc_type
    const expo = catalog.scope(EXPO_A);
    await expo.insertOne("artwork", { title: SENTINEL_TITLE, year: 987654 });
    await expo.findOne("artwork", { title: SENTINEL_TITLE });
    const count = await expo.countDocuments("artwork");
    assertEquals(count, 1);

    const insertSpans = spansNamed(t, "insertOne catalog2");
    assertEquals(insertSpans.length, 1);
    assertEquals(insertSpans[0].attributes[A.SCOPE], EXPO_A);
    assertEquals(insertSpans[0].attributes[A.DOC_TYPE], "artwork");
    assertEquals(insertSpans[0].attributes[A.COLLECTION_NAME], "catalog2");

    const countSpans = spansNamed(t, "countDocuments catalog2");
    assertEquals(countSpans.length, 1);
    assertEquals(countSpans[0].attributes[A.SCOPE], EXPO_A);
    assertEquals(countSpans[0].attributes[A.DOC_TYPE], "artwork");

    // Multi-scope read view: mongodbee.scope is the string[] of scope ids
    await catalog.scopes([EXPO_A, EXPO_B]).findOne("artwork", {
      title: SENTINEL_TITLE,
    });

    // Unscoped admin view: NO mongodbee.scope attribute
    await catalog.unscoped.findOne("artwork", { title: SENTINEL_TITLE });

    const findOneSpans = spansNamed(t, "findOne catalog2");
    assertEquals(findOneSpans.length, 3);
    const [scopedFindOne, multiScopeFindOne, unscopedFindOne] = findOneSpans;
    assertExists(scopedFindOne);
    assertEquals(scopedFindOne.attributes[A.SCOPE], EXPO_A);
    assertEquals(scopedFindOne.attributes[A.DOC_TYPE], "artwork");

    assertEquals(multiScopeFindOne.attributes[A.SCOPE], [EXPO_A, EXPO_B]);
    assertEquals(multiScopeFindOne.attributes[A.DOC_TYPE], "artwork");

    assert(
      !(A.SCOPE in unscopedFindOne.attributes),
      "unscoped span must not carry mongodbee.scope",
    );
    assertEquals(unscopedFindOne.attributes[A.DOC_TYPE], "artwork");

    // Anti-PII: document values never reach the spans (scope ids are
    // structural metadata and ARE expected in the dump)
    const dump = dumpSpans(t.exporter);
    assert(!dump.includes(SENTINEL_TITLE), "document value leaked into spans");
    assert(
      !dump.includes("987654"),
      "numeric document value leaked into spans",
    );
  });
});

Deno.test("telemetry scoped: not-found error keeps the id caller-side but strips it from the span", async () => {
  await withDatabase("telemetry-scoped-notfound", async (db) => {
    const t = makeTestTelemetry();
    const catalog = await scopedMultiCollection(db, "catalog3", {
      scope: refId("exposition"),
      types: {
        artwork: {
          title: v.string(),
        },
      },
      telemetry: t.telemetry,
    });

    // Both the id and the scope value are unique sentinels: a missing getById
    // interpolates them into the caller-facing message.
    const SENTINEL_ID = "artwork:PIISENTINELGETBYIDID9f2a";
    const SENTINEL_SCOPE = "exposition:PIISENTINELSCOPE7b";

    const expo = catalog.scope(SENTINEL_SCOPE);
    const error = await assertRejects(() =>
      expo.getById("artwork", SENTINEL_ID)
    );

    // Caller-facing behaviour is unchanged: the message still names the missing
    // id and the scope so the developer can debug.
    assert(error instanceof Error);
    assert(
      error.message.includes(SENTINEL_ID),
      "id must stay in the caller-facing message",
    );
    assert(
      error.message.includes(SENTINEL_SCOPE),
      "scope value must stay in the caller-facing message",
    );

    const span = spansNamed(t, "getById catalog3").at(-1);
    assertExists(span);
    assertEquals(span.status.code, SpanStatusCode.ERROR);

    // The recorded exception and the status message must carry neither the id
    // nor the scope value. NOTE: the mongodbee.scope ATTRIBUTE legitimately
    // holds the scope value (structural metadata), so this assertion targets
    // the exception event and status message specifically — not the full dump.
    const exceptionEvent = span.events.find((e) => e.name === "exception");
    assertExists(exceptionEvent);
    const exceptionMessage = exceptionEvent.attributes?.["exception.message"];
    assert(typeof exceptionMessage === "string", "exception message recorded");
    for (
      const [channel, text] of [
        ["exception message", exceptionMessage],
        ["status message", String(span.status.message ?? "")],
      ] as const
    ) {
      assert(
        !text.includes(SENTINEL_ID),
        `id leaked into the ${channel}`,
      );
      assert(
        !text.includes(SENTINEL_SCOPE),
        `scope value leaked into the ${channel}`,
      );
    }

    // The scope value IS expected on the dedicated attribute — confirm the
    // structural metadata is still there.
    assertEquals(span.attributes[A.SCOPE], SENTINEL_SCOPE);
  });
});

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
    assertEquals(deleteIdsSpans[0].attributes[A.FILTER_KEYS], "_id");

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
      schemaManagement: "auto",
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
      schemaManagement: "auto",
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

Deno.test("telemetry multi: updateMany carries doc_type as the string[] of type names", async () => {
  await withDatabase("telemetry-multi-updatemany", async (db) => {
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

    const productId = await catalog.insertOne("product", {
      name: "SENTINEL_UPDATEMANY_PRODUCT_2b1a",
      price: 10,
    });
    const categoryId = await catalog.insertOne("category", {
      name: "SENTINEL_UPDATEMANY_CATEGORY_2b1a",
    });
    t.exporter.reset();

    const modified = await catalog.updateMany({
      product: { [productId]: { price: 20 } },
      category: { [categoryId]: { name: "renamed" } },
    });
    assertEquals(modified, 2);

    const updateManySpans = spansNamed(t, "updateMany catalog");
    assertEquals(updateManySpans.length, 1);
    // doc_type is the array of the operation's top-level type keys, in order.
    assertEquals(updateManySpans[0].attributes[A.DOC_TYPE], [
      "product",
      "category",
    ]);
    assertEquals(updateManySpans[0].attributes[A.BATCH_SIZE], 2);
  });
});

Deno.test("telemetry multi: updateOne validation failure records one ERROR span and rethrows", async () => {
  await withDatabase("telemetry-multi-updateone-invalid", async (db) => {
    const t = makeTestTelemetry();
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
        price: v.number(),
      },
    }, { telemetry: t.telemetry });

    const productId = await catalog.insertOne("product", {
      name: "valid",
      price: 1,
    });
    t.exporter.reset();

    // `price` must be a number: this string value fails schema validation,
    // which runs inside the span (so the span is recorded as ERROR).
    const SENTINEL_INVALID = "SENTINEL_INVALID_PRICE_7f3a";
    const error = await assertRejects(() =>
      catalog.updateOne("product", productId, {
        price: SENTINEL_INVALID as unknown as number,
      })
    );

    // The original validation error reaches the caller unchanged.
    assert(error instanceof Error, "the thrown value must be an Error");
    assertEquals((error as Error).name, "ValiError");

    // Exactly one ERROR span named "updateOne catalog".
    const updateOneSpans = spansNamed(t, "updateOne catalog");
    assertEquals(updateOneSpans.length, 1);
    assertEquals(updateOneSpans[0].status.code, SpanStatusCode.ERROR);
    assertEquals(updateOneSpans[0].attributes[A.DOC_TYPE], "product");

    // The rejected value never leaks into the span (synthetic message only).
    assert(
      !dumpSpans(t.exporter).includes(SENTINEL_INVALID),
      "the invalid value leaked into the span dump",
    );
  });
});

Deno.test("telemetry multi: drop emits a CLIENT span", async () => {
  await withDatabase("telemetry-multi-drop", async (db) => {
    const t = makeTestTelemetry();
    const catalog = await multiCollection(db, "catalog", {
      product: {
        name: v.string(),
      },
    }, { telemetry: t.telemetry });
    await catalog.insertOne("product", { name: "SENTINEL_DROP_PRODUCT_9c2d" });
    t.exporter.reset();

    const dropped = await catalog.drop({ force: true });
    assertEquals(dropped, true);

    const dropSpans = spansNamed(t, "drop catalog");
    assertEquals(dropSpans.length, 1);
    assertEquals(dropSpans[0].kind, SpanKind.CLIENT);
    assertEquals(dropSpans[0].attributes[A.DB_SYSTEM], "mongodb");
    assertEquals(dropSpans[0].attributes[A.DB_NAMESPACE], db.databaseName);
    assertEquals(dropSpans[0].attributes[A.COLLECTION_NAME], "catalog");
    assertEquals(dropSpans[0].attributes[A.OPERATION_NAME], "drop");
  });
});

Deno.test("telemetry scoped: drop emits a CLIENT span", async () => {
  await withDatabase("telemetry-scoped-drop", async (db) => {
    const t = makeTestTelemetry();
    const catalog = await scopedMultiCollection(db, "catalog5", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        artwork: {
          title: v.string(),
        },
      },
      telemetry: t.telemetry,
    });
    await catalog.scope(EXPO_A).insertOne("artwork", {
      title: "SENTINEL_DROP_ARTWORK_1e4f",
    });
    t.exporter.reset();

    const dropped = await catalog.drop({ force: true });
    assertEquals(dropped, true);

    const dropSpans = spansNamed(t, "drop catalog5");
    assertEquals(dropSpans.length, 1);
    assertEquals(dropSpans[0].kind, SpanKind.CLIENT);
    assertEquals(dropSpans[0].attributes[A.DB_SYSTEM], "mongodb");
    assertEquals(dropSpans[0].attributes[A.COLLECTION_NAME], "catalog5");
    assertEquals(dropSpans[0].attributes[A.OPERATION_NAME], "drop");
  });
});

Deno.test("telemetry scoped: invalid scope value in a handle op inside a transaction stays caller-side but never lands on the transaction span", async () => {
  await withDatabase("telemetry-scoped-invalid-scope-tx", async (db) => {
    const t = makeTestTelemetry();
    const catalog = await scopedMultiCollection(db, "catalog7", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        artwork: {
          title: v.string(),
        },
      },
      telemetry: t.telemetry,
    });

    // Violates the scope schema (wrong prefix, not `exposition:`). A unique
    // sentinel so we can assert it reaches the caller but never a span.
    const SENTINEL_INVALID_SCOPE = "wrongprefix:PIISENTINELINVALIDSCOPE_4d7e";

    // `assertScopeValue` runs OUTSIDE any op span (scopeExists validates the id
    // before `traced()`), so inside a transaction the throw lands on the
    // `mongodb.transaction` span via `recordSafeError` — the exact leak vector
    // `errorWithSafeMessage` closes.
    const error = await assertRejects(() =>
      catalog.withSession(async () => {
        await catalog.scopeExists(SENTINEL_INVALID_SCOPE);
      })
    );

    // The caller-facing message keeps the offending value for debuggability.
    assert(error instanceof Error);
    assert(
      error.message.includes(SENTINEL_INVALID_SCOPE),
      "the invalid scope value must stay in the caller-facing message",
    );

    // A transaction span was emitted and recorded the failure (aborted/ERROR).
    const txSpans = spansNamed(t, "mongodb.transaction");
    assertEquals(txSpans.length, 1);
    assertEquals(txSpans[0].attributes[A.TX_OUTCOME], "aborted");
    assertEquals(txSpans[0].status.code, SpanStatusCode.ERROR);

    // No scopeExists op span exists — the throw happened before `traced()`.
    assertEquals(spansNamed(t, "scopeExists catalog7").length, 0);

    // The full span dump (transaction span's recorded exception + status)
    // never carries the sentinel scope value.
    assert(
      !dumpSpans(t.exporter).includes(SENTINEL_INVALID_SCOPE),
      "the invalid scope value leaked into the span dump",
    );
  });
});

Deno.test("telemetry scoped: recordScope false omits mongodbee.scope everywhere", async () => {
  await withDatabase("telemetry-scoped-norecord", async (db) => {
    const t = makeTestTelemetry();
    // Both scope ids are unique sentinels: with recordScope disabled, neither
    // the attribute KEY nor the scope VALUE may appear anywhere in the dump.
    const SENTINEL_SCOPE_A = "exposition:norecordscopeaaa01";
    const SENTINEL_SCOPE_B = "exposition:norecordscopebbb02";
    const catalog = await scopedMultiCollection(db, "catalog6", {
      schemaManagement: "auto",
      scope: refId("exposition"),
      types: {
        artwork: {
          title: v.string(),
          year: v.number(),
        },
      },
      allowUnscoped: true,
      telemetry: { ...t.telemetry, recordScope: false },
    });

    // Ops still work end-to-end while no scope metadata is recorded.
    const expo = catalog.scope(SENTINEL_SCOPE_A);
    const id = await expo.insertOne("artwork", { title: "T", year: 1 });
    assertExists(id);
    const found = await expo.findOne("artwork", {});
    assertExists(found);
    assertEquals(await expo.countDocuments("artwork"), 1);

    // Multi-scope and unscoped views run through the other scope-attr path.
    await catalog.scopes([SENTINEL_SCOPE_A, SENTINEL_SCOPE_B]).findOne(
      "artwork",
      {},
    );
    await catalog.unscoped.findOne("artwork", {});

    // Handle ops honour recordScope: false too — their spans also carry the
    // scope value only through the (now-omitted) mongodbee.scope attribute.
    // dropScope runs last so it does not empty the scope before the others.
    assertEquals(await catalog.scopeExists(SENTINEL_SCOPE_A), true);
    await catalog.scopeStats(SENTINEL_SCOPE_A);
    await catalog.dropScope(SENTINEL_SCOPE_A, { confirm: true });

    const dump = dumpSpans(t.exporter);
    assert(
      t.exporter.getFinishedSpans().length > 0,
      "expected at least one finished span",
    );
    assert(
      !dump.includes(A.SCOPE),
      "the mongodbee.scope attribute must be absent when recordScope is false",
    );
    assert(
      !dump.includes(SENTINEL_SCOPE_A) && !dump.includes(SENTINEL_SCOPE_B),
      "scope values must not appear when recordScope is false",
    );

    // Non-scope structural metadata is still recorded (doc_type).
    const insertSpans = spansNamed(t, "insertOne catalog6");
    assertEquals(insertSpans.length, 1);
    assertEquals(insertSpans[0].attributes[A.DOC_TYPE], "artwork");
    assert(
      !(A.SCOPE in insertSpans[0].attributes),
      "the insert span must not carry mongodbee.scope",
    );

    // Handle ops (scopeExists/scopeStats/dropScope) likewise omit the scope
    // attribute when recordScope is false.
    for (const opName of ["scopeExists", "scopeStats", "dropScope"]) {
      const opSpans = spansNamed(t, `${opName} catalog6`);
      assertEquals(opSpans.length, 1, `expected exactly one ${opName} span`);
      assert(
        !(A.SCOPE in opSpans[0].attributes),
        `${opName} span must not carry mongodbee.scope when recordScope is false`,
      );
    }
  });
});

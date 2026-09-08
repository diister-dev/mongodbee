/**
 * Core OpenTelemetry tests for the single-type `collection()` factory.
 *
 * Covers: one CLIENT span per public operation with semconv attributes,
 * batching, disabled/default/no-provider behaviour, PII-free span payloads
 * and error paths (driver + validation).
 */
import { test } from "./+harness.ts";
import * as v from "../src/schema.ts";
import {
  assert,
  assertEquals,
  assertExists,
  assertRejects,
} from "./+assert.ts";
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import { collection } from "../src/collection.ts";
import { TELEMETRY_ATTRIBUTES as TA } from "../telemetry.ts";
import { withDatabase } from "./+shared.ts";
import { dumpSpans, makeTestTelemetry } from "./+telemetry.ts";

const userSchema = {
  name: v.string(),
  email: v.string(),
  age: v.number(),
};

type TestExporter = ReturnType<typeof makeTestTelemetry>["exporter"];

function spansNamed(exporter: TestExporter, name: string) {
  return exporter.getFinishedSpans().filter((span) => span.name === name);
}

function onlySpan(exporter: TestExporter, name: string) {
  const spans = spansNamed(exporter, name);
  assertEquals(
    spans.length,
    1,
    `expected exactly one span named "${name}", got ${spans.length}`,
  );
  return spans[0];
}

test("telemetry: one CLIENT span per operation with semconv attributes", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { exporter, telemetry } = makeTestTelemetry();
    const users = await collection(db, "users", userSchema, { telemetry });
    exporter.reset(); // Only measure the public operations below

    await users.insertOne({
      name: "Alice",
      email: "alice@example.com",
      age: 30,
    });
    const found = await users.findOne({ name: "Alice" });
    assertExists(found);
    assertEquals(found.age, 30);

    const updated = await users.updateOne(
      { name: "Alice" },
      { $set: { age: 31 } },
    );
    assertEquals(updated.matchedCount, 1);
    assertEquals(updated.modifiedCount, 1);

    const listed = await users.find({ age: 31 }).toArray();
    assertEquals(listed.length, 1);

    const page = await users.paginate({}, { limit: 10 });
    assertEquals(page.data.length, 1);

    const deleted = await users.deleteOne({ email: "alice@example.com" });
    assertEquals(deleted.deletedCount, 1);

    // Every public operation emitted exactly one CLIENT span with the
    // stable database semantic convention attributes.
    const operations = [
      "insertOne",
      "findOne",
      "updateOne",
      "find",
      "paginate",
      "deleteOne",
    ];
    for (const operation of operations) {
      const span = onlySpan(exporter, `${operation} users`);
      assertEquals(span.kind, SpanKind.CLIENT, `${operation} span kind`);
      assertEquals(span.attributes[TA.DB_SYSTEM], "mongodb");
      assertEquals(span.attributes[TA.DB_NAMESPACE], db.databaseName);
      assertEquals(span.attributes[TA.COLLECTION_NAME], "users");
      assertEquals(span.attributes[TA.OPERATION_NAME], operation);
    }

    // Operation-specific attributes.
    const insertOneSpan = onlySpan(exporter, "insertOne users");
    assertEquals(insertOneSpan.attributes[TA.INSERTED_COUNT], 1);

    const findOneSpan = onlySpan(exporter, "findOne users");
    assertEquals(findOneSpan.attributes[TA.FILTER_KEYS], "name");

    const updateSpan = onlySpan(exporter, "updateOne users");
    assertEquals(updateSpan.attributes[TA.FILTER_KEYS], "name");
    assertEquals(updateSpan.attributes[TA.UPDATE_OPERATORS], "$set");
    assertEquals(updateSpan.attributes[TA.MATCHED_COUNT], 1);
    assertEquals(updateSpan.attributes[TA.MODIFIED_COUNT], 1);

    const deleteSpan = onlySpan(exporter, "deleteOne users");
    assertEquals(deleteSpan.attributes[TA.DELETED_COUNT], 1);

    const findSpan = onlySpan(exporter, "find users");
    assertEquals(findSpan.attributes[TA.FILTER_KEYS], "age");
    assertEquals(findSpan.attributes[TA.RETURNED_ROWS], 1);
  });
});

test("telemetry: insertMany of N docs emits exactly one span with batch size", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { exporter, telemetry } = makeTestTelemetry();
    const users = await collection(db, "users", userSchema, { telemetry });
    exporter.reset();

    const docs = Array.from({ length: 50 }, (_, i) => ({
      name: `user-${i}`,
      email: `user-${i}@example.com`,
      age: i,
    }));
    const result = await users.insertMany(docs);
    assertEquals(result.insertedCount, 50);

    assertEquals(
      exporter.getFinishedSpans().length,
      1,
      "a 50-doc insertMany must emit exactly one span",
    );
    const span = onlySpan(exporter, "insertMany users");
    assertEquals(span.kind, SpanKind.CLIENT);
    assertEquals(span.attributes[TA.OPERATION_NAME], "insertMany");
    assertEquals(span.attributes[TA.BATCH_SIZE], 50);
    assertEquals(span.attributes[TA.INSERTED_COUNT], 50);
  });
});

test("telemetry: updateOne upsert records upserted_count === 1", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { exporter, telemetry } = makeTestTelemetry();
    const users = await collection(db, "users", userSchema, { telemetry });
    exporter.reset();

    // The filter matches no document, so { upsert: true } inserts a new one.
    // `name` is seeded from the filter equality; `$set` supplies the rest.
    const result = await users.updateOne(
      { name: "Nonexistent" },
      { $set: { email: "upsert@example.com", age: 50 } },
      { upsert: true },
    );
    assertEquals(result.matchedCount, 0);
    assertEquals(result.upsertedCount, 1);

    const span = onlySpan(exporter, "updateOne users");
    assertEquals(span.attributes[TA.MATCHED_COUNT], 0);
    assertEquals(span.attributes[TA.UPSERTED_COUNT], 1);
  });
});

test("telemetry: disabled and default emit zero spans with identical behavior", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { exporter, provider } = makeTestTelemetry();

    // Default: no telemetry option at all.
    const plain = await collection(db, "plain_users", userSchema);
    // Explicitly disabled, even with a working provider at hand.
    const disabled = await collection(db, "disabled_users", userSchema, {
      telemetry: { enabled: false, tracerProvider: provider },
    });

    for (const users of [plain, disabled]) {
      await users.insertOne({ name: "Bob", email: "bob@example.com", age: 42 });
      const found = await users.findOne({ name: "Bob" });
      assertExists(found);
      assertEquals(found.email, "bob@example.com");

      const updated = await users.updateOne(
        { name: "Bob" },
        { $set: { age: 43 } },
      );
      assertEquals(updated.modifiedCount, 1);

      const listed = await users.find({ age: 43 }).toArray();
      assertEquals(listed.length, 1);

      const page = await users.paginate({}, { limit: 5 });
      assertEquals(page.data.length, 1);

      const deleted = await users.deleteOne({ name: "Bob" });
      assertEquals(deleted.deletedCount, 1);
    }

    assertEquals(
      exporter.getFinishedSpans().length,
      0,
      "no span may be emitted when telemetry is disabled or absent",
    );
  });
});

test("telemetry: enabled without any provider is a silent no-op", async (t) => {
  await withDatabase(t.name, async (db) => {
    // enabled: true, no tracerProvider option, and no global provider is ever
    // registered by the test helpers: the API falls back to its no-op tracer.
    const users = await collection(db, "users", userSchema, {
      telemetry: { enabled: true },
    });

    const id = await users.insertOne({
      name: "Carol",
      email: "carol@example.com",
      age: 27,
    });
    assertExists(id);

    const found = await users.findOne({ name: "Carol" });
    assertExists(found);
    assertEquals(found.age, 27);

    const updated = await users.updateOne(
      { name: "Carol" },
      { $set: { age: 28 } },
    );
    assertEquals(updated.modifiedCount, 1);

    const listed = await users.find({ age: 28 }).toArray();
    assertEquals(listed.length, 1);

    const page = await users.paginate({}, { limit: 5 });
    assertEquals(page.data.length, 1);

    const deleted = await users.deleteOne({ name: "Carol" });
    assertEquals(deleted.deletedCount, 1);
  });
});

test("telemetry: spans never contain document or filter values (anti-PII)", async (t) => {
  await withDatabase(t.name, async (db) => {
    const SENTINEL_NAME = "PII_SENTINEL_NAME_c92d10";
    const SENTINEL_EMAIL = "PII_SENTINEL_EMAIL_xyz123";
    const SENTINEL_UPDATE = "PII_SENTINEL_UPDATE_5b8f22";
    const SENTINEL_INVALID = "PII_SENTINEL_INVALID_AGE_19ad3c";

    const { exporter, telemetry } = makeTestTelemetry();
    const users = await collection(db, "users", userSchema, { telemetry });
    exporter.reset();

    await users.insertOne({
      name: SENTINEL_NAME,
      email: SENTINEL_EMAIL,
      age: 33,
    });

    const found = await users.findOne({ email: SENTINEL_EMAIL });
    assertExists(found);
    assertEquals(found.name, SENTINEL_NAME);

    const updated = await users.updateOne(
      { email: SENTINEL_EMAIL },
      { $set: { name: SENTINEL_UPDATE } },
    );
    assertEquals(updated.modifiedCount, 1);

    // Validation failure whose valibot message embeds the received value.
    await assertRejects(() =>
      users.insertOne({
        name: "valid name",
        email: "valid@example.com",
        age: SENTINEL_INVALID as unknown as number,
      }),
    );

    const dump = dumpSpans(exporter);
    assert(exporter.getFinishedSpans().length >= 4, "spans were recorded");
    for (const sentinel of [
      SENTINEL_NAME,
      SENTINEL_EMAIL,
      SENTINEL_UPDATE,
      SENTINEL_INVALID,
    ]) {
      assert(
        !dump.includes(sentinel),
        `span dump leaked the sentinel value ${sentinel}`,
      );
    }

    // Structural info is present: the field NAME, never the value.
    const findOneSpan = onlySpan(exporter, "findOne users");
    assertEquals(findOneSpan.attributes[TA.FILTER_KEYS], "email");
    const updateSpan = onlySpan(exporter, "updateOne users");
    assertEquals(updateSpan.attributes[TA.FILTER_KEYS], "email");
  });
});

test("telemetry: error paths record ERROR spans and re-throw the original error", async (t) => {
  await withDatabase(t.name, async (db) => {
    const DUP_SENTINEL = "PII_SENTINEL_DUP_EMAIL_3af1@example.com";
    const { exporter, telemetry } = makeTestTelemetry();
    const users = await collection(db, "users", userSchema, { telemetry });
    await users.createIndex({ email: 1 }, { unique: true });
    await users.insertOne({ name: "First", email: DUP_SENTINEL, age: 1 });
    exporter.reset();

    // --- Real driver error: duplicate key on the unique index. The driver
    // message embeds the indexed value (`dup key: { email: "..." }`).
    const driverError = await assertRejects(() =>
      users.insertOne({ name: "Second", email: DUP_SENTINEL, age: 2 }),
    );
    assert(driverError instanceof Error, "driver error must be an Error");
    assertEquals(
      (driverError as unknown as { code?: number }).code,
      11000,
      "original driver error code must reach the caller",
    );
    assert(
      driverError.message.includes("E11000"),
      "original driver error message must reach the caller",
    );
    assert(
      driverError.message.includes(DUP_SENTINEL),
      "the indexed value stays in the caller-facing message",
    );

    const dupSpan = onlySpan(exporter, "insertOne users");
    assertEquals(dupSpan.status.code, SpanStatusCode.ERROR);
    const dupErrorType = dupSpan.attributes[TA.ERROR_TYPE];
    assert(
      typeof dupErrorType === "string" && dupErrorType.length > 0,
      "error.type must be a non-empty string",
    );
    assertEquals(dupErrorType, driverError.name);
    const dupException = dupSpan.events.find((e) => e.name === "exception");
    assertExists(dupException, "exception event must be recorded");
    // The indexed value must be redacted before it reaches the span.
    const dupExceptionMessage = dupException.attributes?.["exception.message"];
    assert(
      typeof dupExceptionMessage === "string" &&
        dupExceptionMessage.includes("<redacted>"),
      "duplicate-key value must be redacted in the recorded exception",
    );
    assert(
      !dumpSpans(exporter).includes(DUP_SENTINEL),
      "the indexed value leaked into the span dump",
    );

    exporter.reset();

    // --- Validation failure on write: the ValiError reaches the caller
    // unchanged while the span only carries a synthetic message.
    const SENTINEL = "PII_SENTINEL_ERRPATH_41af";
    const validationError = await assertRejects(() =>
      users.insertOne({
        name: "Bad",
        email: "bad@example.com",
        age: SENTINEL as unknown as number,
      }),
    );
    assert(validationError instanceof Error);
    assertEquals(validationError.name, "ValiError");
    assert(
      validationError.message.includes(SENTINEL),
      "original validation message must reach the caller unchanged",
    );
    const issues = (validationError as unknown as { issues?: unknown[] })
      .issues;
    assert(
      Array.isArray(issues) && issues.length > 0,
      "original valibot issues must reach the caller",
    );

    const valiSpan = onlySpan(exporter, "insertOne users");
    assertEquals(valiSpan.status.code, SpanStatusCode.ERROR);
    assertEquals(valiSpan.attributes[TA.ERROR_TYPE], "ValiError");
    const valiException = valiSpan.events.find((e) => e.name === "exception");
    assertExists(valiException, "exception event must be recorded");
    const exceptionMessage = valiException.attributes?.["exception.message"];
    assert(
      typeof exceptionMessage === "string" &&
        exceptionMessage.includes("details omitted"),
      "validation exception must be replaced by a synthetic message",
    );
    assert(!dumpSpans(exporter).includes(SENTINEL));

    exporter.reset();

    // --- Read-path validation failure: the original plain object
    // { message, errors, result } reaches the caller intact.
    const SENTINEL_DB = "PII_SENTINEL_GETBYID_90bc";
    const raw = await db
      .collection("users")
      .insertOne(
        { name: 12345, email: SENTINEL_DB, age: "not-a-number" },
        { bypassDocumentValidation: true },
      );
    const objectError = await assertRejects(() =>
      users.getById(raw.insertedId),
    );
    const validationObject = objectError as {
      message?: string;
      errors?: unknown;
      result?: { email?: string };
    };
    assertEquals(validationObject.message, "Validation error");
    assertExists(validationObject.errors, "errors payload must be intact");
    assertExists(validationObject.result, "result payload must be intact");
    assertEquals(
      validationObject.result?.email,
      SENTINEL_DB,
      "the raw document must reach the caller intact",
    );
    assert(
      !dumpSpans(exporter).includes(SENTINEL_DB),
      "read-path validation details must not leak into spans",
    );
  });
});

test("telemetry: duplicate-key driver errors are recorded with the indexed value redacted", async (t) => {
  await withDatabase(t.name, async (db) => {
    const SENTINEL_EMAIL = "PII_SENTINEL_DUPKEY_7c3e@example.com";

    const { exporter, telemetry } = makeTestTelemetry();
    const users = await collection(db, "users", userSchema, { telemetry });
    await users.createIndex({ email: 1 }, { unique: true });
    await users.insertOne({ name: "First", email: SENTINEL_EMAIL, age: 1 });
    exporter.reset();

    // The duplicate insert fails on the unique index; the raw driver message
    // embeds the indexed value as `dup key: { email: "<value>" }`.
    const driverError = await assertRejects(() =>
      users.insertOne({ name: "Second", email: SENTINEL_EMAIL, age: 2 }),
    );

    // The caller receives the ORIGINAL, untouched driver error — value included.
    assert(driverError instanceof Error, "driver error must be an Error");
    assertEquals(
      (driverError as unknown as { code?: number }).code,
      11000,
      "original driver error code must reach the caller",
    );
    assert(
      driverError.message.includes("E11000"),
      "original driver error message must reach the caller",
    );
    assert(
      driverError.message.includes(SENTINEL_EMAIL),
      "the indexed value stays in the caller-facing message",
    );

    // The span never leaks the indexed value: it is redacted before recording.
    assert(
      !dumpSpans(exporter).includes(SENTINEL_EMAIL),
      "the indexed value leaked into the span dump",
    );

    const dupSpan = onlySpan(exporter, "insertOne users");
    assertEquals(dupSpan.status.code, SpanStatusCode.ERROR);
    const dupException = dupSpan.events.find((e) => e.name === "exception");
    assertExists(dupException, "exception event must be recorded");
    const exceptionMessage = dupException.attributes?.["exception.message"];
    assert(
      typeof exceptionMessage === "string" &&
        exceptionMessage.includes("<redacted>"),
      "duplicate-key value must be redacted in the recorded exception",
    );
  });
});

test("telemetry: server errors embedding the document _id are recorded with the _id redacted", async (t) => {
  await withDatabase(t.name, async (db) => {
    const { exporter, telemetry } = makeTestTelemetry();
    const items = await collection(
      db,
      "items",
      { name: v.string() },
      {
        telemetry,
      },
    );

    // A sentinel string _id we can scan for. `$inc` on the string field `name`
    // makes the server reject the update with a message that embeds the
    // document's _id: `... {_id: "<value>"} has the field 'name' ...`.
    const SENTINEL_ID = "PII_SENTINEL_INC_ID_5b9c";
    // The schema declares no `_id`, so its input type is an ObjectId; the ODM
    // still accepts a caller-chosen `_id` at runtime (`_id: v.optional(v.any())`).
    await items.insertOne({ _id: SENTINEL_ID, name: "hello" } as any);
    exporter.reset();

    const driverError = await assertRejects(() =>
      // $inc on a non-numeric field — a document-style update the ODM passes
      // straight through to the driver, which returns a Plan-executor error.
      items.updateOne({ _id: SENTINEL_ID }, { $inc: { name: 1 } } as any),
    );

    // The caller still receives the original, untouched driver message.
    assert(driverError instanceof Error, "driver error must be an Error");
    assert(
      driverError.message.includes("non-numeric type"),
      "original driver error message must reach the caller",
    );
    assert(
      driverError.message.includes(SENTINEL_ID),
      "the document _id stays in the caller-facing message",
    );

    // The span records a synthetic exception with the _id redacted.
    const span = onlySpan(exporter, "updateOne items");
    assertEquals(span.status.code, SpanStatusCode.ERROR);
    const exception = span.events.find((e) => e.name === "exception");
    assertExists(exception, "exception event must be recorded");
    const exceptionMessage = exception.attributes?.["exception.message"];
    assert(
      typeof exceptionMessage === "string" &&
        exceptionMessage.includes("{_id: <redacted>}"),
      "the document _id must be redacted in the recorded exception",
    );
    assert(
      !dumpSpans(exporter).includes(SENTINEL_ID),
      "the document _id leaked into the span dump",
    );
  });
});

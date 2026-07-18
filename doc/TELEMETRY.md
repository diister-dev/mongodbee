# MongoDBee OpenTelemetry Tracing

## Overview

MongoDBee can emit OpenTelemetry spans for every collection operation and every transaction, giving you per-operation visibility (latency, errors, retries, result counts) in any OpenTelemetry-compatible backend.

The design principles:

- **Strictly opt-in**: tracing is disabled by default. It is enabled per collection through the `telemetry` option of `collection()`, `multiCollection()` and `scopedMultiCollection()`.
- **API-only**: MongoDBee depends only on `@opentelemetry/api`. It never instantiates an SDK, an exporter or a tracer provider — the host application owns the OpenTelemetry setup.
- **Runtime-agnostic**: no assumption is made about Deno, Bun or Node.js. Any runtime with an OpenTelemetry SDK (or Deno's built-in OpenTelemetry support) works.
- **Silent no-op**: when telemetry is enabled but the application has not registered an SDK, the global API provider is a no-op — spans cost almost nothing and nothing is exported.
- **Zero cost when disabled**: the `enabled` flag is resolved once at collection creation. When disabled (the default), no span is ever allocated and operations run on their regular, non-instrumented code path.

> **Note**: Spans never carry user data. See the [Privacy (PII) policy](#privacy-pii-policy) section for details.

## Quick Start

Tracing is a two-step setup: the **application** registers an OpenTelemetry SDK, then MongoDBee is told to emit spans.

**1. Register a tracer provider (application side)** — example with the Node.js SDK and an OTLP exporter:

```typescript
import { BatchSpanProcessor, NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";

const provider = new NodeTracerProvider({
  spanProcessors: [
    new BatchSpanProcessor(new OTLPTraceExporter({
      url: "http://localhost:4318/v1/traces",
    })),
  ],
});

// Registers the provider globally AND installs the context manager
// (required for span parenting, see below).
provider.register();
```

**2. Enable telemetry on your collections:**

```typescript
import { collection } from "@diister/mongodbee";
import * as v from "@diister/mongodbee/schema";

const users = await collection(db, "users", {
  username: v.string(),
  email: v.string(),
}, {
  telemetry: { enabled: true },
});

await users.insertOne({ username: "jane", email: "jane@example.com" });
// → emits a CLIENT span named "insertOne users"
```

If you prefer not to register a global provider, pass one explicitly:

```typescript
const users = await collection(db, "users", schema, {
  telemetry: { enabled: true, tracerProvider: provider },
});
```

> **Note**: On Deno, the built-in OpenTelemetry support (when enabled) registers a global provider and context manager for you — MongoDBee picks it up through the global API with no extra code.

## Configuration

`TelemetryOptions` (accepted by `collection()`, `multiCollection()` and `scopedMultiCollection()`):

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | `boolean` | `false` | Enable span emission for this collection (and transaction spans for the underlying MongoDB client). Resolved once at collection creation. |
| `tracerProvider` | `TracerProvider` | Global API provider (`trace.getTracerProvider()`) | Provider used to obtain the tracer. The global API provider is a silent no-op when no SDK is registered. |

The tracer is named `@diister/mongodbee` and versioned with the library version, so spans are attributed to MongoDBee in your tracing backend.

## Span Reference

### Operation spans (CLIENT)

Each instrumented operation runs under a span named `"<operation> <collection>"` (e.g. `insertOne users`, `paginate catalog`) with `SpanKind.CLIENT`, created as a child of the active context. Errors are recorded on the span (PII-safe) and re-thrown unchanged.

**`collection()` operations:**

- Writes: `insertOne`, `insertMany`, `replaceOne`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`, `findOneAndDelete`, `findOneAndReplace`, `findOneAndUpdate`, `bulkWrite`
- Reads: `findOne`, `getById`, `find` (span covers `toArray()`), `findInvalid` (span covers `toArray()`), `paginate`, `countDocuments`, `estimatedDocumentCount`, `distinct`

**`multiCollection()` operations:**

- Writes: `insertOne`, `insertMany`, `updateOne`, `updateMany`, `deleteId`, `deleteIds`, `deleteMany`, `deleteAny`
- Reads: `getById`, `findOne`, `find`, `findOneAny`, `findAny`, `paginate`, `countDocuments`, `aggregate`

**`scopedMultiCollection()` operations:**

- All operations performed through the `scope(id)`, `scopes(ids)` and `unscoped` views (same operation names as `multiCollection()`); scoped-view spans additionally carry the `mongodbee.scope` attribute
- Scope management: `listScopes`, `scopeExists`, `dropScope`, `scopeStats`

> **Note**: For cursor-returning reads (`find`, `findInvalid`), the span starts when `toArray()` is invoked — not when the cursor is created — and records the number of returned documents.

### Transaction span (INTERNAL)

`withSession()` runs its callback under an `INTERNAL` span named `mongodb.transaction` that records:

- `mongodbee.transaction.outcome` — `"committed"` or `"aborted"`
- `mongodbee.transaction.retry_count` — total write-conflict retries performed by the operations executed inside the transaction

Operation spans executed inside the callback become children of the transaction span (when a context manager is registered), so a trace shows the whole transaction with each operation nested underneath.

> **Note**: A `mongodb.transaction` span is only emitted when `withSession()` actually starts a new transaction. Nested `withSession()` calls reuse the ambient session and do not create additional spans.

## Attributes

All attribute names are exported as the `TELEMETRY_ATTRIBUTES` constant from `@diister/mongodbee/telemetry`. Attributes follow the stable OpenTelemetry database semantic conventions (`db.*`) plus a `mongodbee.*` namespace for ODM-specific data.

| Attribute | Content | Example |
|-----------|---------|---------|
| `db.system.name` | Database system, always `"mongodb"` | `mongodb` |
| `db.namespace` | Database name | `myapp` |
| `db.collection.name` | Physical MongoDB collection name | `users` |
| `db.operation.name` | Name of the public MongoDBee operation — the ODM method name, an assumed choice (not the low-level driver command) | `insertOne`, `paginate` |
| `db.operation.batch.size` | Number of documents/operations in a batched call | `25` |
| `db.response.returned_rows` | Number of documents returned by the operation | `10` |
| `error.type` | Error class name when the operation failed | `MongoServerError` |
| `mongodbee.doc_type` | Document `_type` targeted by a multi-collection operation | `product` |
| `mongodbee.scope` | Scope value(s) of a scoped multi-collection view (string or string[]) | `exposition:abc123` |
| `mongodbee.filter.keys` | Sorted, comma-joined field paths of the user filter — never values. `$and`/`$or`/`$nor` are flattened into their clauses' keys | `email,status` |
| `mongodbee.update.operators` | Sorted, comma-joined update operator names | `$set,$unset` |
| `mongodbee.update.fields` | Number of fields touched by a document-style update — never values | `3` |
| `mongodbee.retry.count` | Write-conflict retries performed by this operation | `2` |
| `mongodbee.result.matched_count` | Documents matched by an update/replace operation | `1` |
| `mongodbee.result.modified_count` | Documents modified by an update/replace operation | `1` |
| `mongodbee.result.deleted_count` | Documents deleted by a delete operation | `4` |
| `mongodbee.result.inserted_count` | Documents inserted by an insert operation | `25` |
| `mongodbee.transaction.outcome` | Transaction outcome | `committed`, `aborted` |
| `mongodbee.transaction.retry_count` | Write-conflict retries of operations executed inside the transaction | `0` |

## Privacy (PII) Policy

Spans **never carry user data**. Concretely:

- **Filter, document and update values are never recorded.** Only structural information is emitted: field names (`mongodbee.filter.keys`), operator names (`mongodbee.update.operators`) and counts (`mongodbee.update.fields`, batch sizes, result counts).
- **Validation error messages are replaced.** Valibot issues embed the *received values* in their messages, so validation failures (valibot `ValiError`, MongoDBee validation throws) are recorded with a synthetic message: `"Validation failed (details omitted: may contain document values)"`. The error type is still preserved in `error.type`.
- **Driver error messages are recorded as-is**, per standard OpenTelemetry conventions. MongoDB driver messages describe the failure (duplicate key, timeout, ...) and are the primary debugging signal.

This makes the structural attributes safe to use for debugging query shapes. For example, spotting a query that forgot its scope constraint:

```text
findAny catalog
  mongodbee.filter.keys = "status"        ← no _scope key in the filter
  (no mongodbee.scope attribute)          ← not issued through a scoped view
```

A span whose `mongodbee.filter.keys` lacks `_scope` *and* that has no `mongodbee.scope` attribute reveals an unscoped query — a potential cross-tenant read — without exposing a single filter value.

## Span Parenting

Operation spans are created as **children of the active OpenTelemetry context**. For parenting to work (HTTP request span → transaction span → operation spans), the application must have a **context manager** registered:

- `provider.register()` from the Node.js / Deno SDKs installs one automatically.
- Deno's built-in OpenTelemetry support provides one natively.

Without a registered context manager, spans are still emitted and exported — but each one is a root span, with no parent/child relationship between them.

## Limitations (v1)

- **Change streams are not traced**: `watch()` / `.on(...)` event listeners emit no spans.
- **Streamed cursor iteration is not covered**: only `toArray()` is instrumented on cursor-returning reads. Iterating with `next()` or `for await` bypasses the span.
- **`collection().aggregate()` is not traced**: it returns a raw driver cursor. This differs from `multiCollection().aggregate()`, which materializes its results and is traced.
- **DDL / index / admin operations are not traced**: collection creation, validator and index management emit no spans.
- **Migrations and the CLI are not traced.**
- **No metrics**: MongoDBee emits traces only. For low-level per-command spans or metrics, use the MongoDB driver's command monitoring or `@opentelemetry/instrumentation-mongodb` on the application side — both are complementary to MongoDBee's ODM-level spans.

# Time Series Collections

## Overview

`timeSeriesCollection()` opens a MongoDB
[time series collection](https://www.mongodb.com/docs/v8.0/core/timeseries-collections/)
behind the same Valibot validation as the other collection kinds. It is for
append-only measurements: a scan journal, an audit trail, sensor readings.

It is a separate factory, not an option of `collection()`, because a time
series collection shares almost nothing with a regular one: MongoDB gives it
no validator, no unique index, no write inside a transaction, no update. So
its API only has what a time series supports — **inserts, reads and
`deleteMany`** — and everything else fails by name, before the round trip,
instead of surfacing a server error.

Requires **MongoDB 8.0** (what every rule below was checked on). Time series
exist since 5.0, but an older server differs where it matters: a 6.0 only
deletes by `metaField`.

```ts
import {
  defineTimeSeries,
  desc,
  index,
  timeSeriesCollection,
} from "@diister/mongodbee";
import * as v from "@diister/mongodbee/schema";

export const badgeScans = defineTimeSeries({
  schema: v.object({
    at: v.date(), // when the badge was scanned, on the device's clock
    meta: v.object({ exposition: v.string(), point: v.string() }),
    scanId: v.string(),
    participantId: v.string(),
    receivedAt: v.date(),
    data: v.optional(v.record(v.string(), v.unknown())),
  }),
  timeField: "at",
  metaField: "meta",
  granularity: "minutes",
  expireAfterSeconds: 2 * 365 * 24 * 3600,
  indexes: (f) => [
    index(f.scanId),
    index(f.participantId, desc(f.at)),
    index(f.receivedAt),
  ],
});

const scans = await timeSeriesCollection(db, "badge_scans", badgeScans);

await scans.insertOne({ at, meta: { exposition, point }, scanId, participantId, receivedAt: new Date() });

const ofExposition = scans.withMeta({ exposition });
const lastHour = await ofExposition
  .find({ at: { $gte: since, $lt: until } })
  .sort({ at: -1 })
  .limit(100)
  .toArray();

await scans.deleteMany({ participantId }); // GDPR erasure, on a measurement field
```

## Declaring: `defineTimeSeries`

| Field | |
| --- | --- |
| `schema` | One measurement, a `v.object(...)`. |
| `timeField` | A top-level, required `v.date()` field. Typed: only the schema's `Date` fields are accepted. **Fixed at creation.** |
| `metaField` | Optional. A top-level field labelling the series; keep its cardinality low. **Fixed at creation**, and its value is never updated. |
| `granularity` | `"seconds"` (MongoDB's default), `"minutes"` or `"hours"`. Can only be raised later. |
| `bucketMaxSpanSeconds` + `bucketRoundingSeconds` | Custom buckets instead of `granularity`: both, equal, from 1 to 31536000. Can only be raised later. |
| `expireAfterSeconds` | Retention, counted from each measurement's `timeField`. Changes freely. |
| `indexes` | `index(...)` declarations, as in `defineType`. |

A wrong declaration throws `TimeSeriesDefinitionError` where it is written:
a `timeField` that is optional or not a date, a `metaField` equal to the
`timeField` or to `_id`, a dotted field name, `granularity` combined with
custom buckets, unequal bucket values, and the indexes below.

Choose the buckets for how you read, not how you write: MongoDB stores the
measurements of one `metaField` value and one time window together. With
`"minutes"` a bucket spans up to 24 hours, with `"hours"` up to 30 days.

## Operations

| | |
| --- | --- |
| `insertOne(doc)` | Validates, inserts, returns the `_id`. |
| `insertMany(docs)` | Validates the **whole** batch first, then inserts it in one command. |
| `find(filter?, options?)` | A driver `FindCursor` over validated measurements: `.sort()`, `.limit()`, `.skip()`, `for await`, `toArray()`. |
| `findOne(filter?, options?)` | One validated measurement, or `null`. |
| `countDocuments(filter?, options?)` | |
| `aggregate(pipeline, options?)` | A raw driver aggregation, unvalidated. |
| `deleteMany(filter)` | Deletes on **any** field — meta, time or measurement. Refuses an empty filter unless the collection was opened with `safeDelete: false`. |
| `withMeta(partialMeta)` | A view bound to part of the meta object, see below. |
| `withSession` | The client's `withSession`; reads may run inside it, writes may not. |
| `collection` | The driver collection, for what the typed API does not cover. Unvalidated. |

Measurements may arrive **in any order and late** — an offline device syncing
three days after the fact is the normal case. MongoDB files each one in the
bucket of its own `timeField`; nothing has to be sorted or batched by time.

There is no `paginate()`. Its cursor is anchored on a unique, indexed `_id`,
and a time series has neither. Page by time instead, which the default
`{ <metaField>: 1, <timeField>: 1 }` index serves:

```ts
const page = await ofExposition
  .find({ at: { $lt: lastSeenAt } })
  .sort({ at: -1 })
  .limit(50)
  .toArray();
```

### Validation

MongoDB accepts **no validator** on a time series collection (`create` and
`collMod` both reject one), so the schema is enforced by mongodbee alone:

- every insert is parsed through the schema first, and what is stored is the
  parsed output;
- every read (`find`, `findOne`) parses the stored document, and throws
  `DocumentValidationError` on one that fails.

A write that goes around mongodbee (`scans.collection.insertOne`, another
service) is therefore only caught when it is read.

### `withMeta` — a view bound to an exposition

`withMeta` is to a time series what `.scope(id)` is to a scoped
multi-collection. It binds one or more keys of the meta object; the view's
inserts fill them, and its reads and deletes only see the measurements that
carry them.

```ts
const ofExposition = scans.withMeta({ exposition: "exposition:abc" });

// `meta.exposition` is filled; the rest of the meta is the caller's.
await ofExposition.insertOne({ at, meta: { point }, scanId, participantId, receivedAt });

await ofExposition.find({ participantId }).toArray();
await ofExposition.countDocuments();
await ofExposition.aggregate([{ $group: { _id: "$meta.point", n: { $sum: 1 } } }]);
await ofExposition.deleteMany({ participantId });

const atDoor = ofExposition.withMeta({ point }); // narrows further; meta is now optional
```

- The bound filter is AND-ed with the caller's, so a filter cannot widen the
  view — not even one naming the bound key.
- `aggregate` starts with a `$match` on the bound keys.
- A measurement carrying another value for a bound key is refused.
- The bound keys must exist in the meta schema and their values are validated
  against it. It needs a `metaField` whose schema is an object.
- `deleteMany` still needs a filter of its own. Deleting a whole exposition is
  an explicit filter on the collection:
  `scans.deleteMany({ "meta.exposition": id })`.

Unlike a scoped multi-collection, the unbound collection can read and write:
the meta is ordinary data, not a tenant boundary enforced by the type system.

### Expiration

`expireAfterSeconds` is a collection option. MongoDB removes a **bucket** once
every measurement in it has expired (`timeField` + `expireAfterSeconds` is in
the past), on a background task that wakes every 60 seconds. So expiry is not
instant, and a measurement can outlive its retention until the rest of its
bucket has expired — up to one bucket span (24 hours with `"minutes"`).

A late measurement older than the retention is accepted, then removed on the
next pass.

## What is refused, and how

| Attempt | Result |
| --- | --- |
| `insertOne` / `insertMany` / `deleteMany` inside `withSession`, or with a session in a transaction | `TimeSeriesTransactionError`: *"MongoDB cannot write to a time series collection inside a transaction, and this call runs in one (withSession). Move it after the transaction (afterCommit), or wrap it in outsideTransaction(() => ...) so it commits on its own."* |
| A raw write on `.collection` inside `withSession` | The same error, naming `collection.<method>`. |
| `updateOne`, `updateMany`, `replaceOne`, `findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete`, `bulkWrite`, `upsert` | Not in the type. Called anyway (JavaScript, a cast): `TimeSeriesUnsupportedError`, *"timeSeriesCollection("x").updateOne is not available: …"* with the reason and the alternative. |
| `watch`, `on`, `off`, `enableWatching: true` | Not in the type; `TimeSeriesUnsupportedError`: change streams are not supported. |
| `unique(...)` or `withIndex(..., { unique: true })` | `TimeSeriesDefinitionError` at `defineTimeSeries`: *"index (scanId) is unique, and MongoDB does not support unique indexes on a time series collection. Declare it with index(...) and deduplicate on read, or keep what must be unique in a regular collection."* |
| A TTL index (`expiresAfter`, `expireAfterSeconds` on `withIndex`) | `TimeSeriesDefinitionError`: use the collection-level `expireAfterSeconds`. |
| A `session` option on a write | Not in the type. |
| An insert when the collection does not exist and the schema is managed by migrations | `TimeSeriesUnsupportedError`: MongoDB would create a **regular** collection, which can never become a time series. |
| Opening a name held by a regular collection, or whose `timeField` / `metaField` / narrower buckets differ | `TimeSeriesOptionsDriftError`, with `blocked` listing each difference. Nothing is changed. |

Reads inside a transaction are allowed and join it.

A write inside a transaction must be moved out. Nothing in a time series is
unique, so a retried transaction that re-inserts would duplicate: prefer
`afterCommit`, and deduplicate on read with an identifier of your own
(`scanId` above).

## Indexes

MongoDB creates `{ <metaField>: 1, <timeField>: 1 }` with the collection.
mongodbee adds what the declaration lists and leaves that one alone:

- `index(f.a, desc(f.b))` → `{ a: 1, b: -1 }`, named `_idx_a_asc_b_desc`
  (or `_idx_<name>` with `.named()`);
- `withIndex(schema)` on a field → `{ <field>: 1 }`, named after the field.

There is no `_id` suffix and no `_scope` / `_type` prefix: those belong to
`paginate` and to the multi-collection shapes. `.where()` partial filters and
`.collated()` / `.insensitive()` are supported, on meta and measurement
fields alike.

Indexes are reconciled on every open in `"auto"` mode and at the end of every
migration: created when missing, rebuilt when their key or options changed,
dropped when mongodbee named them and the declaration no longer lists them.
An index created by hand is never touched.

MongoDB also refuses, on its own: an indexed measurement field holding an
array, sparse and text indexes, and hashed or wildcard indexes on measurement
fields.

## Schema management

Like the other kinds, `timeSeriesCollection()` follows `schemaManagement`:

- **`"auto"`** — creates the collection when missing, applies the options
  `collMod` accepts (`expireAfterSeconds`, wider buckets) and reconciles the
  indexes. A difference MongoDB cannot apply throws
  `TimeSeriesOptionsDriftError`.
- **`"managed"`** (the default) — the migrations own all of that. Nothing is
  created at open. Before its first insert the collection checks, once, that
  it exists as the declared time series: MongoDB creates a missing collection
  on insert as a regular one, and that mistake cannot be undone.

`inspectTimeSeries(db, name, definition.options)` returns the same comparison
without changing anything: `{ state: "missing" | "not-time-series" | "exists",
stored, plan }`, where `plan.changes` are the differences `collMod` can apply
and `plan.blocked` those it cannot.

### What can change after creation

| Option | After creation |
| --- | --- |
| `timeField`, `metaField` | Never. |
| `granularity` | Raised only: `seconds` → `minutes` → `hours`. |
| Custom buckets | Raised only. Switching between `granularity` and custom buckets is allowed when neither the rounding nor the span decreases (`seconds` 60 s / 1 h, `minutes` 1 h / 24 h, `hours` 24 h / 30 d). |
| `expireAfterSeconds` | Freely, including removing it. |
| Indexes | Freely. |
| The measurement schema | Freely on mongodbee's side — but stored measurements are never rewritten, see below. |

Widening buckets cannot be undone.

## Migrations

Declare the collection in `schemas.timeSeriesCollections` with the same
`defineTimeSeries` object. The snapshot of each migration then carries the
creation options next to the measurement schema and the indexes.

```ts
export default migrationDefinition(id, "badge scans", {
  parent,
  schemas: {
    ...parent.schemas,
    timeSeriesCollections: {
      badge_scans: defineTimeSeries({ /* inlined, as every migration schema */ }),
    },
  },
  migrate: (migration) =>
    migration.createTimeSeriesCollection("badge_scans").end().compile(),
});
```

| Operation | Up | Down |
| --- | --- | --- |
| `createTimeSeriesCollection(name)` | Creates the time series with its options and indexes. Applied twice, it finds what it created. | Drops the collection, measurements included (the migration is marked lossy). |
| `timeSeriesCollection(name).updateOptions()` | `collMod` to the options of this migration's snapshot. | `collMod` back to the parent's — unless buckets widened, which makes the migration irreversible. |

Indexes need no operation: they are reconciled with the snapshot at the end of
every migration, up or down.

**A time series has no `transform`, `seed` nor `dedupe`.** A migration
transform rewrites each document with `replaceOne`, and a seed upserts by
`_id`; MongoDB supports neither on a time series. They are absent from the
builder's type, throw with that reason if called anyway, and
`migration.collection(name)`, `createCollection`, `updateIndexes`,
`renameCollection` and `flow` refuse a time series by name.

So when the measurement schema changes, nothing brings the stored
measurements to it. Keep the new schema compatible with them — new fields
optional, nothing narrowed — or reads of old measurements throw
`DocumentValidationError`. `check` warns whenever a migration changes a
measurement schema. A shape that must really change means a new time series
collection, filled by your own copy.

### Drift

| Where | What is compared | Outcome |
| --- | --- | --- |
| `check`, against `schemas.ts` | The last migration's snapshot and the project schema: options, measurement schema, indexes | An error naming each difference. |
| `check` / `migrate`, simulation | A migration's snapshot and its parent's | New and not created: error. Options changed and `collMod` can apply them: error unless `updateOptions()` is there. Options changed and it cannot: **"option change is not applicable"**, always an error. |
| When a migration is built | `updateOptions()` on a change `collMod` cannot apply | Throws: *"not applicable. timeField is "at" and cannot become "when"…"* |
| End of every migration | The database and the target snapshot | A difference `collMod` cannot apply, or a regular collection under the name, fails the migration. One it could apply is logged as a warning and **left as is**: only an operation of a migration changes options. |
| Runtime, `"managed"` | The database and the declaration, before the first insert | Same split: throw, or warn. |

A collection removed from `schemas.timeSeriesCollections` is not dropped;
`check` says so.

## MongoDB constraints this relies on

Checked against the MongoDB 8.0 manual and measured on a 8.0.20 replica set
(`test/time-series.test.ts`, `test/migration/time-series.test.ts`, `test/migration/cli/time-series.test.ts`).

| Constraint | Source |
| --- | --- |
| No write in a transaction (insert, multi delete, multi update: `OperationNotSupportedInTransaction`); reads are allowed | [Limitations › Transactions](https://www.mongodb.com/docs/v8.0/core/timeseries/timeseries-limitations/#transactions); measured |
| No unique index | [Limitations › Secondary indexes](https://www.mongodb.com/docs/v8.0/core/timeseries/timeseries-limitations/#time-series-secondary-indexes); measured |
| `_id` is neither unique nor indexed by default; an index on `_id` can be created, non-unique | Measured (two inserts with the same `_id` both succeed) |
| No validator, `validationLevel` nor `validationAction`, at `create` or `collMod` | [Limitations › Unsupported features](https://www.mongodb.com/docs/v8.0/core/timeseries/timeseries-limitations/#unsupported-features); measured (`'timeseries' is not allowed with 'validator'`) |
| Updates: `updateMany` only, matching and modifying only the `metaField`, operators only, no upsert | [Limitations › Updates](https://www.mongodb.com/docs/v8.0/core/timeseries/timeseries-limitations/#updates); measured |
| Deletes: any filter, `deleteMany` and `deleteOne`; `findAndModify` refused as a retryable write | Measured. The 8.0 manual pages read list no delete restriction |
| Partial indexes: allowed, on meta and measurement fields. Sparse only on the `metaField`. No text index. Measurement keys must be ascending, descending or `2dsphere`. A TTL index needs a partial filter on the `metaField` | [Limitations › Secondary indexes](https://www.mongodb.com/docs/v8.0/core/timeseries/timeseries-limitations/#time-series-secondary-indexes) for sparse, text and multikey; the rest measured |
| `granularity` is `seconds`, `minutes` or `hours`; custom buckets need both values, equal, 1–31536000, and exclude `granularity` | [`create`](https://www.mongodb.com/docs/v8.0/reference/command/create/); measured |
| `collMod`: `expireAfterSeconds` (number or `"off"`), wider buckets only; `timeField` / `metaField` are not accepted | [`collMod`](https://www.mongodb.com/docs/v8.0/reference/command/collMod/), [Limitations](https://www.mongodb.com/docs/v8.0/core/timeseries/timeseries-limitations/); measured |
| No change stream on the collection | [Limitations › Change streams](https://www.mongodb.com/docs/v8.0/core/timeseries/timeseries-limitations/#change-streams); measured (`CommandNotSupportedOnView`) |
| Expiry removes whole buckets, every 60 s | [Automatic removal](https://www.mongodb.com/docs/v8.0/core/timeseries/timeseries-automatic-removal/); measured |
| `timeField` must be a BSON date; `timeField` and `metaField` are top-level, `metaField` is neither `_id` nor the `timeField` | [`create`](https://www.mongodb.com/docs/v8.0/reference/command/create/); measured |
| A collection cannot be converted into a time series, nor renamed | [Limitations](https://www.mongodb.com/docs/v8.0/core/timeseries/timeseries-limitations/); measured |
| A measurement is at most 4 MB; a bucket holds up to 1000 measurements or 125 KB | [Limitations](https://www.mongodb.com/docs/v8.0/core/timeseries/timeseries-limitations/) (not measured) |

## Not covered

- `paginate`, `distinct` (MongoDB advises against `distinct` on a time
  series), `insertOneReturning`, computed fields, readers and the request
  cache.
- Seeding and deleting measurements from a migration.
- `extract`, `seed`, the privacy plan, the scenario runner and the studio do
  not list time series collections.
- Sharded clusters: the shard key rules of a time series are not handled.

## See also

- [README](../README.md) — the collection shapes
- [MIGRATIONS.md](./MIGRATIONS.md) — the migration system
- [SCOPED-MULTI-COLLECTIONS.md](./SCOPED-MULTI-COLLECTIONS.md) — `.scope(id)`,
  which `withMeta` mirrors

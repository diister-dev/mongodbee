# Scoped Multi-Collections

## Overview

A **scoped multi-collection** is a
[MultiCollection](../README.md#multi-collection-api) partitioned by a
discriminator key — a `_scope` value such as `tenantId` or `expositionId`. Every
scope lives in a **single physical MongoDB collection**; the API layers
scope-bound views on top of it so a query can never accidentally cross a scope
boundary. Scope safety is enforced _by construction_: you narrow to a scope with
`.scope(id)`, and every read, write, aggregation and paginated query issued
through that view is automatically constrained to it.

Each stored document carries three reserved meta fields injected by the library:

- `_id` — the document identifier (auto-minted as `type:<ulid>` when omitted)
- `_type` — the document-type discriminator (as in a plain multi-collection)
- `_scope` — the partition discriminator

`_scope` and `_type` are **reserved**: they cannot appear in your type field
schemas, and they cannot be passed to `insertOne` / `insertMany` / `updateOne`
(the view injects them for you).

This document covers the parts of the API that need more than the quickstart in
the [README](../README.md#-scoped-multi-collections). For a general
introduction, start there.

## `findProject` — partial, unvalidated projected reads

`findProject` is a **distinct method**, not a flag on `find`, because its result
contract is fundamentally different:

```typescript
findProject<K, P>(
  type: K,
  fields: readonly P[],
  filter?: Filter,
  options?: FindOptions,
): Promise<Pick<Doc, P | "_id" | "_type" | "_scope">[]>
```

- **Partial**: only the fields you list are returned, plus the meta fields
  (`_id`, `_type`, `_scope`) which are always kept so a projected document stays
  identifiable.
- **Unvalidated**: projected documents are returned _raw_. Validating a subset
  of fields against the full type schema would reject the omitted ones, so
  `findProject` deliberately skips the per-document parse. Schema transforms are
  **not** applied.

Because it skips validation and returns fewer fields, `findProject` cuts BSON
deserialization — the dominant cost of a large read — and is roughly **2.3×
faster** than a full validated `find`. Reach for it when you need a handful of
fields from many documents (list views, pickers, export projections):

```typescript
const rows = await catalog
  .scope("exposition:abc123")
  .findProject("artwork", ["title", "year"]);
// rows: { _id, _type, _scope, title, year }[]  — partial + unvalidated
```

`findProject` is available on the single-scope view (`.scope(id)`) and on the
read-only multi-scope views (`.scopes([...])` and `.unscoped`). It is **not** a
`paginate` option — use `paginate`'s `pipeline` / `prepare` / `format` hooks for
projected pagination.

## `paginate` — options and cursor semantics

```typescript
paginate<K, EN, R>(
  type: K,
  filter?: Filter,
  options?: {
    limit?: number;                 // default 100
    afterId?: string;               // forward cursor  (mutually exclusive with beforeId)
    beforeId?: string;              // backward cursor (mutually exclusive with afterId)
    sort?: Sort | SortDirection;    // default { _id: 1 }
    pipeline?: (stage) => Stage[];  // scope-safe server-side stages
    prepare?: (doc) => EN;          // enrich after validation
    filter?: (doc: EN) => boolean;  // JS-side row filter
    format?: (doc: EN) => R;        // final transform
    skipTotal?: boolean;            // skip the count query
    peek?: boolean;                 // set hasMore without a count
  },
): Promise<{
  total?: number;
  position?: number;
  data: R[];
  hasMore?: boolean;
}>
```

### Options

- **`limit`** — page size. Defaults to `100`.
- **`afterId` / `beforeId`** — cursor anchors. They are **mutually exclusive**
  (`afterId` takes precedence if both are set) and must carry the type prefix of
  the page (`"artwork:..."` when paginating `artwork`); a mismatched prefix
  throws. The anchor document is looked up **within the bound scope**, so a
  cross-scope id can never seed a cursor.
- **`sort`** — a sort object (`{ year: -1 }`) or a bare direction
  (`1`/`-1`/`"asc"`/`"desc"`) applied to `_id`. Defaults to `{ _id: 1 }`. An
  `_id` tie-breaker is always appended when it is not already part of the sort,
  so duplicate sort values keep a stable, cursor-safe order.
- **`pipeline`** — scope-safe aggregation stages (see
  [the stage builder](#the-scoped-aggregate-stage-builder)) run server-side
  _before_ pagination — lookups, `addFields`, join-based filters, etc. When a
  pipeline is present, `total` reflects the documents that survive the **whole**
  pipeline, not just the base `scope` + `type` match.
- **`prepare(doc)`** — async or sync hook to enrich each validated document
  (e.g. resolve a reference) before it is filtered and formatted. Pipeline-added
  fields (like `$lookup` results) are preserved alongside the validated
  document.
- **`filter(doc)`** — a JS-side predicate applied _after_ `prepare`. A rejecting
  filter shrinks the page below `limit`; the cursor is kept open past `limit`
  and the count is enforced in JS (bounded by an internal hard cap to guard
  against a filter that rejects everything).
- **`format(doc)`** — final per-row transform; its return type becomes the type
  of `data`.
- **`skipTotal`** — skip the `countDocuments` call(s). `total` and `position`
  come back `undefined`. Use it when you only need the rows.
- **`peek`** — fetch one extra row to compute `hasMore` cheaply without a count.
  The extra row is dropped from `data`; `hasMore` is present only when `peek`
  was requested.

### Return shape

- **`total`** — number of documents matching the scoped query (omitted under
  `skipTotal`).
- **`position`** — the 0-based count of documents _before_ this page's first
  row. It is `0` on the first page, and advances as you page forward. Omitted
  under `skipTotal`. This is the value a downstream helper needs to compute
  `hasMore = position + data.length < total`.
- **`data`** — the page rows (typed by `format`, else the validated documents,
  plus any pipeline-added fields).
- **`hasMore`** — present only with `peek`.

### Cursor semantics

- A **single-field `_id` sort** uses a simple comparison (`$gt` / `$lt`
  depending on direction and paging direction).
- A **compound sort** emits the lexicographic `$or` ladder over the sort keys so
  paging is stable across ties.
- **Backward paging** (`beforeId`) walks the _reversed_ sort, then re-reverses
  the returned page so it comes back in forward order; the absolute `position`
  is resolved after the page length is known.

```typescript
const view = catalog.scope("exposition:abc123");

// Page 1
const p1 = await view.paginate("artwork", {}, {
  limit: 25,
  sort: { year: -1 },
});
// p1.position === 0, p1.total === <matching count>

// Next page — anchor on the last row of p1
const p2 = await view.paginate("artwork", {}, {
  limit: 25,
  sort: { year: -1 },
  afterId: p1.data[p1.data.length - 1]._id,
});
// p2.position === 25
```

## The scoped aggregate stage builder

`aggregate(stageBuilder)` and `paginate`'s `pipeline` option both hand your
callback a **scope-aware stage builder**. Every stage it produces respects the
bound scope; in particular, the lookup helpers inject the scope constraint into
the joined sub-pipeline so cross-scope leakage is structurally impossible.

The single-scope view prepends `{ $match: { _scope: <id> } }` to your pipeline;
the multi-scope `.scopes([...])` view prepends
`{ $match: { _scope: { $in: [...] } } }`; the `.unscoped` view prepends nothing.

```typescript
const rows = await catalog.scope("exposition:abc123").aggregate((stage) => [
  stage.match("artwork", { year: { $gte: 1900 } }),
  stage.lookup("artist", "artistId", "_id", "artist"),
  stage.sort({ year: 1 }),
]);
```

### Stage helpers

- **`match(type, filter)`** — `{ $match: { _type: <type>, ...filter } }`.
- **`unwind(type, field)`** — `{ $unwind: "$<field>" }`.
- **`lookup(type, localField, foreignField, asOrOptions?)`** — a `$lookup` into
  the **same** collection, joining to documents of `type`. The sub-pipeline
  matches on `foreignField == localValue`, `_type == type`, **and** the bound
  scope — so the join can only ever pull documents from the same scope. The
  fourth argument is either the output field name (a string, defaulting to
  `localField`) or `{ as?, pipeline?, let? }`. A nested `pipeline` receives the
  same scope-bound stage builder, so nested lookups stay scoped too.
- **`anyLookup(localField, foreignField, asOrOptions?)`** — like `lookup` but
  **without** the `_type` constraint (joins across all document types). Still
  scope-bounded.
- **`externalLookup(fromCollection, localField, foreignField, asOrOptions?)`** —
  a `$lookup` into a **different, external collection**. **No scope injection**
  happens here (the external collection has no `_scope` concept), so this is
  your escape hatch for joining reference data that lives outside the scoped
  collection. Options accept a raw `pipeline` array (not the stage builder) and
  an optional `let`.
- **`project`, `addFields`, `group`, `sort`, `limit`, `skip`** — thin wrappers
  emitting the corresponding `$project` / `$addFields` / `$group` / `$sort` /
  `$limit` / `$skip` stage.

### How scope is injected into a lookup

For `lookup` and `anyLookup`, the generated `$lookup` uses the correlated
sub-pipeline form (`let` + `pipeline`) and adds the scope predicate to the
sub-pipeline's `$match: { $expr: { $and: [...] } }`:

- single scope → `{ $eq: ["$_scope", "<id>"] }`
- multi scope (`.scopes([...])`) → `{ $in: ["$_scope", ["<id>", ...]] }`
- unscoped → no scope predicate is added (every document is reachable)

Because the constraint lives _inside_ the join's sub-pipeline, there is no way
to write a lookup through this builder that reaches documents in another scope.

## Index semantics

Indexes are applied automatically at construction time from the `withIndex(...)`
annotations on your type field schemas (see
[`withIndex`](../README.md#-indexes-with-withindex)). Scoped multi-collections
use a dedicated strategy.

### The always-on base indexes

Two system indexes are **always** created, independent of any `withIndex`
annotations:

- **Base index `{ _scope: 1, _type: 1, _id: 1 }`** (named
  `_scope_1__type_1__id_1`). Every query a scoped view issues leads with
  `_scope` and `_type`, so this covers the common access path even for types
  that declare no `withIndex` fields. The trailing `_id` key exists so the
  default `paginate` sort — which appends `_id` as a tie-breaker (effective
  default sort `{ _id: 1 }` under `{ _scope, _type }` equality) — is served by
  the index instead of forcing an in-memory sort.
- **Unscoped-view index `{ _type: 1 }`** (named `_type_1`, non-unique). The
  unscoped admin view queries `{ _type: ... }` with **no** `_scope` term, which
  would `COLLSCAN` against the `_scope`-leading base index. The name matches the
  plain `multiCollection` index so a collection converted from a
  `multiCollection` adopts its existing `_type_1` instead of duplicating it.

> **Migration note.** Earlier versions created a two-key base index
> `{ _scope: 1, _type: 1 }` named `_scope_1__type_1`. On first construction
> against such a collection, mongodbee performs a one-time, idempotent migration
> that **drops** the old `_scope_1__type_1` index once the `_id`-terminated base
> index is in place. No action is required on your part.

### Per-`(scope, type)` uniqueness by default

`withIndex(schema, { unique: true })` builds a compound index
`{ _scope: 1, _type: 1, <field>: 1 }` with `unique: true` and a
`partialFilterExpression: { _type: { $eq: <type> } }`. The effect is that the
field is unique **per `(scope, type)`**:

- the same value is allowed in **different scopes** (each tenant/exposition can
  reuse it), and
- the same field name on **different types** does not collide (the `_type`
  partial filter separates them).

```typescript
const catalog = await scopedMultiCollection(db, "catalog", {
  scope: refId("exposition"),
  types: {
    user: { email: withIndex(v.string(), { unique: true }), name: v.string() },
  },
});

const a = catalog.scope("exposition:a");
const b = catalog.scope("exposition:b");

await a.insertOne("user", { email: "x@example.com", name: "Alice" });
await b.insertOne("user", { email: "x@example.com", name: "Bob" }); // OK — different scope
await a.insertOne("user", { email: "x@example.com", name: "Eve" }); // E11000 — same scope+type
```

### Cross-scope uniqueness with `global: true`

`withIndex(schema, { unique: true, global: true })` drops `_scope` from the key,
building `{ _type: 1, <field>: 1 }` (unique, same `_type` partial filter). The
constraint then spans **every** scope — the right choice for slugs, public
identifiers and anything that must be globally unique regardless of tenant:

```typescript
types: {
  catalog: {
    slug: withIndex(v.string(), { unique: true, global: true }),
    title: v.string(),
  },
}
// The same slug cannot exist in two scopes.
```

### User `partialFilterExpression`

Any `partialFilterExpression` you pass is **AND-merged** with the automatic
`_type` filter, producing
`{ $and: [ <yourFilter>, { _type: { $eq: <type> } } ] }`. This lets you further
narrow the index (partial indexes, conditional uniqueness).

### Caveat: `unique` + optional field indexes missing values as `null`

The automatic partial filter pins only `_type` — it does **not** add an
`$exists` clause. So for a `unique` index over an **optional** field, documents
that omit the field are still indexed, with the field read as `null`. Two
documents in the same scope+type that both omit the field therefore share the
key `(scope, type, null)` and the second insert fails with **E11000**:

```typescript
types: {
  product: {
    sku: withIndex(v.optional(v.string()), { unique: true }),
    name: v.string(),
  },
}

const s = catalog.scope("exposition:a");
await s.insertOne("product", { name: "no-sku-1" });          // OK
await s.insertOne("product", { name: "no-sku-2" });          // E11000 — both index sku=null
```

To allow multiple documents without the field while still enforcing uniqueness
on present values, supply your own `partialFilterExpression` that requires the
field to exist — it is AND-merged with the `_type` filter, so missing-field
documents drop out of the index:

```typescript
sku: withIndex(v.optional(v.string()), {
  unique: true,
  partialFilterExpression: { sku: { $exists: true } },
}),
// Multiple docs may omit `sku`; duplicates of a present `sku` are still rejected.
```

### Legacy index cleanup

When a collection that was previously managed by a plain `multiCollection` is
re-opened as a scoped multi-collection, its legacy per-field indexes
(`<type>_<field>`, key `{ <field>: 1 }`) enforce **cross-scope** uniqueness —
exactly what scoped semantics must relax. Those legacy unique indexes are
detected (by name prefix _and_ the `_type` partial-filter signature) and
dropped, while user-created custom indexes and the bare `_type_1` index are
preserved.

## Schema management

`scopedMultiCollection()` follows the same `runtime.schemaManagement` setting as
`collection()` and `multiCollection()`:

- `"managed"` (the default): initialisation never issues DDL. The validator and
  the scoped indexes are owned by migrations, which apply them through the
  migration applier. The application account only needs `readWrite`; in
  particular it never runs `collMod`, an action the built-in `readWrite` role
  does not grant.
- `"auto"`: initialisation creates the collection with its validator, or updates
  the validator when the stored one differs, and reconciles the scoped indexes.
  Meant for development, where no migration is written yet.

The per-collection `schemaManagement` option overrides the global setting for
one catalog (`"auto"`, `"managed"`, or `"inherit"`, the default). Initialisation
inside an active session never issues DDL, whatever the mode.

## See also

- [README — Scoped Multi-Collections](../README.md#-scoped-multi-collections) —
  introduction and quickstart.
- [README — Indexes with `withIndex`](../README.md#-indexes-with-withindex) —
  the `withIndex` annotation used above.
- [MIGRATIONS.md](./MIGRATIONS.md) — evolving schemas over time.

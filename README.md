<div align="center">
  <img src="https://raw.githubusercontent.com/diister-dev/mongodbee/main/brand/logo-wordmark.svg" alt="mongodbee" width="360" />

# MongoDBee

**A type-safe MongoDB layer where your Valibot schema is the source of truth —
for validation, for TypeScript types, for indexes, and for migrations.**

[![npm](https://img.shields.io/npm/v/%40diister%2Fmongodbee)](https://www.npmjs.com/package/@diister/mongodbee)
[![JSR](https://jsr.io/badges/@diister/mongodbee)](https://jsr.io/@diister/mongodbee)
[![License](https://img.shields.io/github/license/diister-dev/mongodbee)](https://github.com/diister-dev/mongodbee/blob/main/LICENSE)

</div>

Define a schema once. MongoDBee infers the TypeScript types from it, validates
every write against it, creates the declared indexes, and gives you a migration
system that evolves it — with rollbacks — instead of you editing collections by
hand.

Runs on **Bun**, **Node.js 22.18+** and **Deno**.

## Installation

```bash
# Bun / npm / pnpm / yarn
bun add @diister/mongodbee mongodb

# Deno
deno add jsr:@diister/mongodbee
```

The same code ships to both registries under the same name.

```ts
import { collection, multiCollection, withIndex } from "@diister/mongodbee";
import * as v from "@diister/mongodbee/schema";
import { migrationDefinition } from "@diister/mongodbee/migration";
```

<details>
<summary><b>Node.js: what type stripping means for your migration files</b></summary>

The migration CLI loads your migration and schema files as TypeScript directly,
which relies on the runtime **stripping** types — on by default since Node
22.18, stable since 24.12. The library also uses `Set.prototype.difference`,
which landed in Node 22.

Stripping erases types, it does not compile them, so **your migration and schema
files** must avoid the constructs that need real code generation: `enum`,
`namespace` containing runtime code, parameter properties, decorators, and
import aliases. Write `import type { … }` for type-only imports.

None of this applies under Bun or Deno, which transpile fully, nor to the
library itself, which ships compiled JavaScript.

</details>

## Quick start

```ts
import { collection, MongoClient, withIndex } from "@diister/mongodbee";
import * as v from "@diister/mongodbee/schema";

const client = new MongoClient("mongodb://localhost:27017");
await client.connect();
const db = client.db("myapp");

const users = await collection(db, "users", {
  username: v.pipe(v.string(), v.minLength(3)),
  email: withIndex(v.pipe(v.string(), v.email()), { unique: true }),
  age: v.pipe(v.number(), v.minValue(0)),
});

const id = await users.insertOne({
  username: "janedoe",
  email: "jane@example.com",
  age: 28,
});

const user = await users.findOne({ _id: id });
if (user) user.email; // string — inferred from the schema
```

Invalid data is rejected before it reaches MongoDB, the unique index on `email`
is created for you, and `deleteMany()` without a filter is refused (delete an
explicit list of ids with `deleteIds([...])`).

## The collection shapes

MongoDBee offers three, in increasing order of how much they pack into one
physical MongoDB collection.

| | One collection holds | Narrow with |
| --- | --- | --- |
| `collection` | one document type | — |
| `multiCollection` | several document types, tagged `_type` | `.find("product", …)` |
| `scopedMultiCollection` | several types **× many tenants**, tagged `_type` + `_scope` | `.scope(id).find("product", …)` |

### Multi-collections

Group related document types into one collection instead of spreading them
across many. Each document gets a `_type` field, and every method takes the type
as its first argument.

```ts
const catalog = await multiCollection(db, "catalog", {
  product: {
    name: withIndex(v.string(), { unique: true }),
    price: v.number(),
    category: v.string(),
  },
  category: {
    name: withIndex(v.string(), { unique: true }),
  },
});

const electronicsId = await catalog.insertOne("category", {
  name: "Electronics",
});
const phoneId = await catalog.insertOne("product", {
  name: "Smartphone",
  price: 499.99,
  category: electronicsId,
});

const products = await catalog.find("product", { category: electronicsId });
```

Unique indexes are scoped to the document type via partial filter expressions,
so a `product` and a `category` may share a name.

`aggregate` takes a builder whose stages know the document types:

```ts
const results = await catalog.aggregate((stage) => [
  stage.match("product", { price: { $gt: 50 } }),
  stage.lookup("product", "category", "_id"),
]);
```

Updates accept dot-notation paths into nested objects and arrays, checked
against the schema — a path that does not exist is a compile error:

```ts
const posts = await multiCollection(db, "content", {
  post: {
    title: v.string(),
    metadata: v.object({ views: v.number(), tags: v.array(v.string()) }),
  },
});

const postId = await posts.insertOne("post", {
  title: "Hello",
  metadata: { views: 0, tags: ["intro"] },
});

await posts.updateOne("post", postId, {
  "metadata.views": 42,
  "metadata.tags.0": "welcome",
});
```

#### Update operators

A field value can also be an operator, the way `removeField()` unsets one.
They work in every typed update (`updateOne`, `updateMany`, `updateWhere`,
`findOneAndUpdate`, scoped views included), on dot paths too, and in the
`$set` of a plain `collection` (whose raw `$inc`, `$push`, ... still pass as
written):

| Sentinel | Becomes | Checked against the field schema |
| --- | --- | --- |
| `increment(n)` | `$inc` | the field must accept numbers |
| `push(...items)` | `$push` with `$each` | the field must be an array, each item valid |
| `addToSet(...items)` | `$addToSet` with `$each` | same as `push` |
| `pull(value)` / `pull(condition)` | `$pull` | a plain value must be a valid item; a condition object passes through |
| `min(v)` / `max(v)` | `$min` / `$max` | `v` must be a valid value |

```ts
import { addToSet, increment, max, pull } from "@diister/mongodbee";

await posts.updateOne("post", postId, {
  "metadata.views": increment(1),
  "metadata.tags": addToSet("featured"),
});
await inbox.updateWhere("cursor", { _id: id }, { seenUpTo: max(lastId) });
await orders.updateOne("order", orderId, { lines: pull({ sku: "A-1" }) });
```

The update types only accept a sentinel where it fits: `increment` on a
number, `push` / `addToSet` / `pull` on an array of the right item type,
`min` / `max` on a comparable value, dot paths included on every collection
kind. An update that writes one path twice, or
a path and its parent, is refused before it reaches the server. A sentinel
must sit at a field path: inside a plain nested object (which replaces the
whole field) it is refused; write the dot path or wrap the object in
`partial()`. Computed fields that depend on a field an operator touches are
recomputed as they are after a `$set`. The `max` option of `updateWhere`
still works and is the same `$max` as the `max()` sentinel.

Positional paths take their conditions in `arrayFilters`, on `updateOne`,
`updateWhere` and `findOneAndUpdate`; the value is checked against the array
item's schema:

```ts
await posts.updateOne(
  "post",
  postId,
  { "comments.$[c].reactions": push("like") },
  { arrayFilters: [{ "c.id": commentId }] },
);
```

`findOneAndUpdate` also takes `sort`, to update the first match in that order
(promote the head of a waitlist), and `upsert` with `setOnInsert`, which
inserts the validated document on a miss and returns it.

`distinct(type, field, filter?)` returns the distinct values of a dot path for
one type (scoped views and read-only views bound to their scopes). `find` and
`findAny` refuse a `projection`, which would make every document fail
validation and vanish; `findProject(type, fields, filter?)` is the projected
read, partial and unvalidated by design. Its fields may be dot paths
(`"meta.color"`, `"lines.sku"`), and each row has their nested shape.

### Scoped multi-collections

A scoped multi-collection partitions one physical collection by a `_scope`
discriminator — a `tenantId`, an `expositionId`. The API only ever hands you
**scope-bound views**, so a query cannot accidentally cross a tenant boundary:
you narrow once with `.scope(id)`, and every read, write, aggregation and
paginated query through that view is constrained to it.

```ts
import { refId, scopedMultiCollection } from "@diister/mongodbee";

const catalog = await scopedMultiCollection(db, "catalog", {
  scope: refId("exposition"), // validates every scope id passed to .scope()
  types: {
    artwork: { title: v.string(), year: v.number() },
    artist: { name: withIndex(v.string(), { unique: true }) },
  },
});

const expo = catalog.scope("exposition:abc123");

await expo.insertOne("artist", { name: "Da Vinci" }); // _scope/_type injected
const artworks = await expo.find("artwork", { year: { $gte: 1500 } });

const page = await expo.paginate("artwork", {}, {
  limit: 25,
  sort: { year: -1 },
});
page.total, page.position, page.data;
```

Cross-scope reads are deliberately awkward, because they should be. Use
`catalog.scopes([a, b])` for a read-only view over specific scopes, or opt into
`allowUnscoped: true` for the read-only `catalog.unscoped` view over everything.
Neither can write — to write you must narrow to a single scope, which keeps the
target unambiguous.

Scope-level helpers: `listScopes()`, `scopeExists(id)`, `scopeStats(id)`,
`dropScope(id, { confirm: true })`, `drop({ force: true })`.

📖 [**SCOPED-MULTI-COLLECTIONS.md**](https://github.com/diister-dev/mongodbee/blob/main/doc/SCOPED-MULTI-COLLECTIONS.md)
— the full method surface, `findProject`, cursor semantics, the scope-injecting
aggregate stage builder, and per-`(scope, type)` index rules.

### Multi-models

One shape, many collection instances — per-user workspaces, per-tenant data.
Define the model once with `defineModel`, then create instances in a migration
and open them with `multiCollection`.

```ts
export const workspaceModel = defineModel("workspace", {
  schema: {
    info: { _id: v.literal("info:0"), name: v.string() },
    task: { _id: dbId("task"), title: v.string(), completed: v.boolean() },
  },
});
```

### Reading a document that fails its schema

Every read parses the stored document through the schema, so transforms apply
and the returned type is what the schema says. A document that fails it (data
written around mongodbee, or before a schema change) is handled per read:

| Read | Invalid stored document |
| --- | --- |
| `getById`, `findOne`, `findOneAny`, `findOneAndUpdate` (every collection kind) | throws `DocumentValidationError` (`errors`, `result`) |
| `collection().find()`, on every cursor path (`toArray`, `for await`, `next`) | throws `DocumentValidationError` |
| `find` and `findAny` of multi and scoped collections | left out, logged as a warning without any document value |
| `paginate` (every collection kind) | left out, counted in `page.skippedInvalid`, logged the same way |
| `collection().findInvalid()` | lists exactly those documents |

A list keeps serving its valid rows and says how many it left out; a single
read never hands back data its type does not describe. A scoped `find` with
`validate: false` skips parsing on purpose and returns stored documents as is.

## Indexes

`withIndex()` attaches index metadata to a field's schema; MongoDBee creates,
updates and cleans up the indexes to match.

```ts
const users = await collection(db, "users", {
  email: withIndex(v.string(), { unique: true }),
  username: withIndex(v.string(), { unique: true, insensitive: true }),
  city: withIndex(v.string(), { collation: { locale: "fr", strength: 2 } }),
});
```

| Option | Effect |
| --- | --- |
| `unique` | reject duplicate values (default `false`) |
| `insensitive` | case-insensitive, via MongoDB collation (default `false`) |
| `collation` | custom collation for advanced sorting |

## Migrations

Migrations are how you change the database. Each one names its parent, carries
the full schema as of that point, and can be rolled back.

```bash
bunx @diister/mongodbee init       # or: npx @diister/mongodbee init
```

That scaffolds `migrations/`, `mongodbee.config.ts` and `schemas.ts`:

```ts
import { defineConfig } from "@diister/mongodbee";
import process from "node:process";

export default defineConfig({
  database: {
    connection: { uri: process.env.MONGODB_URI! },
    name: process.env.MONGODB_DATABASE!,
  },
  paths: { migrations: "./migrations", schemas: "./schemas.ts" },
});
```

Once the package is installed, its `mongodbee` binary is on your path, so the
shorter `bunx mongodbee <cmd>` works too:

```bash
bunx mongodbee generate --name initial_schema
bunx mongodbee check      # validate + simulate against a mock database
bunx mongodbee status     # what is applied, what is pending
bunx mongodbee migrate    # apply everything pending
bunx mongodbee rollback   # undo the last one
```

Under Deno: `deno run -A jsr:@diister/mongodbee/migration/cli/bin <cmd>`.

A generated migration looks like this:

```ts
import { migrationDefinition } from "@diister/mongodbee/migration";
import { dbId } from "@diister/mongodbee";
import * as v from "valibot";

export default migrationDefinition(
  "2025_10_14_1234_ABC123@initial_schema",
  "initial_schema",
  {
    parent: null, // the first migration has no parent
    schemas: {
      collections: {
        users: { _id: dbId("user"), name: v.string(), email: v.string() },
      },
      multiCollections: {},
      multiModels: {},
    },
    migrate(migration) {
      migration.createCollection("users")
        .seed([{ name: "Alice", email: "alice@example.com" }])
        .end();

      return migration.compile();
    },
  },
);
```

Then update `schemas.ts` to match — it must always mirror the newest migration.

**The builder**, in short: `createCollection(name)` and `collection(name)` for
plain collections, `createMultiCollection(name)` and
`createMultiModelInstance(instance, model)` for the others — the latter two
chain `.type(name).seed(docs).end()` per document type. `collection(name)
.transform({ up, down, lossy })` rewrites existing documents. Always finish a
chain with `.end()`, and always `return migration.compile()`.

`check` simulates every migration against a mock database and verifies your
`schemas.ts` still matches, so a broken migration fails before it touches
production.

### Guarding startup

```ts
import { checkMigrationStatus } from "@diister/mongodbee/migration";

const status = await checkMigrationStatus({ db }); // paths from your config
if (!status.ok) throw new Error(status.message);
if (status.database && !status.database.isUpToDate) {
  console.warn(`${status.database.pendingCount} pending migration(s)`);
}
```

`assertMigrationSystemHealthy({ db })` is the throw-on-unhealthy version.

📖 [**MIGRATIONS.md**](https://github.com/diister-dev/mongodbee/blob/main/doc/MIGRATIONS.md)
— the complete migration reference.

## Studio

A local web explorer for the database of a MongoDBee project: collections by
type and scope, schemas next to their validators, indexes, the migration chain
and a configurable `check`. It is read-only unless started with `--write`.
It lives in its own npm package, installed next to the core with the same
version:

```bash
npm install --save-dev @diister/mongodbee-studio
npx mongodbee studio
```

📖 [**STUDIO.md**](https://github.com/diister-dev/mongodbee/blob/main/doc/STUDIO.md)
explains every view, the options and the write mode.

## Transactions

`withSession` runs its callback inside a MongoDB transaction. Every MongoDBee
operation on collections sharing the same client joins it automatically, through
the async context (`AsyncContext.Variable` when the runtime has it,
`AsyncLocalStorage` otherwise) — nothing to thread through your call stack.

```ts
await users.withSession(async () => {
  await users.insertOne({ username: "alice", age: 32 });
  await catalog.insertOne("product", {
    name: "Widget",
    price: 79.99,
    category: "tools",
  });
  // both commit, or both roll back
});
```

A transaction always runs on the primary, with `snapshot` reads and a
`w: "majority"` commit, whatever the client's read preference. A
`primaryPreferred` URI no longer breaks it. The commit is retried when its
outcome is unknown, e.g. after a primary stepdown. Pass `retry: true` to also
replay the whole callback on a `TransientTransactionError`, such as an
election mid-transaction. Only do this when the callback has no side effect
outside the database:

```ts
await users.withSession(async () => {
  /* ... */
}, { retry: true, writeConcern: { w: "majority", wtimeoutMS: 5000 } });
```

### Outside the transaction

`outsideTransaction(fn)` runs `fn` as if no transaction were open, for every
client: the typed operations it makes commit on their own, `afterCommit`
runs right away, and a `withSession` inside it opens a transaction of its
own. It is for writes that must not join the caller's transaction, a job
claim, a lease, a cross-pod lock: their conflict would otherwise abort the
caller's work, and their effect must survive its rollback.

```ts
import { outsideTransaction } from "@diister/mongodbee/session";

await users.withSession(async () => {
  const claimed = await outsideTransaction(() =>
    jobs.findOneAndUpdate("job", { _id: id, status: "pending" }, {
      status: "running",
    }),
  );
  // ...
});
```

A write outside the transaction to a document the transaction has already
written waits for the transaction to end, like any other write, so keep the
two on different documents.

### The raw driver collection

`collection.collection`, and `.collection` on a multi-collection or a scoped
multi-collection, hand out the driver collection for what the typed API does
not cover: change streams, exotic pipelines. It skips validation and the type
and scope guards (computed fields are still maintained), but it does not skip
the transaction: every CRUD, read and bulk call made without a `session` option
gets the ambient one, so a raw write rolls back with the rest. Pass
`session: undefined` to run a call outside the transaction on purpose.
`watch`, index and collection DDL, and `estimatedDocumentCount` never get the
session, since none of them may run in a transaction.
`ambientSessionCollection(db.collection(name))`, from
`@diister/mongodbee/session`, wraps any other driver collection the same way.

```ts
await jobs.withSession(async () => {
  await jobs.collection.updateOne({ _id: id }, { $inc: { attempts: 1 } });
  throw new Error("rolled back, attempts included");
});
```

### After the commit

`afterCommit` defers a side effect until the transaction has committed. The
callback runs outside the ended session, once, after the last attempt: a
rolled back attempt, or one replayed by `retry`, drops what it queued. A
nested `withSession` defers to the outermost commit, a failing callback is
logged without failing the commit, and outside a transaction the callback
runs right away.

```ts
import { afterCommit } from "@diister/mongodbee/session";

await users.withSession(async () => {
  await users.insertOne({ username: "alice", age: 32 });
  await afterCommit(() => notifyWelcome("alice"));
});
```

## Request context

`withRequestContext` scopes work to one request. With `memoizeReads`, identical
`getById`, `findOne`, `find`, `findProject` and read-only `aggregate` calls of
that request reach MongoDB once; the first caller gets the loaded value and
every later one its own copy. A result of more than 100 rows is not kept: a
round-trip on an index costs less than copying it. Reads inside a
transaction are never memoized, and any write through MongoDBee or a commit
clears the memo. A write made with the raw driver needs `invalidateReads()`,
or a client created with `monitorCommands: true` and watched once with
`invalidateReadsOnDriverWrites(client)`.

```ts
import {
  invalidateReadsOnDriverWrites,
  withRequestContext,
} from "@diister/mongodbee/session";

invalidateReadsOnDriverWrites(client);

Deno.serve((request) =>
  withRequestContext(() => app.fetch(request), {
    memoizeReads: request.method === "GET",
  })
);
```

`requestReadStats()` reports how many reads were loaded, reused and
invalidated in the current request.

## Replica sets and read preference

Reads follow the collection's read preference, falling back to the client's
(`primary` unless the URI says otherwise). To offload reads that tolerate a
little replication lag, such as listings, dashboards or stats, onto the
secondaries, opt in per collection:

```ts
const stats = await collection(db, "stats", schema, {
  readPreference: { mode: "secondaryPreferred", maxStalenessSeconds: 90 },
});
// multiCollection takes the same option; scopedMultiCollection takes it in its config.
```

- **Inside `withSession`, every read goes to the primary.** A per-call or
  per-collection read preference is ignored there.
- **Read your own writes** with a per-call override:
  `stats.findOne(filter, { readPreference: "primary" })`. `getById`,
  `paginate` and the multi-collection `findOne`/`findOneAny`/`aggregate`
  accept `{ readPreference }` too.
- **Internal reads always use the primary**, whatever the client default:
  index and validator sync, the migration history, the multi-collection
  registry, and the whole migration CLI.
- **`paginate` on a secondary** may compute its `total` and its page on two
  different members, so the count can be slightly off while writes replicate.

### A read preference for a unit of work

When one collection serves both reads that must be fresh (permissions, a read
right after a write) and reads that can lag (analytics), set the preference
around the code instead of on the collection:

```ts
import { withReadPreference } from "@diister/mongodbee/session";

const report = await withReadPreference("secondaryPreferred", () =>
  buildReport(expositionId),
);
```

Every MongoDBee read inside it follows that preference, unless the call passes
its own `readPreference` or runs inside a transaction, which still reads the
primary. It overrides the collection's preference, not a per-call one.
Internal reads stay on the primary. A driver collection obtained with
`readingCollection(db, name)` follows it too, for code that reads with the raw
driver. `currentReadPreference()` returns the one in effect, if any.

## Change streams

Opt in with `enableWatching`, then subscribe. `on()` returns its unsubscribe
function.

```ts
const users = await collection(db, "users", schema, { enableWatching: true });

const unsubscribe = users.on("insert", (event) => {
  console.log("new user:", event.fullDocument);
});
unsubscribe();
```

> Requires MongoDB running as a replica set or sharded cluster. Without
> `enableWatching: true` no stream is opened and handlers never fire. The
> collection may also become ready a moment before the stream is actually
> established, so do not assume writes made immediately after the `collection()`
> call will be observed.

## OpenTelemetry

Strictly opt-in, and dependent only on `@opentelemetry/api` — MongoDBee never
instantiates an SDK. With no provider registered, every path is a no-op.

```ts
const users = await collection(db, "users", schema, {
  telemetry: { enabled: true },
});

await users.insertOne(doc); // → CLIENT span "insertOne users"
```

Spans carry field names, operator names and counts — never filter values,
update values or document contents. Error messages are scrubbed before being
recorded; the caller always receives the original, untouched error.

📖 [**TELEMETRY.md**](https://github.com/diister-dev/mongodbee/blob/main/doc/TELEMETRY.md)
— span catalog, attribute table and PII policy, plus a ready-to-import
[Grafana dashboard](https://github.com/diister-dev/mongodbee/blob/main/doc/grafana/README.md).

## Project status

Production-ready: the migration system, the MultiCollection and
ScopedMultiCollection APIs, multi-models, transactions, change streams and index
management.

Still maturing: type inference on advanced operations (`aggregate`, `bulkWrite`)
is weaker than on the basics, `findOneAnd*` are minimal implementations, and dot
notation for nested updates is complete on multi-collections but only partial on
plain `collection()`.

## Development

The repository is a Bun workspace: `library/` is `@diister/mongodbee`,
`studio/` is `@diister/mongodbee-studio`. Install once at the root, then work
in either package; the studio consumes the core's built `dist`, so build the
core first.

```bash
bun install        # at the repository root
cd library
bun test           # needs a MongoDB on localhost:27017
bun run check      # tsc
bun run lint       # biome
bun run build      # dist/ for npm
cd ../studio
bun run build      # server and prebuilt UI
bun test
```

The suite targets `node:test`, so it runs unchanged under `bun test`,
`node --test` and `deno test`. CI exercises all three, because Bun runs
JavaScriptCore while Node and Deno run V8 — with one exception: the
`test/migration/cli` files generate migrations and import them at run
time, which `deno test` refuses ("Loading unprepared module") although
`deno run` loads them fine, so the Deno job skips that directory and the
CLI under Deno is covered by the consumer job instead.

## License

MIT — see [LICENSE](https://github.com/diister-dev/mongodbee/blob/main/LICENSE).

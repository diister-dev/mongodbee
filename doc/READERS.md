# Readers: declared reads, cached at the right level, invalidated by the database

Status: proposal, revision 3, 2026-09-29. Steps 1 and 2 of section 14 are
implemented on `feat/readers`, with `primeFrom` for singleton readers (section
3.5) and reader spans (section 12.1, `doc/TELEMETRY.md`). Template-typed refIds
(step 0) are not done: swapped string arguments still compile. Diivento's
branch `feat/readers` runs its exposition permission providers (participant
roles, org memberships, lead, map, programme org space) on five readers: on
real data, one request calling the five providers makes 4 reads and 6 hits
where it made about 11 reads. The implementation went through three more
reviews (two adversarial, one overall); this text describes what was built,
where it differs from the plan.
Revisions 2 and 3 each follow an adversarial review that rewrote five real
Diivento loaders against the previous revision and typechecked the result;
section 16 lists what each review changed and why.

## 1. The problem

Diivento spends most of an API call's time before its business logic runs: a
permission preamble of about 17 sequential Mongo round-trips on `develop`
(3 to 5 on the unmerged performance branch), plus the BSON decoding of each
answer, which alone is about half of the API's CPU. Two independent cache
reports (2026-09-29) reach the same conclusions:

- the first lever is a **per-request** memo (L0);
- the lever that pays the most is a **per-process** cache (L1) of permission
  facts and reference data, and only if its invalidation comes from MongoDB
  itself, through a change stream, because only the database sees every
  writer (two clusters, the worker, raw driver writes, migrations);
- the same change stream should carry the future real-time signals.

Two L0 implementations now exist side by side:

| | `withRequestContext` memo (mongodbee, merged) | `requestScoped` (Diivento performance branch) |
|---|---|---|
| What is cached | every identical `find`/`findOne`/`getById`/`aggregate` | explicitly wrapped loaders |
| Key | the exact query | a business key (`userId`, `expositionId`) |
| Invalidation | automatic, on any write | manual `forget…` calls in repositories |
| Copies | yes | no, callers share the object |
| Methods | GET and HEAD only | every method |

The manual side already misses writes: `expositions.repository.ts` `update`,
`updateBrand` and the eight raw lifecycle writes never forget the information
entry, and `scans.events.ts` rewrites participants without a forget. For a
permission fact that is a stale grant for the rest of the request today, and
until a TTL once an L1 exists.

A **reader** gives the business key and the priming of the second column with
the automatic invalidation of the first, and scales from L0 to L1.

## 2. Principles

1. **Nothing the author writes by hand can fail silently in the stale
   direction.** Keys, the fields that matter and the dependencies between
   readers are derived, never declared as free text; the safe choice is the
   default (`consistency`, which arrives with L1, will default to `"strict"`).
2. **A reader is a computed field that is not stored.** It reuses the
   `from(...).by(...).where(...)` builder, the topology and the write
   interceptor of computed fields (`doc/COMPUTED.md`), so an application
   learns one vocabulary and mongodbee maintains one choke point.
3. **Correct by default under concurrency, retries, transactions, replication
   and several processes.** When unsure, read the primary: fail open to
   MongoDB, never to a cached "allow".
4. **Caching is a deployment decision; consistency is a property of the
   data.** The declaration says what the data needs; which readers use L1,
   with which TTL, is decided at registration.
5. **Each level pays for its own machinery.** L0 invalidates coarsely, which
   is exact enough for the handful of entries a request holds; the precise
   routing that L1 needs is built with L1, not before.

Non-goals: a shared cache between processes (Valkey, L2); caching decisions
(readers cache inputs such as memberships and roles, never `can()` results);
putting personal data anywhere but process memory.

## 3. Declaring readers

### 3.1 Query readers

Every fact the permission preamble needs is "the documents of one type, in one
scope, whose field equals the argument". It is declared with the computed-field
builder and a `select` terminal:

```ts
import { from, reader } from "@diister/mongodbee";

const Expo = scoped(ExpositionModel, refId("exposition"));

export const participationsOf = reader(
  "participations-of-user",
  from(Expo, "participant")
    .by((p) => p.personRef.userId)
    .where((p) => [p.personRef.kind, "user"])
    .select(["status", "personRef"]),
);

export const entrepriseMemberships = reader(
  "entreprise-memberships-of-user",
  from("member", MemberType)
    .by((m) => m.userId)
    .where((m) => [m.status, ["active", "invited"]])
    .select(["tenantId", "role", "status"]),
);

export const orgRoleByKey = reader(
  "org-role-by-key",
  from(Expo, "expo_role").by((r) => r.key).one().select(["permissions"]),
);

export const expositionInformation = reader(
  "exposition-information",
  from(Expo, "information").one().select(["name", "entreprise", "modules"]),
);

const rows = await participationsOf(expositionId, userId);
const roles = await orgRoleByKey.many(expositionId, roleKeys);
roles.get("staff");
```

- `from` is the computed fields' `from`, with one more terminal, `select`. A
  file that declares both computed fields and readers imports one `from`.
- **Arguments are derived**: the scope first when the source is scoped, then
  the `by` value. The scope comes from a typed handle, `scoped(Model,
  refId("exposition"))`, shared with `schemas.ts`, because a model alone does
  not know it is scoped. With template-typed refIds, swapping
  `(expositionId, userId)` does not compile.
- **The value is derived**: `DeepReadonly<Pick<Doc, "_id" | selected>>[]`
  ordered by `_id`, or one document or `null` with `.one()` (on its own for a
  singleton such as `information`, or after `.by()` for a unique key). A field
  that is not selected is not in the type and not in memory: `invitationToken`
  cannot reach a cache.
- `select` is required: a reader over full documents is what made narrowing
  unsafe in revision 1, and what puts tokens in memory. It takes top-level
  fields. The computed fields of a type are read by selecting `_computed`,
  which brings every computed field of the document; a recomputation of the
  subject invalidates the entries that selected it.
- `where` takes the computed fields' equality and membership predicates. Its
  values are strings, numbers, booleans or `null`, typed against the field
  (`[p.status, "actve"]` does not compile); `null` also matches a missing
  field, which is how "`$exists: false`" is written. A path takes one `where`;
  a second one on the same path is refused rather than silently replacing the
  first. A date, "an array field contains", `$ne`, `$nin` and `$or` are not
  expressible: `$ne` and `$nin` become the list of accepted values, or a
  selected field filtered by the caller; `$or` becomes two readers and a
  composite.
- **`.many(scope, keys)`** reads the keys that are not cached in one `$in`
  read, stores each key separately, and returns a `ReadonlyMap` from each key
  to its value, so the grouping does not depend on the `by` field being
  selected. The map is keyed by the values the caller passed: a key given as a
  `Date` or an `ObjectId` is found again with that same instance only. A
  reader without `by()` has no `many` (its type is `never`). Only the keys not
  cached yet count against `entriesPerRequest`.
- `select` and `one` exist only on a builder a reader can use: after
  `through()` or `sameScope()`, which belong to computed fields, they do not
  typecheck.
- The load uses the full filter, `where` included, so partial indexes serve it
  (Diivento's `personRef.userId` index is partial on `kind: "user"`).
  Registration checks that a declared index leads with the `by` field, as the
  computed topology does; it does not yet check the scope prefix nor the
  partial filter against the `where`.

### 3.2 Composite readers

A fact combining several types is a function **over other readers only**:

```ts
export const affiliations = reader(
  "exposition-affiliations",
  async (expositionId: ExpositionId, userId: UserId, participantId: ParticipantId | null) => {
    const [memberships, entreprises, created] = await Promise.all([
      participantId ? orgMembershipsOf(expositionId, participantId) : [],
      entrepriseMemberships(userId),
      orgsCreatedBy(expositionId, userId),
    ]);
    const active = entreprises.filter((m) => m.status === "active").map((m) => m.tenantId);
    const viaEntreprise = [...(await orgsOfEntreprise.many(expositionId, active)).values()].flat();
    const reached = [...new Map([...created, ...viaEntreprise].map((o) => [o._id, o])).values()];
    const orgIds = [...new Set([...memberships.map((m) => m.organizationId), ...reached.map((o) => o._id)])];
    const roles = [...(await orgRolesOf.many(expositionId, orgIds)).values()].flat();
    return { memberships, reached, roles };
  },
);
```

- Every reader called inside the function is recorded as an edge; invalidating
  an inner entry invalidates the composite entries that used it.
- A direct read inside a composite throws `ReaderDirectReadError`, through a
  mongodbee collection or through `readingCollection`. This is what makes a
  composite sound without recording filters: revision 2's routing of an update
  by id to "the entries that returned that id" missed a `pending` membership
  accepted by id, which no entry had returned. A bare `db.collection()` read
  cannot be seen and stays the author's responsibility.
- No `reads` list: the edges are recorded, and a list would repeat them.
- Arguments are plain data (`ReaderArgument`: strings, numbers, booleans,
  bigints, `null`, `undefined`, dates, BSON values, arrays and plain objects),
  so they can be keyed exactly. A `Set`, a class instance or a function does
  not typecheck, and is refused at the call if it gets through a cast.
- A composite that calls itself with the same arguments while loading is
  refused instead of waiting on itself, inside a request context or not.
- A composite is kept only when every reader it used is an entry of the same
  request cache: an inner reader that bypassed the limits, ran in a
  transaction, or ran in a nested `withRequestContext` (whose cache nobody
  invalidates once it ends) makes the composite load and not store.

### 3.3 Values

A value is **deep-frozen and shared**: no copy on a hit, typed `DeepReadonly`
so that `roles[0].permissions.push(...)` does not compile.

- A query reader's value is built from the driver's documents, so it is frozen
  in place.
- A composite's value is frozen as a **copy**: the objects the function
  returns may belong to its caller or to a module, and freezing them in place
  would break their owner. Values of other readers inside it are already
  frozen and are shared, not copied. Cycles are preserved.
- A `Date` is frozen by making its setters throw, and is typed without them
  (`FrozenDate`). A `Map` or a `Set` is frozen by making its mutators throw;
  a composite may return them, which is safer than a `Record` keyed by user
  data such as a role key (`"constructor"`).
- BSON values (`_bsontype`: `ObjectId`, `Binary`, `Decimal128`) and binary
  views are shared as they are: `Object.freeze` cannot freeze them.

A composite's value type is checked as `Promise<T & Plain<T>>`, since
`T extends Plain<T>` is a circular constraint.

### 3.4 Keys

The library builds the key from the argument tuple itself, exactly (after the
scope argument has gone through its scope schema, as writes do), plus the
database name, so two e2e scopes in one process never share an entry, plus the
reader's identity rather than its name, so two readers declared with the same
name never read each other's entries. An argument that cannot be encoded
exactly throws `ReaderArgumentError`. Only metrics and logs will see a hashed
key.

### 3.5 Priming

```ts
const expositions = await expositionInformation.primeFrom(() =>
  catalog.unscoped.find("information", { entreprise }),
);
```

`primeFrom` runs the given read and records the `find` and `findOne` calls it
makes, including through a helper. Built for now, it primes **singleton
readers only**: `.one()` without `by()`, whose value is the one document of
its scope, so a document read by anyone is the whole value of its key. A list
reader would need every document of a key, which a filtered or paginated read
does not prove, so it has no `primeFrom` (its type is `never`).

A recorded document primes its scope's entry when it is of the reader's type,
matches its `where`, and the entry is not cached yet; it is projected on the
selection and frozen as a copy. Nothing is primed from a read that ran in a
transaction, off the primary, through a projection (`findProject`), or while a
write reached the request. An `aggregate` or `paginate` read is not recorded,
and neither is a read the request memo (`withRequestContext({ memoizeReads })`)
answered from its own copy: priming sees only the reads that went to MongoDB.

### 3.6 Registration

```ts
registerReaders(client, {
  topology: computedTopology(schemas),
  readers: [participationsOf, entrepriseMemberships, orgRoleByKey, expositionInformation, affiliations],
  limits: { entriesPerRequest: 500, rowsPerEntry: 200 },
  database: () => getDatabase(),
});
```

- `topology` places each reader's source in its physical collection, as for
  computed fields. Readers ride every write through the same interceptor,
  including collections that carry no computed field (`+entreprises` today).
- `readers` is required. A query reader must be listed to be called: the
  registration is where its placement, its scope and its index are checked,
  so a reader left out would skip them; it throws `ReaderNotRegisteredError`
  instead. Names are unique within a registration.
- **The database** a reader reads, in this order: the `database` given to the
  nearest `withRequestContext(fn, { database })`, a `Db` or a function called
  at each read; otherwise the `database` resolver of the registration, when
  exactly one is registered; otherwise, outside any request context only, the
  one `Db` registered with `registerReaders(db, ...)`. Inside a request
  context nothing is guessed. The resolver is how Diivento plugs in its own
  database resolution, which already follows the e2e scope header and the
  test databases, without the header being parsed twice.
- Tests use a real database the same way; a reader has no in-memory fake.
- The L1 opt-in (`process`) comes with step 5 (section 6).

## 4. Level 0: the request

A reader called inside `withRequestContext` caches per key for that request:

- **single flight**: concurrent calls with the same key share one load, and
  an invalidation **drops the pending load** too: a load started before a write
  is never joined by a caller that comes after it;
- **shared frozen values** (section 3.3);
- **transactions**: inside a transaction a reader neither reads nor stores any
  cache, it loads through the session;
- **primary**: a reader always reads the primary, whatever the ambient
  `withReadPreference`, because its value may authorise;
- **bounded**: past `entriesPerRequest` entries, or for a result of more than
  `rowsPerEntry` rows, the reader loads without storing and counts a bypass;
- outside any request context, a reader simply loads;
- reader loads go around the request memo's storage, so nothing is held twice.

Readers are active for every HTTP method.

## 5. Invalidation

### 5.1 At L0: coarse and derived

The computed interceptor sees every write through mongodbee with its
collection, types, scope and update. A write invalidates, in the current
request, **every entry of the readers over that type in that scope**, unless
it is an update that touches none of the reader's `select`, `where` and `by`
paths. A request holds few entries, so this costs a reload at most, and it
needs no reverse index and no routing. The type and the scope are read from
the write's filter or document, `$and` clauses included: a `$in` list of
scopes invalidates those scopes, and a write whose filter does not constrain
`_scope` (or `_type`) invalidates every scope (or type) of the collection. A
replacement, a pipeline update, an insert, an upsert and a delete always count
as touching.

A write invalidates before it runs and again when it returns. Inside a
transaction, its footprint is also replayed at the commit, before any
`afterCommit` callback runs, so a read outside the transaction that cached the
pre-commit state is dropped. A read-only transaction invalidates nothing, and a
transaction's commit does not flush readers of collections it never wrote.

**The race between a load and a write** is closed per entry: an entry exists
from the moment its load starts, and an invalidation marks it stale and removes
it, so its load is returned to the callers that joined it before the write and
never stored, and a caller after the write starts a new load. This replaces the
generations of revision 3: at L0 every load has its entry, so a counter would
say nothing more. L1 will need them again (section 5.2).

**Writes mongodbee cannot attribute** to a type and scope invalidate every entry
of the request, and again at the commit when they happen inside a transaction:
a raw driver write seen through `invalidateReadsOnDriverWrites` (on the
command's start, its success and its failure, so at least once after it
landed), an aggregation with `$out` or `$merge`, and `invalidateAllReaders()`.
A raw write is invisible without `invalidateReadsOnDriverWrites`.

Invalidation reaches the enclosing request contexts too: a write inside a
nested `withRequestContext` is seen by the outer request.

### 5.2 At L1: precise routing (built with L1)

Across processes the change feed is the only source, and a change event of an
update or a delete carries `documentKey: { _id }` and the updated fields, but
no `_type`, `_scope` or unchanged field. Precise routing therefore needs more
than L0 has:

- **type from `_id`**: the prefix-to-type map is built from each type's
  declared `_id` schema, never from the type's name (`information._id` is an
  `exposition:` refId). A type whose prefix is shared or which has no refId is
  routed at collection level. Checked at boot.
- **reverse index** `documentId -> entries` for the ids an L1 entry holds; an
  update touching a `where` path of a document the entry did not return (a
  status flip into the `where`) is invisible to it, so an L1 query reader
  routes `where`-path updates of its type at `(type, all scopes)` for the
  matching `by` value when the event carries it, and at type level otherwise;
- **`by` values from `updatedFields`**, normalised: a whole `personRef` object
  set at once, dotted keys, array indexes such as `userIds.3`;
- **a scope or type change** (a document moved to another scope) invalidates
  the type in the new scope;
- **upserts** arrive as inserts, with their full document;
- **the load/event race**: an event for a document whose entry is not stored
  yet finds nothing in the reverse index. Each L1 load is stamped with its
  read time, and at store time it is checked against a short log of recent
  events keyed by `_id` and by `by` value;
- `drop`, `rename`, `dropDatabase` and `invalidate` flush every reader of the
  collection.

Whether L1 query readers can keep loading with their `where` (and so their
partial indexes) under this scheme, or must load by key only, is the first
question of step 5, answered by measurement.

## 6. Level 1: the process

With `process` enabled for it, a reader's entries also live in a per-process
store:

- **bounded** by entry count, with sampled sizes for the byte metric; a TTL per
  reader, jittered, as a safety net, never as the invalidation;
- **invalidated by the change feed** (section 5.2) and by the local writes;
- **no negative entries** by default: a `null` or an empty list stays in L0;
- **stamped with a cluster time**, the `operationTime` of the load;
- **fail open**: when the feed is not live, eventual readers serve until their
  TTL and nothing strict is in L1 anyway; the feed is live when its
  `postBatchResumeToken` keeps advancing, so an idle healthy stream is not
  mistaken for a late one;
- **lost continuity flushes**: a stream that cannot resume drops every entry and
  bumps every generation;
- **freshness floor**: a request may carry a minimum cluster time, for example
  from a real-time signal or the user's own last write; an older entry is
  reloaded. The application signs the floor, and mongodbee clamps it to the
  server's cluster time.

### 6.1 Strict readers stay at L0

The feed has lag, a window in which a revoked grant would still authorise.
Strict readers are therefore **L0 only**. After L0, `.many` and the computed
fields (`organizationIds` is computed, `roleKeys` can be), what remains of the
preamble is measured; strict L1 is built only if the measurement says it pays,
with one of two gates chosen then:

1. **dependency versions** (`doc/COMPUTED.md` section 13): every write of a key
   bumps a version in the same transaction, and a strict hit reads its keys'
   versions in one round trip. If built, section 13 is this gate, not a
   separate mechanism;
2. **an authorisation epoch** on the user, read with the session the request
   loads anyway, bumped by every revoking path.

## 7. The change feed

`watchChanges(client, spec)` opens one change stream per process:

- filtered on the server by collection and by `_id` prefix (an anchored
  `$regex` on `documentKey._id`). The filter is not indexed: every stream still
  reads the whole oplog once per process; it saves the network and the event
  building, not the scan. Its cost is measured before L1 ships. Projecting only
  field names and `by` values needs `$objectToArray` or `$getField`, since the
  updated keys contain dots;
- majority-committed only, so nothing announced can roll back;
- resumable from its last token, reporting a continuity loss when it cannot;
- with a state: live (progress of the resume token), lag, restarts;
- **public**: `feed.subscribe(filter, handler)` runs after the commit, on every
  process, for changes made by the worker, another cluster, a migration or the
  raw driver, which an in-memory event bus cannot see.

## 8. Real time

Diivento's real-time foundation sends signals ("this changed, refetch") over
SSE. With readers and the feed:

1. the feed invalidates L1 on every process;
2. the same event, through a subscriber, becomes a signal carrying the
   change's cluster time;
3. the client refetches with that cluster time as its freshness floor;
4. an SSE connection never holds one request context for its lifetime: each
   signal or permission check opens its own.

Revocation: a subscriber on the permission types tells the real-time hub,
which re-checks that user's open subscriptions.

## 9. Batching (later)

Calls of one query reader for different keys in the same tick coalesce into
one `.many` read, as DataLoader does. `.many` is the explicit form and ships
first.

## 10. Read preference and causal reads

Readers read the primary (section 4). Other reads that go to secondaries under
`withReadPreference` honour the freshness floor with
`readConcern: { level: "majority", afterClusterTime }`.

## 11. What belongs in a reader

A reader earns its place when the same small fact is read several times per
request, by code that does not share variables: authorization, the context a
request is resolved in, the settings of the tenant it runs under.

| Fact | Kind | L1 |
|---|---|---|
| the settings document of a tenant (name, enabled features, branding) | query `.one()` | first candidate, once `consistency` exists |
| the roles or memberships a subject holds | query | no, until measured |
| the permissions a role grants, by role key | query `.one()` or `.many()` | no, until measured |
| a projection joining several of the above | composite | no, until measured |
| a session and its user | query | no |

Select only what the callers read. A field that changes often and that no
caller reads (a scheduler's bookkeeping, a counter) stays out of the
selection, so writes to it do not invalidate the reader. A caller that needs a
field the reader does not select either widens the selection or keeps its own
read.

Never in readers: documents that grow without bound or are written on every
request (scans, events, counters, queues, workflow state), whole documents
read once, and anything secret: key material, tokens, credentials.

## 12. Observability and control

A cache nobody can see is a cache nobody trusts. Readers report through the
telemetry mongodbee already has (OpenTelemetry, opt-in with the same
`telemetry` options as collections), and give the operator levers that need
no deployment.

### 12.1 See

Built: one `INTERNAL` span per reader call, `reader <name>`, with its kind,
level, outcome, `many()` key count and discarded flag (`doc/TELEMETRY.md`),
enabled by `registerReaders(target, { telemetry })`. mongodbee emits traces
only, so the metrics below come from the collector's span metrics connector,
with the reader attributes as dimensions (`doc/grafana`); a composite's span
parents the spans of the readers it calls. `requestReaderStats()` gives the
request's counters, `primed` included. What follows is the target.

- **Metrics**, per reader and level:
  - calls by outcome: `hit`, `miss`, `bypass` with its reason (strict request,
    transaction, limit, feed not live, freshness floor, raw-write monitoring
    absent);
  - loads and load duration;
  - invalidations by source (write, commit, raw driver, feed, TTL) and width
    (one key, one scope, one type, collection);
  - discarded loads (generation race), dropped pending loads, entries, sampled
    bytes, evictions.
- **Feed metrics**: live, lag, restarts, continuity losses, events per second.
- **Spans**: a reader call inside a traced request records `mongodbee.reader`,
  `mongodbee.reader.level` and `mongodbee.reader.outcome`.
- **Logs**, paired with a counter, for every anomaly: a continuity loss, a
  limit reached, a drift (12.3). Values are never logged, only the reader, the
  key hash and the reason.
- **Dashboard and alerts**: the Grafana dashboard in `doc/grafana` gains a
  readers row and alert rules: drift above zero, feed not live, lag above its
  limit, continuity loss, a sudden hit ratio drop.

### 12.2 Control

- **Switches without deployment**, through mongodbee's runtime configuration:
  all readers, one reader, or L1 only, each falling back to plain loads
  instantly.
- **Strict requests**: `withRequestContext(fn, { fresh: true })`.
- **Rollout per reader**: L1 is enabled one reader at a time.

### 12.3 Prove

- **Shadow mode**: before a reader serves from L1, it always loads, compares
  with what L1 would have returned, and counts mismatches. It serves only once
  the drift is zero over a representative period.
- **Continuous verification**: once serving, a sampled share of hits is
  reloaded in the background and compared; a mismatch is counted, logged, and
  drops the entry.
- **Explain**: in development, a request can list the readers it used, with
  level, outcome and key hash.

### 12.4 Tooling

- `@diister/mongodbee/inspect` exposes the declarations: name, kind, arguments,
  selection, edges, consistency, and the registration's limits and L1 settings.
- The studio shows the declarations, a map of which types feed which readers,
  "what does a write to this type and field invalidate", and the database side
  of the feed (oplog window, the indexes the readers' `by` fields need).
- Live hit rates and lag belong in Grafana, not in the studio.

## 13. Tests that must fail without the mechanism

Steps 1 and 2, each a test in `test/readers*.test.ts`:

- a write through a collection invalidates the reader without any `forget`
  call, including `updateMany`, `deleteMany` and `dropScope`;
- a status flip into the `where` by id, with a patch holding no `by` field,
  is seen by the next call;
- with computed fields registered, and with driver writes monitored, a write
  touching no selected, `where` or `by` path keeps every entry;
- a reader selecting `_computed` follows the recomputation of its subject;
- a load started before a write is not joined by a call after it;
- a load that raced a write is returned but not stored, and a write to another
  type or scope of the same collection does not discard it;
- a raw driver write invalidates when writes are monitored;
- a read-only transaction invalidates nothing; a rolled back one leaves
  nothing stale; a committed one invalidates before `afterCommit` runs, even
  over a read that raced it;
- a write in a nested request context reaches the enclosing one;
- readers over `+entreprises` (no computed field) and over a plain
  `collection()` are invalidated by their writes;
- values are frozen and shared; a composite's value is a frozen copy through
  maps, dates, cycles and frozen parents, and BSON values survive;
- `.many` maps each key, a later single call hits, and at the entry limit the
  keys already held are served without a query;
- the scope argument goes through the scope schema;
- two request contexts on two databases share no entry;
- a reader under `withReadPreference("secondaryPreferred")` reads the primary;
- past a limit, a reader loads and stores nothing;
- `primeFrom` fills a singleton and stores nothing off the primary, in a
  transaction, from a projection, or across a write;
- a composite that reads a collection directly throws; invalidating an inner
  reader invalidates the composite; a composite over `.many` follows its keys;
  a composite whose inner read bypassed, or ran in a nested request context,
  is not kept; a composite calling itself is refused;
- composite arguments are keyed exactly; unkeyable ones are refused;
- reader spans carry their outcome and no key value.

L1 (step 5 onwards): two processes, a write in one invalidates the other's L1
through the feed for an update by id with no `by` field; the `information`
type is routed although its ids are `exposition:` refIds; an event racing a
load is caught at store time; an idle feed stays live; a lost resume token
flushes L1; a freshness floor newer than an entry forces a reload and a floor
beyond the server's cluster time is clamped.

## 14. Build order

0. **Typing prerequisites** in the computed builder, which readers share: a
   distributive field proxy (so `personRef.userId` works on a `v.variant`),
   `where` values typed against their field, the `scoped(Model, refId)` handle,
   and template-typed `refId` outputs. The last one is a breaking typing change
   across Diivento and is planned as its own step.
1. **Query readers at L0**: `select`, `.one()`, `.by().one()`, `.many()`,
   derived arguments and values, ordering by `_id`, single flight with pending
   loads dropped on invalidation, BSON-safe freeze, exact keys with the
   database, primary reads, limits, coarse invalidation through the
   interceptor, footprints replayed at commit, raw writes, `primeFrom`, boot
   index check.
2. **Composite readers at L0**: edges between readers, direct reads refused.
   Diivento moves its preamble to readers, deletes every `forget` call and
   `request-scope.ts` (its `enterWith` fix is already covered by mongodbee's
   request context).
3. **Measure** the preamble on the performance scenario, with `roleKeys`
   computed and `.many` in place.
4. **The change feed**: typed, filtered, resumable, with state and
   subscribers; its oplog cost measured.
5. **L1 for eventual readers**, one at a time, with the routing of section
   5.2: shadow mode, then serving, then continuous verification. Exposition
   information first.
6. **Freshness floor**, `afterClusterTime` for secondary reads.
7. **Strict readers in L1**, only if step 3 says it pays (section 6.1).
8. Automatic batching; studio views and the inspect contract.

## 15. Open questions

- Should the request memo stay once the hot paths are readers? Proposal: yes,
  as the zero-configuration baseline for undeclared GET reads.
- `select` or `project`: mongodbee says "project" elsewhere (`findProject`).
- Can L1 query readers keep their `where` in the load (section 5.2)?
- The strict L1 gate (section 6.1), decided after measurement.

## 16. What the reviews changed

### Revision 2 (review of revision 1)

| Revision 1 | Problem found | Revision 2 |
|---|---|---|
| hand-written `key` and `keyOf` | a one-character mismatch compiles and never invalidates | keys built from the arguments |
| `fields` "whose change matters" | only safe when the value exposes nothing else | `select` required; the touched-path check covers `select`, `where` and `by` |
| `dependsOn` repeated for nested readers | forgotten on the first rewrite | reader-in-reader recorded as an edge |
| `expositions.type("participant")` | collections are opened asynchronously, per database | `from(Model, type)` through the computed topology |
| values copied through BSON | a `Map` becomes an object, a `Set` becomes `{}` | frozen, shared |
| `prime(key, value)` | races writes, leaks transaction snapshots and secondary reads | `primeFrom(read)` |
| `process` in the declaration | L1 is a rollout decision | `consistency` in the declaration, L1 at registration |
| feed routing by `_type`, `_scope` and `keyOf` fields | update and delete events carry none of them | routing by `_id` and `updatedFields` |
| L1 loads under the ambient read preference | a secondary refill stays stale until the TTL | readers read the primary |
| keys ignored the database | e2e scopes would share entries | the database name is part of every key |
| raw writes bump on the command | a racing load stores the old value | bump on `commandSucceeded` |
| strict readers in L1 on the feed | a lag window where a revoked grant authorises | strict readers L0 only until measured |

### Revision 3 (review of revision 2)

| Revision 2 | Problem found | Revision 3 |
|---|---|---|
| the flagship example | `personRef.userId` fails on a `v.variant`; a `where` typo compiles; refIds are plain strings; a model does not know its scope | typing prerequisites as step 0 |
| composites with recorded filters routed by returned ids | a `pending` membership accepted by id was returned by no entry: affiliations stay empty | composites are functions over readers only |
| single flight | a caller after a write joins a load started before it | invalidation drops pending loads |
| load by key, `where` in memory, reverse index at L0 | defeats Diivento's partial index; machinery only L1 needs | L0 loads with its `where` and invalidates coarsely; routing moves to L1 |
| type from the refId prefix | `information._id` is an `exposition:` refId: the first L1 candidate never invalidated | prefix map from the declared `_id` schemas, checked at boot |
| deep freeze | throws on `ObjectId`; `Readonly<Pick>` is shallow | freeze stops at BSON values; `DeepReadonly` |
| generations per `(collection, type, scope)` | unscoped and raw writes have no single scope | hierarchical generations |
| no L0 bound | a request iterating many keys grows without limit | per-request entry and per-entry row limits |
| `consistency` defaults to eventual | a permission reader that forgets the option becomes L1-eligible | defaults to strict |
| `activeOrgMembershipsOf` replaced by `organizationIds` | the grants need `roleKey` and `_id` per membership | an org memberships reader |
| no batching before step 8 | composites would multiply queries | `.many` in step 1 |
| interceptor reuse | it passes through collections without computed fields | readers get their own plan |
| feed filtered by id prefix, presented as cheap | not indexed: every stream scans the whole oplog | measured before L1 |

### Implementation (two reviews of the built code)

One review attacked correctness with failing probe tests, the other ported
Diivento's real preamble against its real schemas. Each row has a test in
`test/readers.test.ts` or `test/readers-types.test.ts`.

| Built first | Problem found | Now |
|---|---|---|
| a composite whose body throws synchronously | the entry kept its placeholder: every later caller got `undefined` | the load is started under a guard; every caller gets the error |
| the scope argument used as given | the scope schema transforms writes (`toLowerCase`), so the reader missed the documents | the scope goes through its schema, for the filter, the key and the footprint |
| keys as EJSON of the arguments | a `Set`, a function or a class instance encode as `{}` or `null`; `undefined` equals `null` | exact encoding, `ReaderArgument` at the type level, `ReaderArgumentError` at run time |
| keys by reader name | two readers named alike read each other's entries | keys by reader identity |
| a reader left out of the registration still worked | its placement and index were never checked | an unlisted query reader is refused |
| any commit, and a computed field's own transaction, flushed the whole request | footprints were bypassed on every write to a collection with computed fields | the commit replays the transaction's footprints; read-only transactions flush nothing |
| raw reads inside a composite | cached with no invalidation | `readingCollection` reads are refused there |
| in-place deep freeze | stopped at a frozen parent, froze the caller's objects, overflowed on cycles, left dates mutable | copy-freeze for composites, cycles kept, dates and collections locked |
| dependents kept on every inner entry | bypassed and invalidated composites piled up in long contexts | links are removed with the entry; bypassed loads are never linked |
| a nested request context | its writes did not reach the enclosing request | invalidation walks the enclosing contexts |
| registrations tracked by `Db` object | `unregisterReaders(client.db(name))` removed nothing; the fallback then refused | tracked by client and name |
| a composite calling itself | it joined its own placeholder and got `undefined` | refused with a definition error |
| the database fallback applied inside request contexts | a context without a database silently read the boot database | a registered resolver, and no guess inside a request context |
| `select`, `one` and `many` on every builder and reader | compiled after `through()`, and on readers without `by()` | typed away where they cannot work |
| a second `where` on one path | silently replaced the first | refused |

### Overall review of the built code

| Built first | Problem found | Now |
|---|---|---|
| composites linked to any inner entry | an inner reader in a nested `withRequestContext` lives in a cache nobody invalidates: the composite stayed stale | a dependency from another cache makes the composite load and not store |
| `.many()` checked the limit against every key asked | at the limit, keys all cached were queried again and stored nothing | only the missing keys count |
| a self-call check that needed a cache entry | outside a request context a composite calling itself recursed forever | checked by call key on the composite frames |
| an entry published with a placeholder promise | the shape that had produced the "`undefined` for later callers" bug | the entry holds its final promise from the start |
| one 1000-line `readers.ts`, registry plumbing copied from computed fields | hard to review | `reader-freeze`, `reader-key`, `reader-load`, `reader-errors` modules and a shared `ClientRegistry` |
| spec claims on rollbacks, unscoped writes, `$exists` and priming under the memo | wrong or unbacked | rewritten, each backed by a test |
| a public `consistency` option | stored and never read: nothing differs at L0 | removed until L1 gives it a meaning |
| `DeepReadonly<unknown>` | produced `{}`, so a `Record<string, unknown>` field became unusable in Diivento | `unknown` stays `unknown` |
| two clients registering the same database resolver | counted twice, so the resolver was ambiguous and nothing resolved | distinct resolvers are counted |

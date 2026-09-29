# Readers: declared reads, cached at the right level, invalidated by the database

Status: proposal, revision 2, 2026-09-29. Nothing below is implemented yet.
Revision 2 follows an adversarial review that rewrote five real Diivento
loaders against revision 1; section 16 lists what it changed and why.

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
   direction.** Keys, the fields that matter, the documents an entry depends
   on and the dependencies between readers are derived or recorded, never
   declared as free text. This rules out revision 1's `keyOf` and `fields`.
2. **A reader is a computed field that is not stored.** It reuses the
   `from(...).by(...).where(...)` builder, the topology and the write
   interceptor of computed fields (`doc/COMPUTED.md`), so an application
   learns one vocabulary and mongodbee maintains one choke point.
3. **Correct by default under concurrency, retries, transactions, replication
   and several processes.** When unsure, read the primary: fail open to
   MongoDB, never to a cached "allow".
4. **Caching is a deployment decision; consistency is a property of the
   data.** The declaration says what the data needs (`consistency`); which
   readers use L1, with which TTL, is decided at registration and can change
   without touching the declaration.

Non-goals: a shared cache between processes (Valkey, L2); caching decisions
(readers cache inputs such as memberships and roles, never `can()` results);
putting personal data anywhere but process memory.

## 3. Two kinds of reader

### 3.1 Query readers (declarative)

Most facts are "the documents of one type, in one scope, whose field equals
the argument". They are declared with the computed-field builder and `select`:

```ts
import { from, reader } from "@diister/mongodbee/readers";

export const participationsOf = reader(
  "participations-of-user",
  from(ExpositionModel, "participant")
    .by((p) => p.personRef.userId)
    .where((p) => [p.personRef.kind, "user"])
    .select(["status", "personRef"]),
  { consistency: "strict" },
);

export const entrepriseMemberships = reader(
  "entreprise-memberships-of-user",
  from(EntreprisesModel, "member")
    .by((m) => m.userId)
    .where((m) => [m.status, ["active", "invited"]])
    .select(["tenantId", "role", "status"]),
  { consistency: "strict" },
);

export const expositionInformation = reader(
  "exposition-information",
  from(ExpositionModel, "information").one().select(["name", "entreprise", "modules"]),
);

const rows = await participationsOf(expositionId, userId);
const members = await entrepriseMemberships(userId);
```

- The arguments are derived: the scope first when the model is scoped, then
  the `by` value, typed from the schema. With refId template types, swapping
  `(expositionId, userId)` does not compile.
- The value is derived: `readonly Readonly<Pick<Doc, "_id" | selected>>[]`, or
  one document or `null` with `.one()`. A field that is not selected is not in
  the type and not in memory: `invitationToken` cannot reach a cache.
- `select` is required. A reader over full documents is what made revision 1's
  field narrowing unsafe, and what puts tokens in memory.
- `where` takes the same equality and membership predicates as computed
  fields: serializable, indexable, checkable at boot.

What makes this kind exact is **how it loads**: by scope and `by` value only,
with the `where` applied in memory, projected on the selected fields plus the
`where` paths. The entry therefore holds every document of its key, matching
or not, and keeps a reverse index `documentId -> entry`. Every change can then
be routed without reading anything (section 5).

### 3.2 Composite readers (imperative)

Some facts combine several types and other readers (Diivento's affiliations:
org memberships, reachable organisations, their roles, entreprise
memberships). They keep a free-form `load`:

```ts
export const affiliationsOf = reader(
  "exposition-affiliations",
  {
    reads: [
      from(ExpositionModel, "org_membership"),
      from(ExpositionModel, "expo_organization"),
      from(ExpositionModel, "expo_organization_role"),
      entrepriseMemberships,
    ],
    consistency: "strict",
  },
  async (expositionId: ExpositionId, userId: UserId, participantId: ParticipantId | null) => {
    const expo = await expositionScope(expositionId);
    const entreprises = await entrepriseMemberships(userId);
    const memberships = participantId
      ? await expo.find("org_membership", { participantId, status: "active" })
      : [];
    return { memberships, entrepriseIds: entreprises.map((m) => m.tenantId) };
  },
);
```

- `reads` names what `load` may read, at type granularity: other readers, or
  `from(model, type)`. It feeds the studio, the inspect contract and the
  change-feed filter. It is **checked, not trusted**: every mongodbee read
  already goes through one path (`readThrough`), so during `load` the reader
  records each read: collection, type, scope, the equality paths of its filter,
  its projection and the ids it returned. A read of a type missing from
  `reads` throws in test mode, and in production is served but never stored,
  with a counter and a log.
- A reader called inside `load` becomes a recorded edge: invalidating the inner
  entry invalidates the outer one. The author never repeats the inner reader's
  types.
- A raw driver read inside `load` (through `readingCollection`) is recorded at
  collection level; a raw aggregation pipeline too. `verify: "throw"` refuses
  both, so a composite that needs one says so at registration.
- Nothing of the old `dependsOn`/`keyOf`/`fields` remains: the recorded ids and
  filter paths are what section 5 routes on.

### 3.3 Values

A value is **plain data, deep-frozen and shared**: no copy on a hit. The type
of a composite's value is constrained to plain data (records, arrays,
primitives, `Date`, `ObjectId`); a `Map` or a `Set` does not compile. Revision
1 copied values through BSON, which silently turns a `Map` into a plain object
and a `Set` into `{}`, and would have spent on every L1 hit the decoding that
L1 exists to save.

### 3.4 Keys

The library builds the key from the argument tuple itself, exactly, plus the
database name (two e2e scopes in one process never share an entry). Hashing
only happens where the key leaves the process memory: metrics, logs, the
explain header, with a keyed hash (a per-process secret), never the 32-bit
`fnv1a` of the current code, whose collisions in an L1 would serve one person
another's participation.

### 3.5 Priming

A list read that already holds the documents can fill a query reader:

```ts
const docs = await expositionInformation.primeFrom(() =>
  catalog.unscoped.find("information", { entreprise }),
);
```

`primeFrom` runs the read itself, so it can capture the generation before it,
project the documents on the reader's selection, and refuse to store when the
read ran in a transaction, off the primary, or raced a write. Revision 1's
`prime(key, value)` could do none of this: a `Promise.all` running the list
read next to a write would re-store the pre-write document after the write had
invalidated it.

### 3.6 Registration

```ts
registerReaders(client, {
  topology: computedTopology(schemas),
  readers: [participationsOf, entrepriseMemberships, expositionInformation, affiliationsOf],
  verify: isTest ? "throw" : "log",
  process: {
    enabled: flags.readersL1,
    readers: { "exposition-information": { ttlMs: 300_000 } },
    maxEntries: 50_000,
  },
});
```

- `topology` is the one computed fields use: it places `from(ExpositionModel,
  "participant")` in its physical collection without the application holding a
  collection object at module load (Diivento opens its collections
  asynchronously, per database).
- The database a reader reads is the one of the ambient request context, which
  is how the e2e scope header already selects it. Tests inject a database the
  same way, instead of passing a `getMultiCollection` into each loader.
- Names are unique per client; a duplicate throws at registration.
- `process` is the L1 opt-in, per reader, with its TTL. Section 6.

## 4. Level 0: the request

A reader called inside `withRequestContext` caches per key for that request:

- **single flight**: concurrent calls with the same key share one load;
- **shared values** (section 3.3);
- **transactions**: inside a transaction a reader neither reads nor stores any
  cache, it loads through the session;
- **primary**: a reader always reads the primary, whatever the ambient
  `withReadPreference`, because its value may authorise;
- outside any request context, a reader simply loads;
- reader loads go around the request memo's storage, so nothing is held twice.

Readers are active for every HTTP method, because their invalidation is
precise. A request that writes through the raw driver must have registered
`invalidateReadsOnDriverWrites`; without it, readers on non-GET requests load
every time and count a bypass, instead of guessing.

## 5. Invalidation

A write invalidates entries; the routing is derived, so no repository calls a
`forget`.

### 5.1 In the writing process

The computed-field interceptor already sees every write through mongodbee,
with the collection, the type, the scope, the ids or the filter, and the
update. Readers reuse it:

| Write | Query reader | Composite reader |
|---|---|---|
| insert | the key of the inserted `by` value | entries whose recorded filter paths match the document |
| update or delete by id | the entry holding that id (reverse index) | entries that returned that id |
| update setting the `by` field | also the key of the new value | as above |
| update or delete by filter | entries of that type in that scope | same |
| an update touching no selected, `where` or `by` path | nothing | nothing when it touches no recorded path |

Invalidation happens twice for a transaction: at the write, and again after
the commit and before any `afterCommit` callback runs, so a load that raced the
transaction is dropped and a callback never reads the old value. Computed
maintenance's internal transactions count as commits.

**Generations** close the race between a load and a write: every
`(collection, type, scope)` has a counter, bumped by each invalidation. A load
captures it before reading; if it moved when the load returns, the value is
given to the caller and not stored. The granularity matters: a per-collection
counter would make any lead or programme write discard every permission load
of `+expositions` during a show.

**Raw driver writes** (`invalidateReadsOnDriverWrites`, command monitoring) bump
on `commandSucceeded`, never on `commandStarted`: a load that starts after the
start and ends before the write lands would store the old value under the new
generation. The command's filter is parsed for `_id`, `_type` and `_scope`
(Diivento's raw writes carry them), which keeps raw-write invalidation as
precise as the table; a filter without them invalidates the collection.
`commitTransaction` carries no namespace, so the writes of a raw transaction
are tracked by session until its commit.

### 5.2 In the other processes: the change feed

A change event of an update or a delete carries `documentKey: { _id }` and the
updated field names and values, but not `_type`, `_scope` or any unchanged
field. Revision 1 assumed otherwise. The routing therefore relies on what the
event does carry:

- **insert** carries the full document: routed like an in-process insert;
- **update and delete** are routed by `_id` through the reverse indexes; an id
  held by no entry needs nothing, because a query reader holds every document
  of its keys, matching its `where` or not;
- **an update that changes a `by` field** carries the new value in
  `updatedFields`: the matching keys are invalidated, in every scope, since the
  event has no scope;
- the type is read from the refId prefix of `_id`, which lets the change stream
  filter by type on the server; a collection without typed ids is routed at
  collection level;
- `drop`, `rename`, `dropDatabase` and `invalidate` flush every reader of the
  collection.

No `updateLookup` and no pre-images: neither costs anything per event.

## 6. Level 1: the process

With `process` enabled for it, a reader's entries also live in a per-process
store:

- **bounded** by entry count, with sampled sizes for the byte metric; a TTL per
  reader, jittered, as a safety net, never as the invalidation;
- **invalidated by the change feed** (section 5.2) and by the local writes
  (section 5.1);
- **no negative entries** by default: a `null` or an empty list stays in L0,
  so probes with random ids cannot fill memory;
- **stamped with a cluster time**: the `operationTime` of the load, which the
  freshness floor compares against;
- **fail open**: when the feed is not live, `strict` readers bypass L1 and
  eventual ones serve until their TTL; the feed is live when its
  `postBatchResumeToken` keeps advancing, so an idle healthy stream is not
  mistaken for a late one;
- **lost continuity flushes**: a stream that cannot resume
  (`ChangeStreamHistoryLost`) drops every entry and bumps every generation;
- **freshness floor**: a request may carry a minimum cluster time, for example
  from a real-time signal or the user's own last write. An entry stamped
  earlier is reloaded. The floor comes from the client, so the application
  signs it, and mongodbee clamps it to the server's cluster time: a forged
  far-future floor cannot turn every request into a bypass.

### 6.1 Strict readers in L1

The feed has lag. For an eventual reader (exposition information, module
configuration) a lag of tens of milliseconds is invisible. For a strict one
(memberships, roles) it is a window where a revoked grant still authorises.
Two gates can close it, and `doc/COMPUTED.md` section 13 already describes the
first:

1. **dependency versions**: every write of a key bumps a version in the same
   transaction; a strict L1 hit reads the versions of its keys in one round
   trip. Exact, sees nothing raw, costs a write per write and a read per
   request;
2. **an authorisation epoch** on the user, read with the session the request
   loads anyway, bumped by every revocation: exact for revocations, free per
   request, but relies on every revoking path bumping it.

Decision for now: **strict readers are L0 only.** After L0 and the computed
fields (`organizationIds` is already computed, `roleKeys` can be), what remains
of the preamble is measured; strict L1 is built only if that measurement says
it pays, with the gate chosen then. Section 13 of `COMPUTED.md` is not built
separately: if it is built, it is this gate.

## 7. The change feed

`watchChanges(client, spec)` opens one change stream per process:

- filtered on the server by collection and by refId prefix of the types that
  readers or subscribers declared; projected on `_id`, the operation, the
  updated field names, the `by` values of query readers and the cluster time;
- majority-committed only, so nothing announced can roll back;
- resumable from its last token, reporting a continuity loss when it cannot;
- with a state: live (progress of the resume token), lag, restarts;
- **public**: `feed.subscribe(filter, handler)` runs after the commit, on every
  process, for changes made by the worker, another cluster, a migration or the
  raw driver, which an in-memory event bus cannot see.

Volume: every process receives the changes of every declared type. For
Diivento that means the permission types of `+expositions`, not leads or
scans, since the server filters by id prefix.

## 8. Real time

Diivento's real-time foundation sends signals ("this changed, refetch") over
SSE. With readers and the feed:

1. the feed invalidates L1 on every process;
2. the same event, through a subscriber, becomes a signal carrying the
   change's cluster time;
3. the client refetches with that cluster time as its freshness floor;
4. an SSE connection never holds one request context for its lifetime: each
   signal or permission check opens its own.

Revocation: when a strict reader's entry is invalidated, the application's
real-time hub is told through a subscriber and re-checks that user's open
subscriptions.

## 9. Batching (later)

Calls of one query reader for different keys in the same tick can coalesce
into one `$in` read, as DataLoader does. The declaration already holds what
this needs (the `by` field), so it costs the author nothing. It comes after L1.

## 10. Read preference and causal reads

Readers read the primary (section 4). Other reads that go to secondaries under
`withReadPreference` honour the freshness floor with
`readConcern: { level: "majority", afterClusterTime }`: a lagging secondary
waits instead of answering stale.

## 11. What Diivento puts in readers

| Reader | Kind | Consistency | L1 |
|---|---|---|---|
| exposition information (name, entreprise, modules, brand) | query `.one()` | eventual | yes, first candidate |
| platform role permissions | query | strict | after measurement |
| entreprise memberships of a user | query | strict | after measurement |
| participations of a user in an exposition | query | strict | after measurement |
| team roles of a user | query | strict | after measurement |
| affiliations | composite | strict | after measurement |
| session and session user | query | strict | no |

`activeOrgMembershipsOf` disappears: `participant._computed.organizationIds`
(merged in #388) is the same fact. The lifecycle fields of the information
document (`lifecycle.executions`, `lifecycle.history`) are not selected, so the
scheduler's writes to them do not invalidate the exposition information.

Never in readers: scans, full participant documents, registrations and seat
counts, leads, flow sessions, jobs, mails, secrets, key material, tokens.

## 12. Observability and control

A cache nobody can see is a cache nobody trusts. Readers report through the
telemetry mongodbee already has (OpenTelemetry, opt-in with the same
`telemetry` options as collections), and give the operator levers that need
no deployment.

### 12.1 See

- **Metrics**, per reader and level:
  - calls by outcome: `hit`, `miss`, `bypass` with its reason (strict request,
    feed not live, freshness floor, transaction, undeclared read, no raw-write
    monitoring);
  - loads and load duration;
  - invalidations by source (write, commit, raw driver, feed, TTL) and width
    (one key, one scope, whole reader);
  - discarded loads (generation race), entries, sampled bytes, evictions.
- **Feed metrics**: live, lag, restarts, continuity losses, events per second.
- **Spans**: a reader call inside a traced request records `mongodbee.reader`,
  `mongodbee.reader.level` and `mongodbee.reader.outcome`, so a slow request
  shows which facts came from memory and which went to the database.
- **Logs**, paired with a counter, for every anomaly: a continuity loss, a
  strict reader bypassing because the feed is late, an undeclared read, a drift
  (12.3). Values are never logged, only the reader, the key hash and the
  reason.
- **Dashboard and alerts**: the Grafana dashboard in `doc/grafana` gains a
  readers row and alert rules: drift above zero, feed not live, lag above its
  limit, continuity loss, a sudden hit ratio drop.

### 12.2 Control

- **Switches without deployment**, through mongodbee's runtime configuration:
  all readers, one reader, or L1 only, each falling back to plain loads
  instantly.
- **Strict requests**: `withRequestContext(fn, { fresh: true })` for the routes
  that must read at the source.
- **Rollout per reader**: L1 is enabled one reader at a time.

### 12.3 Prove

- **Shadow mode**: before a reader serves from L1, it always loads, compares
  with what L1 would have returned, and counts mismatches. It serves only once
  the drift is zero over a representative period.
- **Continuous verification**: once serving, a sampled share of hits (for
  example 1%) is reloaded in the background and compared; a mismatch is
  counted, logged, and drops the entry. The cache's `checkComputed`.
- **Recorded reads in tests** (section 3.2): an undeclared read throws.
- **Explain**: in development, a request can list the readers it used, with
  level, outcome and key hash, for example in a response header.

### 12.4 Tooling

- `@diister/mongodbee/inspect` exposes the declarations: name, kind, arguments,
  selection, reads, consistency, and the registration's L1 settings.
- The studio, which runs in its own process and never sees the application's
  memory, shows the declarations, a map of which types feed which readers,
  "what does a write to this type and field invalidate", and the database side
  of the feed (oplog window, the indexes the query readers' `by` fields need).
- Live hit rates and lag belong in Grafana, not in the studio.

## 13. Tests that must fail without the mechanism

- a write through a service invalidates the reader without any `forget` call;
- an update by id whose patch holds no `by` field invalidates the entry
  holding that id;
- an update setting the `by` field invalidates both the old and the new key;
- a status flip into or out of the `where` invalidates its key;
- an update touching an unselected field (`lifecycle.history`) does not;
- a composite that reads an undeclared type throws in test mode;
- invalidating an inner reader invalidates the composite that called it;
- a `Map` in a composite's value does not compile;
- a raw driver write invalidates on success, and a load racing it is not
  stored;
- a load that raced a write is returned but not stored (generation), and a
  write to another type of the same collection does not discard it;
- a rolled back transaction leaves the cache untouched, a committed one
  invalidates before `afterCommit` runs;
- `primeFrom` inside a transaction or under `secondaryPreferred` stores
  nothing;
- two request contexts on two databases in one process share no entry;
- a reader under `withReadPreference("secondaryPreferred")` reads the primary;
- two processes: a write in one invalidates the other's L1 through the feed,
  for an update by id that carries no `by` field;
- an idle feed stays live; a lost resume token flushes L1;
- a strict reader bypasses L1 while the feed is not live;
- a freshness floor newer than an entry forces a reload, and a floor beyond
  the server's cluster time is clamped.

## 14. Build order

1. **Query readers at L0**: builder, derived arguments and values, `select`,
   single flight, frozen shared values, exact keys with the database, primary
   reads, `primeFrom`, in-process invalidation through the computed
   interceptor, generations per `(collection, type, scope)`, raw-write
   invalidation on success. Diivento moves exposition information,
   memberships, participations and team roles to readers and deletes their
   `forget` calls.
2. **Composite readers at L0**: recorded reads, `verify`, reader edges.
   Diivento moves affiliations and deletes `request-scope.ts` (its `enterWith`
   fix stays in the request context of mongodbee).
3. **Measure** the preamble on the performance scenario.
4. **The change feed**: typed, filtered by id prefix, resumable, with state and
   subscribers.
5. **L1 for eventual readers**, one at a time: shadow mode, then serving, then
   continuous verification. Exposition information first.
6. **Freshness floor**, `afterClusterTime` for secondary reads.
7. **Strict readers in L1**, only if step 3 says it pays (section 6.1).
8. Batching; studio views and the inspect contract.

## 15. Open questions

- Should the request memo stay once the hot paths are readers? Proposal: yes,
  as the zero-configuration baseline for undeclared GET reads; readers go
  around its storage.
- The strict L1 gate (section 6.1), decided after measurement.
- Where the feed runs when the worker and the API scale separately: one stream
  per process, about one connection each.

## 16. What revision 2 changed

| Revision 1 | Problem found | Revision 2 |
|---|---|---|
| hand-written `key` and `keyOf` | a one-character mismatch compiles and never invalidates; no `keyOf` exists for types that do not carry the key | keys built from the arguments; routing by `by` value and by id |
| `fields` "whose change matters" | only safe when the value exposes nothing else; the real loaders returned full documents | `select` required; the touched-path check covers `select`, `where` and `by` |
| `dependsOn` repeated for nested readers | forgotten on the first rewrite | reader-in-reader recorded as an edge |
| `dependsOn` trusted | nothing checked it | `reads` checked against recorded reads |
| `expositions.type("participant")` | collections are opened asynchronously, per database | `from(Model, type)` through the computed topology |
| values copied through BSON | a `Map` becomes an object, a `Set` becomes `{}`; each L1 hit re-decodes | frozen, shared, plain data enforced by type |
| `prime(key, value)` | races writes, leaks transaction snapshots and secondary reads | `primeFrom(read)` |
| `process` in the declaration | L1 is a rollout decision | `consistency` in the declaration, L1 at registration |
| feed routing by `_type`, `_scope` and `keyOf` fields | update and delete events carry none of them | reverse index by `_id`, `by` values from `updatedFields`, type from the refId prefix |
| per-namespace generation, undefined | per collection would thrash during a show | per `(collection, type, scope)` |
| raw writes bump on the command | a racing load stores the old value under the new generation | bump on `commandSucceeded` |
| L1 loads under the ambient read preference | a secondary refill after the invalidation stays stale until the TTL | readers read the primary |
| keys ignored the database | e2e scopes in one process would share entries | the database name is part of every key |
| freshness floor against generations | no time to compare; forgeable | entries stamped with `operationTime`; floor signed and clamped |
| feed lag from the last event | an idle feed looks late forever | liveness from the resume token's progress |
| strict readers in L1 on the feed | a lag window where a revoked grant authorises | strict readers L0 only until measured; gate chosen then, merging `COMPUTED.md` section 13 |

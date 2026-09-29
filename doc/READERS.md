# Readers: declared reads, cached at the right level, invalidated by the database

Status: proposal, 2026-09-29. Nothing below is implemented yet.

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

Each has what the other lacks. A business key dedupes reads of different
shapes that fetch the same fact, and priming lets a list read feed the by-id
reads that follow; automatic invalidation removes the class of bugs where a
write path forgets to call `forget`, which for permission facts means a stale
grant until the end of the request today, and until a TTL once there is an L1.

A **reader** is the one primitive that gives both, and scales from L0 to L1.

## 2. Goals and non-goals

Goals:

1. One declaration per fact: a name, a key, what it reads, how to load it.
2. Invalidation is derived from the declaration, never written by hand.
3. The same declaration works per request (L0) and per process (L1).
4. Correct by default under concurrency, retries, transactions and several
   processes; when unsure, read the database (fail open to MongoDB, never to
   a cached "allow").
5. The change feed that invalidates L1 is a public, typed, resumable API, so
   an application can derive real-time signals from it.

Non-goals:

- A shared cache between processes (Valkey, L2). The declaration leaves room
  for it; this document does not build it.
- Caching decisions. Readers cache inputs (who is a participant, which roles
  exist), never `can()` results.
- Caching documents that are personal data in anything but process memory.

## 3. Declaring a reader

```ts
import { defineReader } from "@diister/mongodbee/readers";

export const participationsOfUser = defineReader({
  name: "participations-of-user",
  key: (userId: string) => userId,
  dependsOn: [
    {
      on: expositions.type("participant"),
      keyOf: (doc) =>
        doc.personRef?.kind === "user" ? doc.personRef.userId : undefined,
      fields: ["personRef", "status"],
    },
  ],
  load: (userId) =>
    expositions.unscoped.findProject("participant", ["status"], {
      "personRef.kind": "user",
      "personRef.userId": userId,
    }),
  process: { ttlMs: 90_000, critical: true },
});

const rows = await participationsOfUser(userId);
participationsOfUser.prime(userId, rows);
```

- `name` is unique per client; it names metrics, logs and the studio view.
- `key` maps the arguments to a string. Arguments that are personal data are
  hashed in the key (the reader never stores them in clear).
- `dependsOn` lists every type the loader reads. Each entry may give:
  - `keyOf(doc)`: the reader keys a changed document affects. With it, a
    change invalidates only those keys; without it, the whole reader.
  - `fields`: the fields whose change matters; a change touching none of
    them is ignored (an update of `lastSeenAt` does not invalidate roles).
- `load` does the reading. It may use any MongoDBee read, the raw driver
  through `readingCollection`, or other readers.
- `process` opts the reader into L1. Without it the reader is L0 only.
- `critical: true` marks a reader that feeds authorization: see section 6.

A declaration lives next to the repository that owns the data, and the client
refuses two readers with the same name.

## 4. Level 0: the request

A reader called inside `withRequestContext` (section 9 of the README) caches
per key for that request:

- **single flight**: concurrent calls with the same key share one load;
- **copies**: the first caller gets the loaded value, later callers a copy,
  as the request memo already does;
- **priming**: `prime(key, value)` fills an entry, for example from a list
  read that already holds the documents;
- **transactions**: inside a transaction a reader always loads, and never
  stores what it read there (the snapshot may still roll back);
- outside any request context, a reader simply loads.

Unlike the request memo, readers are active for every HTTP method, because
their invalidation is precise (next section).

## 5. Invalidation

A write invalidates the readers whose `dependsOn` matches it. The matching is
derived, so no repository ever calls a `forget`.

| Source | Knows | Invalidates |
|---|---|---|
| A MongoDBee write in this process | collection, type, the documents written (by id or by filter), the fields set | matching readers; by key when the documents are known and `keyOf` is declared, otherwise the whole reader |
| A committed transaction in this process | every write it made | again after the commit, so a load that raced the transaction is dropped |
| A raw driver write in this process (`invalidateReadsOnDriverWrites`) | collection only | every reader depending on that collection |
| The change feed (L1 only) | collection, type, document key, changed fields, cluster time | matching readers, by key when possible |

Races are closed with a **generation**: every reader entry records the
generation of its namespace before loading, and an entry whose namespace
changed while it was loading is returned to its caller but never stored.
This is the "capture the generation before the read" rule of both reports.

A delete carries no document. To invalidate by key anyway, a reader with
`keyOf` keeps a reverse index `documentId -> keys` for the entries it holds;
a delete of an id not in the index needs no invalidation.

## 6. Level 1: the process

With `process` set, entries also live in a per-process store:

- **bounded**: an LRU by size in bytes per process, with a TTL per reader
  (`ttlMs`, jittered by 10%) as a safety net, never as the invalidation;
- **fed by the change feed**: one change stream per process (section 7);
  every event invalidates the matching entries;
- **fail open**: if the feed is not live (not started, lagging beyond
  `maxLagMs`, or restarting), a `critical` reader bypasses L1 and reads the
  database, and a non-critical reader keeps serving only until its TTL;
- **lost continuity flushes**: if the feed cannot resume from its token
  (oplog window exceeded, `ChangeStreamHistoryLost`), every L1 entry is
  dropped and the generation of every namespace is bumped;
- **freshness floor**: a request may carry a minimum cluster time (for
  example from a real-time signal, or from a cookie set after the user's own
  write). An L1 entry older than that floor is ignored and reloaded;
- **strict mode**: a request context opened with `{ fresh: true }` uses L0
  but never L1; for administration, security settings and anything the
  application wants read at the source.

What is never put in L1 is the application's decision; section 11 lists what
Diivento's reports exclude.

## 7. The change feed

`watchChanges(db, spec)` opens one change stream for the whole process:

- it filters on the collections and types that declared readers or
  subscribers depend on, and projects only `_id`, `_type`, `_scope`, the
  document keys `keyOf` needs, the names of the updated fields and the
  cluster time: no field value leaves the database unless a `keyOf` asks
  for it;
- it resumes from its last token after a disconnection, and reports a
  continuity loss when it cannot (section 6);
- it exposes its state: `live`, lag (last event cluster time against the
  server), restarts;
- it is **public**: `feed.subscribe(filter, handler)` lets the application
  react to committed changes, for example to emit a real-time signal. A
  subscriber runs after the commit, on every process, including changes
  made by the worker, by another cluster, by a migration or by the raw
  driver, which is what an in-memory event bus cannot see.

Only majority-committed changes reach a change stream, so a signal never
announces a write that may roll back.

## 8. Real time

The real-time foundation of Diivento sends signals ("this changed, refetch")
over SSE. With readers and the feed:

1. the feed invalidates L1 on every process;
2. the same event, through a subscriber, becomes a signal carrying the
   change's cluster time;
3. the client refetches with that cluster time as its freshness floor, so a
   process whose feed is late ignores its L1 entry and reads the database,
   and a read on a secondary asks for `afterClusterTime` (section 10);
4. an SSE connection never holds one request context for its lifetime: each
   signal or permission check opens its own.

Revocation: readers marked `critical` hold authorization inputs. When one of
their keys is invalidated, the application's real-time hub is told through a
feed subscriber and re-checks the open subscriptions of that user, so a
revoked role stops receiving signals.

## 9. Batching (later)

`defineReader({ ..., loadMany: (keys) => Map })` lets calls for different
keys in the same tick coalesce into one `$in` read, as DataLoader does. It is
independent of caching and comes after L1.

## 10. Read preference and causal reads

`withReadPreference` (0.23.0-beta.34) sends analytics reads to secondaries.
A freshness floor from section 6 must also hold there: when a request carries
a minimum cluster time, a secondary read uses
`readConcern: { level: "majority", afterClusterTime }`, so a secondary that has
not replicated the write waits for it instead of answering stale.

## 11. What Diivento puts in readers

From the two reports:

| Reader | Level | Invalidated by |
|---|---|---|
| exposition information and module snapshot | L0 + L1 | `information`, `expo_*` catalogs of that exposition |
| session and session user | L0 (L1 later, strict) | `+sessions`, `+users` of that id |
| entreprise memberships of a user | L0 + L1, critical | `+entreprises` `member:` rows of that user |
| participations, role grants, affiliations of a user in an exposition | L0 + L1, critical | `participant`, `participant_role`, `org_membership`, `expo_organization*` by user or organization |
| platform role permissions | L0 + L1, critical | `+role_permissions` |
| team roles of a user | L0 + L1, critical | `user_role` of that user |

Never in readers: scans, full participant documents, registrations and seat
counts, leads, flow sessions, jobs, mails, secrets, key material, tokens.

## 12. Observability and control

A cache nobody can see is a cache nobody trusts. Readers report through the
telemetry mongodbee already has (OpenTelemetry, opt-in with the same
`telemetry` options as collections), and give the operator levers that need
no deployment.

### 12.1 See

- **Metrics**, per reader and level:
  - calls by outcome: `hit`, `miss`, `bypass` (with its reason: strict mode,
    feed not live, freshness floor, inside a transaction);
  - loads and load duration;
  - invalidations by source (write, commit, raw driver, feed, TTL) and by
    width (one key, whole reader);
  - discarded loads (the generation race), L1 entries, L1 bytes, evictions.
- **Feed metrics**: live, lag, restarts, continuity losses, events per second.
- **Spans**: a reader call inside a traced request records its name, level
  and outcome as span attributes (`mongodbee.reader`, `mongodbee.reader.level`,
  `mongodbee.reader.outcome`), so a slow request shows which facts came from
  memory and which went to the database.
- **Logs**, paired with a counter, for every anomaly: a continuity loss, a
  critical reader bypassing L1 because the feed is late, a drift found by
  verification (12.3). Values are never logged, only the reader, the key
  hash and the reason.
- **Dashboard and alerts**: the Grafana dashboard shipped in `doc/grafana`
  gains a readers row (hit ratio by reader, bypasses, invalidations, feed lag)
  and alert rules: drift above zero, feed not live, lag above `maxLagMs`,
  continuity loss, a sudden hit ratio drop.

### 12.2 Control

- **Switches without deployment**, through mongodbee's runtime configuration:
  all readers, one reader, or one level (L1 off keeps L0), each able to fall
  back to plain loads instantly.
- **Strict requests**: `withRequestContext(fn, { fresh: true })` for the
  routes that must read at the source.
- **Rollout per reader**: L1 is enabled one reader at a time, never globally
  first.

### 12.3 Prove

- **Shadow mode**: before a reader serves from L1, it runs in shadow for a
  while: it always loads from the database, compares with what L1 would have
  returned, and counts mismatches. A reader is switched to serving only when
  its shadow drift is zero over a representative period.
- **Continuous verification**: once serving, a sampled share of L1 hits
  (configurable, for example 1%) is reloaded in the background and compared.
  A mismatch is logged and counted, and the entry is dropped. This is the
  cache's equivalent of `checkComputed`.
- **Dependency check in tests**: in test mode, a reader records the types its
  `load` actually read and fails when one is missing from `dependsOn`. A too
  narrow declaration, the dangerous direction since it leaves stale data, is
  caught by the test suite rather than in production.
- **Explain**: in development, a request can ask for the list of readers it
  used, with level, outcome and key hash, for example through a response
  header the application chooses to expose.

### 12.4 Tooling

- `@diister/mongodbee/inspect` exposes the declarations (name, key shape,
  dependencies, level, TTL, critical) so tools can show them.
- In Diivento, the metrics come from mongodbee's instrumentation through the
  application's OpenTelemetry provider; the application adds its own
  dashboards and alerts on top and declares no duplicate instrument.

What the studio can show, since it runs in its own process and never sees
the application's memory:

- **declarations**: every reader, its level, and a map of which types feed
  which readers;
- **"what does this write invalidate"**: pick a type (and optionally a
  field), see the readers and keys a write there invalidates; this makes a
  missing or too wide `dependsOn` visible;
- **feed health, from the database side**: oplog window, whether the
  collections a reader depends on are covered by indexes its loader needs.

Live hit rates and lag belong in the application's metrics (Grafana), not
in the studio.

## 13. Tests that must fail without the mechanism

- a write through a service invalidates the reader without any `forget` call;
- a write that only touches unrelated fields does not;
- a raw driver write invalidates every reader of that collection;
- a load that raced a write is returned but not stored (generation);
- a rolled back transaction leaves the cache untouched, a committed one
  invalidates;
- a delete invalidates the keys of the deleted document through the reverse
  index;
- a feed that loses continuity flushes L1;
- a `critical` reader bypasses L1 while the feed is not live;
- a freshness floor newer than an entry forces a reload;
- two processes: a write in one invalidates the other's L1 through the feed
  (multi-process test harness, as for computed fields).

## 14. Build order

1. L0 readers: declaration, key, single flight, copies, priming, derived
   invalidation from MongoDBee writes, commits and raw driver writes. Diivento
   replaces `requestScoped` with readers and deletes every `forget` call.
2. The change feed: typed, resumable, projected, with state and subscribers.
3. L1 on top of the feed, behind a per-reader opt-in and a runtime switch,
   one reader at a time: shadow mode first, serving only once its drift is
   zero, then continuous verification (section 12.3).
4. Freshness floor and `afterClusterTime` for secondary reads.
5. Batching.
6. Studio views and the inspect contract.

## 15. Open questions

- Should the request memo stay once the hot paths are readers? It is a zero
  configuration baseline for undeclared reads; keeping both costs little.
- Authorization epochs: one report proposes an `authzEpoch` stored on the user
  (read with the session aggregate) for strict revocation independent of the
  feed's lag. It fits as an extra key component of `critical` readers.
- Where the feed runs when the worker and the API scale separately: one
  stream per process is the reports' choice (about one connection each).

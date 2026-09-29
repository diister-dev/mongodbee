# Readers: declared reads, cached at the right level, invalidated by the database

Status: proposal, revision 3, 2026-09-29. Nothing below is implemented yet.
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
   default (`consistency` defaults to `"strict"`).
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
  from(EntreprisesModel, "member")
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
  { consistency: "eventual" },
);

const rows = await participationsOf(expositionId, userId);
const roles = await orgRolesOf.many(expositionId, organizationIds);
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
  unsafe in revision 1, and what puts tokens in memory.
- `where` takes the computed fields' equality and membership predicates. Its
  values are typed against the field (`[p.status, "actve"]` does not compile)
  and validated against the schema at boot.
- **`.many(scope, keys)`** reads several keys in one `$in` read and stores each
  key separately, exact per key. It replaces today's `$in` reads.
- The load uses the full filter, `where` included, so partial indexes serve it
  (Diivento's `personRef.userId` index is partial on `kind: "user"`).
  Registration checks that an index covers `(scope, by)` under the `where`, as
  the computed topology checks its `by` fields.

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
    const viaEntreprise = await orgsOfEntreprise.many(expositionId, active);
    const reached = [...created, ...viaEntreprise];
    const orgIds = [...new Set([...memberships.map((m) => m.organizationId), ...reached.map((o) => o._id)])];
    return { memberships, reached, roles: await orgRolesOf.many(expositionId, orgIds) };
  },
);
```

- Every reader called inside the function is recorded as an edge; invalidating
  an inner entry invalidates the composite entries that used it.
- A direct collection read inside a composite throws. This is what makes a
  composite sound without recording filters: revision 2's routing of an update
  by id to "the entries that returned that id" missed a `pending` membership
  accepted by id, which no entry had returned.
- No `reads` list: the edges are recorded, and a list would repeat them.

### 3.3 Values

A value is **deep-frozen and shared**: no copy on a hit, typed `DeepReadonly`
so that `roles[0].permissions.push(...)` does not compile. The freeze stops at
BSON values (`_bsontype`: `ObjectId`, `Binary`, `Decimal128`), which
`Object.freeze` cannot freeze. A composite may return `ReadonlyMap` and
`ReadonlySet`; they are frozen into instances whose mutators throw, which is
safer than a `Record` keyed by user data such as a role key (`"constructor"`).
A composite's value type is checked as `Promise<T & Plain<T>>`, since
`T extends Plain<T>` is a circular constraint.

### 3.4 Keys

The library builds the key from the argument tuple itself, exactly, plus the
database name, so two e2e scopes in one process never share an entry. Only
metrics and logs see a hashed key.

### 3.5 Priming

```ts
const page = await expositionInformation.primeFrom(() =>
  catalog.unscoped.paginate("information", { entreprise }, pagination),
);
```

`primeFrom` runs the given read and records the reads of the reader's type it
makes, so it also works through a helper such as `paginate`. It primes only
from reads whose filter constrains nothing beyond the scope, the `by` value and
`_id` (anything narrower would store an incomplete key), it projects on the
reader's selection, and it stores nothing when the read ran in a transaction,
off the primary, or raced a write.

### 3.6 Registration

```ts
registerReaders(client, {
  topology: computedTopology(schemas),
  readers: [participationsOf, entrepriseMemberships, orgRoleByKey, expositionInformation, affiliations],
  limits: { entriesPerRequest: 500, rowsPerEntry: 200 },
  process: { enabled: flags.readersL1, readers: { "exposition-information": { ttlMs: 300_000 } } },
});
```

- `topology` places each reader's source in its physical collection, as for
  computed fields, and gives readers their own write-interception plan: the
  computed interceptor passes through collections that carry no computed
  field (`+entreprises` today), readers must not.
- The database a reader reads is the one of the ambient request context, which
  is how the e2e scope header already selects it. Tests use a real database the
  same way; a reader has no in-memory fake.
- Names are unique per client; a duplicate throws at registration.
- `limits` bound L0 (section 4). `process` is the L1 opt-in (section 6).

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
needs no reverse index and no routing. Writes whose scope is not a single
value (`unscoped`, `_scope: { $in }`, a multi-scope view) invalidate the type
in every scope; a replacement, a pipeline update, an insert, an upsert and a
delete always count as touching.

A transaction invalidates at each write and again after its commit, before any
`afterCommit` callback runs. Computed maintenance's internal transactions count
as commits.

**Generations** close the race between a load and a write. They are
hierarchical: `(collection, type, scope)`, `(collection, type, all scopes)`,
`(collection)`. A load captures the three before reading and is stored only if
none moved. A write bumps the level that matches what it knows: a scoped write
the first, an unscoped or multi-scope write the second, a raw write the third.

**Raw driver writes** (`invalidateReadsOnDriverWrites`) invalidate every reader
of the collection and bump its collection generation on `commandSucceeded`,
never on `commandStarted`, where a racing load would store the old value under
the new generation.

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

## 11. What Diivento puts in readers

| Reader | Kind | Consistency | L1 |
|---|---|---|---|
| exposition information (name, entreprise, modules, brand) | query `.one()` | eventual | first candidate |
| platform role permissions | query | strict | no, until measured |
| entreprise memberships of a user | query | strict | no, until measured |
| participations of a user in an exposition | query | strict | no, until measured |
| team roles of a user | query | strict | no, until measured |
| org memberships of a participant (`roleKey`, `organizationId`) | query | strict | no, until measured |
| organisations created by a user, organisations of an entreprise, active roles of an organisation | query | strict | no, until measured |
| affiliations | composite of the above | strict | no, until measured |
| session and session user | query | strict | no |

The lifecycle fields of the information document (`lifecycle.executions`,
`lifecycle.history`) are not selected, so the scheduler's writes to them do not
invalidate it. Callers that read fields a reader does not select today
(`findByUserId`, `findParticipantByUserId`, `getViewerContext`'s role keys)
either widen the selection or keep their own read.

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

Step 1:

- a write through a service invalidates the reader without any `forget` call;
- a status flip into the `where` by id, with a patch holding no `by` field,
  is seen by the next call;
- an update touching an unselected field (`lifecycle.history`) does not
  invalidate;
- a load started before a write is not joined by a call after it;
- a load that raced a write is returned but not stored, and a write to another
  type of the same collection does not discard it;
- an unscoped write invalidates the type in every scope;
- a raw driver write invalidates on success;
- a rolled back transaction leaves the cache untouched, a committed one
  invalidates before `afterCommit` runs;
- a reader over `+entreprises`, which carries no computed field, is
  invalidated by its writes;
- a value with an `ObjectId` freezes; mutating a nested array throws and does
  not compile;
- `.many` stores each key and a later single call hits;
- two request contexts on two databases share no entry;
- a reader under `withReadPreference("secondaryPreferred")` reads the primary;
- past a limit, a reader loads and stores nothing;
- `primeFrom` inside a transaction, under `secondaryPreferred`, or from a read
  filtered beyond the key stores nothing;
- a composite that reads a collection directly throws; invalidating an inner
  reader invalidates the composite.

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
   database, primary reads, limits, coarse invalidation through a reader plan
   in the interceptor, hierarchical generations, raw writes on success,
   `primeFrom`, boot index check.
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

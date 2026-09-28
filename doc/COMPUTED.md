# Computed fields and dependency versions

Status: steps 0 to 5 implemented on branch `feat/computed` (declarations, full apply and check, inline maintenance, marks and drainer, `through`), measured in §19. Steps 6 (chains), 7 (migrations) and 8 (dependency versions) remain.

## 1. The problem

A value derived from other documents is read far more often than its sources change: the organizations a participant belongs to, their active role keys, the kinds of scan they went through, the number of comments on a lead. Reading it through a `$lookup` on every query does not scale past a few tens of thousands of subjects, cannot leave its collection, and cannot be indexed. MongoDB's answer is the [Computed Pattern](https://www.mongodb.com/docs/manual/data-modeling/design-patterns/computed-values/computed-schema-pattern/): compute at write time, store the result, read it like any field.

MongoDB documents the pattern and leaves the maintenance to the application. The doc's own warning is the whole difficulty: recomputing on every write keeps the value exact, recomputing on a schedule does not. Doing it on every write by hand, in each application, is where it breaks:

- the writer lives far from the declaration, so nothing ties the stored field to what it claims to mirror;
- it runs after the write, outside the transaction, so two concurrent writes on the same subject leave a stale value (last writer wins);
- a repair tool that computes the truth then writes it, while the application runs, overwrites a concurrent write with an older truth;
- a bulk write, a migration or a raw driver call bypasses the writer silently.

Measured in the first consumer before this work: 236 stored arrays out of 747 disagreed with their relation.

This feature makes the pattern a declared, maintained, verified part of the schema. A second, smaller part gives the application what it needs to cache values it reads by key (permissions, capabilities) without trusting a node's memory: versions of declared dependencies, bumped in the writing transaction.

## 2. Expert challenges, and the answers this spec commits to

| # | Challenge | Answer |
|---|---|---|
| 1 | ODM hooks leak: Mongoose middleware famously skips `updateMany`, `findOneAndUpdate`, `bulkWrite`, upserts. One uncovered write path and the field drifts in silence. | A coverage matrix of every write method of every collection kind (§7.4), one test per cell. A write method that cannot maintain a computed field REFUSES to write to a source type. It never passes silently. |
| 2 | An ODM doing hidden writes inside `insertOne` violates least surprise. | The declaration is explicit on the subject type; every write returns the subjects it recomputed; telemetry counts recomputations and marks. Hidden, but observable. |
| 3 | You are reinventing a job queue inside the ODM: who runs the drainer across N nodes? | Mongodbee writes marks and exports `drainComputedPending()`. It never schedules anything. The application calls the drainer from its own recurring job. |
| 4 | Migrations write sources in bulk and bypass maintenance. | A migration that writes a source type marks every computed field that type feeds; the migration ends with a full apply of those fields (§12). |
| 5 | A foreign key can be an array (one source document naming several subjects). | `.by()` accepts an array path. The affected subjects are the union of the arrays before and after the write. |
| 6 | Snapshot isolation anomalies. | Two transactions that touch the same subject both WRITE the subject document, so MongoDB raises a write conflict and one replays. This argument does not hold for a second hop (`through`), where the far-side write only leaves a mark; there, exactness rests on the drainer and is declared "eventual" (§6). |
| 7 | The subject document is also written by the application; recomputations will collide with those writes. | Accepted; the collision is a write conflict, replayed by the whole-transaction retry. Measured under load before adoption (§16). |
| 8 | Dependency versions for caches: is that an ODM concern? | Split: mongodbee bumps versions because it sees the write; the cache itself (keys, storage, eviction, a shared store if ever) belongs to the application. |
| 9 | No replica set, no transaction. | Explicit mode (§7.5): refuse by default; degrade only when the connection declares it. |
| 10 | `through` and chains turn the compiler into a query planner. Scope creep. | `through` is limited to one extra hop; chains are limited to a declared acyclic graph checked at definition time. Nothing else. |

A prerequisite surfaced while writing this spec: `retryOnWriteConflict` retries a single operation even when that operation belongs to a transaction, which replays it inside a transaction the server already aborted (`NoSuchTransaction`). Inside a transaction, a write conflict must propagate so the whole transaction replays. This is fixed first (§17, step 0).

## 3. Scope

In:

- **Projection of a relation into an array** on the subject (`collect`), with or without duplicates.
- **Count of a relation** on the subject (`count`).
- Sources in the subject's collection, in another collection (same scope or global subject), and through one extra hop (`through`).
- Computed fields that depend on other computed fields, as a declared acyclic graph.
- Maintenance on write, marks and drainer, full apply, verification, migration operations, frozen declarations.
- Dependency versions for application caches.

Out, deliberately:

- **Reservation counters and sequences** (a seat counter guarded by capacity, a waitlist rank sequence, a quota). They look like counts and are not: the conditional `$inc` IS the guard that prevents overselling. A count recomputed after the insert would lose the guard. They stay application code.
- **Values computed from the subject's own fields** (for example an id list derived from an embedded array on the same document). There is no relation to maintain; the application computes them in the same write.
- `sum`, `min`, `max`, averages. They arrive with the delta strategy (§15), not before.
- Anything not written to MongoDB: time, external APIs. A dependency on time is not a version; the application keeps an expiry for it.

## 4. The `_computed` root

Every computed field of a type lives under one root, `_computed`:

```
participant {
  _id, _type, _scope,              owned by mongodbee
  fields, status, ...              written by the application
  _computed: {                     written by mongodbee only
    organizationIds: [...],
    roleKeys: [...],
    scanKinds: [...],
  }
}
```

- **One writer.** Any application write whose path starts with `_computed` is rejected (`ComputedFieldWriteError`), including `$set`, `$unset`, `$push`, a replacement document that changes `_computed`, and an insert that provides it.
- **Generated schema.** Mongodbee generates the `_computed` sub-schema from the declarations; the entry types are inferred from the source fields (`collect(m.organizationId)` over a `refId("expo_organization")` field gives `refId("expo_organization")[]`, `count()` gives a non-negative integer). The application does not declare `_computed` in its Valibot schema; declaring it is a definition error.
- **Absent is not empty.** Before its first full apply a computed field is absent, and a reader must be able to tell "not computed yet" from "computed, empty". Once the migration that introduces a field has run its full apply, the generated schema marks it required.
- **Dotted writes only.** A recomputation writes `$set: { "_computed.<name>": value }`, never the whole object, so recomputing one field cannot overwrite a sibling.
- **Indexable.** `_computed.<name>` is a regular path for the index builder (`index(f._computed.organizationIds)`).
- **Exclusion made easy.** Response projections, exports, public views and diffs exclude one root instead of a list of fields to keep in sync.

## 5. Declaration

A computed field is declared on its subject type, next to its indexes. The builder mirrors the index builder: field proxies, `.where(field, value)`, `.named()`-style chaining. It produces a serializable descriptor: no user callback survives into the stored definition, which is what lets a migration freeze it and `check` compare it.

```ts
export const ParticipantType = defineType({
  schema: ParticipantSchema,
  indexes: (f) => [index(f._computed.organizationIds)],
  computed: {
    organizationIds: from("org_membership")
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .collect((m) => m.organizationId),

    roleKeys: from("participant_role")
      .by((r) => r.participantId)
      .where((r) => [r.status, "active"])
      .collect((r) => r.roleKey)
      .distinct(),

    scanKinds: from(ScansModel, "scan")
      .by((s) => s.participantId)
      .sameScope()
      .collect((s) => s.scanType)
      .distinct()
      .maxEntries(64),

    validatedOrganizationIds: from("org_membership")
      .by((m) => m.participantId)
      .where((m) => [m.status, "active"])
      .through("expo_organization", (m) => m.organizationId)
      .where((o) => [o.data.status, "validated"])
      .collect((o) => o._id),
  },
});

export const LeadType = defineType({
  schema: LeadSchema,
  computed: {
    commentCount: from("lead_comment").by((c) => c.leadId).count(),
  },
});
```

| Builder | Meaning |
|---|---|
| `from(type)` / `from(Model, type)` | The source type: in the subject's own (multi-)collection, or in another model. |
| `.by(fk)` | The source field that names the subject: a single reference or an array of references. |
| `.sameScope()` | Required when the source lives in another scoped collection: the subject is looked up in the source's scope. |
| `.where((s) => [s.field, value])` / `.where((s) => [s.field, [values]])` | Equality or membership over the source's field proxy (the same proxy as the index builder). Repeatable, conjunctive. The only predicates: they stay serializable and indexable. |
| `.through(type, fk)` | One extra hop: the value is computed over the far documents reached through `fk`. At most one. |
| `.collect(field)` | Array of the field's values, one entry per contributing document. |
| `.distinct()` | Equal values collapse. A `collect` without it keeps one entry per row, which is what makes `size()` a row count. |
| `.count()` | Number of contributing documents. |
| `.maxEntries(n)` | Refuse, at write time and at full apply, to store more than `n` entries (§7.3). Required on `collect` when the subject is global or the source is in another model. |

Definition-time checks (thrown by `defineType`, so a mistake fails at boot, not in production):

- the source type, every referenced path and the `by` field exist; the `by` field's reference type matches the subject type;
- the collected field's type is storable; `count` and `collect` are not both used;
- `_computed` is not declared in the application schema;
- the graph of computed-on-computed dependencies is acyclic (§10).

## 6. Guarantees per case

| Case | Example | Maintenance | Guarantee |
|---|---|---|---|
| 1. Source in the subject's collection or another collection, same scope | `organizationIds`, `scanKinds` | In the writing transaction | Exact at commit |
| 2. Global subject, sources across scopes | `user._computed.expositionCount` | In the writing transaction | Exact at commit |
| 3. Condition or value on a far document (`through`) | `validatedOrganizationIds` | Near-side write: in the transaction. Far-side write: a mark in the transaction, drained after. | Exact at commit for near-side changes; eventually exact, durably, for far-side changes |
| 4. Computed on computed | a field filtered on `organization._computed.memberCount` | Recomputing a field marks its dependents | Eventually exact, durably |

"Durably" means the obligation to recompute is written in the same transaction as the change that causes it. It can be late; it cannot be lost. A reader that needs a case 3 or 4 value exact at this instant reads the relation itself.

## 7. Maintenance on write

### 7.1 Algorithm

For a write through mongodbee on a source type `S` feeding computed fields `F1..Fn`:

1. **Before-subjects.** Read, in the transaction, the `by` values of the documents the write targets, projected on that field only. An insert has none. If their number exceeds `INLINE_RECOMPUTE_LIMIT`, switch to marks (§8).
2. **The write itself.**
3. **After-subjects.** The `by` values of the written documents (an update can change the foreign key).
4. **Recompute.** For every subject in before ∪ after and every `Fi`: one aggregation over the sources of that subject, then `$set: { "_computed.Fi": value }` on the subject. Subjects that no longer exist are skipped.
5. All of it inside the ambient transaction, or inside one opened for the write when there is none (§7.5).

Recomputing the whole value from the subject's sources, rather than applying a delta, is deliberate for v1: it is exact for every aggregate, it heals a subject that drifted, and its cost is bounded by the sources of one subject.

### 7.2 Far side (`through`) and chains

A write on the far type (`expo_organization` for `validatedOrganizationIds`) does not know its subjects without reading back through the near relation, which can reach thousands of documents. It writes one mark `{ field, far: <far id> }` in its transaction; the drainer expands it into subjects by reading the near relation and recomputes them in bounded transactions.

### 7.3 Bounds

- `inlineLimit` (default 1000, `registerComputed(db, topology, { inlineLimit })`): the most source documents a write may target, and the most subjects it may affect per field, for the recomputation to stay inside the write. Past it the write is never refused: it writes one `whole` mark per affected field in its own transaction and leaves the recomputation to the drainer. Reading past the limit stops at `inlineLimit + 1` documents, so an oversized write costs a bounded read. The default is to be confirmed by measurement (§16).
- `maxEntries`: a `collect` that would exceed it throws `ComputedEntriesExceededError` inside the transaction, so the write that would produce it fails. A field that legitimately grows past it must be a `count`.

### 7.4 Coverage matrix

Every write method of every collection kind maintains, marks, or refuses. No fourth outcome.

| Method | `collection` | `multiCollection` | scoped view |
|---|---|---|---|
| `insertOne`, `insertMany` | maintain | maintain | maintain |
| `updateOne` (filter or id) | maintain | maintain | maintain |
| `updateMany`, `updateWhere` | maintain, or mark past the limit | same | same |
| `findOneAndUpdate`, `findOneAndReplace`, `findOneAndDelete` | maintain | maintain | maintain |
| `replaceOne` | maintain | n/a | n/a |
| `deleteOne`, `deleteId`, `deleteIds`, `deleteMany`, `deleteAny` | maintain, or mark past the limit | same | same |
| `bulkWrite` | maintain per operation, or mark | n/a | n/a |
| upsert (any method) | maintain (the inserted document has no before-subject) | same | same |
| `dropScope`, `drop` | `dropScope` is a scoped `deleteMany`: maintained inline, or a `whole` mark bounded to that scope past the limit; `drop` of a source collection writes a `whole` mark per field it feeds before dropping (a drop cannot join a transaction, so the mark comes first: a failed drop only costs a harmless recomputation) | same | same |
| `.collection` (the driver object a mongodbee collection exposes) | maintain: it is the same intercepted object | same | same |
| `initializeOrderedBulkOp`, `initializeUnorderedBulkOp` | refuse (`ComputedUnsupportedWriteError`); use `bulkWrite` | same | same |
| a driver collection obtained elsewhere (`db.collection(name)`) | cannot see it; caught by `check` | | |

A test per cell asserts the stored value equals a full apply after the write.

**Choke point.** Every collection kind opens exactly one driver collection. Mongodbee wraps it in an interceptor, so every write method, present or future, and every internal path that reaches the driver goes through the same maintenance. The interceptor only reads the topology at write time, so the order in which collections are opened does not matter.

**Topology.** `computedTopology(schemas)` places every subject, source and far type in its physical collection from the application's living schemas definition (the one the migration CLI already reads), and fails at boot on anything it cannot place precisely: a type name declared twice, a missing `sameScope()`, an unbounded `collect` on a global subject or across collections, computed fields on a model template. `registerComputed(db, topology, { inlineLimit?, standaloneMode? })` binds it to a database. Writing to a collection whose types carry computed fields on a database with no registered topology throws `ComputedNotRegisteredError`, so a node that forgot to register cannot write stale data silently. The remaining gap (a node that forgot to register writing a source type whose own collection declares no computed field) closes with the frozen descriptors of §12: the runtime compares them to the registered topology.

**Precision.** An update is inspected before anything is read: a field is maintained only when the update touches one of its inputs (`by`, a `where` path, the collected path or the `through` link). A replacement, a pipeline update, an insert and a delete always count as touching.

### 7.5 Transactions

- Ambient transaction: the maintenance joins it.
- No ambient transaction, replica set: the write and its maintenance run in a transaction opened for them. That transaction belongs to mongodbee, so mongodbee replays it on a write conflict; the caller sees the same behaviour as before computed fields existed.
- No replica set: refused (`ComputedRequiresTransactionError`) unless the registration declares `standaloneMode: "best-effort"`, in which case the write and the recomputation run in sequence and only `check` can catch a crash between them. Test and dev only.
- A write conflict inside the caller's transaction propagates; the caller's whole-transaction retry replays it. `retryOnWriteConflict` never replays a single operation inside a transaction, and does replay a whole transaction it wraps (step 0).

## 8. Marks and the drainer

- Marks live in the internal collection `__dbee_computed_pending__`, next to `__dbee_migration__`. The `_id` is the identity, so repeated marks collapse into one document:
  - `whole`: `"<subject>.<field>|whole|<scope or *>"`. Recompute the field for every subject, or for one scope when the field is scoped and the write was bounded to a scope. Written past the inline limit (a `deleteMany`, an `updateMany`, a `dropScope` over a large scope, a write touching many subjects) and before a source collection is dropped.
  - `subject`: `"<subject>.<field>|subject|<id>"`. Recompute one subject. Used by `through` (§9) and chains (§10).
- Each mark carries a `generation`, incremented by every write that marks it again. The drainer deletes a mark only if its generation is unchanged since it claimed it; a mark renewed while it was being drained is requeued, never lost (proved by a test whose mutant, deleting by `_id` only, is killed).
- `drainComputedPending(db, { topology, limit, batchSize, leaseMs })` claims one mark at a time with a lease (`claimedUntil`), so concurrent drainers on every worker do not duplicate work, and a crashed drainer's lease expires. A `whole` mark runs the full apply path (§11), batched transactions; a `subject` mark recomputes its subject and deletes the mark in one transaction. It returns `{ drained, requeued, remaining, oldestAgeMs }`. A mark whose field no longer exists in the topology is discarded. Idempotent: a mark drained twice recomputes the same truth.
- Mongodbee never schedules it. The application runs it from its recurring job infrastructure, on every worker.
- `pendingComputed(db)` returns `{ count, oldestAgeMs, byField }` for alerting.
- Telemetry: pending marks by field, age of the oldest, recomputations by field and by path (inline, drained, full apply).

## 9. `through` in detail

- The near relation carries the `by` link and its `where`; the far relation carries its own `where` and the collected field.
- Near-side writes: §7.1, exact at commit.
- Far-side writes: §7.2, a far mark; the drainer finds subjects with `{ <fk>: far }` on the near type under the near `where`.
- A far write that cannot change the value (the far `where` fields and the collected field are untouched by the update) writes no mark. Detected from the update document, not from a read.

## 10. Computed on computed

- A field may use another type's `_computed.<name>` in a `where` or as the collected field.
- Definition-time: the dependency graph over all declared computed fields is built and must be acyclic; a cycle names its fields in the error.
- Recomputing a field that others depend on writes a mark for each dependent field and each subject it affects. Chains are therefore eventual (case 4), whatever the depth.

## 11. Full apply

- `applyComputed(db, { type, field, scope?, batchSize })` recomputes every subject of a type, walking subjects by `_id` in batches. One batch is one transaction: read the sources of the batch's subjects, write their `_computed.<field>`.
- Never "compute the truth for everyone, then write outside a transaction": with the application running, a write between the two would be overwritten by an older truth. Batches in transactions turn that race into a write conflict and a replay.
- Used by: migrations (§12), `check --fix` (§14), the drainer's expansion of large marks.

## 12. Migrations

- The computed descriptors of a type are part of the frozen migration schema, exactly like its composite indexes.
- `check` refuses a living `schemas.ts` whose descriptors differ from the last migration's snapshot, like a drifted declared index.
- A migration that adds or changes a descriptor carries the operation that recomputes it: `.type("participant").applyComputed("organizationIds")`, run after the migration's other operations.
- A migration operation that writes a source type (`transform`, `seed`, `deleteWhere`, `flowToScope`, `dedupe`) marks every computed field that type feeds; the migration ends with a full apply of those fields. Declared by the operation, not left to the author.
- Renaming an existing hand-maintained field into `_computed`: introduce the descriptor with its full apply, switch readers, then drop the old field in a later migration.

## 13. Dependency versions (for application caches)

A value the application reads by key and never queries on (permissions of a user on an exposition, the capabilities of an exposition) is cached by the application. Mongodbee gives it the one thing a cache cannot get right alone across nodes: knowing, without trusting any node's memory, that an input changed.

```ts
export const dependencies = defineDependencies({
  roles: dependency("participant_role").per((r) => r._scope),
  grants: dependency("role_permission").per((p) => p.roleId),
  moduleConfig: dependency("information").per((i) => i._scope).fields((i) => [i.modules]),
});
```

- A write through mongodbee on a declared type increments `+dependency_versions` (`_id: "<dependency>|<key>"`) in the same transaction.
- `readVersions(db, ["roles|exposition:x", "grants|role:y"])` returns the current numbers in one read. The application puts them in its cache key; a stale entry becomes unreachable instead of having to be invalidated. Losing the cache loses nothing.
- `.fields()` restricts the bump to writes touching those paths.
- Hot-spot guard: a dependency is a counter shared by every write of its key. Declaring one on a type written in bursts turns it into a contended document (the failure mode of a seat counter). `defineDependencies` requires `.hot("accepted")` to declare a dependency whose key is shared by a high-churn type, so the choice is explicit.
- Time is not a write: a value that depends on today's date keeps an expiry in the application cache.

## 14. Verification

- `check --computed` recomputes, without writing, every computed field of every subject (bounded per run, resumable by `_id`), and reports drifts: `{ type, field, subject, stored, truth }`, plus pending marks older than a threshold.
- `check --computed --fix` repairs drifts through the full apply path (§11), never by a blind overwrite.
- The same check runs as a function so an application can alert on it.

## 15. Delta strategy (v2, gated)

Applying only the difference (`$inc` for a count, adding or removing one entry) is faster for large relations and is exact only when the aggregate is invertible and both images of the source document are known. It is an internal strategy behind the same declaration, chosen per aggregate:

- `count`, `sum`, `collect` with duplicates: invertible (removing one occurrence uses a pipeline update, not `$pull`, which removes them all);
- `collect().distinct()`, `min`, `max`: not invertible; recompute.

It ships only with a property-based oracle proving that, over random sequences of writes, the delta result equals a full apply at every step, and only for a case whose recomputation cost was measured to matter. A delta keeps a drift forever where a recomputation heals it; the oracle and `check` are what make that acceptable.

## 16. Proof plan

Nothing ships without all of these green:

1. **Oracle, property-based.** Random sequences of inserts, updates (including foreign key changes and array foreign keys), deletes, bulk writes, far-side writes and chains, with and without an ambient transaction, with the inline limit forced low to exercise marks. After every step and after draining: stored equals full apply, for every field.
2. **Coverage matrix.** One test per cell of §7.4.
3. **Concurrency.** Transactions writing sources of the same subject concurrently; a drainer and a full apply running during writes. Final state equals full apply; no lost update.
4. **Crash.** Marks written, process killed, drainer finishes the work; full apply interrupted halfway, resumed.
5. **Refusals.** Every forbidden write (`_computed` path, uncovered method, standalone without opt-in, `maxEntries` exceeded) throws the named error.
6. **Definitions.** Every definition-time check fails on a crafted bad declaration, cycles included.
7. **Migrations.** A frozen descriptor, a drifted living schema refused, a migration writing sources ends with a correct full apply.
8. **Mutants.** Every guard and every branch of the maintenance kills at least one test.
9. **Measurements.** Added cost per source write (inline recompute versus none), per recomputed subject, and the default `INLINE_RECOMPUTE_LIMIT` derived from them.

In the first consumer (diivento), before a field switches to `_computed`: a cluster scenario of concurrent membership writes on the same participants across nodes, with nodes killed; and one changing an organization's status under load for `through`.

## 17. Build order

Each step lands only when its part of §16 is green.

0. `retryOnWriteConflict` stops retrying a single operation inside a transaction.
1. Descriptor builder, generated `_computed` schema, definition-time checks, `_computed` write refusal.
2. Full apply and `check --computed` (they define the truth everything else is compared to).
3. Inline maintenance for cases 1 and 2 across the coverage matrix, with the oracle.
4. Marks and drainer; `updateMany`/`deleteMany` past the limit; `dropScope`.
5. `through` (case 3).
6. Computed on computed (case 4).
7. Migrations: frozen descriptors, drift refusal, `applyComputed` operation, source-writing operations marking.
8. Dependency versions.
9. Measurements, then a release.
10. Delta, only on a measured need, behind its oracle.

## 18. Decisions taken

- The root is `_computed`: owned by mongodbee like `_id`, `_type` and `_scope`. "Cache" is reserved for values an application may lose; a computed field is exact and queries rely on it.
- A migration that writes a source type marks the fields it feeds and ends with their full apply, rather than `check` refusing it.
- Mongodbee provides dependency versions; applications own their caches.
- v1 recomputes; delta is a later, oracle-gated optimisation.

## 19. Measurements

Local replica set, one process, `bench/computed-cost.ts` and `bench/computed-contention.ts` (rerun them to reproduce). Medians over 300 sequential writes; "off" is the same write with no computed field registered.

| Write | Off | On | Note |
|---|---|---|---|
| insert a source row (subject holds 1 / 10 / 100 rows) | 0.4 ms | 2.0 / 2.0 / 2.8 ms | the write now opens its own transaction and recomputes one subject |
| change a field the computed value reads | 0.5 ms | 2.2 to 2.6 ms | same |
| change a field no computed value reads | 1 ms | 1 ms | no read, no transaction: the precision of §7.4 pays |
| update the subject itself on its own fields | 1 ms | 1 ms | same |
| delete a source row | 1 ms | 2.2 to 3 ms | |
| full apply | | 0.07 ms per subject | 10 000 subjects in 0.68 s |

Two findings changed the code:

- **Contention on one subject.** 50 concurrent writes feeding the same subject took 1.2 s against 10 ms without computed fields: every transaction writes the subject, so they serialise, and the old retry (10 to 400 ms, 20% jitter) spent most of that time sleeping in lockstep. The default retry is now full jitter between 2 and 50 ms (`DEFAULT_COMPUTED_RETRY`, overridable per registration): 5 / 20 / 50 concurrent writers take about 18 / 100 / 260 ms. What remains is the serialisation itself, about 5 ms per writer, inherent to one document written by every transaction. A subject written in bursts by many sources (a counter on a popular parent) should stay a hand-kept guard or wait for the delta strategy.
- **A global subject read across scopes.** Recomputing an account from the participants of every exposition used the `_type` index and read every participant of the type. `computedTopology` now refuses, at boot, any field whose recompute read has no declared index leading with its `by` (or `through` link) field, and requires a `global` index when the read spans scopes. With it the same read touches exactly the rows it returns.

In the first consumer (Diivento, `participant._computed.organizationIds`, dev database with 21 681 participants and 8 166 memberships) the recompute read is an `IXSCAN` on `_scope, _type, participantId`: one document examined, 2 ms.

/**
 * Per-database serialisation of schema DDL (validator + index reconciliation).
 *
 * `collection()`, `multiCollection()` and `scopedMultiCollection()` bring a
 * collection to its declared shape at construction time: `listCollections`,
 * `createCollection` / `collMod`, `listIndexes`, `dropIndex`, `createIndexes`.
 * Two constructions of the same database running at once would interleave
 * those commands; this module runs each construction's DDL block as one
 * critical section, keyed by the database it targets.
 *
 * Why the key is `(MongoClient, database name)`:
 * - not process-wide: the previous design (`operation.ts`, a single queue with
 *   `maxConcurrent: 1`) made every database in the process wait behind every
 *   other one. A process serving 150 throwaway databases — Playwright E2E,
 *   one database per test — paid the whole backlog before its own first
 *   request could answer. Measured on a single-node replica set, 6 databases
 *   × 31 collections × 3 indexes: 13.6 s process-wide serial, 8.2 s with
 *   databases in parallel and serial within each. Wider parallelism gained
 *   nothing (the server serialises DDL per database anyway) and unbounded
 *   parallelism was slower (9.5 s) while saturating the connection pool.
 * - not per namespace: a database's collections share the server-side DDL
 *   path, and the same measurement showed intra-database parallelism buys
 *   nothing. One lock per database keeps at most one DDL command in flight
 *   per database, which is the natural bound on pool usage.
 * - per client, not per name: two `MongoClient`s may point at different
 *   servers that happen to host a database of the same name. `Db` objects are
 *   not a stable key either — `client.db(name)` returns a fresh instance on
 *   every call — so the map is keyed by the name under a `WeakMap` on the
 *   client, the same shape as `session.ts`'s session context.
 *
 * Why there is deliberately no timeout: the queue this replaces raced each
 * command against a 5 s timer. A timeout on the client cannot cancel an index
 * build on the server, so on expiry the caller was rejected — construction
 * failed, the process typically booted without that collection — while the
 * server kept building; on the next boot the index existed and everything
 * worked, which is how the failure hid. Any index on a non-trivial collection
 * builds for longer than 5 s. If a bound is ever wanted it belongs on the
 * command (`maxTimeMS`), where the server can honour it.
 */

import type { Db, MongoClient } from "./mongodb.ts";

/**
 * One database's lock. `tail` is the promise the next entrant waits on;
 * `holders` counts every caller that has entered and not yet left — waiting or
 * running — so the entry can be dropped from the map exactly when the last
 * one leaves. Without the count, a long-lived process serving many transient
 * databases (again: E2E) would keep one entry per database ever seen.
 */
type DatabaseLock = { tail: Promise<void>; holders: number };

const locksByClient = new WeakMap<MongoClient, Map<string, DatabaseLock>>();

function locksOf(client: MongoClient): Map<string, DatabaseLock> {
  let locks = locksByClient.get(client);
  if (!locks) {
    locks = new Map();
    locksByClient.set(client, locks);
  }
  return locks;
}

/**
 * Runs `work` as the only DDL block in flight for `db` on its client.
 * Callers on the same database run in arrival order; callers on other
 * databases or other clients are not affected. A rejection from `work`
 * propagates unchanged and releases the lock like a normal completion.
 */
export async function withDatabaseDdlLock<T>(
  db: Db,
  work: () => Promise<T>,
): Promise<T> {
  const locks = locksOf(db.client);
  const lock = locks.get(db.databaseName) ?? {
    tail: Promise.resolve(),
    holders: 0,
  };
  // Everything from the map read to the `tail` swap runs synchronously, before
  // the first `await`: two concurrent callers must both see the same entry and
  // chain on each other's promise. An `await` in between would let both read
  // the old `tail` and run concurrently.
  locks.set(db.databaseName, lock);
  lock.holders++;
  const turn = lock.tail;
  let release: () => void = () => {};
  lock.tail = new Promise<void>((resolve) => {
    release = resolve;
  });
  await turn;
  try {
    return await work();
  } finally {
    release();
    lock.holders--;
    if (lock.holders === 0) locks.delete(db.databaseName);
  }
}

/**
 * Names of the databases currently holding or waiting for a lock on
 * `client`. Exposed for tests; not part of the public surface.
 */
export function lockedDatabases(client: MongoClient): string[] {
  return [...(locksByClient.get(client)?.keys() ?? [])];
}

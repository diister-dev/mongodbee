import type { Db, MongoClient } from "./mongodb.ts";

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

export async function withDatabaseDdlLock<T>(
  db: Db,
  work: () => Promise<T>,
): Promise<T> {
  const locks = locksOf(db.client);
  const lock = locks.get(db.databaseName) ?? {
    tail: Promise.resolve(),
    holders: 0,
  };
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

export function lockedDatabases(client: MongoClient): string[] {
  return [...(locksByClient.get(client)?.keys() ?? [])];
}

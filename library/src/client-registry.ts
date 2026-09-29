import type { Db, MongoClient } from "./mongodb.ts";

const EVERY_DATABASE = "*";

export function isDb(target: Db | MongoClient): target is Db {
  return "databaseName" in target && "client" in target;
}

function keyOf(target: Db | MongoClient): {
  client: MongoClient;
  name: string;
} {
  return isDb(target)
    ? { client: target.client, name: target.databaseName }
    : { client: target, name: EVERY_DATABASE };
}

export class ClientRegistry<T> {
  readonly #byClient = new WeakMap<MongoClient, Map<string, T>>();

  set(target: Db | MongoClient, value: T): void {
    const { client, name } = keyOf(target);
    const byName = this.#byClient.get(client) ?? new Map<string, T>();
    byName.set(name, value);
    this.#byClient.set(client, byName);
  }

  delete(target: Db | MongoClient): void {
    const { client, name } = keyOf(target);
    this.#byClient.get(client)?.delete(name);
  }

  deleteClient(client: MongoClient): void {
    this.#byClient.delete(client);
  }

  get(db: Db): T | undefined {
    const byName = this.#byClient.get(db.client);
    return byName?.get(db.databaseName) ?? byName?.get(EVERY_DATABASE);
  }
}

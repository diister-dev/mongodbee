import { AsyncLocalStorage } from "node:async_hooks";

const transactionScope = new AsyncLocalStorage<true>();

export function runInTransactionScope<T>(fn: () => Promise<T>): Promise<T> {
  return transactionScope.run(true, fn);
}

export function insideTransaction(): boolean {
  return transactionScope.getStore() === true;
}

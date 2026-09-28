import { contextVariable } from "./context-variable.ts";

const transactionScope = contextVariable<true>("mongodbee.transaction");

export function runInTransactionScope<T>(fn: () => Promise<T>): Promise<T> {
  return transactionScope.run(true, fn);
}

export function insideTransaction(): boolean {
  return transactionScope.get() === true;
}

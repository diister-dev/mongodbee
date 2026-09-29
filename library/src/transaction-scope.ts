import { contextVariable } from "./context-variable.ts";

export type AfterCommit = () => unknown;

interface TransactionScope {
  readonly afterCommit: AfterCommit[];
}

const transactionScope = contextVariable<TransactionScope>(
  "mongodbee.transaction",
);

export interface TransactionRun<T> {
  readonly result: T;
  readonly afterCommit: readonly AfterCommit[];
}

export async function runInTransactionScope<T>(
  fn: () => Promise<T>,
): Promise<TransactionRun<T>> {
  const scope: TransactionScope = { afterCommit: [] };
  const result = await transactionScope.run(scope, fn);
  return { result, afterCommit: scope.afterCommit };
}

export function insideTransaction(): boolean {
  return transactionScope.get() !== undefined;
}

export async function afterCommit(callback: AfterCommit): Promise<void> {
  const scope = transactionScope.get();
  if (scope) {
    scope.afterCommit.push(callback);
    return;
  }
  await callback();
}

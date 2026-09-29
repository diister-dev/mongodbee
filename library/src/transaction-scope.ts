import { contextVariable } from "./context-variable.ts";

export type AfterCommit = () => unknown;

interface TransactionScope {
  readonly afterCommit: AfterCommit[];
  readonly atCommit: (() => void)[];
}

const transactionScope = contextVariable<TransactionScope>(
  "mongodbee.transaction",
);

export interface TransactionRun<T> {
  readonly result: T;
  readonly afterCommit: readonly AfterCommit[];
  readonly atCommit: readonly (() => void)[];
}

export async function runInTransactionScope<T>(
  fn: () => Promise<T>,
): Promise<TransactionRun<T>> {
  const scope: TransactionScope = { afterCommit: [], atCommit: [] };
  const result = await transactionScope.run(scope, fn);
  return { result, afterCommit: scope.afterCommit, atCommit: scope.atCommit };
}

export function insideTransaction(): boolean {
  return transactionScope.get() !== undefined;
}

export function deferToCommit(action: () => void): void {
  transactionScope.get()?.atCommit.push(action);
}

export async function afterCommit(callback: AfterCommit): Promise<void> {
  const scope = transactionScope.get();
  if (scope) {
    scope.afterCommit.push(callback);
    return;
  }
  await callback();
}

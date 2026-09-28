import { AsyncLocalStorage } from "node:async_hooks";

export interface ContextVariable<T> {
  get(): T | undefined;
  run<R>(value: T, fn: () => R): R;
}

interface NativeAsyncContext {
  Variable: new <T>(options?: { name?: string }) => ContextVariable<T>;
}

function isNativeAsyncContext(value: unknown): value is NativeAsyncContext {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof Reflect.get(value, "Variable") === "function"
  );
}

export function contextVariable<T>(name: string): ContextVariable<T> {
  const native = Reflect.get(globalThis, "AsyncContext");
  if (isNativeAsyncContext(native)) return new native.Variable<T>({ name });
  const storage = new AsyncLocalStorage<T>();
  return {
    get: () => storage.getStore(),
    run: (value, fn) => storage.run(value, fn),
  };
}

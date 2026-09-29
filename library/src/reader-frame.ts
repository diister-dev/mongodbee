import { contextVariable } from "./context-variable.ts";
import type { ReaderEntry } from "./reader-cache.ts";

export class ReaderDirectReadError extends Error {
  override readonly name = "ReaderDirectReadError";
}

export interface CompositeFrame {
  readonly reader: string;
  readonly call: string;
  readonly parent: CompositeFrame | undefined;
  readonly dependOn: (dependency: ReaderEntry | undefined) => void;
}

const compositeFrame = contextVariable<CompositeFrame>(
  "mongodbee.reader.composite",
);

export function runInComposite<T>(frame: CompositeFrame, fn: () => T): T {
  return compositeFrame.run(frame, fn);
}

export function currentComposite(): CompositeFrame | undefined {
  return compositeFrame.get();
}

export function recordDependency(dependency: ReaderEntry | undefined): void {
  compositeFrame.get()?.dependOn(dependency);
}

export function compositeIsLoading(call: string): boolean {
  for (let frame = compositeFrame.get(); frame; frame = frame.parent) {
    if (frame.call === call) return true;
  }
  return false;
}

export function assertOutsideComposite(operation: string): void {
  const frame = compositeFrame.get();
  if (!frame) return;
  throw new ReaderDirectReadError(
    `reader "${frame.reader}" calls ${operation}() directly; a composite reader reads through other readers only, so each fact it uses is invalidated with it`,
  );
}

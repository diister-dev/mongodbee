import { isRecord } from "./utils/guards.ts";

export const COMPUTED_ROOT = "_computed";

export class ComputedFieldWriteError extends Error {
  override readonly name = "ComputedFieldWriteError";

  constructor(path: string) {
    super(
      `"${path}" is maintained by mongodbee from its computed declaration; the application cannot write it`,
    );
  }
}

const isComputedPath = (path: string): boolean =>
  path === COMPUTED_ROOT || path.startsWith(`${COMPUTED_ROOT}.`);

function refuseKeys(record: Readonly<Record<string, unknown>>): void {
  for (const key of Object.keys(record)) {
    if (isComputedPath(key)) throw new ComputedFieldWriteError(key);
  }
}

export function refuseComputedWrite(payload: unknown): void {
  if (Array.isArray(payload)) {
    for (const stage of payload) {
      const serialized = JSON.stringify(stage) ?? "";
      if (
        serialized.includes(`"${COMPUTED_ROOT}`) ||
        serialized.includes(`$${COMPUTED_ROOT}`)
      ) {
        throw new ComputedFieldWriteError(COMPUTED_ROOT);
      }
    }
    return;
  }
  if (!isRecord(payload)) return;
  refuseKeys(payload);
  for (const [key, value] of Object.entries(payload)) {
    if (!key.startsWith("$") || !isRecord(value)) continue;
    refuseKeys(value);
    if (key === "$rename") {
      for (const target of Object.values(value)) {
        if (typeof target === "string" && isComputedPath(target))
          throw new ComputedFieldWriteError(target);
      }
    }
  }
}

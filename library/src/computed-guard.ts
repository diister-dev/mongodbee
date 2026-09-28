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

function refuseKeys(record: Record<string, unknown>): void {
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
  if (typeof payload !== "object" || payload === null) return;
  const record = payload as Record<string, unknown>;
  refuseKeys(record);
  for (const [key, value] of Object.entries(record)) {
    if (
      !key.startsWith("$") ||
      typeof value !== "object" ||
      value === null ||
      Array.isArray(value)
    )
      continue;
    refuseKeys(value as Record<string, unknown>);
    if (key === "$rename") {
      for (const target of Object.values(value as Record<string, unknown>)) {
        if (typeof target === "string" && isComputedPath(target))
          throw new ComputedFieldWriteError(target);
      }
    }
  }
}

export function valueAt(doc: Record<string, unknown>, path: string): unknown {
  let current: unknown = doc;
  for (const key of path.split(".")) {
    if (current === null || typeof current !== "object") return undefined;
    current = (current as Record<string, unknown>)[key];
  }
  return current;
}

export function setValueAt(
  doc: Record<string, unknown>,
  path: string,
  value: unknown,
): void {
  const keys = path.split(".");
  const last = keys.pop()!;
  let current: unknown = doc;
  for (const key of keys) {
    if (current === null || typeof current !== "object") return;
    current = (current as Record<string, unknown>)[key];
  }
  if (current !== null && typeof current === "object") {
    (current as Record<string, unknown>)[last] = value;
  }
}

export function placeValueAt(
  doc: Record<string, unknown>,
  path: string,
  value: unknown,
): void {
  const keys = path.split(".");
  const last = keys.pop()!;
  let current = doc;
  for (const key of keys) {
    const next = current[key];
    if (next === undefined) current[key] = {};
    else if (next === null || typeof next !== "object") return;
    current = current[key] as Record<string, unknown>;
  }
  current[last] = value;
}

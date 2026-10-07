import { isRecord } from "../guards.ts";

export function field(value: unknown, key: string): unknown {
  return isRecord(value) ? value[key] : undefined;
}

export function textField(value: unknown, key: string): string | undefined {
  const found = field(value, key);
  return typeof found === "string" ? found : undefined;
}

export function recordField(
  value: unknown,
  key: string,
): Readonly<Record<string, unknown>> | undefined {
  const found = field(value, key);
  return isRecord(found) ? found : undefined;
}

export function textListField(
  value: unknown,
  key: string,
): string[] | undefined {
  const found = field(value, key);
  return Array.isArray(found) && found.every((item) => typeof item === "string")
    ? found
    : undefined;
}

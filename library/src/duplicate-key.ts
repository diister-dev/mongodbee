export const DUPLICATE_KEY_CODE = 11000;

export interface DuplicateKeyDetails {
  indexName: string | null;
  keyPattern: Record<string, unknown>;
  keyValue: Record<string, unknown>;
}

export function isDuplicateKeyError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { code?: unknown }).code === DUPLICATE_KEY_CODE
  );
}

export function duplicateKeyOf(error: unknown): DuplicateKeyDetails | null {
  if (!isDuplicateKeyError(error)) return null;
  const details = error as {
    message?: string;
    keyPattern?: Record<string, unknown>;
    keyValue?: Record<string, unknown>;
  };
  const named = /index: (\S+)/.exec(details.message ?? "");
  return {
    indexName: named?.[1] ?? null,
    keyPattern: details.keyPattern ?? {},
    keyValue: details.keyValue ?? {},
  };
}

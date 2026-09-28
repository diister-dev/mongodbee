import { BSON } from "mongodb";

export class StudioHttpError extends Error {
  readonly status: number;
  readonly details?: Record<string, unknown>;

  constructor(
    status: number,
    message: string,
    details?: Record<string, unknown>,
  ) {
    super(message);
    this.status = status;
    if (details) this.details = details;
  }
}

export function toJsonSafe(value: unknown): unknown {
  return BSON.EJSON.serialize(value as BSON.Document, { relaxed: true });
}

export function encodeId(id: unknown): string {
  return BSON.EJSON.stringify(id as BSON.Document, { relaxed: false });
}

export function parseExtendedJson(raw: string): unknown {
  try {
    return BSON.EJSON.parse(raw, { relaxed: true });
  } catch {
    throw new StudioHttpError(400, `Invalid extended JSON value: ${raw}`);
  }
}

export function hasOperatorKeys(value: unknown): boolean {
  if (Array.isArray(value)) return value.some(hasOperatorKeys);
  if (!value || typeof value !== "object") return false;
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) return false;
  return Object.entries(value).some(
    ([key, inner]) => key.startsWith("$") || hasOperatorKeys(inner),
  );
}

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(toJsonSafe(body)), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

export function errorResponse(error: unknown): Response {
  if (error instanceof StudioHttpError) {
    return jsonResponse(
      error.details
        ? { error: error.message, ...error.details }
        : { error: error.message },
      error.status,
    );
  }
  const message = error instanceof Error ? error.message : String(error);
  return jsonResponse({ error: message }, 500);
}

export function clampInteger(
  raw: string | null,
  fallback: number,
  min: number,
  max: number,
): number {
  if (raw === null || raw === "") return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed)) {
    throw new StudioHttpError(400, `Expected an integer, got "${raw}"`);
  }
  return Math.min(max, Math.max(min, parsed));
}

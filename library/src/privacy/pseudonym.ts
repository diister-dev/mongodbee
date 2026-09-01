import { createHmac } from "node:crypto";
import { decodeTime } from "@std/ulid";
import {
  encodeUlidRandom,
  encodeUlidTime,
  isUlid,
} from "../utils/ulid-encode.ts";

export { isUlid };

export type PrivacySecret = string | Uint8Array;

export function hmacBytes(secret: PrivacySecret, message: string): Uint8Array {
  return new Uint8Array(createHmac("sha256", secret).update(message).digest());
}

export function hmacSeed(secret: PrivacySecret, message: string): number {
  const b = hmacBytes(secret, message);
  return ((b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3]) >>> 0;
}

export function canonical(value: unknown): string {
  if (typeof value === "string") return value.trim().toLowerCase();
  if (value instanceof Date) return value.toISOString();
  if (value === undefined) return "";
  return JSON.stringify(value) ?? String(value);
}

const ID_PATTERN = /^([a-zA-Z0-9_-]+):(.+)$/;

function base36(bytes: Uint8Array, length: number): string {
  let n = 0n;
  for (const b of bytes) n = (n << 8n) | BigInt(b);
  return n.toString(36).slice(0, length).padStart(length, "0");
}

export function remapUid(
  secret: PrivacySecret,
  uid: string,
  timeShiftMs = 0,
): string {
  const digest = hmacBytes(secret, `uid|${uid}`);
  if (isUlid(uid)) {
    const time = decodeTime(uid.toUpperCase()) + timeShiftMs;
    const out = encodeUlidTime(time) + encodeUlidRandom(digest);
    return uid === uid.toLowerCase() ? out.toLowerCase() : out;
  }
  return base36(digest, Math.max(12, Math.min(uid.length, 26)));
}

export function remapId(
  secret: PrivacySecret,
  id: string,
  timeShiftMs = 0,
): string {
  const match = ID_PATTERN.exec(id);
  if (!match) return remapUid(secret, id, timeShiftMs);
  return `${match[1]}:${remapUid(secret, match[2], timeShiftMs)}`;
}

export function looksLikeId(
  value: unknown,
  spaces?: readonly string[],
): value is string {
  if (typeof value !== "string") return false;
  const match = ID_PATTERN.exec(value);
  if (!match) return false;
  return spaces === undefined || spaces.length === 0 ||
    spaces.includes(match[1]);
}

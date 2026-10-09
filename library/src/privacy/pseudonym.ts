import { createHmac } from "node:crypto";
import { ObjectId } from "mongodb";
import { decodeTime } from "../utils/ulid.ts";
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

const MAX_ULID_TIME = 2 ** 48 - 1;

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
  const ulid = isUlid(uid);
  const digest = hmacBytes(secret, `uid|${ulid ? uid.toLowerCase() : uid}`);
  if (ulid) {
    const time = Math.min(
      MAX_ULID_TIME,
      Math.max(0, Math.round(decodeTime(uid.toUpperCase()) + timeShiftMs)),
    );
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
  return (
    spaces === undefined || spaces.length === 0 || spaces.includes(match[1])
  );
}

const MAX_OBJECT_ID_SECONDS = 2 ** 32 - 1;

export function isObjectId(value: unknown): value is ObjectId {
  return value instanceof ObjectId;
}

export function remapObjectId(
  secret: PrivacySecret,
  id: ObjectId,
  timeShiftMs = 0,
): ObjectId {
  const digest = hmacBytes(secret, `oid|${id.toHexString()}`);
  const seconds = Math.min(
    MAX_OBJECT_ID_SECONDS,
    Math.max(0, Math.round(id.getTimestamp().getTime() + timeShiftMs) / 1000),
  );
  const bytes = new Uint8Array(12);
  new DataView(bytes.buffer).setUint32(0, Math.floor(seconds));
  bytes.set(digest.subarray(0, 8), 4);
  return new ObjectId(bytes);
}

export function defaultTimeShiftMs(secret: PrivacySecret): number {
  const digest = hmacBytes(secret, "shift");
  const view = new DataView(digest.buffer, digest.byteOffset);
  const days = 30 + (view.getUint32(0) % 336);
  return -(days * 86_400_000 + (view.getUint32(4) % 86_400_000));
}

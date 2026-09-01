import { ulid } from "@std/ulid";
import type { MigrationTransformContext } from "../types.ts";
import { fnv1a32 } from "./seed-id.ts";
import { encodeUlidRandom, encodeUlidTime } from "../../utils/ulid-encode.ts";

const MIGRATION_TIME = /^(\d{4})_(\d{2})_(\d{2})_(\d{2})(\d{2})/;
const FALLBACK_TIME = Date.UTC(2000, 0, 1);

export function migrationTime(migrationId: string): number {
  const match = MIGRATION_TIME.exec(migrationId);
  if (!match) return FALLBACK_TIME;
  const [, y, mo, d, h, mi] = match;
  return Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi));
}

function hashBytes(input: string): Uint8Array {
  const out = new Uint8Array(12);
  for (let i = 0; i < 3; i++) {
    const h = fnv1a32(`${i}|${input}`);
    out[i * 4] = (h >>> 24) & 0xff;
    out[i * 4 + 1] = (h >>> 16) & 0xff;
    out[i * 4 + 2] = (h >>> 8) & 0xff;
    out[i * 4 + 3] = h & 0xff;
  }
  return out;
}

export function deterministicUlid(input: string, time: number): string {
  return (encodeUlidTime(time) + encodeUlidRandom(hashBytes(input)))
    .toLowerCase();
}

export function createDeterministicTransformContext(
  migrationId: string,
): MigrationTransformContext {
  const time = migrationTime(migrationId);
  let counter = 0;
  return {
    migrationId,
    newId: () => {
      const index = counter++;
      return deterministicUlid(`${migrationId}|newId|${index}`, time + index);
    },
    now: () => new Date(time),
  };
}

export function createLiveTransformContext(
  migrationId: string,
): MigrationTransformContext {
  return {
    migrationId,
    newId: () => ulid().toLowerCase(),
    now: () => new Date(),
  };
}
